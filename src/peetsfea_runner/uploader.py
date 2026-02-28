from __future__ import annotations

import shlex
import subprocess
from pathlib import Path, PurePosixPath
from time import time
from typing import Protocol

from peetsfea_runner.config import RunnerConfig
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore

E_UPLOAD_NETWORK = "E_UPLOAD_NETWORK"
E_UPLOAD_PERMISSION = "E_UPLOAD_PERMISSION"
E_UPLOAD_REMOTE_PATH = "E_UPLOAD_REMOTE_PATH"
E_UPLOAD_LOCAL_MISSING = "E_UPLOAD_LOCAL_MISSING"


class UploadClientError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class SpoolUploadClient(Protocol):
    def remote_file_exists(self, *, remote_host: str, remote_path: str) -> bool:
        raise NotImplementedError

    def upload_to_spool_inbox(self, *, local_path: Path, remote_host: str, remote_path: str) -> None:
        raise NotImplementedError


class SubprocessSpoolUploadClient:
    def _run_or_raise(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise UploadClientError(f"command={' '.join(args)}; detail={detail}")

    def remote_file_exists(self, *, remote_host: str, remote_path: str) -> bool:
        check_cmd = f"test -f {shlex.quote(remote_path)}"
        result = subprocess.run(["ssh", remote_host, check_cmd], capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise UploadClientError(f"command=ssh {remote_host} {check_cmd}; detail={detail}")

    def upload_to_spool_inbox(self, *, local_path: Path, remote_host: str, remote_path: str) -> None:
        remote_dir = str(PurePosixPath(remote_path).parent)
        tmp_remote_path = f"{remote_path}.part"
        mkdir_cmd = f"mkdir -p {shlex.quote(remote_dir)}"
        move_cmd = f"mv {shlex.quote(tmp_remote_path)} {shlex.quote(remote_path)}"

        self._run_or_raise(["ssh", remote_host, mkdir_cmd])
        self._run_or_raise(["scp", str(local_path), f"{remote_host}:{tmp_remote_path}"])
        self._run_or_raise(["ssh", remote_host, move_cmd])


class UploadDispatcher:
    def __init__(
        self,
        config: RunnerConfig,
        store: JobStore,
        client: SpoolUploadClient | None = None,
    ) -> None:
        self._config = config
        self._store = store
        self._client = client if client is not None else SubprocessSpoolUploadClient()

    def _build_remote_inbox_path(self, *, task_id: str, filename: str) -> str:
        base = self._config.gate_account.spool_paths.inbox.rstrip("/")
        return str(PurePosixPath(base) / task_id / filename)

    def _build_uploaded_path(self, filename: str, uploaded_path_text: str) -> Path:
        if uploaded_path_text:
            return Path(uploaded_path_text)
        return self._config.queue_dirs.uploaded / filename

    def _failed_local_target(self, source_path: Path, tag: str) -> Path:
        stamp = int(time() * 1000)
        return self._config.queue_dirs.failed / f"{source_path.stem}.{tag}_{stamp}{source_path.suffix}"

    def _classify_upload_error(self, message: str) -> str:
        lowered = message.lower()
        if "permission denied" in lowered:
            return E_UPLOAD_PERMISSION
        if "no such file or directory" in lowered:
            return E_UPLOAD_REMOTE_PATH
        return E_UPLOAD_NETWORK

    def _move_to_uploaded(self, *, source_path: Path, uploaded_path: Path) -> None:
        uploaded_path.parent.mkdir(parents=True, exist_ok=True)
        if uploaded_path.exists():
            # Preserve duplicate copy for audit instead of overwriting uploaded artifact.
            duplicate_target = self._failed_local_target(source_path, "upload_duplicate")
            source_path.rename(duplicate_target)
            return
        source_path.rename(uploaded_path)

    def _mark_upload_success(
        self,
        *,
        task_id: str,
        uploaded_path: Path,
        remote_inbox_path: str,
        event_type: str,
    ) -> None:
        self._store.mark_uploaded(
            task_id=task_id,
            uploaded_path=str(uploaded_path),
            remote_inbox_path=remote_inbox_path,
        )
        self._store.record_event(
            task_id=task_id,
            event_type=event_type,
            message=f"uploaded={uploaded_path}; remote={remote_inbox_path}",
        )

    def process_once(self) -> int:
        processed = 0
        for task_id, filename, pending_path_text, uploaded_path_text, remote_path_text in self._store.list_pending_upload_jobs():
            pending_path = Path(pending_path_text) if pending_path_text else self._config.queue_dirs.pending / filename
            uploaded_path = self._build_uploaded_path(filename, uploaded_path_text)
            remote_inbox_path = remote_path_text or self._build_remote_inbox_path(task_id=task_id, filename=filename)

            try:
                remote_exists = self._client.remote_file_exists(
                    remote_host=self._config.gate_account.ssh_alias,
                    remote_path=remote_inbox_path,
                )
            except UploadClientError as exc:
                error_code = self._classify_upload_error(exc.message)
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_UPLOAD,
                    error_code=error_code,
                    error_message=exc.message,
                )
                self._store.record_event(
                    task_id=task_id,
                    event_type="UPLOAD_FAILED",
                    error_code=error_code,
                    message=exc.message,
                )
                processed += 1
                continue

            if remote_exists:
                if pending_path.exists():
                    self._move_to_uploaded(source_path=pending_path, uploaded_path=uploaded_path)
                self._mark_upload_success(
                    task_id=task_id,
                    uploaded_path=uploaded_path,
                    remote_inbox_path=remote_inbox_path,
                    event_type="UPLOAD_RECOVERED_REMOTE_EXISTS",
                )
                processed += 1
                continue

            source_for_upload: Path | None = None
            if pending_path.exists():
                source_for_upload = pending_path
            elif uploaded_path.exists():
                source_for_upload = uploaded_path
                self._store.record_event(
                    task_id=task_id,
                    event_type="UPLOAD_RESUME_FROM_UPLOADED",
                    message=f"uploaded={uploaded_path}",
                )

            if source_for_upload is None:
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_UPLOAD,
                    error_code=E_UPLOAD_LOCAL_MISSING,
                    error_message="No local source found for upload (pending/uploaded missing)",
                )
                self._store.record_event(
                    task_id=task_id,
                    event_type="UPLOAD_FAILED",
                    error_code=E_UPLOAD_LOCAL_MISSING,
                    message="No local source found for upload (pending/uploaded missing)",
                )
                processed += 1
                continue

            self._store.record_event(
                task_id=task_id,
                event_type="UPLOAD_STARTED",
                message=f"source={source_for_upload}; remote={remote_inbox_path}",
            )

            try:
                self._client.upload_to_spool_inbox(
                    local_path=source_for_upload,
                    remote_host=self._config.gate_account.ssh_alias,
                    remote_path=remote_inbox_path,
                )
            except UploadClientError as exc:
                error_code = self._classify_upload_error(exc.message)
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_UPLOAD,
                    error_code=error_code,
                    error_message=exc.message,
                )
                self._store.record_event(
                    task_id=task_id,
                    event_type="UPLOAD_FAILED",
                    error_code=error_code,
                    message=exc.message,
                )
                processed += 1
                continue

            if source_for_upload == pending_path and pending_path.exists():
                self._move_to_uploaded(source_path=pending_path, uploaded_path=uploaded_path)

            self._mark_upload_success(
                task_id=task_id,
                uploaded_path=uploaded_path,
                remote_inbox_path=remote_inbox_path,
                event_type="UPLOAD_DONE",
            )
            processed += 1

        return processed
