from __future__ import annotations

import base64
import shlex
import shutil
import subprocess
from pathlib import Path
from time import time
from typing import Protocol

from peetsfea_runner.config import GateAccount, RunnerConfig
from peetsfea_runner.event_types import (
    COLLECT_DONE,
    COLLECT_DUPLICATE_SKIPPED,
    COLLECT_FAILED,
    COLLECT_STARTED,
)
from peetsfea_runner.ssh_transport import scp_from_remote_command, ssh_command
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore

E_COLLECT_LIST = "E_COLLECT_LIST"
E_COLLECT_DOWNLOAD = "E_COLLECT_DOWNLOAD"
E_COLLECT_LOCAL_MOVE = "E_COLLECT_LOCAL_MOVE"


class ResultsClientError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class SpoolResultsClient(Protocol):
    def list_result_zips(self, *, remote_host: str, remote_results_path: str) -> list[str]:
        raise NotImplementedError

    def download_result_zip(self, *, remote_host: str, remote_path: str, local_path: Path) -> None:
        raise NotImplementedError


class SubprocessSpoolResultsClient:
    @staticmethod
    def _is_windows_path(path: str) -> bool:
        return len(path) >= 2 and path[1] == ":"

    @staticmethod
    def _ps_quote(path: str) -> str:
        return "'" + path.replace("'", "''") + "'"

    @staticmethod
    def _powershell_encoded(script: str) -> str:
        encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
        return f"powershell -NoProfile -EncodedCommand {encoded}"

    def _run_or_raise(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(args, capture_output=True, text=True, check=False, errors="replace")
        except TypeError:
            result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise ResultsClientError(f"command={' '.join(args)}; detail={detail}")

    def list_result_zips(self, *, remote_host: str, remote_results_path: str) -> list[str]:
        if self._is_windows_path(remote_results_path):
            list_cmd = self._powershell_encoded(
                "if (Test-Path -LiteralPath "
                f"{self._ps_quote(remote_results_path)}) "
                "{ Get-ChildItem -LiteralPath "
                f"{self._ps_quote(remote_results_path)} "
                "-Recurse -File -Filter '*.reports.zip' | Sort-Object FullName | "
                "ForEach-Object { $_.FullName } }"
            )
        else:
            escaped_dir = shlex.quote(remote_results_path)
            list_cmd = (
                f"if [ -d {escaped_dir} ]; then "
                f"find {escaped_dir} -type f -name '*.reports.zip' | sort; "
                "fi"
            )
        result = self._run_or_raise(ssh_command(remote_host=remote_host, remote_command=list_cmd))
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return lines

    def download_result_zip(self, *, remote_host: str, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_or_raise(
            scp_from_remote_command(remote_host=remote_host, remote_path=remote_path, local_path=str(local_path))
        )


class ResultsCollector:
    def __init__(
        self,
        config: RunnerConfig,
        store: JobStore,
        client: SpoolResultsClient | None = None,
    ) -> None:
        self._config = config
        self._store = store
        self._client = client if client is not None else SubprocessSpoolResultsClient()
        self._gate_account_by_id: dict[str, GateAccount] = {
            account.account_id: account for account in config.gate_accounts
        }

    def _resolve_candidates(self, preferred_accounts: tuple[GateAccount, ...] | None) -> tuple[GateAccount, ...]:
        if preferred_accounts:
            return preferred_accounts
        if self._config.gate_accounts:
            return self._config.gate_accounts
        return (self._config.gate_account,)

    def _extract_task_id(self, remote_zip_path: str) -> str | None:
        name = remote_zip_path.replace("\\", "/").rsplit("/", 1)[-1]
        if not name.endswith(".reports.zip"):
            return None
        task_id = name.removesuffix(".reports.zip")
        return task_id if task_id else None

    def _build_local_done_path(self, task_id: str) -> Path:
        return self._config.queue_dirs.done / f"{task_id}.reports.zip"

    def _build_tmp_download_path(self, local_done_path: Path) -> Path:
        stamp = int(time() * 1000)
        return local_done_path.parent / f".{local_done_path.name}.{stamp}.part"

    def _mark_collected(
        self,
        *,
        task_id: str,
        remote_path: str,
        local_path: Path,
    ) -> None:
        if self._store.task_exists(task_id):
            self._store.set_report_zip_paths(
                task_id=task_id,
                report_zip_local_path=str(local_path),
                report_zip_remote_path=remote_path,
            )
            self._store.update_state_by_task_id(task_id=task_id, state=JobState.DONE)
            return

        filename = f"{task_id}.aedt"
        inserted = self._store.insert_job(
            task_id=task_id,
            filename=filename,
            source_path=str(self._config.queue_dirs.incoming / filename),
            pending_path=str(self._config.queue_dirs.pending / filename),
            uploaded_path=str(self._config.queue_dirs.uploaded / filename),
            report_zip_local_path=str(local_path),
            report_zip_remote_path=remote_path,
            state=JobState.DONE,
        )
        if not inserted:
            self._store.set_report_zip_paths(
                task_id=task_id,
                report_zip_local_path=str(local_path),
                report_zip_remote_path=remote_path,
            )
            self._store.update_state_by_task_id(task_id=task_id, state=JobState.DONE)

    def _collect_one(
        self,
        *,
        gate_account: GateAccount,
        remote_zip_path: str,
    ) -> bool:
        task_id = self._extract_task_id(remote_zip_path)
        if task_id is None:
            return False

        local_done_path = self._build_local_done_path(task_id)
        if local_done_path.exists():
            self._mark_collected(task_id=task_id, remote_path=remote_zip_path, local_path=local_done_path)
            self._store.record_event(
                task_id=task_id,
                event_type=COLLECT_DUPLICATE_SKIPPED,
                message=f"remote_account={gate_account.account_id}; remote={remote_zip_path}; local={local_done_path}",
            )
            return True

        tmp_path = self._build_tmp_download_path(local_done_path)
        self._store.record_event(
            task_id=task_id,
            event_type=COLLECT_STARTED,
            message=f"remote_account={gate_account.account_id}; remote={remote_zip_path}",
        )
        try:
            self._client.download_result_zip(
                remote_host=gate_account.ssh_alias,
                remote_path=remote_zip_path,
                local_path=tmp_path,
            )
            local_done_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.replace(local_done_path)
        except ResultsClientError as exc:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            self._store.record_event(
                task_id=task_id,
                event_type=COLLECT_FAILED,
                error_code=E_COLLECT_DOWNLOAD,
                message=exc.message,
            )
            return True
        except OSError as exc:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            self._store.record_event(
                task_id=task_id,
                event_type=COLLECT_FAILED,
                error_code=E_COLLECT_LOCAL_MOVE,
                message=str(exc),
            )
            return True

        self._mark_collected(task_id=task_id, remote_path=remote_zip_path, local_path=local_done_path)
        self._store.record_event(
            task_id=task_id,
            event_type=COLLECT_DONE,
            message=f"remote_account={gate_account.account_id}; remote={remote_zip_path}; local={local_done_path}",
        )
        return True

    def process_once(self, preferred_accounts: tuple[GateAccount, ...] | None = None) -> int:
        processed = 0
        candidates = self._resolve_candidates(preferred_accounts)
        for gate_account in candidates:
            try:
                remote_zip_paths = self._client.list_result_zips(
                    remote_host=gate_account.ssh_alias,
                    remote_results_path=gate_account.spool_paths.results,
                )
            except ResultsClientError:
                continue

            for remote_zip_path in remote_zip_paths:
                if self._collect_one(gate_account=gate_account, remote_zip_path=remote_zip_path):
                    processed += 1

        return processed


class LocalMirrorSpoolResultsClient:
    """Test-friendly client for local mirrors of remote spool paths."""

    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def list_result_zips(self, *, remote_host: str, remote_results_path: str) -> list[str]:
        _ = remote_host
        base = self._local_remote_path(remote_results_path)
        if not base.exists():
            return []
        result: list[str] = []
        for path in sorted(base.rglob("*.reports.zip")):
            if not path.is_file():
                continue
            relative = path.relative_to(base).as_posix()
            result.append(f"{remote_results_path.rstrip('/')}/{relative}")
        return result

    def download_result_zip(self, *, remote_host: str, remote_path: str, local_path: Path) -> None:
        _ = remote_host
        source = self._local_remote_path(remote_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, local_path)
