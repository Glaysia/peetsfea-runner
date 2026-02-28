from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import time

from peetsfea_runner.config import RunnerConfig
from peetsfea_runner.event_types import (
    AEDT_DELETE_LOCAL_DONE,
    AEDT_DELETE_LOCAL_FAILED,
    AEDT_RETENTION_VIOLATION_DETECTED,
    RECONCILE_DONE_ZIP_MISSING,
    RECONCILE_DONE_ZIP_RECOVERED,
    RECONCILE_PENDING_TO_UPLOADED,
    RECONCILE_PENDING_TTL_REQUEUE,
    RECONCILE_PENDING_TTL_REQUEUE_CONFLICT,
    RECONCILE_PENDING_TTL_REQUEUE_FAILED,
    RECONCILE_RETRY_PENDING_FROM_FAILED_UPLOAD,
    RECONCILE_UPLOADED_TO_PENDING,
    RECONCILE_UPLOAD_SOURCE_MISSING,
)
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore

E_RECONCILE_UPLOAD_SOURCE_MISSING = "E_RECONCILE_UPLOAD_SOURCE_MISSING"
E_RECONCILE_DONE_ZIP_MISSING = "E_RECONCILE_DONE_ZIP_MISSING"
E_RECONCILE_AEDT_DELETE_FAILED = "E_RECONCILE_AEDT_DELETE_FAILED"
E_RECONCILE_PENDING_REQUEUE_FAILED = "E_RECONCILE_PENDING_REQUEUE_FAILED"


@dataclass(frozen=True)
class ReconcilePolicy:
    pending_ttl_sec: int = 3600


class OperationsReconciler:
    def __init__(
        self,
        config: RunnerConfig,
        store: JobStore,
        policy: ReconcilePolicy | None = None,
    ) -> None:
        self._config = config
        self._store = store
        self._policy = policy if policy is not None else ReconcilePolicy()

    def _failed_target_path(self, *, filename: str, tag: str) -> Path:
        source = Path(filename)
        stamp = int(time() * 1000)
        return self._config.queue_dirs.failed / f"{source.stem}.{tag}_{stamp}{source.suffix}"

    def _record_event(self, *, task_id: str, event_type: str, error_code: str | None = None, message: str | None = None) -> None:
        self._store.record_event(
            task_id=task_id,
            event_type=event_type,
            error_code=error_code,
            message=message,
        )

    def _requeue_pending_if_ttl_exceeded(
        self,
        *,
        task_id: str,
        filename: str,
        pending_path: Path,
        state: str,
        now_ts: float,
    ) -> bool:
        if state != JobState.PENDING.value:
            return False
        if not pending_path.exists():
            return False

        age_sec = now_ts - pending_path.stat().st_mtime
        if age_sec < self._policy.pending_ttl_sec:
            return False

        incoming_path = self._config.queue_dirs.incoming / filename
        try:
            if incoming_path.exists():
                quarantine = self._failed_target_path(filename=filename, tag="requeue_duplicate")
                pending_path.rename(quarantine)
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_LOCAL,
                    error_code=E_RECONCILE_PENDING_REQUEUE_FAILED,
                    error_message=f"Incoming target already exists: {incoming_path}",
                )
                self._record_event(
                    task_id=task_id,
                    event_type=RECONCILE_PENDING_TTL_REQUEUE_CONFLICT,
                    error_code=E_RECONCILE_PENDING_REQUEUE_FAILED,
                    message=f"quarantined={quarantine}",
                )
                return True

            pending_path.rename(incoming_path)
            self._store.update_state_by_task_id(task_id=task_id, state=JobState.NEW)
            self._record_event(
                task_id=task_id,
                event_type=RECONCILE_PENDING_TTL_REQUEUE,
                message=f"pending={pending_path}; incoming={incoming_path}; age_sec={int(age_sec)}",
            )
            return True
        except OSError as exc:
            self._store.update_state_by_task_id(
                task_id=task_id,
                state=JobState.FAILED_LOCAL,
                error_code=E_RECONCILE_PENDING_REQUEUE_FAILED,
                error_message=str(exc),
            )
            self._record_event(
                task_id=task_id,
                event_type=RECONCILE_PENDING_TTL_REQUEUE_FAILED,
                error_code=E_RECONCILE_PENDING_REQUEUE_FAILED,
                message=str(exc),
            )
            return True

    def _reconcile_upload_state(
        self,
        *,
        task_id: str,
        pending_path: Path,
        uploaded_path: Path,
        state: str,
    ) -> bool:
        pending_exists = pending_path.exists()
        uploaded_exists = uploaded_path.exists() if str(uploaded_path) else False

        if state == JobState.PENDING.value:
            if not pending_exists and uploaded_exists:
                self._store.update_state_by_task_id(task_id=task_id, state=JobState.UPLOADED)
                self._record_event(
                    task_id=task_id,
                    event_type=RECONCILE_PENDING_TO_UPLOADED,
                    message=f"uploaded={uploaded_path}",
                )
                return True

            if not pending_exists and not uploaded_exists:
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_UPLOAD,
                    error_code=E_RECONCILE_UPLOAD_SOURCE_MISSING,
                    error_message="No local upload source found during reconcile",
                )
                self._record_event(
                    task_id=task_id,
                    event_type=RECONCILE_UPLOAD_SOURCE_MISSING,
                    error_code=E_RECONCILE_UPLOAD_SOURCE_MISSING,
                    message="pending/uploaded artifact missing",
                )
                return True

        if state == JobState.UPLOADED.value and not uploaded_exists and pending_exists:
            self._store.update_state_by_task_id(task_id=task_id, state=JobState.PENDING)
            self._record_event(
                task_id=task_id,
                event_type=RECONCILE_UPLOADED_TO_PENDING,
                message=f"pending={pending_path}",
            )
            return True

        if state == JobState.FAILED_UPLOAD.value and pending_exists:
            self._store.update_state_by_task_id(task_id=task_id, state=JobState.PENDING)
            self._record_event(
                task_id=task_id,
                event_type=RECONCILE_RETRY_PENDING_FROM_FAILED_UPLOAD,
                message=f"pending={pending_path}",
            )
            return True

        return False

    def _reconcile_done_zip(
        self,
        *,
        task_id: str,
        report_zip_local_path_text: str,
        state: str,
    ) -> bool:
        if state != JobState.DONE.value:
            return False

        report_zip_path = Path(report_zip_local_path_text) if report_zip_local_path_text else None
        if report_zip_path is not None and report_zip_path.exists():
            return False

        candidate = self._config.queue_dirs.done / f"{task_id}.reports.zip"
        if candidate.exists():
            self._store.set_report_zip_local_path(task_id=task_id, report_zip_local_path=str(candidate))
            self._record_event(
                task_id=task_id,
                event_type=RECONCILE_DONE_ZIP_RECOVERED,
                message=f"report_zip_local_path={candidate}",
            )
            return True

        self._store.update_state_by_task_id(
            task_id=task_id,
            state=JobState.FAILED,
            error_code=E_RECONCILE_DONE_ZIP_MISSING,
            error_message="DONE job has no local report zip",
        )
        self._record_event(
            task_id=task_id,
            event_type=RECONCILE_DONE_ZIP_MISSING,
            error_code=E_RECONCILE_DONE_ZIP_MISSING,
            message="DONE job has no local report zip",
        )
        return True

    def _audit_done_aedt_retention(
        self,
        *,
        task_id: str,
        filename: str,
        source_path_text: str,
        pending_path_text: str,
        uploaded_path_text: str,
        state: str,
        aedt_retention: str,
    ) -> bool:
        if state != JobState.DONE.value:
            return False
        if aedt_retention != "delete_after_done":
            return False

        candidates: list[Path] = []
        if source_path_text:
            candidates.append(Path(source_path_text))
        if pending_path_text:
            candidates.append(Path(pending_path_text))
        if uploaded_path_text:
            candidates.append(Path(uploaded_path_text))
        candidates.extend(
            [
                self._config.queue_dirs.incoming / filename,
                self._config.queue_dirs.pending / filename,
                self._config.queue_dirs.uploaded / filename,
            ]
        )

        processed = False
        for candidate in dict.fromkeys(candidates):
            if candidate.suffix.lower() != ".aedt":
                continue
            if not candidate.exists():
                continue
            processed = True
            self._record_event(
                task_id=task_id,
                event_type=AEDT_RETENTION_VIOLATION_DETECTED,
                message=f"path={candidate}",
            )
            try:
                candidate.unlink()
            except OSError as exc:
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED,
                    error_code=E_RECONCILE_AEDT_DELETE_FAILED,
                    error_message=str(exc),
                )
                self._record_event(
                    task_id=task_id,
                    event_type=AEDT_DELETE_LOCAL_FAILED,
                    error_code=E_RECONCILE_AEDT_DELETE_FAILED,
                    message=str(exc),
                )
                return True

            self._store.mark_local_aedt_deleted(task_id=task_id)
            self._record_event(
                task_id=task_id,
                event_type=AEDT_DELETE_LOCAL_DONE,
                message=f"path={candidate}",
            )

        return processed

    def process_once(self) -> int:
        processed = 0
        now_ts = time()

        for (
            task_id,
            filename,
            source_path_text,
            pending_path_text,
            uploaded_path_text,
            report_zip_local_path_text,
            state,
            aedt_retention,
        ) in self._store.list_jobs_for_reconcile():
            pending_path = Path(pending_path_text) if pending_path_text else self._config.queue_dirs.pending / filename
            uploaded_path = Path(uploaded_path_text) if uploaded_path_text else self._config.queue_dirs.uploaded / filename

            if self._requeue_pending_if_ttl_exceeded(
                task_id=task_id,
                filename=filename,
                pending_path=pending_path,
                state=state,
                now_ts=now_ts,
            ):
                processed += 1
                continue

            if self._reconcile_upload_state(
                task_id=task_id,
                pending_path=pending_path,
                uploaded_path=uploaded_path,
                state=state,
            ):
                processed += 1

            # refresh state if it changed
            state_after = self._store.get_job_state(task_id) or state

            if self._reconcile_done_zip(
                task_id=task_id,
                report_zip_local_path_text=report_zip_local_path_text,
                state=state_after,
            ):
                processed += 1
                state_after = self._store.get_job_state(task_id) or state_after

            if self._audit_done_aedt_retention(
                task_id=task_id,
                filename=filename,
                source_path_text=source_path_text,
                pending_path_text=pending_path_text,
                uploaded_path_text=uploaded_path_text,
                state=state_after,
                aedt_retention=aedt_retention,
            ):
                processed += 1

        return processed
