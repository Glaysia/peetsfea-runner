from __future__ import annotations

from pathlib import Path
from time import time

from peetsfea_runner.config import RunnerConfig
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore

E_DUPLICATE_TASK_ID = "E_DUPLICATE_TASK_ID"
E_INGEST_PENDING_COLLISION = "E_INGEST_PENDING_COLLISION"
E_INGEST_MOVE_PENDING = "E_INGEST_MOVE_PENDING"
E_INGEST_QUARANTINE_FAILED = "E_INGEST_QUARANTINE_FAILED"


def discover_aedt_files(incoming_dir: Path) -> list[Path]:
    return sorted(path for path in incoming_dir.glob("*.aedt") if path.is_file())


def task_id_from_path(source_path: Path) -> str:
    # Stable and deterministic task identifier for local intake phase.
    return source_path.stem


class QueueWatcher:
    def __init__(self, config: RunnerConfig, store: JobStore) -> None:
        self._config = config
        self._store = store

    def _failed_target_path(self, source_path: Path, tag: str) -> Path:
        stamp = int(time() * 1000)
        return self._config.queue_dirs.failed / f"{source_path.stem}.{tag}_{stamp}{source_path.suffix}"

    def _quarantine_source(
        self,
        *,
        source_path: Path,
        task_id: str,
        tag: str,
        event_type: str,
        error_code: str,
        message: str,
    ) -> None:
        target_path = self._failed_target_path(source_path, tag)
        try:
            source_path.rename(target_path)
            self._store.record_event(
                task_id=task_id,
                event_type=event_type,
                error_code=error_code,
                message=f"{message}; moved_to={target_path}",
            )
        except OSError as exc:
            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_QUARANTINE_FAILED",
                error_code=E_INGEST_QUARANTINE_FAILED,
                message=f"{message}; quarantine_error={exc}",
            )

    def _recover_pending_state_gaps(self) -> int:
        recovered = 0
        for task_id, pending_path_text in self._store.list_jobs_by_state(JobState.NEW):
            if not pending_path_text:
                continue
            pending_path = Path(pending_path_text)
            if not pending_path.exists():
                continue

            self._store.update_state_by_task_id(
                task_id=task_id,
                state=JobState.PENDING,
                error_code=None,
                error_message=None,
            )
            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_RECOVERED_PENDING_STATE",
                message=f"pending={pending_path}",
            )
            recovered += 1
        return recovered

    def _resume_existing_task(self, *, source_path: Path, task_id: str) -> int:
        pending_path = self._config.queue_dirs.pending / source_path.name
        self._store.refresh_job_paths(
            task_id=task_id,
            filename=source_path.name,
            source_path=str(source_path),
            pending_path=str(pending_path),
        )

        if pending_path.exists():
            # Existing pending file wins; incoming copy is treated as duplicate payload.
            self._store.update_state_by_task_id(
                task_id=task_id,
                state=JobState.PENDING,
                error_code=None,
                error_message=None,
            )
            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_RESUME_PENDING_ALREADY_EXISTS",
                message=f"pending={pending_path}",
            )
            self._quarantine_source(
                source_path=source_path,
                task_id=task_id,
                tag="duplicate",
                event_type="INGEST_DUPLICATE_IGNORED",
                error_code=E_DUPLICATE_TASK_ID,
                message="Duplicate payload ignored while existing task resumed",
            )
            return 1

        try:
            source_path.rename(pending_path)
        except OSError as exc:
            self._store.update_state_by_task_id(
                task_id=task_id,
                state=JobState.FAILED_LOCAL,
                error_code=E_INGEST_MOVE_PENDING,
                error_message=str(exc),
            )
            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_MOVE_FAILED",
                error_code=E_INGEST_MOVE_PENDING,
                message=str(exc),
            )
            self._quarantine_source(
                source_path=source_path,
                task_id=task_id,
                tag="ingest_failed",
                event_type="INGEST_SOURCE_QUARANTINED",
                error_code=E_INGEST_MOVE_PENDING,
                message="Source quarantined after resume move failure",
            )
            return 1

        self._store.update_state_by_task_id(
            task_id=task_id,
            state=JobState.PENDING,
            error_code=None,
            error_message=None,
        )
        self._store.record_event(
            task_id=task_id,
            event_type="INGEST_RESUMED_TO_PENDING",
            message=f"pending={pending_path}",
        )
        return 1

    def process_once(self) -> int:
        processed = self._recover_pending_state_gaps()
        for source_path in discover_aedt_files(self._config.queue_dirs.incoming):
            task_id = task_id_from_path(source_path)

            if self._store.task_exists(task_id):
                existing = self._store.get_job_row(task_id)
                if existing is not None and existing[4] in (JobState.NEW.value, JobState.FAILED_LOCAL.value):
                    processed += self._resume_existing_task(source_path=source_path, task_id=task_id)
                    continue

                self._quarantine_source(
                    source_path=source_path,
                    task_id=task_id,
                    tag="duplicate",
                    event_type="INGEST_DUPLICATE_IGNORED",
                    error_code=E_DUPLICATE_TASK_ID,
                    message="Duplicate task_id detected in incoming",
                )
                processed += 1
                continue

            pending_path = self._config.queue_dirs.pending / source_path.name
            inserted = self._store.insert_job(
                task_id=task_id,
                filename=source_path.name,
                source_path=str(source_path),
                pending_path=str(pending_path),
                state=JobState.NEW,
                error_code=None,
                error_message=None,
            )
            if not inserted:
                # Race-safe fallback: task created by another loop before insert.
                self._quarantine_source(
                    source_path=source_path,
                    task_id=task_id,
                    tag="duplicate",
                    event_type="INGEST_DUPLICATE_IGNORED",
                    error_code=E_DUPLICATE_TASK_ID,
                    message="Duplicate task_id detected during insert",
                )
                processed += 1
                continue

            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_REGISTERED",
                message=f"incoming={source_path}",
            )

            if pending_path.exists():
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_LOCAL,
                    error_code=E_INGEST_PENDING_COLLISION,
                    error_message=f"Pending path already exists: {pending_path}",
                )
                self._quarantine_source(
                    source_path=source_path,
                    task_id=task_id,
                    tag="pending_collision",
                    event_type="INGEST_PENDING_COLLISION",
                    error_code=E_INGEST_PENDING_COLLISION,
                    message=f"Pending path already exists: {pending_path}",
                )
                processed += 1
                continue

            try:
                source_path.rename(pending_path)
            except OSError as exc:
                self._store.update_state_by_task_id(
                    task_id=task_id,
                    state=JobState.FAILED_LOCAL,
                    error_code=E_INGEST_MOVE_PENDING,
                    error_message=str(exc),
                )
                self._store.record_event(
                    task_id=task_id,
                    event_type="INGEST_MOVE_FAILED",
                    error_code=E_INGEST_MOVE_PENDING,
                    message=str(exc),
                )
                self._quarantine_source(
                    source_path=source_path,
                    task_id=task_id,
                    tag="ingest_failed",
                    event_type="INGEST_SOURCE_QUARANTINED",
                    error_code=E_INGEST_MOVE_PENDING,
                    message="Source quarantined after move failure",
                )
                processed += 1
                continue

            self._store.update_state_by_task_id(
                task_id=task_id,
                state=JobState.PENDING,
                error_code=None,
                error_message=None,
            )
            self._store.record_event(
                task_id=task_id,
                event_type="INGEST_MOVED_TO_PENDING",
                message=f"pending={pending_path}",
            )
            processed += 1

        return processed
