from __future__ import annotations

from pathlib import Path
from time import time
from uuid import uuid4

from peetsfea_runner.config import RunnerConfig
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


def discover_aedt_files(inbox_dir: Path) -> list[Path]:
    return sorted(path for path in inbox_dir.glob("*.aedt") if path.is_file())


class QueueWatcher:
    def __init__(self, config: RunnerConfig, store: JobStore) -> None:
        self._config = config
        self._store = store

    def _duplicate_target_path(self, source_path: Path) -> Path:
        stamp = int(time() * 1000)
        return self._config.queue_dirs.failed / f"{source_path.stem}.duplicate_{stamp}{source_path.suffix}"

    def process_once(self) -> int:
        processed = 0
        for source_path in discover_aedt_files(self._config.queue_dirs.inbox):
            filename = source_path.name
            if self._store.filename_exists(filename):
                self._store.update_state_by_filename(
                    filename=filename,
                    state=JobState.SKIPPED_DUPLICATE,
                    error_message="Duplicate filename detected in inbox",
                )
                source_path.rename(self._duplicate_target_path(source_path))
                processed += 1
                continue

            job_id = str(uuid4())
            staging_path = self._config.queue_dirs.staging / filename
            inserted = self._store.insert_job(
                job_id=job_id,
                filename=filename,
                source_path=str(source_path),
                staging_path=str(staging_path),
                state=JobState.QUEUED,
            )
            if not inserted:
                # Race-safe fallback: move to failed and continue.
                source_path.rename(self._duplicate_target_path(source_path))
                continue

            try:
                source_path.rename(staging_path)
            except OSError as exc:
                self._store.update_state_by_job_id(
                    job_id=job_id,
                    state=JobState.FAILED_LOCAL,
                    error_message=str(exc),
                )
                continue

            self._store.update_state_by_job_id(
                job_id=job_id,
                state=JobState.STAGED,
                error_message=None,
            )
            processed += 1

        return processed
