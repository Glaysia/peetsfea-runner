from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import RunnerConfig, build_queue_dirs
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore
from peetsfea_runner.watcher import QueueWatcher


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=base_dir / "runner.duckdb",
        queue_dirs=queue_dirs,
    )


def _mkdir_queue_dirs(config: RunnerConfig) -> None:
    config.base_dir.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.inbox.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.staging.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.done.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.failed.mkdir(parents=True, exist_ok=True)


def test_watcher_moves_only_aedt_to_staging(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    (config.queue_dirs.inbox / "ok.aedt").write_text("aedt")
    (config.queue_dirs.inbox / "ignore.txt").write_text("txt")

    watcher = QueueWatcher(config, store)
    processed = watcher.process_once()

    assert processed == 1
    assert not (config.queue_dirs.inbox / "ok.aedt").exists()
    assert (config.queue_dirs.staging / "ok.aedt").exists()
    assert (config.queue_dirs.inbox / "ignore.txt").exists()
    assert store.get_job_state("ok.aedt") == JobState.STAGED.value
    store.close()


def test_watcher_marks_duplicate_and_moves_to_failed(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    original = config.queue_dirs.inbox / "dup.aedt"
    original.write_text("first")

    watcher = QueueWatcher(config, store)
    watcher.process_once()

    duplicate = config.queue_dirs.inbox / "dup.aedt"
    duplicate.write_text("second")

    processed = watcher.process_once()

    failed_matches = list(config.queue_dirs.failed.glob("dup.duplicate_*.aedt"))
    assert processed == 1
    assert len(failed_matches) == 1
    assert store.get_job_state("dup.aedt") == JobState.SKIPPED_DUPLICATE.value
    store.close()
