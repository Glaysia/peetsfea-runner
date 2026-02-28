from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import RunnerConfig, build_queue_dirs
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore
from peetsfea_runner.watcher import (
    E_DUPLICATE_TASK_ID,
    E_INGEST_PENDING_COLLISION,
    QueueWatcher,
)


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=queue_dirs.state / "runner.duckdb",
        queue_dirs=queue_dirs,
    )


def _mkdir_queue_dirs(config: RunnerConfig) -> None:
    config.base_dir.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.incoming.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.pending.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.uploaded.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.done.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.failed.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.state.mkdir(parents=True, exist_ok=True)


def test_watcher_moves_only_aedt_to_pending(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    (config.queue_dirs.incoming / "ok.aedt").write_text("aedt")
    (config.queue_dirs.incoming / "ignore.txt").write_text("txt")

    watcher = QueueWatcher(config, store)
    processed = watcher.process_once()

    assert processed == 1
    assert not (config.queue_dirs.incoming / "ok.aedt").exists()
    assert (config.queue_dirs.pending / "ok.aedt").exists()
    assert (config.queue_dirs.incoming / "ignore.txt").exists()
    assert store.get_job_state("ok") == JobState.PENDING.value

    events = store.get_task_events("ok")
    assert [event[0] for event in events] == ["INGEST_REGISTERED", "INGEST_MOVED_TO_PENDING"]
    store.close()


def test_watcher_keeps_same_task_and_records_duplicate_event(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    first = config.queue_dirs.incoming / "dup.aedt"
    first.write_text("first")

    watcher = QueueWatcher(config, store)
    watcher.process_once()

    duplicate = config.queue_dirs.incoming / "dup.aedt"
    duplicate.write_text("second")

    processed = watcher.process_once()

    failed_matches = list(config.queue_dirs.failed.glob("dup.duplicate_*.aedt"))
    assert processed == 1
    assert len(failed_matches) == 1
    assert store.get_job_state("dup") == JobState.PENDING.value

    events = store.get_task_events("dup")
    assert events[-1][0] == "INGEST_DUPLICATE_IGNORED"
    assert events[-1][1] == E_DUPLICATE_TASK_ID
    store.close()


def test_watcher_marks_failed_local_on_pending_collision(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    (config.queue_dirs.incoming / "collision.aedt").write_text("source")
    (config.queue_dirs.pending / "collision.aedt").write_text("existing")

    watcher = QueueWatcher(config, store)
    processed = watcher.process_once()

    assert processed == 1
    assert store.get_job_state("collision") == JobState.FAILED_LOCAL.value
    failed_matches = list(config.queue_dirs.failed.glob("collision.pending_collision_*.aedt"))
    assert len(failed_matches) == 1

    events = store.get_task_events("collision")
    assert events[-1][0] == "INGEST_PENDING_COLLISION"
    assert events[-1][1] == E_INGEST_PENDING_COLLISION
    store.close()
