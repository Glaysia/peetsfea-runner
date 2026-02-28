from __future__ import annotations

from pathlib import Path

from peetsfea_runner.event_types import AEDT_DELETE_LOCAL_DONE
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


def test_store_initializes_schema_and_inserts(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    inserted = store.insert_job(
        task_id="task-1",
        filename="a.aedt",
        source_path="/tmp/incoming/a.aedt",
        pending_path="/tmp/pending/a.aedt",
        state=JobState.NEW,
    )
    assert inserted is True
    assert store.task_exists("task-1") is True
    assert store.get_job_state("task-1") == JobState.NEW.value

    row = store.connection.execute(
        "SELECT aedt_retention FROM jobs WHERE task_id = ?",
        ["task-1"],
    ).fetchone()
    assert row is not None
    assert row[0] == "delete_after_done"
    store.close()


def test_store_blocks_duplicate_task_id(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    first = store.insert_job(
        task_id="dup",
        filename="dup.aedt",
        source_path="/tmp/incoming/dup.aedt",
        pending_path="/tmp/pending/dup.aedt",
        state=JobState.NEW,
    )
    second = store.insert_job(
        task_id="dup",
        filename="dup2.aedt",
        source_path="/tmp/incoming/dup2.aedt",
        pending_path="/tmp/pending/dup2.aedt",
        state=JobState.NEW,
    )

    assert first is True
    assert second is False
    store.close()


def test_store_updates_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    store.insert_job(
        task_id="task-2",
        filename="b.aedt",
        source_path="/tmp/incoming/b.aedt",
        pending_path="/tmp/pending/b.aedt",
        state=JobState.NEW,
    )
    store.update_state_by_task_id(task_id="task-2", state=JobState.PENDING)

    assert store.get_job_state("task-2") == JobState.PENDING.value
    store.close()


def test_store_records_events_and_done_promotion_hook(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    store.insert_job(
        task_id="task-3",
        filename="c.aedt",
        source_path="/tmp/incoming/c.aedt",
        pending_path="/tmp/pending/c.aedt",
        state=JobState.PENDING,
    )

    assert store.can_promote_done(task_id="task-3") is False

    store.record_event(task_id="task-3", event_type=AEDT_DELETE_LOCAL_DONE)
    store.mark_local_aedt_deleted(task_id="task-3")

    assert store.can_promote_done(task_id="task-3") is True

    events = store.get_task_events("task-3")
    assert events[-1][0] == AEDT_DELETE_LOCAL_DONE

    row = store.connection.execute(
        "SELECT local_aedt_deleted_ts FROM jobs WHERE task_id = ?",
        ["task-3"],
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    store.close()


def test_store_sets_and_reads_report_zip_local_path(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    store.insert_job(
        task_id="task-zip",
        filename="task-zip.aedt",
        source_path="/tmp/incoming/task-zip.aedt",
        pending_path="/tmp/pending/task-zip.aedt",
        state=JobState.DONE,
    )

    zip_path = "/tmp/done/task-zip.reports.zip"
    store.set_report_zip_local_path(task_id="task-zip", report_zip_local_path=zip_path)

    assert store.get_report_zip_local_path("task-zip") == zip_path
    store.close()


def test_store_sets_and_reads_report_zip_remote_path(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    store.insert_job(
        task_id="task-zip-remote",
        filename="task-zip-remote.aedt",
        source_path="/tmp/incoming/task-zip-remote.aedt",
        pending_path="/tmp/pending/task-zip-remote.aedt",
        state=JobState.UPLOADED,
    )

    local_zip = "/tmp/done/task-zip-remote.reports.zip"
    remote_zip = "/remote/spool/results/task-zip-remote.reports.zip"
    store.set_report_zip_paths(
        task_id="task-zip-remote",
        report_zip_local_path=local_zip,
        report_zip_remote_path=remote_zip,
    )

    assert store.get_report_zip_local_path("task-zip-remote") == local_zip
    assert store.get_report_zip_remote_path("task-zip-remote") == remote_zip
    store.close()
