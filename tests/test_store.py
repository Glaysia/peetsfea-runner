from __future__ import annotations

from pathlib import Path

from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


def test_store_initializes_schema_and_inserts(tmp_path: Path) -> None:
    db_path = tmp_path / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    inserted = store.insert_job(
        job_id="job-1",
        filename="a.aedt",
        source_path="/tmp/a.aedt",
        staging_path="/tmp/staging/a.aedt",
        state=JobState.QUEUED,
    )
    assert inserted is True
    assert store.filename_exists("a.aedt") is True

    store.close()


def test_store_blocks_duplicate_filename(tmp_path: Path) -> None:
    db_path = tmp_path / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    first = store.insert_job(
        job_id="job-1",
        filename="dup.aedt",
        source_path="/tmp/dup.aedt",
        staging_path="/tmp/staging/dup.aedt",
        state=JobState.QUEUED,
    )
    second = store.insert_job(
        job_id="job-2",
        filename="dup.aedt",
        source_path="/tmp/dup2.aedt",
        staging_path="/tmp/staging/dup2.aedt",
        state=JobState.QUEUED,
    )

    assert first is True
    assert second is False
    store.close()


def test_store_updates_state(tmp_path: Path) -> None:
    db_path = tmp_path / "runner.duckdb"
    store = JobStore(db_path)
    store.initialize_schema()

    store.insert_job(
        job_id="job-1",
        filename="b.aedt",
        source_path="/tmp/b.aedt",
        staging_path="/tmp/staging/b.aedt",
        state=JobState.QUEUED,
    )
    store.update_state_by_job_id(job_id="job-1", state=JobState.STAGED)

    assert store.get_job_state("b.aedt") == JobState.STAGED.value
    store.close()
