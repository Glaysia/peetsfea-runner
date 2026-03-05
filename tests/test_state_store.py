from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from peetsfea_runner.state_store import StateStore


class TestStateStore(unittest.TestCase):
    def test_initialize_creates_all_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()

            conn = duckdb.connect(str(db_path))
            try:
                names = {
                    row[0]
                    for row in conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                }
            finally:
                conn.close()

            self.assertIn("runs", names)
            self.assertIn("jobs", names)
            self.assertIn("job_events", names)
            self.assertIn("quarantine_jobs", names)

    def test_job_lifecycle_and_quarantine_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_job(run_id="run_01", job_id="job_0001", aedt_path="/tmp/a.aedt")
            store.append_job_event(run_id="run_01", job_id="job_0001", attempt=1, event_type="RUNNING")
            store.update_job(
                run_id="run_01",
                job_id="job_0001",
                attempt=1,
                state="FAILED",
                session_name="aedt_run_01_01_a1",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:01:00+00:00",
                exit_code=13,
                failure_reason="mock failure",
            )
            store.quarantine_job(
                run_id="run_01",
                job_id="job_0001",
                attempt=1,
                reason="mock failure",
                exit_code=13,
            )
            store.finish_run("run_01", state="FAILED", summary="done")

            conn = duckdb.connect(str(db_path))
            try:
                run_state = conn.execute("SELECT state FROM runs WHERE run_id='run_01'").fetchone()[0]
                job_state = conn.execute(
                    "SELECT state FROM jobs WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM job_events WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
                quarantine_count = conn.execute(
                    "SELECT COUNT(*) FROM quarantine_jobs WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(run_state, "FAILED")
            self.assertEqual(job_state, "FAILED")
            self.assertEqual(event_count, 1)
            self.assertEqual(quarantine_count, 1)


if __name__ == "__main__":
    unittest.main()
