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
            self.assertIn("attempts", names)
            self.assertIn("artifacts", names)
            self.assertIn("events", names)
            self.assertIn("file_lifecycle", names)

    def test_job_lifecycle_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_job(
                run_id="run_01",
                job_id="job_0001",
                input_path="/tmp/in/a.aedt",
                output_path="/tmp/out/a.aedt_all",
                account_id="account_01",
            )
            attempt_id = store.start_attempt(run_id="run_01", job_id="job_0001", attempt_no=1, node="gate1-harry")
            store.finish_attempt(run_id="run_01", attempt_id=attempt_id, exit_code=13, error="mock fail")
            store.update_job_status(
                run_id="run_01",
                job_id="job_0001",
                status="FAILED",
                attempt_no=1,
                failure_reason="mock fail",
            )
            store.mark_delete_retrying(run_id="run_01", job_id="job_0001", retry_count=1)
            store.mark_delete_quarantined(
                run_id="run_01",
                job_id="job_0001",
                retry_count=3,
                quarantine_path="/tmp/delete_failed/a.aedt",
            )
            store.record_artifact(run_id="run_01", job_id="job_0001", artifact_root="/tmp/out/a.aedt_all")
            store.append_event(
                run_id="run_01",
                job_id="job_0001",
                level="ERROR",
                stage="FAILED",
                message="mock fail",
            )
            store.quarantine_job(run_id="run_01", job_id="job_0001", attempt=1, reason="mock fail", exit_code=13)
            store.finish_run("run_01", state="FAILED", summary="done")

            conn = duckdb.connect(str(db_path))
            try:
                run_state = conn.execute("SELECT state FROM runs WHERE run_id='run_01'").fetchone()[0]
                job_state = conn.execute(
                    "SELECT status FROM jobs WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
                lifecycle_state = conn.execute(
                    "SELECT delete_final_state FROM file_lifecycle WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
                attempts_count = conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
                events_count = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE run_id='run_01' AND job_id='job_0001'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(run_state, "FAILED")
            self.assertEqual(job_state, "FAILED")
            self.assertEqual(lifecycle_state, "DELETE_QUARANTINED")
            self.assertEqual(attempts_count, 1)
            self.assertEqual(events_count, 1)


if __name__ == "__main__":
    unittest.main()
