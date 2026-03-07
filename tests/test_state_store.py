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
            self.assertIn("worker_heartbeat", names)
            self.assertIn("window_tasks", names)
            self.assertIn("window_events", names)
            self.assertIn("ingest_index", names)
            self.assertIn("account_capacity_snapshots", names)

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

    def test_worker_heartbeat_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.upsert_worker_heartbeat(
                service_name="peetsfea-runner",
                host="host1",
                pid=1234,
                run_id="run_01",
                status="HEALTHY",
            )
            store.upsert_worker_heartbeat(
                service_name="peetsfea-runner",
                host="host1",
                pid=1234,
                run_id="run_02",
                status="DEGRADED",
            )

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute(
                    """
                    SELECT service_name, host, pid, run_id, status
                    FROM worker_heartbeat
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][3], "run_02")
            self.assertEqual(rows[0][4], "DEGRADED")

    def test_register_ingest_candidate_deduplicates_same_stat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()

            first = store.register_ingest_candidate(
                input_path="/in/a.aedt",
                ready_path="/in/a.aedt.ready",
                ready_present=True,
                ready_mode="SIDECAR",
                ready_error=None,
                file_size=100,
                file_mtime_ns=12345,
            )
            second = store.register_ingest_candidate(
                input_path="/in/a.aedt",
                ready_path="/in/a.aedt.ready",
                ready_present=True,
                ready_mode="SIDECAR",
                ready_error=None,
                file_size=100,
                file_mtime_ns=12345,
            )
            changed = store.register_ingest_candidate(
                input_path="/in/a.aedt",
                ready_path="/in/a.aedt.ready",
                ready_present=True,
                ready_mode="SIDECAR",
                ready_error=None,
                file_size=101,
                file_mtime_ns=12346,
            )

            self.assertTrue(first)
            self.assertFalse(second)
            self.assertTrue(changed)

    def test_register_ingest_candidate_updates_ready_metadata_without_requeue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()

            first = store.register_ingest_candidate(
                input_path="/in/a.aedt",
                ready_path="/in/a.aedt.ready",
                ready_present=False,
                ready_mode="INTERNAL_ONLY",
                ready_error="read-only",
                file_size=100,
                file_mtime_ns=12345,
            )
            second = store.register_ingest_candidate(
                input_path="/in/a.aedt",
                ready_path="/in/a.aedt.ready",
                ready_present=True,
                ready_mode="SIDECAR",
                ready_error=None,
                file_size=100,
                file_mtime_ns=12345,
            )

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT ready_present, ready_mode, ready_error, state
                    FROM ingest_index
                    WHERE input_path = '/in/a.aedt'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual((bool(row[0]), row[1], row[2], row[3]), (True, "SIDECAR", None, "READY"))

    def test_ensure_continuous_run_reuses_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            run1 = store.ensure_continuous_run(rotation_hours=24)
            run2 = store.ensure_continuous_run(rotation_hours=24)
            self.assertEqual(run1, run2)

    def test_window_delete_lifecycle_can_mark_pending_and_retained(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_window_task(
                run_id="run_01",
                window_id="w_001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt.out",
                account_id=None,
            )
            store.mark_window_delete_pending(run_id="run_01", window_id="w_001")
            store.mark_window_delete_retained(run_id="run_01", window_id="w_001")

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT delete_final_state
                    FROM file_lifecycle
                    WHERE run_id = 'run_01' AND window_id = 'w_001'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], "RETAINED")

    def test_window_task_score_and_capacity_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_window_task(
                run_id="run_01",
                window_id="w_001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt_all",
                account_id="account_01",
            )
            store.update_window_task(
                run_id="run_01",
                window_id="w_001",
                state="SUCCEEDED",
                attempt_no=1,
                job_id="job_0001",
                account_id="account_01",
            )
            completed, inflight = store.get_window_throughput_score(run_id="run_01", account_id="account_01")
            self.assertEqual(completed, 1)
            self.assertEqual(inflight, 0)

            store.record_account_capacity_snapshot(
                account_id="account_01",
                host="gate1-harry",
                running_count=2,
                pending_count=1,
                allowed_submit=10,
            )

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT running_count, pending_count, allowed_submit
                    FROM account_capacity_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(tuple(row), (2, 1, 10))


if __name__ == "__main__":
    unittest.main()
