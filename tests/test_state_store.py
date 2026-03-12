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
            self.assertIn("slot_tasks", names)
            self.assertIn("slot_events", names)
            self.assertIn("ingest_index", names)
            self.assertIn("account_capacity_snapshots", names)
            self.assertIn("slurm_workers", names)
            self.assertIn("node_resource_snapshots", names)
            self.assertIn("worker_resource_snapshots", names)
            self.assertIn("slot_resource_snapshots", names)
            self.assertIn("resource_summary_snapshots", names)

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

    def test_slot_delete_lifecycle_can_mark_pending_and_retained(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt.out",
                account_id=None,
            )
            store.mark_slot_delete_pending(run_id="run_01", slot_id="w_001")
            store.mark_slot_delete_retained(run_id="run_01", slot_id="w_001")

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT delete_final_state
                    FROM file_lifecycle
                    WHERE run_id = 'run_01' AND slot_id = 'w_001'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], "RETAINED")

    def test_slot_task_score_and_capacity_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt_all",
                account_id="account_01",
            )
            store.update_slot_task(
                run_id="run_01",
                slot_id="w_001",
                state="SUCCEEDED",
                attempt_no=1,
                job_id="job_0001",
                account_id="account_01",
            )
            completed, inflight = store.get_slot_throughput_score(run_id="run_01", account_id="account_01")
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

    def test_count_active_jobs_by_account_counts_pending_submitted_and_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_job(
                run_id="run_01",
                job_id="job_0001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt.out",
                account_id="account_01",
            )
            store.update_job_status(run_id="run_01", job_id="job_0001", status="SUBMITTED", attempt_no=1)
            store.create_job(
                run_id="run_01",
                job_id="job_0002",
                input_path="/in/b.aedt",
                output_path="/out/b.aedt.out",
                account_id="account_01",
            )
            store.update_job_status(run_id="run_01", job_id="job_0002", status="RUNNING", attempt_no=1)
            store.create_job(
                run_id="run_01",
                job_id="job_0003",
                input_path="/in/c.aedt",
                output_path="/out/c.aedt.out",
                account_id="account_02",
            )
            store.update_job_status(run_id="run_01", job_id="job_0003", status="SUCCEEDED", attempt_no=1)

            counts = store.count_active_jobs_by_account(run_id="run_01")
            self.assertEqual(counts, {"account_01": 2})

    def test_slurm_worker_upsert_tracks_lifecycle_and_active_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")

            store.upsert_slurm_worker(
                run_id="run_01",
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="SUBMITTED",
                observed_node=None,
                slots_configured=4,
                backend="slurm_batch",
            )
            store.upsert_slurm_worker(
                run_id="run_01",
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="RUNNING",
                observed_node="n115",
                slots_configured=4,
                backend="slurm_batch",
            )
            store.upsert_slurm_worker(
                run_id="run_01",
                worker_id="attempt_0002",
                job_id="job_0002",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552741",
                worker_state="COMPLETED",
                observed_node="n108",
                slots_configured=4,
                backend="slurm_batch",
            )

            active_workers = store.list_active_slurm_workers(run_id="run_01")
            all_workers = store.list_slurm_workers(run_id="run_01")

            self.assertEqual(len(active_workers), 1)
            self.assertEqual(active_workers[0]["worker_id"], "attempt_0001")
            self.assertEqual(active_workers[0]["worker_state"], "RUNNING")
            self.assertEqual(active_workers[0]["observed_node"], "n115")
            self.assertEqual(active_workers[0]["tunnel_state"], "PENDING")
            self.assertEqual(len(all_workers), 2)
            self.assertEqual({row["worker_state"] for row in all_workers}, {"RUNNING", "COMPLETED"})

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT submitted_at, started_at, ended_at
                    FROM slurm_workers
                    WHERE run_id = 'run_01' AND worker_id = 'attempt_0001'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(row[0])
            self.assertIsNotNone(row[1])
            self.assertIsNone(row[2])

    def test_update_slurm_worker_control_plane_tracks_tunnel_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.upsert_slurm_worker(
                run_id="run_01",
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="RUNNING",
                observed_node="n115",
                slots_configured=4,
                backend="slurm_batch",
            )

            store.update_slurm_worker_control_plane(
                run_id="run_01",
                worker_id="attempt_0001",
                tunnel_state="CONNECTED",
                tunnel_session_id="session-1",
                observed_node="n115",
            )
            store.update_slurm_worker_control_plane(
                run_id="run_01",
                worker_id="attempt_0001",
                tunnel_state="DEGRADED",
                degraded_reason="tunnel heartbeat stale after main restart",
            )

            row = store.get_slurm_worker(run_id="run_01", worker_id="attempt_0001")
            assert row is not None
            self.assertEqual(row["tunnel_session_id"], "session-1")
            self.assertEqual(row["tunnel_state"], "DEGRADED")
            self.assertEqual(row["degraded_reason"], "tunnel heartbeat stale after main restart")
            self.assertIsNotNone(row["heartbeat_ts"])

    def test_resource_snapshot_tables_record_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.record_node_resource_snapshot(
                run_id="run_01",
                host="mainpc",
                allocated_mem_mb=960 * 1024,
                total_mem_mb=1024,
                used_mem_mb=256,
                free_mem_mb=768,
                load_1=0.5,
                load_5=0.4,
                load_15=0.3,
                tmp_total_mb=100,
                tmp_used_mb=10,
                tmp_free_mb=90,
                process_count=123,
                running_worker_count=2,
                active_slot_count=5,
            )
            store.record_worker_resource_snapshot(
                run_id="run_01",
                worker_id="attempt_0001",
                host="n108",
                slurm_job_id="552740",
                configured_slots=4,
                active_slots=3,
                idle_slots=1,
                rss_mb=512,
                cpu_pct=88.5,
                tunnel_state="CONNECTED",
                process_count=5,
            )
            store.record_slot_resource_snapshot(
                run_id="run_01",
                slot_id="w_001",
                worker_id="attempt_0001",
                host="n108",
                allocated_mem_mb=240 * 1024,
                used_mem_mb=128,
                load_1=1.5,
                rss_mb=128,
                cpu_pct=25.0,
                process_count=3,
                active_process_count=2,
                artifact_bytes=4096,
                progress_ts="2026-03-11T12:00:00+00:00",
                state="RUNNING",
            )
            store.record_resource_summary_snapshot(
                run_id="run_01",
                host="mainpc",
                allocated_mem_mb=960 * 1024,
                used_mem_mb=256,
                free_mem_mb=768,
                load_1=0.5,
                running_worker_count=2,
                active_slot_count=5,
                stale=False,
            )

            conn = duckdb.connect(str(db_path))
            try:
                node_row = conn.execute(
                    "SELECT allocated_mem_mb, total_mem_mb, used_mem_mb, process_count, running_worker_count, active_slot_count FROM node_resource_snapshots"
                ).fetchone()
                worker_row = conn.execute(
                    "SELECT configured_slots, active_slots, rss_mb, tunnel_state FROM worker_resource_snapshots"
                ).fetchone()
                slot_row = conn.execute(
                    "SELECT allocated_mem_mb, used_mem_mb, load_1, rss_mb, process_count, active_process_count, artifact_bytes, state FROM slot_resource_snapshots"
                ).fetchone()
                summary_row = conn.execute(
                    "SELECT allocated_mem_mb, used_mem_mb, active_slot_count, stale FROM resource_summary_snapshots"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(node_row, (960 * 1024, 1024, 256, 123, 2, 5))
            self.assertEqual(worker_row, (4, 3, 512, "CONNECTED"))
            self.assertEqual(slot_row, (240 * 1024, 128, 1.5, 128, 3, 2, 4096, "RUNNING"))
            self.assertEqual(summary_row, (960 * 1024, 256, 5, False))


if __name__ == "__main__":
    unittest.main()
