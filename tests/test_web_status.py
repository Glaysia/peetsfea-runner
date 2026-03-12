from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

import duckdb

from peetsfea_runner.state_store import StateStore
from peetsfea_runner import web_status
from peetsfea_runner import __version__
from peetsfea_runner.web_status import start_status_server


class TestWebStatus(unittest.TestCase):
    def test_query_uses_process_shared_duckdb_connection_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"

            class _FakeCursor:
                def fetchall(self) -> list[tuple]:
                    return [("ok",)]

            class _FakeConnection:
                def __init__(self) -> None:
                    self.closed = False

                def execute(self, sql: str, params: list[object]) -> _FakeCursor:
                    self.sql = sql
                    self.params = params
                    return _FakeCursor()

                def close(self) -> None:
                    self.closed = True

            fake_conn = _FakeConnection()
            with patch.object(web_status.duckdb, "connect", return_value=fake_conn) as connect_mock:
                rows = web_status._query(db_path, "SELECT 1", [123])

            self.assertEqual(rows, [("ok",)])
            connect_mock.assert_called_once_with(str(db_path))
            self.assertTrue(fake_conn.closed)

    def test_status_api_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_00")
            store.create_job(
                run_id="run_00",
                job_id="job_old_0001",
                input_path="/in/old.aedt",
                output_path="/out/old.aedt_all",
                account_id="account_01",
            )
            store.update_job_status(run_id="run_00", job_id="job_old_0001", status="FAILED", attempt_no=1)
            store.finish_run("run_00", state="FAILED", summary="old")
            store.start_run("run_01")
            store.create_job(
                run_id="run_01",
                job_id="job_0001",
                input_path="/in/a.aedt",
                output_path="/out/a.aedt_all",
                account_id="account_01",
            )
            store.update_job_status(run_id="run_01", job_id="job_0001", status="SUCCEEDED", attempt_no=1)
            store.start_attempt(run_id="run_01", job_id="job_0001", attempt_no=1, node="gate1-harry")
            store.append_event(
                run_id="run_01",
                job_id="job_0001",
                level="INFO",
                stage="SUCCEEDED",
                message="done",
            )
            store.mark_input_deleted(run_id="run_01", job_id="job_0001", retry_count=0)
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
            store.append_slot_event(
                run_id="run_01",
                slot_id="w_001",
                level="INFO",
                stage="SUCCEEDED",
                message="slot done",
            )
            store.mark_slot_input_deleted(run_id="run_01", slot_id="w_001", retry_count=0)
            store.record_account_capacity_snapshot(
                account_id="account_01",
                host="gate1-harry",
                running_count=1,
                pending_count=0,
                allowed_submit=12,
            )
            store.record_account_readiness_snapshot(
                account_id="account_01",
                host="gate1-harry",
                ready=True,
                status="READY",
                reason="ok",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
            )
            store.record_account_readiness_snapshot(
                account_id="account_02",
                host="gate1-dhj02",
                ready=False,
                status="DISABLED_FOR_DISPATCH",
                reason="venv,python",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=False,
                python_ok=False,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
            )
            store.record_account_readiness_snapshot(
                account_id="account_03",
                host="gate1-jji0930",
                ready=False,
                status="BLOCKED_STORAGE",
                reason="inode_pct=100",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
                storage_ready=False,
                storage_reason="inode_pct=100",
                inode_use_percent=100,
                free_mb=512,
            )
            store.upsert_worker_heartbeat(
                service_name="peetsfea-runner",
                host="host1",
                pid=1111,
                run_id="run_01",
                status="HEALTHY",
            )
            store.finish_run("run_01", state="SUCCEEDED", summary="ok")

            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/") as resp:
                    html = resp.read().decode("utf-8")
                self.assertIn("Peets FEA Status Dashboard", html)
                self.assertIn("Slot(.aedt)", html)
                self.assertIn(__version__, html)

                with urlopen(f"http://{host}:{port}/api") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["version"], __version__)
                self.assertIn("/api/worker/health", payload["endpoints"])
                self.assertIn("/api/workers", payload["endpoints"])
                self.assertIn("/api/operations/rollout", payload["endpoints"])
                self.assertIn("/api/resources/summary", payload["endpoints"])
                self.assertIn("/api/jobs/{id}/timeline", payload["endpoints"])
                self.assertIn("/api/slots", payload["endpoints"])
                self.assertIn("/api/slots/{id}/timeline", payload["endpoints"])
                self.assertIn("/api/accounts/capacity", payload["endpoints"])

                with urlopen(f"http://{host}:{port}/api/jobs") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["jobs"]), 2)

                with urlopen(f"http://{host}:{port}/api/jobs/job_0001") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["job"]["job_id"], "job_0001")

                with urlopen(f"http://{host}:{port}/api/jobs/job_0001/timeline") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["job"]["job_id"], "job_0001")
                self.assertEqual(len(payload["attempts"]), 1)
                self.assertEqual(payload["file_lifecycle"]["delete_final_state"], "DELETED")

                with urlopen(f"http://{host}:{port}/api/metrics/throughput") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["version"], __version__)
                self.assertEqual(payload["metrics"]["total_jobs"], 1)
                self.assertEqual(payload["metrics"]["succeeded_jobs"], 1)
                self.assertEqual(payload["metrics"]["active_jobs"], 0)
                self.assertEqual(payload["metrics"]["queue_jobs"], 0)
                self.assertEqual(payload["metrics"]["total_slots"], 1)
                self.assertEqual(payload["metrics"]["succeeded_slots"], 1)
                self.assertEqual(payload["metrics"]["quarantined_slots"], 0)
                self.assertEqual(payload["metrics"]["delete_quarantined_slots"], 0)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["configured_target_slots"], 40)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["expansion_target_slots"], 200)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["effective_target_slots"], 0)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["active_slot_shortfall"], 0)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["throughput_mode"], "IDLE")
                self.assertEqual(payload["metrics"]["throughput_kpi"]["active_workers"], 0)
                self.assertEqual(payload["metrics"]["throughput_kpi"]["pending_workers"], 0)
                self.assertEqual(len(payload["metrics"]["account_slot_scores"]), 1)
                self.assertIsNone(payload["metrics"]["resource_summary"])

                with urlopen(f"http://{host}:{port}/api/runs/latest") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["run"]["run_id"], "run_01")
                self.assertIsNotNone(payload["run"]["stale_seconds"])
                self.assertEqual(payload["run"]["is_stale"], False)
                self.assertIsNotNone(payload["run"]["last_event_ts"])

                with urlopen(f"http://{host}:{port}/api/runs/run_01/summary") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["run"]["run_id"], "run_01")
                self.assertTrue(any(x["status"] == "SUCCEEDED" for x in payload["run"]["status_counts"]))
                self.assertTrue(any(x["state"] == "SUCCEEDED" for x in payload["run"]["slot_state_counts"]))

                with urlopen(f"http://{host}:{port}/api/runs/run_01/jobs") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["jobs"]), 1)

                with urlopen(f"http://{host}:{port}/api/slots") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["run_id"], "run_01")
                self.assertEqual(len(payload["slots"]), 1)
                self.assertEqual(payload["slots"][0]["slot_id"], "w_001")

                with urlopen(f"http://{host}:{port}/api/slots/w_001/timeline") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["slot_task"]["slot_id"], "w_001")
                self.assertEqual(payload["attempt_summary"]["slot_attempt_no"], 1)
                self.assertEqual(payload["file_lifecycle"]["delete_final_state"], "DELETED")

                with urlopen(f"http://{host}:{port}/api/accounts/capacity") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["accounts"]), 3)
                self.assertEqual(payload["accounts"][0]["account_id"], "account_01")
                self.assertEqual(payload["accounts"][0]["score"], 1)
                self.assertEqual(payload["accounts"][0]["readiness_status"], "READY")
                self.assertTrue(payload["accounts"][0]["readiness_ready"])
                self.assertTrue(payload["accounts"][0]["storage_ready"])
                self.assertEqual(payload["accounts"][0]["storage_reason"], "ok")
                self.assertEqual(payload["accounts"][0]["configured_worker_jobs"], 10)
                self.assertEqual(payload["accounts"][0]["active_slots"], 0)
                self.assertEqual(payload["accounts"][0]["completed_slots"], 1)
                self.assertEqual(payload["accounts"][0]["quarantined_slots"], 0)
                self.assertEqual(payload["accounts"][0]["live_status"], "UNDER_CAPACITY")
                self.assertEqual(payload["accounts"][1]["account_id"], "account_02")
                self.assertEqual(payload["accounts"][1]["readiness_status"], "DISABLED_FOR_DISPATCH")
                self.assertEqual(payload["accounts"][1]["readiness_reason"], "venv,python")
                self.assertFalse(payload["accounts"][1]["readiness_ready"])
                self.assertEqual(payload["accounts"][1]["running_count"], 0)
                self.assertEqual(payload["accounts"][1]["live_status"], "BLOCKED")
                self.assertEqual(payload["accounts"][2]["account_id"], "account_03")
                self.assertEqual(payload["accounts"][2]["readiness_status"], "BLOCKED_STORAGE")
                self.assertEqual(payload["accounts"][2]["readiness_reason"], "inode_pct=100")
                self.assertFalse(payload["accounts"][2]["readiness_ready"])
                self.assertFalse(payload["accounts"][2]["storage_ready"])
                self.assertEqual(payload["accounts"][2]["storage_reason"], "inode_pct=100")
                self.assertEqual(payload["accounts"][2]["inode_use_percent"], 100)
                self.assertEqual(payload["accounts"][2]["free_mb"], 512)
                self.assertEqual(payload["accounts"][2]["live_status"], "BLOCKED")

                with urlopen(f"http://{host}:{port}/api/events/recent") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertGreaterEqual(len(payload["events"]), 2)
                self.assertIn("source", payload["events"][0])
                self.assertIn("category", payload["events"][0])
                self.assertIn("alertable", payload["events"][0])
                self.assertIn("common_key", payload["events"][0])
                self.assertIn("alerts", payload)

                with urlopen(f"http://{host}:{port}/api/file-lifecycle/summary") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertIn("file_lifecycle", payload)

                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["reason"], "ok")
                self.assertEqual(payload["worker_status"], "HEALTHY")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_worker_health_stale_when_no_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "STALE")
                self.assertEqual(payload["reason"], "no heartbeat")
                self.assertIsNone(payload["worker_status"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_worker_health_exposes_idle_and_active_worker_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                store.upsert_worker_heartbeat(
                    service_name="peetsfea-runner",
                    host="host1",
                    pid=1111,
                    run_id="run_idle",
                    status="IDLE",
                )
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["worker_status"], "IDLE")

                store.upsert_worker_heartbeat(
                    service_name="peetsfea-runner",
                    host="host1",
                    pid=1111,
                    run_id="run_active",
                    status="ACTIVE",
                )
                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["worker_status"], "ACTIVE")

                store.upsert_worker_heartbeat(
                    service_name="peetsfea-runner",
                    host="host1",
                    pid=1111,
                    run_id="run_recover",
                    status="RECOVERING",
                )
                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["worker_status"], "RECOVERING")
                self.assertEqual(payload["reason"], "autorecovery active")

                store.upsert_worker_heartbeat(
                    service_name="peetsfea-runner",
                    host="host1",
                    pid=1111,
                    run_id="run_blocked",
                    status="BLOCKED",
                )
                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["worker_status"], "BLOCKED")
                self.assertEqual(payload["reason"], "autorecovery blocked by readiness")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_internal_worker_control_plane_endpoints_update_tunnel_state(self) -> None:
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
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                register_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "tunnel_session_id": "session-1",
                        "slurm_job_id": "552740",
                        "observed_node": "n115",
                    }
                ).encode("utf-8")
                with urlopen(
                    f"http://{host}:{port}/internal/workers/register",
                    data=register_req,
                ) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                heartbeat_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "tunnel_session_id": "session-1",
                        "observed_node": "n115",
                    }
                ).encode("utf-8")
                with urlopen(
                    f"http://{host}:{port}/internal/workers/heartbeat",
                    data=heartbeat_req,
                ) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                degraded_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "tunnel_session_id": "session-1",
                        "stage": "RETURN_PATH_DNS_FAILURE",
                        "reason": "tunnel heartbeat stale after main restart",
                    }
                ).encode("utf-8")
                with urlopen(
                    f"http://{host}:{port}/internal/workers/degraded",
                    data=degraded_req,
                ) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                recovered_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "tunnel_session_id": "session-1",
                        "observed_node": "n115",
                    }
                ).encode("utf-8")
                with urlopen(
                    f"http://{host}:{port}/internal/workers/recovered",
                    data=recovered_req,
                ) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                with urlopen(f"http://{host}:{port}/api/workers?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            self.assertEqual(len(payload["workers"]), 1)
            worker = payload["workers"][0]
            self.assertEqual(worker["worker_id"], "attempt_0001")
            self.assertEqual(worker["tunnel_session_id"], "session-1")
            self.assertEqual(worker["tunnel_state"], "CONNECTED")
            self.assertIsNotNone(worker["heartbeat_ts"])

            conn = duckdb.connect(str(db_path))
            try:
                event_rows = conn.execute(
                    """
                    SELECT stage
                    FROM events
                    WHERE run_id = 'run_01'
                    ORDER BY ts
                    """
                ).fetchall()
            finally:
                conn.close()
            stages = [str(row[0]) for row in event_rows]
            self.assertIn("CONTROL_TUNNEL_READY", stages)
            self.assertIn("RETURN_PATH_DNS_FAILURE", stages)
            self.assertIn("CONTROL_TUNNEL_RECOVERED", stages)

    def test_api_workers_reports_degraded_and_stale_tunnel_health(self) -> None:
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
                tunnel_session_id="session-1",
                tunnel_state="DEGRADED",
                heartbeat_ts="2000-01-01T00:00:00+00:00",
                degraded_reason="tunnel heartbeat stale after main restart",
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/workers?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            self.assertEqual(payload["run_id"], "run_01")
            self.assertEqual(len(payload["workers"]), 1)
            worker = payload["workers"][0]
            self.assertEqual(worker["worker_id"], "attempt_0001")
            self.assertEqual(worker["tunnel_state"], "DEGRADED")
            self.assertEqual(worker["degraded_reason"], "tunnel heartbeat stale after main restart")
            self.assertTrue(worker["is_tunnel_stale"])
            self.assertIsNotNone(worker["heartbeat_age_seconds"])

    def test_api_workers_does_not_mark_pending_worker_without_heartbeat_as_stale(self) -> None:
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
                worker_state="PENDING",
                observed_node=None,
                slots_configured=4,
                backend="slurm_batch",
                tunnel_state="PENDING",
                heartbeat_ts=None,
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/workers?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            worker = payload["workers"][0]
            self.assertEqual(worker["worker_state"], "PENDING")
            self.assertEqual(worker["tunnel_state"], "PENDING")
            self.assertFalse(worker["is_tunnel_stale"])

    def test_resource_summary_endpoint_and_internal_snapshot_posts(self) -> None:
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
                observed_node="n108",
                slots_configured=4,
                backend="slurm_batch",
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                node_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "host": "mainpc",
                        "allocated_mem_mb": 960 * 1024,
                        "total_mem_mb": 1024,
                        "used_mem_mb": 400,
                        "free_mem_mb": 624,
                        "load_1": 1.2,
                        "load_5": 0.9,
                        "load_15": 0.7,
                        "tmp_total_mb": 100,
                        "tmp_used_mb": 20,
                        "tmp_free_mb": 80,
                        "process_count": 333,
                        "running_worker_count": 2,
                        "active_slot_count": 5,
                    }
                ).encode("utf-8")
                with urlopen(f"http://{host}:{port}/internal/resources/node", data=node_req) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                worker_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "host": "n108",
                        "slurm_job_id": "552740",
                        "configured_slots": 4,
                        "active_slots": 3,
                        "idle_slots": 1,
                        "rss_mb": 512,
                        "cpu_pct": 88.5,
                        "tunnel_state": "CONNECTED",
                        "process_count": 6,
                    }
                ).encode("utf-8")
                with urlopen(f"http://{host}:{port}/internal/resources/worker", data=worker_req) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                slot_req = json.dumps(
                    {
                        "run_id": "run_01",
                        "worker_id": "attempt_0001",
                        "slot_id": "case_01",
                        "host": "n108",
                        "allocated_mem_mb": 240 * 1024,
                        "used_mem_mb": 128,
                        "load_1": 1.2,
                        "rss_mb": 128,
                        "cpu_pct": 25.0,
                        "process_count": 3,
                        "active_process_count": 2,
                        "artifact_bytes": 4096,
                        "progress_ts": "2026-03-11T12:00:00Z",
                        "state": "RUNNING",
                    }
                ).encode("utf-8")
                with urlopen(f"http://{host}:{port}/internal/resources/slot", data=slot_req) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(payload["ok"])

                with urlopen(f"http://{host}:{port}/api/resources/summary?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                summary = payload["summary"]
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            self.assertEqual(summary["allocated_mem_mb"], 960 * 1024)
            self.assertEqual(summary["used_mem_mb"], 400)
            self.assertEqual(summary["free_mem_mb"], 624)
            self.assertEqual(summary["load_1"], 1.2)
            self.assertEqual(summary["running_worker_count"], 2)
            self.assertEqual(summary["active_slot_count"], 5)

            conn = duckdb.connect(str(db_path))
            try:
                node_row = conn.execute(
                    """
                    SELECT allocated_mem_mb, total_mem_mb, used_mem_mb, load_1, process_count
                    FROM node_resource_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchone()
                slot_row = conn.execute(
                    """
                    SELECT allocated_mem_mb, used_mem_mb, load_1, process_count, active_process_count
                    FROM slot_resource_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(node_row, (960 * 1024, 1024, 400, 1.2, 333))
            self.assertEqual(slot_row, (240 * 1024, 128, 1.2, 3, 2))

    def test_overview_endpoint_exposes_summary_alerts_and_operator_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_job(
                run_id="run_01",
                job_id="job_0001",
                input_path="/in/sample.aedt",
                output_path="/out/sample.aedt_all",
                account_id="account_01",
            )
            store.update_job_status(run_id="run_01", job_id="job_0001", status="RUNNING", attempt_no=1)
            store.create_slot_task(
                run_id="run_01",
                slot_id="slot_001",
                input_path="/in/sample.aedt",
                output_path="/out/sample.aedt_all",
                account_id="account_01",
            )
            store.update_slot_task(
                run_id="run_01",
                slot_id="slot_001",
                state="RUNNING",
                attempt_no=1,
                job_id="job_0001",
                account_id="account_01",
            )
            store.record_account_capacity_snapshot(
                account_id="account_01",
                host="gate1-harry",
                running_count=1,
                pending_count=0,
                allowed_submit=0,
            )
            store.record_account_readiness_snapshot(
                account_id="account_01",
                host="gate1-harry",
                ready=True,
                status="READY",
                reason="ok",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
            )
            store.upsert_worker_heartbeat(
                service_name="peetsfea-runner",
                host="mainpc",
                pid=1234,
                run_id="run_01",
                status="ACTIVE",
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
                observed_node="n108",
                slots_configured=4,
                backend="slurm_batch",
                tunnel_session_id="session-1",
                tunnel_state="CONNECTED",
                heartbeat_ts=datetime.now(timezone.utc).isoformat(),
            )
            store.record_node_resource_snapshot(
                run_id="run_01",
                host="mainpc",
                allocated_mem_mb=960 * 1024,
                total_mem_mb=1024,
                used_mem_mb=400,
                free_mem_mb=624,
                load_1=1.2,
                load_5=0.9,
                load_15=0.7,
                tmp_total_mb=100,
                tmp_used_mb=20,
                tmp_free_mb=80,
                process_count=333,
                running_worker_count=1,
                active_slot_count=1,
            )
            store.record_worker_resource_snapshot(
                run_id="run_01",
                worker_id="attempt_0001",
                host="n108",
                slurm_job_id="552740",
                configured_slots=4,
                active_slots=1,
                idle_slots=3,
                rss_mb=128,
                cpu_pct=25.0,
                tunnel_state="CONNECTED",
                process_count=6,
            )
            store.record_resource_summary_snapshot(
                run_id="run_01",
                host="mainpc",
                allocated_mem_mb=960 * 1024,
                used_mem_mb=400,
                free_mem_mb=624,
                load_1=1.2,
                running_worker_count=1,
                active_slot_count=1,
                stale=False,
            )
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="WARN",
                stage="CUTOVER_BLOCKED",
                message="run_id=run_01 queued_slots=4 blocked_accounts=account_01",
            )

            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/overview?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            self.assertEqual(payload["run_id"], "run_01")
            self.assertEqual(payload["health"]["worker_status"], "ACTIVE")
            self.assertEqual(payload["throughput_kpi"]["active_slots"], 1)
            self.assertEqual(payload["resource_summary"]["allocated_mem_mb"], 960 * 1024)
            self.assertEqual(payload["node_summary"]["process_count"], 333)
            self.assertEqual(payload["worker_mix"]["busy_workers"], 1)
            self.assertEqual(payload["worker_mix"]["idle_workers"], 0)
            self.assertEqual(payload["worker_mix"]["idle_slots"], 3)
            self.assertEqual(payload["tunnel_summary"]["active_workers"], 1)
            self.assertEqual(payload["tunnel_summary"]["connected_workers"], 1)
            self.assertEqual(payload["tunnel_summary"]["live_return_path_workers"], 1)
            self.assertTrue(any(alert["alert_key"] == "CUTOVER_BLOCKED" for alert in payload["alerts"]))
            self.assertEqual(payload["accounts"][0]["account_id"], "account_01")

    def test_dashboard_html_uses_overview_first_polling_and_detail_sections(self) -> None:
        html = web_status._dashboard_html(version=__version__)
        self.assertIn("/api/overview", html)
        self.assertIn("overview는 5초, detail은 펼쳤을 때만 30초", html)
        self.assertIn("Process Count", html)
        self.assertIn("Worker Mix", html)
        self.assertIn("Slot Mix", html)
        self.assertIn("Return Path Live", html)
        self.assertIn("rollout-state", html)
        self.assertIn("rollout-boundary", html)
        self.assertIn("detail-panel", html)
        self.assertIn("setInterval(refreshOverview, 5000)", html)
        self.assertIn("setInterval(refreshDetails, 30000)", html)
        self.assertNotIn("setInterval(refresh, 5000)", html)

    def test_rollout_endpoint_exposes_full_rollout_and_service_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="INFO",
                stage="SERVICE_BOUNDARY",
                message=(
                    "input_source_policy=sample_only public_storage_mode=disabled "
                    "runner_scope=control_plane_orchestration storage_scope=separate_service_boundary"
                ),
            )
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="INFO",
                stage="FULL_ROLLOUT_READY",
                message="run_id=run_01 next_stage=full_backlog",
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/operations/rollout?run_id=run_01") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            self.assertEqual(payload["run_id"], "run_01")
            self.assertEqual(payload["state"], "FULL_ROLLOUT_READY")
            self.assertTrue(payload["full_rollout_ready"])
            self.assertFalse(payload["fallback_active"])
            self.assertEqual(payload["service_boundary"]["input_source_policy"], "sample_only")
            self.assertEqual(payload["service_boundary"]["public_storage_mode"], "disabled")

    def test_metrics_throughput_exposes_capacity_shortfall_against_configured_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            for index in range(1, 12):
                state = "RUNNING" if index == 1 else "QUEUED"
                store.create_slot_task(
                    run_id="run_01",
                    slot_id=f"w_{index:03d}",
                    input_path=f"/in/{index:03d}.aedt",
                    output_path=f"/out/{index:03d}.aedt.out",
                    account_id="account_01",
                )
                store.update_slot_task(
                    run_id="run_01",
                    slot_id=f"w_{index:03d}",
                    state=state,
                    attempt_no=1 if state == "RUNNING" else 0,
                    job_id="job_0001" if state == "RUNNING" else None,
                    account_id="account_01",
                )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with patch.dict(
                    os.environ,
                    {
                        "PEETSFEA_ACCOUNTS": "account_01@gate1-harry:1",
                        "PEETSFEA_SLOTS_PER_JOB": "8",
                    },
                    clear=False,
                ):
                    with urlopen(f"http://{host}:{port}/api/metrics/throughput") as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                throughput_kpi = payload["metrics"]["throughput_kpi"]
                self.assertEqual(throughput_kpi["configured_accounts"], 1)
                self.assertEqual(throughput_kpi["configured_worker_jobs"], 1)
                self.assertEqual(throughput_kpi["configured_target_slots"], 8)
                self.assertEqual(throughput_kpi["effective_target_slots"], 8)
                self.assertEqual(throughput_kpi["active_slot_shortfall"], 7)
                self.assertEqual(throughput_kpi["active_workers"], 0)
                self.assertEqual(throughput_kpi["pending_workers"], 0)
                self.assertEqual(throughput_kpi["worker_shortfall"], 1)
                self.assertEqual(throughput_kpi["scheduled_worker_shortfall"], 1)
                self.assertEqual(throughput_kpi["recovery_backlog_slots"], 10)
                self.assertEqual(throughput_kpi["throughput_mode"], "CAPACITY_SHORTFALL")
                self.assertTrue(throughput_kpi["capacity_limited"])
                self.assertFalse(throughput_kpi["input_limited"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_account_capacity_exposes_live_worker_and_slot_breakdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_run",
                input_path="/in/run.aedt",
                output_path="/out/run.aedt.out",
                account_id="account_01",
            )
            store.update_slot_task(
                run_id="run_01",
                slot_id="w_run",
                state="RUNNING",
                attempt_no=1,
                job_id="job_0001",
                account_id="account_01",
            )
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_done",
                input_path="/in/done.aedt",
                output_path="/out/done.aedt.out",
                account_id="account_01",
            )
            store.update_slot_task(
                run_id="run_01",
                slot_id="w_done",
                state="SUCCEEDED",
                attempt_no=1,
                job_id="job_0001",
                account_id="account_01",
            )
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_quar",
                input_path="/in/quar.aedt",
                output_path="/out/quar.aedt.out",
                account_id="account_01",
            )
            store.update_slot_task(
                run_id="run_01",
                slot_id="w_quar",
                state="QUARANTINED",
                attempt_no=1,
                job_id="job_0002",
                account_id="account_01",
            )
            store.record_account_capacity_snapshot(
                account_id="account_01",
                host="gate1-harry",
                running_count=1,
                pending_count=1,
                allowed_submit=1,
            )
            store.record_account_readiness_snapshot(
                account_id="account_01",
                host="gate1-harry",
                ready=True,
                status="READY",
                reason="ok",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with patch.dict(
                    os.environ,
                    {
                        "PEETSFEA_ACCOUNTS": "account_01@gate1-harry:2",
                        "PEETSFEA_SLOTS_PER_JOB": "8",
                    },
                    clear=False,
                ):
                    with urlopen(f"http://{host}:{port}/api/accounts/capacity") as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                account = payload["accounts"][0]
                self.assertEqual(account["configured_worker_jobs"], 2)
                self.assertEqual(account["configured_target_slots"], 16)
                self.assertEqual(account["running_count"], 1)
                self.assertEqual(account["pending_count"], 1)
                self.assertEqual(account["active_slots"], 1)
                self.assertEqual(account["completed_slots"], 2)
                self.assertEqual(account["quarantined_slots"], 1)
                self.assertEqual(account["active_worker_shortfall"], 1)
                self.assertEqual(account["scheduled_worker_shortfall"], 0)
                self.assertEqual(account["active_slot_shortfall"], 15)
                self.assertEqual(account["live_status"], "FILLING")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_recent_events_exposes_alertable_ops_summary_and_run_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_job(
                run_id="run_01",
                job_id="job_block",
                input_path="/in/block.aedt",
                output_path="/out/block.aedt.out",
                account_id="account_01",
            )
            store.append_event(
                run_id="run_01",
                job_id="job_block",
                level="WARN",
                stage="WORKER_LOOP_BLOCKED",
                message="account=account_01 readiness blocked by preflight",
            )
            for index in range(1, 11):
                slot_id = f"w_q_{index:03d}"
                store.create_slot_task(
                    run_id="run_01",
                    slot_id=slot_id,
                    input_path=f"/in/{slot_id}.aedt",
                    output_path=f"/out/{slot_id}.aedt.out",
                    account_id="account_01",
                )
                if index <= 2:
                    store.update_slot_task(
                        run_id="run_01",
                        slot_id=slot_id,
                        state="QUARANTINED",
                        attempt_no=1,
                        job_id=f"job_quar_{index:02d}",
                        account_id="account_01",
                    )
                    store.append_slot_event(
                        run_id="run_01",
                        slot_id=slot_id,
                        level="ERROR",
                        stage="QUARANTINED",
                        message="solve failed and slot quarantined",
                    )
                else:
                    store.update_slot_task(
                        run_id="run_01",
                        slot_id=slot_id,
                        state="QUEUED",
                        attempt_no=0,
                        job_id=None,
                        account_id="account_01",
                    )
            store.start_run("run_02")
            store.create_slot_task(
                run_id="run_02",
                slot_id="w_other",
                input_path="/in/other.aedt",
                output_path="/out/other.aedt.out",
                account_id="account_02",
            )
            store.update_slot_task(
                run_id="run_02",
                slot_id="w_other",
                state="QUARANTINED",
                attempt_no=1,
                job_id="job_other",
                account_id="account_02",
            )
            store.append_slot_event(
                run_id="run_02",
                slot_id="w_other",
                level="ERROR",
                stage="QUARANTINED",
                message="other run quarantine",
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with patch.dict(
                    os.environ,
                    {
                        "PEETSFEA_ACCOUNTS": "account_01@gate1-harry:1",
                        "PEETSFEA_SLOTS_PER_JOB": "8",
                    },
                    clear=False,
                ):
                    with urlopen(f"http://{host}:{port}/api/events/recent?run_id=run_01&limit=50") as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["run_id"], "run_01")
                self.assertEqual(payload["throughput_kpi"]["throughput_mode"], "CAPACITY_SHORTFALL")
                self.assertTrue(payload["throughput_kpi"]["capacity_limited"])
                self.assertTrue(all(event["run_id"] == "run_01" for event in payload["events"]))

                blocked_event = next(event for event in payload["events"] if event["stage"] == "WORKER_LOOP_BLOCKED")
                self.assertEqual(blocked_event["account_id"], "account_01")
                self.assertEqual(blocked_event["category"], "readiness")
                self.assertTrue(blocked_event["alertable"])
                self.assertEqual(
                    blocked_event["common_key"],
                    "readiness:account_01:WORKER_LOOP_BLOCKED",
                )

                quarantine_event = next(event for event in payload["events"] if event["stage"] == "QUARANTINED")
                self.assertEqual(quarantine_event["account_id"], "account_01")
                self.assertEqual(quarantine_event["category"], "quarantine")
                self.assertEqual(quarantine_event["severity"], "ERROR")

                alert_keys = {alert["alert_key"] for alert in payload["alerts"]}
                self.assertIn("CAPACITY_SHORTFALL", alert_keys)
                self.assertIn("READINESS_BLOCKED", alert_keys)
                self.assertIn("QUARANTINE_BURST", alert_keys)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_recent_events_classifies_cutover_and_truth_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="INFO",
                stage="SLURM_TRUTH_REFRESHED",
                message="account=account_01 host=gate1-harry remote_running=1 remote_pending=0 local_active_jobs=1 allowed_submit=0",
            )
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="WARN",
                stage="CUTOVER_BLOCKED",
                message="run_id=run_01 queued_slots=4 blocked_accounts=account_01",
            )
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="INFO",
                stage="SLURM_WORKERS_REDISCOVERED",
                message="run_id=run_01 count=1",
            )
            store.append_event(
                run_id="run_01",
                job_id="__worker__",
                level="WARN",
                stage="ACCOUNT_COOLDOWN",
                message="account=account_04 host=gate1-dw16 cooldown_seconds=300 reason=Connection closed by 172.16.10.36 port 22",
            )
            store.append_event(
                run_id="run_01",
                job_id="job_0001",
                level="WARN",
                stage="RETRY_BACKOFF",
                message="account=account_04 host=gate1-dw16 attempt=1 reason=LAUNCH_TRANSIENT: Connection closed by 172.16.10.36 port 22",
            )
            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/events/recent?run_id=run_01&limit=20") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))

                truth_event = next(event for event in payload["events"] if event["stage"] == "SLURM_TRUTH_REFRESHED")
                self.assertEqual(truth_event["category"], "capacity")
                self.assertFalse(truth_event["alertable"])

                blocked_event = next(event for event in payload["events"] if event["stage"] == "CUTOVER_BLOCKED")
                self.assertEqual(blocked_event["category"], "lifecycle")
                self.assertTrue(blocked_event["alertable"])
                self.assertEqual(blocked_event["severity"], "WARN")

                rediscovered_event = next(
                    event for event in payload["events"] if event["stage"] == "SLURM_WORKERS_REDISCOVERED"
                )
                self.assertEqual(rediscovered_event["category"], "recovery")

                cooldown_event = next(event for event in payload["events"] if event["stage"] == "ACCOUNT_COOLDOWN")
                self.assertEqual(cooldown_event["category"], "capacity")
                self.assertTrue(cooldown_event["alertable"])

                retry_event = next(event for event in payload["events"] if event["stage"] == "RETRY_BACKOFF")
                self.assertEqual(retry_event["category"], "recovery")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
