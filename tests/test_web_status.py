from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

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
                self.assertEqual(len(payload["accounts"]), 2)
                self.assertEqual(payload["accounts"][0]["account_id"], "account_01")
                self.assertEqual(payload["accounts"][0]["score"], 1)
                self.assertEqual(payload["accounts"][0]["readiness_status"], "READY")
                self.assertTrue(payload["accounts"][0]["readiness_ready"])
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


if __name__ == "__main__":
    unittest.main()
