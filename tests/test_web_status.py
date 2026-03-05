from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from peetsfea_runner.state_store import StateStore
from peetsfea_runner.web_status import start_status_server


class TestWebStatus(unittest.TestCase):
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
            store.append_window_event(
                run_id="run_01",
                window_id="w_001",
                level="INFO",
                stage="SUCCEEDED",
                message="window done",
            )
            store.mark_window_input_deleted(run_id="run_01", window_id="w_001", retry_count=0)
            store.record_account_capacity_snapshot(
                account_id="account_01",
                host="gate1-harry",
                running_count=1,
                pending_count=0,
                allowed_submit=12,
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
                self.assertIn("Window(.aedt)", html)

                with urlopen(f"http://{host}:{port}/api") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertIn("/api/worker/health", payload["endpoints"])
                self.assertIn("/api/jobs/{id}/timeline", payload["endpoints"])
                self.assertIn("/api/windows", payload["endpoints"])
                self.assertIn("/api/windows/{id}/timeline", payload["endpoints"])
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
                self.assertEqual(payload["metrics"]["total_jobs"], 1)
                self.assertEqual(payload["metrics"]["succeeded_jobs"], 1)
                self.assertEqual(payload["metrics"]["active_jobs"], 0)
                self.assertEqual(payload["metrics"]["queue_jobs"], 0)
                self.assertEqual(payload["metrics"]["total_windows"], 1)
                self.assertEqual(payload["metrics"]["succeeded_windows"], 1)
                self.assertEqual(payload["metrics"]["quarantined_windows"], 0)
                self.assertEqual(payload["metrics"]["delete_quarantined_windows"], 0)
                self.assertEqual(len(payload["metrics"]["account_window_scores"]), 1)

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
                self.assertTrue(any(x["state"] == "SUCCEEDED" for x in payload["run"]["window_state_counts"]))

                with urlopen(f"http://{host}:{port}/api/runs/run_01/jobs") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["jobs"]), 1)

                with urlopen(f"http://{host}:{port}/api/windows") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["run_id"], "run_01")
                self.assertEqual(len(payload["windows"]), 1)
                self.assertEqual(payload["windows"][0]["window_id"], "w_001")

                with urlopen(f"http://{host}:{port}/api/windows/w_001/timeline") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["window_task"]["window_id"], "w_001")
                self.assertEqual(payload["attempt_summary"]["window_attempt_no"], 1)
                self.assertEqual(payload["file_lifecycle"]["delete_final_state"], "DELETED")

                with urlopen(f"http://{host}:{port}/api/accounts/capacity") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["accounts"]), 1)
                self.assertEqual(payload["accounts"][0]["account_id"], "account_01")
                self.assertEqual(payload["accounts"][0]["score"], 1)

                with urlopen(f"http://{host}:{port}/api/events/recent") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertGreaterEqual(len(payload["events"]), 2)
                self.assertIn("source", payload["events"][0])

                with urlopen(f"http://{host}:{port}/api/file-lifecycle/summary") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertIn("file_lifecycle", payload)

                with urlopen(f"http://{host}:{port}/api/worker/health") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["status"], "HEALTHY")
                self.assertEqual(payload["reason"], "ok")
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
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
