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
            store.finish_run("run_01", state="SUCCEEDED", summary="ok")

            server = start_status_server(db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urlopen(f"http://{host}:{port}/api/jobs") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(payload["jobs"]), 1)

                with urlopen(f"http://{host}:{port}/api/jobs/job_0001") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["job"]["job_id"], "job_0001")

                with urlopen(f"http://{host}:{port}/api/metrics/throughput") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["metrics"]["total_jobs"], 1)
                self.assertEqual(payload["metrics"]["succeeded_jobs"], 1)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
