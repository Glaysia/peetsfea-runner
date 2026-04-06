from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen

from peetsfea_runner.pipeline import LeaseServerContext
from peetsfea_runner.state_store import StateStore
from peetsfea_runner.web_status import start_status_server


def _read_json(url: str, data: bytes | None = None) -> dict[str, object]:
    request = Request(url, data=data)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    with urlopen(request, timeout=5) as response:  # nosec - loopback test server
        return json.loads(response.read().decode("utf-8"))


def test_health_and_lease_request_round_trip() -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        input_path = root / "input.aedt"
        input_path.write_text("dummy", encoding="utf-8")
        output_path = root / "input.aedt.out"

        store = StateStore(root / "runtime.state")
        store.initialize()
        store.start_run("run_01")
        store.create_slot_task(
            run_id="run_01",
            slot_id="slot_01",
            input_path=str(input_path),
            output_path=str(output_path),
            account_id=None,
        )
        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_01",
            job_id="worker_01",
            attempt_no=1,
            account_id="account_01",
            host_alias="gate1-harry261",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=48,
            backend="slurm_batch",
        )

        lease_context = LeaseServerContext(
            delete_input_after_upload=False,
            rename_input_to_done_on_success=True,
            delete_failed_quarantine_dir=str(root / "_delete_failed"),
            ready_sidecar_suffix=".ready",
            retain_aedtresults=False,
            worker_requeue_limit=1,
            lease_ttl_seconds=120,
        )
        server = start_status_server(state_store=store, host="127.0.0.1", port=0, lease_context=lease_context)
        try:
            host, port = server.server_address
            from threading import Thread

            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()

            health = _read_json(f"http://{host}:{port}/health")
            assert health["ok"] is True

            payload = json.dumps(
                {
                    "run_id": "run_01",
                    "worker_id": "worker_01",
                    "account_id": "account_01",
                    "slurm_job_id": "12345",
                }
            ).encode("utf-8")
            lease = _read_json(f"http://{host}:{port}/internal/leases/request", data=payload)
            assert lease["ok"] is True
            assert lease["lease_available"] is True
            assert lease["slot_id"] == "slot_01"
            assert isinstance(lease["lease_token"], str)
        finally:
            server.shutdown()
            server.server_close()
