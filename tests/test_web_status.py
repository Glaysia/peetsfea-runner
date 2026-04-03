from __future__ import annotations

import io
import json
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path
from urllib import error, request

from peetsfea_runner.pipeline import LeaseServerContext
from peetsfea_runner.state_store import StateStore
from peetsfea_runner.web_status import (
    _canary_artifact_status_payload,
    _design_outputs_manifest_gate_reason,
    _rolling_valid_csv_throughput_payload,
    start_status_server,
)


class TestWebStatus(unittest.TestCase):
    def test_design_outputs_manifest_gate_reason_accepts_report_native_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            reports_dir = root / "design_outputs" / "HFSSDesign1" / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "coupling.csv").write_text("k_ratio,Lrx_uH\n0.91,12.5\n", encoding="utf-8")
            (root / "design_outputs" / "index.csv").write_text(
                "design_name,reports_dir\n"
                "HFSSDesign1,design_outputs/HFSSDesign1/reports\n",
                encoding="utf-8",
            )

            self.assertEqual(_design_outputs_manifest_gate_reason(output_root=root), "ok")

    def test_canary_artifact_status_payload_reports_manifest_and_report_csv_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            reports_dir = root / "design_outputs" / "HFSSDesign1" / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "coupling.csv").write_text("k_ratio,Lrx_uH\n0.91,12.5\n", encoding="utf-8")
            (root / "design_outputs" / "index.csv").write_text(
                "design_name,reports_dir\n"
                "HFSSDesign1,design_outputs/HFSSDesign1/reports\n",
                encoding="utf-8",
            )
            (root / "run.log").write_text("log", encoding="utf-8")
            (root / "exit.code").write_text("0", encoding="utf-8")

            payload = _canary_artifact_status_payload(str(root))

            self.assertTrue(payload["canary_output_root_exists"])
            self.assertTrue(payload["canary_materialized_output_present"])
            self.assertEqual(payload["canary_design_outputs_manifest_gate"], "ok")
            self.assertEqual(
                payload["canary_design_outputs_manifest_path"],
                str((root / "design_outputs" / "index.csv").resolve()),
            )
            self.assertEqual(
                payload["canary_design_outputs_report_csv_path"],
                str((reports_dir / "coupling.csv").resolve()),
            )
            self.assertNotIn("canary_output_variables_csv_gate", payload)
            self.assertNotIn("canary_output_variables_csv_path", payload)

    def test_rolling_throughput_basis_uses_report_native_outputs(self) -> None:
        payload = _rolling_valid_csv_throughput_payload(Path("/tmp/unused.duckdb"), run_id=None)
        self.assertEqual(payload["throughput_measurement_basis"], "valid_design_outputs_reports")

    def test_lease_endpoints_materialize_artifact_and_rename_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_dir = root / "in"
            output_dir = root / "out"
            source_dir = root / "source"
            db_path = root / "state.duckdb"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            source_dir.mkdir(parents=True, exist_ok=True)
            source_path = source_dir / "sample.aedt"
            source_path.write_text("fixture", encoding="utf-8")
            input_path = input_dir / "sample.aedt"
            input_path.symlink_to(source_path)
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")

            store = StateStore(db_path)
            store.initialize()
            store.create_slot_task(
                run_id="run_01",
                slot_id="slot_01",
                input_path=str(input_path),
                output_path=str(output_dir / "sample.aedt.out"),
                account_id="account_01",
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
                backend="slurm_pull",
            )

            server = start_status_server(
                db_path=str(db_path),
                host="127.0.0.1",
                port=0,
                lease_context=LeaseServerContext(
                    delete_input_after_upload=False,
                    rename_input_to_done_on_success=True,
                    delete_failed_quarantine_dir=str(root / "delete_failed"),
                    ready_sidecar_suffix=".ready",
                    retain_aedtresults=False,
                    worker_requeue_limit=1,
                    lease_ttl_seconds=120,
                ),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{server.server_port}"

            try:
                lease_req = request.Request(
                    base_url + "/internal/leases/request",
                    data=json.dumps(
                        {"run_id": "run_01", "worker_id": "worker_01", "account_id": "account_01", "slurm_job_id": "12345"}
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(lease_req) as resp:
                    lease_payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(lease_payload["lease_available"])
                lease_token = lease_payload["lease_token"]

                with request.urlopen(base_url + f"/internal/leases/input?run_id=run_01&lease_token={lease_token}") as resp:
                    input_bytes = resp.read()
                self.assertEqual(input_bytes, b"fixture")

                archive_buffer = io.BytesIO()
                with tarfile.open(fileobj=archive_buffer, mode="w:gz") as handle:
                    run_log_info = tarfile.TarInfo("run.log")
                    run_log_bytes = b"log"
                    run_log_info.size = len(run_log_bytes)
                    handle.addfile(run_log_info, io.BytesIO(run_log_bytes))
                    exit_info = tarfile.TarInfo("exit.code")
                    exit_bytes = b"0"
                    exit_info.size = len(exit_bytes)
                    handle.addfile(exit_info, io.BytesIO(exit_bytes))

                artifact_req = request.Request(
                    base_url + f"/internal/leases/artifact?run_id=run_01&lease_token={lease_token}",
                    data=archive_buffer.getvalue(),
                    headers={"Content-Type": "application/gzip"},
                    method="POST",
                )
                with request.urlopen(artifact_req) as resp:
                    artifact_payload = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(artifact_payload["ok"])
                self.assertEqual(artifact_payload["state"], "SUCCEEDED")

                complete_req = request.Request(
                    base_url + "/internal/leases/complete",
                    data=json.dumps({"run_id": "run_01", "lease_token": lease_token}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(error.HTTPError) as exc_info:
                    request.urlopen(complete_req)
                self.assertEqual(exc_info.exception.code, 404)
            finally:
                server.shutdown()
                server.server_close()

            self.assertTrue((output_dir / "sample.aedt.out" / "run.log").exists())
            self.assertTrue((input_dir / "sample.aedt.done").exists())
            self.assertTrue(source_path.exists())
