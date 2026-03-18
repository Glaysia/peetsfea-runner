from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from peetsfea_runner.web_status import (
    _canary_artifact_status_payload,
    _design_outputs_manifest_gate_reason,
    _rolling_valid_csv_throughput_payload,
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
