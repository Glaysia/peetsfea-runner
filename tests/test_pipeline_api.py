from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from peetsfea_runner import PipelineConfig, run_pipeline
from peetsfea_runner.pipeline import EXIT_CODE_SUCCESS, PipelineResult


class TestPipelineApi(unittest.TestCase):
    def test_defaults_match_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sample = Path(tmpdir) / "sample.aedt"
            sample.write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(input_aedt_path=str(sample))

            self.assertEqual(config.host, "gate1-harry")
            self.assertEqual(config.partition, "cpu2")
            self.assertEqual(config.nodes, 1)
            self.assertEqual(config.ntasks, 1)
            self.assertEqual(config.cpus, 32)
            self.assertEqual(config.mem, "320G")
            self.assertEqual(config.time_limit, "05:00:00")
            self.assertEqual(config.retry_count, 1)
            self.assertEqual(config.remote_root, "~/aedt_runs")
            self.assertEqual(config.local_artifacts_dir, "./artifacts")
            self.assertEqual(config.parallel_windows, 8)
            self.assertEqual(config.cores_per_window, 4)

    def test_validate_rejects_non_aedt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            wrong = Path(tmpdir) / "project.txt"
            wrong.write_text("x", encoding="utf-8")
            config = PipelineConfig(input_aedt_path=str(wrong))

            with self.assertRaises(ValueError):
                config.validate()

    def test_validate_rejects_missing_file(self) -> None:
        missing = Path(tempfile.gettempdir()) / "definitely_missing_file.aedt"
        config = PipelineConfig(input_aedt_path=str(missing))

        with self.assertRaises(FileNotFoundError):
            config.validate()

    def test_run_pipeline_returns_success_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sample = Path(tmpdir) / "sample.aedt"
            sample.write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "artifacts"

            config = PipelineConfig(
                input_aedt_path=str(sample),
                local_artifacts_dir=str(output_root),
            )
            result = run_pipeline(config)

            self.assertIsInstance(result, PipelineResult)
            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            self.assertTrue(result.run_id)
            self.assertIn(result.run_id, result.remote_run_dir)
            self.assertTrue(Path(result.local_artifacts_dir).is_dir())

    def test_validate_rejects_insufficient_cpus_for_parallel_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sample = Path(tmpdir) / "sample.aedt"
            sample.write_text("placeholder", encoding="utf-8")
            config = PipelineConfig(
                input_aedt_path=str(sample),
                cpus=31,
                parallel_windows=8,
                cores_per_window=4,
            )

            with self.assertRaises(ValueError):
                config.validate()


if __name__ == "__main__":
    unittest.main()
