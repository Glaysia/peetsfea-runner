from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from peetsfea_runner import PipelineConfig, run_pipeline
from peetsfea_runner.pipeline import EXIT_CODE_SUCCESS, PipelineResult


class TestPipelineApi(unittest.TestCase):
    def test_defaults_match_roadmap07(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "sample.aedt").write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(input_aedt_dir=str(input_dir))

            self.assertEqual(config.host, "gate1-harry")
            self.assertEqual(config.partition, "cpu2")
            self.assertEqual(config.nodes, 1)
            self.assertEqual(config.ntasks, 1)
            self.assertEqual(config.cpus_per_job, 32)
            self.assertEqual(config.mem, "320G")
            self.assertEqual(config.time_limit, "05:00:00")
            self.assertEqual(config.remote_root, "~/aedt_runs")
            self.assertEqual(config.local_artifacts_dir, "./artifacts")
            self.assertEqual(config.execute_remote, False)
            self.assertEqual(config.max_jobs_per_account, 10)
            self.assertEqual(config.windows_per_job, 8)
            self.assertEqual(config.cores_per_window, 4)
            self.assertEqual(config.license_cap_per_account, 80)
            self.assertEqual(config.job_retry_count, 1)
            self.assertEqual(config.scan_recursive, False)
            self.assertEqual(config.metadata_db_path, "./peetsfea_runner.duckdb")

    def test_validate_rejects_missing_directory(self) -> None:
        missing = Path(tempfile.gettempdir()) / "definitely_missing_dir_for_aedt_scan"
        config = PipelineConfig(input_aedt_dir=str(missing))
        with self.assertRaises(FileNotFoundError):
            config.validate()

    def test_validate_rejects_non_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "single.aedt"
            file_path.write_text("placeholder", encoding="utf-8")
            config = PipelineConfig(input_aedt_dir=str(file_path))
            with self.assertRaises(ValueError):
                config.validate()

    def test_validate_rejects_no_aedt_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "not_aedt.txt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_aedt_dir=str(input_dir))
            with self.assertRaises(ValueError):
                config.validate()

    def test_run_pipeline_fails_explicitly_when_no_aedt_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "not_aedt.txt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_aedt_dir=str(input_dir))
            with self.assertRaises(ValueError):
                run_pipeline(config)

    def test_validate_non_recursive_scan_excludes_nested_aedt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            nested = input_dir / "nested"
            nested.mkdir(parents=True, exist_ok=True)
            top_file = input_dir / "top.aedt"
            nested_file = nested / "nested.aedt"
            top_file.write_text("top", encoding="utf-8")
            nested_file.write_text("nested", encoding="utf-8")

            config = PipelineConfig(input_aedt_dir=str(input_dir), scan_recursive=False)
            files = config.validate()
            self.assertEqual(files, [top_file.resolve()])

    def test_validate_rejects_insufficient_cpus_for_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "sample.aedt").write_text("placeholder", encoding="utf-8")
            config = PipelineConfig(
                input_aedt_dir=str(input_dir),
                cpus_per_job=31,
                windows_per_job=8,
                cores_per_window=4,
            )
            with self.assertRaises(ValueError):
                config.validate()

    def test_run_pipeline_returns_success_result_for_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "a_01.aedt").write_text("placeholder", encoding="utf-8")
            (input_dir / "a_02.aedt").write_text("placeholder", encoding="utf-8")

            output_root = Path(tmpdir) / "artifacts"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_aedt_dir=str(input_dir),
                local_artifacts_dir=str(output_root),
                metadata_db_path=str(db_path),
            )
            result = run_pipeline(config)

            self.assertIsInstance(result, PipelineResult)
            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            self.assertEqual(result.total_jobs, 2)
            self.assertEqual(result.success_jobs, 2)
            self.assertEqual(result.failed_jobs, 0)
            self.assertEqual(result.quarantined_jobs, 0)
            self.assertTrue(Path(result.local_artifacts_dir).is_dir())
            self.assertTrue(db_path.exists())
            self.assertIn("total_jobs=2", result.summary)
            self.assertIn("failed_job_ids=[]", result.summary)

            conn = duckdb.connect(str(db_path))
            try:
                job_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
                self.assertEqual(job_count, 2)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
