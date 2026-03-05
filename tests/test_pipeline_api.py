from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline
from peetsfea_runner.pipeline import EXIT_CODE_SUCCESS, PipelineResult
from peetsfea_runner.remote_job import CaseExecutionSummary, RemoteJobAttemptResult
from peetsfea_runner.scheduler import AccountCapacitySnapshot
from peetsfea_runner.state_store import StateStore


class TestPipelineApi(unittest.TestCase):
    def test_defaults_match_roadmap09(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "sample.aedt").write_text("x", encoding="utf-8")
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir))
            self.assertEqual(config.output_root_dir, "./output")
            self.assertTrue(config.delete_input_after_upload)
            self.assertEqual(config.delete_failed_quarantine_dir, "./output/_delete_failed")
            self.assertTrue(config.license_observe_only)
            self.assertEqual(len(config.accounts_registry), 1)
            self.assertTrue(config.continuous_mode)
            self.assertEqual(config.ready_sidecar_suffix, ".ready")
            self.assertEqual(config.run_rotation_hours, 24)

    def test_validate_rejects_missing_directory(self) -> None:
        missing = Path(tempfile.gettempdir()) / "missing_input_queue"
        config = PipelineConfig(input_queue_dir=str(missing))
        with self.assertRaises(FileNotFoundError):
            config.validate()

    def test_validate_rejects_no_aedt_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "x.txt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            with self.assertRaises(ValueError):
                config.validate()

    def test_validate_allows_empty_queue_in_continuous_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=True)
            _input_root, _output_root, files, _accounts = config.validate()
            self.assertEqual(files, [])

    def test_validate_requires_ready_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "foo.aedt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            with self.assertRaises(ValueError):
                config.validate()

    def test_dry_run_creates_mirrored_aedt_all_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            nested = input_dir / "a" / "b"
            nested.mkdir(parents=True, exist_ok=True)
            (nested / "foo.aedt").write_text("placeholder", encoding="utf-8")
            (nested / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
            )

            result = run_pipeline(config)

            expected = output_root / "a" / "b" / "foo.aedt.aedt_all"
            self.assertTrue(expected.is_dir())
            self.assertIsInstance(result, PipelineResult)
            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            self.assertEqual(result.total_jobs, 1)
            self.assertIn("failed_job_ids=[]", result.summary)

    def test_continuous_mode_schedules_existing_queued_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_window_task(
                run_id="run_01",
                window_id="w_backlog_0001",
                input_path=str(input_dir / "backlog.aedt"),
                output_path=str(output_root / "backlog.aedt.aedt_all"),
                account_id=None,
                state="QUEUED",
            )
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
            )

            result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_windows, 1)
            self.assertEqual(result.total_jobs, 1)
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute(
                    "SELECT state FROM window_tasks WHERE run_id = ? AND window_id = ?",
                    ["run_01", "w_backlog_0001"],
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "SUCCEEDED")

    def test_upload_success_deletes_input_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=8, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            self.assertFalse(input_file.exists())
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute(
                    "SELECT delete_final_state FROM file_lifecycle ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "DELETED")

    def test_delete_failure_moves_to_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            quarantine_root = Path(tmpdir) / "delete_failed"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                delete_failed_quarantine_dir=str(quarantine_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=8, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("pathlib.Path.unlink", side_effect=OSError("locked")),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            quarantined_path = quarantine_root / "foo.aedt"
            self.assertTrue(quarantined_path.exists())


if __name__ == "__main__":
    unittest.main()
