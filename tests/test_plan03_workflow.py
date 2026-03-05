from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from peetsfea_runner import PipelineConfig, run_pipeline
from peetsfea_runner.pipeline import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_RUN_FAILURE,
    EXIT_CODE_SUCCESS,
    _CaseExecutionSummary,
    _WorkflowError,
    _build_case_aggregation_command,
    _build_case_window_command,
    _build_remote_job_script_content,
    _build_wait_all_command,
    _count_screen_windows,
    _parse_case_summary_lines,
)
from peetsfea_runner.remote_job import RemoteJobAttemptResult


class TestPlan03Workflow(unittest.TestCase):
    def test_remote_script_contains_required_environment_setup(self) -> None:
        content = _build_remote_job_script_content()
        self.assertIn("export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/AnsysEM", content)
        self.assertIn("export SCREENDIR=\"$HOME/.screen\"", content)
        self.assertIn("export LANG=en_US.UTF-8", content)
        self.assertIn("export LC_ALL=en_US.UTF-8", content)
        self.assertIn("unset LANGUAGE", content)
        self.assertIn("export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81", content)
        self.assertIn("module load ansys-electronics/v252", content)
        self.assertIn("if [ ! -f .env_initialized ]; then", content)
        self.assertIn("VENV_DIR=\"$HOME/.peetsfea-runner-venv\"", content)
        self.assertIn("Run scripts/remote_bootstrap_install.sh first.", content)
        self.assertIn("BASE_PREFIX=\"$($VENV_DIR/bin/python -c 'import sys; print(sys.base_prefix)')\"", content)
        self.assertIn("export LD_LIBRARY_PATH=\"$BASE_PREFIX/lib:$HOME/miniconda3/lib:${LD_LIBRARY_PATH:-}\"", content)
        self.assertIn("\"$VENV_DIR/bin/python\" -m uv --version", content)
        self.assertIn("\"$VENV_DIR/bin/python\" -m uv pip install pyaedt==0.25.1", content)
        self.assertIn("DEPS_READY_MARKER=\"$VENV_DIR/.peets_deps_ready\"", content)
        self.assertIn("DEPS_LOCK_DIR=\"$VENV_DIR/.peets_deps_lock\"", content)
        self.assertIn("while ! mkdir \"$DEPS_LOCK_DIR\" 2>/dev/null; do", content)
        self.assertIn("\"$VENV_DIR/bin/python\" run_sim.py", content)
        self.assertNotIn("python3 -m venv .venv", content)
        self.assertNotIn("source .venv/bin/activate", content)
        self.assertIn("new_desktop=False", content)
        self.assertIn("PEETS_CORES", content)
        self.assertIn("save_project failed but solve completed", content)

    def test_run_pipeline_remote_success_with_mocked_job_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            for idx in range(3):
                (input_dir / f"sample_{idx}.aedt").write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(
                input_aedt_dir=str(input_dir),
                execute_remote=True,
                local_artifacts_dir=str(Path(tmpdir) / "artifacts"),
                metadata_db_path=str(Path(tmpdir) / "state.duckdb"),
            )

            with (
                patch(
                    "peetsfea_runner.pipeline.run_remote_job_attempt",
                    return_value=RemoteJobAttemptResult(
                        success=True,
                        exit_code=EXIT_CODE_SUCCESS,
                        session_name="mock",
                        case_summary=_CaseExecutionSummary(success_cases=8, failed_cases=0, case_lines=[]),
                        message="ok",
                        failed_case_lines=[],
                    ),
                ) as mocked_attempt,
                patch("peetsfea_runner.pipeline.cleanup_orphan_session") as mocked_cleanup_session,
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run") as mocked_cleanup_run,
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            self.assertEqual(result.total_jobs, 3)
            self.assertEqual(result.success_jobs, 3)
            self.assertEqual(result.failed_jobs, 0)
            self.assertEqual(result.quarantined_jobs, 0)
            self.assertIn("failed_job_ids=[]", result.summary)
            self.assertEqual(mocked_attempt.call_count, 3)
            self.assertEqual(mocked_cleanup_session.call_count, 3)
            mocked_cleanup_run.assert_called_once()

    def test_run_pipeline_retries_once_then_quarantines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "good.aedt").write_text("placeholder", encoding="utf-8")
            (input_dir / "bad.aedt").write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(
                input_aedt_dir=str(input_dir),
                execute_remote=True,
                local_artifacts_dir=str(Path(tmpdir) / "artifacts"),
                metadata_db_path=str(Path(tmpdir) / "state.duckdb"),
                job_retry_count=1,
            )

            def _mock_attempt(*, remote_job_dir: str, **kwargs):
                if remote_job_dir.endswith("/job_0001"):
                    return RemoteJobAttemptResult(
                        success=False,
                        exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                        session_name="bad",
                        case_summary=_CaseExecutionSummary(success_cases=6, failed_cases=2, case_lines=["case_03:1"]),
                        message="failed",
                        failed_case_lines=["case_03:1"],
                    )
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=EXIT_CODE_SUCCESS,
                    session_name="good",
                    case_summary=_CaseExecutionSummary(success_cases=8, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt) as mocked_attempt,
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertEqual(result.failed_jobs, 1)
            self.assertEqual(result.quarantined_jobs, 1)
            self.assertIn("job_0001", result.summary)
            self.assertEqual(mocked_attempt.call_count, 3)
            conn = duckdb.connect(str(Path(tmpdir) / "state.duckdb"))
            try:
                state = conn.execute(
                    "SELECT state FROM jobs WHERE run_id = ? AND job_id = 'job_0001'",
                    [result.run_id],
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "QUARANTINED")

    def test_run_pipeline_reports_download_failure_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "inputs"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "bad.aedt").write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(
                input_aedt_dir=str(input_dir),
                execute_remote=True,
                local_artifacts_dir=str(Path(tmpdir) / "artifacts"),
                metadata_db_path=str(Path(tmpdir) / "state.duckdb"),
                job_retry_count=0,
            )

            with (
                patch(
                    "peetsfea_runner.pipeline.run_remote_job_attempt",
                    return_value=RemoteJobAttemptResult(
                        success=False,
                        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
                        session_name="bad",
                        case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                        message="download failed",
                        failed_case_lines=[],
                    ),
                ),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_DOWNLOAD_FAILURE)
            self.assertIn("failed_jobs=1", result.summary)

    def test_retry_calls_action_again(self) -> None:
        from peetsfea_runner.pipeline import _run_with_retry

        calls = {"count": 0}

        def flaky() -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise _WorkflowError("first fail", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)

        _run_with_retry("srun", retry_count=1, action=flaky, sleep_seconds=0)
        self.assertEqual(calls["count"], 2)

    def test_retry_does_not_retry_non_retryable_exit_code(self) -> None:
        from peetsfea_runner.pipeline import _run_with_retry

        calls = {"count": 0}

        def always_fail_remote_run() -> None:
            calls["count"] += 1
            raise _WorkflowError("remote run failed", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)

        with self.assertRaises(_WorkflowError):
            _run_with_retry(
                "srun",
                retry_count=1,
                action=always_fail_remote_run,
                sleep_seconds=0,
                retryable_exit_codes={11},
            )

        self.assertEqual(calls["count"], 1)

    def test_retry_waits_between_attempts(self) -> None:
        from peetsfea_runner.pipeline import _run_with_retry

        calls = {"count": 0}

        def flaky_once() -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise _WorkflowError("temporary", exit_code=11)

        with patch("peetsfea_runner.pipeline.time.sleep") as mocked_sleep:
            _run_with_retry(
                "srun",
                retry_count=1,
                action=flaky_once,
                retryable_exit_codes={11},
            )
            mocked_sleep.assert_called_once_with(10)

    def test_window_count_parser(self) -> None:
        self.assertEqual(_count_screen_windows("0$ case_01 1 case_02 2 case_03"), 3)
        self.assertEqual(_count_screen_windows(""), 0)

    def test_wait_all_and_aggregation_commands_contain_parallel_window_count(self) -> None:
        self.assertIn("seq 1 8", _build_wait_all_command(8))
        self.assertIn("seq 1 8", _build_case_aggregation_command(8))

    def test_case_window_command_contains_case_name_and_core_count(self) -> None:
        command = _build_case_window_command(case_index=3, cores_per_window=4)
        self.assertIn("case_03", command)
        self.assertIn("PEETS_CORES=4", command)

    def test_parse_case_summary_lines_only_keeps_failed_cases(self) -> None:
        parsed = _parse_case_summary_lines("case_01:0\ncase_02:2\ncase_03:137\nnoise\n")
        self.assertEqual(parsed, ["case_02:2", "case_03:137"])


if __name__ == "__main__":
    unittest.main()
