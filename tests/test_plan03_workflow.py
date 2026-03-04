from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from peetsfea_runner import PipelineConfig, run_pipeline
from peetsfea_runner.pipeline import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_RUN_FAILURE,
    EXIT_CODE_SUCCESS,
    _WorkflowError,
    _build_remote_job_script_content,
)


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
        self.assertIn("\"$VENV_DIR/bin/python\" run_sim.py", content)
        self.assertNotIn("python3 -m venv .venv", content)
        self.assertNotIn("source .venv/bin/activate", content)
        self.assertIn("new_desktop=False", content)
        self.assertIn("PEETS_CORES", content)

    def test_run_pipeline_remote_success_with_mocked_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sample = Path(tmpdir) / "sample.aedt"
            sample.write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(
                input_aedt_path=str(sample),
                execute_remote=True,
                local_artifacts_dir=str(Path(tmpdir) / "artifacts"),
            )

            with (
                patch("peetsfea_runner.pipeline._prepare_remote_workspace") as prepare,
                patch("peetsfea_runner.pipeline._upload_files") as upload,
                patch("peetsfea_runner.pipeline._run_remote_workflow_interactive") as interactive,
                patch("peetsfea_runner.pipeline._download_results") as download,
                patch("peetsfea_runner.pipeline._cleanup_remote_workspace") as cleanup,
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            prepare.assert_called_once()
            upload.assert_called_once()
            interactive.assert_called_once()
            download.assert_called_once()
            cleanup.assert_called_once()

    def test_run_pipeline_remote_stage_failure_maps_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sample = Path(tmpdir) / "sample.aedt"
            sample.write_text("placeholder", encoding="utf-8")

            config = PipelineConfig(
                input_aedt_path=str(sample),
                execute_remote=True,
                local_artifacts_dir=str(Path(tmpdir) / "artifacts"),
            )

            with (
                patch("peetsfea_runner.pipeline._prepare_remote_workspace"),
                patch("peetsfea_runner.pipeline._upload_files"),
                patch("peetsfea_runner.pipeline._run_remote_workflow_interactive"),
                patch(
                    "peetsfea_runner.pipeline._download_results",
                    side_effect=_WorkflowError("download failed", exit_code=EXIT_CODE_DOWNLOAD_FAILURE),
                ),
                patch("peetsfea_runner.pipeline._cleanup_remote_workspace") as cleanup,
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_DOWNLOAD_FAILURE)
            self.assertIn("download failed", result.summary)
            cleanup.assert_not_called()

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


if __name__ == "__main__":
    unittest.main()
