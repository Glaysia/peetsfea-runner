from __future__ import annotations

import unittest
from unittest.mock import patch

from peetsfea_runner.pipeline import (
    EXIT_CODE_REMOTE_RUN_FAILURE,
    _WorkflowError,
    _build_case_aggregation_command,
    _build_case_window_command,
    _build_remote_job_script_content,
    _build_wait_all_command,
    _count_screen_windows,
    _parse_case_summary_lines,
    _run_with_retry,
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
        self.assertIn("\"$VENV_DIR/bin/python\" run_sim.py", content)

    def test_retry_calls_action_again(self) -> None:
        calls = {"count": 0}

        def flaky() -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise _WorkflowError("first fail", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)

        _run_with_retry("srun", retry_count=1, action=flaky, sleep_seconds=0)
        self.assertEqual(calls["count"], 2)

    def test_retry_does_not_retry_non_retryable_exit_code(self) -> None:
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
        calls = {"count": 0}

        def flaky_once() -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise _WorkflowError("temporary", exit_code=11)

        with patch("peetsfea_runner.pipeline.time.sleep") as mocked_sleep:
            _run_with_retry("srun", retry_count=1, action=flaky_once, retryable_exit_codes={11})
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
