from __future__ import annotations

import unittest
from unittest.mock import patch

from peetsfea_runner.pipeline import (
    EXIT_CODE_REMOTE_RUN_FAILURE,
    _WorkflowError,
    _build_case_aggregation_command,
    _build_case_window_command,
    _build_remote_dispatch_script_content,
    _build_remote_job_script_content,
    _build_wait_all_command,
    _count_screen_windows,
    _extract_meaningful_remote_failure_details,
    _has_remote_workflow_markers,
    _parse_marked_case_summary_lines,
    _parse_marked_failed_count,
    _parse_case_summary_lines,
    _run_with_retry,
)


class TestPlan03Workflow(unittest.TestCase):
    def test_remote_script_contains_required_environment_setup(self) -> None:
        content = _build_remote_job_script_content()
        self.assertIn("export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/AnsysEM", content)
        self.assertIn("export LANG=en_US.UTF-8", content)
        self.assertIn("export LC_ALL=en_US.UTF-8", content)
        self.assertIn("unset LANGUAGE", content)
        self.assertIn("export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81", content)
        self.assertIn("module load ansys-electronics/v252", content)
        self.assertIn("\"$VENV_DIR/bin/python\" run_sim.py", content)
        self.assertIn("os.environ['TMPDIR'] = str(tmpdir)", content)

    def test_remote_dispatch_script_uses_noninteractive_srun_without_screen(self) -> None:
        class _Cfg:
            partition = "cpu2"
            nodes = 1
            ntasks = 1
            cpus_per_job = 32
            mem = "320G"
            time_limit = "05:00:00"
            windows_per_job = 8
            cores_per_window = 4

        content = _build_remote_dispatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/job_0001",
            case_count=2,
        )
        self.assertIn("srun -D /tmp -p cpu2 -N 1 -n 1 -c 32 --mem=320G --time=05:00:00", content)
        self.assertNotIn("screen -dmS", content)
        self.assertNotIn("srun --pty", content)
        self.assertIn("__PEETS_FAILED_COUNT__", content)
        self.assertIn("max_parallel=8", content)
        self.assertIn("wait -n || true", content)
        self.assertIn("archive_path=$(mktemp /tmp/peetsfea-results.", content)
        self.assertIn('tar -czf "$archive_path" case_* case_summary.txt failed.count', content)
        self.assertNotIn("tar --exclude='.venv' --exclude='results.tgz' --exclude='.env_initialized' -czf results.tgz .", content)

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

    def test_parse_marked_summary_output(self) -> None:
        output = (
            "__PEETS_FAILED_COUNT__:2\n"
            "__PEETS_CASE_SUMMARY_BEGIN__\n"
            "case_01:0\ncase_02:2\ncase_03:137\n"
            "__PEETS_CASE_SUMMARY_END__\n"
        )
        self.assertEqual(_parse_marked_failed_count(output), 2)
        self.assertEqual(_parse_marked_case_summary_lines(output), ["case_02:2", "case_03:137"])

    def test_marker_detection_accepts_combined_remote_output(self) -> None:
        output = (
            "srun: job 123 queued and waiting for resources\n"
            "srun: job 123 has been allocated resources\n"
            "__PEETS_FAILED_COUNT__:0\n"
            "__PEETS_CASE_SUMMARY_BEGIN__\n"
            "case_01:0\n"
            "__PEETS_CASE_SUMMARY_END__\n"
            "__PEETS_RESULTS_TGZ_BEGIN__\n"
            "Zm9v\n"
            "__PEETS_RESULTS_TGZ_END__\n"
        )
        self.assertTrue(_has_remote_workflow_markers(output))

    def test_failure_detail_filter_ignores_slurm_progress_lines(self) -> None:
        output = (
            "srun: job 123 queued and waiting for resources\n"
            "srun: job 123 has been allocated resources\n"
            "real failure line\n"
        )
        self.assertEqual(_extract_meaningful_remote_failure_details(output), "real failure line")


if __name__ == "__main__":
    unittest.main()
