from __future__ import annotations

import tempfile
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from peetsfea_runner.constants import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    EXIT_CODE_SLURM_FAILURE,
)
from peetsfea_runner.pipeline import (
    EXIT_CODE_REMOTE_RUN_FAILURE,
    _WorkflowError,
    _build_case_aggregation_command,
    _build_case_slot_command,
    _build_remote_dispatch_script_content,
    _build_remote_job_script_content,
    _build_wait_all_command,
    _count_screen_slots,
    _extract_meaningful_remote_failure_details,
    _has_remote_workflow_markers,
    _parse_marked_case_summary_lines,
    _parse_marked_failed_count,
    _parse_case_summary_lines,
    _run_with_retry,
)
from peetsfea_runner.remote_job import _categorize_failure, _resolve_remote_path
from peetsfea_runner.remote_job import (
    SlotInput,
    WorkflowError,
    _classify_return_path_failure_stage,
    _build_worker_payload_script_content,
    _build_remote_sbatch_script_content,
    _build_windows_remote_dispatch_script_content,
    _build_windows_remote_job_script_content,
    _parse_sbatch_job_id,
    _parse_squeue_state_line,
    _read_remote_optional_text_file,
    _run_completed_process_capture_with_transport_retry,
    _run_remote_workflow_sbatch,
    _wait_for_remote_sbatch_completion,
    query_slurm_job_state,
    run_remote_job_attempt,
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
        self.assertIn("MINICONDA_DIR=\"$HOME/miniconda3\"", content)
        self.assertIn("ensure_miniconda() {", content)
        self.assertIn("ensure_conda_python312() {", content)
        self.assertIn("ensure_runner_venv() {", content)
        self.assertIn("\"$VENV_DIR/bin/python\" run_sim.py", content)
        self.assertIn("os.environ['TMPDIR'] = str(tmpdir)", content)
        self.assertIn("hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", content)
        self.assertIn("EMIT_OUTPUT_VARIABLES_CSV: bool = True", content)
        self.assertIn("OUTPUT_VARIABLES_CSV_NAME: str = 'output_variables.csv'", content)
        self.assertIn("def extract_output_variables_csv(hfss: Hfss, workdir: Path, project_file: Path) -> None:", content)
        self.assertIn("def reopen_solved_project(project_file: Path, grpc_port: int) -> Hfss:", content)
        self.assertIn("def get_all_report_names(hfss: Hfss) -> list[str]:", content)
        self.assertIn("def create_output_variables_report(hfss: Hfss) -> str | None:", content)
        self.assertIn("def create_input_parameter_report(hfss: Hfss) -> str | None:", content)
        self.assertIn("def create_parameter_reports(hfss: Hfss) -> list[str]:", content)
        self.assertIn("def export_all_reports(hfss: Hfss, workdir: Path) -> dict[str, Path]:", content)
        self.assertIn("reports_dir = workdir / 'all_reports'", content)
        self.assertIn("hfss.post.export_report_to_csv", content)
        self.assertIn("reopened_hfss = reopen_solved_project(project_file, grpc_port)", content)
        self.assertIn("solve_hfss.release_desktop(close_projects=True, close_desktop=False)", content)
        self.assertIn("write_output_variables_csv(workdir, row)", content)
        self.assertIn("error_log_path.unlink()", content)
        self.assertIn("(workdir / OUTPUT_VARIABLES_ERROR_LOG_NAME).write_text(str(exc), encoding='utf-8')", content)
        self.assertIn("merge_report_rows(", content)
        self.assertNotIn("cores=[cores]", content)
        self.assertNotIn("tasks=[tasks]", content)
        self.assertIn('if [ -e "$MINICONDA_DIR" ]; then', content)
        self.assertIn('rm -rf "$MINICONDA_DIR"', content)

    def test_remote_script_can_disable_output_variables_csv_export(self) -> None:
        content = _build_remote_job_script_content(emit_output_variables_csv=False)
        self.assertIn("EMIT_OUTPUT_VARIABLES_CSV: bool = False", content)
        self.assertIn("if not EMIT_OUTPUT_VARIABLES_CSV:", content)

    def test_remote_script_merges_nominal_variations_into_csv_row_before_outputs(self) -> None:
        content = _build_remote_job_script_content()
        self.assertIn("import json", content)
        self.assertIn("def build_output_variable_row_variations(hfss: Hfss) -> dict[str, object]:", content)
        self.assertIn("if name == 'Freq':", content)
        self.assertIn("row[str(name)] = serialize_row_value(value)", content)
        self.assertIn("def write_synthetic_input_report(workdir: Path, hfss: Hfss) -> Path:", content)
        self.assertIn("canonical_input_path = write_synthetic_input_report(workdir, hfss)", content)
        self.assertIn("merge_report_rows(", content)
        self.assertLess(
            content.index("def build_output_variable_row_variations(hfss: Hfss) -> dict[str, object]:"),
            content.index("def write_synthetic_input_report(workdir: Path, hfss: Hfss) -> Path:"),
        )

    def test_remote_dispatch_script_uses_noninteractive_srun_without_screen(self) -> None:
        class _Cfg:
            partition = "cpu2"
            nodes = 1
            ntasks = 1
            cpus_per_job = 16
            mem = "960G"
            time_limit = "05:00:00"
            slots_per_job = 4
            cores_per_slot = 4
            slurm_exclude_nodes = ("n108", "n109")

        content = _build_remote_dispatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/job_0001",
            case_count=2,
        )
        self.assertIn("srun -D /tmp -p cpu2 -N 1 -n 1 -c 16 --mem=960G --time=05:00:00 --exclude=n108,n109", content)
        self.assertNotIn("screen -dmS", content)
        self.assertNotIn("srun --pty", content)
        self.assertIn("__PEETS_FAILED_COUNT__", content)
        self.assertIn("max_parallel=4", content)
        self.assertIn("export REMOTE_JOB_DIR", content)
        self.assertIn("wait -n || true", content)
        self.assertIn("if PEETS_SLOT_CORES=4 PEETS_SLOT_TASKS=1 bash ../remote_job.sh > run.log 2>&1; then", content)
        self.assertIn("echo \"$rc\" > exit.code", content)
        self.assertIn("archive_path=$(mktemp /tmp/peetsfea-results.", content)
        self.assertIn('tar -czf "$archive_path" case_* case_summary.txt failed.count', content)
        self.assertNotIn("tar --exclude='.venv' --exclude='results.tgz' --exclude='.env_initialized' -czf results.tgz .", content)

    def test_worker_payload_can_refill_pool_with_prefetched_cases(self) -> None:
        class _Cfg:
            slots_per_job = 4
            cores_per_slot = 4
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "harrypc"
            control_plane_return_host = "192.168.0.10"
            control_plane_return_port = 5722
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30

        content = _build_worker_payload_script_content(
            config=_Cfg(),
            case_count=8,
            run_id="run_01",
            worker_id="attempt_0001",
        )
        self.assertIn("max_parallel=4", content)
        self.assertIn("for i in $(seq 1 8); do", content)
        self.assertIn("wait -n || true", content)
        self.assertIn("ssh -M -S \"$PEETS_TUNNEL_SOCKET\" -fnNT", content)
        self.assertIn("-p \"$PEETS_CONTROL_RETURN_PORT\"", content)
        self.assertIn("-o StrictHostKeyChecking=no", content)
        self.assertIn("-o UserKnownHostsFile=/dev/null", content)
        self.assertIn("classify_return_path_stage()", content)
        self.assertIn("RETURN_PATH_DNS_FAILURE", content)
        self.assertIn("if [ -n \"${REMOTE_JOB_DIR:-}\" ]; then", content)
        self.assertIn("rm -rf \"$REMOTE_JOB_DIR\" >/dev/null 2>&1 || true", content)
        self.assertIn("find . -maxdepth 1 -name '*.lock' -type f -delete", content)
        self.assertIn("rm -rf tmp >/dev/null 2>&1 || true", content)
        self.assertIn("touch results.tgz.ready", content)
        self.assertIn("/internal/workers/register", content)
        self.assertIn("/internal/workers/heartbeat", content)
        self.assertIn("/internal/resources/node", content)
        self.assertIn("/internal/resources/worker", content)
        self.assertIn("/internal/resources/slot", content)
        self.assertIn("PEETS_CONTROL_WORKER_ID=attempt_0001", content)
        self.assertIn("PEETS_CONTROL_RUN_ID=run_01", content)

    def test_windows_remote_scripts_use_powershell_without_slurm(self) -> None:
        class _Cfg:
            platform = "windows"
            scheduler = "none"
            partition = "cpu2"
            nodes = 1
            ntasks = 1
            cpus_per_job = 16
            mem = "960G"
            time_limit = "05:00:00"
            slots_per_job = 4
            cores_per_slot = 4

        job_content = _build_windows_remote_job_script_content()
        dispatch_content = _build_windows_remote_dispatch_script_content(
            config=_Cfg(),
            remote_job_dir=r"C:\peets\job_0001",
            case_count=2,
        )
        self.assertIn("Scripts\\python.exe", job_content)
        self.assertIn("hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", job_content)
        self.assertNotIn("module load", job_content)
        self.assertNotIn("srun", dispatch_content)
        self.assertNotIn("squeue", dispatch_content)
        self.assertNotIn("bash -lc", dispatch_content)
        self.assertIn("remote_job.ps1", dispatch_content)
        self.assertIn("__PEETS_RESULTS_TGZ_BEGIN__", dispatch_content)

    def test_remote_sbatch_script_contains_worker_job_submission_shape(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            partition = "cpu2"
            nodes = 1
            ntasks = 1
            cpus_per_job = 16
            mem = "960G"
            time_limit = "05:00:00"
            slots_per_job = 4
            cores_per_slot = 4
            platform = "linux"
            scheduler = "slurm"
            remote_execution_backend = "slurm_batch"
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "peetsmain@192.168.0.10"
            control_plane_return_host = "192.168.0.10"
            control_plane_return_port = 5722
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30
            slurm_exclude_nodes = ("n108", "n109")

        content = _build_remote_sbatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/job_0001",
            run_id="run_01",
            worker_id="attempt_0001",
        )
        self.assertTrue(content.startswith("#!/bin/bash\n"))
        self.assertIn("#SBATCH -p cpu2", content)
        self.assertIn("#SBATCH -c 16", content)
        self.assertIn("#SBATCH -o slurm-%j.out", content)
        self.assertIn("#SBATCH -e slurm-%j.err", content)
        self.assertIn("#SBATCH --exclude=n108,n109", content)
        self.assertIn('cd "$REMOTE_JOB_DIR"', content)
        self.assertIn("export REMOTE_JOB_DIR", content)
        self.assertIn("/bin/bash ./remote_worker_payload.sh > worker.stdout 2> worker.stderr", content)
        self.assertNotIn("screen -dmS", content)
        self.assertIn("export PEETS_CONTROL_RUN_ID=run_01", content)
        self.assertIn("export PEETS_CONTROL_WORKER_ID=attempt_0001", content)
        self.assertIn("export PEETS_CONTROL_SSH_TARGET=peetsmain@192.168.0.10", content)
        self.assertIn("export PEETS_CONTROL_RETURN_HOST=192.168.0.10", content)
        self.assertIn("export PEETS_CONTROL_RETURN_USER=peetsmain", content)
        self.assertIn("export PEETS_CONTROL_RETURN_PORT=5722", content)

    def test_read_remote_optional_text_file_uses_remote_ssh_port(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            remote_ssh_port = 2202

        with patch("peetsfea_runner.remote_job._run_completed_process") as run_completed:
            run_completed.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="ok", stderr="")
            payload = _read_remote_optional_text_file(
                config=_Cfg(),
                remote_path="/tmp/job",
                filenames=("worker.stdout",),
                stage="worker stdout",
            )

        self.assertEqual(payload, "ok")
        command = run_completed.call_args.args[0]
        self.assertEqual(command[:4], ["ssh", "-p", "2202", "gate1-harry"])

    def test_classify_return_path_failure_stage(self) -> None:
        self.assertEqual(
            _classify_return_path_failure_stage("ssh: Could not resolve hostname harrypc: Name or service not known"),
            "RETURN_PATH_DNS_FAILURE",
        )
        self.assertEqual(
            _classify_return_path_failure_stage("ssh: connect to host 192.168.0.10 port 5722: Connection refused"),
            "RETURN_PATH_PORT_MISMATCH",
        )

    def test_parse_sbatch_job_id_accepts_parsable_output(self) -> None:
        self.assertEqual(_parse_sbatch_job_id("552740\n"), "552740")
        self.assertEqual(_parse_sbatch_job_id("552741;cpu2\n"), "552741")

    def test_parse_squeue_state_line_handles_node_and_missing_separator(self) -> None:
        self.assertEqual(_parse_squeue_state_line("RUNNING|n115"), ("RUNNING", "n115"))
        self.assertEqual(_parse_squeue_state_line("PENDING"), ("PENDING", ""))

    def test_query_slurm_job_state_prefers_squeue_and_falls_back_to_sacct(self) -> None:
        class _Cfg:
            host = "gate1-harry"

        with patch(
            "peetsfea_runner.remote_job._run_completed_process_capture_with_transport_retry",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="RUNNING|n115\n", stderr=""),
            ],
        ):
            self.assertEqual(query_slurm_job_state(_Cfg(), slurm_job_id="552740"), ("RUNNING", "n115"))

        with patch(
            "peetsfea_runner.remote_job._run_completed_process_capture_with_transport_retry",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=[], returncode=0, stdout="552740|COMPLETED|n115\n", stderr=""),
            ],
        ):
            self.assertEqual(query_slurm_job_state(_Cfg(), slurm_job_id="552740"), ("COMPLETED", "n115"))

    def test_query_slurm_job_state_returns_unknown_when_both_queries_are_empty(self) -> None:
        class _Cfg:
            host = "gate1-harry"

        with patch(
            "peetsfea_runner.remote_job._run_completed_process_capture_with_transport_retry",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            ],
        ):
            self.assertEqual(query_slurm_job_state(_Cfg(), slurm_job_id="552740"), ("UNKNOWN", None))

    def test_sbatch_failure_keeps_submitted_slurm_job_id_in_attempt_result(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            partition = "cpu2"
            nodes = 1
            ntasks = 1
            cpus_per_job = 16
            mem = "960G"
            time_limit = "05:00:00"
            slots_per_job = 4
            cores_per_slot = 4
            platform = "linux"
            scheduler = "slurm"
            remote_execution_backend = "slurm_batch"

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "sample.aedt"
            input_path.write_text("placeholder", encoding="utf-8")
            local_job_dir = Path(tmpdir) / "out"
            local_job_dir.mkdir(parents=True, exist_ok=True)

            with (
                patch("peetsfea_runner.remote_job._prepare_remote_workspace"),
                patch("peetsfea_runner.remote_job._upload_supporting_files"),
                patch("peetsfea_runner.remote_job._cleanup_remote_workspace") as cleanup_mock,
                patch("peetsfea_runner.remote_job._extract_local_results_archive"),
                patch("peetsfea_runner.remote_job._submit_remote_workflow_sbatch", return_value="552818"),
                patch(
                    "peetsfea_runner.remote_job._wait_for_remote_sbatch_completion",
                    side_effect=WorkflowError(
                        "worker failed",
                        exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                        stdout="stdout",
                        stderr="stderr",
                    ),
                ),
            ):
                result = run_remote_job_attempt(
                    config=_Cfg(),
                    slot_inputs=[SlotInput(slot_id="slot_0001", input_path=input_path)],
                    remote_job_dir="/tmp/peetsfea/run_01/job_0001/a1",
                    local_job_dir=local_job_dir,
                    session_name="s",
                )

            self.assertFalse(result.success)
            self.assertEqual(result.slurm_job_id, "552818")
            cleanup_mock.assert_called()

    def test_read_remote_optional_text_file_falls_back_to_slurm_output(self) -> None:
        class _Cfg:
            host = "gate1-harry"

        with patch(
            "peetsfea_runner.remote_job._run_completed_process",
            return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="fallback markers", stderr=""),
        ) as mocked_run:
            output = _read_remote_optional_text_file(
                config=_Cfg(),
                remote_path="/tmp/peetsfea/run_01/job_0001",
                filenames=("worker.stdout", "slurm-552818.out"),
                stage="worker stdout",
            )

        self.assertEqual(output, "fallback markers")
        command = mocked_run.call_args.args[0]
        self.assertEqual(command[:4], ["ssh", "-p", "22", "gate1-harry"])
        self.assertIn("if [ -f worker.stdout ]; then cat worker.stdout;", command[4])
        self.assertIn("elif [ -f slurm-552818.out ]; then cat slurm-552818.out;", command[4])

    def test_wait_for_remote_sbatch_completion_uses_slurm_output_when_worker_stdout_missing(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            time_limit = "00:05:00"

        combined_output = (
            "__PEETS_FAILED_COUNT__:0\n"
            "__PEETS_CASE_SUMMARY_BEGIN__\n"
            "case_01:0\n"
            "__PEETS_CASE_SUMMARY_END__\n"
            "__PEETS_RESULTS_TGZ_BEGIN__\n"
            "Zm9v\n"
            "__PEETS_RESULTS_TGZ_END__\n"
        )
        with (
            patch(
                "peetsfea_runner.remote_job.query_slurm_job_state",
                return_value=("COMPLETED", "n115"),
            ),
            patch(
                "peetsfea_runner.remote_job._read_remote_optional_text_file",
                side_effect=[combined_output, ""],
            ) as mocked_read,
        ):
            stdout, stderr, observed_node, state = _wait_for_remote_sbatch_completion(
                _Cfg(),
                remote_job_dir="/tmp/peetsfea/run_01/job_0001",
                slurm_job_id="552818",
            )

        self.assertEqual(stdout, combined_output)
        self.assertEqual(stderr, "")
        self.assertEqual(observed_node, "n115")
        self.assertEqual(state, "COMPLETED")
        first_call = mocked_read.call_args_list[0].kwargs
        self.assertEqual(first_call["filenames"], ("worker.stdout", "slurm-552818.out"))
        second_call = mocked_read.call_args_list[1].kwargs
        self.assertEqual(second_call["filenames"], ("worker.stderr", "slurm-552818.err"))

    def test_wait_for_remote_sbatch_completion_retries_after_transient_probe_failure(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            time_limit = "00:05:00"

        combined_output = (
            "__PEETS_FAILED_COUNT__:0\n"
            "__PEETS_CASE_SUMMARY_BEGIN__\n"
            "case_01:0\n"
            "__PEETS_CASE_SUMMARY_END__\n"
            "__PEETS_RESULTS_TGZ_BEGIN__\n"
            "Zm9v\n"
            "__PEETS_RESULTS_TGZ_END__\n"
        )
        with (
            patch(
                "peetsfea_runner.remote_job.query_slurm_job_state",
                side_effect=[
                    WorkflowError(
                        "squeue failed: Connection closed by 172.16.10.36 port 22",
                        exit_code=EXIT_CODE_SLURM_FAILURE,
                    ),
                    ("RUNNING", "n115"),
                    ("COMPLETED", "n115"),
                ],
            ),
            patch(
                "peetsfea_runner.remote_job._read_remote_optional_text_file",
                side_effect=[combined_output, ""],
            ),
            patch("peetsfea_runner.remote_job.time.sleep") as mocked_sleep,
        ):
            stdout, stderr, observed_node, state = _wait_for_remote_sbatch_completion(
                _Cfg(),
                remote_job_dir="/tmp/peetsfea/run_01/job_0001",
                slurm_job_id="552818",
            )

        self.assertEqual(stdout, combined_output)
        self.assertEqual(stderr, "")
        self.assertEqual(observed_node, "n115")
        self.assertEqual(state, "COMPLETED")
        mocked_sleep.assert_called()

    def test_retryable_transport_capture_retries_connection_closed_once(self) -> None:
        first = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=255,
            stdout="",
            stderr="Connection closed by 172.16.10.36 port 22",
        )
        second = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="552818",
            stderr="",
        )
        with (
            patch(
                "peetsfea_runner.remote_job._run_completed_process_capture",
                side_effect=[first, second],
            ),
            patch("peetsfea_runner.remote_job.time.sleep") as mocked_sleep,
        ):
            completed = _run_completed_process_capture_with_transport_retry(
                ["ssh", "-p", "22", "gate1-harry", "sbatch"],
                stage="sbatch submit",
                exit_code=11,
                timeout_seconds=30,
            )
        self.assertEqual(completed.returncode, 0)
        mocked_sleep.assert_called_once_with(5)

    def test_run_remote_workflow_sbatch_collects_results_after_lost_worker_when_marker_exists(self) -> None:
        class _Cfg:
            host = "gate1-harry"
            time_limit = "00:05:00"

        with (
            patch("peetsfea_runner.remote_job._submit_remote_workflow_sbatch", return_value="552818"),
            patch(
                "peetsfea_runner.remote_job._wait_for_remote_sbatch_completion",
                side_effect=WorkflowError(
                    "sbatch worker failed job_id=552818: LOST",
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                    stdout="worker lost",
                    stderr="",
                    slurm_job_id="552818",
                    observed_node="n115",
                    worker_terminal_state="LOST",
                ),
            ),
            patch(
                "peetsfea_runner.remote_job._probe_remote_completion_artifacts",
                return_value=SimpleNamespace(
                    collect_probe_state="MARKER_PRESENT",
                    marker_present=True,
                    has_results_ready=True,
                    has_results_archive=True,
                    has_case_summary=True,
                    has_failed_count=True,
                ),
            ),
            patch(
                "peetsfea_runner.remote_job._collect_remote_workflow_result_from_probe",
                return_value=SimpleNamespace(
                    case_summary=SimpleNamespace(success_cases=1, failed_cases=0, case_lines=[], success=True),
                    archive_bytes=b"archive",
                    submission=SimpleNamespace(stdout="worker lost", stderr="", combined_output="worker lost"),
                    collect_probe_state="MARKER_PRESENT",
                    marker_present=True,
                ),
            ),
        ):
            result, slurm_job_id, observed_node, terminal_state = _run_remote_workflow_sbatch(
                _Cfg(),
                remote_job_dir="/tmp/peetsfea/run_01/job_0001",
                case_count=1,
            )
        self.assertEqual(slurm_job_id, "552818")
        self.assertEqual(observed_node, "n115")
        self.assertEqual(terminal_state, "LOST")
        self.assertEqual(result.collect_probe_state, "MARKER_PRESENT")
        self.assertTrue(result.marker_present)

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

    def test_slot_count_parser(self) -> None:
        self.assertEqual(_count_screen_slots("0$ case_01 1 case_02 2 case_03"), 3)
        self.assertEqual(_count_screen_slots(""), 0)

    def test_wait_all_and_aggregation_commands_contain_parallel_slot_count(self) -> None:
        self.assertIn("seq 1 8", _build_wait_all_command(8))
        command = _build_case_aggregation_command(8)
        self.assertIn("seq 1 8", command)
        self.assertIn('if [ -f "$case_dir/exit.code" ]; then', command)
        self.assertIn('code=97; echo "$code" > "$case_dir/exit.code";', command)

    def test_case_slot_command_contains_case_name_and_core_count(self) -> None:
        command = _build_case_slot_command(case_index=3, cores_per_slot=4, tasks_per_slot=1)
        self.assertIn("case_03", command)
        self.assertIn("PEETS_SLOT_CORES=4", command)
        self.assertIn("PEETS_SLOT_TASKS=1", command)

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

    def test_failure_category_taxonomy(self) -> None:
        self.assertEqual(
            _categorize_failure(exit_code=EXIT_CODE_REMOTE_RUN_FAILURE, message="preflight failed", failed_case_lines=[]),
            "readiness",
        )
        self.assertEqual(
            _categorize_failure(exit_code=EXIT_CODE_DOWNLOAD_FAILURE, message="archive decode failed", failed_case_lines=[]),
            "collect",
        )
        self.assertEqual(
            _categorize_failure(
                exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
                message="cleanup failed",
                failed_case_lines=[],
            ),
            "cleanup",
        )
        self.assertEqual(
            _categorize_failure(exit_code=EXIT_CODE_REMOTE_RUN_FAILURE, message="failed", failed_case_lines=["case_01:1"]),
            "solve",
        )
        self.assertEqual(
            _categorize_failure(exit_code=EXIT_CODE_REMOTE_RUN_FAILURE, message="ssh failed", failed_case_lines=[]),
            "launch",
        )
        self.assertEqual(
            _categorize_failure(
                exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                message="sbatch submit failed: Connection closed by 172.16.10.36 port 22",
                failed_case_lines=[],
            ),
            "launch_transient",
        )

    def test_tmp_remote_root_is_scoped_per_remote_user(self) -> None:
        class _Cfg:
            host = "gate1-dhj02"

        with patch("peetsfea_runner.remote_job._get_remote_home", return_value="/home/dhj02"):
            resolved = _resolve_remote_path(
                config=_Cfg(),
                path="/tmp/peetsfea-runner/20260306_115056/account_02/job_0011/a2",
            )
        self.assertEqual(
            resolved,
            "/tmp/dhj02/peetsfea-runner/20260306_115056/account_02/job_0011/a2",
        )

    def test_slurm_batch_tmp_remote_root_is_promoted_to_shared_home_path(self) -> None:
        class _Cfg:
            host = "gate1-dhj02"
            scheduler = "slurm"
            remote_execution_backend = "slurm_batch"

        with patch("peetsfea_runner.remote_job._get_remote_home", return_value="/home/dhj02"):
            resolved = _resolve_remote_path(
                config=_Cfg(),
                path="/tmp/peetsfea-runner/20260306_115056/account_02/job_0011/a2",
            )
        self.assertEqual(
            resolved,
            "/home/dhj02/aedt_runs/20260306_115056/account_02/job_0011/a2",
        )


if __name__ == "__main__":
    unittest.main()
