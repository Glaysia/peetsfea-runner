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
    _build_enroot_remote_job_script_content,
    _build_pull_remote_sbatch_script_content,
    _build_pull_worker_payload_script_content,
    _build_worker_payload_script_content,
    _build_remote_sbatch_script_content,
    _build_windows_remote_dispatch_script_content,
    _build_windows_remote_job_script_content,
    _parse_sbatch_job_id,
    _parse_squeue_state_line,
    _read_remote_optional_text_file,
    _scp_command,
    _ssh_command,
    _run_completed_process_capture_with_transport_retry,
    _run_remote_workflow_sbatch,
    _wait_for_remote_sbatch_completion,
    query_slurm_job_state,
    run_remote_job_attempt,
)


class TestPlan03Workflow(unittest.TestCase):
    def test_remote_script_contains_required_environment_setup(self) -> None:
        content = _build_remote_job_script_content()
        self.assertIn("export ANSYSEM_ROOT252=/mnt/AnsysEM", content)
        self.assertIn("export LANG=C.UTF-8", content)
        self.assertIn("export LC_ALL=C.UTF-8", content)
        self.assertIn("unset LANGUAGE", content)
        self.assertIn("export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81", content)
        self.assertNotIn("TMP_SHARED_ROOT=", content)
        self.assertIn('export PEETS_RAMDISK_ROOT="${PEETS_RAMDISK_ROOT:-$HOME/ansys_ram}"', content)
        self.assertIn('export PEETS_DISK_TMPDIR="${PEETS_DISK_TMPDIR:-$PEETS_DISK_ROOT/tmp}"', content)
        self.assertIn('export ANSYS_WORK_DIR="${ANSYS_WORK_DIR:-$PEETS_RAMDISK_ANSYS_WORK_DIR}"', content)
        self.assertIn('export TMPDIR="${TMPDIR:-$PEETS_DISK_TMPDIR}"', content)
        self.assertIn("IMAGE_PYTHON=\"/opt/miniconda3/bin/python\"", content)
        self.assertIn("IMAGE_LD_LIBRARY_PATH=\"$BASE_PREFIX/lib:${LD_LIBRARY_PATH:-}\"", content)
        self.assertIn('LD_LIBRARY_PATH="$IMAGE_LD_LIBRARY_PATH" "$IMAGE_PYTHON" run_sim.py', content)
        self.assertIn("export PEETS_ORIGINAL_LD_LIBRARY_PATH=\"${LD_LIBRARY_PATH:-}\"", content)
        self.assertIn("        'ANSYS_WORK_DIR',", content)
        self.assertIn("    os.environ['ANSYS_WORK_DIR'] = str(ansys_work_dir)", content)
        self.assertIn("    os.environ['TEMP'] = str(tmpdir)", content)
        self.assertIn("os.environ['TMPDIR'] = str(tmpdir)", content)
        self.assertIn("ANSYS_EXECUTABLE: str = '/mnt/AnsysEM/ansysedt'", content)
        self.assertIn("def get_available_port(host: str = '127.0.0.1') -> int:", content)
        self.assertIn("def build_ansys_launch_env() -> dict[str, str]:", content)
        self.assertIn("def launch_ansys_grpc_server(port: int) -> subprocess.Popen:", content)
        self.assertIn("cmd = [ANSYS_EXECUTABLE, '-ng', '-waitforlicense', '-grpcsrv', str(port)]", content)
        self.assertIn("launch_env = build_ansys_launch_env()", content)
        self.assertIn("original_ld_library_path = os.environ.get('PEETS_ORIGINAL_LD_LIBRARY_PATH', '').strip()", content)
        self.assertIn("launch_env.pop('LD_LIBRARY_PATH', None)", content)
        self.assertIn("runtime_cwd = Path(os.environ.get('TMPDIR', str(Path.cwd() / 'tmp'))).resolve()", content)
        self.assertIn("process = subprocess.Popen(cmd, cwd=str(runtime_cwd), env=launch_env, stdout=stdout_handle, stderr=stderr_handle)", content)
        self.assertIn("from contextlib import contextmanager", content)
        self.assertIn("from typing import Any", content)
        self.assertIn("from ansys.aedt.core import Desktop, Hfss, Icepak, Maxwell3d, Q2d, Q3d, settings", content)
        self.assertIn("settings.wait_for_license = True", content)
        self.assertIn("settings.skip_license_check = False", content)
        self.assertIn("settings.aedt_environment_variables[env_key] = env_value", content)
        self.assertIn("def use_ansys_launch_env() -> object:", content)
        self.assertIn("with use_ansys_launch_env():", content)
        self.assertIn("def connect_grpc_desktop(grpc_port: int, *, close_on_exit: bool) -> Desktop:", content)
        self.assertIn("def release_desktop_session(handle: Any, *, close_projects: bool, close_desktop: bool) -> None:", content)
        self.assertIn("{'close_projects': close_projects, 'close_desktop': close_desktop}", content)
        self.assertIn("{'close_projects': close_projects, 'close_on_exit': close_desktop}", content)
        self.assertIn("def parse_top_design_names(project: Any) -> list[str]:", content)
        self.assertIn("for raw_name in list(project.GetTopDesignList()):", content)
        self.assertIn("def open_existing_project(desktop: Desktop, project_file: Path) -> tuple[str, Any]:", content)
        self.assertIn("project = desktop.odesktop.OpenProject(str(project_file.resolve()))", content)
        self.assertIn("project = desktop.odesktop.SetActiveProject(expected_project_name)", content)
        self.assertIn("def discover_hfss_design_names(project: Any, project_file: Path) -> tuple[list[str], list[str]]:", content)
        self.assertIn("project_text = project_file.read_text(encoding='utf-8', errors='ignore')", content)
        self.assertIn("discovered_designs = parse_top_design_names(project)", content)
        self.assertIn("normalized_name = normalize_design_factory(design_name)", content)
        self.assertIn("normalized_factory = normalize_design_factory(resolve_design_factory(project_text, design_name))", content)
        self.assertIn("if normalized_factory.startswith('HFSS') or normalized_name.startswith('HFSS'):", content)
        self.assertIn("def parse_top_design_entries(project: Any) -> list[tuple[str, str]]:", content)
        self.assertIn("raw_text = str(raw_name).strip()", content)
        self.assertIn("entries.append((raw_text or design_name, design_name))", content)
        self.assertIn("def resolve_design_identifier(project: Any, design_name: str) -> str:", content)
        self.assertIn("def normalize_design_factory(factory: str) -> str:", content)
        self.assertIn("def extract_design_metadata_block(project_text: str, design_name: str) -> str:", content)
        self.assertIn("def resolve_design_factory(project_text: str, design_name: str) -> str:", content)
        self.assertIn("import re", content)
        self.assertIn("def parse_named_setup_list(block_text: str, label: str) -> list[str]:", content)
        self.assertIn(
            "def extract_design_setup_names(project_text: str, design_name: str) -> tuple[list[str], list[str]]:",
            content,
        )
        self.assertIn(
            "def open_project_for_session_solve(project_file: Path, grpc_port: int) -> tuple[Desktop, str, str, list[str]]:",
            content,
        )
        self.assertIn("hfss_design_names, discovered_designs = discover_hfss_design_names(project, project_file)", content)
        self.assertIn("hfss_design_identifier = resolve_design_identifier(project, hfss_design_names[0])", content)
        self.assertIn("def build_design_app(project_reference: str, design_name: str | None, design_factory: str, grpc_port: int) -> Any:", content)
        self.assertIn("'project': project_reference,", content)
        self.assertIn("if design_name:", content)
        self.assertIn("common_kwargs['design'] = design_name", content)
        self.assertIn("'machine': 'localhost',", content)
        self.assertIn("return Hfss(**common_kwargs)", content)
        self.assertIn("return Maxwell3d(**common_kwargs)", content)
        self.assertIn(
            "def validate_design_app_binding(app: Any, expected_project_name: str, expected_design_name: str) -> tuple[str, str, bool]:",
            content,
        )
        self.assertIn(
            "def activate_project_design(desktop: Desktop, project_name: str, design_identifier: str, design_name: str) -> tuple[Any, Any, str]:",
            content,
        )
        self.assertIn("design = project.SetActiveDesign(candidate)", content)
        self.assertIn("design = project.GetDesign(candidate)", content)
        self.assertIn(
            "def attach_design_app(desktop: Desktop, project_name: str, project_file: Path, design_identifier: str, design_name: str, design_factory: str, grpc_port: int) -> Any:",
            content,
        )
        self.assertIn("for candidate_project_ref in (project_name,):", content)
        self.assertIn("app = build_design_app(project_reference, requested_design_name, design_factory, grpc_port)", content)
        self.assertIn("active_project_name, bound_design_name, matches = validate_design_app_binding(", content)
        self.assertIn("_project, _design, active_design_name = activate_project_design(", content)
        self.assertIn("requested_design_names: list[str | None] = [None]", content)
        self.assertIn("app = build_design_app(project_name, requested_design_name, design_factory, grpc_port)", content)
        self.assertIn("release_desktop_session(app, close_projects=False, close_desktop=False)", content)
        self.assertIn(
            "def attach_hfss_to_open_project(desktop: Desktop, project_name: str, project_file: Path, design_identifier: str, design_name: str, grpc_port: int) -> Any:",
            content,
        )
        self.assertIn("for candidate_project_ref in (project_name,):", content)
        self.assertIn("for candidate_design_name in (design_name, str(design_identifier).rsplit(';', 1)[-1].strip()):", content)
        self.assertIn("app = Hfss(", content)
        self.assertIn("project=project_reference,", content)
        self.assertIn("design=requested_design_name,", content)
        self.assertIn("if matches or bound_design_name == design_name:", content)
        self.assertIn("active_project_name, bound_design_name, _matches = validate_design_app_binding(", content)
        self.assertIn("if active_project_name == project_name:", content)
        self.assertIn("Bound AEDT session does not support solve_in_batch", content)
        self.assertIn("Session-level HFSS attach did not match request project=", content)
        self.assertIn("if last_error is None:", content)
        self.assertIn("for requested_design_name in (active_design_name, design_name):", content)
        self.assertIn("_project, _design, active_design_name = activate_project_design(", content)
        self.assertIn("if not hasattr(app, 'solve_in_batch'):", content)
        self.assertIn("Failed to bind HFSS to open AEDT project=", content)
        self.assertIn("def reopen_solved_project(project_file: Path, grpc_port: int, project_name: str | None = None) -> Desktop:", content)
        self.assertIn("desktop = connect_grpc_desktop(grpc_port, close_on_exit=False)", content)
        self.assertIn("active_project = desktop.odesktop.SetActiveProject(project_name)", content)
        self.assertIn("_project_name, _project = open_existing_project(desktop, project_file)", content)
        self.assertIn("grpc_port = get_available_port()", content)
        self.assertIn("ansys_process = None", content)
        self.assertIn("solve_desktop = None", content)
        self.assertIn("ansys_process = launch_ansys_grpc_server(grpc_port)", content)
        self.assertIn(
            "solve_desktop, solve_project_name, solve_design_identifier, discovered_designs = open_project_for_session_solve(",
            content,
        )
        self.assertIn("solve_design_name = str(solve_design_identifier).rsplit(';', 1)[-1].strip()", content)
        self.assertIn("f'[INFO] session solve prepared project={solve_project_name!r} '", content)
        self.assertIn("f'design={solve_design_name!r} discovered_designs={discovered_designs!r}'", content)
        self.assertIn("solve_hfss = attach_hfss_to_open_project(", content)
        self.assertIn("solve_desktop = None", content)
        self.assertIn("desktop_class = getattr(solve_hfss, 'desktop_class', None)", content)
        self.assertIn("change_license_type = getattr(desktop_class, 'change_license_type', None)", content)
        self.assertIn("change_license_type('Pool')", content)
        self.assertIn("close_project = getattr(solve_hfss, 'close_project', None)", content)
        self.assertIn("if callable(close_project):", content)
        self.assertIn("close_project(save=False)", content)
        self.assertIn("solve_hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", content)
        self.assertIn("release_desktop_session(solve_hfss, close_projects=True, close_desktop=False)", content)
        self.assertIn("release_desktop_session(solve_desktop, close_projects=True, close_desktop=False)", content)
        self.assertNotIn("solve_hfss = Hfss(", content)
        self.assertIn("reopened_desktop = reopen_solved_project(project_file, grpc_port, solve_project_name)", content)
        self.assertIn("extract_design_reports(reopened_desktop, workdir, project_file, grpc_port)", content)
        self.assertIn("stop_process(ansys_process)", content)
        self.assertIn("EMIT_OUTPUT_VARIABLES_CSV: bool = True", content)
        self.assertIn("REPORT_EXPORT_ERROR_LOG_NAME: str = 'report_export.error.log'", content)
        self.assertIn("LICENSE_DIAGNOSTICS_NAME: str = 'license_diagnostics.txt'", content)
        self.assertIn("DESIGN_OUTPUTS_DIR_NAME: str = 'design_outputs'", content)
        self.assertIn("def iter_project_design_apps(desktop: Desktop, project_file: Path, grpc_port: int) -> list[tuple[str, Any]]:", content)
        self.assertIn("def extract_design_reports(desktop: Desktop, workdir: Path, project_file: Path, grpc_port: int) -> None:", content)
        self.assertIn("def get_all_report_names(app: Any) -> list[str]:", content)
        self.assertIn("def create_output_variables_report(app: Any) -> str | None:", content)
        self.assertIn("def create_input_parameter_report(app: Any) -> str | None:", content)
        self.assertIn("def create_parameter_reports(app: Any) -> list[str]:", content)
        self.assertIn("def design_dir_name(design_name: str) -> str:", content)
        self.assertIn("def reports_dir_for_design_name(workdir: Path, design_name: str) -> Path:", content)
        self.assertIn("def reports_dir_for_design(workdir: Path, app: Any) -> Path:", content)
        self.assertIn("def export_all_reports(app: Any, workdir: Path, design_name: str) -> tuple[dict[str, Path], list[str]]:", content)
        self.assertIn("reports_dir = design_output_dir(workdir, design_name) / 'reports'", content)
        self.assertIn("app.post.export_report_to_csv", content)
        self.assertIn("def write_design_output_error_log(workdir: Path, design_name: str, errors: list[str]) -> Path:", content)
        self.assertIn("SYNTHETIC_REPORT_FILENAMES: set[str] = {'peetsfea_input_parameters.csv'}", content)
        self.assertIn("def count_design_report_files(workdir: Path, design_name: str, *, include_synthetic: bool = True) -> int:", content)
        self.assertIn("def count_synthetic_design_report_files(workdir: Path, design_name: str) -> int:", content)
        self.assertIn("def classify_design_report_status(*, native_report_count: int, synthetic_report_count: int, errors: list[str]) -> str:", content)
        self.assertIn("def write_design_outputs_manifest(workdir: Path, rows: list[dict[str, object]]) -> None:", content)
        self.assertIn("ordered_columns = ['design_name', 'reports_dir', 'report_count', 'native_report_count', 'synthetic_report_count', 'status', 'error_log']", content)
        self.assertIn("(workdir / DESIGN_OUTPUTS_DIR_NAME).mkdir(parents=True, exist_ok=True)", content)
        self.assertIn("write_design_outputs_manifest(workdir, design_output_manifest_rows)", content)
        self.assertIn("error_log_path.unlink()", content)
        self.assertIn("(workdir / REPORT_EXPORT_ERROR_LOG_NAME).write_text(str(exc), encoding='utf-8')", content)
        self.assertIn("existing_runtime_logs = snapshot_pyaedt_logs()", content)
        self.assertIn("def run_command_capture(command: list[str], *, env: dict[str, str] | None = None, timeout_seconds: int = 30) -> tuple[int, str]:", content)
        self.assertIn("def write_license_diagnostics(workdir: Path) -> None:", content)
        self.assertIn("('ansysli_util -checkexists EMAG', [str(ansysli_util), '-checkexists', 'EMAG'], 30),", content)
        self.assertIn("('ansysli_util -checkexists PREPPOST', [str(ansysli_util), '-checkexists', 'PREPPOST'], 30),", content)
        self.assertIn("'lmutil lmstat -a -c 1055@172.16.10.81',", content)
        self.assertIn("write_license_diagnostics(workdir)", content)
        self.assertIn("copy_runtime_logs(workdir, existing_runtime_logs)", content)
        self.assertIn("Path('exit.code').write_text(str(exit_code), encoding='utf-8')", content)
        self.assertNotIn("cores=[cores]", content)
        self.assertNotIn("tasks=[tasks]", content)
        self.assertIn('LD_LIBRARY_PATH="$IMAGE_LD_LIBRARY_PATH" "$IMAGE_PYTHON" -m uv --version', content)
        self.assertIn('import ansys.aedt.core, pandas, pyvista', content)
        self.assertNotIn("def open_project_session(project_file: Path) -> Hfss:", content)
        self.assertNotIn("DEFAULT_DESIGN_NAME: str | None = os.environ.get('PEETS_AEDT_DESIGN_NAME', 'HFSSDesign1').strip() or None", content)
        self.assertNotIn("design=DEFAULT_DESIGN_NAME", content)
        self.assertNotIn("extract_hfss = open_project_session(project_file)", content)
        self.assertNotIn("solved = extract_hfss.analyze(", content)
        self.assertNotIn("solve_in_batch=False", content)
        self.assertNotIn("blocking=True", content)
        self.assertNotIn("def discover_batch_solve_targets(project_file: Path, hfss_design_names: list[str]) -> list[str]:", content)
        self.assertNotIn("def open_project_for_batch_solve(project_file: Path, grpc_port: int) -> tuple[Desktop, str, str, list[str], list[str]]:", content)
        self.assertNotIn("def wait_for_path(path: Path, *, timeout_seconds: float, process: subprocess.Popen | None = None) -> None:", content)
        self.assertNotIn("def wait_for_any_path(paths: tuple[Path, ...], *, timeout_seconds: float, process: subprocess.Popen | None = None) -> Path:", content)
        self.assertNotIn("def run_batch_solve(project_file: Path, solve_targets: list[str], *, cores: int, tasks: int) -> None:", content)
        self.assertNotIn("def solve_project_in_batch(project_file: Path, grpc_port: int, *, cores: int, tasks: int) -> None:", content)
        self.assertNotIn("solve_hfss = Hfss(project=str(project_file.resolve())", content)
        self.assertNotIn("app = desktop[[project_name, candidate]]", content)
        self.assertNotIn("desktop.load_project(str(project_file.resolve()))", content)
        self.assertNotIn("design_type = str(design.GetDesignType())", content)
        self.assertNotIn("f'[WARN] explicit design attach failed; falling back to session-level Hfss solve '", content)
        self.assertIn("solve_hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", content)
        self.assertNotIn("OUTPUT_VARIABLES_CSV_NAME: str = 'output_variables.csv'", content)
        self.assertNotIn("def write_design_output_variables_csv(project_file: Path, workdir: Path, design_name: str, design_row: dict[str, object]) -> Path:", content)
        self.assertNotIn("def write_output_variables_csv(workdir: Path, row: dict[str, object]) -> None:", content)

    def test_remote_script_can_disable_output_variables_csv_export(self) -> None:
        content = _build_remote_job_script_content(emit_output_variables_csv=False)
        self.assertIn("EMIT_OUTPUT_VARIABLES_CSV: bool = False", content)
        self.assertIn("if not EMIT_OUTPUT_VARIABLES_CSV:", content)

    def test_remote_script_merges_nominal_variations_into_csv_row_before_outputs(self) -> None:
        content = _build_remote_job_script_content()
        self.assertIn("import json", content)
        self.assertIn("def get_nominal_variation_values(app: Any) -> dict[str, object]:", content)
        self.assertIn("def build_variable_manager_row(app: Any) -> dict[str, object]:", content)
        self.assertIn("def serialize_variable_manager_value(variable: object) -> object:", content)
        self.assertIn("def build_output_variable_row_variations(app: Any) -> dict[str, object]:", content)
        self.assertIn("if name == 'Freq':", content)
        self.assertIn("row[str(name)] = serialize_row_value(value)", content)
        self.assertIn("row.setdefault(key, value)", content)
        self.assertIn("def write_synthetic_input_report(workdir: Path, design_name: str, app: Any) -> Path:", content)
        self.assertIn("reports_dir = reports_dir_for_design_name(workdir, design_name)", content)
        self.assertIn("write_synthetic_input_report(workdir, design_name, app)", content)
        self.assertIn("export_errors.append(f'report export failed report={report_name}: {exc}')", content)
        self.assertIn("native_report_count = count_design_report_files(workdir, design_name, include_synthetic=False)", content)
        self.assertIn("synthetic_report_count = count_synthetic_design_report_files(workdir, design_name)", content)
        self.assertIn("merge_report_rows(", content)
        self.assertLess(
            content.index("def build_output_variable_row_variations(app: Any) -> dict[str, object]:"),
            content.index("def write_synthetic_input_report(workdir: Path, design_name: str, app: Any) -> Path:"),
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
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"

        content = _build_remote_dispatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/job_0001",
            case_count=2,
        )
        self.assertIn('srun -D "$HOME/aedt_runs/_runtime" -p cpu2 -N 1 -n 1 -c 16 --mem=960G --time=05:00:00 --exclude=n108,n109', content)
        self.assertNotIn("screen -dmS", content)
        self.assertNotIn("srun --pty", content)
        self.assertIn("__PEETS_FAILED_COUNT__", content)
        self.assertIn("max_parallel=4", content)
        self.assertIn("export REMOTE_JOB_DIR", content)
        self.assertIn("wait -n \"${case_pids[@]}\" || true", content)
        self.assertIn('if PEETS_SLOT_CORES=4 PEETS_SLOT_TASKS=1 run_case_command "$case_dir_path" > run.log 2>&1; then', content)
        self.assertIn("echo \"$rc\" > exit.code", content)
        self.assertIn('archive_path=$(mktemp "$REMOTE_RUNTIME_ROOT/results.${SLURM_JOB_ID:-nojob}.XXXXXX.tgz")', content)
        self.assertIn('tar -czf "$archive_path" case_* case_summary.txt failed.count', content)
        self.assertNotIn("tar --exclude='.venv' --exclude='results.tgz' --exclude='.env_initialized' -czf results.tgz .", content)

    def test_worker_payload_can_refill_pool_with_prefetched_cases(self) -> None:
        class _Cfg:
            slots_per_job = 4
            cores_per_slot = 4
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "harrypc"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"

        content = _build_worker_payload_script_content(
            config=_Cfg(),
            case_count=8,
            run_id="run_01",
            worker_id="attempt_0001",
        )
        self.assertIn("max_parallel=4", content)
        self.assertIn("for i in $(seq 1 8); do", content)
        self.assertIn("wait -n \"${case_pids[@]}\" || true", content)
        self.assertIn("ssh \"${ssh_args[@]}\" -M -S \"$PEETS_TUNNEL_SOCKET\" -fnNT", content)
        self.assertIn("-p \"$PEETS_CONTROL_RETURN_PORT\"", content)
        self.assertIn("-o StrictHostKeyChecking=no", content)
        self.assertIn("-o UserKnownHostsFile=/dev/null", content)
        self.assertIn("PEETS_CONTROL_SSH_IDENTITY=\"${PEETS_CONTROL_SSH_IDENTITY:-}\"", content)
        self.assertIn("ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")", content)
        self.assertIn("PEETS_CONTROL_LOCAL_PORT=$((PEETS_CONTROL_PORT + 1000 + (${SLURM_JOB_ID:-0} % 1000)))", content)
        self.assertIn("classify_return_path_stage()", content)
        self.assertIn("RETURN_PATH_DNS_FAILURE", content)
        self.assertIn("export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}", content)
        self.assertIn("launch_probe_file=\"${REMOTE_JOB_DIR:-$workdir}/launch_probe.txt\"", content)
        self.assertIn("printf 'tool.%s=%s\\n' \"$tool\" \"${resolved_tool:-MISSING}\"", content)
        self.assertNotIn("rm -rf \"$REMOTE_JOB_DIR\" >/dev/null 2>&1 || true", content)

    def test_pull_worker_payload_requests_leases_and_uploads_per_slot_artifacts(self) -> None:
        class _Cfg:
            slots_per_job = 48
            slot_min_concurrency = 5
            slot_max_concurrency = 48
            slot_memory_pressure_high_watermark_percent = 90
            slot_memory_pressure_resume_watermark_percent = 80
            slot_memory_probe_interval_seconds = 5
            lease_ttl_seconds = 120
            lease_heartbeat_seconds = 15
            worker_idle_poll_seconds = 10
            slot_request_backoff_seconds = 5
            cores_per_slot = 4
            tasks_per_slot = 1
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "harrypc"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30
            mem = "288G"
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"
            remote_ansys_executable = "/mnt/AnsysEM/ansysedt"
            host = "gate1-harry261"
            remote_root = "~/aedt_runs"
            partition = "cpu2"
            slurm_partitions_allowlist = ("cpu2",)
            nodes = 1
            ntasks = 1
            cpus_per_job = 48
            time_limit = "05:00:00"
            platform = "linux"
            scheduler = "slurm"
            remote_execution_backend = "slurm_batch"
            remote_ssh_port = 22
            ssh_config_path = ""
            worker_payload_slot_limit = 48
            slurm_exclude_nodes = ()

        content = _build_pull_worker_payload_script_content(
            config=_Cfg(),
            run_id="run_01",
            worker_id="worker_01",
        )
        self.assertIn("/internal/leases/request", content)
        self.assertIn("/internal/leases/input?run_id=${PEETS_CONTROL_RUN_ID}&lease_token=${lease_token}", content)
        self.assertIn("/internal/leases/artifact?run_id=${PEETS_CONTROL_RUN_ID}&lease_token=${lease_token}", content)
        self.assertIn("/internal/leases/complete", content)
        self.assertIn("/internal/leases/fail", content)
        self.assertIn("PEETS_LEASE_HEARTBEAT_INTERVAL=15", content)
        self.assertIn("PEETS_WORKER_IDLE_POLL=10", content)
        self.assertIn("PEETS_SLOT_REQUEST_BACKOFF=5", content)
        self.assertIn("if [ \"$active_now\" -lt \"$PEETS_SLOT_MIN_CONCURRENCY\" ]; then", content)
        self.assertIn("PEETS_CONTROL_SSH_IDENTITY=\"${PEETS_CONTROL_SSH_IDENTITY:-}\"", content)
        self.assertIn("ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")", content)
        self.assertIn("PEETS_CONTROL_LOCAL_PORT=$((PEETS_CONTROL_PORT + 1000 + (${SLURM_JOB_ID:-0} % 1000)))", content)
        self.assertNotIn("project_01.aedt", content)
        self.assertNotIn("for i in $(seq 1 ", content)

    def test_pull_remote_sbatch_only_stages_scripts(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
            remote_root = "~/aedt_runs"
            partition = "cpu2"
            slurm_partitions_allowlist = ("cpu2",)
            nodes = 1
            ntasks = 1
            cpus_per_job = 48
            mem = "288G"
            time_limit = "05:00:00"
            slots_per_job = 48
            worker_payload_slot_limit = 48
            slot_min_concurrency = 5
            slot_max_concurrency = 48
            slot_memory_pressure_high_watermark_percent = 90
            slot_memory_pressure_resume_watermark_percent = 80
            slot_memory_probe_interval_seconds = 5
            lease_ttl_seconds = 120
            lease_heartbeat_seconds = 15
            worker_idle_poll_seconds = 10
            slot_request_backoff_seconds = 5
            cores_per_slot = 4
            tasks_per_slot = 1
            platform = "linux"
            scheduler = "slurm"
            remote_execution_backend = "slurm_batch"
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "harrypc"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "peetsmain"
            tunnel_heartbeat_timeout_seconds = 90
            tunnel_recovery_grace_seconds = 30
            remote_ssh_port = 22
            ssh_config_path = ""
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"
            remote_ansys_executable = "/mnt/AnsysEM/ansysedt"
            slurm_exclude_nodes = ()

        content = _build_pull_remote_sbatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/worker_01",
            run_id="run_01",
            worker_id="worker_01",
        )
        self.assertIn("remote_pull_worker_payload.sh", content)
        self.assertNotIn("project_*.aedt", content)
        self.assertIn("/tmp/peetsfea-runner/submit", content)
        self.assertIn("./remote_pull_worker_payload.sh > worker.stdout 2> worker.stderr", content)
        self.assertIn("upload_debug_back() {", content)
        self.assertIn("launch_probe.txt worker.stdout worker.stderr control_tunnel_bootstrap.err", content)
        self.assertIn("tar -czf - remote_job.sh remote_pull_worker_payload.sh", content)
        self.assertIn("stage_control_ssh_identity() {", content)
        self.assertIn("export PEETS_CONTROL_SSH_IDENTITY=\"$staged_dir/id_control\"", content)
        self.assertNotIn("results.tgz", content)
        self.assertIn("PEETS_CONTROL_WORKER_ID=worker_01", content)
        self.assertIn("PEETS_CONTROL_RUN_ID=run_01", content)

    def test_enroot_worker_payload_uses_cd_then_mount_contract(self) -> None:
        class _Cfg:
            slots_per_job = 4
            cores_per_slot = 4
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "peetsmain@172.16.165.146"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt-ubuntu2404-pyaedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"
            remote_ansys_executable = "/mnt/AnsysEM/ansysedt"

        content = _build_worker_payload_script_content(
            config=_Cfg(),
            case_count=3,
            run_id="run_02",
            worker_id="attempt_0002",
        )

        self.assertIn("PEETS_REMOTE_CONTAINER_RUNTIME=enroot", content)
        self.assertIn('REMOTE_HOST_ANSYS_ROOT="/opt/ohpc/pub/Electronics/v252/AnsysEM"', content)
        self.assertIn('REMOTE_HOST_ANSYS_BASE="/opt/ohpc/pub/Electronics/v252"', content)
        self.assertIn('enroot_base="$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}/${case_name}-$$"', content)
        self.assertIn('(\n    container_name="peets-${SLURM_JOB_ID:-nojob}-${case_name}-$$"', content)
        self.assertIn('container_name="peets-${SLURM_JOB_ID:-nojob}-${case_name}-$$"', content)
        self.assertIn('enroot create -f -n "$container_name" "$REMOTE_CONTAINER_IMAGE" >/dev/null', content)
        self.assertIn('JOB_TMPFS_ROOT="$workdir/job_tmpfs"', content)
        self.assertIn('JOB_DISK_ROOT="$workdir/job_disk"', content)
        self.assertIn('JOB_DISK_BUDGET_GB=90', content)
        self.assertIn('exec unshare --user --map-root-user --mount /bin/bash -c', content)
        self.assertIn('mount -t tmpfs -o size=100G tmpfs "$job_tmpfs_root"', content)
        self.assertIn('case_ram_root="$JOB_TMPFS_ROOT/$case_name"', content)
        self.assertIn('case_disk_root="$JOB_DISK_ROOT/$case_name"', content)
        self.assertIn('enroot start --root --rw --mount "$REMOTE_HOST_ANSYS_ROOT:/mnt/AnsysEM" --mount "$REMOTE_HOST_ANSYS_BASE:/ansys_inc/v252" --mount "$case_dir:/work" --mount "$JOB_TMPFS_ROOT:/job_tmpfs" --mount "$JOB_DISK_ROOT:/job_disk"', content)
        self.assertIn('mkdir -p \\"$case_ram_root/tmp\\" \\"$case_ram_root/ansys_tmp\\" \\"$case_disk_root/home\\" \\"$case_disk_root/tmp\\" \\"$case_disk_root/ansys_tmp\\"', content)
        self.assertIn('export HOME=\\"$case_disk_root/home\\"', content)
        self.assertIn('export PEETS_DISK_ROOT=\\"$case_disk_root\\"', content)
        self.assertIn('export PEETS_DISK_TMPDIR=\\"$case_disk_root/tmp\\"', content)
        self.assertIn('export PEETS_DISK_ANSYS_WORK_DIR=\\"$case_disk_root/ansys_tmp\\"', content)
        self.assertIn('export ANSYS_WORK_DIR=\\"$case_ram_root/ansys_tmp\\"', content)
        self.assertIn('export TEMP=\\"$case_disk_root/tmp\\"', content)
        self.assertIn('export TMPDIR=\\"$case_disk_root/tmp\\"', content)
        self.assertIn('export XDG_CONFIG_HOME=\\"$case_disk_root/home/.config\\"', content)
        self.assertIn("export ANS_IGNOREOS=1", content)
        self.assertIn('cp -f remote_job.sh case_01/remote_job.sh', content)
        self.assertIn('run_case_command "$case_dir_path" > run.log 2>&1', content)
        self.assertIn('case_name="$(basename "$case_dir")"', content)
        self.assertIn('mkdir -p "$REMOTE_JOB_DIR/$case_name"', content)
        self.assertIn('license_diagnostics.txt', content)
        self.assertIn("cleanup_workdir() {", content)
        self.assertIn("teardown_control_plane() {", content)
        self.assertIn("teardown_control_plane", content)
        self.assertIn("rmdir \"$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}\" >/dev/null 2>&1 || true", content)
        self.assertEqual(content.count("trap cleanup EXIT"), 1)
        self.assertNotIn("trap teardown_control_plane EXIT", content)

    def test_enroot_remote_job_script_uses_image_python_and_mounted_ansys(self) -> None:
        class _Cfg:
            emit_output_variables_csv = True
            remote_container_runtime = "enroot"
            remote_ansys_executable = "/mnt/AnsysEM/ansysedt"

        content = _build_enroot_remote_job_script_content(config=_Cfg())

        self.assertIn("export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81", content)
        self.assertIn("export ANSYSEM_ROOT252=/mnt/AnsysEM", content)
        self.assertNotIn("module load ansys-electronics/v252", content)
        self.assertNotIn("ensure_miniconda()", content)
        self.assertIn("IMAGE_PYTHON=\"/opt/miniconda3/bin/python\"", content)
        self.assertIn('LD_LIBRARY_PATH="$IMAGE_LD_LIBRARY_PATH" "$IMAGE_PYTHON" run_sim.py', content)
        self.assertIn("ANSYS_EXECUTABLE: str = '/mnt/AnsysEM/ansysedt'", content)
        self.assertIn("launch_ansys_grpc_server(grpc_port)", content)
        self.assertIn("project = desktop.odesktop.OpenProject(str(project_file.resolve()))", content)
        self.assertIn("if normalized_factory.startswith('HFSS') or normalized_name.startswith('HFSS'):", content)
        self.assertIn(
            "solve_desktop, solve_project_name, solve_design_identifier, discovered_designs = open_project_for_session_solve(",
            content,
        )
        self.assertIn("solve_hfss = attach_hfss_to_open_project(", content)
        self.assertIn("close_project = getattr(solve_hfss, 'close_project', None)", content)
        self.assertIn("close_project(save=False)", content)
        self.assertIn("solve_hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", content)
        self.assertIn("release_desktop_session(solve_hfss, close_projects=True, close_desktop=False)", content)
        self.assertIn("app = build_design_app(project_reference, requested_design_name, design_factory, grpc_port)", content)
        self.assertIn("design = project.SetActiveDesign(candidate)", content)
        self.assertIn("for design_identifier, design_name in parse_top_design_entries(project):", content)
        self.assertIn("design_factory = resolve_design_factory(project_text, design_name)", content)
        self.assertIn("app = attach_design_app(", content)
        self.assertIn("desktop, project_name, project_file, design_identifier, design_name, design_factory, grpc_port", content)
        self.assertIn("with use_ansys_launch_env():", content)
        self.assertIn("f'[INFO] session solve prepared project={solve_project_name!r} '", content)
        self.assertIn("release_desktop_session(solve_desktop, close_projects=True, close_desktop=False)", content)
        self.assertNotIn("run_batch_solve(project_file, solve_targets, cores=cores, tasks=tasks)", content)
        self.assertNotIn("validate_design_app_binding(\n                app, project_name, active_design_name or design_name", content)
        self.assertIn("reopened_desktop = reopen_solved_project(project_file, grpc_port, solve_project_name)", content)
        self.assertIn("for design_name, app in iter_project_design_apps(desktop, project_file, grpc_port):", content)
        self.assertIn("Path('exit.code').write_text(str(exit_code), encoding='utf-8')", content)
        self.assertIn("return Desktop(", content)
        self.assertIn("machine='localhost'", content)
        self.assertIn("port=grpc_port", content)
        self.assertNotIn("DEFAULT_DESIGN_NAME: str | None = os.environ.get('PEETS_AEDT_DESIGN_NAME', 'HFSSDesign1').strip() or None", content)
        self.assertNotIn("design=DEFAULT_DESIGN_NAME", content)
        self.assertNotIn("extract_hfss = open_project_session(project_file)", content)
        self.assertNotIn("solve_in_batch=False", content)
        self.assertNotIn("def attach_hfss_to_project(", content)
        self.assertNotIn("app = desktop[[project_name, candidate]]", content)
        self.assertNotIn("desktop.load_project(str(project_file.resolve()))", content)
        self.assertNotIn("f'[WARN] explicit design attach failed; falling back to session-level Hfss solve '", content)

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
        self.assertIn("$CondaPython", job_content)
        self.assertIn("hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)", job_content)
        self.assertNotIn("module load", job_content)
        self.assertNotIn("srun", dispatch_content)
        self.assertNotIn("squeue", dispatch_content)
        self.assertNotIn("bash -lc", dispatch_content)
        self.assertIn("remote_job.ps1", dispatch_content)
        self.assertIn("__PEETS_RESULTS_TGZ_BEGIN__", dispatch_content)

    def test_remote_sbatch_script_contains_worker_job_submission_shape(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
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
            control_plane_ssh_target = "peetsmain@172.16.165.146"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
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
        self.assertNotIn("#SBATCH -D /tmp", content)
        self.assertIn("#SBATCH --exclude=n108,n109", content)
        self.assertIn("export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}", content)
        self.assertIn("SUBMIT_SPOOL_DIR=/tmp/peetsfea/run_01/job_0001", content)
        self.assertIn('REMOTE_RUNTIME_ROOT="$HOME/aedt_runs/_runtime"', content)
        self.assertIn('EXEC_DIR=$(mktemp -d "$REMOTE_RUNTIME_ROOT/sbatch.${SLURM_JOB_ID:-nojob}.XXXXXX")', content)
        self.assertIn("ssh \"${SSH_OPTS[@]}\" \"$SSH_REMOTE\"", content)
        self.assertIn("export REMOTE_JOB_DIR=\"$EXEC_DIR\"", content)
        self.assertIn("periodic_upload_loop() {", content)
        self.assertIn("periodic_upload_loop &", content)
        self.assertIn("/bin/bash ./remote_worker_payload.sh > worker.stdout 2> worker.stderr", content)
        self.assertNotIn("screen -dmS", content)
        self.assertIn("export PEETS_CONTROL_RUN_ID=run_01", content)
        self.assertIn("export PEETS_CONTROL_WORKER_ID=attempt_0001", content)
        self.assertIn("export PEETS_CONTROL_SSH_TARGET=peetsmain@172.16.165.146", content)
        self.assertIn("export PEETS_CONTROL_RETURN_HOST=172.16.165.146", content)
        self.assertIn("export PEETS_CONTROL_RETURN_USER=peetsmain", content)
        self.assertIn("export PEETS_CONTROL_RETURN_PORT=22", content)
        self.assertIn("stage_control_ssh_identity() {", content)
        self.assertIn("export PEETS_CONTROL_SSH_IDENTITY=\"$staged_dir/id_control\"", content)

    def test_read_remote_optional_text_file_uses_remote_ssh_port(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
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
        self.assertEqual(command[:4], ["ssh", "-p", "2202", "gate1-harry261"])

    def test_remote_ssh_and_scp_commands_include_repo_local_config_when_present(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
            remote_ssh_port = 2202
            ssh_config_path = "/repo/.ssh/config"

        ssh_command = _ssh_command(_Cfg(), "gate1-harry261", "true")
        scp_command = _scp_command(_Cfg(), "src.txt", "gate1-harry261:/tmp/dst.txt")

        self.assertEqual(
            ssh_command,
            ["ssh", "-p", "2202", "-F", "/repo/.ssh/config", "gate1-harry261", "true"],
        )
        self.assertEqual(
            scp_command,
            ["scp", "-P", "2202", "-F", "/repo/.ssh/config", "src.txt", "gate1-harry261:/tmp/dst.txt"],
        )

    def test_classify_return_path_failure_stage(self) -> None:
        self.assertEqual(
            _classify_return_path_failure_stage("ssh: Could not resolve hostname harrypc: Name or service not known"),
            "RETURN_PATH_DNS_FAILURE",
        )
        self.assertEqual(
            _classify_return_path_failure_stage("ssh: connect to host 172.16.165.146 port 5722: Connection refused"),
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
            host = "gate1-harry261"

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
            host = "gate1-harry261"

        with patch(
            "peetsfea_runner.remote_job._run_completed_process_capture_with_transport_retry",
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            ],
        ):
            self.assertEqual(query_slurm_job_state(_Cfg(), slurm_job_id="552740"), ("UNKNOWN", None))

    def test_query_slurm_job_state_falls_back_when_squeue_reports_missing_job_id(self) -> None:
        class _Cfg:
            host = "gate1-harry261"

        with patch(
            "peetsfea_runner.remote_job._run_completed_process_capture_with_transport_retry",
            side_effect=[
                subprocess.CompletedProcess(
                    args=[],
                    returncode=1,
                    stdout="",
                    stderr="slurm_load_jobs error: Invalid job id specified",
                ),
                subprocess.CompletedProcess(args=[], returncode=0, stdout="552740|COMPLETED|n115\n", stderr=""),
            ],
        ):
            self.assertEqual(query_slurm_job_state(_Cfg(), slurm_job_id="552740"), ("COMPLETED", "n115"))

    def test_sbatch_failure_keeps_submitted_slurm_job_id_in_attempt_result(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
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
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"

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
            cleanup_mock.assert_called_once()

    def test_read_remote_optional_text_file_falls_back_to_slurm_output(self) -> None:
        class _Cfg:
            host = "gate1-harry261"

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
        self.assertEqual(command[:4], ["ssh", "-p", "22", "gate1-harry261"])
        self.assertIn("if [ -f worker.stdout ]; then cat worker.stdout;", command[4])
        self.assertIn("elif [ -f slurm-552818.out ]; then cat slurm-552818.out;", command[4])

    def test_wait_for_remote_sbatch_completion_uses_slurm_output_when_worker_stdout_missing(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
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
            host = "gate1-harry261"
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
                ["ssh", "-p", "22", "gate1-harry261", "sbatch"],
                stage="sbatch submit",
                exit_code=11,
                timeout_seconds=30,
            )
        self.assertEqual(completed.returncode, 0)
        mocked_sleep.assert_called_once_with(5)

    def test_run_remote_workflow_sbatch_collects_results_after_lost_worker_when_marker_exists(self) -> None:
        class _Cfg:
            host = "gate1-harry261"
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
                path="/tmp/$USER/aedt_runs/20260306_115056/account_02/job_0011/a2",
            )
        self.assertEqual(
            resolved,
            "/tmp/dhj02/aedt_runs/20260306_115056/account_02/job_0011/a2",
        )


if __name__ == "__main__":
    unittest.main()
