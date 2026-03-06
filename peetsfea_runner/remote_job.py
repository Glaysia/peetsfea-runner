from __future__ import annotations

import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Protocol, Sequence

from .constants import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    EXIT_CODE_REMOTE_RUN_FAILURE,
    EXIT_CODE_SCREEN_FAILURE,
    EXIT_CODE_SLURM_FAILURE,
    EXIT_CODE_SSH_FAILURE,
)


class RemoteJobConfig(Protocol):
    host: str
    partition: str
    nodes: int
    ntasks: int
    cpus_per_job: int
    mem: str
    time_limit: str
    windows_per_job: int
    cores_per_window: int


@dataclass(slots=True)
class CaseExecutionSummary:
    success_cases: int
    failed_cases: int
    case_lines: list[str]

    @property
    def success(self) -> bool:
        return self.failed_cases == 0


@dataclass(slots=True)
class RemoteJobAttemptResult:
    success: bool
    exit_code: int
    session_name: str
    case_summary: CaseExecutionSummary
    message: str
    failed_case_lines: list[str]


class WorkflowError(RuntimeError):
    def __init__(self, message: str, *, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(slots=True, frozen=True)
class WindowInput:
    window_id: str
    input_path: Path


@dataclass(slots=True)
class _RemoteWorkflowResult:
    case_summary: CaseExecutionSummary
    archive_bytes: bytes


@dataclass(slots=True)
class _RemoteWorkflowSubmission:
    stdout: str
    stderr: str
    return_code: int

    @property
    def combined_output(self) -> str:
        parts = [part.strip() for part in (self.stdout, self.stderr) if part and part.strip()]
        return "\n".join(parts).strip()


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


def run_remote_job_attempt(
    *,
    config: RemoteJobConfig,
    aedt_path: Path | None = None,
    window_inputs: Sequence[WindowInput] | None = None,
    remote_job_dir: str,
    local_job_dir: Path,
    session_name: str,
    on_upload_success: Callable[[], None] | None = None,
) -> RemoteJobAttemptResult:
    inputs = _normalize_window_inputs(
        config=config,
        aedt_path=aedt_path,
        window_inputs=window_inputs,
    )
    case_count = len(inputs)

    workflow_result: _RemoteWorkflowResult | None = None
    local_job_dir.mkdir(parents=True, exist_ok=True)
    resolved_remote_job_dir = _resolve_remote_path(config=config, path=remote_job_dir)
    _log_stage(
        "job attempt start "
        f"session={session_name} window_count={case_count} remote_dir={remote_job_dir} "
        f"resolved_remote_dir={resolved_remote_job_dir}"
    )
    try:
        _prepare_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
        with TemporaryDirectory(prefix="peetsfea_runner_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            staged_projects: list[Path] = []
            for index, window in enumerate(inputs, start=1):
                staged_project = tmpdir_path / f"project_{index:02d}.aedt"
                shutil.copy2(window.input_path, staged_project)
                staged_projects.append(staged_project)
            remote_script = _write_remote_job_script(tmpdir_path)
            remote_dispatch_script = _write_remote_dispatch_script(
                tmpdir_path,
                config=config,
                remote_job_dir=resolved_remote_job_dir,
                case_count=case_count,
            )
            _upload_files(
                config,
                project_files=staged_projects,
                remote_job_dir=resolved_remote_job_dir,
                remote_script=remote_script,
                remote_dispatch_script=remote_dispatch_script,
            )
            if on_upload_success is not None:
                on_upload_success()
        workflow_result = _run_remote_workflow_noninteractive(
            config,
            remote_job_dir=resolved_remote_job_dir,
            case_count=case_count,
        )
        local_archive = local_job_dir / "results.tgz"
        local_archive.write_bytes(workflow_result.archive_bytes)
        _extract_local_results_archive(local_job_dir=local_job_dir)
        _cleanup_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
    except WorkflowError as exc:
        _log_stage(f"job attempt failed session={session_name} exit_code={exc.exit_code} reason={exc}")
        return RemoteJobAttemptResult(
            success=False,
            exit_code=exc.exit_code,
            session_name=session_name,
            case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message=str(exc),
            failed_case_lines=[],
        )

    if workflow_result is None:
        return RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name=session_name,
            case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message="Case summary missing.",
            failed_case_lines=[],
        )

    case_summary = workflow_result.case_summary
    if not case_summary.success:
        failed_items = ", ".join(case_summary.case_lines)
        return RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name=session_name,
            case_summary=case_summary,
            message=(
                f"{case_count} cases completed "
                f"(success={case_summary.success_cases}, failed={case_summary.failed_cases}). "
                f"failed_cases={failed_items}"
            ),
            failed_case_lines=case_summary.case_lines,
        )

    _log_stage(f"job attempt success session={session_name}")
    return RemoteJobAttemptResult(
        success=True,
        exit_code=0,
        session_name=session_name,
        case_summary=case_summary,
        message=(
            f"{case_count} cases completed "
            f"(success={case_summary.success_cases}, failed={case_summary.failed_cases})."
        ),
        failed_case_lines=[],
    )


def cleanup_orphan_session(*, config: RemoteJobConfig, session_name: str) -> None:
    # The primary remote execution path is non-interactive and no longer creates screen sessions.
    return None


def cleanup_orphan_sessions_for_run(*, config: RemoteJobConfig, run_id: str) -> None:
    # The primary remote execution path is non-interactive and no longer creates screen sessions.
    return None


def _resolve_remote_path(*, config: RemoteJobConfig, path: str) -> str:
    if path == "~" or path.startswith("~/"):
        home = _get_remote_home(config=config)
        if path == "~":
            return home
        return f"{home}/{path[2:]}"
    return path


def _get_remote_home(*, config: RemoteJobConfig) -> str:
    completed = subprocess.run(
        ["ssh", config.host, "printf %s \"$HOME\""],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        if details:
            raise WorkflowError(f"ssh failed: {details}", exit_code=EXIT_CODE_SSH_FAILURE)
        raise WorkflowError("ssh failed while resolving remote home directory.", exit_code=EXIT_CODE_SSH_FAILURE)
    home = (completed.stdout or "").strip()
    if not home.startswith("/"):
        raise WorkflowError("Unable to resolve remote home directory.", exit_code=EXIT_CODE_SSH_FAILURE)
    return home


def _prepare_remote_workspace(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    remote_path = _remote_path_for_shell(remote_job_dir)
    _log_stage(f"prepare remote workspace path={remote_job_dir}")
    _run_subprocess(
        ["ssh", config.host, f"mkdir -p {shlex.quote(remote_path)}"],
        stage="ssh",
        exit_code=EXIT_CODE_SSH_FAILURE,
    )


def _upload_files(
    config: RemoteJobConfig,
    *,
    project_files: Sequence[Path],
    remote_job_dir: str,
    remote_script: Path,
    remote_dispatch_script: Path,
) -> None:
    remote_target = f"{config.host}:{remote_job_dir}/"
    _log_stage(f"upload files target={remote_target}")
    upload_sources = [str(path) for path in project_files]
    upload_sources.append(str(remote_script.resolve()))
    upload_sources.append(str(remote_dispatch_script.resolve()))
    _run_subprocess(
        ["scp", *upload_sources, remote_target],
        stage="scp upload",
        exit_code=EXIT_CODE_SSH_FAILURE,
    )


def _download_results(config: RemoteJobConfig, *, remote_job_dir: str, local_job_dir: Path) -> None:
    local_archive = local_job_dir / "results.tgz"
    remote_archive = f"{config.host}:{remote_job_dir}/results.tgz"
    _log_stage(f"download results from={remote_archive} to={local_archive}")
    _run_subprocess(
        ["scp", remote_archive, str(local_archive)],
        stage="scp download",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )
    _run_subprocess(
        ["tar", "-xzf", str(local_archive), "-C", str(local_job_dir)],
        stage="download extract",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )


def _extract_local_results_archive(*, local_job_dir: Path) -> None:
    local_archive = local_job_dir / "results.tgz"
    _run_subprocess(
        ["tar", "-xzf", str(local_archive), "-C", str(local_job_dir)],
        stage="download extract",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )


def _cleanup_remote_workspace(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    remote_path = _remote_path_for_shell(remote_job_dir)
    _log_stage(f"cleanup remote workspace path={remote_job_dir}")
    _run_subprocess(
        ["ssh", config.host, f"rm -rf {shlex.quote(remote_path)}"],
        stage="remote cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


def _run_remote_workflow_noninteractive(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
    case_count: int,
) -> _RemoteWorkflowResult:
    if case_count <= 0:
        raise WorkflowError("case_count must be > 0", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    submission = _submit_remote_workflow_noninteractive(
        config=config,
        remote_job_dir=remote_job_dir,
    )
    output = submission.combined_output
    failed_count = _parse_marked_failed_count(output)
    case_lines = _parse_marked_case_summary_lines(output)
    return _RemoteWorkflowResult(
        case_summary=CaseExecutionSummary(
            success_cases=case_count - failed_count,
            failed_cases=failed_count,
            case_lines=case_lines,
        ),
        archive_bytes=_parse_marked_results_archive(output),
    )


def _submit_remote_workflow_noninteractive(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
) -> _RemoteWorkflowSubmission:
    remote_path = _remote_path_for_shell(remote_job_dir)
    command = f"cd {shlex.quote(remote_path)} && bash ./remote_dispatch.sh"
    timeout_seconds = _time_limit_to_seconds(config.time_limit) + 900
    _log_stage(f"launch non-interactive remote workflow host={config.host} path={remote_job_dir}")
    completed = _run_completed_process_capture(
        ["ssh", config.host, command],
        stage="remote run",
        exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
        timeout_seconds=timeout_seconds,
    )
    submission = _RemoteWorkflowSubmission(
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
        return_code=completed.returncode,
    )
    _raise_for_remote_submission_failure(submission, stage="remote run")
    return submission


def _run_subprocess(command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None) -> None:
    completed = _run_completed_process(command, stage=stage, exit_code=exit_code, timeout_seconds=timeout_seconds)
    if completed.stdout or completed.stderr:
        return None


def _run_subprocess_capture(
    command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None
) -> str:
    completed = _run_completed_process(command, stage=stage, exit_code=exit_code, timeout_seconds=timeout_seconds)
    return (completed.stdout or "").strip()


def _run_completed_process_capture(
    command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkflowError(f"{stage} timed out after {timeout_seconds}s", exit_code=exit_code) from exc


def _run_completed_process(
    command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None
) -> subprocess.CompletedProcess[str]:
    completed = _run_completed_process_capture(
        command,
        stage=stage,
        exit_code=exit_code,
        timeout_seconds=timeout_seconds,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        if details:
            raise WorkflowError(f"{stage} failed: {details}", exit_code=exit_code)
        raise WorkflowError(f"{stage} failed with return code {completed.returncode}", exit_code=exit_code)
    return completed


def _normalize_window_inputs(
    *,
    config: RemoteJobConfig,
    aedt_path: Path | None,
    window_inputs: Sequence[WindowInput] | None,
) -> list[WindowInput]:
    if aedt_path is None and window_inputs is None:
        raise ValueError("Either aedt_path or window_inputs must be provided.")
    if aedt_path is not None and window_inputs is not None:
        raise ValueError("aedt_path and window_inputs are mutually exclusive.")

    if window_inputs is not None:
        normalized = list(window_inputs)
        if not normalized:
            raise ValueError("window_inputs must not be empty.")
        return normalized

    assert aedt_path is not None
    return [
        WindowInput(window_id=f"case_{index:02d}", input_path=aedt_path)
        for index in range(1, config.windows_per_job + 1)
    ]


def _remote_path_for_shell(path: str) -> str:
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return f"$HOME/{path[2:]}"
    return path


def _build_remote_job_script_content() -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "if [ ! -f .env_initialized ]; then",
        "  export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/AnsysEM",
        "  export LANG=en_US.UTF-8",
        "  export LC_ALL=en_US.UTF-8",
        "  unset LANGUAGE",
        "  export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81",
        "  module load ansys-electronics/v252",
        "  touch .env_initialized",
        "fi",
        "",
        "VENV_DIR=\"$HOME/.peetsfea-runner-venv\"",
        "if [ ! -x \"$VENV_DIR/bin/python\" ]; then",
        "  echo \"[ERROR] Shared venv not found: $VENV_DIR\" >&2",
        "  echo \"[ERROR] Run scripts/remote_bootstrap_install.sh first.\" >&2",
        "  exit 1",
        "fi",
        "BASE_PREFIX=\"$($VENV_DIR/bin/python -c 'import sys; print(sys.base_prefix)')\"",
        "export LD_LIBRARY_PATH=\"$BASE_PREFIX/lib:$HOME/miniconda3/lib:${LD_LIBRARY_PATH:-}\"",
        "DEPS_READY_MARKER=\"$VENV_DIR/.peets_deps_ready\"",
        "DEPS_LOCK_DIR=\"$VENV_DIR/.peets_deps_lock\"",
        "while ! mkdir \"$DEPS_LOCK_DIR\" 2>/dev/null; do",
        "  sleep 1",
        "done",
        "cleanup_deps_lock() {",
        "  rmdir \"$DEPS_LOCK_DIR\" 2>/dev/null || true",
        "}",
        "trap cleanup_deps_lock EXIT",
        "if [ ! -f \"$DEPS_READY_MARKER\" ]; then",
        "  \"$VENV_DIR/bin/python\" -m ensurepip --upgrade || true",
        "  \"$VENV_DIR/bin/python\" -m pip install --upgrade pip",
        "  \"$VENV_DIR/bin/python\" -m pip install uv",
        "  if ! \"$VENV_DIR/bin/python\" -m uv --version >/dev/null 2>&1; then",
        "    echo \"[ERROR] uv is not available in shared venv: $VENV_DIR\" >&2",
        "    exit 1",
        "  fi",
        "  \"$VENV_DIR/bin/python\" -m uv pip install pyaedt==0.25.1",
        "  touch \"$DEPS_READY_MARKER\"",
        "fi",
        "trap - EXIT",
        "cleanup_deps_lock",
        "cat > run_sim.py <<'PY'",
        "from __future__ import annotations",
        "",
        "import os",
        "import socket",
        "import subprocess",
        "import time",
        "from pathlib import Path",
        "",
        "from ansys.aedt.core import Hfss",
        "",
        "",
        "AEDT_FILENAME: str = 'project.aedt'",
        "USE_GRAPHIC: bool = False",
        "ANSYS_EXECUTABLE: str = '/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt'",
        "",
        "",
        "def remove_lock_files(workdir: Path) -> None:",
        "    for lock_file in workdir.glob('*.lock'):",
        "        try:",
        "            lock_file.unlink()",
        "        except OSError as exc:",
        "            raise OSError(f'Failed to remove lock file: {lock_file}') from exc",
        "",
        "",
        "def get_available_port(host: str = '127.0.0.1') -> int:",
        "    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:",
        "        sock.bind((host, 0))",
        "        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
        "        return int(sock.getsockname()[1])",
        "",
        "",
        "def launch_ansys_grpc_server(port: int) -> subprocess.Popen:",
        "    cmd = [ANSYS_EXECUTABLE, '-ng', '-grpcsrv', str(port)]",
        "    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)",
        "    time.sleep(6)",
        "    if process.poll() is not None:",
        "        raise RuntimeError(f'Failed to start ansysedt gRPC server on port {port}')",
        "    return process",
        "",
        "",
        "def stop_process(process: subprocess.Popen) -> None:",
        "    if process.poll() is not None:",
        "        return",
        "    process.terminate()",
        "    try:",
        "        process.wait(timeout=10)",
        "    except subprocess.TimeoutExpired:",
        "        process.kill()",
        "        process.wait(timeout=5)",
        "",
        "",
        "def main() -> None:",
        "    workdir = Path.cwd()",
        "    project_file = workdir / AEDT_FILENAME",
        "    if not project_file.exists():",
        "        raise FileNotFoundError(f'AEDT file not found: {project_file}')",
        "",
        "    tmpdir = workdir / 'tmp'",
        "    tmpdir.mkdir(parents=True, exist_ok=True)",
        "    os.environ['TMPDIR'] = str(tmpdir)",
        "    cores = int(os.environ.get('PEETS_CORES', '32'))",
        "    remove_lock_files(workdir)",
        "    grpc_port = get_available_port()",
        "    ansys_process = launch_ansys_grpc_server(grpc_port)",
        "    hfss = None",
        "    try:",
        "        hfss = Hfss(",
        "            non_graphical=(not USE_GRAPHIC),",
        "            new_desktop=False,",
        "            machine='localhost',",
        "            port=grpc_port,",
        "            close_on_exit=True,",
        "        )",
        "        hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=cores)",
        "        # Batch solve output is already persisted in project artifacts.",
        "        # save_project() can intermittently fail on shared Ansoft temp path",
        "        # even when simulation completed normally; treat it as non-fatal.",
        "        try:",
        "            hfss.save_project()",
        "        except Exception as exc:",
        "            print(f'[WARN] save_project failed but solve completed: {exc}')",
        "    finally:",
        "        if hfss is not None:",
        "            hfss.release_desktop(close_projects=True, close_desktop=True)",
        "        stop_process(ansys_process)",
        "",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "PY",
        "\"$VENV_DIR/bin/python\" run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _write_remote_job_script(tmpdir: Path) -> Path:
    script = tmpdir / "remote_job.sh"
    script.write_text(_build_remote_job_script_content(), encoding="utf-8")
    return script


def _write_remote_dispatch_script(tmpdir: Path, *, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> Path:
    script = tmpdir / "remote_dispatch.sh"
    script.write_text(
        _build_remote_dispatch_script_content(
            config=config,
            remote_job_dir=remote_job_dir,
            case_count=case_count,
        ),
        encoding="utf-8",
    )
    return script


def _build_remote_dispatch_script_content(*, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> str:
    remote_path = _remote_path_for_shell(remote_job_dir)
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"REMOTE_JOB_DIR={shlex.quote(remote_path)}",
        "cd \"$REMOTE_JOB_DIR\"",
        "tar -czf - remote_job.sh project_*.aedt | \\",
        "  " + _build_noninteractive_srun_command(
            config=config,
            remote_job_dir=remote_path,
            case_count=case_count,
        ),
    ]
    return "\n".join(lines) + "\n"


def _build_noninteractive_srun_command(*, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> str:
    max_parallel = max(1, int(getattr(config, "windows_per_job", case_count)))
    payload_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "workdir=$(mktemp -d /tmp/peetsfea-slot.XXXXXX)",
        "cleanup() { rm -rf \"$workdir\"; }",
        "trap cleanup EXIT",
        "cd \"$workdir\"",
        "tar -xzf -",
    ]
    for case_index in range(1, case_count + 1):
        case_name = f"case_{case_index:02d}"
        source_name = f"project_{case_index:02d}.aedt"
        payload_lines.extend(
            [
                f"mkdir -p {shlex.quote(case_name)}",
                f"cp -f {shlex.quote(source_name)} {shlex.quote(case_name)}/project.aedt",
            ]
        )
    payload_lines.extend(
        [
            f"max_parallel={max_parallel}",
            "for i in $(seq 1 " + str(case_count) + "); do",
            "  while [ \"$(jobs -pr | wc -l)\" -ge \"$max_parallel\" ]; do",
            "    wait -n || true",
            "  done",
            "  case_dir=$(printf 'case_%02d' \"$i\")",
            "  (",
            "    cd \"$case_dir\"",
            "    mkdir -p tmp",
            "    export TMPDIR=\"$PWD/tmp\"",
            f"    PEETS_CORES={config.cores_per_window} bash ../remote_job.sh > run.log 2>&1",
            "    rc=$?",
            "    echo \"$rc\" > exit.code",
            "  ) &",
            "done",
            "while [ \"$(jobs -pr | wc -l)\" -gt 0 ]; do",
            "  wait -n || true",
            "done",
        ]
    )
    payload_lines.extend(
        [
            _build_case_aggregation_command(case_count),
            "archive_path=$(mktemp /tmp/peetsfea-results.XXXXXX.tgz)",
            "cleanup_archive() { rm -f \"$archive_path\"; }",
            "trap cleanup_archive EXIT",
            "tar -czf \"$archive_path\" case_* case_summary.txt failed.count",
            "printf '__PEETS_FAILED_COUNT__:%s\\n' \"$(cat failed.count)\"",
            "printf '__PEETS_CASE_SUMMARY_BEGIN__\\n'",
            "cat case_summary.txt",
            "printf '__PEETS_CASE_SUMMARY_END__\\n'",
            "printf '__PEETS_RESULTS_TGZ_BEGIN__\\n'",
            "base64 -w0 \"$archive_path\"",
            "printf '\\n__PEETS_RESULTS_TGZ_END__\\n'",
        ]
    )
    payload = "\n".join(payload_lines)
    return (
        f"srun -D /tmp -p {config.partition} -N {config.nodes} -n {config.ntasks} "
        f"-c {config.cpus_per_job} --mem={config.mem} --time={config.time_limit} "
        f"bash -lc {shlex.quote(payload)}"
    )


def _count_screen_windows(output: str) -> int:
    tokens = output.strip()
    if not tokens:
        return 0
    window_tokens = re.findall(r"(?:(?<=^)|(?<=\s))\d+\$?(?=\s)", tokens)
    return len(window_tokens)


def _build_case_window_command(*, case_index: int, cores_per_window: int) -> str:
    case_name = f"case_{case_index:02d}"
    return (
        f"cd {shlex.quote(case_name)} && "
        f"PEETS_CORES={cores_per_window} bash ../remote_job.sh > run.log 2>&1; "
        "echo $? > exit.code"
    )


def _build_wait_all_command(parallel_windows: int) -> str:
    return (
        "while true; do "
        "done_count=0; "
        f"for i in $(seq 1 {parallel_windows}); do "
        "case_dir=$(printf 'case_%02d' \"$i\"); "
        "[ -f \"$case_dir/exit.code\" ] && done_count=$((done_count+1)); "
        "done; "
        f"[ \"$done_count\" -eq {parallel_windows} ] && break; "
        "sleep 5; "
        "done"
    )


def _build_case_aggregation_command(parallel_windows: int) -> str:
    return (
        "rm -f case_summary.txt failed.count; "
        "failed=0; "
        f"for i in $(seq 1 {parallel_windows}); do "
        "case_dir=$(printf 'case_%02d' \"$i\"); "
        "code=$(cat \"$case_dir/exit.code\"); "
        "echo \"$case_dir:$code\" >> case_summary.txt; "
        "if [ \"$code\" -ne 0 ]; then failed=$((failed+1)); fi; "
        "done; "
        "echo \"$failed\" > failed.count"
    )


def _parse_case_summary_lines(output: str) -> list[str]:
    lines = []
    for raw in output.splitlines():
        line = raw.strip()
        if not line:
            continue
        if ":" not in line:
            continue
        case_name, code_value = line.split(":", 1)
        case_name = case_name.strip()
        code_value = code_value.strip()
        if not case_name.startswith("case_"):
            continue
        if _parse_exit_code(code_value) == 0:
            continue
        lines.append(f"{case_name}:{code_value}")
    return lines


def _parse_marked_failed_count(output: str) -> int:
    match = re.search(r"__PEETS_FAILED_COUNT__:(-?\d+)", output)
    if match is None:
        raise WorkflowError("Unable to parse failed.count marker.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    return int(match.group(1))


def _parse_marked_case_summary_lines(output: str) -> list[str]:
    match = re.search(
        r"__PEETS_CASE_SUMMARY_BEGIN__\n(?P<body>.*?)(?:\n)?__PEETS_CASE_SUMMARY_END__",
        output,
        flags=re.DOTALL,
    )
    if match is None:
        raise WorkflowError("Unable to parse case summary marker.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    return _parse_case_summary_lines(match.group("body"))


def _parse_marked_results_archive(output: str) -> bytes:
    match = re.search(
        r"__PEETS_RESULTS_TGZ_BEGIN__\n(?P<body>.*?)(?:\n)?__PEETS_RESULTS_TGZ_END__",
        output,
        flags=re.DOTALL,
    )
    if match is None:
        raise WorkflowError("Unable to parse results archive marker.", exit_code=EXIT_CODE_DOWNLOAD_FAILURE)
    try:
        import base64

        return base64.b64decode(match.group("body").strip(), validate=True)
    except Exception as exc:
        raise WorkflowError("Unable to decode results archive marker.", exit_code=EXIT_CODE_DOWNLOAD_FAILURE) from exc


def _parse_exit_code(output: str) -> int:
    match = re.search(r"(-?\d+)", output)
    if match is None:
        raise WorkflowError("Unable to parse remote exit code.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    return int(match.group(1))


_SLURM_PROGRESS_PATTERNS = (
    re.compile(r"^srun: job \d+ queued and waiting for resources$"),
    re.compile(r"^srun: job \d+ has been allocated resources$"),
)


def _raise_for_remote_submission_failure(submission: _RemoteWorkflowSubmission, *, stage: str) -> None:
    output = submission.combined_output
    if _has_remote_workflow_markers(output):
        return
    if submission.return_code == 0:
        return
    details = _extract_meaningful_remote_failure_details(output)
    if details:
        raise WorkflowError(f"{stage} failed: {details}", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    raise WorkflowError(
        f"{stage} failed with return code {submission.return_code}",
        exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
    )


def _has_remote_workflow_markers(output: str) -> bool:
    return (
        "__PEETS_FAILED_COUNT__:" in output
        and "__PEETS_CASE_SUMMARY_BEGIN__" in output
        and "__PEETS_CASE_SUMMARY_END__" in output
        and "__PEETS_RESULTS_TGZ_BEGIN__" in output
        and "__PEETS_RESULTS_TGZ_END__" in output
    )


def _extract_meaningful_remote_failure_details(output: str) -> str:
    lines: list[str] = []
    for raw in output.splitlines():
        line = raw.strip()
        if not line:
            continue
        if any(pattern.match(line) for pattern in _SLURM_PROGRESS_PATTERNS):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _time_limit_to_seconds(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 3:
        return 5 * 3600
    hours, minutes, seconds = parts
    try:
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except ValueError:
        return 5 * 3600
