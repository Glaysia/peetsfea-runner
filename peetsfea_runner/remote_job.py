from __future__ import annotations

import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol

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


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


def run_remote_job_attempt(
    *,
    config: RemoteJobConfig,
    aedt_path: Path,
    remote_job_dir: str,
    local_job_dir: Path,
    session_name: str,
) -> RemoteJobAttemptResult:
    case_summary: CaseExecutionSummary | None = None
    local_job_dir.mkdir(parents=True, exist_ok=True)
    resolved_remote_job_dir = _resolve_remote_path(config=config, path=remote_job_dir)
    _log_stage(
        "job attempt start "
        f"session={session_name} aedt={aedt_path.name} remote_dir={remote_job_dir} "
        f"resolved_remote_dir={resolved_remote_job_dir}"
    )
    try:
        _prepare_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
        with TemporaryDirectory(prefix="peetsfea_runner_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            staged_project = tmpdir_path / "project.aedt"
            shutil.copy2(aedt_path, staged_project)
            remote_script = _write_remote_job_script(tmpdir_path)
            _upload_files(
                config,
                project_file=staged_project,
                remote_job_dir=resolved_remote_job_dir,
                remote_script=remote_script,
            )
        case_summary = _run_remote_workflow_interactive(
            config,
            remote_job_dir=resolved_remote_job_dir,
            session_name=session_name,
        )
        _download_results(config, remote_job_dir=resolved_remote_job_dir, local_job_dir=local_job_dir)
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

    if case_summary is None:
        return RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name=session_name,
            case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message="Case summary missing.",
            failed_case_lines=[],
        )

    if not case_summary.success:
        failed_items = ", ".join(case_summary.case_lines)
        return RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name=session_name,
            case_summary=case_summary,
            message=(
                f"{config.windows_per_job} cases completed "
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
            f"{config.windows_per_job} cases completed "
            f"(success={case_summary.success_cases}, failed={case_summary.failed_cases})."
        ),
        failed_case_lines=[],
    )


def cleanup_orphan_session(*, config: RemoteJobConfig, session_name: str) -> None:
    command = f"screen -S {shlex.quote(session_name)} -X quit || true"
    _run_subprocess(
        ["ssh", config.host, command],
        stage="orphan cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


def cleanup_orphan_sessions_for_run(*, config: RemoteJobConfig, run_id: str) -> None:
    prefix = f"aedt_{run_id}_"
    pattern = re.escape(prefix)
    command = (
        f"screen -ls | awk '$1 ~ /{pattern}/ {{print $1}}' | "
        "while read -r sid; do screen -S \"$sid\" -X quit || true; done"
    )
    _run_subprocess(
        ["ssh", config.host, command],
        stage="orphan cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


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


def _upload_files(config: RemoteJobConfig, *, project_file: Path, remote_job_dir: str, remote_script: Path) -> None:
    remote_target = f"{config.host}:{remote_job_dir}/"
    _log_stage(f"upload files target={remote_target}")
    _run_subprocess(
        ["scp", str(project_file), str(remote_script.resolve()), remote_target],
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


def _cleanup_remote_workspace(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    remote_path = _remote_path_for_shell(remote_job_dir)
    _log_stage(f"cleanup remote workspace path={remote_job_dir}")
    _run_subprocess(
        ["ssh", config.host, f"rm -rf {shlex.quote(remote_path)}"],
        stage="remote cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


def _run_remote_workflow_interactive(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
    session_name: str,
) -> CaseExecutionSummary:
    pexpect = _import_pexpect()
    remote_path = _remote_path_for_shell(remote_job_dir)
    session = pexpect.spawn(f"ssh {config.host}", encoding="utf-8", timeout=60)
    try:
        _log_stage(f"ssh spawned host={config.host} session={session_name}")
        _log_stage(f"waiting ssh prompt session={session_name}")
        _expect_prompt(session, timeout=60, stage="ssh")
        _log_stage(f"ssh prompt acquired session={session_name}")

        srun_cmd = (
            f"srun --pty -p {config.partition} -N {config.nodes} -n {config.ntasks} "
            f"-c {config.cpus_per_job} --mem={config.mem} --time={config.time_limit} bash"
        )
        _log_stage(f"launch srun session={session_name} partition={config.partition} cpus={config.cpus_per_job}")
        _run_interactive_command(session, srun_cmd, stage="srun", timeout=300)
        _log_stage(f"srun prompt acquired session={session_name}")
        _run_interactive_command(session, f"cd {shlex.quote(remote_path)}", stage="srun", timeout=60)

        _run_interactive_command(
            session,
            f"for i in $(seq 1 {config.windows_per_job}); do "
            "case_dir=$(printf 'case_%02d' \"$i\"); "
            "mkdir -p \"$case_dir\"; "
            "cp -f project.aedt \"$case_dir/project.aedt\"; "
            "done",
            stage="remote run",
            timeout=120,
        )
        _log_stage(f"case directories prepared session={session_name} windows={config.windows_per_job}")

        first_window_cmd = _build_case_window_command(
            case_index=1,
            cores_per_window=config.cores_per_window,
        )
        _run_interactive_command(
            session,
            f"screen -dmS {shlex.quote(session_name)} -t case_01 bash -lc {shlex.quote(first_window_cmd)}",
            stage="screen",
            timeout=60,
        )

        for case_index in range(2, config.windows_per_job + 1):
            case_name = f"case_{case_index:02d}"
            case_cmd = _build_case_window_command(
                case_index=case_index,
                cores_per_window=config.cores_per_window,
            )
            _run_interactive_command(
                session,
                (
                    f"screen -S {shlex.quote(session_name)} -X screen "
                    f"-t {shlex.quote(case_name)} bash -lc {shlex.quote(case_cmd)}"
                ),
                stage="screen",
                timeout=60,
            )

        windows_output = _run_interactive_command(
            session, f"screen -S {shlex.quote(session_name)} -Q windows", stage="screen", timeout=60
        )
        if _count_screen_windows(windows_output) != config.windows_per_job:
            raise WorkflowError("Screen window validation failed.", exit_code=EXIT_CODE_SCREEN_FAILURE)
        _log_stage(f"screen windows validated session={session_name} count={config.windows_per_job}")

        wait_timeout = _time_limit_to_seconds(config.time_limit) + 600
        _run_interactive_command(
            session,
            _build_wait_all_command(config.windows_per_job),
            stage="remote run",
            timeout=wait_timeout,
        )
        _log_stage(f"wait-all completed session={session_name}")

        _run_interactive_command(
            session,
            _build_case_aggregation_command(config.windows_per_job),
            stage="remote run",
            timeout=60,
        )
        failed_count_output = _run_interactive_command(session, "cat failed.count", stage="remote run", timeout=30)
        failed_count = _parse_exit_code(failed_count_output)
        case_summary_output = _run_interactive_command(session, "cat case_summary.txt", stage="remote run", timeout=30)
        case_lines = _parse_case_summary_lines(case_summary_output)
        summary = CaseExecutionSummary(
            success_cases=config.windows_per_job - failed_count,
            failed_cases=failed_count,
            case_lines=case_lines,
        )

        _run_interactive_command(
            session,
            "tar --exclude='.venv' --exclude='results.tgz' --exclude='.env_initialized' -czf results.tgz .",
            stage="remote run",
            timeout=120,
        )
        _log_stage(f"results archive created session={session_name}")
        return summary
    finally:
        try:
            session.sendline("exit")
        except Exception:
            pass
        session.close(force=True)


def _import_pexpect():
    try:
        import pexpect
    except ImportError as exc:  # pragma: no cover - dependency availability check
        raise WorkflowError("pexpect is required for execute_remote=True.", exit_code=EXIT_CODE_SSH_FAILURE) from exc
    return pexpect


def _run_interactive_command(session, command: str, *, stage: str, timeout: int) -> str:
    session.sendline(command)
    return _expect_prompt(session, timeout=timeout, stage=stage)


def _expect_prompt(session, *, timeout: int, stage: str) -> str:
    pexpect = _import_pexpect()
    prompt = r"[$#] "
    idx = session.expect([prompt, pexpect.TIMEOUT, pexpect.EOF], timeout=timeout)
    if idx == 0:
        return session.before or ""
    if idx == 1:
        raise _stage_error(stage, f"Timeout while waiting for prompt in stage '{stage}'.")
    raise _stage_error(stage, f"Unexpected EOF in stage '{stage}'.")


def _stage_error(stage: str, message: str) -> WorkflowError:
    if stage == "ssh":
        return WorkflowError(message, exit_code=EXIT_CODE_SSH_FAILURE)
    if stage == "srun":
        return WorkflowError(message, exit_code=EXIT_CODE_SLURM_FAILURE)
    if stage == "screen":
        return WorkflowError(message, exit_code=EXIT_CODE_SCREEN_FAILURE)
    return WorkflowError(message, exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)


def _run_subprocess(command: list[str], *, stage: str, exit_code: int) -> None:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        if details:
            raise WorkflowError(f"{stage} failed: {details}", exit_code=exit_code)
        raise WorkflowError(f"{stage} failed with return code {completed.returncode}", exit_code=exit_code)


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
        "  export SCREENDIR=\"$HOME/.screen\"",
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


def _parse_exit_code(output: str) -> int:
    match = re.search(r"(-?\d+)", output)
    if match is None:
        raise WorkflowError("Unable to parse remote exit code.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    return int(match.group(1))


def _time_limit_to_seconds(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 3:
        return 5 * 3600
    hours, minutes, seconds = parts
    try:
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except ValueError:
        return 5 * 3600
