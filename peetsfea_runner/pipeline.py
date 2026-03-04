from __future__ import annotations

import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Final


EXIT_CODE_SUCCESS: Final[int] = 0
EXIT_CODE_SSH_FAILURE: Final[int] = 10
EXIT_CODE_SLURM_FAILURE: Final[int] = 11
EXIT_CODE_SCREEN_FAILURE: Final[int] = 12
EXIT_CODE_REMOTE_RUN_FAILURE: Final[int] = 13
EXIT_CODE_DOWNLOAD_FAILURE: Final[int] = 14
EXIT_CODE_REMOTE_CLEANUP_FAILURE: Final[int] = 15


@dataclass(slots=True)
class PipelineConfig:
    input_aedt_path: str
    host: str = "gate1-harry"
    partition: str = "cpu2"
    nodes: int = 1
    ntasks: int = 1
    cpus: int = 32
    mem: str = "320G"
    time_limit: str = "05:00:00"
    retry_count: int = 1
    remote_root: str = "~/aedt_runs"
    local_artifacts_dir: str = "./artifacts"
    execute_remote: bool = False

    def validate(self) -> Path:
        candidate = Path(self.input_aedt_path).expanduser()
        if candidate.suffix.lower() != ".aedt":
            raise ValueError("input_aedt_path must end with .aedt")
        if not candidate.exists():
            raise FileNotFoundError(f"AEDT file not found: {candidate}")
        if not candidate.is_file():
            raise ValueError(f"AEDT path is not a file: {candidate}")

        _ensure_positive("nodes", self.nodes)
        _ensure_positive("ntasks", self.ntasks)
        _ensure_positive("cpus", self.cpus)
        if self.retry_count < 0:
            raise ValueError("retry_count must be >= 0")
        if not self.host.strip():
            raise ValueError("host must not be empty")
        if not self.partition.strip():
            raise ValueError("partition must not be empty")
        if not self.mem.strip():
            raise ValueError("mem must not be empty")
        if not self.time_limit.strip():
            raise ValueError("time_limit must not be empty")
        if not self.remote_root.strip():
            raise ValueError("remote_root must not be empty")
        if not self.local_artifacts_dir.strip():
            raise ValueError("local_artifacts_dir must not be empty")

        return candidate.resolve()


@dataclass(slots=True)
class PipelineResult:
    success: bool
    exit_code: int
    run_id: str
    remote_run_dir: str
    local_artifacts_dir: str
    summary: str


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    if not isinstance(config, PipelineConfig):
        raise TypeError("config must be a PipelineConfig")

    project_file = config.validate()

    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    remote_run_dir = _join_remote_root(config.remote_root, run_id)

    local_root = Path(config.local_artifacts_dir).expanduser().resolve()
    local_run_dir = local_root / run_id
    local_run_dir.mkdir(parents=True, exist_ok=True)

    if not config.execute_remote:
        summary = (
            "Phase 01-02 API implementation complete: input validation and run metadata "
            "preparation are ready. Remote execution workflow is disabled."
        )
        return PipelineResult(
            success=True,
            exit_code=EXIT_CODE_SUCCESS,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_artifacts_dir=str(local_run_dir),
            summary=summary,
        )

    try:
        _prepare_remote_workspace(config, remote_run_dir)
        with TemporaryDirectory(prefix="peetsfea_runner_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            staged_project = tmpdir_path / "project.aedt"
            shutil.copy2(project_file, staged_project)
            remote_script = _write_remote_job_script(tmpdir_path)
            _upload_files(
                config,
                project_file=staged_project,
                remote_run_dir=remote_run_dir,
                remote_script=remote_script,
            )
        _run_remote_workflow_interactive(config, remote_run_dir=remote_run_dir, run_id=run_id)
        _download_results(config, remote_run_dir=remote_run_dir, local_run_dir=local_run_dir)
        _cleanup_remote_workspace(config, remote_run_dir=remote_run_dir)
    except _WorkflowError as exc:
        return PipelineResult(
            success=False,
            exit_code=exc.exit_code,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_artifacts_dir=str(local_run_dir),
            summary=str(exc),
        )

    return PipelineResult(
        success=True,
        exit_code=EXIT_CODE_SUCCESS,
        run_id=run_id,
        remote_run_dir=remote_run_dir,
        local_artifacts_dir=str(local_run_dir),
        summary="Remote workflow completed successfully.",
    )


def _ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _join_remote_root(remote_root: str, run_id: str) -> str:
    normalized = remote_root.rstrip("/")
    if normalized:
        return f"{normalized}/{run_id}"
    return run_id


class _WorkflowError(RuntimeError):
    def __init__(self, message: str, *, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def _prepare_remote_workspace(config: PipelineConfig, remote_run_dir: str) -> None:
    def _mkdir_remote() -> None:
        _run_subprocess(
            ["ssh", config.host, f"mkdir -p {shlex.quote(remote_run_dir)}"],
            stage="ssh",
            exit_code=EXIT_CODE_SSH_FAILURE,
        )

    _run_with_retry("ssh", config.retry_count, _mkdir_remote)


def _upload_files(config: PipelineConfig, *, project_file: Path, remote_run_dir: str, remote_script: Path) -> None:
    upload_targets = [
        str(project_file),
        str(remote_script.resolve()),
    ]
    remote_target = f"{config.host}:{remote_run_dir}/"

    def _scp_files() -> None:
        _run_subprocess(
            ["scp", *upload_targets, remote_target],
            stage="scp upload",
            exit_code=EXIT_CODE_SSH_FAILURE,
        )

    _run_with_retry("scp upload", config.retry_count, _scp_files)


def _download_results(config: PipelineConfig, *, remote_run_dir: str, local_run_dir: Path) -> None:
    local_archive = local_run_dir / "results.tgz"
    remote_archive = f"{config.host}:{remote_run_dir}/results.tgz"

    def _scp_download() -> None:
        _run_subprocess(
            ["scp", remote_archive, str(local_archive)],
            stage="scp download",
            exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
        )

    _run_with_retry("scp download", config.retry_count, _scp_download)
    _run_subprocess(
        ["tar", "-xzf", str(local_archive), "-C", str(local_run_dir)],
        stage="download extract",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )


def _cleanup_remote_workspace(config: PipelineConfig, *, remote_run_dir: str) -> None:
    _run_subprocess(
        ["ssh", config.host, f"rm -rf {shlex.quote(remote_run_dir)}"],
        stage="remote cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


def _run_remote_workflow_interactive(config: PipelineConfig, *, remote_run_dir: str, run_id: str) -> None:
    def _run_once() -> None:
        pexpect = _import_pexpect()
        session = pexpect.spawn(f"ssh {config.host}", encoding="utf-8", timeout=60)
        try:
            _expect_prompt(session, timeout=60, stage="ssh")

            srun_cmd = (
                f"srun --pty -p {config.partition} -N {config.nodes} -n {config.ntasks} "
                f"-c {config.cpus} --mem={config.mem} --time={config.time_limit} bash"
            )
            _run_interactive_command(session, srun_cmd, stage="srun", timeout=300)
            _run_interactive_command(session, f"cd {shlex.quote(remote_run_dir)}", stage="srun", timeout=60)

            session_name = f"aedt_{run_id}"
            quoted_script = shlex.quote(
                f"PEETS_CORES={config.cpus} bash ./remote_job.sh > remote_run.log 2>&1; echo $? > exit.code"
            )
            screen_cmd = f"screen -dmS {shlex.quote(session_name)} -t main bash -lc {quoted_script}"
            _run_interactive_command(session, screen_cmd, stage="screen", timeout=60)

            windows_output = _run_interactive_command(
                session, f"screen -S {shlex.quote(session_name)} -Q windows", stage="screen", timeout=60
            )
            if not _looks_like_single_window(windows_output):
                raise _WorkflowError("Screen window validation failed.", exit_code=EXIT_CODE_SCREEN_FAILURE)

            wait_timeout = _time_limit_to_seconds(config.time_limit) + 600
            _run_interactive_command(
                session,
                "while [ ! -f exit.code ]; do sleep 5; done",
                stage="remote run",
                timeout=wait_timeout,
            )

            exit_code_output = _run_interactive_command(session, "cat exit.code", stage="remote run", timeout=30)
            remote_exit = _parse_exit_code(exit_code_output)
            if remote_exit != 0:
                raise _WorkflowError(
                    f"Remote simulation failed with exit code {remote_exit}.",
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                )

            _run_interactive_command(
                session,
                "tar --exclude='.venv' --exclude='results.tgz' --exclude='.env_initialized' -czf results.tgz .",
                stage="remote run",
                timeout=120,
            )
        finally:
            try:
                session.sendline("exit")
            except Exception:
                pass
            session.close(force=True)

    _run_with_retry(
        "srun",
        config.retry_count,
        _run_once,
        retryable_exit_codes={EXIT_CODE_SLURM_FAILURE},
    )


def _import_pexpect():
    try:
        import pexpect
    except ImportError as exc:  # pragma: no cover - dependency availability check
        raise _WorkflowError("pexpect is required for execute_remote=True.", exit_code=EXIT_CODE_SSH_FAILURE) from exc
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


def _stage_error(stage: str, message: str) -> _WorkflowError:
    if stage == "ssh":
        return _WorkflowError(message, exit_code=EXIT_CODE_SSH_FAILURE)
    if stage == "srun":
        return _WorkflowError(message, exit_code=EXIT_CODE_SLURM_FAILURE)
    if stage == "screen":
        return _WorkflowError(message, exit_code=EXIT_CODE_SCREEN_FAILURE)
    return _WorkflowError(message, exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)


def _run_subprocess(command: list[str], *, stage: str, exit_code: int) -> None:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        if details:
            raise _WorkflowError(f"{stage} failed: {details}", exit_code=exit_code)
        raise _WorkflowError(f"{stage} failed with return code {completed.returncode}", exit_code=exit_code)


def _run_with_retry(
    stage: str,
    retry_count: int,
    action,
    *,
    sleep_seconds: int = 10,
    retryable_exit_codes: set[int] | None = None,
) -> None:
    last_error: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            action()
            return
        except _WorkflowError as exc:
            last_error = exc
            if retryable_exit_codes is not None and exc.exit_code not in retryable_exit_codes:
                raise
            if attempt >= retry_count:
                raise
            time.sleep(sleep_seconds)
    if last_error is not None:
        raise last_error
    raise _WorkflowError(f"Unknown retry failure in {stage}.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)


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
        "python3 -m venv .venv",
        "source .venv/bin/activate",
        "python -m ensurepip --upgrade || true",
        "python -m pip install --upgrade pip",
        "python -m pip install pyaedt",
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
        "    cmd = ['/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt', '-ng', '-grpcsrv', str(port)]",
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
        "    project_file = workdir / 'project.aedt'",
        "    if not project_file.exists():",
        "        raise FileNotFoundError(f'AEDT file not found: {project_file}')",
        "",
        "    cores = int(os.environ.get('PEETS_CORES', '32'))",
        "    remove_lock_files(workdir)",
        "    grpc_port = get_available_port()",
        "    ansys_process = launch_ansys_grpc_server(grpc_port)",
        "",
        "    hfss = None",
        "    try:",
        "        hfss = Hfss(",
        "            non_graphical=True,",
        "            new_desktop=True,",
        "            machine='localhost',",
        "            port=grpc_port,",
        "            close_on_exit=True,",
        "        )",
        "        hfss.solve_in_batch(file_name=str(project_file.resolve()), cores=cores, tasks=cores)",
        "        hfss.save_project()",
        "    finally:",
        "        if hfss is not None:",
        "            hfss.release_desktop(close_projects=True, close_desktop=True)",
        "        stop_process(ansys_process)",
        "",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "PY",
        "python run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _write_remote_job_script(tmpdir: Path) -> Path:
    script = tmpdir / "remote_job.sh"
    script.write_text(_build_remote_job_script_content(), encoding="utf-8")
    return script


def _looks_like_single_window(output: str) -> bool:
    tokens = output.strip()
    if not tokens:
        return False
    # Typical output is like "0$ bash"
    return "$" in tokens and tokens.count("$") == 1


def _parse_exit_code(output: str) -> int:
    match = re.search(r"(-?\d+)", output)
    if match is None:
        raise _WorkflowError("Unable to parse remote exit code.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
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
