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
    slots_per_job: int
    cores_per_slot: int
    tasks_per_slot: int
    platform: str
    scheduler: str
    remote_execution_backend: str
    control_plane_host: str
    control_plane_port: int
    control_plane_ssh_target: str
    control_plane_return_host: str
    control_plane_return_port: int
    control_plane_return_user: str
    tunnel_heartbeat_timeout_seconds: int
    tunnel_recovery_grace_seconds: int
    remote_ssh_port: int
    emit_output_variables_csv: bool
    slurm_exclude_nodes: tuple[str, ...]


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
    failure_category: str | None = None
    slurm_job_id: str | None = None
    observed_node: str | None = None
    worker_terminal_state: str | None = None
    collect_probe_state: str | None = None
    marker_present: bool | None = None


class WorkflowError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        exit_code: int,
        stdout: str = "",
        stderr: str = "",
        slurm_job_id: str | None = None,
        observed_node: str | None = None,
        worker_terminal_state: str | None = None,
        collect_probe_state: str | None = None,
        marker_present: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.slurm_job_id = slurm_job_id
        self.observed_node = observed_node
        self.worker_terminal_state = worker_terminal_state
        self.collect_probe_state = collect_probe_state
        self.marker_present = marker_present


@dataclass(slots=True, frozen=True)
class SlotInput:
    slot_id: str
    input_path: Path


@dataclass(slots=True)
class _RemoteWorkflowResult:
    case_summary: CaseExecutionSummary
    archive_bytes: bytes
    submission: _RemoteWorkflowSubmission
    collect_probe_state: str | None = None
    marker_present: bool | None = None


@dataclass(slots=True)
class _RemoteWorkflowSubmission:
    stdout: str
    stderr: str
    return_code: int

    @property
    def combined_output(self) -> str:
        parts = [part.strip() for part in (self.stdout, self.stderr) if part and part.strip()]
        return "\n".join(parts).strip()


@dataclass(slots=True)
class _RemoteCollectProbe:
    collect_probe_state: str
    marker_present: bool
    has_results_ready: bool
    has_results_archive: bool
    has_case_summary: bool
    has_failed_count: bool


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


def _remote_platform(config: RemoteJobConfig) -> str:
    return getattr(config, "platform", "linux").strip().lower()


def _remote_scheduler(config: RemoteJobConfig) -> str:
    default = "slurm" if _remote_platform(config) == "linux" else "none"
    return getattr(config, "scheduler", default).strip().lower()


def _remote_execution_backend(config: RemoteJobConfig) -> str:
    return getattr(config, "remote_execution_backend", "foreground_ssh").strip().lower()


def _remote_ssh_port(config: RemoteJobConfig) -> int:
    return int(getattr(config, "remote_ssh_port", 22))


def _control_plane_return_host(config: RemoteJobConfig) -> str:
    explicit = str(getattr(config, "control_plane_return_host", "")).strip()
    if explicit:
        return explicit
    target = str(getattr(config, "control_plane_ssh_target", "")).strip()
    if "@" in target:
        return target.split("@", 1)[1].strip()
    return target


def _control_plane_return_user(config: RemoteJobConfig) -> str:
    explicit = str(getattr(config, "control_plane_return_user", "")).strip()
    if explicit:
        return explicit
    target = str(getattr(config, "control_plane_ssh_target", "")).strip()
    if "@" in target:
        return target.split("@", 1)[0].strip()
    return ""


def _control_plane_return_port(config: RemoteJobConfig) -> int:
    return int(getattr(config, "control_plane_return_port", 5722))


def _control_plane_ssh_target(config: RemoteJobConfig) -> str:
    user = _control_plane_return_user(config)
    host = _control_plane_return_host(config)
    if user and host:
        return f"{user}@{host}"
    return str(getattr(config, "control_plane_ssh_target", "")).strip()


def _slurm_exclude_nodes(config: RemoteJobConfig) -> tuple[str, ...]:
    raw_nodes = getattr(config, "slurm_exclude_nodes", ())
    if isinstance(raw_nodes, str):
        iterable = raw_nodes.split(",")
    else:
        iterable = raw_nodes
    normalized = [str(node).strip() for node in iterable if str(node).strip()]
    return tuple(dict.fromkeys(normalized))


def _ssh_command(config: RemoteJobConfig, *args: str) -> list[str]:
    return ["ssh", "-p", str(_remote_ssh_port(config)), *args]


def _scp_command(config: RemoteJobConfig, *args: str) -> list[str]:
    return ["scp", "-P", str(_remote_ssh_port(config)), *args]


_TRANSIENT_LAUNCH_PATTERNS = (
    "connection closed by",
    "kex_exchange_identification",
    "connection reset",
    "broken pipe",
    "connection timed out",
    "operation timed out",
    "ssh_exchange_identification",
)
_TRANSIENT_LAUNCH_RETURN_CODES = {255}
_SSH_RETRY_BACKOFF_SECONDS = (5, 15, 30)


def _memory_to_mb(mem: str) -> int | None:
    raw = str(mem).strip().upper()
    if not raw:
        return None
    multiplier = 1
    if raw.endswith("TB") or raw.endswith("T"):
        multiplier = 1024 * 1024
        raw = raw[:-2] if raw.endswith("TB") else raw[:-1]
    elif raw.endswith("GB") or raw.endswith("G"):
        multiplier = 1024
        raw = raw[:-2] if raw.endswith("GB") else raw[:-1]
    elif raw.endswith("MB") or raw.endswith("M"):
        multiplier = 1
        raw = raw[:-2] if raw.endswith("MB") else raw[:-1]
    elif raw.endswith("KB") or raw.endswith("K"):
        multiplier = 1 / 1024
        raw = raw[:-2] if raw.endswith("KB") else raw[:-1]
    try:
        return int(float(raw) * multiplier)
    except ValueError:
        return None


def run_remote_job_attempt(
    *,
    config: RemoteJobConfig,
    run_id: str | None = None,
    worker_id: str | None = None,
    aedt_path: Path | None = None,
    slot_inputs: Sequence[SlotInput] | None = None,
    remote_job_dir: str,
    local_job_dir: Path,
    session_name: str,
    on_upload_success: Callable[[], None] | None = None,
    on_worker_submitted: Callable[[str, str | None], None] | None = None,
    on_worker_state_change: Callable[[str, str | None], None] | None = None,
) -> RemoteJobAttemptResult:
    inputs = _normalize_slot_inputs(
        config=config,
        aedt_path=aedt_path,
        slot_inputs=slot_inputs,
    )
    case_count = len(inputs)

    workflow_result: _RemoteWorkflowResult | None = None
    slurm_job_id: str | None = None
    observed_node: str | None = None
    worker_terminal_state: str | None = None
    collect_probe_state: str | None = None
    marker_present: bool | None = None
    local_job_dir.mkdir(parents=True, exist_ok=True)
    resolved_remote_job_dir = _resolve_remote_path(config=config, path=remote_job_dir)
    _log_stage(
        "job attempt start "
        f"session={session_name} slot_count={case_count} remote_dir={remote_job_dir} "
        f"resolved_remote_dir={resolved_remote_job_dir}"
    )
    try:
        _prepare_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
        with TemporaryDirectory(prefix="peetsfea_runner_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            staged_projects: list[Path] = []
            for index, slot in enumerate(inputs, start=1):
                staged_project = tmpdir_path / f"project_{index:02d}.aedt"
                shutil.copy2(slot.input_path, staged_project)
                staged_projects.append(staged_project)
            remote_script = (
                _write_windows_remote_job_script(tmpdir_path)
                if _remote_platform(config) == "windows"
                else _write_remote_job_script(tmpdir_path, config=config)
            )
            supporting_files = [remote_script]
            if (_remote_platform(config), _remote_scheduler(config), _remote_execution_backend(config)) == (
                "linux",
                "slurm",
                "slurm_batch",
            ):
                remote_worker_payload = _write_remote_worker_payload_script(
                    tmpdir_path,
                    config=config,
                    case_count=case_count,
                    run_id=run_id,
                    worker_id=worker_id,
                )
                remote_sbatch_script = _write_remote_sbatch_script(
                    tmpdir_path,
                    config=config,
                    remote_job_dir=resolved_remote_job_dir,
                    run_id=run_id,
                    worker_id=worker_id,
                )
                supporting_files.extend([remote_worker_payload, remote_sbatch_script])
            else:
                remote_dispatch_script = _write_remote_dispatch_script(
                    tmpdir_path,
                    config=config,
                    remote_job_dir=resolved_remote_job_dir,
                    case_count=case_count,
                )
                supporting_files.append(remote_dispatch_script)
            _upload_supporting_files(
                config,
                project_files=staged_projects,
                remote_job_dir=resolved_remote_job_dir,
                supporting_files=supporting_files,
            )
            if on_upload_success is not None:
                on_upload_success()
        if (_remote_platform(config), _remote_scheduler(config), _remote_execution_backend(config)) == (
            "linux",
            "slurm",
            "slurm_batch",
        ):
            workflow_result, slurm_job_id, observed_node, worker_terminal_state = _run_remote_workflow_sbatch(
                config,
                remote_job_dir=resolved_remote_job_dir,
                case_count=case_count,
                on_worker_submitted=on_worker_submitted,
                on_worker_state_change=on_worker_state_change,
            )
            collect_probe_state = workflow_result.collect_probe_state
            marker_present = workflow_result.marker_present
        else:
            workflow_result = _run_remote_workflow_noninteractive(
                config,
                remote_job_dir=resolved_remote_job_dir,
                case_count=case_count,
            )
            slurm_job_id = None
            observed_node = None
        local_archive = local_job_dir / ("results.zip" if _remote_platform(config) == "windows" else "results.tgz")
        local_archive.write_bytes(workflow_result.archive_bytes)
        _extract_local_results_archive(local_job_dir=local_job_dir, archive_name=local_archive.name)
        _cleanup_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
    except WorkflowError as exc:
        _cleanup_remote_workspace_best_effort(config, remote_job_dir=resolved_remote_job_dir)
        worker_terminal_state = exc.worker_terminal_state or worker_terminal_state
        collect_probe_state = exc.collect_probe_state or collect_probe_state
        marker_present = exc.marker_present if exc.marker_present is not None else marker_present
        failure_category = _categorize_failure(
            exit_code=exc.exit_code,
            message=str(exc),
            failed_case_lines=[],
        )
        _write_failure_artifacts(
            local_job_dir=local_job_dir,
            exit_code=exc.exit_code,
            failure_category=failure_category,
            message=str(exc),
            failed_case_lines=[],
            stdout=exc.stdout,
            stderr=exc.stderr,
        )
        _log_stage(f"job attempt failed session={session_name} exit_code={exc.exit_code} reason={exc}")
        return RemoteJobAttemptResult(
            success=False,
            exit_code=exc.exit_code,
            session_name=session_name,
            case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message=str(exc),
            failed_case_lines=[],
            failure_category=failure_category,
            slurm_job_id=exc.slurm_job_id or slurm_job_id,
            observed_node=exc.observed_node or observed_node,
            worker_terminal_state=worker_terminal_state,
            collect_probe_state=collect_probe_state,
            marker_present=marker_present,
        )

    if workflow_result is None:
        return RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name=session_name,
            case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message="Case summary missing.",
            failed_case_lines=[],
            failure_category="collect",
            slurm_job_id=slurm_job_id,
            observed_node=observed_node,
            worker_terminal_state=worker_terminal_state,
            collect_probe_state=collect_probe_state,
            marker_present=marker_present,
        )

    _write_failure_artifacts(
        local_job_dir=local_job_dir,
        exit_code=0 if workflow_result.case_summary.success else EXIT_CODE_REMOTE_RUN_FAILURE,
        failure_category="solve" if not workflow_result.case_summary.success else None,
        message=(
            f"{case_count} cases completed "
            f"(success={workflow_result.case_summary.success_cases}, failed={workflow_result.case_summary.failed_cases})."
        ),
        failed_case_lines=workflow_result.case_summary.case_lines,
        stdout=workflow_result.submission.stdout,
        stderr=workflow_result.submission.stderr,
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
            failure_category="solve",
            slurm_job_id=slurm_job_id,
            observed_node=observed_node,
            worker_terminal_state=worker_terminal_state,
            collect_probe_state=collect_probe_state,
            marker_present=marker_present,
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
        slurm_job_id=slurm_job_id,
        observed_node=observed_node,
        worker_terminal_state=worker_terminal_state,
        collect_probe_state=collect_probe_state,
        marker_present=marker_present,
    )


def cleanup_orphan_session(*, config: RemoteJobConfig, session_name: str) -> None:
    # The primary remote execution path is non-interactive and no longer creates screen sessions.
    return None


def cleanup_orphan_sessions_for_run(*, config: RemoteJobConfig, run_id: str) -> None:
    # The primary remote execution path is non-interactive and no longer creates screen sessions.
    return None


def _resolve_remote_path(*, config: RemoteJobConfig, path: str) -> str:
    if _remote_platform(config) == "windows":
        home = _get_remote_home(config=config)
        if path == "~":
            return home
        if path.startswith("~/"):
            return f"{home}\\{path[2:].replace('/', '\\')}"
        if path == "/tmp/peetsfea-runner" or path.startswith("/tmp/peetsfea-runner/"):
            scoped_root = f"{home}\\AppData\\Local\\Temp\\peetsfea-runner"
            if path == "/tmp/peetsfea-runner":
                return scoped_root
            suffix = path[len("/tmp/peetsfea-runner/") :].replace("/", "\\")
            return f"{scoped_root}\\{suffix}"
        return path.replace("/", "\\")
    if path == "~" or path.startswith("~/"):
        home = _get_remote_home(config=config)
        if path == "~":
            return home
        return f"{home}/{path[2:]}"
    if path == "/tmp/peetsfea-runner" or path.startswith("/tmp/peetsfea-runner/"):
        if _remote_scheduler(config) == "slurm" and _remote_execution_backend(config) == "slurm_batch":
            home = _get_remote_home(config=config)
            shared_root = f"{home}/aedt_runs"
            if path == "/tmp/peetsfea-runner":
                return shared_root
            return f"{shared_root}{path[len('/tmp/peetsfea-runner'):]}"
        remote_user = _get_remote_user(config=config)
        scoped_root = f"/tmp/{remote_user}/peetsfea-runner"
        if path == "/tmp/peetsfea-runner":
            return scoped_root
        return f"{scoped_root}{path[len('/tmp/peetsfea-runner'):]}"
    return path


def _get_remote_home(*, config: RemoteJobConfig) -> str:
    command = _ssh_command(config, config.host)
    if _remote_platform(config) == "windows":
        command.append('powershell -NoProfile -NonInteractive -Command "$HOME"')
    else:
        command.append('printf %s "$HOME"')
    completed = subprocess.run(
        command,
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
    if _remote_platform(config) == "windows":
        if ":" not in home:
            raise WorkflowError("Unable to resolve remote home directory.", exit_code=EXIT_CODE_SSH_FAILURE)
        return home
    if not home.startswith("/"):
        raise WorkflowError("Unable to resolve remote home directory.", exit_code=EXIT_CODE_SSH_FAILURE)
    return home


def _get_remote_user(*, config: RemoteJobConfig) -> str:
    home = _get_remote_home(config=config)
    user = Path(home).name.strip()
    if not user:
        raise WorkflowError("Unable to resolve remote user from home directory.", exit_code=EXIT_CODE_SSH_FAILURE)
    return user


def _prepare_remote_workspace(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    _log_stage(f"prepare remote workspace path={remote_job_dir}")
    if _remote_platform(config) == "windows":
        command = (
            "powershell -NoProfile -NonInteractive -Command "
            + shlex.quote(f"New-Item -ItemType Directory -Force -Path '{remote_job_dir}' | Out-Null")
        )
        _run_subprocess_with_transport_retry(
            _ssh_command(config, config.host, command),
            stage="ssh",
            exit_code=EXIT_CODE_SSH_FAILURE,
        )
        return
    _run_subprocess_with_transport_retry(
        _ssh_command(config, config.host, f"mkdir -p {shlex.quote(remote_path)}"),
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
    _upload_supporting_files(
        config,
        project_files=project_files,
        remote_job_dir=remote_job_dir,
        supporting_files=[remote_script, remote_dispatch_script],
    )


def _upload_supporting_files(
    config: RemoteJobConfig,
    *,
    project_files: Sequence[Path],
    remote_job_dir: str,
    supporting_files: Sequence[Path],
) -> None:
    remote_target = f"{config.host}:{remote_job_dir}/"
    _log_stage(f"upload files target={remote_target}")
    upload_sources = [str(path) for path in project_files]
    upload_sources.extend(str(path.resolve()) for path in supporting_files)
    _run_subprocess_with_transport_retry(
        _scp_command(config, *upload_sources, remote_target),
        stage="scp upload",
        exit_code=EXIT_CODE_SSH_FAILURE,
    )


def _download_results(config: RemoteJobConfig, *, remote_job_dir: str, local_job_dir: Path) -> None:
    local_archive = local_job_dir / "results.tgz"
    remote_archive = f"{config.host}:{remote_job_dir}/results.tgz"
    _log_stage(f"download results from={remote_archive} to={local_archive}")
    _run_subprocess_with_transport_retry(
        _scp_command(config, remote_archive, str(local_archive)),
        stage="scp download",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )
    _run_subprocess(
        ["tar", "-xzf", str(local_archive), "-C", str(local_job_dir)],
        stage="download extract",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )


def _extract_local_results_archive(*, local_job_dir: Path, archive_name: str = "results.tgz") -> None:
    local_archive = local_job_dir / archive_name
    if local_archive.suffix == ".zip":
        try:
            shutil.unpack_archive(str(local_archive), str(local_job_dir), format="zip")
        except Exception as exc:
            raise WorkflowError("download extract failed: unable to extract zip archive", exit_code=EXIT_CODE_DOWNLOAD_FAILURE) from exc
        return
    _run_subprocess(
        ["tar", "-xzf", str(local_archive), "-C", str(local_job_dir)],
        stage="download extract",
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
    )


def _cleanup_remote_workspace(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    _log_stage(f"cleanup remote workspace path={remote_job_dir}")
    if _remote_platform(config) == "windows":
        command = (
            "powershell -NoProfile -NonInteractive -Command "
            + shlex.quote(f"if (Test-Path '{remote_job_dir}') {{ Remove-Item -Recurse -Force '{remote_job_dir}' }}")
        )
        _run_subprocess(
            _ssh_command(config, config.host, command),
            stage="remote cleanup",
            exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
        )
        return
    _run_subprocess(
        _ssh_command(config, config.host, f"rm -rf {shlex.quote(remote_path)}"),
        stage="remote cleanup",
        exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    )


def _cleanup_remote_workspace_best_effort(config: RemoteJobConfig, *, remote_job_dir: str) -> None:
    try:
        _cleanup_remote_workspace(config, remote_job_dir=remote_job_dir)
    except WorkflowError as exc:
        _log_stage(f"cleanup remote workspace skipped path={remote_job_dir} reason={exc}")


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
        submission=submission,
    )


def _submit_remote_workflow_noninteractive(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
) -> _RemoteWorkflowSubmission:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    if _remote_platform(config) == "windows":
        command = (
            "powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "
            + shlex.quote(f"Set-Location '{remote_job_dir}'; .\\remote_dispatch.ps1")
        )
    else:
        command = f"cd {shlex.quote(remote_path)} && bash ./remote_dispatch.sh"
    timeout_seconds = _time_limit_to_seconds(config.time_limit) + 900
    _log_stage(f"launch non-interactive remote workflow host={config.host} path={remote_job_dir}")
    completed = _run_completed_process_capture(
        _ssh_command(config, config.host, command),
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


def _run_remote_workflow_sbatch(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
    case_count: int,
    on_worker_submitted: Callable[[str, str | None], None] | None = None,
    on_worker_state_change: Callable[[str, str | None], None] | None = None,
) -> tuple[_RemoteWorkflowResult, str, str | None, str]:
    slurm_job_id = _submit_remote_workflow_sbatch(config=config, remote_job_dir=remote_job_dir)
    if on_worker_submitted is not None:
        on_worker_submitted(slurm_job_id, None)
    try:
        stdout, stderr, observed_node, terminal_state = _wait_for_remote_sbatch_completion(
            config=config,
            remote_job_dir=remote_job_dir,
            slurm_job_id=slurm_job_id,
            on_worker_state_change=on_worker_state_change,
        )
    except WorkflowError as exc:
        if exc.slurm_job_id is None:
            exc.slurm_job_id = slurm_job_id
        if (exc.worker_terminal_state or "").upper() in {"FAILED", "LOST"}:
            probe = _probe_remote_completion_artifacts(
                config=config,
                remote_job_dir=remote_job_dir,
                slurm_job_id=slurm_job_id,
            )
            exc.collect_probe_state = probe.collect_probe_state
            exc.marker_present = probe.marker_present
            if probe.marker_present:
                workflow_result = _collect_remote_workflow_result_from_probe(
                    config=config,
                    remote_job_dir=remote_job_dir,
                    case_count=case_count,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    probe=probe,
                )
                return (
                    workflow_result,
                    slurm_job_id,
                    exc.observed_node,
                    exc.worker_terminal_state or "FAILED",
                )
        raise
    submission = _RemoteWorkflowSubmission(stdout=stdout, stderr=stderr, return_code=0)
    output = submission.combined_output
    failed_count = _parse_marked_failed_count(output)
    case_lines = _parse_marked_case_summary_lines(output)
    return (
        _RemoteWorkflowResult(
            case_summary=CaseExecutionSummary(
                success_cases=case_count - failed_count,
                failed_cases=failed_count,
                case_lines=case_lines,
            ),
            archive_bytes=_parse_marked_results_archive(output),
            submission=submission,
            collect_probe_state="NOT_NEEDED",
            marker_present=True,
        ),
        slurm_job_id,
        observed_node,
        terminal_state,
    )


def _submit_remote_workflow_sbatch(config: RemoteJobConfig, *, remote_job_dir: str) -> str:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    completed = _run_completed_process_capture_with_transport_retry(
        _ssh_command(config, config.host, f"cd {shlex.quote(remote_path)} && sbatch --parsable ./remote_sbatch.sh"),
        stage="sbatch submit",
        exit_code=EXIT_CODE_SLURM_FAILURE,
        timeout_seconds=30,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        raise WorkflowError(
            f"sbatch submit failed: {details or completed.returncode}",
            exit_code=EXIT_CODE_SLURM_FAILURE,
            stdout=stdout,
            stderr=stderr,
        )
    return _parse_sbatch_job_id(completed.stdout or "")


def _probe_remote_completion_artifacts(
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    slurm_job_id: str,
) -> _RemoteCollectProbe:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    probe_targets = (
        "results.tgz.ready",
        "results.tgz",
        "case_summary.txt",
        "failed.count",
        "worker.stdout",
        "worker.stderr",
        f"slurm-{slurm_job_id}.out",
        f"slurm-{slurm_job_id}.err",
    )
    probe_lines = [
        "set +e",
        f"cd {shlex.quote(remote_path)} || exit 0",
    ]
    for name in probe_targets:
        probe_lines.append(
            f"if [ -f {shlex.quote(name)} ]; then echo {shlex.quote(name)}=1; else echo {shlex.quote(name)}=0; fi"
        )
    try:
        output = _run_subprocess_capture(
            _ssh_command(config, config.host, "bash -lc " + shlex.quote("\n".join(probe_lines))),
            stage="collect probe",
            exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
            timeout_seconds=20,
        )
    except WorkflowError:
        return _RemoteCollectProbe(
            collect_probe_state="PROBE_FAILED",
            marker_present=False,
            has_results_ready=False,
            has_results_archive=False,
            has_case_summary=False,
            has_failed_count=False,
        )
    flags: dict[str, bool] = {}
    for raw in output.splitlines():
        if "=" not in raw:
            continue
        name, value = raw.split("=", 1)
        flags[name.strip()] = value.strip() == "1"
    marker_present = bool(flags.get("results.tgz.ready")) or (
        bool(flags.get("results.tgz"))
        and bool(flags.get("case_summary.txt"))
        and bool(flags.get("failed.count"))
    )
    return _RemoteCollectProbe(
        collect_probe_state="MARKER_PRESENT" if marker_present else "MARKER_MISSING",
        marker_present=marker_present,
        has_results_ready=bool(flags.get("results.tgz.ready")),
        has_results_archive=bool(flags.get("results.tgz")),
        has_case_summary=bool(flags.get("case_summary.txt")),
        has_failed_count=bool(flags.get("failed.count")),
    )


def _download_results_archive_bytes(config: RemoteJobConfig, *, remote_job_dir: str) -> bytes:
    remote_archive = f"{config.host}:{remote_job_dir}/results.tgz"
    with TemporaryDirectory(prefix="peetsfea_results_") as tmpdir:
        local_archive = Path(tmpdir) / "results.tgz"
        _run_subprocess_with_transport_retry(
            _scp_command(config, remote_archive, str(local_archive)),
            stage="scp download",
            exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
        )
        return local_archive.read_bytes()


def _parse_failed_count_text(output: str) -> int:
    match = re.search(r"(-?\d+)", output)
    if match is None:
        raise WorkflowError("Unable to parse failed.count file.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)
    return int(match.group(1))


def _collect_remote_workflow_result_from_probe(
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    case_count: int,
    stdout: str,
    stderr: str,
    probe: _RemoteCollectProbe,
) -> _RemoteWorkflowResult:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    failed_count_text = _read_remote_optional_text_file(
        config=config,
        remote_path=remote_path,
        filenames=("failed.count",),
        stage="failed.count",
        timeout_seconds=20,
    )
    case_summary_text = _read_remote_optional_text_file(
        config=config,
        remote_path=remote_path,
        filenames=("case_summary.txt",),
        stage="case summary",
        timeout_seconds=20,
    )
    failed_count = _parse_failed_count_text(failed_count_text)
    case_lines = _parse_case_summary_lines(case_summary_text)
    archive_bytes = _download_results_archive_bytes(config=config, remote_job_dir=remote_job_dir)
    return _RemoteWorkflowResult(
        case_summary=CaseExecutionSummary(
            success_cases=case_count - failed_count,
            failed_cases=failed_count,
            case_lines=case_lines,
        ),
        archive_bytes=archive_bytes,
        submission=_RemoteWorkflowSubmission(stdout=stdout, stderr=stderr, return_code=0),
        collect_probe_state=probe.collect_probe_state,
        marker_present=probe.marker_present,
    )


def query_slurm_job_state(
    config: RemoteJobConfig,
    *,
    slurm_job_id: str,
) -> tuple[str, str | None]:
    completed = _run_completed_process_capture_with_transport_retry(
        _ssh_command(config, config.host, f"squeue -h -j {shlex.quote(slurm_job_id)} -o \"%T|%N\""),
        stage="squeue",
        exit_code=EXIT_CODE_SLURM_FAILURE,
        timeout_seconds=20,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        raise WorkflowError(
            f"squeue failed: {details or completed.returncode}",
            exit_code=EXIT_CODE_SLURM_FAILURE,
            stdout=stdout,
            stderr=stderr,
            slurm_job_id=slurm_job_id,
        )
    output = (completed.stdout or "").strip()
    if output:
        state, node = _parse_squeue_state_line(output)
        normalized = state.strip().upper()
        if normalized in {"RUNNING", "COMPLETING", "CG"}:
            return "RUNNING", node.strip() or None
        if normalized in {"PENDING", "CONFIGURING", "PD"}:
            return "PENDING", None
        return normalized, node.strip() or None

    completed = _run_completed_process_capture_with_transport_retry(
        _ssh_command(config, config.host, f"sacct -j {shlex.quote(slurm_job_id)} -P -n -o JobIDRaw,State,NodeList"),
        stage="sacct",
        exit_code=EXIT_CODE_SLURM_FAILURE,
        timeout_seconds=20,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr if stderr else stdout
        raise WorkflowError(
            f"sacct failed: {details or completed.returncode}",
            exit_code=EXIT_CODE_SLURM_FAILURE,
            stdout=stdout,
            stderr=stderr,
            slurm_job_id=slurm_job_id,
        )
    for raw in (completed.stdout or "").splitlines():
        parts = [part.strip() for part in raw.split("|")]
        if len(parts) < 3:
            continue
        if parts[0] != slurm_job_id:
            continue
        state = parts[1].upper()
        node = parts[2] or None
        if state.startswith("COMPLETED"):
            return "COMPLETED", node
        if state.startswith("RUNNING"):
            return "RUNNING", node
        if state.startswith("PENDING"):
            return "PENDING", node
        if state.startswith("CANCELLED") or state.startswith("FAILED") or state.startswith("TIMEOUT") or state.startswith("OUT_OF_MEMORY") or state.startswith("NODE_FAIL"):
            return "FAILED", node
        return state, node
    return "UNKNOWN", None


def _parse_squeue_state_line(output: str) -> tuple[str, str]:
    first_line = output.strip().splitlines()[0].strip()
    if "|" not in first_line:
        return first_line, ""
    state, node = first_line.split("|", 1)
    return state.strip(), node.strip()


def _wait_for_remote_sbatch_completion(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
    slurm_job_id: str,
    on_worker_state_change: Callable[[str, str | None], None] | None = None,
) -> tuple[str, str, str | None, str]:
    timeout_seconds = _time_limit_to_seconds(config.time_limit) + 900
    started = time.monotonic()
    last_state: str | None = None
    observed_node: str | None = None
    last_probe_error: str | None = None
    while True:
        try:
            state, node = query_slurm_job_state(config, slurm_job_id=slurm_job_id)
            last_probe_error = None
        except WorkflowError as exc:
            if exc.slurm_job_id is None:
                exc.slurm_job_id = slurm_job_id
            if exc.observed_node is None:
                exc.observed_node = observed_node
            if exc.exit_code != EXIT_CODE_SLURM_FAILURE:
                raise
            last_probe_error = str(exc)
            if time.monotonic() - started > timeout_seconds:
                raise WorkflowError(
                    f"sbatch job timed out waiting for completion job_id={slurm_job_id}: {last_probe_error}",
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                    slurm_job_id=slurm_job_id,
                    observed_node=observed_node,
                    worker_terminal_state=last_state or "UNKNOWN",
                ) from exc
            time.sleep(5)
            continue
        observed_node = node or observed_node
        if state != last_state:
            last_state = state
            if on_worker_state_change is not None and state != "UNKNOWN":
                on_worker_state_change(state, observed_node)
        if state in {"COMPLETED", "FAILED"}:
            break
        if time.monotonic() - started > timeout_seconds:
            details = f": {last_probe_error}" if last_probe_error else ""
            raise WorkflowError(
                f"sbatch job timed out waiting for completion job_id={slurm_job_id}{details}",
                exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                slurm_job_id=slurm_job_id,
                observed_node=observed_node,
                worker_terminal_state=state,
            )
        time.sleep(5)

    if state == "COMPLETED" and on_worker_state_change is not None:
        on_worker_state_change("IDLE_DRAINING", observed_node)

    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    try:
        stdout = _read_remote_optional_text_file(
            config=config,
            remote_path=remote_path,
            filenames=("worker.stdout", f"slurm-{slurm_job_id}.out"),
            stage="worker stdout",
        )
        stderr = _read_remote_optional_text_file(
            config=config,
            remote_path=remote_path,
            filenames=("worker.stderr", f"slurm-{slurm_job_id}.err"),
            stage="worker stderr",
        )
    except WorkflowError as exc:
        if exc.slurm_job_id is None:
            exc.slurm_job_id = slurm_job_id
        if exc.observed_node is None:
            exc.observed_node = observed_node
        if exc.worker_terminal_state is None:
            exc.worker_terminal_state = state
        raise
    if state != "COMPLETED" and not _has_remote_workflow_markers("\n".join(part for part in (stdout, stderr) if part)):
        details = _extract_meaningful_remote_failure_details("\n".join(part for part in (stdout, stderr) if part))
        return_path_stage = _classify_return_path_failure_stage(details)
        if return_path_stage is not None:
            details = (
                f"{details} stage=return_path host={_control_plane_return_host(config)} "
                f"port={_control_plane_return_port(config)} ssh_target={_control_plane_ssh_target(config)}"
            )
        raise WorkflowError(
            f"sbatch worker failed job_id={slurm_job_id}: {details or state}",
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            stdout=stdout,
            stderr=stderr,
            slurm_job_id=slurm_job_id,
            observed_node=observed_node,
            worker_terminal_state=state,
        )
    return stdout, stderr, observed_node, state


def _run_subprocess(command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None) -> None:
    completed = _run_completed_process(command, stage=stage, exit_code=exit_code, timeout_seconds=timeout_seconds)
    if completed.stdout or completed.stderr:
        return None


def _is_launch_transient_message(message: str) -> bool:
    normalized = message.lower()
    return any(pattern in normalized for pattern in _TRANSIENT_LAUNCH_PATTERNS)


def _is_retryable_transport_failure(*, returncode: int, stdout: str, stderr: str) -> bool:
    combined = "\n".join(part for part in (stdout, stderr) if part)
    if _is_launch_transient_message(combined):
        return True
    return returncode in _TRANSIENT_LAUNCH_RETURN_CODES


def _retry_history_lines(*, stage: str, attempts: list[str]) -> str:
    if not attempts:
        return ""
    history = "\n".join(attempts)
    return f"{stage} retry history:\n{history}"


def _with_retry_history(
    completed: subprocess.CompletedProcess[str],
    *,
    stage: str,
    attempts: list[str],
) -> subprocess.CompletedProcess[str]:
    history = _retry_history_lines(stage=stage, attempts=attempts)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if history:
        stderr = "\n".join(part for part in (history.strip(), stderr.strip()) if part).strip()
        if stderr:
            stderr = f"{stderr}\n"
    return subprocess.CompletedProcess(
        args=completed.args,
        returncode=completed.returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _run_completed_process_capture_with_transport_retry(
    command: list[str],
    *,
    stage: str,
    exit_code: int,
    timeout_seconds: int | None = None,
    backoff_seconds: Sequence[int] = _SSH_RETRY_BACKOFF_SECONDS,
) -> subprocess.CompletedProcess[str]:
    attempts: list[str] = []
    total_attempts = len(backoff_seconds) + 1
    last_completed: subprocess.CompletedProcess[str] | None = None
    for attempt_no in range(1, total_attempts + 1):
        try:
            completed = _run_completed_process_capture(
                command,
                stage=stage,
                exit_code=exit_code,
                timeout_seconds=timeout_seconds,
            )
        except WorkflowError as exc:
            if not _is_launch_transient_message(str(exc)):
                raise
            attempts.append(f"attempt={attempt_no} timeout={timeout_seconds}s detail={exc}")
            if attempt_no >= total_attempts:
                exc.stderr = "\n".join(part for part in (_retry_history_lines(stage=stage, attempts=attempts), exc.stderr.strip()) if part)
                raise
            sleep_seconds = backoff_seconds[attempt_no - 1]
            _log_stage(
                f"retry backoff stage={stage} attempt={attempt_no} sleep={sleep_seconds}s reason={exc}"
            )
            time.sleep(sleep_seconds)
            continue
        last_completed = completed
        if completed.returncode == 0:
            return completed
        if not _is_retryable_transport_failure(
            returncode=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        ):
            return completed
        attempts.append(
            f"attempt={attempt_no} rc={completed.returncode} detail={_extract_meaningful_remote_failure_details((completed.stderr or '') + chr(10) + (completed.stdout or '')) or completed.returncode}"
        )
        if attempt_no >= total_attempts:
            return _with_retry_history(completed, stage=stage, attempts=attempts)
        sleep_seconds = backoff_seconds[attempt_no - 1]
        _log_stage(
            f"retry backoff stage={stage} attempt={attempt_no} sleep={sleep_seconds}s "
            f"rc={completed.returncode}"
        )
        time.sleep(sleep_seconds)
    if last_completed is not None:
        return last_completed
    raise WorkflowError(f"{stage} failed before command execution", exit_code=exit_code)


def _run_subprocess_with_transport_retry(
    command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None
) -> None:
    completed = _run_completed_process_capture_with_transport_retry(
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
            raise WorkflowError(
                f"{stage} failed: {details}",
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
            )
        raise WorkflowError(f"{stage} failed with return code {completed.returncode}", exit_code=exit_code)


def _run_subprocess_capture(
    command: list[str], *, stage: str, exit_code: int, timeout_seconds: int | None = None
) -> str:
    completed = _run_completed_process(command, stage=stage, exit_code=exit_code, timeout_seconds=timeout_seconds)
    return (completed.stdout or "").strip()


def _read_remote_optional_text_file(
    *,
    config: RemoteJobConfig,
    remote_path: str,
    filenames: Sequence[str],
    stage: str,
    timeout_seconds: int | None = None,
) -> str:
    clauses: list[str] = []
    for index, filename in enumerate(filenames):
        keyword = "if" if index == 0 else "elif"
        quoted = shlex.quote(filename)
        clauses.append(f"{keyword} [ -f {quoted} ]; then cat {quoted};")
    if not clauses:
        return ""
    command = f"cd {shlex.quote(remote_path)} && " + " ".join(clauses) + " fi"
    return _run_subprocess_capture(
        _ssh_command(config, config.host, command),
        stage=stage,
        exit_code=EXIT_CODE_DOWNLOAD_FAILURE,
        timeout_seconds=timeout_seconds,
    )


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
            raise WorkflowError(
                f"{stage} failed: {details}",
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
            )
        raise WorkflowError(
            f"{stage} failed with return code {completed.returncode}",
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )
    return completed


def _normalize_slot_inputs(
    *,
    config: RemoteJobConfig,
    aedt_path: Path | None,
    slot_inputs: Sequence[SlotInput] | None,
) -> list[SlotInput]:
    if aedt_path is None and slot_inputs is None:
        raise ValueError("Either aedt_path or slot_inputs must be provided.")
    if aedt_path is not None and slot_inputs is not None:
        raise ValueError("aedt_path and slot_inputs are mutually exclusive.")

    if slot_inputs is not None:
        normalized = list(slot_inputs)
        if not normalized:
            raise ValueError("slot_inputs must not be empty.")
        return normalized

    assert aedt_path is not None
    return [
        SlotInput(slot_id=f"case_{index:02d}", input_path=aedt_path)
        for index in range(1, config.slots_per_job + 1)
    ]


def _remote_path_for_shell(*, config: RemoteJobConfig, path: str) -> str:
    if _remote_platform(config) == "windows":
        return path
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return f"$HOME/{path[2:]}"
    return path


def _build_remote_job_script_content(*, emit_output_variables_csv: bool = True) -> str:
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
        "MINICONDA_DIR=\"$HOME/miniconda3\"",
        "CONDA_ENV_NAME=\"peetsfea-runner-py312\"",
        "CONDA_PYTHON_PATH=\"$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python\"",
        "download_miniconda_installer() {",
        "  installer_path=\"$1\"",
        "  url=\"https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh\"",
        "  if command -v curl >/dev/null 2>&1; then",
        "    curl -fsSL \"$url\" -o \"$installer_path\"",
        "    return 0",
        "  fi",
        "  if command -v wget >/dev/null 2>&1; then",
        "    wget -qO \"$installer_path\" \"$url\"",
        "    return 0",
        "  fi",
        "  echo \"[ERROR] curl or wget is required to install Miniconda3\" >&2",
        "  exit 1",
        "}",
        "ensure_miniconda() {",
        "  if [ -x \"$MINICONDA_DIR/bin/conda\" ]; then",
        "    return 0",
        "  fi",
        "  if [ -e \"$MINICONDA_DIR\" ]; then",
        "    rm -rf \"$MINICONDA_DIR\"",
        "  fi",
        "  installer_path=$(mktemp /tmp/miniconda_installer.XXXXXX.sh)",
        "  download_miniconda_installer \"$installer_path\"",
        "  bash \"$installer_path\" -b -p \"$MINICONDA_DIR\"",
        "  rm -f \"$installer_path\"",
        "}",
        "ensure_conda_python312() {",
        "  if \"$MINICONDA_DIR/bin/conda\" tos --help >/dev/null 2>&1; then",
        "    \"$MINICONDA_DIR/bin/conda\" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true",
        "    \"$MINICONDA_DIR/bin/conda\" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r >/dev/null 2>&1 || true",
        "  fi",
        "  if [ -x \"$CONDA_PYTHON_PATH\" ]; then",
        "    major_minor=\"$($CONDA_PYTHON_PATH -c 'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")')\"",
        "    if [ \"$major_minor\" = \"3.12\" ]; then",
        "      return 0",
        "    fi",
        "  fi",
        "  \"$MINICONDA_DIR/bin/conda\" create -y -n \"$CONDA_ENV_NAME\" python=3.12",
        "}",
        "ensure_runner_venv() {",
        "  recreate=0",
        "  if [ -d \"$VENV_DIR\" ]; then",
        "    if [ ! -x \"$VENV_DIR/bin/python\" ]; then",
        "      recreate=1",
        "    else",
        "      major_minor=\"$($VENV_DIR/bin/python -c 'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")')\"",
        "      if [ \"$major_minor\" != \"3.12\" ]; then",
        "        recreate=1",
        "      fi",
        "    fi",
        "  else",
        "    recreate=1",
        "  fi",
        "  if [ \"$recreate\" -eq 1 ]; then",
        "    rm -rf \"$VENV_DIR\"",
        "    \"$CONDA_PYTHON_PATH\" -m venv \"$VENV_DIR\"",
        "  fi",
        "}",
        "if [ ! -x \"$VENV_DIR/bin/python\" ]; then",
        "  ensure_miniconda",
        "  ensure_conda_python312",
        "  ensure_runner_venv",
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
        "import csv",
        "import json",
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
        f"EMIT_OUTPUT_VARIABLES_CSV: bool = {str(bool(emit_output_variables_csv))}",
        "OUTPUT_VARIABLES_CSV_NAME: str = 'output_variables.csv'",
        "OUTPUT_VARIABLES_ERROR_LOG_NAME: str = 'output_variables.error.log'",
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
        "def build_output_variable_variations(hfss: Hfss) -> dict[str, list[str]]:",
        "    variations: dict[str, list[str]] = {}",
        "    for name, value in dict(hfss.available_variations.nominal_values).items():",
        "        if isinstance(value, list):",
        "            variations[str(name)] = [str(item) for item in value]",
        "        else:",
        "            variations[str(name)] = [str(value)]",
        "    if 'Freq' not in variations:",
        "        variations['Freq'] = ['All']",
        "    return variations",
        "",
        "",
        "def normalize_scalar(value: object) -> object:",
        "    if hasattr(value, 'item'):",
        "        try:",
        "            return value.item()",
        "        except Exception:",
        "            return value",
        "    return value",
        "",
        "",
        "def serialize_row_value(value: object) -> object:",
        "    value = normalize_scalar(value)",
        "    if value is None:",
        "        return ''",
        "    if isinstance(value, (str, int, float, bool)):",
        "        return value",
        "    if isinstance(value, (list, tuple)):",
        "        normalized_items = [serialize_row_value(item) for item in value]",
        "        if len(normalized_items) == 0:",
        "            return ''",
        "        if len(normalized_items) == 1:",
        "            return normalized_items[0]",
        "        return json.dumps(normalized_items)",
        "    return str(value)",
        "",
        "",
        "def build_output_variable_row_variations(hfss: Hfss) -> dict[str, object]:",
        "    row: dict[str, object] = {}",
        "    for name, value in dict(hfss.available_variations.nominal_values).items():",
        "        if name == 'Freq':",
        "            continue",
        "        row[str(name)] = serialize_row_value(value)",
        "    return row",
        "",
        "",
        "def normalize_report_column_name(column_name: object) -> str:",
        "    stripped = str(column_name).strip().strip('\"')",
        "    if not stripped:",
        "        return ''",
        "    if stripped.lower().startswith('freq '):",
        "        return 'Freq'",
        "    if ' [' in stripped:",
        "        stripped = stripped.split(' [', 1)[0]",
        "    return stripped",
        "",
        "",
        "def report_slug(name: str) -> str:",
        "    slug = ''.join(character if character.isalnum() or character in '._-' else '_' for character in str(name))",
        "    slug = slug.strip('._-').lower()",
        "    return slug or 'report'",
        "",
        "",
        "def get_all_report_names(hfss: Hfss) -> list[str]:",
        "    return sorted(str(report_name) for report_name in hfss.post.all_report_names)",
        "",
        "",
        "def delete_report_if_present(hfss: Hfss, report_name: str) -> None:",
        "    if report_name in get_all_report_names(hfss):",
        "        hfss.post.delete_report(report_name)",
        "",
        "",
        "def get_available_report_quantities_by_type(hfss: Hfss) -> dict[str, list[str]]:",
        "    report_quantities: dict[str, list[str]] = {}",
        "    for report_type in [str(report_type) for report_type in getattr(hfss.post, 'available_report_types', [])]:",
        "        try:",
        "            expressions = [",
        "                str(expression)",
        "                for expression in hfss.post.available_report_quantities(report_category=report_type)",
        "            ]",
        "        except Exception:",
        "            continue",
        "        if expressions:",
        "            report_quantities[report_type] = expressions",
        "    return report_quantities",
        "",
        "",
        "def create_output_variables_report(hfss: Hfss) -> str | None:",
        "    if not hfss.output_variables:",
        "        return None",
        "    plot_name = 'PEETSFEA__output_variables'",
        "    delete_report_if_present(hfss, plot_name)",
        "    report = hfss.post.create_report(",
        "        expressions=list(hfss.output_variables),",
        "        setup_sweep_name=hfss.nominal_adaptive,",
        "        variations=build_output_variable_variations(hfss),",
        "        primary_sweep_variable='Freq',",
        "        report_category='Output Variables',",
        "        plot_type='Data Table',",
        "        plot_name=plot_name,",
        "    )",
        "    if not report:",
        "        raise RuntimeError('Failed to create output variables report')",
        "    return plot_name",
        "",
        "",
        "def create_input_parameter_report(hfss: Hfss) -> str | None:",
        "    input_keys = {str(key) for key in build_output_variable_row_variations(hfss).keys()}",
        "    if not input_keys:",
        "        return None",
        "    for report_type, expressions in get_available_report_quantities_by_type(hfss).items():",
        "        selected = [",
        "            expression",
        "            for expression in expressions",
        "            if normalize_report_column_name(expression) in input_keys",
        "            and normalize_report_column_name(expression) != 'Freq'",
        "        ]",
        "        if not selected:",
        "            continue",
        "        plot_name = 'PEETSFEA__input_parameters'",
        "        delete_report_if_present(hfss, plot_name)",
        "        try:",
        "            report = hfss.post.create_report(",
        "                expressions=selected,",
        "                setup_sweep_name=hfss.nominal_adaptive,",
        "                report_category=report_type,",
        "                plot_type='Data Table',",
        "                plot_name=plot_name,",
        "            )",
        "        except Exception:",
        "            continue",
        "        if report:",
        "            return plot_name",
        "    return None",
        "",
        "",
        "def create_parameter_reports(hfss: Hfss) -> list[str]:",
        "    created_reports: list[str] = []",
        "    for report_type, expressions in get_available_report_quantities_by_type(hfss).items():",
        "        report_key = report_slug(report_type)",
        "        if report_key in {'output_variables', 'input_parameters'}:",
        "            continue",
        "        plot_name = f'PEETSFEA__{report_key}__summary'",
        "        delete_report_if_present(hfss, plot_name)",
        "        try:",
        "            report = hfss.post.create_report(",
        "                expressions=expressions,",
        "                setup_sweep_name=hfss.nominal_adaptive,",
        "                report_category=report_type,",
        "                plot_type='Data Table',",
        "                plot_name=plot_name,",
        "            )",
        "        except Exception:",
        "            report = False",
        "        if report:",
        "            created_reports.append(plot_name)",
        "            continue",
        "        for index, expression in enumerate(expressions, start=1):",
        "            item_plot_name = f'PEETSFEA__{report_key}__{index}'",
        "            delete_report_if_present(hfss, item_plot_name)",
        "            try:",
        "                report = hfss.post.create_report(",
        "                    expressions=[expression],",
        "                    setup_sweep_name=hfss.nominal_adaptive,",
        "                    report_category=report_type,",
        "                    plot_type='Data Table',",
        "                    plot_name=item_plot_name,",
        "                )",
        "            except Exception:",
        "                continue",
        "            if report:",
        "                created_reports.append(item_plot_name)",
        "    return created_reports",
        "",
        "",
        "def update_all_reports(hfss: Hfss) -> None:",
        "    report_setup = hfss.post.oreportsetup",
        "    update_all_reports_method = getattr(report_setup, 'UpdateAllReports', None)",
        "    if callable(update_all_reports_method):",
        "        update_all_reports_method()",
        "        return",
        "    update_reports_method = getattr(report_setup, 'UpdateReports', None)",
        "    if callable(update_reports_method):",
        "        update_reports_method(get_all_report_names(hfss))",
        "",
        "",
        "def write_synthetic_input_report(workdir: Path, hfss: Hfss) -> Path:",
        "    reports_dir = workdir / 'all_reports'",
        "    reports_dir.mkdir(parents=True, exist_ok=True)",
        "    report_path = reports_dir / 'peetsfea_input_parameters.csv'",
        "    row = build_output_variable_row_variations(hfss)",
        "    columns = sorted(row)",
        "    with report_path.open('w', encoding='utf-8', newline='') as handle:",
        "        writer = csv.writer(handle)",
        "        writer.writerow(columns)",
        "        writer.writerow([row.get(column, '') for column in columns])",
        "    return report_path",
        "",
        "",
        "def export_all_reports(hfss: Hfss, workdir: Path) -> dict[str, Path]:",
        "    reports_dir = workdir / 'all_reports'",
        "    reports_dir.mkdir(parents=True, exist_ok=True)",
        "    exported_paths: dict[str, Path] = {}",
        "    used_filenames: set[str] = set()",
        "    for report_name in get_all_report_names(hfss):",
        "        slug = report_slug(report_name)",
        "        filename = f'{slug}.csv'",
        "        suffix = 1",
        "        while filename in used_filenames:",
        "            suffix += 1",
        "            filename = f'{slug}_{suffix}.csv'",
        "        exported_path = Path(hfss.post.export_report_to_csv(str(reports_dir.resolve()), report_name))",
        "        canonical_path = reports_dir / filename",
        "        if exported_path.exists() and exported_path.resolve() != canonical_path.resolve():",
        "            exported_path.replace(canonical_path)",
        "            exported_path = canonical_path",
        "        used_filenames.add(exported_path.name)",
        "        exported_paths[report_name] = exported_path",
        "    return exported_paths",
        "",
        "",
        "def read_report_rows(csv_path: Path) -> list[dict[str, object]]:",
        "    with csv_path.open('r', encoding='utf-8', newline='') as handle:",
        "        reader = csv.DictReader(handle)",
        "        rows: list[dict[str, object]] = []",
        "        for parsed_row in reader:",
        "            if not parsed_row:",
        "                continue",
        "            row: dict[str, object] = {}",
        "            for raw_key, value in parsed_row.items():",
        "                key = normalize_report_column_name(raw_key)",
        "                if not key or key == 'Freq':",
        "                    continue",
        "                row[key] = serialize_row_value(value)",
        "            if any(str(value).strip() for value in row.values()):",
        "                rows.append(row)",
        "    return rows",
        "",
        "",
        "def merge_report_rows(target_row: dict[str, object], rows: list[dict[str, object]], report_name: str, *, bare_first_row: bool) -> None:",
        "    slug = report_slug(report_name)",
        "    for row_index, report_row in enumerate(rows, start=1):",
        "        for key, value in report_row.items():",
        "            if bare_first_row and row_index == 1:",
        "                target_row.setdefault(key, value)",
        "            else:",
        "                target_row[f'{slug}__r{row_index}__{key}'] = value",
        "",
        "",
        "def first_value(values: object) -> object:",
        "    if values is None:",
        "        return ''",
        "    try:",
        "        if len(values) == 0:",
        "            return ''",
        "        return normalize_scalar(values[0])",
        "    except Exception:",
        "        return normalize_scalar(values)",
        "",
        "",
        "def has_nonzero_imaginary(value: object) -> bool:",
        "    if value in ('', None):",
        "        return False",
        "    try:",
        "        return abs(float(value)) > 1e-12",
        "    except Exception:",
        "        return True",
        "",
        "",
        "def write_output_variables_csv(workdir: Path, row: dict[str, object]) -> None:",
        "    output_csv_path = workdir / OUTPUT_VARIABLES_CSV_NAME",
        "    ordered_columns = ['source_aedt_path', 'source_case_dir', 'source_aedt_name']",
        "    remaining_columns = sorted(column for column in row if column not in ordered_columns)",
        "    columns = ordered_columns + remaining_columns",
        "    with output_csv_path.open('w', encoding='utf-8', newline='') as handle:",
        "        writer = csv.writer(handle)",
        "        writer.writerow(columns)",
        "        writer.writerow([row.get(column, '') for column in columns])",
        "",
        "",
        "def reopen_solved_project(project_file: Path, grpc_port: int) -> Hfss:",
        "    return Hfss(",
        "        project=str(project_file.resolve()),",
        "        non_graphical=(not USE_GRAPHIC),",
        "        new_desktop=False,",
        "        machine='localhost',",
        "        port=grpc_port,",
        "        close_on_exit=True,",
        "    )",
        "",
        "",
        "def extract_output_variables_csv(hfss: Hfss, workdir: Path, project_file: Path) -> None:",
        "    if not EMIT_OUTPUT_VARIABLES_CSV:",
        "        return",
        "    row: dict[str, object] = {",
        "        'source_aedt_path': str(project_file.resolve()),",
        "        'source_case_dir': str(workdir.resolve()),",
        "        'source_aedt_name': project_file.name,",
        "    }",
        "    reports_dir = workdir / 'all_reports'",
        "    reports_dir.mkdir(parents=True, exist_ok=True)",
        "    errors: list[str] = []",
        "    canonical_input_report_name: str | None = None",
        "    canonical_output_report_name: str | None = None",
        "    canonical_input_path: Path | None = None",
        "    canonical_output_path: Path | None = None",
        "    exported_paths: dict[str, Path] = {}",
        "    try:",
        "        canonical_input_report_name = create_input_parameter_report(hfss)",
        "        canonical_output_report_name = create_output_variables_report(hfss)",
        "        create_parameter_reports(hfss)",
        "        update_all_reports(hfss)",
        "        exported_paths = export_all_reports(hfss, workdir)",
        "    except Exception as exc:",
        "        errors.append(str(exc))",
        "    if canonical_input_report_name and canonical_input_report_name in exported_paths:",
        "        canonical_input_path = exported_paths[canonical_input_report_name]",
        "    if canonical_output_report_name and canonical_output_report_name in exported_paths:",
        "        canonical_output_path = exported_paths[canonical_output_report_name]",
        "    if canonical_input_path is None:",
        "        try:",
        "            canonical_input_path = write_synthetic_input_report(workdir, hfss)",
        "        except Exception as exc:",
        "            errors.append(f'synthetic input report failed: {exc}')",
        "    if canonical_input_path is not None and canonical_input_path.exists():",
        "        merge_report_rows(",
        "            row,",
        "            read_report_rows(canonical_input_path),",
        "            canonical_input_report_name or 'input_parameters',",
        "            bare_first_row=True,",
        "        )",
        "    if canonical_output_path is not None and canonical_output_path.exists():",
        "        merge_report_rows(",
        "            row,",
        "            read_report_rows(canonical_output_path),",
        "            canonical_output_report_name or 'output_variables',",
        "            bare_first_row=True,",
        "        )",
        "    else:",
        "        errors.append('Output Variables report export missing')",
        "    for report_name, report_path in sorted(exported_paths.items()):",
        "        if canonical_input_path is not None and report_path.resolve() == canonical_input_path.resolve():",
        "            continue",
        "        if canonical_output_path is not None and report_path.resolve() == canonical_output_path.resolve():",
        "            continue",
        "        try:",
        "            merge_report_rows(row, read_report_rows(report_path), report_name, bare_first_row=False)",
        "        except Exception as exc:",
        "            errors.append(f'Failed to merge report {report_name}: {exc}')",
        "    write_output_variables_csv(workdir, row)",
        "    if errors:",
        "        raise RuntimeError('; '.join(errors))",
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
        "    cores = int(os.environ.get('PEETS_SLOT_CORES', '4'))",
        "    tasks = int(os.environ.get('PEETS_SLOT_TASKS', '1'))",
        "    remove_lock_files(workdir)",
        "    grpc_port = get_available_port()",
        "    ansys_process = launch_ansys_grpc_server(grpc_port)",
        "    solve_hfss = None",
        "    reopened_hfss = None",
        "    try:",
        "        solve_hfss = Hfss(",
        "            non_graphical=(not USE_GRAPHIC),",
        "            new_desktop=False,",
        "            machine='localhost',",
        "            port=grpc_port,",
        "            close_on_exit=True,",
        "        )",
        "        solve_hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)",
        "        # Batch solve output is already persisted in project artifacts.",
        "        # save_project() can intermittently fail on shared Ansoft temp path",
        "        # even when simulation completed normally; treat it as non-fatal.",
        "        try:",
        "            solve_hfss.save_project()",
        "        except Exception as exc:",
        "            print(f'[WARN] save_project failed but solve completed: {exc}')",
        "        if solve_hfss is not None:",
        "            solve_hfss.release_desktop(close_projects=True, close_desktop=False)",
        "            solve_hfss = None",
        "        try:",
        "            reopened_hfss = reopen_solved_project(project_file, grpc_port)",
        "            extract_output_variables_csv(reopened_hfss, workdir, project_file)",
        "            error_log_path = workdir / OUTPUT_VARIABLES_ERROR_LOG_NAME",
        "            if error_log_path.exists():",
        "                error_log_path.unlink()",
        "        except Exception as exc:",
        "            (workdir / OUTPUT_VARIABLES_ERROR_LOG_NAME).write_text(str(exc), encoding='utf-8')",
        "            print(f'[WARN] output variable csv export failed but solve completed: {exc}')",
        "    finally:",
        "        if reopened_hfss is not None:",
        "            reopened_hfss.release_desktop(close_projects=True, close_desktop=True)",
        "        elif solve_hfss is not None:",
        "            solve_hfss.release_desktop(close_projects=True, close_desktop=True)",
        "        stop_process(ansys_process)",
        "",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "PY",
        "\"$VENV_DIR/bin/python\" run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _build_windows_remote_job_script_content() -> str:
    lines = [
        "$ErrorActionPreference = 'Stop'",
        "$VenvDir = Join-Path $HOME '.peetsfea-runner-venv'",
        "$RunSim = @'",
        "from __future__ import annotations",
        "",
        "import os",
        "from pathlib import Path",
        "from ansys.aedt.core import Hfss",
        "",
        "AEDT_FILENAME = 'project.aedt'",
        "",
        "def remove_lock_files(workdir: Path) -> None:",
        "    for lock_file in workdir.glob('*.lock'):",
        "        lock_file.unlink(missing_ok=True)",
        "",
        "def main() -> None:",
        "    workdir = Path.cwd()",
        "    project_file = workdir / AEDT_FILENAME",
        "    if not project_file.exists():",
        "        raise FileNotFoundError(project_file)",
        "    tmpdir = workdir / 'tmp'",
        "    tmpdir.mkdir(parents=True, exist_ok=True)",
        "    os.environ['TMPDIR'] = str(tmpdir)",
        "    cores = int(os.environ.get('PEETS_SLOT_CORES', '4'))",
        "    tasks = int(os.environ.get('PEETS_SLOT_TASKS', '1'))",
        "    remove_lock_files(workdir)",
        "    hfss = Hfss(non_graphical=True, new_desktop=True, close_on_exit=True)",
        "    try:",
        "        hfss.solve_in_batch(file_name='./project.aedt', cores=cores, tasks=tasks)",
        "        hfss.save_project()",
        "    finally:",
        "        hfss.release_desktop(close_projects=True, close_desktop=True)",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "'@",
        "Set-Content -Path run_sim.py -Value $RunSim",
        "& (Join-Path $VenvDir 'Scripts\\python.exe') run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _write_remote_job_script(tmpdir: Path, *, config: RemoteJobConfig) -> Path:
    script = tmpdir / "remote_job.sh"
    script.write_text(
        _build_remote_job_script_content(
            emit_output_variables_csv=getattr(config, "emit_output_variables_csv", True),
        ),
        encoding="utf-8",
    )
    return script


def _write_windows_remote_job_script(tmpdir: Path) -> Path:
    script = tmpdir / "remote_job.ps1"
    script.write_text(_build_windows_remote_job_script_content(), encoding="utf-8")
    return script


def _write_remote_dispatch_script(tmpdir: Path, *, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> Path:
    if _remote_platform(config) == "windows":
        script = tmpdir / "remote_dispatch.ps1"
        script.write_text(
            _build_windows_remote_dispatch_script_content(
                config=config,
                remote_job_dir=remote_job_dir,
                case_count=case_count,
            ),
            encoding="utf-8",
        )
        return script
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
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"REMOTE_JOB_DIR={shlex.quote(remote_path)}",
        "export REMOTE_JOB_DIR",
        "cd \"$REMOTE_JOB_DIR\"",
        "tar -czf - remote_job.sh project_*.aedt | \\",
        "  " + _build_noninteractive_srun_command(
            config=config,
            remote_job_dir=remote_path,
            case_count=case_count,
        ),
    ]
    return "\n".join(lines) + "\n"


def _build_windows_remote_dispatch_script_content(*, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> str:
    tasks_per_slot = int(getattr(config, "tasks_per_slot", 1))
    lines = [
        "$ErrorActionPreference = 'Stop'",
        f"$RemoteJobDir = {remote_job_dir!r}",
        "Set-Location $RemoteJobDir",
        "New-Item -ItemType Directory -Force -Path '.\\results' | Out-Null",
    ]
    for case_index in range(1, case_count + 1):
        case_name = f"case_{case_index:02d}"
        source_name = f"project_{case_index:02d}.aedt"
        lines.extend(
            [
                f"New-Item -ItemType Directory -Force -Path '.\\{case_name}' | Out-Null",
                f"Copy-Item -Force '.\\{source_name}' '.\\{case_name}\\project.aedt'",
                f"Set-Location '.\\{case_name}'",
                f"$env:PEETS_SLOT_CORES = '{config.cores_per_slot}'",
                f"$env:PEETS_SLOT_TASKS = '{tasks_per_slot}'",
                "powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File ..\\remote_job.ps1 *> run.log",
                "if ($LASTEXITCODE -eq $null) { $LASTEXITCODE = 0 }",
                "Set-Content -Path exit.code -Value $LASTEXITCODE",
                "Set-Location ..",
            ]
        )
    lines.extend(
        [
            "$failed = 0",
            "Remove-Item -Force case_summary.txt, failed.count -ErrorAction SilentlyContinue",
            f"1..{case_count} | ForEach-Object {{",
            "  $caseDir = ('case_{0:d2}' -f $_)",
            "  if (Test-Path (Join-Path $caseDir 'exit.code')) { $code = (Get-Content (Join-Path $caseDir 'exit.code')).Trim() } else { $code = '97' }",
            "  Add-Content -Path case_summary.txt -Value ($caseDir + ':' + $code)",
            "  if ([int]$code -ne 0) { $failed += 1 }",
            "}",
            "Set-Content -Path failed.count -Value $failed",
            "Compress-Archive -Path case_*, case_summary.txt, failed.count -DestinationPath results.zip -Force",
            "Write-Output ('__PEETS_FAILED_COUNT__:{0}' -f $failed)",
            "Write-Output '__PEETS_CASE_SUMMARY_BEGIN__'",
            "Get-Content case_summary.txt",
            "Write-Output '__PEETS_CASE_SUMMARY_END__'",
            "Write-Output '__PEETS_RESULTS_TGZ_BEGIN__'",
            "[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $RemoteJobDir 'results.zip')))",
            "Write-Output '__PEETS_RESULTS_TGZ_END__'",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_noninteractive_srun_command(*, config: RemoteJobConfig, remote_job_dir: str, case_count: int) -> str:
    payload = _build_worker_payload_script_content(config=config, case_count=case_count)
    exclude_nodes = _slurm_exclude_nodes(config)
    exclude_arg = f" --exclude={','.join(exclude_nodes)}" if exclude_nodes else ""
    return (
        f"srun -D /tmp -p {config.partition} -N {config.nodes} -n {config.ntasks} "
        f"-c {config.cpus_per_job} --mem={config.mem} --time={config.time_limit}{exclude_arg} "
        f"bash -lc {shlex.quote(payload)}"
    )


def _build_worker_payload_script_content(
    *,
    config: RemoteJobConfig,
    case_count: int,
    run_id: str | None = None,
    worker_id: str | None = None,
) -> str:
    max_parallel = max(1, int(getattr(config, "slots_per_job", case_count)))
    tasks_per_slot = int(getattr(config, "tasks_per_slot", 1))
    total_allocated_mem_mb = _memory_to_mb(getattr(config, "mem", ""))
    slot_allocated_mem_mb = max(
        1,
        int(total_allocated_mem_mb / max_parallel) if total_allocated_mem_mb is not None else 0,
    ) if total_allocated_mem_mb is not None else 0
    control_plane_host = getattr(config, "control_plane_host", "127.0.0.1")
    control_plane_port = int(getattr(config, "control_plane_port", 8765))
    control_plane_ssh_target = _control_plane_ssh_target(config)
    control_plane_return_host = _control_plane_return_host(config)
    control_plane_return_user = _control_plane_return_user(config)
    control_plane_return_port = _control_plane_return_port(config)
    heartbeat_interval_seconds = max(5, int(getattr(config, "tunnel_recovery_grace_seconds", 30)))
    payload_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "workdir=$(mktemp -d /tmp/peetsfea-slot.XXXXXX)",
        "cleanup() {",
        "  rc=$?",
        "  cd /tmp >/dev/null 2>&1 || true",
        "  rm -rf \"$workdir\"",
        "  if [ -n \"${REMOTE_JOB_DIR:-}\" ]; then",
        "    rm -rf \"$REMOTE_JOB_DIR\" >/dev/null 2>&1 || true",
        "  fi",
        "  exit \"$rc\"",
        "}",
        "trap cleanup EXIT",
        "cd \"$workdir\"",
        "tar -xzf -",
        f"PEETS_CONTROL_RUN_ID={shlex.quote(run_id or '')}",
        f"PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id or '')}",
        f"PEETS_CONTROL_HOST={shlex.quote(control_plane_host)}",
        f"PEETS_CONTROL_PORT={control_plane_port}",
        f"PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
        f"PEETS_CONTROL_HEARTBEAT_INTERVAL={heartbeat_interval_seconds}",
        f"max_parallel={max_parallel}",
        "PEETS_CONTROL_LOCAL_PORT=$((PEETS_CONTROL_PORT + 1000))",
        "PEETS_TUNNEL_SOCKET=\"$workdir/control-plane.sock\"",
        "PEETS_TUNNEL_SESSION_ID=\"${SLURM_JOB_ID:-nojob}-${PEETS_CONTROL_WORKER_ID:-worker}-$$\"",
        "PEETS_CONTROL_API_URL=\"http://127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}\"",
        "case_pids=()",
        "count_case_jobs() {",
        "  local pid",
        "  local -a active_pids=()",
        "  for pid in \"${case_pids[@]}\"; do",
        "    if kill -0 \"$pid\" >/dev/null 2>&1; then",
        "      active_pids+=(\"$pid\")",
        "    fi",
        "  done",
        "  case_pids=(\"${active_pids[@]}\")",
        "  printf '%s\\n' \"${#case_pids[@]}\"",
        "}",
        "control_plane_post() {",
        "  endpoint=\"$1\"",
        "  payload=\"$2\"",
        "  control_python=\"$(command -v python3 || command -v python || true)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_PAYLOAD=\"$payload\" \"$control_python\" - <<'PY'",
        "from __future__ import annotations",
        "import os",
        "import urllib.request",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'],",
        "    data=os.environ['CONTROL_PAYLOAD'].encode('utf-8'),",
        "    headers={'Content-Type': 'application/json'},",
        "    method='POST',",
        ")",
        "with urllib.request.urlopen(req, timeout=5) as resp:",
        "    resp.read()",
        "PY",
        "}",
        "classify_return_path_stage() {",
        "  details=\"$1\"",
        "  case \"$details\" in",
        "    *\"Could not resolve hostname\"*|*\"Name or service not known\"*) echo RETURN_PATH_DNS_FAILURE ;;",
        "    *\"Connection refused\"*) echo RETURN_PATH_PORT_MISMATCH ;;",
        "    *\"Permission denied\"*|*\"Host key verification failed\"*) echo RETURN_PATH_AUTH_FAILURE ;;",
        "    *\"Connection timed out\"*|*\"No route to host\"*|*\"Operation timed out\"*) echo RETURN_PATH_CONNECT_FAILURE ;;",
        "    *) echo CONTROL_TUNNEL_LOST ;;",
        "  esac",
        "}",
        "start_control_tunnel() {",
        "  if [ -z \"$PEETS_CONTROL_RETURN_HOST\" ] || [ -z \"$PEETS_CONTROL_RETURN_USER\" ]; then",
        "    return 1",
        "  fi",
        "  ssh -M -S \"$PEETS_TUNNEL_SOCKET\" -fnNT \\",
        "    -p \"$PEETS_CONTROL_RETURN_PORT\" \\",
        "    -o BatchMode=yes \\",
        "    -o ExitOnForwardFailure=yes \\",
        "    -o StrictHostKeyChecking=no \\",
        "    -o UserKnownHostsFile=/dev/null \\",
        "    -L 127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}:127.0.0.1:${PEETS_CONTROL_PORT} \\",
        "    \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\"",
        "}",
        "stop_control_tunnel() {",
        "  if [ -S \"$PEETS_TUNNEL_SOCKET\" ]; then",
        "    ssh -p \"$PEETS_CONTROL_RETURN_PORT\" -S \"$PEETS_TUNNEL_SOCKET\" -O exit \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\" >/dev/null 2>&1 || true",
        "  fi",
        "}",
        "emit_control_register() {",
        "  control_plane_post /internal/workers/register \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"slurm_job_id\\\":\\\"${SLURM_JOB_ID:-}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_heartbeat() {",
        "  control_plane_post /internal/workers/heartbeat \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_degraded() {",
        "  stage=\"$1\"",
        "  reason=\"$2\"",
        "  control_plane_post /internal/workers/degraded \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"stage\\\":\\\"${stage}\\\",\\\"reason\\\":\\\"${reason}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_recovered() {",
        "  control_plane_post /internal/workers/recovered \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_node_snapshot() {",
        "  total_mem_mb=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)",
        "  avail_mem_mb=$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)",
        "  used_mem_mb=$(( total_mem_mb - avail_mem_mb ))",
        "  process_count=$(find /proc -maxdepth 1 -type d -regex '.*/[0-9]+' 2>/dev/null | wc -l | awk '{print $1+0}')",
        "  tmp_stats=$(df -Pm \"${TMPDIR:-/tmp}\" | awk 'NR==2 {print $2\" \"$3\" \"$4}')",
        "  tmp_total_mb=$(printf '%s' \"$tmp_stats\" | awk '{print $1}')",
        "  tmp_used_mb=$(printf '%s' \"$tmp_stats\" | awk '{print $2}')",
        "  tmp_free_mb=$(printf '%s' \"$tmp_stats\" | awk '{print $3}')",
        "  load_1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)",
        "  load_5=$(awk '{print $2}' /proc/loadavg 2>/dev/null || echo 0)",
        "  load_15=$(awk '{print $3}' /proc/loadavg 2>/dev/null || echo 0)",
        "  control_plane_post /internal/resources/node \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"allocated_mem_mb\\\":${total_mem_mb:-0},\\\"total_mem_mb\\\":${total_mem_mb:-0},\\\"used_mem_mb\\\":${used_mem_mb:-0},\\\"free_mem_mb\\\":${avail_mem_mb:-0},\\\"load_1\\\":${load_1:-0},\\\"load_5\\\":${load_5:-0},\\\"load_15\\\":${load_15:-0},\\\"tmp_total_mb\\\":${tmp_total_mb:-0},\\\"tmp_used_mb\\\":${tmp_used_mb:-0},\\\"tmp_free_mb\\\":${tmp_free_mb:-0},\\\"process_count\\\":${process_count:-0},\\\"running_worker_count\\\":1,\\\"active_slot_count\\\":$(count_case_jobs)}\" >/dev/null 2>&1 || true",
        "}",
        "emit_worker_snapshot() {",
        "  active_slots=\"${1:-0}\"",
        "  idle_slots=\"${2:-0}\"",
        "  rss_mb=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$$/status 2>/dev/null || echo 0)",
        "  cpu_pct=$(ps -p $$ -o %cpu= 2>/dev/null | awk '{print $1+0}' || echo 0)",
        "  process_count=$(ps --no-headers --ppid $$ 2>/dev/null | wc -l | awk '{print $1+0}')",
        "  control_plane_post /internal/resources/worker \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"slurm_job_id\\\":\\\"${SLURM_JOB_ID:-}\\\",\\\"configured_slots\\\":${max_parallel},\\\"active_slots\\\":${active_slots},\\\"idle_slots\\\":${idle_slots},\\\"rss_mb\\\":${rss_mb:-0},\\\"cpu_pct\\\":${cpu_pct:-0},\\\"tunnel_state\\\":\\\"CONNECTED\\\",\\\"process_count\\\":${process_count:-0}}\" >/dev/null 2>&1 || true",
        "}",
        "emit_slot_snapshot() {",
        "  slot_id=\"$1\"",
        "  slot_state=\"$2\"",
        "  artifact_bytes=\"$3\"",
        "  used_mem_mb=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$$/status 2>/dev/null || echo 0)",
        f"  allocated_mem_mb={slot_allocated_mem_mb}",
        "  load_1=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)",
        "  rss_mb=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$$/status 2>/dev/null || echo 0)",
        "  cpu_pct=$(ps -p $$ -o %cpu= 2>/dev/null | awk '{print $1+0}' || echo 0)",
        "  active_process_count=$(ps --no-headers --ppid $$ 2>/dev/null | wc -l | awk '{print $1+0}')",
        "  process_count=$(( active_process_count + 1 ))",
        "  progress_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)",
        "  control_plane_post /internal/resources/slot \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"slot_id\\\":\\\"${slot_id}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"allocated_mem_mb\\\":${allocated_mem_mb:-0},\\\"used_mem_mb\\\":${used_mem_mb:-0},\\\"load_1\\\":${load_1:-0},\\\"rss_mb\\\":${rss_mb:-0},\\\"cpu_pct\\\":${cpu_pct:-0},\\\"process_count\\\":${process_count:-0},\\\"active_process_count\\\":${active_process_count:-0},\\\"artifact_bytes\\\":${artifact_bytes:-0},\\\"progress_ts\\\":\\\"${progress_ts}\\\",\\\"state\\\":\\\"${slot_state}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "heartbeat_loop() {",
        "  while sleep \"$PEETS_CONTROL_HEARTBEAT_INTERVAL\"; do",
        "    emit_control_heartbeat",
        "    emit_node_snapshot",
        "    active_now=$(count_case_jobs)",
        "    idle_now=$(( max_parallel - active_now ))",
        "    if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
        "    emit_worker_snapshot \"$active_now\" \"$idle_now\"",
        "  done",
        "}",
        "heartbeat_pid=''",
        "if start_control_tunnel 2> control_tunnel_bootstrap.err; then",
        "  emit_control_register",
        "  emit_node_snapshot",
        "  emit_worker_snapshot 0 \"$max_parallel\"",
        "  heartbeat_loop &",
        "  heartbeat_pid=$!",
        "else",
        "  bootstrap_error=$(tr '\\n' ' ' < control_tunnel_bootstrap.err 2>/dev/null | sed 's/[[:space:]]\\+/ /g' | sed 's/^ //;s/ $//')",
        "  if [ -z \"$bootstrap_error\" ]; then bootstrap_error='ssh tunnel bootstrap failed'; fi",
        "  emit_control_degraded \"$(classify_return_path_stage \"$bootstrap_error\")\" \"$bootstrap_error\"",
        "fi",
        "teardown_control_plane() {",
        "  if [ -n \"$heartbeat_pid\" ]; then",
        "    kill \"$heartbeat_pid\" >/dev/null 2>&1 || true",
        "    wait \"$heartbeat_pid\" >/dev/null 2>&1 || true",
        "  fi",
        "  stop_control_tunnel",
        "}",
        "trap teardown_control_plane EXIT",
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
            "for i in $(seq 1 " + str(case_count) + "); do",
            "  while [ \"$(count_case_jobs)\" -ge \"$max_parallel\" ]; do",
            "    wait -n \"${case_pids[@]}\" || true",
            "  done",
            "  case_dir=$(printf 'case_%02d' \"$i\")",
            "  (",
            "    cd \"$case_dir\"",
            "    mkdir -p tmp",
            "    export TMPDIR=\"$PWD/tmp\"",
            "    emit_slot_snapshot \"$case_dir\" \"RUNNING\" 0",
            (
                f"    if PEETS_SLOT_CORES={config.cores_per_slot} "
                f"PEETS_SLOT_TASKS={tasks_per_slot} bash ../remote_job.sh > run.log 2>&1; then"
            ),
            "      rc=0",
            "    else",
            "      rc=$?",
            "    fi",
            "    echo \"$rc\" > exit.code",
            "    find . -maxdepth 1 -name '*.lock' -type f -delete >/dev/null 2>&1 || true",
            "    rm -rf tmp >/dev/null 2>&1 || true",
            "    artifact_bytes=$(du -sb . 2>/dev/null | awk '{print $1+0}')",
            "    if [ \"$rc\" -eq 0 ]; then",
            "      emit_slot_snapshot \"$case_dir\" \"SUCCEEDED\" \"$artifact_bytes\"",
            "    else",
            "      emit_slot_snapshot \"$case_dir\" \"FAILED\" \"$artifact_bytes\"",
            "    fi",
            "  ) &",
            "  case_pids+=(\"$!\")",
            "done",
            "while [ \"$(count_case_jobs)\" -gt 0 ]; do",
            "  wait -n \"${case_pids[@]}\" || true",
            "  active_now=$(count_case_jobs)",
            "  idle_now=$(( max_parallel - active_now ))",
            "  if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
            "  emit_worker_snapshot \"$active_now\" \"$idle_now\"",
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
            "touch results.tgz.ready",
            "printf '__PEETS_FAILED_COUNT__:%s\\n' \"$(cat failed.count)\"",
            "printf '__PEETS_CASE_SUMMARY_BEGIN__\\n'",
            "cat case_summary.txt",
            "printf '__PEETS_CASE_SUMMARY_END__\\n'",
            "printf '__PEETS_RESULTS_TGZ_BEGIN__\\n'",
            "base64 -w0 \"$archive_path\"",
            "printf '\\n__PEETS_RESULTS_TGZ_END__\\n'",
            "emit_control_recovered",
        ]
    )
    payload = "\n".join(payload_lines)
    return payload


def _write_remote_worker_payload_script(
    tmpdir: Path,
    *,
    config: RemoteJobConfig,
    case_count: int,
    run_id: str | None = None,
    worker_id: str | None = None,
) -> Path:
    script = tmpdir / "remote_worker_payload.sh"
    script.write_text(
        _build_worker_payload_script_content(
            config=config,
            case_count=case_count,
            run_id=run_id,
            worker_id=worker_id,
        ),
        encoding="utf-8",
    )
    return script


def _write_remote_sbatch_script(
    tmpdir: Path,
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    run_id: str | None = None,
    worker_id: str | None = None,
) -> Path:
    script = tmpdir / "remote_sbatch.sh"
    script.write_text(
        _build_remote_sbatch_script_content(
            config=config,
            remote_job_dir=remote_job_dir,
            run_id=run_id,
            worker_id=worker_id,
        ),
        encoding="utf-8",
    )
    return script


def _build_remote_sbatch_script_content(
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    run_id: str | None = None,
    worker_id: str | None = None,
) -> str:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    control_plane_ssh_target = _control_plane_ssh_target(config)
    control_plane_return_host = _control_plane_return_host(config)
    control_plane_return_user = _control_plane_return_user(config)
    control_plane_return_port = _control_plane_return_port(config)
    exclude_nodes = _slurm_exclude_nodes(config)
    exclude_lines = [f"#SBATCH --exclude={','.join(exclude_nodes)}"] if exclude_nodes else []
    return "\n".join(
        [
            "#!/bin/bash",
            f"#SBATCH -p {config.partition}",
            f"#SBATCH -N {config.nodes}",
            f"#SBATCH -n {config.ntasks}",
            f"#SBATCH -c {config.cpus_per_job}",
            f"#SBATCH --mem={config.mem}",
            f"#SBATCH --time={config.time_limit}",
            *exclude_lines,
            "#SBATCH -o slurm-%j.out",
            "#SBATCH -e slurm-%j.err",
            "set -euo pipefail",
            f"REMOTE_JOB_DIR={shlex.quote(remote_path)}",
            f"export PEETS_CONTROL_RUN_ID={shlex.quote(run_id or '')}",
            f"export PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id or '')}",
            f"export PEETS_CONTROL_HOST={shlex.quote(getattr(config, 'control_plane_host', '127.0.0.1'))}",
            f"export PEETS_CONTROL_PORT={int(getattr(config, 'control_plane_port', 8765))}",
            f"export PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
            f"export PEETS_CONTROL_RETURN_HOST={shlex.quote(control_plane_return_host)}",
            f"export PEETS_CONTROL_RETURN_USER={shlex.quote(control_plane_return_user)}",
            f"export PEETS_CONTROL_RETURN_PORT={control_plane_return_port}",
            f"export PEETS_CONTROL_HEARTBEAT_INTERVAL={max(5, int(getattr(config, 'tunnel_recovery_grace_seconds', 30)))}",
            "export REMOTE_JOB_DIR",
            "cd \"$REMOTE_JOB_DIR\"",
            "tar -czf - remote_job.sh project_*.aedt | /bin/bash ./remote_worker_payload.sh > worker.stdout 2> worker.stderr",
        ]
    ) + "\n"


def _parse_sbatch_job_id(output: str) -> str:
    text = output.strip()
    if not text:
        raise WorkflowError("Unable to parse sbatch job id.", exit_code=EXIT_CODE_SLURM_FAILURE)
    token = text.splitlines()[-1].strip().split(";", 1)[0].strip()
    if not token.isdigit():
        raise WorkflowError("Unable to parse sbatch job id.", exit_code=EXIT_CODE_SLURM_FAILURE)
    return token


def _count_screen_slots(output: str) -> int:
    tokens = output.strip()
    if not tokens:
        return 0
    slot_tokens = re.findall(r"(?:(?<=^)|(?<=\s))\d+\$?(?=\s)", tokens)
    return len(slot_tokens)


def _build_case_slot_command(*, case_index: int, cores_per_slot: int, tasks_per_slot: int) -> str:
    case_name = f"case_{case_index:02d}"
    return (
        f"cd {shlex.quote(case_name)} && "
        f"PEETS_SLOT_CORES={cores_per_slot} PEETS_SLOT_TASKS={tasks_per_slot} bash ../remote_job.sh > run.log 2>&1; "
        "echo $? > exit.code"
    )


def _build_wait_all_command(parallel_slots: int) -> str:
    return (
        "while true; do "
        "done_count=0; "
        f"for i in $(seq 1 {parallel_slots}); do "
        "case_dir=$(printf 'case_%02d' \"$i\"); "
        "[ -f \"$case_dir/exit.code\" ] && done_count=$((done_count+1)); "
        "done; "
        f"[ \"$done_count\" -eq {parallel_slots} ] && break; "
        "sleep 5; "
        "done"
    )


def _build_case_aggregation_command(parallel_slots: int) -> str:
    return (
        "rm -f case_summary.txt failed.count; "
        "failed=0; "
        f"for i in $(seq 1 {parallel_slots}); do "
        "case_dir=$(printf 'case_%02d' \"$i\"); "
        "if [ -f \"$case_dir/exit.code\" ]; then "
        "code=$(cat \"$case_dir/exit.code\"); "
        "else "
        "code=97; "
        "echo \"$code\" > \"$case_dir/exit.code\"; "
        "fi; "
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
        raise WorkflowError(
            f"{stage} failed: {details}",
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            stdout=submission.stdout,
            stderr=submission.stderr,
        )
    raise WorkflowError(
        f"{stage} failed with return code {submission.return_code}",
        exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
        stdout=submission.stdout,
        stderr=submission.stderr,
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


def _classify_return_path_failure_stage(message: str) -> str | None:
    normalized = message.lower()
    if "could not resolve hostname" in normalized or "name or service not known" in normalized:
        return "RETURN_PATH_DNS_FAILURE"
    if "connection refused" in normalized:
        return "RETURN_PATH_PORT_MISMATCH"
    if "permission denied" in normalized or "host key verification failed" in normalized:
        return "RETURN_PATH_AUTH_FAILURE"
    if "connection timed out" in normalized or "operation timed out" in normalized or "no route to host" in normalized:
        return "RETURN_PATH_CONNECT_FAILURE"
    return None


def _categorize_failure(*, exit_code: int, message: str, failed_case_lines: Sequence[str]) -> str:
    normalized = message.lower()
    if _classify_return_path_failure_stage(message) is not None:
        return "return_path"
    if _is_launch_transient_message(normalized):
        return "launch_transient"
    if "readiness" in normalized or "preflight" in normalized or "bootstrap" in normalized:
        return "readiness"
    if (
        "storage" in normalized
        or "inode_pct=" in normalized
        or "free_mb=" in normalized
        or "no space left" in normalized
        or "disk quota" in normalized
        or ("scp upload failed" in normalized and "write remote" in normalized)
    ):
        return "storage"
    if exit_code == EXIT_CODE_REMOTE_CLEANUP_FAILURE or "cleanup" in normalized:
        return "cleanup"
    if exit_code == EXIT_CODE_DOWNLOAD_FAILURE or "archive" in normalized or "download" in normalized:
        return "collect"
    if failed_case_lines:
        return "solve"
    return "launch"


def _write_failure_artifacts(
    *,
    local_job_dir: Path,
    exit_code: int,
    failure_category: str | None,
    message: str,
    failed_case_lines: Sequence[str],
    stdout: str,
    stderr: str,
) -> None:
    local_job_dir.mkdir(parents=True, exist_ok=True)
    (local_job_dir / "bundle.exit.code").write_text(str(exit_code), encoding="utf-8")
    if stdout:
        (local_job_dir / "remote_stdout.log").write_text(stdout, encoding="utf-8")
    if stderr:
        (local_job_dir / "remote_stderr.log").write_text(stderr, encoding="utf-8")
    combined = "\n".join(part for part in (stdout.strip(), stderr.strip()) if part)
    if combined:
        (local_job_dir / "remote_submission.log").write_text(combined + "\n", encoding="utf-8")
    if failure_category:
        (local_job_dir / "failure_category.txt").write_text(failure_category, encoding="utf-8")
    if message:
        (local_job_dir / "failure_reason.txt").write_text(message, encoding="utf-8")
    if failed_case_lines:
        (local_job_dir / "failed_case_lines.txt").write_text("\n".join(failed_case_lines) + "\n", encoding="utf-8")


def _time_limit_to_seconds(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 3:
        return 5 * 3600
    hours, minutes, seconds = parts
    try:
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except ValueError:
        return 5 * 3600
