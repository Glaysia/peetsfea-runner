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
from .runtime_policy import (
    JOB_DISK_FILESYSTEM_SIZE_GB,
    JOB_TMPFS_SIZE_GB,
    RUNTIME_JANITOR_MIN_TTL_SECONDS,
    remote_runtime_root,
)


class RemoteJobConfig(Protocol):
    host: str
    remote_root: str
    partition: str
    slurm_partitions_allowlist: tuple[str, ...]
    nodes: int
    ntasks: int
    cpus_per_job: int
    mem: str
    time_limit: str
    slots_per_job: int
    worker_payload_slot_limit: int
    slot_min_concurrency: int
    slot_max_concurrency: int
    slot_memory_pressure_high_watermark_percent: int
    slot_memory_pressure_resume_watermark_percent: int
    slot_memory_probe_interval_seconds: int
    lease_ttl_seconds: int
    lease_heartbeat_seconds: int
    worker_idle_poll_seconds: int
    slot_request_backoff_seconds: int
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
    ssh_config_path: str
    remote_container_runtime: str
    remote_container_image: str
    remote_container_ansys_root: str
    remote_ansys_executable: str
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


def _remote_container_runtime(config: RemoteJobConfig) -> str:
    return str(getattr(config, "remote_container_runtime", "none")).strip().lower() or "none"


def _remote_container_image(config: RemoteJobConfig) -> str:
    return str(getattr(config, "remote_container_image", "")).strip()


def _remote_container_ansys_root(config: RemoteJobConfig) -> str:
    value = str(getattr(config, "remote_container_ansys_root", "")).strip()
    return value or "/opt/ohpc/pub/Electronics/v252"


def _remote_host_ansys_mount_root(config: RemoteJobConfig) -> str:
    value = _remote_container_ansys_root(config).rstrip("/")
    if value.endswith("/AnsysEM"):
        return value
    return f"{value}/AnsysEM"


def _remote_host_ansys_base_root(config: RemoteJobConfig) -> str:
    value = _remote_container_ansys_root(config).rstrip("/")
    if value.endswith("/AnsysEM"):
        return str(Path(value).parent).rstrip("/")
    return value


def _remote_ansys_executable(config: RemoteJobConfig) -> str:
    explicit = str(getattr(config, "remote_ansys_executable", "")).strip()
    if explicit:
        return explicit
    if _remote_container_runtime(config) == "enroot":
        return "/mnt/AnsysEM/ansysedt"
    return "/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt"


def _remote_ansysem_root(config: RemoteJobConfig) -> str:
    executable = _remote_ansys_executable(config).strip()
    if not executable:
        return "/opt/ohpc/pub/Electronics/v252/AnsysEM"
    parent = Path(executable).parent.as_posix()
    return parent or "/opt/ohpc/pub/Electronics/v252/AnsysEM"


def _remote_ssh_port(config: RemoteJobConfig) -> int:
    return int(getattr(config, "remote_ssh_port", 22))


def _remote_ssh_config_path(config: RemoteJobConfig) -> str:
    return str(getattr(config, "ssh_config_path", "")).strip()


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
    return int(getattr(config, "control_plane_return_port", 22))


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
    command = ["ssh", "-p", str(_remote_ssh_port(config))]
    ssh_config_path = _remote_ssh_config_path(config)
    if ssh_config_path:
        command.extend(["-F", ssh_config_path])
    command.extend(args)
    return command


def _scp_command(config: RemoteJobConfig, *args: str) -> list[str]:
    command = ["scp", "-P", str(_remote_ssh_port(config))]
    ssh_config_path = _remote_ssh_config_path(config)
    if ssh_config_path:
        command.extend(["-F", ssh_config_path])
    command.extend(args)
    return command


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


def _slurm_partitions_allowlist(config: RemoteJobConfig) -> tuple[str, ...]:
    raw = getattr(config, "slurm_partitions_allowlist", ())
    if isinstance(raw, str):
        iterable = raw.split(",")
    else:
        iterable = raw
    normalized = [str(item).strip() for item in iterable if str(item).strip()]
    if normalized:
        return tuple(dict.fromkeys(normalized))
    fallback = str(getattr(config, "partition", "")).strip()
    return (fallback,) if fallback else ()


def _slurm_partition_value(config: RemoteJobConfig) -> str:
    return ",".join(_slurm_partitions_allowlist(config))


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
        _cleanup_remote_workspace_best_effort(config, remote_job_dir=resolved_remote_job_dir)
    except WorkflowError as exc:
        _download_remote_debug_artifacts(
            config,
            remote_job_dir=resolved_remote_job_dir,
            local_job_dir=local_job_dir,
            slurm_job_id=exc.slurm_job_id or slurm_job_id,
        )
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
    if _remote_platform(config) != "linux":
        return None
    try:
        runtime_root = _resolve_remote_path(config=config, path=_remote_runtime_root(config))
        scratch_root = _resolve_remote_path(config=config, path=getattr(config, "remote_root", "~/aedt_runs"))
        ttl_minutes = max(1, int(RUNTIME_JANITOR_MIN_TTL_SECONDS // 60))
        command = "\n".join(
            [
                "set +e",
                f"runtime_root={_double_quoted_shell_value(runtime_root)}",
                f"scratch_root={_double_quoted_shell_value(scratch_root)}",
                f"current_run={_double_quoted_shell_value(run_id)}",
                f"ttl_minutes={ttl_minutes}",
                'mkdir -p "$runtime_root" "$scratch_root" >/dev/null 2>&1 || true',
                'if [ -d "$runtime_root" ]; then',
                '  find "$runtime_root" -mindepth 1 -maxdepth 1 -type d -mmin +"$ttl_minutes" -exec rm -rf {} + >/dev/null 2>&1 || true',
                "fi",
                'if [ -d "$scratch_root" ]; then',
                '  find "$scratch_root" -mindepth 1 -maxdepth 1 -type d ! -name "_runtime" ! -name "$current_run" -mmin +"$ttl_minutes" -exec rm -rf {} + >/dev/null 2>&1 || true',
                "fi",
            ]
        )
        _run_subprocess_with_transport_retry(
            _ssh_command(config, config.host, f"bash -lc {shlex.quote(command)}"),
            stage="remote cleanup",
            exit_code=EXIT_CODE_REMOTE_CLEANUP_FAILURE,
        )
    except WorkflowError:
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
    remote_user: str | None = None
    if "${USER}" in path or "$USER" in path:
        remote_user = _get_remote_user(config=config)
        path = path.replace("${USER}", remote_user).replace("$USER", remote_user)
    if path == "/tmp/$USER" or path.startswith("/tmp/$USER/"):
        remote_user = remote_user or _get_remote_user(config=config)
        if path == "/tmp/$USER":
            return f"/tmp/{remote_user}"
        return f"/tmp/{remote_user}{path[len('/tmp/$USER'):]}"
    if path == "/tmp/peetsfea-runner" or path.startswith("/tmp/peetsfea-runner/"):
        remote_user = remote_user or _get_remote_user(config=config)
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
    runtime_root = _resolve_remote_path(config=config, path=_remote_runtime_root(config))
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
        _ssh_command(config, config.host, f"mkdir -p {shlex.quote(remote_path)} {shlex.quote(runtime_root)}"),
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


def _download_remote_debug_artifacts(
    config: RemoteJobConfig,
    *,
    remote_job_dir: str,
    local_job_dir: Path,
    slurm_job_id: str | None = None,
) -> None:
    if _remote_platform(config) != "linux":
        return
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    local_archive = local_job_dir / "remote_debug.tgz"
    slurm_targets = [f"slurm-{slurm_job_id}.out", f"slurm-{slurm_job_id}.err"] if slurm_job_id else []
    script_lines = [
        "set +e",
        "shopt -s nullglob",
        f"cd {shlex.quote(remote_path)} || exit 0",
        "files=()",
    ]
    for name in ("launch_probe.txt", "worker.stdout", "worker.stderr", "control_tunnel_bootstrap.err", *slurm_targets):
        script_lines.append(f"[ -f {shlex.quote(name)} ] && files+=({shlex.quote(name)})")
    script_lines.extend(
        [
            "for path in case_*/run.log case_*/exit.code case_*/batch.log case_*/report_export.error.log case_*/license_diagnostics.txt case_*/runtime_logs.json case_*/ansys_grpc.stdout.log case_*/ansys_grpc.stderr.log case_*/pyaedt*.log case_*/remote_job.sh case_*/run_sim.py case_*/project.aedt case_*/project.aedt.q case_*/project.aedt.q.complete case_*/project.aedt.q.completed case_*/all_reports case_*/design_outputs case_*/project.aedtresults; do",
            '  [ -f "$path" ] && files+=("$path")',
            '  [ -d "$path" ] && files+=("$path")',
            "done",
            'if [ "${#files[@]}" -eq 0 ]; then exit 0; fi',
            'tar -czf - "${files[@]}"',
        ]
    )
    completed = subprocess.run(
        _ssh_command(config, config.host, "bash -lc " + shlex.quote("\n".join(script_lines))),
        check=False,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode != 0 or not completed.stdout:
        return
    local_job_dir.mkdir(parents=True, exist_ok=True)
    local_archive.write_bytes(completed.stdout)
    try:
        _extract_local_results_archive(local_job_dir=local_job_dir, archive_name=local_archive.name)
    except WorkflowError:
        return


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
        if not _is_missing_slurm_job_error(details):
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


def _is_missing_slurm_job_error(message: str) -> bool:
    normalized = message.strip().lower()
    if not normalized:
        return False
    return "invalid job id specified" in normalized or "invalid job id" in normalized


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
    payload_slot_limit = max(1, int(getattr(config, "worker_payload_slot_limit", getattr(config, "slots_per_job", 1))))
    return [
        SlotInput(slot_id=f"case_{index:02d}", input_path=aedt_path)
        for index in range(1, payload_slot_limit + 1)
    ]


def _remote_path_for_shell(*, config: RemoteJobConfig, path: str) -> str:
    if _remote_platform(config) == "windows":
        return path
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return f"$HOME/{path[2:]}"
    return path


def _remote_runtime_root(config: RemoteJobConfig) -> str:
    return remote_runtime_root(getattr(config, "remote_root", "~/aedt_runs"))


def _remote_runtime_root_shell_path(*, config: RemoteJobConfig) -> str:
    return _remote_path_for_shell(config=config, path=_remote_runtime_root(config))


def _double_quoted_shell_value(value: str) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def _build_remote_job_script_content(*, emit_output_variables_csv: bool = True) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "export LANG=C.UTF-8",
        "export LC_ALL=C.UTF-8",
        "unset LANGUAGE",
        "export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81",
        "export ANSYSEM_ROOT252=/mnt/AnsysEM",
        "export PEETS_RAMDISK_ROOT=\"${PEETS_RAMDISK_ROOT:-$HOME/ansys_ram}\"",
        "export PEETS_RAMDISK_TMPDIR=\"${PEETS_RAMDISK_TMPDIR:-$PEETS_RAMDISK_ROOT/tmp}\"",
        "export PEETS_RAMDISK_ANSYS_WORK_DIR=\"${PEETS_RAMDISK_ANSYS_WORK_DIR:-$PEETS_RAMDISK_ROOT/ansys_tmp}\"",
        "export PEETS_DISK_ROOT=\"${PEETS_DISK_ROOT:-$HOME}\"",
        "export PEETS_DISK_TMPDIR=\"${PEETS_DISK_TMPDIR:-$PEETS_DISK_ROOT/tmp}\"",
        "export PEETS_DISK_ANSYS_WORK_DIR=\"${PEETS_DISK_ANSYS_WORK_DIR:-$PEETS_DISK_ROOT/ansys_tmp}\"",
        "mkdir -p \"$HOME\" \"$PEETS_RAMDISK_TMPDIR\" \"$PEETS_RAMDISK_ANSYS_WORK_DIR\" \"$PEETS_DISK_TMPDIR\" \"$PEETS_DISK_ANSYS_WORK_DIR\"",
        "export ANSYS_WORK_DIR=\"${ANSYS_WORK_DIR:-$PEETS_RAMDISK_ANSYS_WORK_DIR}\"",
        "export TEMP=\"${TEMP:-$PEETS_DISK_TMPDIR}\"",
        "export TMPDIR=\"${TMPDIR:-$PEETS_DISK_TMPDIR}\"",
        "",
        "IMAGE_PYTHON=\"/opt/miniconda3/bin/python\"",
        "if [ ! -x \"$IMAGE_PYTHON\" ]; then",
        "  echo \"[ERROR] image python is missing: $IMAGE_PYTHON\" >&2",
        "  exit 1",
        "fi",
        "export PEETS_ORIGINAL_LD_LIBRARY_PATH=\"${LD_LIBRARY_PATH:-}\"",
        "BASE_PREFIX=\"$($IMAGE_PYTHON -c 'import sys; print(sys.base_prefix)')\"",
        "IMAGE_LD_LIBRARY_PATH=\"$BASE_PREFIX/lib:${LD_LIBRARY_PATH:-}\"",
        "if ! LD_LIBRARY_PATH=\"$IMAGE_LD_LIBRARY_PATH\" \"$IMAGE_PYTHON\" -m uv --version >/dev/null 2>&1; then",
        "  echo \"[ERROR] uv is not available in image python: $IMAGE_PYTHON\" >&2",
        "  exit 1",
        "fi",
        "if ! LD_LIBRARY_PATH=\"$IMAGE_LD_LIBRARY_PATH\" \"$IMAGE_PYTHON\" -c \"import ansys.aedt.core, pandas, pyvista\" >/dev/null 2>&1; then",
        "  echo \"[ERROR] required Python modules are missing from image python: $IMAGE_PYTHON\" >&2",
        "  exit 1",
        "fi",
        "cat > run_sim.py <<'PY'",
        "from __future__ import annotations",
        "",
        "import csv",
        "from contextlib import contextmanager",
        "import json",
        "import os",
        "import re",
        "import shutil",
        "import socket",
        "import subprocess",
        "import sys",
        "import time",
        "import traceback",
        "from pathlib import Path",
        "from typing import Any",
        "",
        "from ansys.aedt.core import Desktop, Hfss, Icepak, Maxwell3d, Q2d, Q3d, settings",
        "",
        "",
        "AEDT_FILENAME: str = 'project.aedt'",
        f"EMIT_OUTPUT_VARIABLES_CSV: bool = {str(bool(emit_output_variables_csv))}",
        "REPORT_EXPORT_ERROR_LOG_NAME: str = 'report_export.error.log'",
        "DESIGN_OUTPUTS_DIR_NAME: str = 'design_outputs'",
        "SYNTHETIC_REPORT_FILENAMES: set[str] = {'peetsfea_input_parameters.csv'}",
        "LICENSE_DIAGNOSTICS_NAME: str = 'license_diagnostics.txt'",
        "USE_GRAPHIC: bool = False",
        "ANSYS_EXECUTABLE: str = '/mnt/AnsysEM/ansysedt'",
        "",
        "",
        "settings.wait_for_license = True",
        "settings.skip_license_check = False",
        "for env_key in ('ANSYSLMD_LICENSE_FILE', 'ANSYSEM_ROOT252', 'ANS_IGNOREOS'):",
        "    env_value = os.environ.get(env_key, '').strip()",
        "    if env_value:",
        "        settings.aedt_environment_variables[env_key] = env_value",
        "",
        "",
        "def remove_lock_files(workdir: Path) -> None:",
        "    patterns = (",
        "        '*.lock',",
        "        '*.q',",
        "        '*.q.complete',",
        "        '*.q.completed',",
        "        'project.aedtresults/**/*.lock',",
        "        'project.aedtresults/**/*.lck',",
        "    )",
        "    seen_paths: set[Path] = set()",
        "    for pattern in patterns:",
        "        for candidate in workdir.glob(pattern):",
        "            lock_path = candidate.resolve()",
        "            if lock_path in seen_paths or not candidate.is_file():",
        "                continue",
        "            seen_paths.add(lock_path)",
        "            try:",
        "                candidate.unlink()",
        "                print(f'[INFO] removed stale lock {candidate.relative_to(workdir)}')",
        "            except OSError as exc:",
        "                print(f'[WARN] failed to remove stale lock {candidate.relative_to(workdir)}: {exc}')",
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
        "    cmd = [ANSYS_EXECUTABLE, '-ng', '-waitforlicense', '-grpcsrv', str(port)]",
        "    stdout_path = Path('ansys_grpc.stdout.log')",
        "    stderr_path = Path('ansys_grpc.stderr.log')",
        "    stdout_handle = stdout_path.open('w', encoding='utf-8')",
        "    stderr_handle = stderr_path.open('w', encoding='utf-8')",
        "    launch_env = build_ansys_launch_env()",
        "    runtime_cwd = Path(os.environ.get('TMPDIR', str(Path.cwd() / 'tmp'))).resolve()",
        "    runtime_cwd.mkdir(parents=True, exist_ok=True)",
        "    process = subprocess.Popen(cmd, cwd=str(runtime_cwd), env=launch_env, stdout=stdout_handle, stderr=stderr_handle)",
        "    deadline = time.time() + 180",
        "    while time.time() < deadline:",
        "        if process.poll() is not None:",
        "            break",
        "        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:",
        "            probe.settimeout(1)",
        "            if probe.connect_ex(('127.0.0.1', port)) == 0:",
        "                stdout_handle.close()",
        "                stderr_handle.close()",
        "                return process",
        "        time.sleep(2)",
        "    stdout_handle.close()",
        "    stderr_handle.close()",
        "    stdout_tail = ''",
        "    stderr_tail = ''",
        "    if stdout_path.exists():",
        "        stdout_tail = stdout_path.read_text(encoding='utf-8', errors='ignore')[-2000:]",
        "    if stderr_path.exists():",
        "        stderr_tail = stderr_path.read_text(encoding='utf-8', errors='ignore')[-2000:]",
        "    raise RuntimeError(",
        "        f'Failed to start ansysedt gRPC server on port {port}; '",
        "        f'returncode={process.poll()} stdout_tail={stdout_tail!r} stderr_tail={stderr_tail!r}'",
        "    )",
        "",
        "",
        "def build_ansys_launch_env() -> dict[str, str]:",
        "    launch_env: dict[str, str] = {}",
        "    allowed_keys = {",
        "        'ANSYSLIC_DIR',",
        "        'ANSYSLMD_LICENSE_FILE',",
        "        'ANSYSEM_ROOT252',",
        "        'ANS_IGNOREOS',",
        "        'HOME',",
        "        'HOSTNAME',",
        "        'LANG',",
        "        'LC_ALL',",
        "        'LOGNAME',",
        "        'PATH',",
        "        'PWD',",
        "        'SHELL',",
        "        'TERM',",
        "        'ANSYS_WORK_DIR',",
        "        'TMP',",
        "        'TEMP',",
        "        'TMPDIR',",
        "        'USER',",
        "    }",
        "    allowed_prefixes = ('CUDA_', 'NVIDIA_', 'OMP_', 'SLURM_')",
        "    for key, value in os.environ.items():",
        "        if key in allowed_keys or key.startswith(allowed_prefixes):",
        "            launch_env[key] = value",
        "    launch_env['PATH'] = '/usr/bin:/bin:/usr/sbin:/sbin'",
        "    original_ld_library_path = os.environ.get('PEETS_ORIGINAL_LD_LIBRARY_PATH', '').strip()",
        "    if original_ld_library_path:",
        "        launch_env['LD_LIBRARY_PATH'] = original_ld_library_path",
        "    else:",
        "        launch_env.pop('LD_LIBRARY_PATH', None)",
        "    return launch_env",
        "",
        "",
        "@contextmanager",
        "def use_ansys_launch_env() -> object:",
        "    original_ld_library_path = os.environ.get('LD_LIBRARY_PATH')",
        "    launch_env = build_ansys_launch_env()",
        "    if 'LD_LIBRARY_PATH' in launch_env:",
        "        os.environ['LD_LIBRARY_PATH'] = launch_env['LD_LIBRARY_PATH']",
        "    else:",
        "        os.environ.pop('LD_LIBRARY_PATH', None)",
        "    try:",
        "        yield",
        "    finally:",
        "        if original_ld_library_path is None:",
        "            os.environ.pop('LD_LIBRARY_PATH', None)",
        "        else:",
        "            os.environ['LD_LIBRARY_PATH'] = original_ld_library_path",
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
        "def snapshot_pyaedt_logs() -> set[Path]:",
        "    runtime_tmp = Path(os.environ.get('TMPDIR', str(Path.cwd() / 'tmp'))).resolve()",
        "    return {path.resolve() for path in runtime_tmp.glob('pyaedt_*.log') if path.is_file()}",
        "",
        "",
        "def copy_runtime_logs(workdir: Path, existing_logs: set[Path]) -> None:",
        "    copied_paths: set[Path] = set()",
        "    runtime_tmp = Path(os.environ.get('TMPDIR', str(Path.cwd() / 'tmp'))).resolve()",
        "    runtime_logs = [",
        "        path.resolve()",
        "        for path in runtime_tmp.glob('pyaedt_*.log')",
        "        if path.is_file()",
        "    ]",
        "    for index, source_path in enumerate(sorted(runtime_logs), start=1):",
        "        destination_name = 'pyaedt.log' if index == 1 else f'pyaedt_{index}.log'",
        "        destination_path = workdir / destination_name",
        "        try:",
        "            if source_path in existing_logs and destination_path.exists():",
        "                continue",
        "            shutil.copy2(source_path, destination_path)",
        "            copied_paths.add(destination_path)",
        "        except Exception:",
        "            continue",
        "    for log_name in ('ansys_grpc.stdout.log', 'ansys_grpc.stderr.log'):",
        "        source_path = workdir / log_name",
        "        if not source_path.exists():",
        "            continue",
        "        copied_paths.add(source_path)",
        "    if copied_paths:",
        "        manifest_path = workdir / 'runtime_logs.json'",
        "        manifest_path.write_text(",
        "            json.dumps(sorted(path.name for path in copied_paths), ensure_ascii=True, indent=2),",
        "            encoding='utf-8',",
        "        )",
        "",
        "",
        "def run_command_capture(command: list[str], *, env: dict[str, str] | None = None, timeout_seconds: int = 30) -> tuple[int, str]:",
        "    try:",
        "        completed = subprocess.run(",
        "            command,",
        "            stdout=subprocess.PIPE,",
        "            stderr=subprocess.STDOUT,",
        "            text=True,",
        "            encoding='utf-8',",
        "            errors='ignore',",
        "            timeout=timeout_seconds,",
        "            env=env,",
        "            check=False,",
        "        )",
        "    except Exception as exc:",
        "        return 1, f'{type(exc).__name__}: {exc}'",
        "    return int(completed.returncode), completed.stdout",
        "",
        "",
        "def write_license_diagnostics(workdir: Path) -> None:",
        "    diagnostics_path = workdir / LICENSE_DIAGNOSTICS_NAME",
        "    lines: list[str] = []",
        "    lines.append(f'ANSYSLMD_LICENSE_FILE={os.environ.get(\"ANSYSLMD_LICENSE_FILE\", \"\")}')",
        "    lines.append(f'ANSYSEM_ROOT252={os.environ.get(\"ANSYSEM_ROOT252\", \"\")}')",
        "    lines.append(f'hostname={socket.gethostname()}')",
        "    licensing_root = Path('/opt/ansys/v252/licensingclient/linx64')",
        "    ansysli_util = licensing_root / 'ansysli_util'",
        "    lmutil = licensing_root / 'lmutil'",
        "    launch_env = build_ansys_launch_env()",
        "    commands: list[tuple[str, list[str], int]] = []",
        "    if ansysli_util.exists():",
        "        commands.extend(",
        "            [",
        "                ('ansysli_util -envvar', [str(ansysli_util), '-envvar'], 30),",
        "                ('ansysli_util -hostinfo 172.16.10.81', [str(ansysli_util), '-hostinfo', '172.16.10.81'], 30),",
        "                ('ansysli_util -checkexists EMAG', [str(ansysli_util), '-checkexists', 'EMAG'], 30),",
        "                ('ansysli_util -checkexists PREPPOST', [str(ansysli_util), '-checkexists', 'PREPPOST'], 30),",
        "            ]",
        "        )",
        "    if lmutil.exists():",
        "        commands.append(",
        "            (",
        "                'lmutil lmstat -a -c 1055@172.16.10.81',",
        "                [str(lmutil), 'lmstat', '-a', '-c', '1055@172.16.10.81'],",
        "                30,",
        "            )",
        "        )",
        "    for label, command, timeout_seconds in commands:",
        "        lines.append('')",
        "        lines.append(f'=== {label} ===')",
        "        return_code, output = run_command_capture(command, env=launch_env, timeout_seconds=timeout_seconds)",
        "        lines.append(f'returncode={return_code}')",
        "        lines.append(output.rstrip())",
        "    diagnostics_path.write_text('\\n'.join(lines).rstrip() + '\\n', encoding='utf-8')",
        "",
        "",
        "def get_nominal_variation_values(app: Any) -> dict[str, object]:",
        "    try:",
        "        return dict(app.available_variations.nominal_values)",
        "    except Exception:",
        "        return {}",
        "",
        "",
        "def build_output_variable_variations(app: Any) -> dict[str, list[str]]:",
        "    variations: dict[str, list[str]] = {}",
        "    for name, value in get_nominal_variation_values(app).items():",
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
        "def serialize_variable_manager_value(variable: object) -> object:",
        "    for attribute_name in ('expression', 'string_value', 'evaluated_value', 'value'):",
        "        try:",
        "            attribute_value = getattr(variable, attribute_name)",
        "        except Exception:",
        "            continue",
        "        if callable(attribute_value):",
        "            continue",
        "        if attribute_value not in (None, ''):",
        "            return serialize_row_value(attribute_value)",
        "    return serialize_row_value(variable)",
        "",
        "",
        "def build_variable_manager_row(app: Any) -> dict[str, object]:",
        "    row: dict[str, object] = {}",
        "    variable_manager = getattr(app, 'variable_manager', None)",
        "    if variable_manager is None:",
        "        return row",
        "    scoped_variables = (",
        "        ('design_var', getattr(variable_manager, 'design_variables', {})),",
        "        ('project_var', getattr(variable_manager, 'project_variables', {})),",
        "    )",
        "    for scope_key, variable_map in scoped_variables:",
        "        try:",
        "            items = dict(variable_map).items()",
        "        except Exception:",
        "            continue",
        "        for name, variable in items:",
        "            normalized_name = str(name).strip()",
        "            if not normalized_name or normalized_name == 'Freq':",
        "                continue",
        "            row[f'{scope_key}__{normalized_name}'] = serialize_variable_manager_value(variable)",
        "    return row",
        "",
        "",
        "def build_output_variable_row_variations(app: Any) -> dict[str, object]:",
        "    row: dict[str, object] = {}",
        "    for name, value in get_nominal_variation_values(app).items():",
        "        if name == 'Freq':",
        "            continue",
        "        row[str(name)] = serialize_row_value(value)",
        "    for key, value in build_variable_manager_row(app).items():",
        "        row.setdefault(key, value)",
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
        "def design_dir_name(design_name: str) -> str:",
        "    name = str(design_name).strip()",
        "    if not name:",
        "        return 'design'",
        "    safe_name = ''.join('_' if character in '/\\\\\\x00' else character for character in name)",
        "    safe_name = safe_name.strip()",
        "    return safe_name or 'design'",
        "",
        "",
        "def reports_dir_for_design_name(workdir: Path, design_name: str) -> Path:",
        "    reports_dir = design_output_dir(workdir, design_name) / 'reports'",
        "    reports_dir.mkdir(parents=True, exist_ok=True)",
        "    return reports_dir",
        "",
        "",
        "def reports_dir_for_design(workdir: Path, app: Any) -> Path:",
        "    return reports_dir_for_design_name(workdir, getattr(app, 'design_name', '') or 'design')",
        "",
        "",
        "def get_all_report_names(app: Any) -> list[str]:",
        "    return sorted(str(report_name) for report_name in app.post.all_report_names)",
        "",
        "",
        "def delete_report_if_present(app: Any, report_name: str) -> None:",
        "    if report_name in get_all_report_names(app):",
        "        app.post.delete_report(report_name)",
        "",
        "",
        "def get_available_report_quantities_by_type(app: Any) -> dict[str, list[str]]:",
        "    report_quantities: dict[str, list[str]] = {}",
        "    for report_type in [str(report_type) for report_type in getattr(app.post, 'available_report_types', [])]:",
        "        try:",
        "            expressions = [",
        "                str(expression)",
        "                for expression in app.post.available_report_quantities(report_category=report_type)",
        "            ]",
        "        except Exception:",
        "            continue",
        "        if expressions:",
        "            report_quantities[report_type] = expressions",
        "    return report_quantities",
        "",
        "",
        "def create_output_variables_report(app: Any) -> str | None:",
        "    if not getattr(app, 'output_variables', None):",
        "        return None",
        "    plot_name = 'PEETSFEA__output_variables'",
        "    delete_report_if_present(app, plot_name)",
        "    report = app.post.create_report(",
        "        expressions=list(app.output_variables),",
        "        setup_sweep_name=app.nominal_adaptive,",
        "        variations=build_output_variable_variations(app),",
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
        "def create_input_parameter_report(app: Any) -> str | None:",
        "    input_keys = {str(key) for key in build_output_variable_row_variations(app).keys()}",
        "    if not input_keys:",
        "        return None",
        "    for report_type, expressions in get_available_report_quantities_by_type(app).items():",
        "        selected = [",
        "            expression",
        "            for expression in expressions",
        "            if normalize_report_column_name(expression) in input_keys",
        "            and normalize_report_column_name(expression) != 'Freq'",
        "        ]",
        "        if not selected:",
        "            continue",
        "        plot_name = 'PEETSFEA__input_parameters'",
        "        delete_report_if_present(app, plot_name)",
        "        try:",
        "            report = app.post.create_report(",
        "                expressions=selected,",
        "                setup_sweep_name=app.nominal_adaptive,",
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
        "def create_parameter_reports(app: Any) -> list[str]:",
        "    created_reports: list[str] = []",
        "    for report_type, expressions in get_available_report_quantities_by_type(app).items():",
        "        report_key = report_slug(report_type)",
        "        if report_key in {'output_variables', 'input_parameters'}:",
        "            continue",
        "        plot_name = f'PEETSFEA__{report_key}__summary'",
        "        delete_report_if_present(app, plot_name)",
        "        try:",
        "            report = app.post.create_report(",
        "                expressions=expressions,",
        "                setup_sweep_name=app.nominal_adaptive,",
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
        "            delete_report_if_present(app, item_plot_name)",
        "            try:",
        "                report = app.post.create_report(",
        "                    expressions=[expression],",
        "                    setup_sweep_name=app.nominal_adaptive,",
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
        "def update_all_reports(app: Any) -> None:",
        "    report_setup = app.post.oreportsetup",
        "    update_all_reports_method = getattr(report_setup, 'UpdateAllReports', None)",
        "    if callable(update_all_reports_method):",
        "        update_all_reports_method()",
        "        return",
        "    update_reports_method = getattr(report_setup, 'UpdateReports', None)",
        "    if callable(update_reports_method):",
        "        update_reports_method(get_all_report_names(app))",
        "",
        "",
        "def write_synthetic_input_report(workdir: Path, design_name: str, app: Any) -> Path:",
        "    reports_dir = reports_dir_for_design_name(workdir, design_name)",
        "    report_path = reports_dir / 'peetsfea_input_parameters.csv'",
        "    row = build_output_variable_row_variations(app)",
        "    columns = sorted(row)",
        "    with report_path.open('w', encoding='utf-8', newline='') as handle:",
        "        writer = csv.writer(handle)",
        "        writer.writerow(columns)",
        "        writer.writerow([row.get(column, '') for column in columns])",
        "    return report_path",
        "",
        "",
        "def export_all_reports(app: Any, workdir: Path, design_name: str) -> tuple[dict[str, Path], list[str]]:",
        "    reports_dir = reports_dir_for_design_name(workdir, design_name)",
        "    exported_paths: dict[str, Path] = {}",
        "    export_errors: list[str] = []",
        "    used_filenames: set[str] = set()",
        "    for report_name in get_all_report_names(app):",
        "        slug = report_slug(report_name)",
        "        filename = f'{slug}.csv'",
        "        suffix = 1",
        "        while filename in used_filenames:",
        "            suffix += 1",
        "            filename = f'{slug}_{suffix}.csv'",
        "        try:",
        "            exported_path = Path(app.post.export_report_to_csv(str(reports_dir.resolve()), report_name))",
        "        except Exception as exc:",
        "            export_errors.append(f'report export failed report={report_name}: {exc}')",
        "            continue",
        "        canonical_path = reports_dir / filename",
        "        if exported_path.exists() and exported_path.resolve() != canonical_path.resolve():",
        "            exported_path.replace(canonical_path)",
        "            exported_path = canonical_path",
        "        used_filenames.add(exported_path.name)",
        "        exported_paths[report_name] = exported_path",
        "    return exported_paths, export_errors",
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
        "def write_csv_row(output_csv_path: Path, row: dict[str, object], *, ordered_columns: list[str]) -> None:",
        "    output_csv_path.parent.mkdir(parents=True, exist_ok=True)",
        "    remaining_columns = sorted(column for column in row if column not in ordered_columns)",
        "    columns = ordered_columns + remaining_columns",
        "    with output_csv_path.open('w', encoding='utf-8', newline='') as handle:",
        "        writer = csv.writer(handle)",
        "        writer.writerow(columns)",
        "        writer.writerow([row.get(column, '') for column in columns])",
        "",
        "",
        "def design_output_dir(workdir: Path, design_name: str) -> Path:",
        "    output_dir = workdir / DESIGN_OUTPUTS_DIR_NAME / design_dir_name(design_name)",
        "    output_dir.mkdir(parents=True, exist_ok=True)",
        "    return output_dir",
        "",
        "",
        "def write_design_output_error_log(workdir: Path, design_name: str, errors: list[str]) -> Path:",
        "    error_log_path = design_output_dir(workdir, design_name) / REPORT_EXPORT_ERROR_LOG_NAME",
        "    error_log_path.write_text('; '.join(errors), encoding='utf-8')",
        "    return error_log_path",
        "",
        "",
        "def count_design_report_files(workdir: Path, design_name: str, *, include_synthetic: bool = True) -> int:",
        "    reports_dir = reports_dir_for_design_name(workdir, design_name)",
        "    report_paths = [path for path in reports_dir.glob('*.csv') if path.is_file()]",
        "    if include_synthetic:",
        "        return len(report_paths)",
        "    return sum(1 for path in report_paths if path.name not in SYNTHETIC_REPORT_FILENAMES)",
        "",
        "",
        "def count_synthetic_design_report_files(workdir: Path, design_name: str) -> int:",
        "    reports_dir = reports_dir_for_design_name(workdir, design_name)",
        "    return sum(1 for path in reports_dir.glob('*.csv') if path.is_file() and path.name in SYNTHETIC_REPORT_FILENAMES)",
        "",
        "",
        "def classify_design_report_status(*, native_report_count: int, synthetic_report_count: int, errors: list[str]) -> str:",
        "    if native_report_count > 0 and errors:",
        "        return 'report_export_error_present'",
        "    if native_report_count > 0:",
        "        return 'native_reports_present'",
        "    if synthetic_report_count > 0:",
        "        return 'synthetic_only_fallback'",
        "    return 'no_reports_exported'",
        "",
        "",
        "def write_design_outputs_manifest(workdir: Path, rows: list[dict[str, object]]) -> None:",
        "    manifest_path = workdir / DESIGN_OUTPUTS_DIR_NAME / 'index.csv'",
        "    ordered_columns = ['design_name', 'reports_dir', 'report_count', 'native_report_count', 'synthetic_report_count', 'status', 'error_log']",
        "    manifest_path.parent.mkdir(parents=True, exist_ok=True)",
        "    with manifest_path.open('w', encoding='utf-8', newline='') as handle:",
        "        writer = csv.DictWriter(handle, fieldnames=ordered_columns)",
        "        writer.writeheader()",
        "        for row in rows:",
        "            writer.writerow({column: row.get(column, '') for column in ordered_columns})",
        "",
        "",
        "def connect_grpc_desktop(grpc_port: int, *, close_on_exit: bool) -> Desktop:",
        "    return Desktop(",
        "        non_graphical=(not USE_GRAPHIC),",
        "        new_desktop=False,",
        "        machine='localhost',",
        "        port=grpc_port,",
        "        close_on_exit=close_on_exit,",
        "    )",
        "",
        "",
        "def release_desktop_session(handle: Any, *, close_projects: bool, close_desktop: bool) -> None:",
        "    if handle is None:",
        "        return",
        "    release = getattr(handle, 'release_desktop', None)",
        "    if not callable(release):",
        "        return",
        "    last_error: Exception | None = None",
        "    for kwargs in (",
        "        {'close_projects': close_projects, 'close_desktop': close_desktop},",
        "        {'close_projects': close_projects, 'close_on_exit': close_desktop},",
        "    ):",
        "        try:",
        "            release(**kwargs)",
        "            return",
        "        except TypeError as exc:",
        "            last_error = exc",
        "            continue",
        "    if last_error is not None:",
        "        raise last_error",
        "",
        "",
        "def parse_top_design_names(project: Any) -> list[str]:",
        "    design_names: list[str] = []",
        "    for raw_name in list(project.GetTopDesignList()):",
        "        design_name = str(raw_name).rsplit(';', 1)[-1].strip()",
        "        if design_name:",
        "            design_names.append(design_name)",
        "    return design_names",
        "",
        "",
        "def parse_top_design_entries(project: Any) -> list[tuple[str, str]]:",
        "    entries: list[tuple[str, str]] = []",
        "    for raw_name in list(project.GetTopDesignList()):",
        "        raw_text = str(raw_name).strip()",
        "        design_name = raw_text.rsplit(';', 1)[-1].strip()",
        "        if design_name:",
        "            entries.append((raw_text or design_name, design_name))",
        "    return entries",
        "",
        "",
        "def normalize_design_factory(factory: str) -> str:",
        "    return ''.join(character for character in str(factory).upper() if character.isalnum())",
        "",
        "",
        "def open_existing_project(desktop: Desktop, project_file: Path) -> tuple[str, Any]:",
        "    expected_project_name = project_file.stem",
        "    project_names = [str(name) for name in list(desktop.odesktop.GetProjectList())]",
        "    if expected_project_name in project_names:",
        "        project = desktop.odesktop.SetActiveProject(expected_project_name)",
        "    else:",
        "        project = desktop.odesktop.OpenProject(str(project_file.resolve()))",
        "    if not project:",
        "        raise RuntimeError(f'Failed to open AEDT project: {project_file}')",
        "    project_name = str(project.GetName())",
        "    if not project_name:",
        "        raise RuntimeError(f'Opened AEDT project has no name: {project_file}')",
        "    return project_name, project",
        "",
        "",
        "def extract_design_metadata_block(project_text: str, design_name: str) -> str:",
        "    marker = f\"DesignName='{design_name}'\"",
        "    start_index = project_text.find(marker)",
        "    if start_index < 0:",
        "        raise RuntimeError(f'Failed to locate design metadata for {design_name!r}')",
        "    end_index = project_text.find(\"DesignName='\", start_index + len(marker))",
        "    return project_text[start_index:] if end_index < 0 else project_text[start_index:end_index]",
        "",
        "",
        "def resolve_design_factory(project_text: str, design_name: str) -> str:",
        "    design_block = extract_design_metadata_block(project_text, design_name)",
        "    match = re.search(r\"Factory='([^']+)'\", design_block)",
        "    if match:",
        "        return match.group(1).strip()",
        "    return ''",
        "",
        "",
        "def discover_hfss_design_names(project: Any, project_file: Path) -> tuple[list[str], list[str]]:",
        "    project_text = project_file.read_text(encoding='utf-8', errors='ignore')",
        "    discovered_designs = parse_top_design_names(project)",
        "    hfss_design_names: list[str] = []",
        "    for design_name in discovered_designs:",
        "        normalized_name = normalize_design_factory(design_name)",
        "        normalized_factory = normalize_design_factory(resolve_design_factory(project_text, design_name))",
        "        if normalized_factory.startswith('HFSS') or normalized_name.startswith('HFSS'):",
        "            hfss_design_names.append(design_name)",
        "    return hfss_design_names, discovered_designs",
        "",
        "",
        "def resolve_design_identifier(project: Any, design_name: str) -> str:",
        "    for raw_text, parsed_name in parse_top_design_entries(project):",
        "        if parsed_name == design_name:",
        "            return raw_text or design_name",
        "    return design_name",
        "",
        "",
        "def parse_named_setup_list(block_text: str, label: str) -> list[str]:",
        "    match = re.search(rf\"'{re.escape(label)}'\\[(\\d+):(.*?)\\]\", block_text, re.S)",
        "    if not match:",
        "        return []",
        "    return [name for name in re.findall(r\"'([^']+)'\", match.group(2)) if name]",
        "",
        "",
        "def extract_design_setup_names(project_text: str, design_name: str) -> tuple[list[str], list[str]]:",
        "    marker = f\"DesignName='{design_name}'\"",
        "    start_index = project_text.find(marker)",
        "    if start_index < 0:",
        "        raise RuntimeError(f'Failed to locate setup metadata for design {design_name!r}')",
        "    design_block = extract_design_metadata_block(project_text, design_name)",
        "    nominal_setups = parse_named_setup_list(design_block, 'Nominal Setups')",
        "    optimetrics_setups = parse_named_setup_list(design_block, 'Optimetrics Setups')",
        "    return nominal_setups, optimetrics_setups",
        "",
        "",
        "def open_project_for_session_solve(project_file: Path, grpc_port: int) -> tuple[Desktop, str, str, list[str]]:",
        "    desktop = connect_grpc_desktop(grpc_port, close_on_exit=False)",
        "    project_name, project = open_existing_project(desktop, project_file)",
        "    hfss_design_names, discovered_designs = discover_hfss_design_names(project, project_file)",
        "    if not hfss_design_names:",
        "        raise RuntimeError(",
        "            f'No HFSS design found in {project_file.name}; project_name={project_name!r} '",
        "            f'discovered_designs={discovered_designs!r}'",
        "        )",
        "    hfss_design_identifier = resolve_design_identifier(project, hfss_design_names[0])",
        "    return desktop, project_name, hfss_design_identifier, discovered_designs",
        "",
        "",
        "def build_design_app(project_reference: str, design_name: str | None, design_factory: str, grpc_port: int) -> Any:",
        "    common_kwargs = {",
        "        'project': project_reference,",
        "        'non_graphical': (not USE_GRAPHIC),",
        "        'new_desktop': False,",
        "        'machine': 'localhost',",
        "        'port': grpc_port,",
        "        'close_on_exit': False,",
        "    }",
        "    if design_name:",
        "        common_kwargs['design'] = design_name",
        "    normalized_factory = normalize_design_factory(design_factory or design_name)",
        "    if normalized_factory.startswith('HFSS'):",
        "        return Hfss(**common_kwargs)",
        "    if normalized_factory.startswith('MAXWELL3D') or normalized_factory == 'MAXWELL':",
        "        return Maxwell3d(**common_kwargs)",
        "    if normalized_factory.startswith('ICEPAK'):",
        "        return Icepak(**common_kwargs)",
        "    if normalized_factory.startswith('Q3D'):",
        "        return Q3d(**common_kwargs)",
        "    if normalized_factory.startswith('Q2D'):",
        "        return Q2d(**common_kwargs)",
        "    raise RuntimeError(",
        "        f'Unsupported AEDT design factory for attach project={project_reference!r} '",
        "        f'design={design_name!r} factory={design_factory!r}'",
        "    )",
        "",
        "",
        "def validate_design_app_binding(app: Any, expected_project_name: str, expected_design_name: str) -> tuple[str, str, bool]:",
        "    active_project_name = ''",
        "    bound_design_name = ''",
        "    try:",
        "        if getattr(app, 'oproject', None) is not None:",
        "            active_project_name = str(app.oproject.GetName())",
        "    except Exception:",
        "        active_project_name = ''",
        "    try:",
        "        if getattr(app, 'odesign', None) is not None:",
        "            bound_design_name = str(app.odesign.GetName())",
        "    except Exception:",
        "        bound_design_name = ''",
        "    matches = active_project_name == expected_project_name and bound_design_name == expected_design_name",
        "    return active_project_name, bound_design_name, matches",
        "",
        "",
        "def activate_project_design(desktop: Desktop, project_name: str, design_identifier: str, design_name: str) -> tuple[Any, Any, str]:",
        "    project = desktop.odesktop.SetActiveProject(project_name)",
        "    if project is None:",
        "        raise RuntimeError(f'Failed to activate AEDT project {project_name!r}')",
        "    candidate_names: list[str] = []",
        "    for candidate in (design_name, design_identifier, str(design_identifier).rsplit(';', 1)[-1].strip()):",
        "        if candidate and candidate not in candidate_names:",
        "            candidate_names.append(candidate)",
        "    last_error: Exception | None = None",
        "    for candidate in candidate_names:",
        "        design = None",
        "        try:",
        "            design = project.SetActiveDesign(candidate)",
        "        except Exception as exc:",
        "            last_error = exc",
        "        if design is None:",
        "            try:",
        "                design = project.GetDesign(candidate)",
        "            except Exception as exc:",
        "                last_error = exc",
        "                continue",
        "        if design is None:",
        "            continue",
        "        actual_design_name = ''",
        "        try:",
        "            actual_design_name = str(design.GetName())",
        "        except Exception:",
        "            actual_design_name = str(candidate).rsplit(';', 1)[-1].strip()",
        "        if actual_design_name:",
        "            return project, design, actual_design_name",
        "    raise RuntimeError(",
        "        f'Failed to activate AEDT design project={project_name!r} candidates={candidate_names!r}: {last_error}'",
        "    )",
        "",
        "",
        "def attach_design_app(desktop: Desktop, project_name: str, project_file: Path, design_identifier: str, design_name: str, design_factory: str, grpc_port: int) -> Any:",
        "    last_error: Exception | None = None",
        "    requested_project_refs: list[str] = []",
        "    for candidate_project_ref in (project_name,):",
        "        if candidate_project_ref and candidate_project_ref not in requested_project_refs:",
        "            requested_project_refs.append(candidate_project_ref)",
        "    requested_design_names: list[str | None] = []",
        "    for candidate_design_name in (design_name, str(design_identifier).rsplit(';', 1)[-1].strip(), None):",
        "        if candidate_design_name not in requested_design_names:",
        "            requested_design_names.append(candidate_design_name)",
        "    for project_reference in requested_project_refs:",
        "        for requested_design_name in requested_design_names:",
        "            try:",
        "                app = build_design_app(project_reference, requested_design_name, design_factory, grpc_port)",
        "            except Exception as exc:",
        "                last_error = exc",
        "                continue",
        "            active_project_name, bound_design_name, matches = validate_design_app_binding(",
        "                app, project_name, design_name",
        "            )",
        "            if matches:",
        "                return app",
        "            last_error = RuntimeError(",
        "                f'Attached AEDT design did not match request project={project_name!r} design={design_name!r} '",
        "                f'factory={design_factory!r} project_reference={project_reference!r} '",
        "                f'active_project={active_project_name!r} active_design={bound_design_name!r} '",
        "                f'requested_design={requested_design_name!r}'",
        "            )",
        "            try:",
        "                release_desktop_session(app, close_projects=False, close_desktop=False)",
        "            except Exception:",
        "                pass",
        "    for _attempt in range(10):",
        "        try:",
        "            _project, _design, active_design_name = activate_project_design(",
        "                desktop, project_name, design_identifier, design_name",
            "            )",
        "        except Exception as exc:",
        "            last_error = exc",
        "            time.sleep(0.5)",
        "            continue",
        "        requested_design_names: list[str | None] = [None]",
        "        for candidate in (active_design_name, design_name):",
        "            if candidate and candidate not in requested_design_names:",
        "                requested_design_names.append(candidate)",
        "        for requested_design_name in requested_design_names:",
            "            try:",
        "                app = build_design_app(project_name, requested_design_name, design_factory, grpc_port)",
        "            except Exception as exc:",
        "                last_error = exc",
        "                continue",
        "            active_project_name, bound_design_name, matches = validate_design_app_binding(",
        "                app, project_name, design_name",
        "            )",
        "            if matches or bound_design_name == active_design_name:",
        "                return app",
        "            last_error = RuntimeError(",
        "                f'Attached AEDT design did not match request project={project_name!r} design={design_name!r} '",
        "                f'factory={design_factory!r} active_project={active_project_name!r} active_design={bound_design_name!r} '",
        "                f'activated_design={active_design_name!r} requested_design={requested_design_name!r}'",
        "            )",
        "            try:",
        "                release_desktop_session(app, close_projects=False, close_desktop=False)",
        "            except Exception:",
        "                pass",
        "        time.sleep(0.5)",
        "    if last_error is not None:",
        "        raise RuntimeError(",
        "            f'Failed to bind to open AEDT design project={project_name!r} design={design_name!r} '",
        "            f'factory={design_factory!r}: {last_error}'",
        "        ) from last_error",
        "    return None",
        "",
        "",
        "def attach_hfss_to_open_project(desktop: Desktop, project_name: str, project_file: Path, design_identifier: str, design_name: str, grpc_port: int) -> Any:",
        "    active_design_name = ''",
        "    last_error: Exception | None = None",
        "    requested_project_refs: list[str] = []",
        "    for candidate_project_ref in (project_name,):",
        "        if candidate_project_ref and candidate_project_ref not in requested_project_refs:",
        "            requested_project_refs.append(candidate_project_ref)",
        "    requested_design_names: list[str] = []",
        "    for candidate_design_name in (design_name, str(design_identifier).rsplit(';', 1)[-1].strip()):",
        "        if candidate_design_name and candidate_design_name not in requested_design_names:",
        "            requested_design_names.append(candidate_design_name)",
        "    for project_reference in requested_project_refs:",
        "        for requested_design_name in requested_design_names:",
        "            try:",
        "                app = Hfss(",
        "                    project=project_reference,",
        "                    design=requested_design_name,",
        "                    non_graphical=(not USE_GRAPHIC),",
        "                    new_desktop=False,",
        "                    machine='localhost',",
        "                    port=grpc_port,",
        "                    close_on_exit=False,",
        "                )",
        "            except Exception as exc:",
        "                last_error = exc",
        "                continue",
        "            active_project_name, bound_design_name, matches = validate_design_app_binding(",
        "                app, project_name, design_name",
        "            )",
        "            if matches or bound_design_name == design_name:",
        "                if not hasattr(app, 'analyze'):",
        "                    raise RuntimeError(",
        "                        f'Bound AEDT design does not support analyze '",
        "                        f'project={project_name!r} design={design_name!r} app_type={type(app).__name__}'",
        "                    )",
        "                return app",
        "            last_error = RuntimeError(",
        "                f'Attached HFSS design did not match request project={project_name!r} design={design_name!r} '",
        "                f'project_reference={project_reference!r} requested_design={requested_design_name!r} '",
        "                f'active_project={active_project_name!r} active_design={bound_design_name!r} '",
        "                f'activated_design={active_design_name!r}'",
        "            )",
        "            try:",
        "                release_desktop_session(app, close_projects=False, close_desktop=False)",
        "            except Exception:",
        "                pass",
        "    try:",
        "        app = Hfss(",
        "            non_graphical=(not USE_GRAPHIC),",
        "            new_desktop=False,",
        "            machine='localhost',",
        "            port=grpc_port,",
        "            close_on_exit=False,",
        "        )",
        "    except Exception as exc:",
        "        if last_error is None:",
        "            last_error = exc",
        "    else:",
        "        active_project_name, bound_design_name, _matches = validate_design_app_binding(",
        "            app, project_name, design_name",
        "        )",
        "        if active_project_name == project_name:",
        "            if not hasattr(app, 'analyze'):",
        "                raise RuntimeError(",
        "                    f'Bound AEDT session does not support analyze '",
        "                    f'project={project_name!r} design={design_name!r} app_type={type(app).__name__}'",
        "                )",
        "            return app",
        "        last_error = RuntimeError(",
        "            f'Session-level HFSS attach did not match request project={project_name!r} design={design_name!r} '",
        "            f'active_project={active_project_name!r} active_design={bound_design_name!r}'",
        "        )",
        "        try:",
        "            release_desktop_session(app, close_projects=False, close_desktop=False)",
        "        except Exception:",
        "            pass",
        "    try:",
        "        _project, _design, active_design_name = activate_project_design(",
        "            desktop, project_name, design_identifier, design_name",
        "        )",
        "    except Exception as exc:",
        "        if last_error is None:",
        "            last_error = exc",
        "    else:",
        "        for requested_design_name in (active_design_name, design_name):",
        "            if not requested_design_name:",
        "                continue",
        "            try:",
        "                app = Hfss(",
        "                    project=project_name,",
        "                    design=requested_design_name,",
        "                    non_graphical=(not USE_GRAPHIC),",
        "                    new_desktop=False,",
        "                    machine='localhost',",
        "                    port=grpc_port,",
        "                    close_on_exit=False,",
        "                )",
        "            except Exception as exc:",
        "                last_error = exc",
        "                continue",
        "            active_project_name, bound_design_name, matches = validate_design_app_binding(",
        "                app, project_name, design_name",
        "            )",
        "            if matches or bound_design_name == active_design_name or bound_design_name == design_name:",
        "                if not hasattr(app, 'analyze'):",
        "                    raise RuntimeError(",
        "                        f'Bound AEDT design does not support analyze '",
        "                        f'project={project_name!r} design={design_name!r} app_type={type(app).__name__}'",
        "                    )",
        "                return app",
        "            last_error = RuntimeError(",
        "                f'Attached HFSS design did not match request project={project_name!r} design={design_name!r} '",
        "                f'project_reference={project_name!r} requested_design={requested_design_name!r} '",
        "                f'active_project={active_project_name!r} active_design={bound_design_name!r} '",
        "                f'activated_design={active_design_name!r}'",
        "            )",
        "            try:",
        "                release_desktop_session(app, close_projects=False, close_desktop=False)",
        "            except Exception:",
        "                pass",
        "    if last_error is not None:",
        "        raise RuntimeError(",
        "            f'Failed to bind HFSS to open AEDT project={project_name!r} design={design_name!r}: {last_error}'",
        "        ) from last_error",
        "    raise RuntimeError(",
        "        f'Failed to bind HFSS to open AEDT project={project_name!r} design={design_name!r}'",
        "    )",
        "",
        "",
        "def resolve_first_setup_name(app: Any, project_file: Path, design_name: str) -> str:",
        "    setup_names = [str(name).strip() for name in getattr(app, 'setup_names', []) if str(name).strip()]",
        "    if setup_names:",
        "        return setup_names[0]",
        "    project_text = project_file.read_text(encoding='utf-8', errors='ignore')",
        "    nominal_setups, optimetrics_setups = extract_design_setup_names(project_text, design_name)",
        "    candidates = nominal_setups or optimetrics_setups",
        "    if candidates:",
        "        return candidates[0]",
        "    raise RuntimeError(f'No setup name found for design={design_name!r} project={project_file}')",
        "",
        "",
        "def reopen_solved_project(project_file: Path, grpc_port: int, project_name: str | None = None) -> Desktop:",
        "    desktop = connect_grpc_desktop(grpc_port, close_on_exit=False)",
        "    active_project = None",
        "    if project_name:",
        "        try:",
        "            active_project = desktop.odesktop.SetActiveProject(project_name)",
        "        except Exception:",
        "            active_project = None",
        "    if active_project is None:",
        "        _project_name, _project = open_existing_project(desktop, project_file)",
        "    return desktop",
        "",
        "",
        "def iter_project_design_apps(desktop: Desktop, project_file: Path, grpc_port: int) -> list[tuple[str, Any]]:",
        "    project_name, project = open_existing_project(desktop, project_file)",
        "    project_text = project_file.read_text(encoding='utf-8', errors='ignore')",
        "    design_apps: list[tuple[str, Any]] = []",
        "    for design_identifier, design_name in parse_top_design_entries(project):",
        "        try:",
        "            design_factory = resolve_design_factory(project_text, design_name)",
        "        except Exception:",
        "            continue",
        "        try:",
        "            app = attach_design_app(",
        "                desktop, project_name, project_file, design_identifier, design_name, design_factory, grpc_port",
        "            )",
        "        except Exception:",
        "            continue",
        "        if app is None:",
        "            continue",
        "        design_apps.append((design_name, app))",
        "    return design_apps",
        "",
        "",
        "def extract_design_output_row(design_name: str, app: Any, workdir: Path) -> tuple[int, int, int, str, list[str]]:",
        "    errors: list[str] = []",
        "    try:",
        "        write_synthetic_input_report(workdir, design_name, app)",
        "    except Exception as exc:",
        "        errors.append(f'synthetic input report failed: {exc}')",
        "    try:",
        "        if isinstance(app, Hfss):",
        "            create_output_variables_report(app)",
        "    except Exception as exc:",
        "        errors.append(f'output variables report failed: {exc}')",
        "    try:",
        "        create_parameter_reports(app)",
        "    except Exception as exc:",
        "        errors.append(f'parameter report creation failed: {exc}')",
        "    try:",
        "        update_all_reports(app)",
        "    except Exception as exc:",
        "        errors.append(f'report update failed: {exc}')",
        "    try:",
        "        _, export_errors = export_all_reports(app, workdir, design_name)",
        "        errors.extend(export_errors)",
        "    except Exception as exc:",
        "        errors.append(f'report export failed: {exc}')",
        "    report_count = count_design_report_files(workdir, design_name)",
        "    native_report_count = count_design_report_files(workdir, design_name, include_synthetic=False)",
        "    synthetic_report_count = count_synthetic_design_report_files(workdir, design_name)",
        "    status = classify_design_report_status(",
        "        native_report_count=native_report_count,",
        "        synthetic_report_count=synthetic_report_count,",
        "        errors=errors,",
        "    )",
        "    return report_count, native_report_count, synthetic_report_count, status, errors",
        "",
        "",
        "def extract_design_reports(desktop: Desktop, workdir: Path, project_file: Path, grpc_port: int) -> None:",
        "    if not EMIT_OUTPUT_VARIABLES_CSV:",
        "        return",
        "    (workdir / DESIGN_OUTPUTS_DIR_NAME).mkdir(parents=True, exist_ok=True)",
        "    fatal_errors: list[str] = []",
        "    design_output_manifest_rows: list[dict[str, object]] = []",
        "    for design_name, app in iter_project_design_apps(desktop, project_file, grpc_port):",
        "        try:",
        "            report_count, native_report_count, synthetic_report_count, design_status, design_errors = extract_design_output_row(design_name, app, workdir)",
        "        finally:",
        "            try:",
        "                release_desktop_session(app, close_projects=False, close_desktop=False)",
        "            except Exception:",
        "                pass",
        "        error_log_path = ''",
        "        if design_errors:",
        "            error_log_path = str(write_design_output_error_log(workdir, design_name, design_errors).relative_to(workdir))",
        "        design_output_manifest_rows.append(",
        "            {",
        "                'design_name': design_name,",
        "                'reports_dir': str(reports_dir_for_design_name(workdir, design_name).relative_to(workdir)),",
        "                'report_count': str(report_count),",
        "                'native_report_count': str(native_report_count),",
        "                'synthetic_report_count': str(synthetic_report_count),",
        "                'status': design_status,",
        "                'error_log': error_log_path,",
        "            }",
        "        )",
        "    write_design_outputs_manifest(workdir, design_output_manifest_rows)",
        "    if not design_output_manifest_rows:",
        "        fatal_errors.append('No compatible design session was available for report export')",
        "    if not any(int(row.get('report_count', '0') or '0') > 0 for row in design_output_manifest_rows):",
        "        fatal_errors.append('No design report CSV files were exported')",
        "    if fatal_errors:",
        "        raise RuntimeError('; '.join(fatal_errors))",
        "",
        "",
        "def close_all_projects(desktop: Desktop) -> None:",
        "    close_project = getattr(desktop.odesktop, 'CloseProject', None)",
        "    if not callable(close_project):",
        "        return",
        "    for project_name in list(desktop.odesktop.GetProjectList()):",
        "        try:",
        "            close_project(str(project_name))",
        "        except Exception:",
        "            continue",
        "",
        "",
        "def main() -> int:",
        "    workdir = Path.cwd()",
        "    project_file = workdir / AEDT_FILENAME",
        "    if not project_file.exists():",
        "        raise FileNotFoundError(f'AEDT file not found: {project_file}')",
        "",
        "    existing_runtime_logs = snapshot_pyaedt_logs()",
        "    write_license_diagnostics(workdir)",
        "    tmpdir = Path(os.environ.get('TMPDIR', str(workdir / 'tmp'))).resolve()",
        "    tmpdir.mkdir(parents=True, exist_ok=True)",
        "    ansys_work_dir = Path(os.environ.get('ANSYS_WORK_DIR', str(tmpdir / 'ansys_tmp'))).resolve()",
        "    ansys_work_dir.mkdir(parents=True, exist_ok=True)",
        "    os.environ['ANSYS_WORK_DIR'] = str(ansys_work_dir)",
        "    os.environ['TEMP'] = str(tmpdir)",
        "    os.environ['TMPDIR'] = str(tmpdir)",
        "    cores = int(os.environ.get('PEETS_SLOT_CORES', '4'))",
        "    tasks = int(os.environ.get('PEETS_SLOT_TASKS', '1'))",
        "    remove_lock_files(workdir)",
        "    reuse_grpc_port_value = os.environ.get('PEETS_REUSE_GRPC_PORT', '').strip()",
        "    reuse_session = bool(reuse_grpc_port_value)",
        "    grpc_port = int(reuse_grpc_port_value) if reuse_session else get_available_port()",
        "    ansys_process = None",
        "    solve_desktop = None",
        "    solve_hfss = None",
        "    try:",
        "        with use_ansys_launch_env():",
        "            if reuse_session:",
        "                solve_desktop = connect_grpc_desktop(grpc_port, close_on_exit=False)",
        "            else:",
        "                ansys_process = launch_ansys_grpc_server(grpc_port)",
        "                solve_desktop = connect_grpc_desktop(grpc_port, close_on_exit=False)",
        "            close_all_projects(solve_desktop)",
        "            solve_project_name, solve_project = open_existing_project(solve_desktop, project_file)",
        "            hfss_design_names, discovered_designs = discover_hfss_design_names(solve_project, project_file)",
        "            if not hfss_design_names:",
        "                raise RuntimeError(",
        "                    f'No HFSS design found in {project_file.name}; project_name={solve_project_name!r} '",
        "                    f'discovered_designs={discovered_designs!r}'",
        "                )",
        "            solve_design_identifier = resolve_design_identifier(solve_project, hfss_design_names[0])",
        "            solve_design_name = str(solve_design_identifier).rsplit(';', 1)[-1].strip()",
        "            if not solve_design_name:",
        "                raise RuntimeError(",
        "                    f'Failed to resolve HFSS design name from identifier {solve_design_identifier!r} '",
        "                    f'project={solve_project_name!r} discovered_designs={discovered_designs!r}'",
        "                )",
        "            print(",
        "                f'[INFO] session solve prepared project={solve_project_name!r} '",
        "                f'design={solve_design_name!r} discovered_designs={discovered_designs!r}'",
        "            )",
        "            solve_hfss = attach_hfss_to_open_project(",
        "                solve_desktop,",
        "                solve_project_name,",
        "                project_file,",
        "                solve_design_identifier,",
        "                solve_design_name,",
        "                grpc_port,",
        "            )",
        "            desktop_class = getattr(solve_hfss, 'desktop_class', None)",
        "            change_license_type = getattr(desktop_class, 'change_license_type', None)",
        "            if callable(change_license_type):",
        "                try:",
        "                    change_license_type('Pool')",
        "                except Exception as exc:",
        "                    print(f'[WARN] failed to set AEDT HPC license type to Pool: {exc}')",
        "            setup_name = resolve_first_setup_name(solve_hfss, project_file, solve_design_name)",
        "            solved = solve_hfss.analyze(",
        "                setup=setup_name,",
        "                cores=cores,",
        "                tasks=tasks,",
        "                solve_in_batch=False,",
        "                blocking=True,",
        "            )",
        "            if not solved:",
        "                raise RuntimeError(",
        "                    f'HFSS analyze returned false for project={solve_project_name!r} setup={setup_name!r}'",
        "                )",
        "            remove_lock_files(workdir)",
        "            save_project = getattr(solve_hfss, 'save_project', None)",
        "            if callable(save_project):",
        "                save_project()",
        "            if solve_hfss is not None:",
        "                try:",
        "                    extract_design_reports(solve_desktop, workdir, project_file, grpc_port)",
        "                    error_log_path = workdir / REPORT_EXPORT_ERROR_LOG_NAME",
        "                    if error_log_path.exists():",
        "                        error_log_path.unlink()",
        "                except Exception as exc:",
        "                    (workdir / REPORT_EXPORT_ERROR_LOG_NAME).write_text(str(exc), encoding='utf-8')",
        "                    print(f'[WARN] design report export failed but solve completed: {exc}')",
        "            return 0",
        "    finally:",
        "        copy_runtime_logs(workdir, existing_runtime_logs)",
        "        if solve_hfss is not None:",
        "            release_desktop_session(solve_hfss, close_projects=True, close_desktop=False)",
        "        elif solve_desktop is not None:",
        "            release_desktop_session(solve_desktop, close_projects=True, close_desktop=False)",
        "        if ansys_process is not None:",
        "            stop_process(ansys_process)",
        "",
        "",
        "if __name__ == '__main__':",
        "    exit_code = 0",
        "    try:",
        "        exit_code = int(main())",
        "    except Exception:",
        "        traceback.print_exc()",
        "        exit_code = 1",
        "    Path('exit.code').write_text(str(exit_code), encoding='utf-8')",
        "    sys.stdout.flush()",
        "    sys.stderr.flush()",
        "    raise SystemExit(exit_code)",
        "PY",
        "LD_LIBRARY_PATH=\"$IMAGE_LD_LIBRARY_PATH\" \"$IMAGE_PYTHON\" run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _build_enroot_remote_job_script_content(*, config: RemoteJobConfig) -> str:
    content = _build_remote_job_script_content(
        emit_output_variables_csv=getattr(config, "emit_output_variables_csv", True),
    )
    content = content.replace(
        "ANSYS_EXECUTABLE: str = '/mnt/AnsysEM/ansysedt'",
        f"ANSYS_EXECUTABLE: str = {_remote_ansys_executable(config)!r}",
        1,
    )
    return content


def _build_windows_remote_job_script_content() -> str:
    lines = [
        "$ErrorActionPreference = 'Stop'",
        "$CondaPython = Join-Path (Join-Path $HOME 'miniconda3') 'python.exe'",
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
        "        hfss.release_desktop(close_projects=True, close_on_exit=True)",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "'@",
        "Set-Content -Path run_sim.py -Value $RunSim",
        "& $CondaPython run_sim.py",
    ]
    return "\n".join(lines) + "\n"


def _write_remote_job_script(tmpdir: Path, *, config: RemoteJobConfig) -> Path:
    script = tmpdir / "remote_job.sh"
    if _remote_platform(config) == "windows":
        raise ValueError("windows remote jobs must use _write_windows_remote_job_script")
    if _remote_container_runtime(config) != "enroot":
        raise ValueError("linux remote job generation requires remote_container_runtime='enroot'")
    content = _build_enroot_remote_job_script_content(config=config)
    script.write_text(content, encoding="utf-8")
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
    remote_runtime_root = _remote_runtime_root_shell_path(config=config)
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"REMOTE_JOB_DIR={shlex.quote(remote_path)}",
        f"REMOTE_RUNTIME_ROOT={_double_quoted_shell_value(remote_runtime_root)}",
        "export REMOTE_JOB_DIR",
        "mkdir -p \"$REMOTE_RUNTIME_ROOT\"",
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
    runtime_root = _remote_runtime_root_shell_path(config=config)
    partition_value = _slurm_partition_value(config)
    partition_arg = f" -p {partition_value}" if partition_value else ""
    return (
        f"srun -D {_double_quoted_shell_value(runtime_root)}{partition_arg} -N {config.nodes} -n {config.ntasks} "
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
    configured_max_parallel = int(getattr(config, "slot_max_concurrency", getattr(config, "slots_per_job", case_count)))
    max_parallel = max(1, configured_max_parallel)
    min_parallel = max(1, int(getattr(config, "slot_min_concurrency", min(5, max_parallel))))
    max_parallel = max(min_parallel, max_parallel)
    tasks_per_slot = int(getattr(config, "tasks_per_slot", 1))
    container_runtime = _remote_container_runtime(config)
    container_image = _remote_container_image(config)
    host_ansys_root = _remote_host_ansys_mount_root(config)
    host_ansys_base = _remote_host_ansys_base_root(config)
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
    runtime_root = _remote_runtime_root_shell_path(config=config)
    payload_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}",
        f"REMOTE_RUNTIME_ROOT={_double_quoted_shell_value(runtime_root)}",
        "mkdir -p \"$REMOTE_RUNTIME_ROOT\"",
        "workdir=$(mktemp -d \"$REMOTE_RUNTIME_ROOT/slot.${SLURM_JOB_ID:-nojob}.XXXXXX\")",
        "JOB_TMPFS_ROOT=\"$workdir/job_tmpfs\"",
        "JOB_DISK_ROOT=\"$workdir/job_disk\"",
        "JOB_DISK_BUDGET_GB=" + str(JOB_DISK_FILESYSTEM_SIZE_GB),
        "enter_job_filesystem_namespace() {",
        "  mkdir -p \"$JOB_TMPFS_ROOT\" \"$JOB_DISK_ROOT\"",
        "  if [ \"${PEETS_JOB_TMPFS_NAMESPACE_READY:-0}\" = \"1\" ]; then",
        "    return 0",
        "  fi",
        "  if ! command -v unshare >/dev/null 2>&1; then",
        "    echo \"[ERROR] unshare is required for rootless job tmpfs setup\" >&2",
        "    return 127",
        "  fi",
        "  exec unshare --user --map-root-user --mount /bin/bash -c 'set -euo pipefail; job_tmpfs_root=\"$1\"; shift; mount -t tmpfs -o size="
        + str(JOB_TMPFS_SIZE_GB)
        + "G tmpfs \"$job_tmpfs_root\"; export PEETS_JOB_TMPFS_NAMESPACE_READY=1; export PEETS_JOB_TMPFS_NAMESPACE_ACTIVE=1; exec \"$@\"' /bin/bash \"$JOB_TMPFS_ROOT\" /bin/bash \"$0\" \"$@\"",
        "}",
        "cleanup_workdir() {",
        "  cd \"$REMOTE_RUNTIME_ROOT\" >/dev/null 2>&1 || true",
        "  if [ \"${PEETS_JOB_TMPFS_NAMESPACE_ACTIVE:-0}\" = \"1\" ] && awk -v target=\"$JOB_TMPFS_ROOT\" '$2 == target && $3 == \"tmpfs\" {found=1} END {exit found?0:1}' /proc/mounts; then",
        "    umount \"$JOB_TMPFS_ROOT\" >/dev/null 2>&1 || true",
        "  fi",
        "  rm -rf \"$workdir\" >/dev/null 2>&1 || true",
        "  rmdir \"$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}\" >/dev/null 2>&1 || true",
        "}",
        "teardown_control_plane() {",
        "  :",
        "}",
        "cleanup() {",
        "  rc=$?",
        "  teardown_control_plane",
        "  cleanup_workdir",
        "  exit \"$rc\"",
        "}",
        "trap cleanup EXIT",
        "enter_job_filesystem_namespace \"$@\"",
        "launch_probe_file=\"${REMOTE_JOB_DIR:-$workdir}/launch_probe.txt\"",
        "{",
        "  printf 'hostname=%s\\n' \"$(hostname 2>/dev/null || true)\"",
        "  printf 'pwd=%s\\n' \"$PWD\"",
        "  printf 'path=%s\\n' \"$PATH\"",
        "  printf 'job_tmpfs_root=%s\\n' \"$JOB_TMPFS_ROOT\"",
        "  printf 'job_tmpfs_size_gb=%s\\n' \"" + str(JOB_TMPFS_SIZE_GB) + "\"",
        "  printf 'job_disk_root=%s\\n' \"$JOB_DISK_ROOT\"",
        "  printf 'job_disk_budget_gb=%s\\n' \"$JOB_DISK_BUDGET_GB\"",
        "  for tool in bash tar seq cp base64 ssh enroot python3 python; do",
        "    resolved_tool=\"$(command -v \"$tool\" 2>/dev/null || true)\"",
        "    printf 'tool.%s=%s\\n' \"$tool\" \"${resolved_tool:-MISSING}\"",
        "  done",
        "  if command -v enroot >/dev/null 2>&1; then",
        "    printf 'enroot.version=%s\\n' \"$(enroot version 2>/dev/null | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/^ //;s/ $//')\"",
        "  fi",
        "} > \"$launch_probe_file\" 2>&1 || true",
        "cd \"$workdir\"",
        "tar --no-same-owner -xzf -",
        f"PEETS_CONTROL_RUN_ID={shlex.quote(run_id or '')}",
        f"PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id or '')}",
        f"PEETS_CONTROL_HOST={shlex.quote(control_plane_host)}",
        f"PEETS_CONTROL_PORT={control_plane_port}",
        f"PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
        f"PEETS_CONTROL_HEARTBEAT_INTERVAL={heartbeat_interval_seconds}",
        f"PEETS_SLOT_MIN_CONCURRENCY={min_parallel}",
        f"max_parallel={max_parallel}",
        f"PEETS_SLOT_MEMORY_PRESSURE_HIGH_WATERMARK={int(getattr(config, 'slot_memory_pressure_high_watermark_percent', 90))}",
        f"PEETS_SLOT_MEMORY_PRESSURE_RESUME_WATERMARK={int(getattr(config, 'slot_memory_pressure_resume_watermark_percent', 80))}",
        f"PEETS_SLOT_MEMORY_PROBE_INTERVAL={max(1, int(getattr(config, 'slot_memory_probe_interval_seconds', 5)))}",
        "PEETS_CONTROL_LOCAL_PORT=$((PEETS_CONTROL_PORT + 1000 + (${SLURM_JOB_ID:-0} % 1000)))",
        "PEETS_TUNNEL_SOCKET=\"$workdir/control-plane.sock\"",
        "PEETS_TUNNEL_SESSION_ID=\"${SLURM_JOB_ID:-nojob}-${PEETS_CONTROL_WORKER_ID:-worker}-$$\"",
        "PEETS_CONTROL_API_URL=\"http://127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}\"",
        "PEETS_CONTROL_SSH_IDENTITY=\"${PEETS_CONTROL_SSH_IDENTITY:-}\"",
        f"PEETS_REMOTE_CONTAINER_RUNTIME={shlex.quote(container_runtime)}",
        (
            "REMOTE_CONTAINER_IMAGE="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=container_image))
        ),
        (
            "REMOTE_HOST_ANSYS_ROOT="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=host_ansys_root))
        ),
        (
            "REMOTE_HOST_ANSYS_BASE="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=host_ansys_base))
        ),
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
        "memory_stats() {",
        "  local total_kb avail_kb used_kb pressure_pct",
        "  total_kb=$(awk '/MemTotal/ {print $2+0}' /proc/meminfo 2>/dev/null || echo 0)",
        "  avail_kb=$(awk '/MemAvailable/ {print $2+0}' /proc/meminfo 2>/dev/null || echo 0)",
        "  used_kb=$(( total_kb - avail_kb ))",
        "  if [ \"$used_kb\" -lt 0 ]; then used_kb=0; fi",
        "  if [ \"$total_kb\" -gt 0 ]; then",
        "    pressure_pct=$(( (used_kb * 100) / total_kb ))",
        "  else",
        "    pressure_pct=0",
        "  fi",
        "  printf '%s %s %s %s\\n' \"$(( total_kb / 1024 ))\" \"$(( avail_kb / 1024 ))\" \"$(( used_kb / 1024 ))\" \"$pressure_pct\"",
        "}",
        "memory_gate_open=1",
        "last_target_slots=''",
        "last_memory_gate=''",
        "emit_scheduler_event() {",
        "  stage=\"$1\"",
        "  message=\"$2\"",
        "  control_plane_post /internal/events/worker \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"stage\\\":\\\"${stage}\\\",\\\"message\\\":\\\"${message}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "compute_target_slots() {",
        "  active_now=\"$1\"",
        "  queued_now=\"$2\"",
        "  read -r total_mem_mb avail_mem_mb used_mem_mb memory_pressure_pct <<EOF",
        "  $(memory_stats)",
        "EOF",
        "  if [ \"${memory_pressure_pct:-0}\" -ge \"$PEETS_SLOT_MEMORY_PRESSURE_HIGH_WATERMARK\" ]; then",
        "    memory_gate_open=0",
        "  elif [ \"${memory_pressure_pct:-0}\" -le \"$PEETS_SLOT_MEMORY_PRESSURE_RESUME_WATERMARK\" ]; then",
        "    memory_gate_open=1",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 1 ]; then",
        "    target_slots=\"$max_parallel\"",
        "  else",
        "    target_slots=\"$active_now\"",
        "  fi",
        "  if [ \"$target_slots\" -gt \"$max_parallel\" ]; then target_slots=\"$max_parallel\"; fi",
        "  if [ \"$target_slots\" -lt \"$active_now\" ]; then target_slots=\"$active_now\"; fi",
        "  if [ \"$target_slots\" -lt 0 ]; then target_slots=0; fi",
        "  if [ \"$last_target_slots\" != \"$target_slots\" ] && [ \"$target_slots\" -gt \"$active_now\" ]; then",
        "    emit_scheduler_event SLOT_SCALE_UP \"target_slots=${target_slots} active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct} queued_slots=${queued_now}\"",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 0 ] && [ \"$last_memory_gate\" != \"0\" ]; then",
        "    emit_scheduler_event SLOT_START_BLOCKED_MEMORY \"active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct} queued_slots=${queued_now}\"",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 0 ] && [ \"$queued_now\" -gt 0 ] && [ \"$active_now\" -gt 0 ]; then",
        "    emit_scheduler_event SLOT_DRAIN_CONTINUE \"active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct} queued_slots=${queued_now}\"",
        "  fi",
        "  last_target_slots=\"$target_slots\"",
        "  last_memory_gate=\"$memory_gate_open\"",
        "  printf '%s %s %s %s %s\\n' \"$target_slots\" \"$memory_pressure_pct\" \"$memory_gate_open\" \"$avail_mem_mb\" \"$used_mem_mb\"",
        "}",
        "control_plane_post() {",
        "  endpoint=\"$1\"",
        "  payload=\"$2\"",
        "  control_python=\"$(command -v python3 || command -v python || true)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_PAYLOAD=\"$payload\" \"$control_python\" - <<'PY'",
        "import os",
        "import urllib.request",
        "import urllib.error",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'],",
        "    data=os.environ['CONTROL_PAYLOAD'].encode('utf-8'),",
        "    headers={'Content-Type': 'application/json'},",
        "    method='POST',",
        ")",
        "try:",
        "    with urllib.request.urlopen(req, timeout=5) as resp:",
        "        resp.read()",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'control_plane_post endpoint={os.environ[\"CONTROL_ENDPOINT\"]} status={exc.code} detail={detail}')",
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
        "  local -a ssh_args=(-F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -o BatchMode=yes -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)",
        "  if [ -n \"$PEETS_CONTROL_SSH_IDENTITY\" ]; then",
        "    ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")",
        "  fi",
        "  ssh \"${ssh_args[@]}\" -M -S \"$PEETS_TUNNEL_SOCKET\" -fnNT \\",
        "    -L 127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}:127.0.0.1:${PEETS_CONTROL_PORT} \\",
        "    \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\"",
        "}",
        "stop_control_tunnel() {",
        "  if [ -S \"$PEETS_TUNNEL_SOCKET\" ]; then",
        "    local -a ssh_args=(-F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)",
        "    if [ -n \"$PEETS_CONTROL_SSH_IDENTITY\" ]; then",
        "      ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")",
        "    fi",
        "    ssh \"${ssh_args[@]}\" -S \"$PEETS_TUNNEL_SOCKET\" -O exit \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\" >/dev/null 2>&1 || true",
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
        "  target_slots=\"${3:-0}\"",
        "  memory_pressure_pct=\"${4:-0}\"",
        "  memory_gate_open=\"${5:-1}\"",
        "  queued_slots_inside_worker=\"${6:-0}\"",
        "  rss_mb=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$$/status 2>/dev/null || echo 0)",
        "  cpu_pct=$(ps -p $$ -o %cpu= 2>/dev/null | awk '{print $1+0}' || echo 0)",
        "  process_count=$(ps --no-headers --ppid $$ 2>/dev/null | wc -l | awk '{print $1+0}')",
        "  control_plane_post /internal/resources/worker \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"slurm_job_id\\\":\\\"${SLURM_JOB_ID:-}\\\",\\\"configured_slots\\\":${max_parallel},\\\"active_slots\\\":${active_slots},\\\"idle_slots\\\":${idle_slots},\\\"target_slots\\\":${target_slots},\\\"memory_pressure_pct\\\":${memory_pressure_pct},\\\"memory_gate_open\\\":${memory_gate_open},\\\"queued_slots_inside_worker\\\":${queued_slots_inside_worker},\\\"rss_mb\\\":${rss_mb:-0},\\\"cpu_pct\\\":${cpu_pct:-0},\\\"tunnel_state\\\":\\\"CONNECTED\\\",\\\"process_count\\\":${process_count:-0}}\" >/dev/null 2>&1 || true",
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
        "    read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
        "  $(compute_target_slots \"$active_now\" 0)",
        "EOF",
        "    idle_now=$(( target_now - active_now ))",
        "    if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
        "    emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\" 0",
        "  done",
        "}",
        "heartbeat_pid=''",
        "if start_control_tunnel 2> control_tunnel_bootstrap.err; then",
        "  emit_control_register",
        "  emit_node_snapshot",
        "  emit_worker_snapshot 0 \"$PEETS_SLOT_MIN_CONCURRENCY\" \"$PEETS_SLOT_MIN_CONCURRENCY\" 0 1 "
        + str(case_count),
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
        "run_case_command() {",
        "  case_dir=\"$1\"",
        "  case_name=\"$(basename \"$case_dir\")\"",
        "  export PEETS_SLOT_CORES PEETS_SLOT_TASKS",
        "  if [ \"$PEETS_REMOTE_CONTAINER_RUNTIME\" != \"enroot\" ]; then",
        "    echo \"[ERROR] linux worker payload requires enroot\" >&2",
        "    return 1",
        "  fi",
        "  (",
        "    container_name=\"peets-${SLURM_JOB_ID:-nojob}-${case_name}-$$\"",
        "    case_ram_root=\"$JOB_TMPFS_ROOT/$case_name\"",
        "    case_disk_root=\"$JOB_DISK_ROOT/$case_name\"",
        "    mkdir -p \"$case_ram_root/tmp\" \"$case_ram_root/ansys_tmp\" \"$case_disk_root/home\" \"$case_disk_root/tmp\" \"$case_disk_root/ansys_tmp\"",
        "    enroot_base=\"$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}/${case_name}-$$\"",
        "    export ENROOT_RUNTIME_PATH=\"$enroot_base/runtime\"",
        "    export ENROOT_CACHE_PATH=\"$enroot_base/cache\"",
        "    export ENROOT_DATA_PATH=\"$enroot_base/data\"",
        "    export ENROOT_TEMP_PATH=\"$enroot_base/tmp\"",
        "    mkdir -p \"$ENROOT_RUNTIME_PATH\" \"$ENROOT_CACHE_PATH\" \"$ENROOT_DATA_PATH\" \"$ENROOT_TEMP_PATH\"",
        "    chmod 700 \"$ENROOT_RUNTIME_PATH\" \"$ENROOT_CACHE_PATH\" \"$ENROOT_DATA_PATH\" \"$ENROOT_TEMP_PATH\"",
        "    enroot create -f -n \"$container_name\" \"$REMOTE_CONTAINER_IMAGE\" >/dev/null",
        "    trap 'enroot remove -f \"$container_name\" >/dev/null 2>&1 || true; rm -rf \"$enroot_base\" >/dev/null 2>&1 || true' EXIT",
        "    enroot start --root --rw --mount \"$REMOTE_HOST_ANSYS_ROOT:/mnt/AnsysEM\" --mount \"$REMOTE_HOST_ANSYS_BASE:/ansys_inc/v252\" --mount \"$REMOTE_HOST_ANSYS_BASE/licensingclient:/mnt/licensingclient\" --mount \"$case_dir:/work\" --mount \"$JOB_TMPFS_ROOT:/job_tmpfs\" --mount \"$JOB_DISK_ROOT:/job_disk\" \"$container_name\" /bin/bash -lc \"set -euo pipefail; case_ram_root=/job_tmpfs/$case_name; case_disk_root=/job_disk/$case_name; mkdir -p \\\"$case_ram_root/tmp\\\" \\\"$case_ram_root/ansys_tmp\\\" \\\"$case_disk_root/home\\\" \\\"$case_disk_root/tmp\\\" \\\"$case_disk_root/ansys_tmp\\\"; export HOME=\\\"$case_disk_root/home\\\"; export PEETS_RAMDISK_ROOT=\\\"$case_ram_root\\\"; export PEETS_RAMDISK_TMPDIR=\\\"$case_ram_root/tmp\\\"; export PEETS_RAMDISK_ANSYS_WORK_DIR=\\\"$case_ram_root/ansys_tmp\\\"; export PEETS_DISK_ROOT=\\\"$case_disk_root\\\"; export PEETS_DISK_TMPDIR=\\\"$case_disk_root/tmp\\\"; export PEETS_DISK_ANSYS_WORK_DIR=\\\"$case_disk_root/ansys_tmp\\\"; export ANSYS_WORK_DIR=\\\"$case_ram_root/ansys_tmp\\\"; export TEMP=\\\"$case_disk_root/tmp\\\"; export TMPDIR=\\\"$case_disk_root/tmp\\\"; export XDG_CONFIG_HOME=\\\"$case_disk_root/home/.config\\\"; cd /work; export ANS_IGNOREOS=1; bash ./remote_job.sh\"",
        "  )",
        "}",
        "sync_case_artifacts_back() {",
        "  case_dir=\"$1\"",
        "  case_name=\"$(basename \"$case_dir\")\"",
        "  if [ -z \"${REMOTE_JOB_DIR:-}\" ] || [ ! -d \"$case_dir\" ]; then",
        "    return 0",
        "  fi",
        "  mkdir -p \"$REMOTE_JOB_DIR/$case_name\"",
        "  (",
        "    cd \"$case_dir\"",
        "    shopt -s nullglob",
        "    files=()",
        "    for path in run.log exit.code batch.log report_export.error.log license_diagnostics.txt runtime_logs.json ansys_grpc.stdout.log ansys_grpc.stderr.log pyaedt*.log remote_job.sh run_sim.py project.aedt *.q *.q.complete *.q.completed all_reports design_outputs project.aedtresults; do",
        "      [ -e \"$path\" ] && files+=(\"$path\")",
        "    done",
        "    if [ \"${#files[@]}\" -eq 0 ]; then",
        "      exit 0",
        "    fi",
        "    tar -czf - \"${files[@]}\"",
        "  ) | (",
        "    cd \"$REMOTE_JOB_DIR/$case_name\"",
        "    tar --no-same-owner -xzf -",
        "  ) >/dev/null 2>&1 || true",
        "}",
        "sync_bundle_artifacts_back() {",
        "  if [ -z \"${REMOTE_JOB_DIR:-}\" ]; then",
        "    return 0",
        "  fi",
        "  mkdir -p \"$REMOTE_JOB_DIR\"",
        "  shopt -s nullglob",
        "  files=()",
        "  for path in case_summary.txt failed.count results.tgz.ready results.tgz; do",
        "    [ -e \"$path\" ] && files+=(\"$path\")",
        "  done",
        "  if [ \"${#files[@]}\" -eq 0 ]; then",
        "    return 0",
        "  fi",
        "  tar -czf - \"${files[@]}\" | (",
        "    cd \"$REMOTE_JOB_DIR\"",
        "    tar --no-same-owner -xzf -",
        "  ) >/dev/null 2>&1 || true",
        "}",
    ]
    for case_index in range(1, case_count + 1):
        case_name = f"case_{case_index:02d}"
        source_name = f"project_{case_index:02d}.aedt"
        payload_lines.extend(
            [
                f"mkdir -p {shlex.quote(case_name)}",
                f"cp -f {shlex.quote(source_name)} {shlex.quote(case_name)}/project.aedt",
                f"cp -f remote_job.sh {shlex.quote(case_name)}/remote_job.sh",
            ]
        )
    payload_lines.extend(
        [
            "for i in $(seq 1 " + str(case_count) + "); do",
            "  queued_remaining=$(( " + str(case_count) + " - i + 1 ))",
            "  while true; do",
            "    active_now=$(count_case_jobs)",
            "    read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
            "  $(compute_target_slots \"$active_now\" \"$queued_remaining\")",
            "EOF",
            "    idle_now=$(( target_now - active_now ))",
            "    if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
            "    emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\" \"$queued_remaining\"",
            "    if [ \"$active_now\" -lt \"$target_now\" ]; then",
            "      break",
            "    fi",
            "    if [ \"$active_now\" -gt 0 ]; then",
            "      wait -n \"${case_pids[@]}\" || true",
            "    else",
            "      sleep \"$PEETS_SLOT_MEMORY_PROBE_INTERVAL\"",
            "    fi",
            "  done",
            "  case_dir=$(printf 'case_%02d' \"$i\")",
            "  (",
            "    cd \"$case_dir\"",
            "    mkdir -p tmp",
            "    export TMPDIR=\"$PWD/tmp\"",
            "    case_dir_path=\"$PWD\"",
            "    sync_pid=''",
            "    periodic_case_sync() {",
            "      while sleep 5; do",
            "        sync_case_artifacts_back \"$case_dir_path\"",
            "      done",
            "    }",
            "    emit_slot_snapshot \"$case_dir\" \"RUNNING\" 0",
            "    periodic_case_sync &",
            "    sync_pid=$!",
            (
                f"    if PEETS_SLOT_CORES={config.cores_per_slot} "
                f"PEETS_SLOT_TASKS={tasks_per_slot} run_case_command \"$case_dir_path\" > run.log 2>&1; then"
            ),
            "      rc=0",
            "    else",
            "      rc=$?",
            "    fi",
            "    if [ -n \"$sync_pid\" ]; then",
            "      kill \"$sync_pid\" >/dev/null 2>&1 || true",
            "      wait \"$sync_pid\" >/dev/null 2>&1 || true",
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
            "    sync_case_artifacts_back \"$case_dir_path\"",
            "  ) &",
            "  case_pids+=(\"$!\")",
            "  active_now=$(count_case_jobs)",
            "  queued_after_launch=$(( " + str(case_count) + " - i ))",
            "  read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
            "  $(compute_target_slots \"$active_now\" \"$queued_after_launch\")",
            "EOF",
            "  idle_now=$(( target_now - active_now ))",
            "  if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
            "  emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\" \"$queued_after_launch\"",
            "done",
            "while [ \"$(count_case_jobs)\" -gt 0 ]; do",
            "  wait -n \"${case_pids[@]}\" || true",
            "  active_now=$(count_case_jobs)",
            "  read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
            "  $(compute_target_slots \"$active_now\" 0)",
            "EOF",
            "  idle_now=$(( target_now - active_now ))",
            "  if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
            "  emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\" 0",
            "done",
        ]
    )
    payload_lines.extend(
        [
            _build_case_aggregation_command(case_count),
            "mkdir -p \"$REMOTE_RUNTIME_ROOT\"",
            "archive_path=$(mktemp \"$REMOTE_RUNTIME_ROOT/results.${SLURM_JOB_ID:-nojob}.XXXXXX.tgz\")",
            "cleanup_archive() { rm -f \"$archive_path\"; }",
            "trap cleanup_archive EXIT",
            "tar -czf \"$archive_path\" case_* case_summary.txt failed.count",
            "cp -f \"$archive_path\" results.tgz",
            "touch results.tgz.ready",
            "sync_bundle_artifacts_back",
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


def _build_pull_worker_payload_script_content(
    *,
    config: RemoteJobConfig,
    run_id: str,
    worker_id: str,
) -> str:
    configured_max_parallel = int(getattr(config, "slot_max_concurrency", getattr(config, "slots_per_job", 1)))
    max_parallel = max(1, configured_max_parallel)
    min_parallel = max(1, int(getattr(config, "slot_min_concurrency", min(5, max_parallel))))
    max_parallel = max(min_parallel, max_parallel)
    tasks_per_slot = int(getattr(config, "tasks_per_slot", 1))
    container_runtime = _remote_container_runtime(config)
    container_image = _remote_container_image(config)
    host_ansys_root = _remote_host_ansys_mount_root(config)
    host_ansys_base = _remote_host_ansys_base_root(config)
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
    lease_ttl_seconds = max(30, int(getattr(config, "lease_ttl_seconds", 120)))
    lease_heartbeat_seconds = max(5, int(getattr(config, "lease_heartbeat_seconds", 15)))
    worker_idle_poll_seconds = max(1, int(getattr(config, "worker_idle_poll_seconds", 10)))
    slot_request_backoff_seconds = max(1, int(getattr(config, "slot_request_backoff_seconds", 5)))
    runtime_root = _remote_path_for_shell(config=config, path="/tmp/$USER/peetsfea-runner/runtime")
    payload_lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}",
        f"REMOTE_RUNTIME_ROOT={_double_quoted_shell_value(runtime_root)}",
        "mkdir -p \"$REMOTE_RUNTIME_ROOT\"",
        "workdir=$(mktemp -d \"$REMOTE_RUNTIME_ROOT/pull.${SLURM_JOB_ID:-nojob}.XXXXXX\")",
        "slots_root=\"$workdir/slots\"",
        "artifacts_root=\"$workdir/artifacts\"",
        "mkdir -p \"$slots_root\" \"$artifacts_root\"",
        "JOB_TMPFS_ROOT=\"$workdir/job_tmpfs\"",
        "JOB_DISK_ROOT=\"$workdir/job_disk\"",
        "JOB_DISK_BUDGET_GB=" + str(JOB_DISK_FILESYSTEM_SIZE_GB),
        "enter_job_filesystem_namespace() {",
        "  mkdir -p \"$JOB_TMPFS_ROOT\" \"$JOB_DISK_ROOT\"",
        "  if [ \"${PEETS_JOB_TMPFS_NAMESPACE_READY:-0}\" = \"1\" ]; then",
        "    return 0",
        "  fi",
        "  if ! command -v unshare >/dev/null 2>&1; then",
        "    echo \"[ERROR] unshare is required for rootless job tmpfs setup\" >&2",
        "    return 127",
        "  fi",
        "  exec unshare --user --map-root-user --mount /bin/bash -c 'set -euo pipefail; job_tmpfs_root=\"$1\"; shift; mount -t tmpfs -o size="
        + str(JOB_TMPFS_SIZE_GB)
        + "G tmpfs \"$job_tmpfs_root\"; export PEETS_JOB_TMPFS_NAMESPACE_READY=1; export PEETS_JOB_TMPFS_NAMESPACE_ACTIVE=1; exec \"$@\"' /bin/bash \"$JOB_TMPFS_ROOT\" /bin/bash \"$0\" \"$@\"",
        "}",
        "cleanup_workdir() {",
        "  cd \"$REMOTE_RUNTIME_ROOT\" >/dev/null 2>&1 || true",
        "  if [ \"${PEETS_JOB_TMPFS_NAMESPACE_ACTIVE:-0}\" = \"1\" ] && awk -v target=\"$JOB_TMPFS_ROOT\" '$2 == target && $3 == \"tmpfs\" {found=1} END {exit found?0:1}' /proc/mounts; then",
        "    umount \"$JOB_TMPFS_ROOT\" >/dev/null 2>&1 || true",
        "  fi",
        "  rm -rf \"$workdir\" >/dev/null 2>&1 || true",
        "  rmdir \"$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}\" >/dev/null 2>&1 || true",
        "}",
        "teardown_control_plane() {",
        "  :",
        "}",
        "cleanup() {",
        "  rc=$?",
        "  teardown_control_plane",
        "  cleanup_workdir",
        "  exit \"$rc\"",
        "}",
        "trap cleanup EXIT",
        "enter_job_filesystem_namespace \"$@\"",
        f"PEETS_CONTROL_RUN_ID={shlex.quote(run_id)}",
        f"PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id)}",
        f"PEETS_CONTROL_HOST={shlex.quote(control_plane_host)}",
        f"PEETS_CONTROL_PORT={control_plane_port}",
        f"PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
        f"PEETS_CONTROL_RETURN_HOST={shlex.quote(control_plane_return_host)}",
        f"PEETS_CONTROL_RETURN_USER={shlex.quote(control_plane_return_user)}",
        f"PEETS_CONTROL_RETURN_PORT={control_plane_return_port}",
        f"PEETS_CONTROL_HEARTBEAT_INTERVAL={heartbeat_interval_seconds}",
        f"PEETS_SLOT_MIN_CONCURRENCY={min_parallel}",
        f"max_parallel={max_parallel}",
        f"PEETS_SLOT_MEMORY_PRESSURE_HIGH_WATERMARK={int(getattr(config, 'slot_memory_pressure_high_watermark_percent', 90))}",
        f"PEETS_SLOT_MEMORY_PRESSURE_RESUME_WATERMARK={int(getattr(config, 'slot_memory_pressure_resume_watermark_percent', 80))}",
        f"PEETS_SLOT_MEMORY_PROBE_INTERVAL={max(1, int(getattr(config, 'slot_memory_probe_interval_seconds', 5)))}",
        f"PEETS_LEASE_TTL_SECONDS={lease_ttl_seconds}",
        f"PEETS_LEASE_HEARTBEAT_INTERVAL={lease_heartbeat_seconds}",
        f"PEETS_WORKER_IDLE_POLL={worker_idle_poll_seconds}",
        f"PEETS_SLOT_REQUEST_BACKOFF={slot_request_backoff_seconds}",
        "PEETS_CONTROL_LOCAL_PORT=$((PEETS_CONTROL_PORT + 1000 + (${SLURM_JOB_ID:-0} % 1000)))",
        "PEETS_TUNNEL_SOCKET=\"$workdir/control-plane.sock\"",
        "PEETS_TUNNEL_SESSION_ID=\"${SLURM_JOB_ID:-nojob}-${PEETS_CONTROL_WORKER_ID:-worker}-$$\"",
        "PEETS_CONTROL_API_URL=\"http://127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}\"",
        "PEETS_CONTROL_SSH_IDENTITY=\"${PEETS_CONTROL_SSH_IDENTITY:-}\"",
        f"PEETS_REMOTE_CONTAINER_RUNTIME={shlex.quote(container_runtime)}",
        (
            "REMOTE_CONTAINER_IMAGE="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=container_image))
        ),
        (
            "REMOTE_HOST_ANSYS_ROOT="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=host_ansys_root))
        ),
        (
            "REMOTE_HOST_ANSYS_BASE="
            + _double_quoted_shell_value(_remote_path_for_shell(config=config, path=host_ansys_base))
        ),
        "control_python_bin() { command -v python3 || command -v python || true; }",
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
        "memory_stats() {",
        "  local total_kb avail_kb used_kb pressure_pct",
        "  total_kb=$(awk '/MemTotal/ {print $2+0}' /proc/meminfo 2>/dev/null || echo 0)",
        "  avail_kb=$(awk '/MemAvailable/ {print $2+0}' /proc/meminfo 2>/dev/null || echo 0)",
        "  used_kb=$(( total_kb - avail_kb ))",
        "  if [ \"$used_kb\" -lt 0 ]; then used_kb=0; fi",
        "  if [ \"$total_kb\" -gt 0 ]; then",
        "    pressure_pct=$(( (used_kb * 100) / total_kb ))",
        "  else",
        "    pressure_pct=0",
        "  fi",
        "  printf '%s %s %s %s\\n' \"$(( total_kb / 1024 ))\" \"$(( avail_kb / 1024 ))\" \"$(( used_kb / 1024 ))\" \"$pressure_pct\"",
        "}",
        "memory_gate_open=1",
        "last_target_slots=''",
        "last_memory_gate=''",
        "control_plane_request_json() {",
        "  endpoint=\"$1\"",
        "  payload=\"${2:-{}}\"",
        "  method=\"${3:-POST}\"",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_PAYLOAD=\"$payload\" CONTROL_METHOD=\"$method\" \"$control_python\" - <<'PY'",
        "import os",
        "import urllib.request",
        "import urllib.error",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'],",
        "    data=os.environ['CONTROL_PAYLOAD'].encode('utf-8') if os.environ['CONTROL_METHOD'] != 'GET' else None,",
        "    headers={'Content-Type': 'application/json'},",
        "    method=os.environ['CONTROL_METHOD'],",
        ")",
        "try:",
        "    with urllib.request.urlopen(req, timeout=30) as resp:",
        "        print(resp.read().decode('utf-8'))",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'control_plane_request_json endpoint={os.environ[\"CONTROL_ENDPOINT\"]} status={exc.code} detail={detail}')",
        "PY",
        "}",
        "control_plane_upload_file() {",
        "  endpoint=\"$1\"",
        "  file_path=\"$2\"",
        "  content_type=\"${3:-application/octet-stream}\"",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_FILE_PATH=\"$file_path\" CONTROL_CONTENT_TYPE=\"$content_type\" \"$control_python\" - <<'PY'",
        "import os",
        "import urllib.request",
        "import urllib.error",
        "with open(os.environ['CONTROL_FILE_PATH'], 'rb') as handle:",
        "    body = handle.read()",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'],",
        "    data=body,",
        "    headers={'Content-Type': os.environ['CONTROL_CONTENT_TYPE']},",
        "    method='POST',",
        ")",
        "try:",
        "    with urllib.request.urlopen(req, timeout=300) as resp:",
        "        resp.read()",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'control_plane_upload_file endpoint={os.environ[\"CONTROL_ENDPOINT\"]} status={exc.code} detail={detail}')",
        "PY",
        "}",
        "control_plane_download() {",
        "  endpoint=\"$1\"",
        "  output_path=\"$2\"",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_OUTPUT_PATH=\"$output_path\" \"$control_python\" - <<'PY'",
        "import os",
        "import urllib.request",
        "import urllib.error",
        "try:",
        "    with urllib.request.urlopen(os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'], timeout=300) as resp:",
        "        data = resp.read()",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'control_plane_download endpoint={os.environ[\"CONTROL_ENDPOINT\"]} status={exc.code} detail={detail}')",
        "with open(os.environ['CONTROL_OUTPUT_PATH'], 'wb') as handle:",
        "    handle.write(data)",
        "PY",
        "}",
        "emit_scheduler_event() {",
        "  stage=\"$1\"",
        "  message=\"$2\"",
        "  control_plane_request_json /internal/events/worker \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"stage\\\":\\\"${stage}\\\",\\\"message\\\":\\\"${message}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "compute_target_slots() {",
        "  active_now=\"$1\"",
        "  read -r total_mem_mb avail_mem_mb used_mem_mb memory_pressure_pct <<EOF",
        "  $(memory_stats)",
        "EOF",
        "  if [ \"${memory_pressure_pct:-0}\" -ge \"$PEETS_SLOT_MEMORY_PRESSURE_HIGH_WATERMARK\" ]; then",
        "    memory_gate_open=0",
        "  elif [ \"${memory_pressure_pct:-0}\" -le \"$PEETS_SLOT_MEMORY_PRESSURE_RESUME_WATERMARK\" ]; then",
        "    memory_gate_open=1",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 1 ]; then",
        "    if [ \"$active_now\" -lt \"$PEETS_SLOT_MIN_CONCURRENCY\" ]; then",
        "      target_slots=\"$PEETS_SLOT_MIN_CONCURRENCY\"",
        "    else",
        "      target_slots=\"$max_parallel\"",
        "    fi",
        "  else",
        "    target_slots=\"$active_now\"",
        "  fi",
        "  if [ \"$target_slots\" -gt \"$max_parallel\" ]; then target_slots=\"$max_parallel\"; fi",
        "  if [ \"$target_slots\" -lt \"$active_now\" ]; then target_slots=\"$active_now\"; fi",
        "  if [ \"$last_target_slots\" != \"$target_slots\" ] && [ \"$target_slots\" -gt \"$active_now\" ]; then",
        "    emit_scheduler_event SLOT_SCALE_UP \"target_slots=${target_slots} active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct}\"",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 0 ] && [ \"$last_memory_gate\" != \"0\" ]; then",
        "    emit_scheduler_event SLOT_START_BLOCKED_MEMORY \"active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct}\"",
        "  fi",
        "  if [ \"$memory_gate_open\" -eq 0 ] && [ \"$active_now\" -gt 0 ]; then",
        "    emit_scheduler_event SLOT_DRAIN_CONTINUE \"active_slots=${active_now} memory_pressure_pct=${memory_pressure_pct}\"",
        "  fi",
        "  last_target_slots=\"$target_slots\"",
        "  last_memory_gate=\"$memory_gate_open\"",
        "  printf '%s %s %s %s %s\\n' \"$target_slots\" \"$memory_pressure_pct\" \"$memory_gate_open\" \"$avail_mem_mb\" \"$used_mem_mb\"",
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
        "  local -a ssh_args=(-F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -o BatchMode=yes -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)",
        "  if [ -n \"$PEETS_CONTROL_SSH_IDENTITY\" ]; then",
        "    ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")",
        "  fi",
        "  ssh \"${ssh_args[@]}\" -M -S \"$PEETS_TUNNEL_SOCKET\" -fnNT \\",
        "    -L 127.0.0.1:${PEETS_CONTROL_LOCAL_PORT}:127.0.0.1:${PEETS_CONTROL_PORT} \\",
        "    \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\"",
        "}",
        "stop_control_tunnel() {",
        "  if [ -S \"$PEETS_TUNNEL_SOCKET\" ]; then",
        "    local -a ssh_args=(-F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)",
        "    if [ -n \"$PEETS_CONTROL_SSH_IDENTITY\" ]; then",
        "      ssh_args+=(-o IdentitiesOnly=yes -i \"$PEETS_CONTROL_SSH_IDENTITY\")",
        "    fi",
        "    ssh \"${ssh_args[@]}\" -S \"$PEETS_TUNNEL_SOCKET\" -O exit \"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}\" >/dev/null 2>&1 || true",
        "  fi",
        "}",
        "emit_control_register() {",
        "  control_plane_request_json /internal/workers/register \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"slurm_job_id\\\":\\\"${SLURM_JOB_ID:-}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_heartbeat() {",
        "  control_plane_request_json /internal/workers/heartbeat \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_degraded() {",
        "  stage=\"$1\"",
        "  reason=\"$2\"",
        "  control_plane_request_json /internal/workers/degraded \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"stage\\\":\\\"${stage}\\\",\\\"reason\\\":\\\"${reason}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "emit_control_recovered() {",
        "  control_plane_request_json /internal/workers/recovered \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"tunnel_session_id\\\":\\\"${PEETS_TUNNEL_SESSION_ID}\\\",\\\"observed_node\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\"}\" >/dev/null 2>&1 || true",
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
        "  control_plane_request_json /internal/resources/node \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"allocated_mem_mb\\\":${total_mem_mb:-0},\\\"total_mem_mb\\\":${total_mem_mb:-0},\\\"used_mem_mb\\\":${used_mem_mb:-0},\\\"free_mem_mb\\\":${avail_mem_mb:-0},\\\"load_1\\\":${load_1:-0},\\\"load_5\\\":${load_5:-0},\\\"load_15\\\":${load_15:-0},\\\"tmp_total_mb\\\":${tmp_total_mb:-0},\\\"tmp_used_mb\\\":${tmp_used_mb:-0},\\\"tmp_free_mb\\\":${tmp_free_mb:-0},\\\"process_count\\\":${process_count:-0},\\\"running_worker_count\\\":1,\\\"active_slot_count\\\":$(count_case_jobs)}\" >/dev/null 2>&1 || true",
        "}",
        "emit_worker_snapshot() {",
        "  active_slots=\"${1:-0}\"",
        "  idle_slots=\"${2:-0}\"",
        "  target_slots=\"${3:-0}\"",
        "  memory_pressure_pct=\"${4:-0}\"",
        "  memory_gate_open=\"${5:-1}\"",
        "  rss_mb=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$$/status 2>/dev/null || echo 0)",
        "  cpu_pct=$(ps -p $$ -o %cpu= 2>/dev/null | awk '{print $1+0}' || echo 0)",
        "  process_count=$(ps --no-headers --ppid $$ 2>/dev/null | wc -l | awk '{print $1+0}')",
        "  control_plane_request_json /internal/resources/worker \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"slurm_job_id\\\":\\\"${SLURM_JOB_ID:-}\\\",\\\"configured_slots\\\":${max_parallel},\\\"active_slots\\\":${active_slots},\\\"idle_slots\\\":${idle_slots},\\\"target_slots\\\":${target_slots},\\\"memory_pressure_pct\\\":${memory_pressure_pct},\\\"memory_gate_open\\\":${memory_gate_open},\\\"queued_slots_inside_worker\\\":0,\\\"rss_mb\\\":${rss_mb:-0},\\\"cpu_pct\\\":${cpu_pct:-0},\\\"tunnel_state\\\":\\\"CONNECTED\\\",\\\"process_count\\\":${process_count:-0}}\" >/dev/null 2>&1 || true",
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
        "  control_plane_request_json /internal/resources/slot \"{\\\"run_id\\\":\\\"${PEETS_CONTROL_RUN_ID}\\\",\\\"worker_id\\\":\\\"${PEETS_CONTROL_WORKER_ID}\\\",\\\"slot_id\\\":\\\"${slot_id}\\\",\\\"host\\\":\\\"${SLURMD_NODENAME:-${HOSTNAME:-}}\\\",\\\"allocated_mem_mb\\\":${allocated_mem_mb:-0},\\\"used_mem_mb\\\":${used_mem_mb:-0},\\\"load_1\\\":${load_1:-0},\\\"rss_mb\\\":${rss_mb:-0},\\\"cpu_pct\\\":${cpu_pct:-0},\\\"process_count\\\":${process_count:-0},\\\"active_process_count\\\":${active_process_count:-0},\\\"artifact_bytes\\\":${artifact_bytes:-0},\\\"progress_ts\\\":\\\"${progress_ts}\\\",\\\"state\\\":\\\"${slot_state}\\\"}\" >/dev/null 2>&1 || true",
        "}",
        "heartbeat_loop() {",
        "  while sleep \"$PEETS_CONTROL_HEARTBEAT_INTERVAL\"; do",
        "    emit_control_heartbeat",
        "    emit_node_snapshot",
        "    active_now=$(count_case_jobs)",
        "    read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
        "  $(compute_target_slots \"$active_now\")",
        "EOF",
        "    idle_now=$(( target_now - active_now ))",
        "    if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
        "    emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\"",
        "  done",
        "}",
        "request_lease() {",
        "  local control_python",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  response=$(",
        "    CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_RUN_ID=\"$PEETS_CONTROL_RUN_ID\" CONTROL_WORKER_ID=\"$PEETS_CONTROL_WORKER_ID\" CONTROL_ACCOUNT_ID=\"${PEETS_ACCOUNT_ID:-account_01}\" CONTROL_SLURM_JOB_ID=\"${SLURM_JOB_ID:-}\" \"$control_python\" - <<'PY'",
        "import json, os, urllib.request, urllib.error",
        "payload = {",
        "    'run_id': os.environ.get('CONTROL_RUN_ID', ''),",
        "    'worker_id': os.environ.get('CONTROL_WORKER_ID', ''),",
        "    'account_id': os.environ.get('CONTROL_ACCOUNT_ID', ''),",
        "    'slurm_job_id': os.environ.get('CONTROL_SLURM_JOB_ID', ''),",
        "}",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + '/internal/leases/request',",
        "    data=json.dumps(payload).encode('utf-8'),",
        "    headers={'Content-Type': 'application/json'},",
        "    method='POST',",
        ")",
        "try:",
        "    with urllib.request.urlopen(req, timeout=30) as resp:",
        "        print(resp.read().decode('utf-8'))",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'control_plane_request_json endpoint=/internal/leases/request status={exc.code} detail={detail}')",
        "PY",
        "  ) || return 1",
        "  CONTROL_RESPONSE=\"$response\" \"$control_python\" - <<'PY'",
        "import json, os",
        "payload = json.loads(os.environ.get('CONTROL_RESPONSE') or '{}')",
        "if not payload.get('lease_available'):",
        "    print('')",
        "else:",
        "    print('\\t'.join([str(payload.get('lease_token') or ''), str(payload.get('slot_id') or ''), str(payload.get('input_name') or '')]))",
        "PY",
        "}",
        "lease_control_request() {",
        "  endpoint=\"$1\"",
        "  lease_token=\"$2\"",
        "  slot_state=\"${3:-}\"",
        "  reason=\"${4:-}\"",
        "  local control_python response",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  response=$(",
        "    CONTROL_API_URL=\"$PEETS_CONTROL_API_URL\" CONTROL_ENDPOINT=\"$endpoint\" CONTROL_RUN_ID=\"$PEETS_CONTROL_RUN_ID\" CONTROL_LEASE_TOKEN=\"$lease_token\" CONTROL_SLOT_STATE=\"$slot_state\" CONTROL_REASON=\"$reason\" \"$control_python\" - <<'PY'",
        "import json, os, urllib.request, urllib.error",
        "payload = {",
        "    'run_id': os.environ.get('CONTROL_RUN_ID', ''),",
        "    'lease_token': os.environ.get('CONTROL_LEASE_TOKEN', ''),",
        "}",
        "slot_state = os.environ.get('CONTROL_SLOT_STATE', '')",
        "if slot_state:",
        "    payload['slot_state'] = slot_state",
        "reason = os.environ.get('CONTROL_REASON', '')",
        "if reason:",
        "    payload['reason'] = reason",
        "req = urllib.request.Request(",
        "    os.environ['CONTROL_API_URL'] + os.environ['CONTROL_ENDPOINT'],",
        "    data=json.dumps(payload).encode('utf-8'),",
        "    headers={'Content-Type': 'application/json'},",
        "    method='POST',",
        ")",
        "try:",
        "    with urllib.request.urlopen(req, timeout=30) as resp:",
        "        print(resp.read().decode('utf-8'))",
        "except urllib.error.HTTPError as exc:",
        "    detail = exc.read().decode('utf-8', 'replace')",
        "    raise SystemExit(f'lease_control_request endpoint={os.environ[\"CONTROL_ENDPOINT\"]} status={exc.code} detail={detail}')",
        "PY",
        "  ) || return 1",
        "  printf '%s\\n' \"$response\"",
        "}",
        "prepare_case_dir() {",
        "  case_dir=\"$1\"",
        "  rm -rf \"$case_dir\" >/dev/null 2>&1 || true",
        "  mkdir -p \"$case_dir\"",
        "}",
        "get_available_port() {",
        "  local control_python",
        "  control_python=\"$(control_python_bin)\"",
        "  if [ -z \"$control_python\" ]; then",
        "    return 1",
        "  fi",
        "  \"$control_python\" - <<'PY'",
        "import socket",
        "with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:",
        "    sock.bind(('127.0.0.1', 0))",
        "    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
        "    print(sock.getsockname()[1])",
        "PY",
        "}",
        "copy_shared_session_logs() {",
        "  case_dir=\"$1\"",
        "  session_root=\"$2\"",
        "  for log_name in ansys_grpc.stdout.log ansys_grpc.stderr.log; do",
        "    if [ -f \"$session_root/$log_name\" ]; then",
        "      cp -f \"$session_root/$log_name\" \"$case_dir/$log_name\" >/dev/null 2>&1 || true",
        "    fi",
        "  done",
        "}",
        "summarize_bootstrap_failure() {",
        "  session_root=\"$1\"",
        "  summary=\"$(tail -n 40 \"$session_root/enroot.create.stderr\" \"$session_root/session.stderr\" 2>/dev/null | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/^ //;s/ $//')\"",
        "  if [ -z \"$summary\" ]; then",
        "    summary=\"slot session container bootstrap failed\"",
        "  fi",
        "  printf '%s\\n' \"$summary\"",
        "}",
        "write_slot_session_bootstrap_script() {",
        "  session_id=\"$1\"",
        "  session_root=\"$2\"",
        "  grpc_port=\"$3\"",
        "  {",
        "    printf '%s\\n' '#!/usr/bin/env bash'",
        "    printf '%s\\n' 'set -euo pipefail'",
        "    printf '%s\\n' \"session_ram_root=\\\"/job_tmpfs/$session_id\\\"\"",
        "    printf '%s\\n' \"session_disk_root=\\\"/job_disk/$session_id\\\"\"",
        "    printf '%s\\n' 'mkdir -p \"$session_ram_root\" \"$session_ram_root/tmp\" \"$session_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'mkdir -p \"$session_disk_root/home\" \"$session_disk_root/tmp\" \"$session_disk_root/ansys_tmp\"'",
        "    printf '%s\\n' 'mkdir -p /work/session_logs'",
        "    printf '%s\\n' 'export HOME=\"$session_disk_root/home\"'",
        "    printf '%s\\n' 'export XDG_CONFIG_HOME=\"$session_disk_root/home/.config\"'",
        "    printf '%s\\n' 'export TMPDIR=\"$session_disk_root/tmp\"'",
        "    printf '%s\\n' 'export TEMP=\"$session_disk_root/tmp\"'",
        "    printf '%s\\n' 'export ANSYS_WORK_DIR=\"$session_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_ROOT=\"$session_ram_root\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_TMPDIR=\"$session_disk_root/tmp\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_ANSYS_WORK_DIR=\"$session_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export PEETS_DISK_ROOT=\"$session_disk_root\"'",
        "    printf '%s\\n' 'export PEETS_DISK_TMPDIR=\"$session_disk_root/tmp\"'",
        "    printf '%s\\n' 'export PEETS_DISK_ANSYS_WORK_DIR=\"$session_disk_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81'",
        "    printf '%s\\n' 'export ANSYSEM_ROOT252=/mnt/AnsysEM'",
        "    printf '%s\\n' 'export ANS_IGNOREOS=1'",
        "    printf '%s\\n' 'export PATH=/mnt/AnsysEM:/usr/bin:/bin:/usr/sbin:/sbin'",
        "    printf '%s\\n' 'export LD_LIBRARY_PATH=/mnt/AnsysEM:/mnt/AnsysEM/Delcross:/mnt/AnsysEM/common/mono/Linux64/lib64'",
        "    printf '%s\\n' 'export PEETS_ORIGINAL_LD_LIBRARY_PATH=/mnt/AnsysEM:/mnt/AnsysEM/Delcross:/mnt/AnsysEM/common/mono/Linux64/lib64'",
        "    printf '%s\\n' \"export PEETS_BOOTSTRAP_GRPC_PORT=\\\"$grpc_port\\\"\"",
        "    printf '%s\\n' 'cd /work'",
        "    printf '%s\\n' 'nohup /mnt/AnsysEM/ansysedt -ng -waitforlicense -grpcsrv \"$PEETS_BOOTSTRAP_GRPC_PORT\" > /work/ansys_grpc.stdout.log 2> /work/ansys_grpc.stderr.log < /dev/null &'",
        "    printf '%s\\n' 'ansys_pid=$!'",
        "    printf '%s\\n' 'echo \"$ansys_pid\" > /work/ansys.pid'",
        "    printf '%s\\n' \"/opt/miniconda3/bin/python - <<'PY'\"",
        "    printf '%s\\n' 'import os'",
        "    printf '%s\\n' 'import socket'",
        "    printf '%s\\n' 'import time'",
        "    printf '%s\\n' \"port = int(os.environ['PEETS_BOOTSTRAP_GRPC_PORT'])\"",
        "    printf '%s\\n' 'deadline = time.time() + 240'",
        "    printf '%s\\n' 'while time.time() < deadline:'",
        "    printf '%s\\n' '    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:'",
        "    printf '%s\\n' '        sock.settimeout(1.0)'",
        "    printf '%s\\n' \"        if sock.connect_ex(('127.0.0.1', port)) == 0:\"",
        "    printf '%s\\n' '            raise SystemExit(0)'",
        "    printf '%s\\n' '    time.sleep(1.0)'",
        "    printf '%s\\n' 'raise SystemExit(1)'",
        "    printf '%s\\n' 'PY'",
        "    printf '%s\\n' 'touch /work/session.ready'",
        "    printf '%s\\n' \"trap 'kill \\\"\\$ansys_pid\\\" >/dev/null 2>&1 || true' EXIT\"",
        "    printf '%s\\n' 'while true; do'",
        "    printf '%s\\n' '  sleep 3600'",
        "    printf '%s\\n' 'done'",
        "  } > \"$session_root/session_bootstrap.sh\"",
        "  chmod +x \"$session_root/session_bootstrap.sh\"",
        "}",
        "write_slot_case_exec_script() {",
        "  session_id=\"$1\"",
        "  case_name=\"$2\"",
        "  grpc_port=\"$3\"",
        "  case_dir=\"$4\"",
        "  {",
        "    printf '%s\\n' '#!/usr/bin/env bash'",
        "    printf '%s\\n' 'set -euo pipefail'",
        "    printf '%s\\n' \"case_ram_root=\\\"/job_tmpfs/$session_id/$case_name\\\"\"",
        "    printf '%s\\n' \"case_disk_root=\\\"/job_disk/$session_id/$case_name\\\"\"",
        "    printf '%s\\n' 'mkdir -p \"$case_ram_root\" \"$case_ram_root/tmp\" \"$case_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'mkdir -p \"$case_disk_root/home\" \"$case_disk_root/tmp\" \"$case_disk_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export HOME=\"$case_disk_root/home\"'",
        "    printf '%s\\n' 'export XDG_CONFIG_HOME=\"$case_disk_root/home/.config\"'",
        "    printf '%s\\n' 'export TMPDIR=\"$case_disk_root/tmp\"'",
        "    printf '%s\\n' 'export TEMP=\"$case_disk_root/tmp\"'",
        "    printf '%s\\n' 'export ANSYS_WORK_DIR=\"$case_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_ROOT=\"$case_ram_root\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_TMPDIR=\"$case_disk_root/tmp\"'",
        "    printf '%s\\n' 'export PEETS_RAMDISK_ANSYS_WORK_DIR=\"$case_ram_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export PEETS_DISK_ROOT=\"$case_disk_root\"'",
        "    printf '%s\\n' 'export PEETS_DISK_TMPDIR=\"$case_disk_root/tmp\"'",
        "    printf '%s\\n' 'export PEETS_DISK_ANSYS_WORK_DIR=\"$case_disk_root/ansys_tmp\"'",
        "    printf '%s\\n' 'export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81'",
        "    printf '%s\\n' 'export ANSYSEM_ROOT252=/mnt/AnsysEM'",
        "    printf '%s\\n' 'export ANS_IGNOREOS=1'",
        "    printf '%s\\n' 'export PATH=/mnt/AnsysEM:/usr/bin:/bin:/usr/sbin:/sbin'",
        "    printf '%s\\n' 'export LD_LIBRARY_PATH=/mnt/AnsysEM:/mnt/AnsysEM/Delcross:/mnt/AnsysEM/common/mono/Linux64/lib64'",
        "    printf '%s\\n' 'export PEETS_ORIGINAL_LD_LIBRARY_PATH=/mnt/AnsysEM:/mnt/AnsysEM/Delcross:/mnt/AnsysEM/common/mono/Linux64/lib64'",
        f"    printf '%s\\n' 'export PEETS_SLOT_CORES={config.cores_per_slot}'",
        f"    printf '%s\\n' 'export PEETS_SLOT_TASKS={tasks_per_slot}'",
        "    printf '%s\\n' \"export PEETS_REUSE_GRPC_PORT=\\\"$grpc_port\\\"\"",
        "    printf '%s\\n' \"cd \\\"/work/$case_name\\\"\"",
        "    printf '%s\\n' 'bash ./remote_job.sh'",
        "  } > \"$case_dir/session_case_exec.sh\"",
        "  chmod +x \"$case_dir/session_case_exec.sh\"",
        "}",
        "start_slot_session_container() {",
        "  session_id=\"$1\"",
        "  session_root=\"$2\"",
        "  grpc_port=\"$3\"",
        "  container_name=\"peets-${SLURM_JOB_ID:-nojob}-${session_id}-$$\"",
        "  session_ram_root=\"$JOB_TMPFS_ROOT/$session_id\"",
        "  session_disk_root=\"$JOB_DISK_ROOT/$session_id\"",
        "  mkdir -p \"$session_root\" \"$session_ram_root/tmp\" \"$session_ram_root/ansys_tmp\" \"$session_disk_root/home\" \"$session_disk_root/tmp\" \"$session_disk_root/ansys_tmp\"",
        "  enroot_base=\"$REMOTE_RUNTIME_ROOT/enroot/${SLURM_JOB_ID:-nojob}/${session_id}-$$\"",
        "  bootstrap_create_stdout=\"$session_root/enroot.create.stdout\"",
        "  bootstrap_create_stderr=\"$session_root/enroot.create.stderr\"",
        "  bootstrap_start_stdout=\"$session_root/session.stdout\"",
        "  bootstrap_start_stderr=\"$session_root/session.stderr\"",
        "  mkdir -p \"$enroot_base/runtime\" \"$enroot_base/cache\" \"$enroot_base/data\" \"$enroot_base/tmp\"",
        "  chmod 700 \"$enroot_base/runtime\" \"$enroot_base/cache\" \"$enroot_base/data\" \"$enroot_base/tmp\"",
        "  write_slot_session_bootstrap_script \"$session_id\" \"$session_root\" \"$grpc_port\"",
        "  ENROOT_RUNTIME_PATH=\"$enroot_base/runtime\" ENROOT_CACHE_PATH=\"$enroot_base/cache\" ENROOT_DATA_PATH=\"$enroot_base/data\" ENROOT_TEMP_PATH=\"$enroot_base/tmp\" enroot create -f -n \"$container_name\" \"$REMOTE_CONTAINER_IMAGE\" >\"$bootstrap_create_stdout\" 2>\"$bootstrap_create_stderr\"",
        "  rm -f \"$session_root/session.ready\" \"$session_root/ansys.pid\"",
        "  ENROOT_RUNTIME_PATH=\"$enroot_base/runtime\" ENROOT_CACHE_PATH=\"$enroot_base/cache\" ENROOT_DATA_PATH=\"$enroot_base/data\" ENROOT_TEMP_PATH=\"$enroot_base/tmp\" \\",
        "    enroot start --root --rw --mount \"$REMOTE_HOST_ANSYS_ROOT:/mnt/AnsysEM\" --mount \"$REMOTE_HOST_ANSYS_BASE:/ansys_inc/v252\" --mount \"$REMOTE_HOST_ANSYS_BASE/licensingclient:/mnt/licensingclient\" --mount \"$session_root:/work\" --mount \"$JOB_TMPFS_ROOT:/job_tmpfs\" --mount \"$JOB_DISK_ROOT:/job_disk\" \"$container_name\" /bin/bash /work/session_bootstrap.sh > \"$bootstrap_start_stdout\" 2> \"$bootstrap_start_stderr\" &",
        "  session_pid=$!",
        "  for _wait in $(seq 1 180); do",
        "    if [ -f \"$session_root/session.ready\" ] && kill -0 \"$session_pid\" >/dev/null 2>&1; then",
        "      printf '%s\\t%s\\t%s\\n' \"$container_name\" \"$session_pid\" \"$enroot_base\"",
        "      return 0",
        "    fi",
        "    if ! kill -0 \"$session_pid\" >/dev/null 2>&1; then",
        "      break",
        "    fi",
        "    sleep 1",
        "  done",
        "  kill \"$session_pid\" >/dev/null 2>&1 || true",
        "  wait \"$session_pid\" >/dev/null 2>&1 || true",
        "  enroot remove -f \"$container_name\" >/dev/null 2>&1 || true",
        "  rm -rf \"$enroot_base\" >/dev/null 2>&1 || true",
        "  return 1",
        "}",
        "stop_slot_session_container() {",
        "  container_name=\"$1\"",
        "  session_pid=\"$2\"",
        "  enroot_base=\"$3\"",
        "  kill \"$session_pid\" >/dev/null 2>&1 || true",
        "  wait \"$session_pid\" >/dev/null 2>&1 || true",
        "  ENROOT_RUNTIME_PATH=\"$enroot_base/runtime\" ENROOT_CACHE_PATH=\"$enroot_base/cache\" ENROOT_DATA_PATH=\"$enroot_base/data\" ENROOT_TEMP_PATH=\"$enroot_base/tmp\" enroot remove -f \"$container_name\" >/dev/null 2>&1 || true",
        "  rm -rf \"$enroot_base\" >/dev/null 2>&1 || true",
        "}",
        "run_case_in_slot_session() {",
        "  session_pid=\"$1\"",
        "  enroot_base=\"$2\"",
        "  session_id=\"$3\"",
        "  case_name=\"$4\"",
        "  grpc_port=\"$5\"",
        "  case_dir=\"$6\"",
        "  write_slot_case_exec_script \"$session_id\" \"$case_name\" \"$grpc_port\" \"$case_dir\"",
        "  ENROOT_RUNTIME_PATH=\"$enroot_base/runtime\" ENROOT_CACHE_PATH=\"$enroot_base/cache\" ENROOT_DATA_PATH=\"$enroot_base/data\" ENROOT_TEMP_PATH=\"$enroot_base/tmp\" enroot exec \"$session_pid\" /bin/bash \"/work/$case_name/session_case_exec.sh\"",
        "}",
        "launch_slot() {",
        "  initial_lease_token=\"$1\"",
        "  initial_slot_id=\"$2\"",
        "  initial_input_name=\"$3\"",
        "  (",
        "    session_id=\"slotproc-${PEETS_CONTROL_WORKER_ID}-${BASHPID}\"",
        "    session_root=\"$slots_root/$session_id\"",
        "    grpc_port=\"$(get_available_port || true)\"",
        "    if [ -z \"$grpc_port\" ]; then",
        "      lease_control_request /internal/leases/fail \"$initial_lease_token\" \"\" \"slot session port allocation failed\" >/dev/null 2>&1 || true",
        "      return 1",
        "    fi",
        "    session_info=\"$(start_slot_session_container \"$session_id\" \"$session_root\" \"$grpc_port\" || true)\"",
        "    if [ -z \"$session_info\" ]; then",
        "      bootstrap_reason=\"$(summarize_bootstrap_failure \"$session_root\")\"",
        "      lease_control_request /internal/leases/fail \"$initial_lease_token\" \"\" \"$bootstrap_reason\" >/dev/null 2>&1 || true",
        "      return 1",
        "    fi",
        "    IFS=$'\\t' read -r container_name session_pid enroot_base <<< \"$session_info\"",
        "    lease_token=\"$initial_lease_token\"",
        "    slot_id=\"$initial_slot_id\"",
        "    input_name=\"$initial_input_name\"",
        "    idle_misses=0",
        "    while true; do",
        "      case_dir=\"$session_root/$slot_id\"",
        "      prepare_case_dir \"$case_dir\"",
        "      if ! control_plane_download \"/internal/leases/input?run_id=${PEETS_CONTROL_RUN_ID}&lease_token=${lease_token}\" \"$case_dir/project.aedt\"; then",
        "        lease_control_request /internal/leases/fail \"$lease_token\" \"\" \"input download failed\" >/dev/null 2>> \"$case_dir/control_plane.log\" || true",
        "        rm -rf \"$case_dir\" >/dev/null 2>&1 || true",
        "      else",
        "        cp -f \"$workdir/remote_job.sh.seed\" \"$case_dir/remote_job.sh\"",
        "        cd \"$case_dir\"",
        "        mkdir -p tmp",
        "        export TMPDIR=\"$PWD/tmp\"",
        "        emit_slot_snapshot \"$slot_id\" \"RUNNING\" 0",
        "        slot_hb_pid=''",
        "        slot_heartbeat_loop() {",
        "          while sleep \"$PEETS_LEASE_HEARTBEAT_INTERVAL\"; do",
        "            lease_control_request /internal/leases/heartbeat \"$lease_token\" \"RUNNING\" >/dev/null 2>&1 || true",
        "          done",
        "        }",
        "        lease_control_request /internal/leases/heartbeat \"$lease_token\" \"RUNNING\" >/dev/null 2>&1 || true",
        "        slot_heartbeat_loop &",
        "        slot_hb_pid=$!",
        "        if run_case_in_slot_session \"$session_pid\" \"$enroot_base\" \"$session_id\" \"$slot_id\" \"$grpc_port\" \"$case_dir\" > run.log 2>&1; then",
        "          rc=0",
        "        else",
        "          rc=$?",
        "        fi",
        "        if [ -n \"$slot_hb_pid\" ]; then",
        "          kill \"$slot_hb_pid\" >/dev/null 2>&1 || true",
        "          wait \"$slot_hb_pid\" >/dev/null 2>&1 || true",
        "        fi",
        "        copy_shared_session_logs \"$case_dir\" \"$session_root\"",
        "        echo \"$rc\" > exit.code",
        "        artifact_bytes=$(du -sb . 2>/dev/null | awk '{print $1+0}')",
        "        if [ \"$rc\" -eq 0 ]; then",
        "          emit_slot_snapshot \"$slot_id\" \"SUCCEEDED\" \"$artifact_bytes\"",
        "        else",
        "          emit_slot_snapshot \"$slot_id\" \"FAILED\" \"$artifact_bytes\"",
        "        fi",
        "        artifact_path=\"$artifacts_root/${slot_id}.tgz\"",
        "        tar -czf \"$artifact_path\" -C \"$case_dir\" .",
        "        lease_control_request /internal/leases/heartbeat \"$lease_token\" \"UPLOADING\" >/dev/null 2>&1 || true",
        "        if control_plane_upload_file \"/internal/leases/artifact?run_id=${PEETS_CONTROL_RUN_ID}&lease_token=${lease_token}\" \"$artifact_path\" \"application/gzip\"; then",
        "          if [ \"$rc\" -eq 0 ]; then",
        "            lease_control_request /internal/leases/complete \"$lease_token\" >/dev/null 2>> control_plane.log || true",
        "          else",
        "            failure_reason=$(tail -n 20 run.log 2>/dev/null | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/\"//g' | sed 's/^ //;s/ $//')",
        "            if [ -z \"$failure_reason\" ]; then failure_reason=\"slot exited rc=${rc}\"; fi",
        "            lease_control_request /internal/leases/fail \"$lease_token\" \"\" \"$failure_reason\" >/dev/null 2>> control_plane.log || true",
        "          fi",
        "        else",
        "          lease_control_request /internal/leases/fail \"$lease_token\" \"\" \"artifact upload failed\" >/dev/null 2>> control_plane.log || true",
        "        fi",
        "      fi",
        "      next_lease=\"$(request_lease || true)\"",
        "      if [ -n \"$next_lease\" ]; then",
        "        idle_misses=0",
        "        IFS=$'\\t' read -r lease_token slot_id input_name <<< \"$next_lease\"",
        "        continue",
        "      fi",
        "      idle_misses=$(( idle_misses + 1 ))",
        "      if [ \"$idle_misses\" -ge 2 ]; then",
        "        break",
        "      fi",
        "      sleep \"$PEETS_SLOT_REQUEST_BACKOFF\"",
        "    done",
        "    stop_slot_session_container \"$container_name\" \"$session_pid\" \"$enroot_base\"",
        "  ) &",
        "  case_pids+=(\"$!\")",
        "}",
        "heartbeat_pid=''",
        "if start_control_tunnel 2> control_tunnel_bootstrap.err; then",
        "  emit_control_register",
        "  emit_node_snapshot",
        "  emit_worker_snapshot 0 \"$PEETS_SLOT_MIN_CONCURRENCY\" \"$PEETS_SLOT_MIN_CONCURRENCY\" 0 1",
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
        "cp -f remote_job.sh \"$workdir/remote_job.sh.seed\"",
        "while true; do",
        "  active_now=$(count_case_jobs)",
        "  read -r target_now pressure_now gate_open_now avail_mem_now used_mem_now <<EOF",
        "  $(compute_target_slots \"$active_now\")",
        "EOF",
        "  idle_now=$(( target_now - active_now ))",
        "  if [ \"$idle_now\" -lt 0 ]; then idle_now=0; fi",
        "  emit_worker_snapshot \"$active_now\" \"$idle_now\" \"$target_now\" \"$pressure_now\" \"$gate_open_now\"",
        "  if [ \"$active_now\" -lt \"$target_now\" ]; then",
        "    lease_info=$(request_lease || true)",
        "    if [ -n \"$lease_info\" ]; then",
        "      IFS=$'\\t' read -r lease_token slot_id input_name <<< \"$lease_info\"",
        "      launch_slot \"$lease_token\" \"$slot_id\" \"$input_name\"",
        "      continue",
        "    fi",
        "  fi",
        "  if [ \"$active_now\" -gt 0 ]; then",
        "    wait -n \"${case_pids[@]}\" || true",
        "  else",
        "    sleep \"$PEETS_WORKER_IDLE_POLL\"",
        "  fi",
        "done",
    ]
    return "\n".join(payload_lines)


def _write_pull_worker_payload_script(
    tmpdir: Path,
    *,
    config: RemoteJobConfig,
    run_id: str,
    worker_id: str,
) -> Path:
    script = tmpdir / "remote_pull_worker_payload.sh"
    script.write_text(
        _build_pull_worker_payload_script_content(
            config=config,
            run_id=run_id,
            worker_id=worker_id,
        ),
        encoding="utf-8",
    )
    return script


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
    runtime_root = _remote_runtime_root_shell_path(config=config)
    control_plane_ssh_target = _control_plane_ssh_target(config)
    control_plane_return_host = _control_plane_return_host(config)
    control_plane_return_user = _control_plane_return_user(config)
    control_plane_return_port = _control_plane_return_port(config)
    exclude_nodes = _slurm_exclude_nodes(config)
    exclude_lines = [f"#SBATCH --exclude={','.join(exclude_nodes)}"] if exclude_nodes else []
    partition_value = _slurm_partition_value(config)
    partition_lines = [f"#SBATCH -p {partition_value}"] if partition_value else []
    submit_spool_dir = shlex.quote(remote_path)
    return "\n".join(
        [
            "#!/bin/bash",
            *partition_lines,
            f"#SBATCH -N {config.nodes}",
            f"#SBATCH -n {config.ntasks}",
            f"#SBATCH -c {config.cpus_per_job}",
            f"#SBATCH --mem={config.mem}",
            f"#SBATCH --time={config.time_limit}",
            *exclude_lines,
            "#SBATCH -o slurm-%j.out",
            "#SBATCH -e slurm-%j.err",
            "set -euo pipefail",
            "export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}",
            f"SUBMIT_SPOOL_DIR={submit_spool_dir}",
            f"REMOTE_RUNTIME_ROOT={_double_quoted_shell_value(runtime_root)}",
            f"export PEETS_CONTROL_RUN_ID={shlex.quote(run_id or '')}",
            f"export PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id or '')}",
            f"export PEETS_CONTROL_HOST={shlex.quote(getattr(config, 'control_plane_host', '127.0.0.1'))}",
            f"export PEETS_CONTROL_PORT={int(getattr(config, 'control_plane_port', 8765))}",
            f"export PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
            f"export PEETS_CONTROL_RETURN_HOST={shlex.quote(control_plane_return_host)}",
            f"export PEETS_CONTROL_RETURN_USER={shlex.quote(control_plane_return_user)}",
            f"export PEETS_CONTROL_RETURN_PORT={control_plane_return_port}",
            f"export PEETS_CONTROL_HEARTBEAT_INTERVAL={max(5, int(getattr(config, 'tunnel_recovery_grace_seconds', 30)))}",
            "SUBMIT_HOST=\"${SLURM_SUBMIT_HOST:-}\"",
            "SUBMIT_USER=\"${USER:-$(id -un)}\"",
            "SSH_REMOTE=\"${SUBMIT_USER}@${SUBMIT_HOST}\"",
            "SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2)",
            "mkdir -p \"$REMOTE_RUNTIME_ROOT\"",
            "EXEC_DIR=$(mktemp -d \"$REMOTE_RUNTIME_ROOT/sbatch.${SLURM_JOB_ID:-nojob}.XXXXXX\")",
            "uploader_pid=''",
            "upload_back() {",
            "  if [ -z \"$SUBMIT_HOST\" ] || [ ! -d \"$EXEC_DIR\" ]; then",
            "    return 0",
            "  fi",
            "  shopt -s nullglob",
            "  files=()",
            "  for path in launch_probe.txt worker.stdout worker.stderr control_tunnel_bootstrap.err results.tgz.ready results.tgz case_summary.txt failed.count case_*/run.log case_*/exit.code case_*/batch.log case_*/report_export.error.log case_*/license_diagnostics.txt case_*/runtime_logs.json case_*/ansys_grpc.stdout.log case_*/ansys_grpc.stderr.log case_*/pyaedt*.log case_*/remote_job.sh case_*/run_sim.py case_*/project.aedt case_*/project.aedt.q case_*/project.aedt.q.complete case_*/project.aedt.q.completed case_*/all_reports case_*/design_outputs case_*/project.aedtresults; do",
            "    [ -e \"$path\" ] && files+=(\"$path\")",
            "  done",
            "  if [ \"${#files[@]}\" -eq 0 ]; then",
            "    return 0",
            "  fi",
            "  tar -czf - \"${files[@]}\" | ssh \"${SSH_OPTS[@]}\" \"$SSH_REMOTE\" \"bash -lc 'mkdir -p "
            + submit_spool_dir
            + " && cd "
            + submit_spool_dir
            + " && tar --no-same-owner -xzf -'\" >/dev/null 2>&1 || true",
            "}",
            "periodic_upload_loop() {",
            "  while sleep 5; do",
            "    upload_back",
            "  done",
            "}",
            "cleanup_exec() {",
            "  rc=$?",
            "  if [ -n \"$uploader_pid\" ]; then",
            "    kill \"$uploader_pid\" >/dev/null 2>&1 || true",
            "    wait \"$uploader_pid\" >/dev/null 2>&1 || true",
            "  fi",
            "  if [ -d \"$EXEC_DIR\" ]; then",
            "    cd \"$EXEC_DIR\" >/dev/null 2>&1 || true",
            "    upload_back",
            "    cd \"$REMOTE_RUNTIME_ROOT\" >/dev/null 2>&1 || true",
            "    rm -rf \"$EXEC_DIR\" >/dev/null 2>&1 || true",
            "  fi",
            "  exit \"$rc\"",
            "}",
            "trap cleanup_exec EXIT",
            "if [ -z \"$SUBMIT_HOST\" ]; then",
            "  echo \"[ERROR] SLURM_SUBMIT_HOST is empty\" >&2",
            "  exit 127",
            "fi",
            "cd \"$EXEC_DIR\"",
            "printf 'hostname=%s\\n' \"$(hostname 2>/dev/null || true)\" > launch_probe.txt",
            "printf 'pwd=%s\\n' \"$PWD\" >> launch_probe.txt",
            "printf 'path=%s\\n' \"$PATH\" >> launch_probe.txt",
            "printf 'submit_host=%s\\n' \"$SUBMIT_HOST\" >> launch_probe.txt",
            "printf 'submit_spool_dir=%s\\n' \"$SUBMIT_SPOOL_DIR\" >> launch_probe.txt",
            "stage_control_ssh_identity() {",
            "  local candidate",
            "  local staged_dir=\"$EXEC_DIR/control-plane-ssh\"",
            "  mkdir -p \"$staged_dir\"",
            "  chmod 700 \"$staged_dir\" >/dev/null 2>&1 || true",
            "  for candidate in \"$HOME/.ssh/id_ed25519\" \"$HOME/.ssh/id_rsa\"; do",
            "    if [ -r \"$candidate\" ]; then",
            "      cp \"$candidate\" \"$staged_dir/id_control\"",
            "      chmod 600 \"$staged_dir/id_control\" >/dev/null 2>&1 || true",
            "      export PEETS_CONTROL_SSH_IDENTITY=\"$staged_dir/id_control\"",
            "      printf 'control_plane_ssh_identity=%s\\n' \"$candidate\" >> launch_probe.txt",
            "      return 0",
            "    fi",
            "  done",
            "  echo \"[ERROR] no readable SSH identity found under $HOME/.ssh\" >&2",
            "  return 127",
            "}",
            "for tool in bash tar seq cp base64 ssh enroot python3 python; do",
            "  resolved_tool=\"$(command -v \"$tool\" 2>/dev/null || true)\"",
            "  printf 'tool.%s=%s\\n' \"$tool\" \"${resolved_tool:-MISSING}\" >> launch_probe.txt",
            "done",
            "stage_control_ssh_identity",
            "ssh \"${SSH_OPTS[@]}\" \"$SSH_REMOTE\" \"bash -lc 'set -euo pipefail; shopt -s nullglob; cd "
            + submit_spool_dir
            + " && tar -czf - remote_job.sh remote_worker_payload.sh project_*.aedt'\" | tar --no-same-owner -xzf -",
            "chmod 700 remote_job.sh remote_worker_payload.sh >/dev/null 2>&1 || true",
            "export REMOTE_JOB_DIR=\"$EXEC_DIR\"",
            "periodic_upload_loop &",
            "uploader_pid=$!",
            "tar -czf - remote_job.sh project_*.aedt | /bin/bash ./remote_worker_payload.sh > worker.stdout 2> worker.stderr",
        ]
    ) + "\n"


def _build_pull_remote_sbatch_script_content(
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    run_id: str,
    worker_id: str,
) -> str:
    remote_path = _remote_path_for_shell(config=config, path=remote_job_dir)
    exec_root = _remote_path_for_shell(config=config, path="/tmp/$USER/peetsfea-runner/submit")
    control_plane_ssh_target = _control_plane_ssh_target(config)
    control_plane_return_host = _control_plane_return_host(config)
    control_plane_return_user = _control_plane_return_user(config)
    control_plane_return_port = _control_plane_return_port(config)
    exclude_nodes = _slurm_exclude_nodes(config)
    exclude_lines = [f"#SBATCH --exclude={','.join(exclude_nodes)}"] if exclude_nodes else []
    partition_value = _slurm_partition_value(config)
    partition_lines = [f"#SBATCH -p {partition_value}"] if partition_value else []
    submit_spool_dir = shlex.quote(remote_path)
    return "\n".join(
        [
            "#!/bin/bash",
            *partition_lines,
            f"#SBATCH -N {config.nodes}",
            f"#SBATCH -n {config.ntasks}",
            f"#SBATCH -c {config.cpus_per_job}",
            f"#SBATCH --mem={config.mem}",
            f"#SBATCH --time={config.time_limit}",
            *exclude_lines,
            "#SBATCH -o slurm-%j.out",
            "#SBATCH -e slurm-%j.err",
            "set -euo pipefail",
            "export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}",
            f"SUBMIT_SPOOL_DIR={submit_spool_dir}",
            f"EXEC_ROOT={_double_quoted_shell_value(exec_root)}",
            f"export PEETS_CONTROL_RUN_ID={shlex.quote(run_id)}",
            f"export PEETS_CONTROL_WORKER_ID={shlex.quote(worker_id)}",
            f"export PEETS_CONTROL_HOST={shlex.quote(getattr(config, 'control_plane_host', '127.0.0.1'))}",
            f"export PEETS_CONTROL_PORT={int(getattr(config, 'control_plane_port', 8765))}",
            f"export PEETS_CONTROL_SSH_TARGET={shlex.quote(control_plane_ssh_target)}",
            f"export PEETS_CONTROL_RETURN_HOST={shlex.quote(control_plane_return_host)}",
            f"export PEETS_CONTROL_RETURN_USER={shlex.quote(control_plane_return_user)}",
            f"export PEETS_CONTROL_RETURN_PORT={control_plane_return_port}",
            f"export PEETS_CONTROL_HEARTBEAT_INTERVAL={max(5, int(getattr(config, 'tunnel_recovery_grace_seconds', 30)))}",
            "export PEETS_ACCOUNT_ID=\"${PEETS_ACCOUNT_ID:-account_01}\"",
            "SUBMIT_HOST=\"${SLURM_SUBMIT_HOST:-}\"",
            "SUBMIT_USER=\"${USER:-$(id -un)}\"",
            "SSH_REMOTE=\"${SUBMIT_USER}@${SUBMIT_HOST}\"",
            "SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2)",
            "mkdir -p \"$EXEC_ROOT\"",
            "EXEC_DIR=$(mktemp -d \"$EXEC_ROOT/pull.${SLURM_JOB_ID:-nojob}.XXXXXX\")",
            "upload_debug_back() {",
            "  if [ -z \"$SUBMIT_HOST\" ] || [ ! -d \"$EXEC_DIR\" ]; then",
            "    return 0",
            "  fi",
            "  shopt -s nullglob",
            "  files=()",
            "  for path in launch_probe.txt worker.stdout worker.stderr control_tunnel_bootstrap.err slurm-%j.out slurm-%j.err; do",
            "    [ -e \"$path\" ] && files+=(\"$path\")",
            "  done",
            "  if [ \"${#files[@]}\" -eq 0 ]; then",
            "    return 0",
            "  fi",
            "  tar -czf - \"${files[@]}\" | ssh \"${SSH_OPTS[@]}\" \"$SSH_REMOTE\" \"bash -lc 'mkdir -p "
            + submit_spool_dir
            + " && cd "
            + submit_spool_dir
            + " && tar --no-same-owner -xzf -'\" >/dev/null 2>&1 || true",
            "}",
            "cleanup_exec() {",
            "  rc=$?",
            "  if [ -d \"$EXEC_DIR\" ]; then",
            "    cd \"$EXEC_DIR\" >/dev/null 2>&1 || true",
            "    upload_debug_back",
            "    cd \"$EXEC_ROOT\" >/dev/null 2>&1 || true",
            "    rm -rf \"$EXEC_DIR\" >/dev/null 2>&1 || true",
            "  fi",
            "  exit \"$rc\"",
            "}",
            "trap cleanup_exec EXIT",
            "if [ -z \"$SUBMIT_HOST\" ]; then",
            "  echo \"[ERROR] SLURM_SUBMIT_HOST is empty\" >&2",
            "  exit 127",
            "fi",
            "cd \"$EXEC_DIR\"",
            "printf 'hostname=%s\\n' \"$(hostname 2>/dev/null || true)\" > launch_probe.txt",
            "printf 'pwd=%s\\n' \"$PWD\" >> launch_probe.txt",
            "printf 'submit_host=%s\\n' \"$SUBMIT_HOST\" >> launch_probe.txt",
            "printf 'submit_spool_dir=%s\\n' \"$SUBMIT_SPOOL_DIR\" >> launch_probe.txt",
            "stage_control_ssh_identity() {",
            "  local candidate",
            "  local staged_dir=\"$EXEC_DIR/control-plane-ssh\"",
            "  mkdir -p \"$staged_dir\"",
            "  chmod 700 \"$staged_dir\" >/dev/null 2>&1 || true",
            "  for candidate in \"$HOME/.ssh/id_ed25519\" \"$HOME/.ssh/id_rsa\"; do",
            "    if [ -r \"$candidate\" ]; then",
            "      cp \"$candidate\" \"$staged_dir/id_control\"",
            "      chmod 600 \"$staged_dir/id_control\" >/dev/null 2>&1 || true",
            "      export PEETS_CONTROL_SSH_IDENTITY=\"$staged_dir/id_control\"",
            "      printf 'control_plane_ssh_identity=%s\\n' \"$candidate\" >> launch_probe.txt",
            "      return 0",
            "    fi",
            "  done",
            "  echo \"[ERROR] no readable SSH identity found under $HOME/.ssh\" >&2",
            "  return 127",
            "}",
            "stage_control_ssh_identity",
            "ssh \"${SSH_OPTS[@]}\" \"$SSH_REMOTE\" \"bash -lc 'set -euo pipefail; cd "
            + submit_spool_dir
            + " && tar -czf - remote_job.sh remote_pull_worker_payload.sh'\" | tar --no-same-owner -xzf -",
            "chmod 700 remote_job.sh remote_pull_worker_payload.sh >/dev/null 2>&1 || true",
            "export REMOTE_JOB_DIR=\"$EXEC_DIR\"",
            "export PEETS_ACCOUNT_ID=\"${PEETS_ACCOUNT_ID:-account_01}\"",
            "./remote_pull_worker_payload.sh > worker.stdout 2> worker.stderr",
        ]
    ) + "\n"


def _write_pull_remote_sbatch_script(
    tmpdir: Path,
    *,
    config: RemoteJobConfig,
    remote_job_dir: str,
    run_id: str,
    worker_id: str,
) -> Path:
    script = tmpdir / "remote_pull_sbatch.sh"
    script.write_text(
        _build_pull_remote_sbatch_script_content(
            config=config,
            remote_job_dir=remote_job_dir,
            run_id=run_id,
            worker_id=worker_id,
        ),
        encoding="utf-8",
    )
    return script


def submit_pull_worker(
    *,
    config: RemoteJobConfig,
    run_id: str,
    worker_id: str,
    remote_job_dir: str,
) -> str:
    resolved_remote_job_dir = _resolve_remote_path(config=config, path=remote_job_dir)
    _prepare_remote_workspace(config, remote_job_dir=resolved_remote_job_dir)
    with TemporaryDirectory(prefix="peetsfea_pull_worker_") as tmpdir:
        tmpdir_path = Path(tmpdir)
        remote_script = (
            _write_windows_remote_job_script(tmpdir_path)
            if _remote_platform(config) == "windows"
            else _write_remote_job_script(tmpdir_path, config=config)
        )
        pull_payload = _write_pull_worker_payload_script(
            tmpdir_path,
            config=config,
            run_id=run_id,
            worker_id=worker_id,
        )
        pull_sbatch = _write_pull_remote_sbatch_script(
            tmpdir_path,
            config=config,
            remote_job_dir=resolved_remote_job_dir,
            run_id=run_id,
            worker_id=worker_id,
        )
        _upload_supporting_files(
            config,
            project_files=(),
            remote_job_dir=resolved_remote_job_dir,
            supporting_files=[remote_script, pull_payload, pull_sbatch],
        )
    remote_path = _remote_path_for_shell(config=config, path=resolved_remote_job_dir)
    command = f"cd {shlex.quote(remote_path)} && sbatch --parsable ./remote_pull_sbatch.sh"
    completed = _run_completed_process_capture_with_transport_retry(
        _ssh_command(config, config.host, command),
        stage="sbatch submit",
        exit_code=EXIT_CODE_SLURM_FAILURE,
    )
    if completed.returncode != 0:
        details = _extract_meaningful_remote_failure_details(stdout=completed.stdout or "", stderr=completed.stderr or "")
        raise WorkflowError(
            f"sbatch submit failed: {details or completed.returncode}",
            exit_code=EXIT_CODE_SLURM_FAILURE,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )
    return _parse_sbatch_job_id(completed.stdout or "")


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
