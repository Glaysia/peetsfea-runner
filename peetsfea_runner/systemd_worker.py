from __future__ import annotations

import os
import shutil
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from .pipeline import AccountConfig, PipelineConfig, PipelineResult, run_pipeline
from .state_store import StateStore
from .version import get_version
from .web_status import start_status_server


APP_VERSION = get_version()


def _normalize_control_plane_ssh_target(target: str) -> str:
    normalized = target.strip()
    if not normalized or "@" in normalized:
        return normalized
    local_user = (os.getenv("USER") or os.getenv("LOGNAME") or "").strip()
    if not local_user:
        return normalized
    return f"{local_user}@{normalized}"


def _split_ssh_target(target: str) -> tuple[str | None, str | None]:
    normalized = target.strip()
    if not normalized:
        return None, None
    if "@" not in normalized:
        return None, normalized
    user, host = normalized.split("@", 1)
    user = user.strip() or None
    host = host.strip() or None
    return user, host


def _discover_control_plane_return_host() -> str:
    try:
        for family, socktype, proto, _canonname, sockaddr in socket.getaddrinfo(
            socket.gethostname(),
            None,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        ):
            if family != socket.AF_INET or socktype != socket.SOCK_STREAM:
                continue
            candidate = sockaddr[0].strip()
            if candidate and not candidate.startswith("127."):
                return candidate
    except OSError:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 80))
            candidate = str(probe.getsockname()[0]).strip()
            if candidate and not candidate.startswith("127."):
                return candidate
    except OSError:
        pass
    return ""


def _parse_mem_to_mb(mem: str) -> int | None:
    raw = mem.strip().upper()
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


def _read_meminfo_mb() -> tuple[int | None, int | None, int | None]:
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        return None, None, None
    values: dict[str, int] = {}
    for line in meminfo_path.read_text(encoding="utf-8").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parts = value.strip().split()
        if not parts:
            continue
        try:
            values[key.strip()] = int(parts[0])
        except ValueError:
            continue
    total_kb = values.get("MemTotal")
    available_kb = values.get("MemAvailable")
    if total_kb is None or available_kb is None:
        return None, None, None
    total_mb = total_kb // 1024
    free_mb = available_kb // 1024
    used_mb = max(total_mb - free_mb, 0)
    return total_mb, used_mb, free_mb


def _read_self_rss_mb() -> int | None:
    status_path = Path("/proc/self/status")
    if not status_path.exists():
        return None
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return int(parts[1]) // 1024
                except ValueError:
                    return None
    return None


def _load_average() -> tuple[float | None, float | None, float | None]:
    try:
        load1, load5, load15 = os.getloadavg()
    except OSError:
        return None, None, None
    return float(load1), float(load5), float(load15)


def _tmp_usage_mb() -> tuple[int | None, int | None, int | None]:
    usage = shutil.disk_usage(Path(os.getenv("TMPDIR", "/tmp")))
    total_mb = usage.total // (1024 * 1024)
    free_mb = usage.free // (1024 * 1024)
    used_mb = usage.used // (1024 * 1024)
    return total_mb, used_mb, free_mb


def _system_process_count() -> int:
    proc_root = Path("/proc")
    if not proc_root.exists():
        return 0
    count = 0
    for entry in proc_root.iterdir():
        if entry.name.isdigit():
            count += 1
    return count


def _record_local_resource_snapshots(
    *,
    store: StateStore,
    config: PipelineConfig,
    service_name: str,
    host: str,
    pid: int,
    runtime_state: _WorkerRuntimeState,
) -> None:
    run_id, _status = runtime_state.snapshot()
    if not run_id:
        return
    total_mem_mb, used_mem_mb, free_mem_mb = _read_meminfo_mb()
    load_1, load_5, load_15 = _load_average()
    tmp_total_mb, tmp_used_mb, tmp_free_mb = _tmp_usage_mb()
    active_slot_count = store.count_active_slots(run_id=run_id)
    running_worker_count = len(store.list_active_slurm_workers(run_id=run_id))
    configured_mem_mb = _parse_mem_to_mb(config.mem)
    worker_status = store.get_slurm_worker(run_id=run_id, worker_id=f"{service_name}:{host}:{pid}")
    tunnel_state = None
    if worker_status is not None:
        tunnel_state = str(worker_status.get("tunnel_state") or "")
    rss_mb = _read_self_rss_mb()
    process_count = _system_process_count()
    store.record_node_resource_snapshot(
        run_id=run_id,
        host=host,
        allocated_mem_mb=configured_mem_mb,
        total_mem_mb=total_mem_mb,
        used_mem_mb=used_mem_mb,
        free_mem_mb=free_mem_mb,
        load_1=load_1,
        load_5=load_5,
        load_15=load_15,
        tmp_total_mb=tmp_total_mb,
        tmp_used_mb=tmp_used_mb,
        tmp_free_mb=tmp_free_mb,
        process_count=process_count,
        running_worker_count=running_worker_count,
        active_slot_count=active_slot_count,
    )
    store.record_worker_resource_snapshot(
        run_id=run_id,
        worker_id=f"{service_name}:{host}:{pid}",
        host=host,
        slurm_job_id=None,
        configured_slots=config.slots_per_job,
        active_slots=active_slot_count,
        idle_slots=max(config.slots_per_job - active_slot_count, 0),
        rss_mb=rss_mb,
        cpu_pct=None,
        tunnel_state=tunnel_state,
        process_count=process_count,
    )
    store.record_resource_summary_snapshot(
        run_id=run_id,
        host=host,
        allocated_mem_mb=configured_mem_mb,
        used_mem_mb=used_mem_mb,
        free_mem_mb=free_mem_mb,
        load_1=load_1,
        running_worker_count=running_worker_count,
        active_slot_count=active_slot_count,
        stale=False,
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(slots=True)
class _WorkerRuntimeState:
    run_id: str | None = None
    status: str = "IDLE"
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def set(self, *, run_id: str | None, status: str) -> None:
        with self._lock:
            self.run_id = run_id
            self.status = status

    def snapshot(self) -> tuple[str | None, str]:
        with self._lock:
            return self.run_id, self.status


@dataclass(slots=True)
class _AutorecoveryControlState:
    last_stage: str | None = None
    last_emit_monotonic: float = 0.0


def _parse_accounts_from_env() -> tuple[AccountConfig, ...]:
    """
    Parse PEETSFEA_ACCOUNTS as comma-separated entries.
    Entry formats:
      - account_id@host_alias:max_jobs
      - account_id@host_alias:max_jobs:platform:scheduler
      - account_id@host_alias
    Example:
      account_01@gate1-harry:10,account_02@gate1-dhj02:10,account_03@gate1-jji0930:10,account_04@gate1-dw16:2:windows:none
    """
    raw = os.getenv("PEETSFEA_ACCOUNTS", "").strip()
    if not raw:
        return ()

    accounts: list[AccountConfig] = []
    for chunk in raw.split(","):
        entry = chunk.strip()
        if not entry:
            continue
        if "@" not in entry:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (missing @): {entry}")
        account_id, host_and_jobs = entry.split("@", 1)
        account_id = account_id.strip()
        if not account_id:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (empty account_id): {entry}")

        max_jobs = 10
        platform = "linux"
        scheduler = "slurm"
        host_alias = host_and_jobs.strip()
        parts = [part.strip() for part in host_and_jobs.split(":")]
        if len(parts) >= 2:
            host_alias = parts[0]
            max_jobs = int(parts[1])
        if len(parts) >= 3 and parts[2]:
            platform = parts[2].lower()
        if len(parts) >= 4 and parts[3]:
            scheduler = parts[3].lower()
        if len(parts) > 4:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (too many fields): {entry}")
        if not host_alias:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (empty host_alias): {entry}")
        if max_jobs <= 0:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (max_jobs must be > 0): {entry}")

        accounts.append(
            AccountConfig(
                account_id=account_id,
                host_alias=host_alias,
                max_jobs=max_jobs,
                platform=platform,
                scheduler=scheduler,
            )
        )

    if not accounts:
        raise ValueError("PEETSFEA_ACCOUNTS is set but no valid account entries were parsed.")
    return tuple(accounts)


def _default_repo_ssh_config_path(repo_root: Path) -> str:
    candidate = repo_root / ".ssh" / "config"
    if candidate.is_file():
        return str(candidate)
    return ""


def _build_config() -> PipelineConfig:
    repo_root = Path(__file__).resolve().parent.parent
    input_queue_dir = os.getenv("PEETSFEA_INPUT_QUEUE_DIR", str(repo_root / "input_queue"))
    output_root_dir = os.getenv("PEETSFEA_OUTPUT_ROOT_DIR", str(repo_root / "output"))
    delete_failed_dir = os.getenv("PEETSFEA_DELETE_FAILED_DIR", str(repo_root / "output" / "_delete_failed"))
    metadata_db_path = os.getenv("PEETSFEA_DB_PATH", str(repo_root / "peetsfea_runner.duckdb"))

    accounts_registry = _parse_accounts_from_env()
    if not accounts_registry:
        account_id = os.getenv("PEETSFEA_ACCOUNT_ID", "account_01")
        host_alias = os.getenv("PEETSFEA_HOST_ALIAS", "gate1-harry")
        max_jobs = int(os.getenv("PEETSFEA_MAX_JOBS_PER_ACCOUNT", "10"))
        accounts_registry = (AccountConfig(account_id=account_id, host_alias=host_alias, max_jobs=max_jobs),)

    control_plane_ssh_target = _normalize_control_plane_ssh_target(os.getenv("PEETSFEA_CONTROL_PLANE_SSH_TARGET", ""))
    target_user, target_host = _split_ssh_target(control_plane_ssh_target)
    local_user = (os.getenv("USER") or os.getenv("LOGNAME") or "").strip()
    control_plane_return_user = (
        os.getenv("PEETSFEA_CONTROL_PLANE_RETURN_USER", "").strip()
        or target_user
        or local_user
    )
    control_plane_return_host = (
        os.getenv("PEETSFEA_CONTROL_PLANE_RETURN_HOST", "").strip()
        or target_host
        or _discover_control_plane_return_host()
    )
    control_plane_return_port = int(os.getenv("PEETSFEA_CONTROL_PLANE_RETURN_PORT", "5722"))
    if control_plane_return_user and control_plane_return_host:
        control_plane_ssh_target = f"{control_plane_return_user}@{control_plane_return_host}"

    return PipelineConfig(
        input_queue_dir=input_queue_dir,
        output_root_dir=output_root_dir,
        delete_input_after_upload=_env_bool("PEETSFEA_DELETE_INPUT_AFTER_UPLOAD", True),
        delete_failed_quarantine_dir=delete_failed_dir,
        metadata_db_path=metadata_db_path,
        accounts_registry=accounts_registry,
        execute_remote=_env_bool("PEETSFEA_EXECUTE_REMOTE", True),
        remote_execution_backend=os.getenv("PEETSFEA_REMOTE_EXECUTION_BACKEND", "foreground_ssh"),
        partition=os.getenv("PEETSFEA_PARTITION", "cpu2"),
        cpus_per_job=int(os.getenv("PEETSFEA_CPUS_PER_JOB", "16")),
        mem=os.getenv("PEETSFEA_MEM", "960G"),
        time_limit=os.getenv("PEETSFEA_TIME_LIMIT", "05:00:00"),
        remote_root=os.getenv("PEETSFEA_REMOTE_ROOT", "~/aedt_runs"),
        control_plane_host=os.getenv("PEETSFEA_CONTROL_PLANE_HOST", "127.0.0.1"),
        control_plane_port=int(os.getenv("PEETSFEA_CONTROL_PLANE_PORT", os.getenv("PEETSFEA_WEB_PORT", "8765"))),
        control_plane_ssh_target=control_plane_ssh_target,
        control_plane_return_host=control_plane_return_host,
        control_plane_return_port=control_plane_return_port,
        control_plane_return_user=control_plane_return_user,
        tunnel_heartbeat_timeout_seconds=int(os.getenv("PEETSFEA_TUNNEL_HEARTBEAT_TIMEOUT_SECONDS", "90")),
        tunnel_recovery_grace_seconds=int(os.getenv("PEETSFEA_TUNNEL_RECOVERY_GRACE_SECONDS", "30")),
        remote_ssh_port=int(os.getenv("PEETSFEA_REMOTE_SSH_PORT", "22")),
        ssh_config_path=os.getenv("PEETSFEA_SSH_CONFIG_PATH", _default_repo_ssh_config_path(repo_root)),
        remote_container_runtime=os.getenv("PEETSFEA_REMOTE_CONTAINER_RUNTIME", "none"),
        remote_container_image=os.getenv("PEETSFEA_REMOTE_CONTAINER_IMAGE", ""),
        remote_container_ansys_root=os.getenv(
            "PEETSFEA_REMOTE_CONTAINER_ANSYS_ROOT",
            "/opt/ohpc/pub/Electronics/v252",
        ),
        remote_ansys_executable=os.getenv("PEETSFEA_REMOTE_ANSYS_EXECUTABLE", ""),
        continuous_mode=_env_bool("PEETSFEA_CONTINUOUS_MODE", True),
        ingest_poll_seconds=int(os.getenv("PEETSFEA_INGEST_POLL_SECONDS", "30")),
        ready_sidecar_suffix=os.getenv("PEETSFEA_READY_SIDECAR_SUFFIX", ".ready"),
        slots_per_job=int(os.getenv("PEETSFEA_SLOTS_PER_JOB", "4")),
        worker_bundle_multiplier=int(os.getenv("PEETSFEA_WORKER_BUNDLE_MULTIPLIER", "1")),
        cores_per_slot=int(os.getenv("PEETSFEA_CORES_PER_SLOT", "4")),
        tasks_per_slot=int(os.getenv("PEETSFEA_TASKS_PER_SLOT", "1")),
        worker_requeue_limit=int(os.getenv("PEETSFEA_WORKER_REQUEUE_LIMIT", "1")),
        retain_aedtresults=_env_bool("PEETSFEA_RETAIN_AEDTRESULTS", True),
        run_rotation_hours=int(os.getenv("PEETSFEA_RUN_ROTATION_HOURS", "24")),
        run_namespace=os.getenv("PEETSFEA_RUN_NAMESPACE", "").strip(),
        pending_buffer_per_account=int(os.getenv("PEETSFEA_PENDING_BUFFER_PER_ACCOUNT", "3")),
        capacity_scope=os.getenv("PEETSFEA_CAPACITY_SCOPE", "all_user_jobs"),
        balance_metric=os.getenv("PEETSFEA_BALANCE_METRIC", "slot_throughput"),
        input_source_policy=os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only"),
        public_storage_mode=os.getenv("PEETSFEA_PUBLIC_STORAGE_MODE", "disabled"),
        remote_storage_inode_block_percent=int(os.getenv("PEETSFEA_REMOTE_STORAGE_INODE_BLOCK_PERCENT", "98")),
        remote_storage_min_free_mb=int(os.getenv("PEETSFEA_REMOTE_STORAGE_MIN_FREE_MB", "0")),
    )


def _start_embedded_web_if_enabled() -> None:
    backend = os.getenv("PEETSFEA_REMOTE_EXECUTION_BACKEND", "foreground_ssh").strip().lower()
    embed_web = _env_bool("PEETSFEA_EMBED_WEB", True)
    if not embed_web and backend != "slurm_batch":
        return
    repo_root = Path(__file__).resolve().parent.parent
    db_path = os.getenv("PEETSFEA_DB_PATH", str(repo_root / "peetsfea_runner.duckdb"))
    host = os.getenv("PEETSFEA_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("PEETSFEA_WEB_PORT", "8765"))
    if port <= 0 or port > 65535:
        raise ValueError("PEETSFEA_WEB_PORT must be in 1..65535")

    server = start_status_server(db_path=db_path, host=host, port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="peetsfea-web")
    thread.start()
    print(f"[peetsfea][web] version={APP_VERSION} embedded status server listening on http://{host}:{port}", flush=True)


def _heartbeat_once(
    *,
    store: StateStore,
    config: PipelineConfig,
    service_name: str,
    host: str,
    pid: int,
    runtime_state: _WorkerRuntimeState,
) -> None:
    run_id, status = runtime_state.snapshot()
    store.upsert_worker_heartbeat(
        service_name=service_name,
        host=host,
        pid=pid,
        run_id=run_id,
        status=status,
    )
    _record_local_resource_snapshots(
        store=store,
        config=config,
        service_name=service_name,
        host=host,
        pid=pid,
        runtime_state=runtime_state,
    )


def _heartbeat_loop(
    *,
    store: StateStore,
    config: PipelineConfig,
    service_name: str,
    host: str,
    pid: int,
    runtime_state: _WorkerRuntimeState,
    stop_event: threading.Event,
    interval_seconds: int,
) -> None:
    _heartbeat_once(
        store=store,
        config=config,
        service_name=service_name,
        host=host,
        pid=pid,
        runtime_state=runtime_state,
    )
    while not stop_event.wait(interval_seconds):
        _heartbeat_once(
            store=store,
            config=config,
            service_name=service_name,
            host=host,
            pid=pid,
            runtime_state=runtime_state,
        )


def _run_worker_iteration(
    *,
    config: PipelineConfig,
    store: StateStore,
    runtime_state: _WorkerRuntimeState,
    control_state: _AutorecoveryControlState | None = None,
    autorecovery_min_interval_seconds: int = 60,
) -> PipelineResult:
    current_run_id, _status = runtime_state.snapshot()
    if config.continuous_mode:
        current_run_id = store.ensure_continuous_run(
            rotation_hours=config.run_rotation_hours,
            namespace=config.run_namespace,
        )
    runtime_state.set(run_id=current_run_id, status="ACTIVE")
    store.append_event(
        run_id=current_run_id or "__worker__",
        job_id="__worker__",
        level="INFO",
        stage="WORKER_LOOP_ACTIVE",
        message=f"run_pipeline start version={APP_VERSION}",
    )
    result = run_pipeline(config)
    _apply_post_iteration_status(
        store=store,
        runtime_state=runtime_state,
        result=result,
        control_state=control_state,
        autorecovery_min_interval_seconds=autorecovery_min_interval_seconds,
    )
    return result


def _apply_post_iteration_status(
    *,
    store: StateStore,
    runtime_state: _WorkerRuntimeState,
    result: PipelineResult,
    control_state: _AutorecoveryControlState | None,
    autorecovery_min_interval_seconds: int,
) -> None:
    if result.blocked:
        status = "BLOCKED"
        stage = "WORKER_LOOP_BLOCKED"
        level = "WARN"
        message = (
            f"blocked_accounts={list(result.blocked_accounts)} "
            f"readiness_blocked_slots={result.readiness_blocked_slots}"
        )
    elif result.recovery_needed:
        status = "RECOVERING"
        stage = "WORKER_LOOP_RECOVERING"
        level = "WARN"
        message = (
            f"terminal_jobs={result.terminal_jobs} replacement_jobs={result.replacement_jobs} "
            f"failed_jobs={result.failed_jobs} quarantined_jobs={result.quarantined_jobs} "
            f"failed_slots={result.failed_slots} quarantined_slots={result.quarantined_slots}"
        )
    elif result.total_slots == 0:
        status = "IDLE"
        stage = "WORKER_LOOP_IDLE"
        level = "INFO"
        message = result.summary
    else:
        status = "IDLE"
        stage = "WORKER_LOOP_OK"
        level = "INFO"
        message = result.summary

    runtime_state.set(run_id=result.run_id, status=status)

    if _should_emit_control_event(
        control_state=control_state,
        stage=stage,
        autorecovery_min_interval_seconds=autorecovery_min_interval_seconds,
    ):
        store.append_event(
            run_id=result.run_id,
            job_id="__worker__",
            level=level,
            stage=stage,
            message=message,
        )


def _should_emit_control_event(
    *,
    control_state: _AutorecoveryControlState | None,
    stage: str,
    autorecovery_min_interval_seconds: int,
) -> bool:
    if control_state is None:
        return True
    if stage not in {"WORKER_LOOP_RECOVERING", "WORKER_LOOP_BLOCKED"}:
        control_state.last_stage = stage
        control_state.last_emit_monotonic = time.monotonic()
        return True

    now = time.monotonic()
    if control_state.last_stage != stage or (
        now - control_state.last_emit_monotonic
    ) >= max(1, autorecovery_min_interval_seconds):
        control_state.last_stage = stage
        control_state.last_emit_monotonic = now
        return True
    return False


def _next_poll_seconds_for_result(
    *,
    result: PipelineResult,
    base_poll_seconds: int,
    recovery_poll_seconds: int,
    blocked_poll_seconds: int,
) -> int:
    if result.blocked:
        return blocked_poll_seconds
    if result.recovery_needed:
        return recovery_poll_seconds
    return base_poll_seconds


def run_user_worker_once() -> None:
    config = _build_config()
    result = run_pipeline(config)
    print(result.summary, flush=True)


def run_user_worker_loop() -> None:
    poll_seconds = int(os.getenv("PEETSFEA_POLL_SECONDS", "30"))
    if poll_seconds <= 0:
        raise ValueError("PEETSFEA_POLL_SECONDS must be > 0")
    heartbeat_seconds = int(os.getenv("PEETSFEA_HEARTBEAT_SECONDS", "15"))
    if heartbeat_seconds <= 0:
        raise ValueError("PEETSFEA_HEARTBEAT_SECONDS must be > 0")
    recovery_poll_seconds = int(os.getenv("PEETSFEA_RECOVERY_POLL_SECONDS", "5"))
    if recovery_poll_seconds <= 0:
        raise ValueError("PEETSFEA_RECOVERY_POLL_SECONDS must be > 0")
    blocked_poll_seconds = int(os.getenv("PEETSFEA_BLOCKED_POLL_SECONDS", str(poll_seconds)))
    if blocked_poll_seconds <= 0:
        raise ValueError("PEETSFEA_BLOCKED_POLL_SECONDS must be > 0")
    autorecovery_min_interval_seconds = int(os.getenv("PEETSFEA_AUTORECOVERY_MIN_INTERVAL_SECONDS", "60"))
    if autorecovery_min_interval_seconds <= 0:
        raise ValueError("PEETSFEA_AUTORECOVERY_MIN_INTERVAL_SECONDS must be > 0")

    config = _build_config()
    _start_embedded_web_if_enabled()
    store = StateStore(Path(config.metadata_db_path))
    store.initialize()
    service_name = os.getenv("PEETSFEA_WORKER_SERVICE_NAME", "peetsfea-runner")
    host = socket.gethostname()
    pid = os.getpid()
    runtime_state = _WorkerRuntimeState()
    control_state = _AutorecoveryControlState()
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        kwargs={
            "store": store,
            "config": config,
            "service_name": service_name,
            "host": host,
            "pid": pid,
            "runtime_state": runtime_state,
            "stop_event": stop_event,
            "interval_seconds": heartbeat_seconds,
        },
        daemon=True,
        name="peetsfea-heartbeat",
    )
    heartbeat_thread.start()
    store.append_event(
        run_id="__worker__",
        job_id="__worker__",
        level="INFO",
        stage="WORKER_LOOP_START",
        message=(
            f"version={APP_VERSION} "
            f"poll_seconds={poll_seconds} heartbeat_seconds={heartbeat_seconds} "
            f"recovery_poll_seconds={recovery_poll_seconds} blocked_poll_seconds={blocked_poll_seconds} "
            f"autorecovery_min_interval_seconds={autorecovery_min_interval_seconds}"
        ),
    )

    try:
        while True:
            current_run_id, _status = runtime_state.snapshot()
            sleep_seconds = poll_seconds
            try:
                result = _run_worker_iteration(
                    config=config,
                    store=store,
                    runtime_state=runtime_state,
                    control_state=control_state,
                    autorecovery_min_interval_seconds=autorecovery_min_interval_seconds,
                )
                print(result.summary, flush=True)
                sleep_seconds = _next_poll_seconds_for_result(
                    result=result,
                    base_poll_seconds=poll_seconds,
                    recovery_poll_seconds=recovery_poll_seconds,
                    blocked_poll_seconds=blocked_poll_seconds,
                )
            except Exception as exc:  # pragma: no cover - runtime resilience path
                print(f"[peetsfea][worker] loop error: {exc}", flush=True)
                runtime_state.set(run_id=current_run_id, status="DEGRADED")
                store.append_event(
                    run_id=current_run_id or "__worker__",
                    job_id="__worker__",
                    level="ERROR",
                    stage="WORKER_LOOP_ERROR",
                    message=str(exc),
                )
            time.sleep(sleep_seconds)
    finally:  # pragma: no cover - service shutdown path
        stop_event.set()
        heartbeat_thread.join(timeout=2)
