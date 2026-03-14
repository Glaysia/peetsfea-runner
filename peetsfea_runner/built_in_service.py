from __future__ import annotations

import getpass
import os
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from .pipeline import AccountConfig, PipelineConfig
from .state_store import StateStore
from .systemd_worker import (
    _AutorecoveryControlState,
    _WorkerRuntimeState,
    _heartbeat_loop,
    _next_poll_seconds_for_result,
    _run_worker_iteration,
)
from .version import get_version
from .web_status import start_status_server


APP_VERSION = get_version()
EXPECTED_LANE_NAMES = ("preserve_results", "prune_results")


@dataclass(frozen=True, slots=True)
class LaneSpec:
    lane_id: str
    input_root: Path
    output_root: Path
    accounts: tuple[AccountConfig, ...]
    cpus_per_job: int
    slots_per_job: int
    cores_per_slot: int
    tasks_per_slot: int
    retain_aedtresults: bool
    delete_input_after_upload: bool


@dataclass(frozen=True, slots=True)
class ServiceProfile:
    repo_root: Path
    input_queue_root: Path
    output_root: Path
    delete_failed_root: Path
    db_path: Path
    control_plane_host: str
    control_plane_port: int
    control_plane_ssh_target: str
    control_plane_return_host: str
    control_plane_return_port: int
    control_plane_return_user: str
    web_host: str
    web_port: int
    poll_seconds: int
    heartbeat_seconds: int
    recovery_poll_seconds: int
    blocked_poll_seconds: int
    autorecovery_min_interval_seconds: int
    ingest_poll_seconds: int
    lanes: tuple[LaneSpec, ...]


def build_service_profile(*, repo_root: Path | None = None) -> ServiceProfile:
    resolved_repo_root = (repo_root or Path(__file__).resolve().parent.parent).expanduser().resolve()
    service_user = getpass.getuser().strip() or "peets"
    input_queue_root = resolved_repo_root / "input_queue"
    output_root = resolved_repo_root / "output"
    delete_failed_root = output_root / "_delete_failed"
    db_path = resolved_repo_root / "peetsfea_runner.duckdb"
    control_plane_return_host = "172.16.165.84"
    control_plane_return_port = 5722
    control_plane_return_user = service_user
    control_plane_ssh_target = f"{control_plane_return_user}@{control_plane_return_host}"

    preserve_results_lane = LaneSpec(
        lane_id="preserve_results",
        input_root=input_queue_root / "preserve_results",
        output_root=output_root / "preserve_results",
        accounts=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),),
        cpus_per_job=32,
        slots_per_job=1,
        cores_per_slot=32,
        tasks_per_slot=4,
        retain_aedtresults=True,
        delete_input_after_upload=False,
    )
    prune_results_lane = LaneSpec(
        lane_id="prune_results",
        input_root=input_queue_root / "prune_results",
        output_root=output_root / "prune_results",
        accounts=(
            AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
            AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
            AccountConfig(account_id="account_04", host_alias="gate1-dw16", max_jobs=10),
        ),
        cpus_per_job=20,
        slots_per_job=5,
        cores_per_slot=4,
        tasks_per_slot=1,
        retain_aedtresults=False,
        delete_input_after_upload=False,
    )
    return ServiceProfile(
        repo_root=resolved_repo_root,
        input_queue_root=input_queue_root,
        output_root=output_root,
        delete_failed_root=delete_failed_root,
        db_path=db_path,
        control_plane_host="127.0.0.1",
        control_plane_port=8765,
        control_plane_ssh_target=control_plane_ssh_target,
        control_plane_return_host=control_plane_return_host,
        control_plane_return_port=control_plane_return_port,
        control_plane_return_user=control_plane_return_user,
        web_host="127.0.0.1",
        web_port=8765,
        poll_seconds=30,
        heartbeat_seconds=15,
        recovery_poll_seconds=5,
        blocked_poll_seconds=30,
        autorecovery_min_interval_seconds=60,
        ingest_poll_seconds=120,
        lanes=(preserve_results_lane, prune_results_lane),
    )


def validate_service_layout(*, profile: ServiceProfile) -> None:
    if not profile.input_queue_root.exists():
        raise FileNotFoundError(f"input_queue root not found: {profile.input_queue_root}")
    if not profile.input_queue_root.is_dir():
        raise ValueError(f"input_queue root must be a directory: {profile.input_queue_root}")

    expected_names = set(EXPECTED_LANE_NAMES)
    for lane_name in EXPECTED_LANE_NAMES:
        lane_input = profile.input_queue_root / lane_name
        if not lane_input.exists():
            raise FileNotFoundError(f"missing required lane directory: {lane_input}")
        if not lane_input.is_dir():
            raise ValueError(f"lane path must be a directory: {lane_input}")

    for entry in sorted(profile.input_queue_root.iterdir()):
        if entry.name.startswith("."):
            continue
        if entry.is_file() and entry.suffix.lower() == ".aedt":
            raise ValueError(f"loose .aedt file not allowed at input_queue root: {entry}")
        if entry.name not in expected_names:
            raise ValueError(f"unexpected top-level input_queue entry: {entry.name}")

    profile.output_root.mkdir(parents=True, exist_ok=True)
    profile.delete_failed_root.mkdir(parents=True, exist_ok=True)
    for lane in profile.lanes:
        lane.output_root.mkdir(parents=True, exist_ok=True)


def _lane_pipeline_config(profile: ServiceProfile, lane: LaneSpec) -> PipelineConfig:
    return PipelineConfig(
        input_queue_dir=str(lane.input_root),
        output_root_dir=str(lane.output_root),
        delete_input_after_upload=lane.delete_input_after_upload,
        delete_failed_quarantine_dir=str(profile.delete_failed_root),
        metadata_db_path=str(profile.db_path),
        accounts_registry=lane.accounts,
        partition="cpu2",
        nodes=1,
        ntasks=1,
        cpus_per_job=lane.cpus_per_job,
        mem="960G",
        time_limit="05:00:00",
        remote_root="~/aedt_runs",
        execute_remote=True,
        remote_execution_backend="slurm_batch",
        control_plane_host=profile.control_plane_host,
        control_plane_port=profile.control_plane_port,
        control_plane_ssh_target=profile.control_plane_ssh_target,
        control_plane_return_host=profile.control_plane_return_host,
        control_plane_return_port=profile.control_plane_return_port,
        control_plane_return_user=profile.control_plane_return_user,
        tunnel_heartbeat_timeout_seconds=90,
        tunnel_recovery_grace_seconds=30,
        remote_ssh_port=22,
        slots_per_job=lane.slots_per_job,
        worker_bundle_multiplier=1,
        cores_per_slot=lane.cores_per_slot,
        tasks_per_slot=lane.tasks_per_slot,
        worker_requeue_limit=1,
        retain_aedtresults=lane.retain_aedtresults,
        continuous_mode=True,
        ingest_poll_seconds=profile.ingest_poll_seconds,
        ready_sidecar_suffix=".ready",
        run_rotation_hours=24,
        run_namespace=lane.lane_id,
        pending_buffer_per_account=3,
        capacity_scope="all_user_jobs",
        balance_metric="slot_throughput",
        input_source_policy="allow_original",
        public_storage_mode="disabled",
        host=lane.accounts[0].host_alias,
    )


def _lane_worker_loop(*, profile: ServiceProfile, lane: LaneSpec, stop_event: threading.Event) -> None:
    validate_service_layout(profile=profile)
    config = _lane_pipeline_config(profile, lane)
    store = StateStore(profile.db_path)
    store.initialize()
    runtime_state = _WorkerRuntimeState()
    control_state = _AutorecoveryControlState()
    service_name = f"peetsfea-runner-{lane.lane_id}"
    host = socket.gethostname()
    pid = os.getpid()
    heartbeat_stop = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        kwargs={
            "store": store,
            "config": config,
            "service_name": service_name,
            "host": host,
            "pid": pid,
            "runtime_state": runtime_state,
            "stop_event": heartbeat_stop,
            "interval_seconds": profile.heartbeat_seconds,
        },
        daemon=True,
        name=f"{service_name}-heartbeat",
    )
    heartbeat_thread.start()
    store.append_event(
        run_id=f"__worker__:{lane.lane_id}",
        job_id="__worker__",
        level="INFO",
        stage="WORKER_LOOP_START",
        message=(
            f"version={APP_VERSION} lane={lane.lane_id} poll_seconds={profile.poll_seconds} "
            f"heartbeat_seconds={profile.heartbeat_seconds} recovery_poll_seconds={profile.recovery_poll_seconds} "
            f"blocked_poll_seconds={profile.blocked_poll_seconds} "
            f"autorecovery_min_interval_seconds={profile.autorecovery_min_interval_seconds}"
        ),
    )
    try:
        while not stop_event.is_set():
            sleep_seconds = profile.poll_seconds
            try:
                validate_service_layout(profile=profile)
                result = _run_worker_iteration(
                    config=config,
                    store=store,
                    runtime_state=runtime_state,
                    control_state=control_state,
                    autorecovery_min_interval_seconds=profile.autorecovery_min_interval_seconds,
                )
                print(f"[peetsfea][{lane.lane_id}] {result.summary}", flush=True)
                sleep_seconds = _next_poll_seconds_for_result(
                    result=result,
                    base_poll_seconds=profile.poll_seconds,
                    recovery_poll_seconds=profile.recovery_poll_seconds,
                    blocked_poll_seconds=profile.blocked_poll_seconds,
                )
            except Exception as exc:  # pragma: no cover - runtime resilience path
                current_run_id, _status = runtime_state.snapshot()
                runtime_state.set(run_id=current_run_id, status="DEGRADED")
                store.append_event(
                    run_id=current_run_id or f"__worker__:{lane.lane_id}",
                    job_id="__worker__",
                    level="ERROR",
                    stage="WORKER_LOOP_ERROR",
                    message=f"lane={lane.lane_id} reason={exc}",
                )
                print(f"[peetsfea][{lane.lane_id}] loop error: {exc}", flush=True)
            stop_event.wait(sleep_seconds)
    finally:  # pragma: no cover - service shutdown path
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=2)


def run_built_in_service() -> None:
    profile = build_service_profile()
    validate_service_layout(profile=profile)

    server = start_status_server(
        db_path=str(profile.db_path),
        host=profile.web_host,
        port=profile.web_port,
    )
    threading.Thread(target=server.serve_forever, daemon=True, name="peetsfea-web").start()
    print(
        f"[peetsfea][web] version={APP_VERSION} embedded status server listening on "
        f"http://{profile.web_host}:{profile.web_port}",
        flush=True,
    )

    stop_event = threading.Event()
    worker_threads = [
        threading.Thread(
            target=_lane_worker_loop,
            kwargs={"profile": profile, "lane": lane, "stop_event": stop_event},
            daemon=False,
            name=f"peetsfea-{lane.lane_id}",
        )
        for lane in profile.lanes
    ]
    for thread in worker_threads:
        thread.start()
    try:
        for thread in worker_threads:
            thread.join()
    finally:  # pragma: no cover - service shutdown path
        stop_event.set()
        server.shutdown()
        server.server_close()
