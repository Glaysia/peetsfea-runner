from __future__ import annotations

from collections import deque
import os
import shutil
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterator

from .constants import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    EXIT_CODE_REMOTE_RUN_FAILURE,
    EXIT_CODE_SCREEN_FAILURE,
    EXIT_CODE_SLURM_FAILURE,
    EXIT_CODE_SSH_FAILURE,
    EXIT_CODE_SUCCESS,
)
from .remote_job import (
    CaseExecutionSummary as _CaseExecutionSummary,
    RemoteJobAttemptResult,
    SlotInput,
    WorkflowError as _WorkflowError,
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
    cleanup_orphan_session,
    cleanup_orphan_sessions_for_run,
    run_remote_job_attempt,
)
from .scheduler import (
    AccountCapacitySnapshot,
    AccountReadinessSnapshot,
    BundleSpec,
    SlotWorkerController,
    SlotTaskRef,
    bootstrap_account_runtime,
    query_account_preflight,
    query_account_readiness,
    query_account_capacity,
    run_slot_workers,
)
from .state_store import StateStore
from .version import get_version


APP_VERSION = get_version()


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


@dataclass(slots=True)
class AccountConfig:
    account_id: str
    host_alias: str
    max_jobs: int = 10
    enabled: bool = True
    platform: str = "linux"
    scheduler: str = "slurm"


@dataclass(slots=True, frozen=True)
class _ReadyArtifactState:
    ready_path: Path
    ready_present: bool
    ready_mode: str
    ready_error: str | None = None
    locked: bool = False


@dataclass(slots=True)
class PipelineConfig:
    input_queue_dir: str
    output_root_dir: str = "./output"
    delete_input_after_upload: bool = True
    delete_failed_quarantine_dir: str = "./output/_delete_failed"
    metadata_db_path: str = "./peetsfea_runner.duckdb"
    accounts_registry: tuple[AccountConfig, ...] = field(
        default_factory=lambda: (AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),)
    )
    # Execution/runtime settings
    partition: str = "cpu2"
    nodes: int = 1
    ntasks: int = 1
    cpus_per_job: int = 16
    mem: str = "960G"
    time_limit: str = "05:00:00"
    remote_root: str = "~/aedt_runs"
    execute_remote: bool = False
    slots_per_job: int = 4
    cores_per_slot: int = 4
    job_retry_count: int = 1
    worker_requeue_limit: int = 1
    scan_recursive: bool = True
    # License policy
    license_observe_only: bool = True
    # Backward-compatible alias for old callers/docs
    max_jobs_per_account: int = 10
    local_artifacts_dir: str = "./output"
    host: str = "gate1-harry"
    # 11-01 continuous ingest settings
    continuous_mode: bool = True
    ingest_poll_seconds: int = 30
    ready_sidecar_suffix: str = ".ready"
    run_rotation_hours: int = 24
    pending_buffer_per_account: int = 3
    capacity_scope: str = "all_user_jobs"
    balance_metric: str = "slot_throughput"

    def validate(self) -> tuple[Path, Path, list[Path], list[AccountConfig]]:
        input_root = Path(self.input_queue_dir).expanduser().resolve()
        if not input_root.exists():
            raise FileNotFoundError(f"input_queue_dir not found: {input_root}")
        if not input_root.is_dir():
            raise ValueError(f"input_queue_dir must be a directory: {input_root}")

        output_root = Path(self.output_root_dir).expanduser().resolve()
        delete_quarantine = Path(self.delete_failed_quarantine_dir).expanduser().resolve()
        output_root.mkdir(parents=True, exist_ok=True)
        delete_quarantine.mkdir(parents=True, exist_ok=True)

        _ensure_positive("nodes", self.nodes)
        _ensure_positive("ntasks", self.ntasks)
        _ensure_positive("cpus_per_job", self.cpus_per_job)
        _ensure_positive("slots_per_job", self.slots_per_job)
        _ensure_positive("cores_per_slot", self.cores_per_slot)
        _ensure_positive("ingest_poll_seconds", self.ingest_poll_seconds)
        _ensure_positive("run_rotation_hours", self.run_rotation_hours)
        if self.job_retry_count < 0:
            raise ValueError("job_retry_count must be >= 0")
        if self.worker_requeue_limit < 0:
            raise ValueError("worker_requeue_limit must be >= 0")
        if self.pending_buffer_per_account < 0:
            raise ValueError("pending_buffer_per_account must be >= 0")
        if self.capacity_scope != "all_user_jobs":
            raise ValueError("capacity_scope must be 'all_user_jobs'")
        if self.balance_metric != "slot_throughput":
            raise ValueError("balance_metric must be 'slot_throughput'")
        if not self.ready_sidecar_suffix.strip():
            raise ValueError("ready_sidecar_suffix must not be empty")
        for name in ("partition", "mem", "time_limit", "remote_root", "metadata_db_path"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.cpus_per_job < (self.slots_per_job * self.cores_per_slot):
            raise ValueError(
                f"cpus_per_job must be >= slots_per_job * cores_per_slot ({self.slots_per_job * self.cores_per_slot})"
            )

        accounts = [account for account in self.accounts_registry if account.enabled]
        if not accounts:
            raise ValueError("At least one enabled account is required.")
        for account in accounts:
            if not account.account_id.strip():
                raise ValueError("account_id must not be empty")
            if not account.host_alias.strip():
                raise ValueError("host_alias must not be empty")
            if account.max_jobs <= 0:
                raise ValueError("account.max_jobs must be > 0")
            platform = account.platform.strip().lower()
            scheduler = account.scheduler.strip().lower()
            if (platform, scheduler) not in {("linux", "slurm"), ("windows", "none")}:
                raise ValueError("account platform/scheduler must be linux/slurm or windows/none")

        files: list[Path] = []
        if not self.continuous_mode:
            files = _scan_input_aedt_files(input_root=input_root, recursive=self.scan_recursive)
        for path in files:
            _ensure_ready_artifact(path, self.ready_sidecar_suffix)
        if not files and not self.continuous_mode:
            raise ValueError(f"No .aedt files found in input_queue_dir: {input_root}")
        return input_root, output_root, files, accounts


@dataclass(slots=True)
class PipelineResult:
    success: bool
    exit_code: int
    run_id: str
    remote_run_dir: str
    local_artifacts_dir: str
    summary: str
    total_jobs: int
    success_jobs: int
    failed_jobs: int
    quarantined_jobs: int
    total_slots: int = 0
    active_slots: int = 0
    success_slots: int = 0
    failed_slots: int = 0
    quarantined_slots: int = 0
    queued_slots: int = 0
    terminal_jobs: int = 0
    replacement_jobs: int = 0
    ready_accounts: tuple[str, ...] = ()
    blocked_accounts: tuple[str, ...] = ()
    bootstrapping_accounts: tuple[str, ...] = ()
    readiness_blocked_slots: int = 0
    version: str = APP_VERSION

    @property
    def blocked(self) -> bool:
        return self.readiness_blocked_slots > 0 or bool(self.blocked_accounts)

    @property
    def recovery_needed(self) -> bool:
        if self.blocked or self.total_slots == 0:
            return False
        return (
            self.failed_jobs > 0
            or self.quarantined_jobs > 0
            or self.failed_slots > 0
            or self.quarantined_slots > 0
            or self.terminal_jobs > self.replacement_jobs
        )


@dataclass(slots=True)
class _BundleRuntimeOutcome:
    job_id: str
    success: bool
    quarantined: bool
    exit_code: int
    message: str
    success_slots: int
    failed_slots: int
    quarantined_slots: int
    requeue_slots: tuple[SlotTaskRef, ...] = ()

    @property
    def terminal_worker(self) -> bool:
        return bool(self.requeue_slots) or not self.success


@dataclass(slots=True)
class _RemoteExecutionConfig:
    host: str
    partition: str
    nodes: int
    ntasks: int
    cpus_per_job: int
    mem: str
    time_limit: str
    slots_per_job: int
    cores_per_slot: int
    platform: str = "linux"
    scheduler: str = "slurm"


def _scan_input_aedt_files(*, input_root: Path, recursive: bool) -> list[Path]:
    return list(_iter_input_aedt_files(input_root=input_root, recursive=recursive))


def _iter_input_aedt_files(*, input_root: Path, recursive: bool) -> Iterator[Path]:
    if not recursive:
        yield from sorted(
            [
                path
                for path in input_root.iterdir()
                if path.is_file() and path.suffix.lower() == ".aedt"
            ],
            key=lambda p: str(p.relative_to(input_root)).lower(),
        )
        return

    visited_dirs: set[tuple[int, int]] = set()
    for root, dirs, filenames in os.walk(input_root, followlinks=True):
        try:
            root_stat = os.stat(root)
        except OSError:
            dirs[:] = []
            continue
        root_key = (root_stat.st_dev, root_stat.st_ino)
        if root_key in visited_dirs:
            dirs[:] = []
            continue
        visited_dirs.add(root_key)
        dirs.sort()
        for filename in sorted(filenames):
            path = Path(root) / filename
            if path.suffix.lower() != ".aedt":
                continue
            if not path.is_file():
                continue
            yield path


def _configured_target_slots(*, config: PipelineConfig, accounts: list[AccountConfig]) -> int:
    return sum(max(1, account.max_jobs) for account in accounts) * config.slots_per_job


def _continuous_backlog_limits(*, config: PipelineConfig, accounts: list[AccountConfig]) -> tuple[int, int]:
    low_watermark = max(config.slots_per_job, _configured_target_slots(config=config, accounts=accounts))
    high_watermark = max(low_watermark, low_watermark * 2)
    return low_watermark, high_watermark


def _blocked_readiness_snapshot(*, account: AccountConfig, reason: str) -> AccountReadinessSnapshot:
    return AccountReadinessSnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        ready=False,
        status="DISABLED_FOR_DISPATCH",
        reason=reason,
        home_ok=False,
        runtime_path_ok=False,
        venv_ok=False,
        python_ok=False,
        module_ok=False,
        binaries_ok=False,
        ansys_ok=False,
    )


def _update_readiness_snapshot(
    snapshot: AccountReadinessSnapshot,
    *,
    status: str,
    reason: str,
    ready: bool | None = None,
) -> AccountReadinessSnapshot:
    return replace(
        snapshot,
        status=status,
        reason=reason,
        ready=snapshot.ready if ready is None else ready,
    )


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    if not isinstance(config, PipelineConfig):
        raise TypeError("config must be a PipelineConfig")

    input_root, output_root, aedt_files, accounts = config.validate()
    state_store = StateStore(Path(config.metadata_db_path))
    state_store.initialize()
    if config.continuous_mode:
        run_id = state_store.ensure_continuous_run(rotation_hours=config.run_rotation_hours)
    else:
        run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        state_store.start_run(run_id)
    remote_run_dir = _join_remote_root(config.remote_root, run_id)
    total_inputs_label = "streaming" if config.continuous_mode else str(len(aedt_files))
    _log_stage(
        f"pipeline start version={APP_VERSION} run_id={run_id} "
        f"total_inputs={total_inputs_label} execute_remote={config.execute_remote}"
    )

    queued_slots = _load_schedulable_slots(state_store=state_store, run_id=run_id, input_root=input_root)
    total_slots = len(queued_slots)
    discovered_count = 0
    if queued_slots:
        _log_stage(f"restored queued slots run_id={run_id} count={len(queued_slots)}")

    def _capacity_log(snapshot: AccountCapacitySnapshot) -> None:
        state_store.record_account_capacity_snapshot(
            account_id=snapshot.account_id,
            host=snapshot.host_alias,
            running_count=snapshot.running_count,
            pending_count=snapshot.pending_count,
            allowed_submit=snapshot.allowed_submit,
        )
        _log_stage(
            f"capacity account={snapshot.account_id} running={snapshot.running_count} "
            f"pending={snapshot.pending_count} allowed_submit={snapshot.allowed_submit}"
        )

    def _capacity_error(account: AccountConfig, exc: Exception) -> None:
        _log_stage(f"capacity query failed account={account.account_id} host={account.host_alias} reason={exc}")

    def _bundle_submitted(bundle: BundleSpec) -> None:
        completed, inflight = state_store.get_slot_throughput_score(run_id=run_id, account_id=bundle.account_id)
        _log_stage(
            f"scheduler pick account={bundle.account_id} score={completed + inflight} slots={bundle.slot_count}"
        )
        _log_stage(
            f"bundle submitted job_id={bundle.job_id} account={bundle.account_id} slot_count={bundle.slot_count}"
        )

    outcomes: list[_BundleRuntimeOutcome] = []
    max_inflight_jobs = 0
    terminal_jobs = 0
    replacement_jobs = 0
    ready_account_ids: list[str] = []
    blocked_account_ids: list[str] = []
    bootstrapping_account_ids: list[str] = []
    readiness_blocked_slots = 0
    next_job_index = state_store.get_next_job_index(run_id=run_id)
    controller: SlotWorkerController[_BundleRuntimeOutcome] | None = None

    def _current_completed_slots() -> dict[str, int]:
        return {
            account.account_id: state_store.get_slot_throughput_score(run_id=run_id, account_id=account.account_id)[0]
            for account in accounts
        }

    def _record_readiness(snapshot: AccountReadinessSnapshot) -> None:
        state_store.record_account_readiness_snapshot(
            account_id=snapshot.account_id,
            host=snapshot.host_alias,
            ready=snapshot.ready,
            status=snapshot.status,
            reason=snapshot.reason,
            home_ok=snapshot.home_ok,
            runtime_path_ok=snapshot.runtime_path_ok,
            venv_ok=snapshot.venv_ok,
            python_ok=snapshot.python_ok,
            module_ok=snapshot.module_ok,
            binaries_ok=snapshot.binaries_ok,
            ansys_ok=snapshot.ansys_ok,
            uv_ok=snapshot.uv_ok,
            pyaedt_ok=snapshot.pyaedt_ok,
        )
        if snapshot.ready:
            _log_stage(
                f"account readiness ready account={snapshot.account_id} host={snapshot.host_alias} "
                f"reason={snapshot.reason}"
            )
        else:
            _log_stage(
                f"account readiness {snapshot.status.lower()} account={snapshot.account_id} host={snapshot.host_alias} "
                f"reason={snapshot.reason}"
            )
            state_store.append_event(
                run_id=run_id,
                job_id="__worker__",
                level="WARN",
                stage=snapshot.status,
                message=f"account={snapshot.account_id} host={snapshot.host_alias} reason={snapshot.reason}",
            )

    def _resolve_dispatch_accounts() -> list[AccountConfig]:
        nonlocal ready_account_ids, blocked_account_ids, bootstrapping_account_ids
        if not config.execute_remote:
            ready_account_ids = [account.account_id for account in accounts]
            blocked_account_ids = []
            bootstrapping_account_ids = []
            return list(accounts)

        ready_accounts: list[AccountConfig] = []
        ready_account_ids = []
        blocked_account_ids = []
        bootstrapping_account_ids = []
        for account in accounts:
            try:
                snapshot = query_account_readiness(account=account)
            except Exception as exc:
                snapshot = _blocked_readiness_snapshot(account=account, reason=str(exc))
            _record_readiness(snapshot)

            if not snapshot.ready and snapshot.status == "BOOTSTRAP_REQUIRED":
                bootstrapping_account_ids.append(account.account_id)
                bootstrapping_snapshot = _update_readiness_snapshot(
                    snapshot,
                    status="BOOTSTRAPPING",
                    reason=snapshot.reason,
                    ready=False,
                )
                _record_readiness(bootstrapping_snapshot)
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="ACCOUNT_BOOTSTRAP_START",
                    message=f"account={account.account_id} host={account.host_alias} reason={snapshot.reason}",
                )
                try:
                    bootstrap_account_runtime(account=account)
                except Exception as exc:
                    failed_snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="BOOTSTRAP_FAILED",
                        reason=str(exc),
                        ready=False,
                    )
                    _record_readiness(failed_snapshot)
                    blocked_account_ids.append(account.account_id)
                    continue
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="ACCOUNT_BOOTSTRAP_OK",
                    message=f"account={account.account_id} host={account.host_alias}",
                )
                try:
                    snapshot = query_account_preflight(account=account)
                except Exception as exc:
                    snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="PREFLIGHT_FAILED",
                        reason=str(exc),
                        ready=False,
                    )
            elif snapshot.ready:
                try:
                    snapshot = query_account_preflight(account=account)
                except Exception as exc:
                    snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="PREFLIGHT_FAILED",
                        reason=str(exc),
                        ready=False,
                    )

            _record_readiness(snapshot)
            if snapshot.ready:
                ready_accounts.append(account)
                ready_account_ids.append(account.account_id)
            else:
                blocked_account_ids.append(account.account_id)
        return ready_accounts

    def _run_slot_batch(slot_batch: list[SlotTaskRef]) -> bool:
        nonlocal max_inflight_jobs, next_job_index, terminal_jobs, replacement_jobs, readiness_blocked_slots
        if not slot_batch:
            return False
        dispatch_accounts = _resolve_dispatch_accounts()
        if not dispatch_accounts:
            readiness_blocked_slots = len(slot_batch)
            return False
        completed_slots = _current_completed_slots()
        if config.execute_remote:
            batch = run_slot_workers(
                slot_queue=slot_batch,
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=lambda bundle: _run_bundle_with_retry(
                    config=config,
                    state_store=state_store,
                    run_id=run_id,
                    remote_run_dir=remote_run_dir,
                    bundle=bundle,
                ),
                capacity_lookup=query_account_capacity,
                initial_completed_slots=completed_slots,
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_capacity_error=_capacity_error,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=lambda _bundle, outcome: outcome.requeue_slots,
                terminal_bundle_lookup=lambda _bundle, outcome: outcome.terminal_worker,
            )
        else:
            batch = run_slot_workers(
                slot_queue=slot_batch,
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=lambda bundle: _run_dry_bundle(
                    run_id=run_id,
                    state_store=state_store,
                    bundle=bundle,
                ),
                capacity_lookup=_local_capacity_lookup,
                initial_completed_slots=completed_slots,
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=lambda _bundle, outcome: (),
                terminal_bundle_lookup=lambda _bundle, outcome: False,
            )
        max_inflight_jobs = max(max_inflight_jobs, batch.max_inflight_jobs)
        next_job_index += batch.submitted_jobs
        terminal_jobs += batch.terminal_jobs
        replacement_jobs += batch.replacement_jobs
        outcomes.extend(batch.results)
        return True

    def _dispatch_queued_slots(*, max_slots: int | None = None) -> None:
        nonlocal queued_slots
        while True:
            if not queued_slots:
                queued_slots = _load_schedulable_slots(
                    state_store=state_store,
                    run_id=run_id,
                    input_root=input_root,
                )
                if not queued_slots:
                    break
            if max_slots is None or max_slots <= 0 or len(queued_slots) <= max_slots:
                batch_slots = queued_slots
                queued_slots = []
            else:
                batch_slots = queued_slots[:max_slots]
                queued_slots = queued_slots[max_slots:]
            dispatched = _run_slot_batch(batch_slots)
            if not dispatched:
                queued_slots = batch_slots + queued_slots
                break
            if max_slots is not None:
                break

    if not config.continuous_mode:
        _dispatch_queued_slots()

    def _on_slot_enqueued(slot: SlotTaskRef) -> None:
        nonlocal total_slots
        queued_slots.append(slot)
        total_slots += 1
        _dispatch_queued_slots()

    if config.continuous_mode:
        low_watermark, high_watermark = _continuous_backlog_limits(config=config, accounts=accounts)
        pending_scan_files: deque[Path] = deque()
        rescan_scan_files: deque[Path] = deque()
        next_scan_monotonic = 0.0

        def _start_controller(*, force: bool = False) -> bool:
            nonlocal controller, queued_slots, readiness_blocked_slots
            if controller is not None:
                return True
            if not queued_slots:
                return False
            if not force and len(queued_slots) < config.slots_per_job:
                return False
            dispatch_accounts = _resolve_dispatch_accounts()
            if not dispatch_accounts:
                readiness_blocked_slots = len(queued_slots)
                return False
            controller = SlotWorkerController(
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=(
                    lambda bundle: _run_bundle_with_retry(
                        config=config,
                        state_store=state_store,
                        run_id=run_id,
                        remote_run_dir=remote_run_dir,
                        bundle=bundle,
                    )
                )
                if config.execute_remote
                else (
                    lambda bundle: _run_dry_bundle(
                        run_id=run_id,
                        state_store=state_store,
                        bundle=bundle,
                    )
                ),
                capacity_lookup=query_account_capacity if config.execute_remote else _local_capacity_lookup,
                initial_completed_slots=_current_completed_slots(),
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_capacity_error=_capacity_error if config.execute_remote else None,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=(
                    (lambda _bundle, outcome: outcome.requeue_slots)
                    if config.execute_remote
                    else (lambda _bundle, outcome: ())
                ),
                terminal_bundle_lookup=(
                    (lambda _bundle, outcome: outcome.terminal_worker)
                    if config.execute_remote
                    else (lambda _bundle, outcome: False)
                ),
            )
            controller.enqueue_slots(queued_slots)
            queued_slots = []
            controller.step(wait_for_progress=False)
            return True

        while True:
            controller_has_work = controller is not None and controller.has_work()
            if (
                not controller_has_work
                and not queued_slots
                and not pending_scan_files
                and not rescan_scan_files
                and total_slots > 0
            ):
                break

            controller_snapshot = controller.snapshot() if controller is not None else None
            backlog_slots = len(queued_slots)
            if controller_snapshot is not None:
                backlog_slots += (
                    controller_snapshot.queued_slots
                    + controller_snapshot.pending_slots
                    + controller_snapshot.inflight_slots
                )

            if controller is None and queued_slots:
                scan_backlog_empty = not pending_scan_files and not rescan_scan_files
                forced_start = scan_backlog_empty or len(queued_slots) >= config.slots_per_job
                if forced_start and _start_controller(force=scan_backlog_empty):
                    continue

            now = time.monotonic()
            if now >= next_scan_monotonic:
                scan_results = _scan_input_aedt_files(input_root=input_root, recursive=config.scan_recursive)
                queued_scan_paths = set(pending_scan_files)
                queued_scan_paths.update(rescan_scan_files)
                filtered_scan_results = [
                    path
                    for path in scan_results
                    if path not in queued_scan_paths and not Path(f"{path}{config.ready_sidecar_suffix}").exists()
                ]
                has_active_ingest_backlog = bool(
                    pending_scan_files or rescan_scan_files or queued_slots or controller is not None
                )
                if has_active_ingest_backlog:
                    rescan_scan_files.extend(filtered_scan_results)
                else:
                    pending_scan_files.extend(filtered_scan_results)
                next_scan_monotonic = now + config.ingest_poll_seconds

            allow_rescan_ingest = bool(rescan_scan_files)
            scan_queue = rescan_scan_files if allow_rescan_ingest else pending_scan_files
            if scan_queue and (backlog_slots < high_watermark or allow_rescan_ingest):
                input_file = scan_queue.popleft()
                slots, _discovered_delta = _ingest_slot_queue(
                    config=config,
                    state_store=state_store,
                    run_id=run_id,
                    input_root=input_root,
                    output_root=output_root,
                    aedt_files=[input_file],
                )
                discovered_count += len(slots)
                if slots:
                    total_slots += len(slots)
                    if controller is not None:
                        controller.enqueue_slots(slots)
                        controller.step(wait_for_progress=False)
                    else:
                        queued_slots.extend(slots)
                        if len(queued_slots) >= config.slots_per_job:
                            _start_controller()
                continue

            if controller is not None:
                controller_snapshot = controller.snapshot()
                controller_backlog = (
                    controller_snapshot.queued_slots
                    + controller_snapshot.pending_slots
                    + controller_snapshot.inflight_slots
                )
                wait_for_progress = not pending_scan_files and not rescan_scan_files
                progressed = controller.step(wait_for_progress=wait_for_progress)
                if progressed:
                    continue
                if not controller.has_work() and not queued_slots and not pending_scan_files and not rescan_scan_files:
                    break

            if controller is None and not pending_scan_files and not rescan_scan_files:
                if queued_slots:
                    if _start_controller(force=True):
                        continue
                    readiness_blocked_slots = len(queued_slots)
                else:
                    break

            if controller is None and blocked_account_ids and backlog_slots >= high_watermark:
                readiness_blocked_slots = backlog_slots
                break
    else:
        slots, discovered_count = _ingest_slot_queue(
            config=config,
            state_store=state_store,
            run_id=run_id,
            input_root=input_root,
            output_root=output_root,
            aedt_files=aedt_files,
            on_slot_enqueued=None,
        )
        if slots:
            queued_slots.extend(slots)
            total_slots += len(slots)

        _dispatch_queued_slots()

    if config.continuous_mode and controller is not None:
        batch = controller.finalize()
        max_inflight_jobs = max(max_inflight_jobs, batch.max_inflight_jobs)
        next_job_index += batch.submitted_jobs
        terminal_jobs += batch.terminal_jobs
        replacement_jobs += batch.replacement_jobs
        outcomes.extend(batch.results)

    if total_slots == 0:
        summary = (
            f"version={APP_VERSION} total_slots=0 active_slots=0 success_slots=0 failed_slots=0 quarantined_slots=0 "
            f"active_jobs=0 ingest_discovered={discovered_count} ingest_enqueued=0"
        )
        if config.continuous_mode:
            state_store.update_run_summary(run_id=run_id, summary=summary)
        else:
            state_store.finish_run(run_id, state="SUCCEEDED", summary=summary)
        _log_stage(f"pipeline idle run_id={run_id} discovered={discovered_count}")
        return PipelineResult(
            success=True,
            exit_code=EXIT_CODE_SUCCESS,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_artifacts_dir=str(output_root),
            summary=summary,
            total_jobs=0,
            success_jobs=0,
            failed_jobs=0,
            quarantined_jobs=0,
            total_slots=0,
            active_slots=0,
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
            queued_slots=0,
            terminal_jobs=0,
            replacement_jobs=0,
            ready_accounts=tuple(ready_account_ids),
            blocked_accounts=tuple(blocked_account_ids),
            bootstrapping_accounts=tuple(bootstrapping_account_ids),
            readiness_blocked_slots=0,
        )

    orphan_cleanup_error: _WorkflowError | None = None
    if config.execute_remote:
        for account in accounts:
            cleanup_cfg = _RemoteExecutionConfig(
                host=account.host_alias,
                partition=config.partition,
                nodes=config.nodes,
                ntasks=config.ntasks,
                cpus_per_job=config.cpus_per_job,
                mem=config.mem,
                time_limit=config.time_limit,
                slots_per_job=config.slots_per_job,
                cores_per_slot=config.cores_per_slot,
                platform=account.platform,
                scheduler=account.scheduler,
            )
            try:
                cleanup_orphan_sessions_for_run(config=cleanup_cfg, run_id=run_id)
            except _WorkflowError as exc:
                orphan_cleanup_error = exc
                _log_stage(f"orphan cleanup failed run_id={run_id} account={account.account_id} reason={exc}")

    outcomes = sorted(outcomes, key=lambda item: item.job_id)
    total_jobs = len(outcomes)
    failed_jobs = sum(1 for item in outcomes if not item.success)
    quarantined_jobs = sum(1 for item in outcomes if item.quarantined)
    success_jobs = total_jobs - failed_jobs
    failed_job_ids = [item.job_id for item in outcomes if not item.success]

    success_slots = sum(item.success_slots for item in outcomes)
    failed_slots = sum(item.failed_slots for item in outcomes)
    quarantined_slots = sum(item.quarantined_slots for item in outcomes)

    success = (
        failed_jobs == 0
        and quarantined_jobs == 0
        and failed_slots == 0
        and quarantined_slots == 0
        and orphan_cleanup_error is None
        and readiness_blocked_slots == 0
    )
    first_failure_code = next((item.exit_code for item in outcomes if item.exit_code != EXIT_CODE_SUCCESS), EXIT_CODE_SUCCESS)
    exit_code = EXIT_CODE_SUCCESS if success else first_failure_code
    if exit_code == EXIT_CODE_SUCCESS and readiness_blocked_slots > 0:
        exit_code = EXIT_CODE_REMOTE_RUN_FAILURE
    if orphan_cleanup_error is not None:
        exit_code = orphan_cleanup_error.exit_code

    summary = (
        f"version={APP_VERSION} total_jobs={total_jobs} success_jobs={success_jobs} failed_jobs={failed_jobs} "
        f"quarantined_jobs={quarantined_jobs} total_slots={total_slots} "
        f"active_slots=0 success_slots={success_slots} failed_slots={failed_slots} "
        f"quarantined_slots={quarantined_slots} failed_job_ids={failed_job_ids} "
        f"active_jobs=0 max_inflight_jobs={max_inflight_jobs} "
        f"terminal_jobs={terminal_jobs} replacement_jobs={replacement_jobs} "
        f"ready_accounts={ready_account_ids} blocked_accounts={blocked_account_ids} "
        f"bootstrapping_accounts={bootstrapping_account_ids} "
        f"readiness_blocked_slots={readiness_blocked_slots}"
    )
    if orphan_cleanup_error is not None:
        summary = f"{summary} orphan_cleanup_error={orphan_cleanup_error}"

    if config.continuous_mode:
        state_store.update_run_summary(run_id=run_id, summary=summary)
    else:
        state_store.finish_run(run_id, state="SUCCEEDED" if success else "FAILED", summary=summary)
    _log_stage(
        f"pipeline end version={APP_VERSION} run_id={run_id} success={success} "
        f"total_jobs={total_jobs} failed_jobs={failed_jobs} quarantined_jobs={quarantined_jobs}"
    )

    return PipelineResult(
        success=success,
        exit_code=exit_code,
        run_id=run_id,
        remote_run_dir=remote_run_dir,
        local_artifacts_dir=str(output_root),
        summary=summary,
        total_jobs=total_jobs,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        quarantined_jobs=quarantined_jobs,
        total_slots=total_slots,
        active_slots=0,
        success_slots=success_slots,
        failed_slots=failed_slots,
        quarantined_slots=quarantined_slots,
        queued_slots=readiness_blocked_slots,
        terminal_jobs=terminal_jobs,
        replacement_jobs=replacement_jobs,
        ready_accounts=tuple(ready_account_ids),
        blocked_accounts=tuple(blocked_account_ids),
        bootstrapping_accounts=tuple(bootstrapping_account_ids),
        readiness_blocked_slots=readiness_blocked_slots,
    )


def _load_schedulable_slots(*, state_store: StateStore, run_id: str, input_root: Path) -> list[SlotTaskRef]:
    slots: list[SlotTaskRef] = []
    for slot_id, input_path, output_path, attempt_no in state_store.list_schedulable_slot_tasks(run_id=run_id):
        input_ref = Path(input_path)
        output_ref = Path(output_path)
        try:
            relative_path = input_ref.relative_to(input_root)
        except Exception:
            relative_path = Path(input_ref.name)
        slots.append(
            SlotTaskRef(
                run_id=run_id,
                slot_id=slot_id,
                input_path=input_ref,
                relative_path=Path(relative_path),
                output_dir=output_ref,
                attempt_no=max(1, attempt_no),
            )
        )
    return slots


def _ingest_slot_queue(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    input_root: Path,
    output_root: Path,
    aedt_files: list[Path],
    on_slot_enqueued: Callable[[SlotTaskRef], None] | None = None,
) -> tuple[list[SlotTaskRef], int]:
    slots: list[SlotTaskRef] = []
    discovered_count = 0

    for input_file in aedt_files:
        relative_path = input_file.relative_to(input_root)
        output_dir = _build_slot_output_dir(
            output_root=output_root,
            relative_path=relative_path,
        )
        ready_state = _ensure_ready_artifact(input_file, config.ready_sidecar_suffix)
        if getattr(ready_state, "locked", False):
            discovered_count += 1
            continue
        file_stat = input_file.stat()
        discovered_count += 1

        if config.continuous_mode:
            inserted = state_store.register_ingest_candidate(
                input_path=str(input_file),
                ready_path=str(ready_state.ready_path),
                ready_present=ready_state.ready_present,
                ready_mode=ready_state.ready_mode,
                ready_error=ready_state.ready_error,
                file_size=file_stat.st_size,
                file_mtime_ns=file_stat.st_mtime_ns,
            )
            if not inserted:
                continue

        slot_id = _build_slot_id(relative_path=relative_path, mtime_ns=file_stat.st_mtime_ns)
        state_store.create_slot_task(
            run_id=run_id,
            slot_id=slot_id,
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot_id,
            level="INFO",
            stage="READY" if ready_state.ready_present else "READY_INTERNAL",
            message=(
                f"ready_mode={ready_state.ready_mode}"
                if ready_state.ready_error is None
                else f"ready_mode={ready_state.ready_mode} error={ready_state.ready_error}"
            ),
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot_id,
            level="INFO",
            stage="QUEUED",
            message="ingested",
        )
        if config.continuous_mode:
            state_store.mark_ingest_state(input_path=str(input_file), state="QUEUED")
        slot_ref = SlotTaskRef(
            run_id=run_id,
            slot_id=slot_id,
            input_path=input_file,
            relative_path=relative_path,
            output_dir=output_dir,
            attempt_no=1,
        )
        if on_slot_enqueued is None:
            slots.append(slot_ref)
        else:
            on_slot_enqueued(slot_ref)

    return slots, discovered_count


def _run_dry_bundle(
    *,
    run_id: str,
    state_store: StateStore,
    bundle: BundleSpec,
) -> _BundleRuntimeOutcome:
    if not bundle.slot_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no slots",
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
        )

    first_slot = bundle.slot_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_slot.input_path),
        output_path=str(first_slot.output_dir),
        account_id=bundle.account_id,
    )
    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=0)
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="RUNNING",
        message=f"dry-run slot_count={bundle.slot_count}",
    )

    for slot in bundle.slot_inputs:
        _initialize_slot_output_dir(slot=slot)
        state_store.update_slot_task(
            run_id=run_id,
            slot_id=slot.slot_id,
            state="SUCCEEDED",
            attempt_no=0,
            job_id=bundle.job_id,
            account_id=bundle.account_id,
            failure_reason=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="INFO",
            stage="SUCCEEDED",
            message="execute_remote=False dry run",
        )
        state_store.mark_ingest_state(input_path=str(slot.input_path), state="SUCCEEDED")
        state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(slot.output_dir))

    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUCCEEDED", attempt_no=0)
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="SUCCEEDED",
        message="execute_remote=False dry run",
    )
    return _BundleRuntimeOutcome(
        job_id=bundle.job_id,
        success=True,
        quarantined=False,
        exit_code=EXIT_CODE_SUCCESS,
        message="execute_remote=False dry run",
        success_slots=bundle.slot_count,
        failed_slots=0,
        quarantined_slots=0,
    )


def _run_bundle_with_retry(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    remote_run_dir: str,
    bundle: BundleSpec,
) -> _BundleRuntimeOutcome:
    if not bundle.slot_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no slots",
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
        )

    first_slot = bundle.slot_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_slot.input_path),
        output_path=str(first_slot.output_dir),
        account_id=bundle.account_id,
    )
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="PENDING",
        message=f"bundle queued account={bundle.account_id} slots={bundle.slot_count}",
    )

    remote_cfg = _RemoteExecutionConfig(
        host=bundle.host_alias,
        partition=config.partition,
        nodes=config.nodes,
        ntasks=config.ntasks,
        cpus_per_job=config.cpus_per_job,
        mem=config.mem,
        time_limit=config.time_limit,
        slots_per_job=config.slots_per_job,
        cores_per_slot=config.cores_per_slot,
        platform=bundle.platform,
        scheduler=bundle.scheduler,
    )
    with TemporaryDirectory(prefix=f"peetsfea_bundle_{bundle.job_id}_") as tmpdir:
        local_bundle_dir = Path(tmpdir) / "bundle"
        local_bundle_dir.mkdir(parents=True, exist_ok=True)
        retry_inputs_dir = local_bundle_dir / "_retry_inputs"
        retry_inputs_dir.mkdir(parents=True, exist_ok=True)
        pending_slots = list(bundle.slot_inputs)
        staged_input_paths: dict[str, Path] = {}
        for slot in pending_slots:
            staged_path = retry_inputs_dir / f"{slot.slot_id}.aedt"
            try:
                if slot.input_path.exists():
                    shutil.copy2(slot.input_path, staged_path)
                    staged_input_paths[slot.slot_id] = staged_path
            except OSError:
                continue
        deleted_slot_ids: set[str] = set()
        success_slots = 0
        failed_slots = 0
        quarantined_slots = 0
        terminal_exit_code = EXIT_CODE_SUCCESS
        terminal_message = "ok"
        last_attempt_no = 0
        worker_requeue_slots: list[SlotTaskRef] = []

        for attempt in range(1, config.job_retry_count + 2):
            if not pending_slots:
                break
            last_attempt_no = attempt

            runnable_slots: list[SlotTaskRef] = []
            for slot in pending_slots:
                source_path = staged_input_paths.get(slot.slot_id, slot.input_path)
                if source_path.exists():
                    runnable_slots.append(slot)
                    continue
                state_store.quarantine_job(
                    run_id=run_id,
                    job_id=slot.slot_id,
                    attempt=attempt,
                    reason=f"input missing: {slot.input_path}",
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                )
                state_store.update_slot_task(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    state="QUARANTINED",
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    failure_reason=f"input missing: {slot.input_path}",
                )
                state_store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="ERROR",
                    stage="QUARANTINED",
                    message=f"input missing: {slot.input_path}",
                )
                state_store.mark_ingest_state(input_path=str(slot.input_path), state="QUARANTINED")
                quarantined_slots += 1

            if not runnable_slots:
                pending_slots = []
                terminal_exit_code = EXIT_CODE_REMOTE_RUN_FAILURE
                terminal_message = "all slots quarantined due to missing input files"
                break

            session_name = _build_session_name(run_id, bundle.job_index, attempt)
            _log_stage(
                f"job start job_id={bundle.job_id} attempt={attempt} session={session_name} slots={len(runnable_slots)}"
            )
            state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUBMITTED", attempt_no=attempt)
            state_store.append_event(
                run_id=run_id,
                job_id=bundle.job_id,
                level="INFO",
                stage="SUBMITTED",
                message=f"attempt={attempt} slots={len(runnable_slots)}",
            )

            for slot in runnable_slots:
                state_store.update_slot_task(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    state="ASSIGNED",
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    failure_reason=None,
                )
                state_store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="INFO",
                    stage="ASSIGNED",
                    message=f"job={bundle.job_id} attempt={attempt}",
                )

            _mark_slot_lifecycle_stage(
                state_store=state_store,
                run_id=run_id,
                slots=runnable_slots,
                attempt_no=attempt,
                job_id=bundle.job_id,
                account_id=bundle.account_id,
                state="UPLOADING",
                message=f"job={bundle.job_id} attempt={attempt}",
            )

            state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=attempt)
            attempt_id = state_store.start_attempt(
                run_id=run_id,
                job_id=bundle.job_id,
                attempt_no=attempt,
                node=bundle.host_alias,
            )

            def _delete_after_upload() -> None:
                if config.delete_input_after_upload:
                    for slot in runnable_slots:
                        state_store.mark_slot_delete_pending(run_id=run_id, slot_id=slot.slot_id)
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="INFO",
                            stage="DELETE_PENDING",
                            message="uploaded; waiting for terminal materialization",
                        )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="UPLOADED")
                _mark_slot_lifecycle_stage(
                    state_store=state_store,
                    run_id=run_id,
                    slots=runnable_slots,
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    state="RUNNING",
                    message=f"job={bundle.job_id} attempt={attempt} uploaded",
                )

            try:
                result = run_remote_job_attempt(
                    config=remote_cfg,
                    slot_inputs=[
                        SlotInput(
                            slot_id=slot.slot_id,
                            input_path=staged_input_paths.get(slot.slot_id, slot.input_path),
                        )
                        for slot in runnable_slots
                    ],
                    remote_job_dir=_join_remote_root(
                        _join_remote_root(_join_remote_root(remote_run_dir, bundle.account_id), bundle.job_id),
                        f"a{attempt}",
                    ),
                    local_job_dir=local_bundle_dir,
                    session_name=session_name,
                    on_upload_success=_delete_after_upload,
                )
            except Exception as exc:  # pragma: no cover
                result = RemoteJobAttemptResult(
                    success=False,
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                    session_name=session_name,
                    case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                    message=f"unexpected exception: {exc}",
                    failed_case_lines=[],
                    failure_category="launch",
                )
            materialized_slot_ids = _materialize_slot_outputs(
                local_bundle_dir=local_bundle_dir,
                slots=runnable_slots,
                staged_input_paths=staged_input_paths,
            )
            if not result.success or result.failure_category:
                _materialize_slot_failure_artifacts(
                    local_bundle_dir=local_bundle_dir,
                    slots=runnable_slots,
                    staged_input_paths=staged_input_paths,
                )
            formatted_result_message = _format_failure_message(
                message=result.message,
                failure_category=result.failure_category,
            )

            try:
                cleanup_orphan_session(config=remote_cfg, session_name=session_name)
            except _WorkflowError as cleanup_error:
                state_store.append_event(
                    run_id=run_id,
                    job_id=bundle.job_id,
                    level="WARN",
                    stage="ORPHAN_CLEANUP_FAILED",
                    message=str(cleanup_error),
                )

            state_store.finish_attempt(
                run_id=run_id,
                attempt_id=attempt_id,
                exit_code=result.exit_code,
                error=None if result.success else formatted_result_message,
            )
            terminal_exit_code = result.exit_code
            terminal_message = formatted_result_message

            if not result.success and result.failure_category:
                state_store.append_event(
                    run_id=run_id,
                    job_id=bundle.job_id,
                    level="ERROR",
                    stage=f"FAILURE_{result.failure_category.upper()}",
                    message=terminal_message,
                )

            failed_indices = _parse_failed_case_indices(result.failed_case_lines, len(runnable_slots))
            if not result.success and not failed_indices:
                failed_indices = set(range(len(runnable_slots)))
            for index, slot in enumerate(runnable_slots):
                if slot.slot_id not in materialized_slot_ids:
                    failed_indices.add(index)
                    if result.success:
                        terminal_exit_code = EXIT_CODE_DOWNLOAD_FAILURE
                        terminal_message = "output materialization missing"

            retry_slots: list[SlotTaskRef] = []
            for index, slot in enumerate(runnable_slots):
                if index in failed_indices:
                    failure_message = terminal_message
                    should_requeue = (
                        slot.slot_id not in materialized_slot_ids
                        and slot.attempt_no <= config.worker_requeue_limit
                    )
                    if attempt <= config.job_retry_count:
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="FAILED")
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="RETRY_QUEUED",
                            attempt_no=attempt,
                            job_id=bundle.job_id,
                            account_id=bundle.account_id,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="WARN",
                            stage="RETRY_QUEUED",
                            message=f"attempt={attempt} reason={failure_message}",
                        )
                        retry_slots.append(
                            SlotTaskRef(
                                run_id=slot.run_id,
                                slot_id=slot.slot_id,
                                input_path=slot.input_path,
                                relative_path=slot.relative_path,
                                output_dir=slot.output_dir,
                                attempt_no=slot.attempt_no + 1,
                            )
                        )
                    elif should_requeue:
                        restore_source = staged_input_paths.get(slot.slot_id)
                        if restore_source is not None:
                            _restore_slot_input_from_stage(
                                source_path=restore_source,
                                target_path=slot.input_path,
                                ready_suffix=config.ready_sidecar_suffix,
                            )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="RETRY_QUEUED")
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="RETRY_QUEUED",
                            attempt_no=slot.attempt_no + 1,
                            job_id=bundle.job_id,
                            account_id=None,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="WARN",
                            stage="RETRY_QUEUED",
                            message=f"worker_requeue attempt_no={slot.attempt_no + 1} reason={failure_message}",
                        )
                        worker_requeue_slots.append(
                            SlotTaskRef(
                                run_id=slot.run_id,
                                slot_id=slot.slot_id,
                                input_path=slot.input_path,
                                relative_path=slot.relative_path,
                                output_dir=slot.output_dir,
                                attempt_no=slot.attempt_no + 1,
                            )
                        )
                    else:
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="FAILED")
                        state_store.quarantine_job(
                            run_id=run_id,
                            job_id=slot.slot_id,
                            attempt=attempt,
                            reason=failure_message,
                            exit_code=terminal_exit_code,
                        )
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="QUARANTINED",
                            attempt_no=attempt,
                            job_id=bundle.job_id,
                            account_id=bundle.account_id,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="ERROR",
                            stage="QUARANTINED",
                            message=failure_message,
                        )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="QUARANTINED")
                        _finalize_slot_input_cleanup(
                            config=config,
                            state_store=state_store,
                            run_id=run_id,
                            slot=slot,
                            deleted_slot_ids=deleted_slot_ids,
                        )
                        quarantined_slots += 1
                else:
                    state_store.update_slot_task(
                        run_id=run_id,
                        slot_id=slot.slot_id,
                        state="SUCCEEDED",
                        attempt_no=attempt,
                        job_id=bundle.job_id,
                        account_id=bundle.account_id,
                        failure_reason=None,
                    )
                    state_store.append_slot_event(
                        run_id=run_id,
                        slot_id=slot.slot_id,
                        level="INFO",
                        stage="SUCCEEDED",
                        message=f"job={bundle.job_id} attempt={attempt}",
                    )
                    state_store.mark_ingest_state(input_path=str(slot.input_path), state="SUCCEEDED")
                    state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(slot.output_dir))
                    _finalize_slot_input_cleanup(
                        config=config,
                        state_store=state_store,
                        run_id=run_id,
                        slot=slot,
                        deleted_slot_ids=deleted_slot_ids,
                    )
                    success_slots += 1

            if not retry_slots:
                failed_slots = 0
                break
            pending_slots = retry_slots
            _log_stage(f"job retry scheduled job_id={bundle.job_id} next_attempt={attempt + 1}")

    quarantined = quarantined_slots > 0
    success = (failed_slots == 0) and (quarantined_slots == 0)
    final_status = "SUCCEEDED" if success else ("QUARANTINED" if quarantined else "FAILED")
    state_store.update_job_status(
        run_id=run_id,
        job_id=bundle.job_id,
        status=final_status,
        attempt_no=max(1, last_attempt_no),
        failure_reason=None if success else terminal_message,
    )
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO" if success else "ERROR",
        stage=final_status,
        message=terminal_message,
    )
    _log_stage(
        f"bundle result job_id={bundle.job_id} success_slots={success_slots} "
        f"failed_slots={failed_slots} quarantined_slots={quarantined_slots}"
    )
    return _BundleRuntimeOutcome(
        job_id=bundle.job_id,
        success=success,
        quarantined=quarantined,
        exit_code=EXIT_CODE_SUCCESS if success else terminal_exit_code,
        message=terminal_message,
        success_slots=success_slots,
        failed_slots=failed_slots,
        quarantined_slots=quarantined_slots,
        requeue_slots=tuple(worker_requeue_slots),
    )


def _build_slot_output_dir(*, output_root: Path, relative_path: Path) -> Path:
    return output_root / relative_path.parent / f"{relative_path.name}.out"


def _seed_slot_output_dir(*, slot: SlotTaskRef, seed_input_path: Path | None = None) -> None:
    slot.output_dir.mkdir(parents=True, exist_ok=True)
    source_input = seed_input_path if seed_input_path is not None else slot.input_path
    target_input = slot.output_dir / slot.input_path.name
    if source_input.exists() and not target_input.exists():
        shutil.copy2(source_input, target_input)


def _initialize_slot_output_dir(*, slot: SlotTaskRef, seed_input_path: Path | None = None) -> None:
    if slot.output_dir.exists():
        shutil.rmtree(slot.output_dir)
    _seed_slot_output_dir(slot=slot, seed_input_path=seed_input_path)


def _materialize_slot_outputs(
    *,
    local_bundle_dir: Path,
    slots: list[SlotTaskRef],
    staged_input_paths: dict[str, Path] | None = None,
) -> set[str]:
    materialized_slot_ids: set[str] = set()
    for index, slot in enumerate(slots, start=1):
        case_dir = local_bundle_dir / f"case_{index:02d}"
        if not case_dir.exists():
            continue
        seed_input_path = None if staged_input_paths is None else staged_input_paths.get(slot.slot_id)
        _initialize_slot_output_dir(slot=slot, seed_input_path=seed_input_path)
        copied_any = False
        for item in case_dir.iterdir():
            if item.name == "tmp":
                continue
            target_name = _rename_case_output_name(case_name=item.name, input_name=slot.input_path.name)
            target_path = slot.output_dir / target_name
            if item.is_dir():
                shutil.copytree(item, target_path, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target_path)
            copied_any = True
        _copy_bundle_summary_artifacts(local_bundle_dir=local_bundle_dir, output_dir=slot.output_dir)
        if copied_any:
            materialized_slot_ids.add(slot.slot_id)
    return materialized_slot_ids


def _materialize_slot_failure_artifacts(
    *,
    local_bundle_dir: Path,
    slots: list[SlotTaskRef],
    staged_input_paths: dict[str, Path] | None = None,
) -> set[str]:
    artifact_names = (
        "bundle.exit.code",
        "remote_stdout.log",
        "remote_stderr.log",
        "remote_submission.log",
        "failure_category.txt",
        "failure_reason.txt",
        "failed_case_lines.txt",
    )
    artifact_paths = [local_bundle_dir / name for name in artifact_names if (local_bundle_dir / name).exists()]
    if not artifact_paths:
        return set()

    materialized_slot_ids: set[str] = set()
    for slot in slots:
        seed_input_path = None if staged_input_paths is None else staged_input_paths.get(slot.slot_id)
        _seed_slot_output_dir(slot=slot, seed_input_path=seed_input_path)

        copied_any = False
        for artifact_path in artifact_paths:
            shutil.copy2(artifact_path, slot.output_dir / artifact_path.name)
            copied_any = True

        bundle_exit_path = local_bundle_dir / "bundle.exit.code"
        output_exit_path = slot.output_dir / "exit.code"
        if not output_exit_path.exists() and bundle_exit_path.exists():
            shutil.copy2(bundle_exit_path, output_exit_path)
            copied_any = True

        output_run_log = slot.output_dir / "run.log"
        if not output_run_log.exists():
            remote_submission_log = local_bundle_dir / "remote_submission.log"
            remote_stderr_log = local_bundle_dir / "remote_stderr.log"
            failure_reason_path = local_bundle_dir / "failure_reason.txt"
            if remote_submission_log.exists():
                shutil.copy2(remote_submission_log, output_run_log)
                copied_any = True
            elif remote_stderr_log.exists():
                shutil.copy2(remote_stderr_log, output_run_log)
                copied_any = True
            elif failure_reason_path.exists():
                output_run_log.write_text(failure_reason_path.read_text(encoding="utf-8"), encoding="utf-8")
                copied_any = True
        _copy_bundle_summary_artifacts(local_bundle_dir=local_bundle_dir, output_dir=slot.output_dir)

        if copied_any:
            materialized_slot_ids.add(slot.slot_id)

    return materialized_slot_ids


def _copy_bundle_summary_artifacts(*, local_bundle_dir: Path, output_dir: Path) -> None:
    for name in ("case_summary.txt", "failed.count"):
        source_path = local_bundle_dir / name
        if source_path.exists():
            shutil.copy2(source_path, output_dir / name)


def _format_failure_message(*, message: str, failure_category: str | None) -> str:
    normalized_message = message.strip()
    if not failure_category:
        return normalized_message
    prefix = f"{failure_category.upper()}: "
    if normalized_message.startswith(prefix):
        return normalized_message
    return f"{prefix}{normalized_message}"


def _rename_case_output_name(*, case_name: str, input_name: str) -> str:
    if case_name.startswith("project.aedt"):
        return f"{input_name}{case_name[len('project.aedt'):]}"
    return case_name


def _delete_slot_input_files(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slots: list[SlotTaskRef],
    deleted_slot_ids: set[str],
) -> None:
    if not config.delete_input_after_upload:
        return
    for slot in slots:
        if slot.slot_id in deleted_slot_ids:
            continue
        _delete_slot_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            slot=slot,
        )
        deleted_slot_ids.add(slot.slot_id)


def _finalize_slot_input_cleanup(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slot: SlotTaskRef,
    deleted_slot_ids: set[str],
) -> None:
    if slot.slot_id in deleted_slot_ids:
        return
    if config.delete_input_after_upload and _slot_output_has_materialized_artifacts(
        output_dir=slot.output_dir,
        input_name=slot.input_path.name,
    ):
        _delete_slot_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            slot=slot,
        )
        deleted_slot_ids.add(slot.slot_id)
        return

    retain_reason = "delete disabled"
    if config.delete_input_after_upload:
        retain_reason = "materialized output missing; input retained"
    state_store.mark_slot_delete_retained(run_id=run_id, slot_id=slot.slot_id)
    state_store.append_slot_event(
        run_id=run_id,
        slot_id=slot.slot_id,
        level="INFO" if not config.delete_input_after_upload else "WARN",
        stage="DELETE_RETAINED",
        message=retain_reason,
    )


def _delete_slot_input_file(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slot: SlotTaskRef,
) -> None:
    path = slot.input_path
    ready_path = _ready_path_for_input(path, config.ready_sidecar_suffix)
    for retry in range(3):
        try:
            if path.exists():
                path.unlink()
            if ready_path.exists():
                ready_path.unlink()
            state_store.mark_slot_input_deleted(run_id=run_id, slot_id=slot.slot_id, retry_count=retry)
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="INFO",
                stage="INPUT_DELETED",
                message=f"retry={retry}",
            )
            state_store.mark_ingest_state(input_path=str(path), state="DELETED")
            return
        except OSError as exc:
            state_store.mark_slot_delete_retrying(run_id=run_id, slot_id=slot.slot_id, retry_count=retry + 1)
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="WARN",
                stage="DELETE_RETRYING",
                message=str(exc),
            )
            time.sleep(0.2)

    quarantine_root = Path(config.delete_failed_quarantine_dir).expanduser().resolve()
    quarantine_target = quarantine_root / slot.relative_path
    quarantine_ready_target = _ready_path_for_input(quarantine_target, config.ready_sidecar_suffix)
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists():
            shutil.move(str(path), str(quarantine_target))
        if ready_path.exists():
            shutil.move(str(ready_path), str(quarantine_ready_target))
    finally:
        state_store.mark_slot_delete_quarantined(
            run_id=run_id,
            slot_id=slot.slot_id,
            retry_count=3,
            quarantine_path=str(quarantine_target),
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="ERROR",
            stage="DELETE_QUARANTINED",
            message=str(quarantine_target),
        )
        state_store.mark_ingest_state(input_path=str(path), state="DELETE_QUARANTINED")


def _slot_output_has_materialized_artifacts(*, output_dir: Path, input_name: str) -> bool:
    if not output_dir.exists():
        return False
    for item in output_dir.iterdir():
        if item.name == input_name:
            continue
        return True
    return False


def _mark_slot_lifecycle_stage(
    *,
    state_store: StateStore,
    run_id: str,
    slots: list[SlotTaskRef],
    attempt_no: int,
    job_id: str,
    account_id: str,
    state: str,
    message: str,
) -> None:
    for slot in slots:
        state_store.update_slot_task(
            run_id=run_id,
            slot_id=slot.slot_id,
            state=state,
            attempt_no=attempt_no,
            job_id=job_id,
            account_id=account_id,
            failure_reason=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="INFO",
            stage=state,
            message=message,
        )


def _parse_failed_case_indices(case_lines: list[str], case_count: int) -> set[int]:
    failed_indices: set[int] = set()
    for line in case_lines:
        parts = line.split(":", 1)
        if len(parts) != 2:
            continue
        case_name = parts[0].strip()
        if not case_name.startswith("case_"):
            continue
        try:
            index = int(case_name.split("_", 1)[1])
        except ValueError:
            continue
        if 1 <= index <= case_count:
            failed_indices.add(index - 1)
    return failed_indices


def _local_capacity_lookup(*, account: AccountConfig, pending_buffer_per_account: int) -> AccountCapacitySnapshot:
    return AccountCapacitySnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        running_count=0,
        pending_count=0,
        allowed_submit=max(0, account.max_jobs) + max(0, pending_buffer_per_account),
    )


def _build_session_name(run_id: str, job_index: int, attempt: int) -> str:
    return f"aedt_{run_id}_{job_index:02d}_a{attempt}"


def _ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _join_remote_root(remote_root: str, suffix: str) -> str:
    normalized = remote_root.rstrip("/")
    if normalized:
        return f"{normalized}/{suffix}"
    return suffix


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


def _ready_path_for_input(input_path: Path, ready_suffix: str) -> Path:
    return Path(f"{input_path}{ready_suffix}")


def _lock_path_for_input(input_path: Path) -> Path:
    return Path(f"{input_path}.lock")


def _restore_slot_input_from_stage(*, source_path: Path, target_path: Path, ready_suffix: str) -> None:
    if not source_path.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    _ensure_ready_artifact(target_path, ready_suffix)


def _ensure_ready_artifact(input_path: Path, ready_suffix: str) -> _ReadyArtifactState:
    ready_path = _ready_path_for_input(input_path, ready_suffix)
    lock_path = _lock_path_for_input(input_path)
    if lock_path.exists():
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=ready_path.exists(),
            ready_mode="LOCKED",
            ready_error=f"lock present: {lock_path.name}",
            locked=True,
        )
    try:
        ready_path.parent.mkdir(parents=True, exist_ok=True)
        ready_path.touch(exist_ok=True)
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=True,
            ready_mode="SIDECAR",
            locked=False,
        )
    except OSError as exc:
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=ready_path.exists(),
            ready_mode="INTERNAL_ONLY",
            ready_error=str(exc),
            locked=False,
        )


def _build_slot_id(*, relative_path: Path, mtime_ns: int) -> str:
    digest = sha1(f"{relative_path.as_posix()}:{mtime_ns}".encode("utf-8")).hexdigest()[:16]
    return f"w_{digest}"
