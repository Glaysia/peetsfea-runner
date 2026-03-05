from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path

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
    WindowInput,
    WorkflowError as _WorkflowError,
    _build_case_aggregation_command,
    _build_case_window_command,
    _build_remote_job_script_content,
    _build_wait_all_command,
    _count_screen_windows,
    _parse_case_summary_lines,
    cleanup_orphan_session,
    cleanup_orphan_sessions_for_run,
    run_remote_job_attempt,
)
from .scheduler import (
    AccountCapacitySnapshot,
    BundleSpec,
    WindowTaskRef,
    query_account_capacity,
    run_window_bundles,
)
from .state_store import StateStore


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


@dataclass(slots=True)
class AccountConfig:
    account_id: str
    host_alias: str
    max_jobs: int = 10
    enabled: bool = True


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
    cpus_per_job: int = 32
    mem: str = "320G"
    time_limit: str = "05:00:00"
    remote_root: str = "~/aedt_runs"
    execute_remote: bool = False
    windows_per_job: int = 8
    cores_per_window: int = 4
    job_retry_count: int = 1
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
    balance_metric: str = "window_throughput"

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
        _ensure_positive("windows_per_job", self.windows_per_job)
        _ensure_positive("cores_per_window", self.cores_per_window)
        _ensure_positive("ingest_poll_seconds", self.ingest_poll_seconds)
        _ensure_positive("run_rotation_hours", self.run_rotation_hours)
        if self.job_retry_count < 0:
            raise ValueError("job_retry_count must be >= 0")
        if self.pending_buffer_per_account < 0:
            raise ValueError("pending_buffer_per_account must be >= 0")
        if self.capacity_scope != "all_user_jobs":
            raise ValueError("capacity_scope must be 'all_user_jobs'")
        if self.balance_metric != "window_throughput":
            raise ValueError("balance_metric must be 'window_throughput'")
        if not self.ready_sidecar_suffix.strip():
            raise ValueError("ready_sidecar_suffix must not be empty")
        for name in ("partition", "mem", "time_limit", "remote_root", "metadata_db_path"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.cpus_per_job < (self.windows_per_job * self.cores_per_window):
            raise ValueError(
                f"cpus_per_job must be >= windows_per_job * cores_per_window ({self.windows_per_job * self.cores_per_window})"
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

        if self.scan_recursive:
            files = sorted(
                [p.resolve() for p in input_root.rglob("*") if p.is_file() and p.suffix.lower() == ".aedt"],
                key=lambda p: str(p.relative_to(input_root)).lower(),
            )
        else:
            files = sorted(
                [p.resolve() for p in input_root.glob("*") if p.is_file() and p.suffix.lower() == ".aedt"],
                key=lambda p: str(p.relative_to(input_root)).lower(),
            )
        files = [path for path in files if _ready_path_for_input(path, self.ready_sidecar_suffix).exists()]
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
    total_windows: int = 0
    active_windows: int = 0
    success_windows: int = 0
    failed_windows: int = 0
    quarantined_windows: int = 0


@dataclass(slots=True)
class _BundleRuntimeOutcome:
    job_id: str
    success: bool
    quarantined: bool
    exit_code: int
    message: str
    success_windows: int
    failed_windows: int
    quarantined_windows: int


@dataclass(slots=True)
class _RemoteExecutionConfig:
    host: str
    partition: str
    nodes: int
    ntasks: int
    cpus_per_job: int
    mem: str
    time_limit: str
    windows_per_job: int
    cores_per_window: int


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
    _log_stage(f"pipeline start run_id={run_id} total_inputs={len(aedt_files)} execute_remote={config.execute_remote}")

    windows, discovered_count = _ingest_window_queue(
        config=config,
        state_store=state_store,
        run_id=run_id,
        input_root=input_root,
        output_root=output_root,
        aedt_files=aedt_files,
    )

    if not windows:
        summary = (
            f"total_windows=0 active_windows=0 success_windows=0 failed_windows=0 quarantined_windows=0 "
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
            total_windows=0,
            active_windows=0,
            success_windows=0,
            failed_windows=0,
            quarantined_windows=0,
        )

    initial_completed_windows = {
        account.account_id: state_store.get_window_throughput_score(run_id=run_id, account_id=account.account_id)[0]
        for account in accounts
    }

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
        completed, inflight = state_store.get_window_throughput_score(run_id=run_id, account_id=bundle.account_id)
        _log_stage(
            f"scheduler pick account={bundle.account_id} score={completed + inflight} windows={bundle.window_count}"
        )
        _log_stage(
            f"bundle submitted job_id={bundle.job_id} account={bundle.account_id} window_count={bundle.window_count}"
        )

    if config.execute_remote:
        batch = run_window_bundles(
            window_queue=windows,
            accounts=accounts,
            windows_per_job=config.windows_per_job,
            pending_buffer_per_account=config.pending_buffer_per_account,
            worker=lambda bundle: _run_bundle_with_retry(
                config=config,
                state_store=state_store,
                run_id=run_id,
                remote_run_dir=remote_run_dir,
                bundle=bundle,
            ),
            capacity_lookup=query_account_capacity,
            initial_completed_windows=initial_completed_windows,
            on_capacity_snapshot=_capacity_log,
            on_capacity_error=_capacity_error,
            on_bundle_submitted=_bundle_submitted,
        )
    else:
        batch = run_window_bundles(
            window_queue=windows,
            accounts=accounts,
            windows_per_job=config.windows_per_job,
            pending_buffer_per_account=config.pending_buffer_per_account,
            worker=lambda bundle: _run_dry_bundle(
                run_id=run_id,
                state_store=state_store,
                bundle=bundle,
            ),
            capacity_lookup=_local_capacity_lookup,
            initial_completed_windows=initial_completed_windows,
            on_capacity_snapshot=_capacity_log,
            on_bundle_submitted=_bundle_submitted,
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
                windows_per_job=config.windows_per_job,
                cores_per_window=config.cores_per_window,
            )
            try:
                cleanup_orphan_sessions_for_run(config=cleanup_cfg, run_id=run_id)
            except _WorkflowError as exc:
                orphan_cleanup_error = exc
                _log_stage(f"orphan cleanup failed run_id={run_id} account={account.account_id} reason={exc}")

    outcomes = sorted(batch.results, key=lambda item: item.job_id)
    total_jobs = len(outcomes)
    failed_jobs = sum(1 for item in outcomes if not item.success)
    quarantined_jobs = sum(1 for item in outcomes if item.quarantined)
    success_jobs = total_jobs - failed_jobs
    failed_job_ids = [item.job_id for item in outcomes if not item.success]

    total_windows = len(windows)
    success_windows = sum(item.success_windows for item in outcomes)
    failed_windows = sum(item.failed_windows for item in outcomes)
    quarantined_windows = sum(item.quarantined_windows for item in outcomes)

    success = (
        failed_jobs == 0
        and quarantined_jobs == 0
        and failed_windows == 0
        and quarantined_windows == 0
        and orphan_cleanup_error is None
    )
    first_failure_code = next((item.exit_code for item in outcomes if item.exit_code != EXIT_CODE_SUCCESS), EXIT_CODE_SUCCESS)
    exit_code = EXIT_CODE_SUCCESS if success else first_failure_code
    if orphan_cleanup_error is not None:
        exit_code = orphan_cleanup_error.exit_code

    summary = (
        f"total_jobs={total_jobs} success_jobs={success_jobs} failed_jobs={failed_jobs} "
        f"quarantined_jobs={quarantined_jobs} total_windows={total_windows} "
        f"active_windows=0 success_windows={success_windows} failed_windows={failed_windows} "
        f"quarantined_windows={quarantined_windows} failed_job_ids={failed_job_ids} "
        f"active_jobs=0 max_inflight_jobs={batch.max_inflight_jobs}"
    )
    if orphan_cleanup_error is not None:
        summary = f"{summary} orphan_cleanup_error={orphan_cleanup_error}"

    if config.continuous_mode:
        state_store.update_run_summary(run_id=run_id, summary=summary)
    else:
        state_store.finish_run(run_id, state="SUCCEEDED" if success else "FAILED", summary=summary)
    _log_stage(
        f"pipeline end run_id={run_id} success={success} total_jobs={total_jobs} "
        f"failed_jobs={failed_jobs} quarantined_jobs={quarantined_jobs}"
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
        total_windows=total_windows,
        active_windows=0,
        success_windows=success_windows,
        failed_windows=failed_windows,
        quarantined_windows=quarantined_windows,
    )


def _ingest_window_queue(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    input_root: Path,
    output_root: Path,
    aedt_files: list[Path],
) -> tuple[list[WindowTaskRef], int]:
    windows: list[WindowTaskRef] = []
    discovered_count = 0

    for input_file in aedt_files:
        relative_path = input_file.relative_to(input_root)
        output_dir = output_root / relative_path.parent / f"{input_file.name}.aedt_all"
        ready_path = _ready_path_for_input(input_file, config.ready_sidecar_suffix)
        file_stat = input_file.stat()
        discovered_count += 1

        if config.continuous_mode:
            inserted = state_store.register_ingest_candidate(
                input_path=str(input_file),
                ready_path=str(ready_path),
                file_size=file_stat.st_size,
                file_mtime_ns=file_stat.st_mtime_ns,
            )
            if not inserted:
                continue
            state_store.mark_ingest_state(input_path=str(input_file), state="QUEUED")

        window_id = _build_window_id(relative_path=relative_path, mtime_ns=file_stat.st_mtime_ns)
        state_store.create_window_task(
            run_id=run_id,
            window_id=window_id,
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id=None,
        )
        state_store.append_window_event(
            run_id=run_id,
            window_id=window_id,
            level="INFO",
            stage="QUEUED",
            message="ingested",
        )
        windows.append(
            WindowTaskRef(
                run_id=run_id,
                window_id=window_id,
                input_path=input_file,
                relative_path=relative_path,
                output_dir=output_dir,
                attempt_no=1,
            )
        )

    return windows, discovered_count


def _run_dry_bundle(
    *,
    run_id: str,
    state_store: StateStore,
    bundle: BundleSpec,
) -> _BundleRuntimeOutcome:
    if not bundle.window_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no windows",
            success_windows=0,
            failed_windows=0,
            quarantined_windows=0,
        )

    first_window = bundle.window_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_window.input_path),
        output_path=str(first_window.output_dir.parent),
        account_id=bundle.account_id,
    )
    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=0)
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="RUNNING",
        message=f"dry-run window_count={bundle.window_count}",
    )

    for window in bundle.window_inputs:
        window.output_dir.mkdir(parents=True, exist_ok=True)
        state_store.update_window_task(
            run_id=run_id,
            window_id=window.window_id,
            state="SUCCEEDED",
            attempt_no=0,
            job_id=bundle.job_id,
            account_id=bundle.account_id,
            failure_reason=None,
        )
        state_store.append_window_event(
            run_id=run_id,
            window_id=window.window_id,
            level="INFO",
            stage="SUCCEEDED",
            message="execute_remote=False dry run",
        )
        state_store.mark_ingest_state(input_path=str(window.input_path), state="SUCCEEDED")

    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUCCEEDED", attempt_no=0)
    state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(first_window.output_dir.parent))
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
        success_windows=bundle.window_count,
        failed_windows=0,
        quarantined_windows=0,
    )


def _run_bundle_with_retry(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    remote_run_dir: str,
    bundle: BundleSpec,
) -> _BundleRuntimeOutcome:
    if not bundle.window_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no windows",
            success_windows=0,
            failed_windows=0,
            quarantined_windows=0,
        )

    local_artifacts_root = Path(config.local_artifacts_dir).expanduser().resolve()
    local_bundle_dir = local_artifacts_root / run_id / bundle.job_id
    local_bundle_dir.mkdir(parents=True, exist_ok=True)
    first_window = bundle.window_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_window.input_path),
        output_path=str(local_bundle_dir),
        account_id=bundle.account_id,
    )
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="PENDING",
        message=f"bundle queued account={bundle.account_id} windows={bundle.window_count}",
    )

    remote_cfg = _RemoteExecutionConfig(
        host=bundle.host_alias,
        partition=config.partition,
        nodes=config.nodes,
        ntasks=config.ntasks,
        cpus_per_job=config.cpus_per_job,
        mem=config.mem,
        time_limit=config.time_limit,
        windows_per_job=config.windows_per_job,
        cores_per_window=config.cores_per_window,
    )
    pending_windows = list(bundle.window_inputs)
    deleted_window_ids: set[str] = set()
    success_windows = 0
    failed_windows = 0
    quarantined_windows = 0
    terminal_exit_code = EXIT_CODE_SUCCESS
    terminal_message = "ok"
    last_attempt_no = 0

    for attempt in range(1, config.job_retry_count + 2):
        if not pending_windows:
            break
        last_attempt_no = attempt

        session_name = _build_session_name(run_id, bundle.job_index, attempt)
        _log_stage(
            f"job start job_id={bundle.job_id} attempt={attempt} session={session_name} windows={len(pending_windows)}"
        )
        state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUBMITTED", attempt_no=attempt)
        state_store.append_event(
            run_id=run_id,
            job_id=bundle.job_id,
            level="INFO",
            stage="SUBMITTED",
            message=f"attempt={attempt} windows={len(pending_windows)}",
        )

        for window in pending_windows:
            state_store.update_window_task(
                run_id=run_id,
                window_id=window.window_id,
                state="RUNNING",
                attempt_no=attempt,
                job_id=bundle.job_id,
                account_id=bundle.account_id,
                failure_reason=None,
            )
            state_store.append_window_event(
                run_id=run_id,
                window_id=window.window_id,
                level="INFO",
                stage="RUNNING",
                message=f"job={bundle.job_id} attempt={attempt}",
            )

        state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=attempt)
        attempt_id = state_store.start_attempt(run_id=run_id, job_id=bundle.job_id, attempt_no=attempt, node=bundle.host_alias)

        def _delete_after_upload() -> None:
            _delete_window_input_files(
                config=config,
                state_store=state_store,
                run_id=run_id,
                windows=pending_windows,
                deleted_window_ids=deleted_window_ids,
            )

        try:
            result = run_remote_job_attempt(
                config=remote_cfg,
                window_inputs=[
                    WindowInput(window_id=window.window_id, input_path=window.input_path) for window in pending_windows
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
            error=None if result.success else result.message,
        )
        terminal_exit_code = result.exit_code
        terminal_message = result.message

        failed_indices = _parse_failed_case_indices(result.failed_case_lines, len(pending_windows))
        if not result.success and not failed_indices:
            failed_indices = set(range(len(pending_windows)))

        retry_windows: list[WindowTaskRef] = []
        for index, window in enumerate(pending_windows):
            if index in failed_indices:
                state_store.mark_ingest_state(input_path=str(window.input_path), state="FAILED")
                if attempt <= config.job_retry_count:
                    state_store.update_window_task(
                        run_id=run_id,
                        window_id=window.window_id,
                        state="RETRY_QUEUED",
                        attempt_no=attempt,
                        job_id=bundle.job_id,
                        account_id=bundle.account_id,
                        failure_reason=result.message,
                    )
                    state_store.append_window_event(
                        run_id=run_id,
                        window_id=window.window_id,
                        level="WARN",
                        stage="RETRY_QUEUED",
                        message=f"attempt={attempt} reason={result.message}",
                    )
                    retry_windows.append(
                        WindowTaskRef(
                            run_id=window.run_id,
                            window_id=window.window_id,
                            input_path=window.input_path,
                            relative_path=window.relative_path,
                            output_dir=window.output_dir,
                            attempt_no=window.attempt_no + 1,
                        )
                    )
                else:
                    state_store.quarantine_job(
                        run_id=run_id,
                        job_id=window.window_id,
                        attempt=attempt,
                        reason=result.message,
                        exit_code=result.exit_code,
                    )
                    state_store.update_window_task(
                        run_id=run_id,
                        window_id=window.window_id,
                        state="QUARANTINED",
                        attempt_no=attempt,
                        job_id=bundle.job_id,
                        account_id=bundle.account_id,
                        failure_reason=result.message,
                    )
                    state_store.append_window_event(
                        run_id=run_id,
                        window_id=window.window_id,
                        level="ERROR",
                        stage="QUARANTINED",
                        message=result.message,
                    )
                    state_store.mark_ingest_state(input_path=str(window.input_path), state="QUARANTINED")
                    quarantined_windows += 1
            else:
                window.output_dir.mkdir(parents=True, exist_ok=True)
                state_store.update_window_task(
                    run_id=run_id,
                    window_id=window.window_id,
                    state="SUCCEEDED",
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    failure_reason=None,
                )
                state_store.append_window_event(
                    run_id=run_id,
                    window_id=window.window_id,
                    level="INFO",
                    stage="SUCCEEDED",
                    message=f"job={bundle.job_id} attempt={attempt}",
                )
                state_store.mark_ingest_state(input_path=str(window.input_path), state="SUCCEEDED")
                success_windows += 1

        if not retry_windows:
            failed_windows = 0
            break
        pending_windows = retry_windows
        _log_stage(f"job retry scheduled job_id={bundle.job_id} next_attempt={attempt + 1}")

    quarantined = quarantined_windows > 0
    success = (failed_windows == 0) and (quarantined_windows == 0)
    final_status = "SUCCEEDED" if success else ("QUARANTINED" if quarantined else "FAILED")
    state_store.update_job_status(
        run_id=run_id,
        job_id=bundle.job_id,
        status=final_status,
        attempt_no=max(1, last_attempt_no),
        failure_reason=None if success else terminal_message,
    )
    state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(local_bundle_dir))
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO" if success else "ERROR",
        stage=final_status,
        message=terminal_message,
    )
    _log_stage(
        f"bundle result job_id={bundle.job_id} success_windows={success_windows} "
        f"failed_windows={failed_windows} quarantined_windows={quarantined_windows}"
    )
    return _BundleRuntimeOutcome(
        job_id=bundle.job_id,
        success=success,
        quarantined=quarantined,
        exit_code=EXIT_CODE_SUCCESS if success else terminal_exit_code,
        message=terminal_message,
        success_windows=success_windows,
        failed_windows=failed_windows,
        quarantined_windows=quarantined_windows,
    )


def _delete_window_input_files(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    windows: list[WindowTaskRef],
    deleted_window_ids: set[str],
) -> None:
    if not config.delete_input_after_upload:
        return
    for window in windows:
        if window.window_id in deleted_window_ids:
            continue
        _delete_window_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            window=window,
        )
        deleted_window_ids.add(window.window_id)


def _delete_window_input_file(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    window: WindowTaskRef,
) -> None:
    path = window.input_path
    ready_path = _ready_path_for_input(path, config.ready_sidecar_suffix)
    for retry in range(3):
        try:
            if path.exists():
                path.unlink()
            if ready_path.exists():
                ready_path.unlink()
            state_store.mark_window_input_deleted(run_id=run_id, window_id=window.window_id, retry_count=retry)
            state_store.append_window_event(
                run_id=run_id,
                window_id=window.window_id,
                level="INFO",
                stage="INPUT_DELETED",
                message=f"retry={retry}",
            )
            state_store.mark_ingest_state(input_path=str(path), state="UPLOADED")
            return
        except OSError as exc:
            state_store.mark_window_delete_retrying(run_id=run_id, window_id=window.window_id, retry_count=retry + 1)
            state_store.append_window_event(
                run_id=run_id,
                window_id=window.window_id,
                level="WARN",
                stage="DELETE_RETRYING",
                message=str(exc),
            )
            time.sleep(0.2)

    quarantine_root = Path(config.delete_failed_quarantine_dir).expanduser().resolve()
    quarantine_target = quarantine_root / window.relative_path
    quarantine_ready_target = _ready_path_for_input(quarantine_target, config.ready_sidecar_suffix)
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists():
            shutil.move(str(path), str(quarantine_target))
        if ready_path.exists():
            shutil.move(str(ready_path), str(quarantine_ready_target))
    finally:
        state_store.mark_window_delete_quarantined(
            run_id=run_id,
            window_id=window.window_id,
            retry_count=3,
            quarantine_path=str(quarantine_target),
        )
        state_store.append_window_event(
            run_id=run_id,
            window_id=window.window_id,
            level="ERROR",
            stage="DELETE_QUARANTINED",
            message=str(quarantine_target),
        )
        state_store.mark_ingest_state(input_path=str(path), state="DELETE_QUARANTINED")


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


def _build_window_id(*, relative_path: Path, mtime_ns: int) -> str:
    digest = sha1(f"{relative_path.as_posix()}:{mtime_ns}".encode("utf-8")).hexdigest()[:16]
    return f"w_{digest}"
