from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
from .scheduler import JobSpec, calculate_effective_slots, run_jobs_with_slots
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
        if self.job_retry_count < 0:
            raise ValueError("job_retry_count must be >= 0")
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
        if not files:
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


@dataclass(slots=True)
class _JobRuntimeOutcome:
    job_id: str
    success: bool
    quarantined: bool
    exit_code: int
    message: str


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
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    remote_run_dir = _join_remote_root(config.remote_root, run_id)
    _log_stage(f"pipeline start run_id={run_id} total_inputs={len(aedt_files)} execute_remote={config.execute_remote}")

    state_store = StateStore(Path(config.metadata_db_path))
    state_store.initialize()
    state_store.start_run(run_id)

    job_specs = _build_job_specs(aedt_files=aedt_files, input_root=input_root, output_root=output_root, accounts=accounts)
    for job in job_specs:
        state_store.create_job(
            run_id=run_id,
            job_id=job.job_id,
            input_path=str(job.input_path),
            output_path=str(job.output_dir),
            account_id=job.account_id,
        )
        state_store.append_event(
            run_id=run_id,
            job_id=job.job_id,
            level="INFO",
            stage="PENDING",
            message=f"queued on account={job.account_id}",
        )

    if not config.execute_remote:
        for job in job_specs:
            job.output_dir.mkdir(parents=True, exist_ok=True)
            state_store.update_job_status(run_id=run_id, job_id=job.job_id, status="SUCCEEDED", attempt_no=0)
            state_store.record_artifact(run_id=run_id, job_id=job.job_id, artifact_root=str(job.output_dir))
            state_store.append_event(
                run_id=run_id,
                job_id=job.job_id,
                level="INFO",
                stage="SUCCEEDED",
                message="execute_remote=False dry run",
            )
        summary = (
            f"total_jobs={len(job_specs)} success_jobs={len(job_specs)} failed_jobs=0 quarantined_jobs=0 "
            "failed_job_ids=[] remote_execution=disabled"
        )
        state_store.finish_run(run_id, state="SUCCEEDED", summary=summary)
        _log_stage(f"pipeline dry-run complete run_id={run_id}")
        return PipelineResult(
            success=True,
            exit_code=EXIT_CODE_SUCCESS,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_artifacts_dir=str(output_root),
            summary=summary,
            total_jobs=len(job_specs),
            success_jobs=len(job_specs),
            failed_jobs=0,
            quarantined_jobs=0,
        )

    effective_slots = sum(calculate_effective_slots(max_jobs_per_account=account.max_jobs) for account in accounts)
    _log_stage(
        f"scheduler configured run_id={run_id} effective_slots={effective_slots} "
        f"accounts={len(accounts)} license_observe_only={config.license_observe_only}"
    )

    batch = run_jobs_with_slots(
        job_specs=job_specs,
        max_slots=effective_slots,
        worker=lambda job_spec: _run_job_with_retry(
            config=config,
            state_store=state_store,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            job_spec=job_spec,
        ),
    )

    orphan_cleanup_error: _WorkflowError | None = None
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

    success = failed_jobs == 0 and quarantined_jobs == 0 and orphan_cleanup_error is None
    first_failure_code = next((item.exit_code for item in outcomes if item.exit_code != EXIT_CODE_SUCCESS), EXIT_CODE_SUCCESS)
    exit_code = EXIT_CODE_SUCCESS if success else first_failure_code
    if orphan_cleanup_error is not None:
        exit_code = orphan_cleanup_error.exit_code

    summary = (
        f"total_jobs={total_jobs} success_jobs={success_jobs} failed_jobs={failed_jobs} "
        f"quarantined_jobs={quarantined_jobs} failed_job_ids={failed_job_ids} max_inflight={batch.max_inflight}"
    )
    if orphan_cleanup_error is not None:
        summary = f"{summary} orphan_cleanup_error={orphan_cleanup_error}"

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
    )


def _run_job_with_retry(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    remote_run_dir: str,
    job_spec: JobSpec,
) -> _JobRuntimeOutcome:
    staged_input = job_spec.output_dir / "_staged_input.aedt"
    staged_input.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(job_spec.input_path, staged_input)
    input_deleted = not job_spec.input_path.exists()
    last_result: RemoteJobAttemptResult | None = None

    remote_cfg = _RemoteExecutionConfig(
        host=job_spec.host_alias,
        partition=config.partition,
        nodes=config.nodes,
        ntasks=config.ntasks,
        cpus_per_job=config.cpus_per_job,
        mem=config.mem,
        time_limit=config.time_limit,
        windows_per_job=config.windows_per_job,
        cores_per_window=config.cores_per_window,
    )

    def _delete_input_once() -> None:
        nonlocal input_deleted
        if input_deleted or not config.delete_input_after_upload:
            return
        _delete_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            job_spec=job_spec,
        )
        input_deleted = True

    for attempt in range(1, config.job_retry_count + 2):
        _log_stage(f"job start job_id={job_spec.job_id} attempt={attempt} session={_build_session_name(run_id, job_spec.job_index, attempt)}")
        state_store.update_job_status(run_id=run_id, job_id=job_spec.job_id, status="SUBMITTED", attempt_no=attempt)
        state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="INFO", stage="SUBMITTED", message=f"attempt={attempt}")
        state_store.update_job_status(run_id=run_id, job_id=job_spec.job_id, status="RUNNING", attempt_no=attempt)
        state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="INFO", stage="RUNNING", message=f"attempt={attempt}")
        attempt_id = state_store.start_attempt(run_id=run_id, job_id=job_spec.job_id, attempt_no=attempt, node=job_spec.host_alias)

        try:
            result = run_remote_job_attempt(
                config=remote_cfg,
                aedt_path=staged_input,
                remote_job_dir=_join_remote_root(_join_remote_root(remote_run_dir, job_spec.account_id), job_spec.job_id),
                local_job_dir=job_spec.output_dir,
                session_name=_build_session_name(run_id, job_spec.job_index, attempt),
                on_upload_success=_delete_input_once,
            )
        except Exception as exc:  # pragma: no cover
            result = RemoteJobAttemptResult(
                success=False,
                exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                session_name="",
                case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                message=f"unexpected exception: {exc}",
                failed_case_lines=[],
            )

        try:
            cleanup_orphan_session(config=remote_cfg, session_name=_build_session_name(run_id, job_spec.job_index, attempt))
        except _WorkflowError as cleanup_error:
            state_store.append_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                level="WARN",
                stage="ORPHAN_CLEANUP_FAILED",
                message=str(cleanup_error),
            )

        state_store.finish_attempt(run_id=run_id, attempt_id=attempt_id, exit_code=result.exit_code, error=None if result.success else result.message)

        if result.success:
            state_store.update_job_status(run_id=run_id, job_id=job_spec.job_id, status="SUCCEEDED", attempt_no=attempt)
            state_store.record_artifact(run_id=run_id, job_id=job_spec.job_id, artifact_root=str(job_spec.output_dir))
            state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="INFO", stage="SUCCEEDED", message=result.message)
            return _JobRuntimeOutcome(
                job_id=job_spec.job_id,
                success=True,
                quarantined=False,
                exit_code=EXIT_CODE_SUCCESS,
                message=result.message,
            )

        _log_stage(
            f"job failed job_id={job_spec.job_id} attempt={attempt} exit_code={result.exit_code} reason={result.message}"
        )
        state_store.update_job_status(
            run_id=run_id,
            job_id=job_spec.job_id,
            status="FAILED",
            attempt_no=attempt,
            failure_reason=result.message,
        )
        state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="ERROR", stage="FAILED", message=result.message)
        last_result = result

        if attempt <= config.job_retry_count:
            state_store.append_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                level="WARN",
                stage="RETRYING",
                message=f"attempt {attempt} failed",
            )
            continue

        state_store.quarantine_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            reason=result.message,
            exit_code=result.exit_code,
        )
        state_store.update_job_status(
            run_id=run_id,
            job_id=job_spec.job_id,
            status="QUARANTINED",
            attempt_no=attempt,
            failure_reason=result.message,
        )
        state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="ERROR", stage="QUARANTINED", message=result.message)
        return _JobRuntimeOutcome(
            job_id=job_spec.job_id,
            success=False,
            quarantined=True,
            exit_code=result.exit_code,
            message=result.message,
        )

    if last_result is None:
        last_result = RemoteJobAttemptResult(
            success=False,
            exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
            session_name="",
            case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
            message="Unknown job failure",
            failed_case_lines=[],
        )
    return _JobRuntimeOutcome(
        job_id=job_spec.job_id,
        success=False,
        quarantined=True,
        exit_code=last_result.exit_code,
        message=last_result.message,
    )


def _delete_input_file(*, config: PipelineConfig, state_store: StateStore, run_id: str, job_spec: JobSpec) -> None:
    path = job_spec.input_path
    for retry in range(3):
        try:
            if path.exists():
                path.unlink()
            state_store.mark_input_deleted(run_id=run_id, job_id=job_spec.job_id, retry_count=retry)
            state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="INFO", stage="INPUT_DELETED", message=f"retry={retry}")
            return
        except OSError as exc:
            state_store.mark_delete_retrying(run_id=run_id, job_id=job_spec.job_id, retry_count=retry + 1)
            state_store.append_event(run_id=run_id, job_id=job_spec.job_id, level="WARN", stage="DELETE_RETRYING", message=str(exc))
            time.sleep(0.2)

    quarantine_root = Path(config.delete_failed_quarantine_dir).expanduser().resolve()
    quarantine_target = quarantine_root / job_spec.relative_path
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists():
            shutil.move(str(path), str(quarantine_target))
    finally:
        state_store.mark_delete_quarantined(
            run_id=run_id,
            job_id=job_spec.job_id,
            retry_count=3,
            quarantine_path=str(quarantine_target),
        )
        state_store.append_event(
            run_id=run_id,
            job_id=job_spec.job_id,
            level="ERROR",
            stage="DELETE_QUARANTINED",
            message=str(quarantine_target),
        )


def _build_job_specs(
    *,
    aedt_files: list[Path],
    input_root: Path,
    output_root: Path,
    accounts: list[AccountConfig],
) -> list[JobSpec]:
    specs: list[JobSpec] = []
    for index, input_file in enumerate(aedt_files, start=1):
        relative_path = input_file.relative_to(input_root)
        output_dir = output_root / relative_path.parent / f"{input_file.name}.aedt_all"
        account = accounts[(index - 1) % len(accounts)]
        specs.append(
            JobSpec(
                job_id=f"job_{index:04d}",
                job_index=index,
                input_path=input_file,
                relative_path=relative_path,
                output_dir=output_dir,
                account_id=account.account_id,
                host_alias=account.host_alias,
            )
        )
    return specs


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
