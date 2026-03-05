from __future__ import annotations

import time
from dataclasses import dataclass
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
class PipelineConfig:
    input_aedt_dir: str
    host: str = "gate1-harry"
    partition: str = "cpu2"
    nodes: int = 1
    ntasks: int = 1
    cpus_per_job: int = 32
    mem: str = "320G"
    time_limit: str = "05:00:00"
    remote_root: str = "~/aedt_runs"
    local_artifacts_dir: str = "./artifacts"
    execute_remote: bool = False
    max_jobs_per_account: int = 10
    windows_per_job: int = 8
    cores_per_window: int = 4
    license_cap_per_account: int = 80
    job_retry_count: int = 1
    scan_recursive: bool = False
    metadata_db_path: str = "./peetsfea_runner.duckdb"

    def validate(self) -> list[Path]:
        input_dir = Path(self.input_aedt_dir).expanduser()
        if not input_dir.exists():
            raise FileNotFoundError(f"input_aedt_dir not found: {input_dir}")
        if not input_dir.is_dir():
            raise ValueError(f"input_aedt_dir must be a directory: {input_dir}")

        _ensure_positive("nodes", self.nodes)
        _ensure_positive("ntasks", self.ntasks)
        _ensure_positive("cpus_per_job", self.cpus_per_job)
        _ensure_positive("max_jobs_per_account", self.max_jobs_per_account)
        _ensure_positive("windows_per_job", self.windows_per_job)
        _ensure_positive("cores_per_window", self.cores_per_window)
        _ensure_positive("license_cap_per_account", self.license_cap_per_account)
        if self.job_retry_count < 0:
            raise ValueError("job_retry_count must be >= 0")

        for field_name in ("host", "partition", "mem", "time_limit", "remote_root", "local_artifacts_dir", "metadata_db_path"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")

        required_cpus = self.windows_per_job * self.cores_per_window
        if self.cpus_per_job < required_cpus:
            raise ValueError(f"cpus_per_job must be >= windows_per_job * cores_per_window ({required_cpus})")

        if (self.license_cap_per_account // self.windows_per_job) <= 0:
            raise ValueError("license_cap_per_account // windows_per_job must be >= 1")

        if self.scan_recursive:
            candidates = sorted(
                [
                    path.resolve()
                    for path in input_dir.rglob("*")
                    if path.is_file() and path.suffix.lower() == ".aedt"
                ],
                key=lambda path: path.name.lower(),
            )
        else:
            candidates = sorted(
                [
                    path.resolve()
                    for path in input_dir.glob("*")
                    if path.is_file() and path.suffix.lower() == ".aedt"
                ],
                key=lambda path: path.name.lower(),
            )

        if not candidates:
            raise ValueError(f"No .aedt files found in input_aedt_dir: {input_dir}")
        return candidates


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
    failed_case_lines: list[str]
    message: str


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    if not isinstance(config, PipelineConfig):
        raise TypeError("config must be a PipelineConfig")

    aedt_files = config.validate()
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    remote_run_dir = _join_remote_root(config.remote_root, run_id)
    _log_stage(f"pipeline start run_id={run_id} total_inputs={len(aedt_files)} execute_remote={config.execute_remote}")

    local_root = Path(config.local_artifacts_dir).expanduser().resolve()
    local_run_dir = local_root / run_id
    local_run_dir.mkdir(parents=True, exist_ok=True)

    state_store = StateStore(Path(config.metadata_db_path))
    state_store.initialize()
    state_store.start_run(run_id)

    job_specs = _build_job_specs(aedt_files)
    for job in job_specs:
        state_store.create_job(run_id=run_id, job_id=job.job_id, aedt_path=str(job.aedt_path))
        state_store.append_job_event(run_id=run_id, job_id=job.job_id, attempt=0, event_type="PENDING")

    if not config.execute_remote:
        now = _utc_now_iso()
        for job in job_specs:
            state_store.update_job(
                run_id=run_id,
                job_id=job.job_id,
                attempt=0,
                state="SUCCEEDED",
                session_name="dry_run",
                started_at=now,
                finished_at=now,
                exit_code=EXIT_CODE_SUCCESS,
                failure_reason="",
            )
            state_store.append_job_event(
                run_id=run_id,
                job_id=job.job_id,
                attempt=0,
                event_type="SUCCEEDED",
                details="execute_remote=False dry run",
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
            local_artifacts_dir=str(local_run_dir),
            summary=summary,
            total_jobs=len(job_specs),
            success_jobs=len(job_specs),
            failed_jobs=0,
            quarantined_jobs=0,
        )

    effective_slots = calculate_effective_slots(
        max_jobs_per_account=config.max_jobs_per_account,
        license_cap_per_account=config.license_cap_per_account,
        windows_per_job=config.windows_per_job,
    )
    _log_stage(
        f"scheduler configured run_id={run_id} effective_slots={effective_slots} "
        f"max_jobs_per_account={config.max_jobs_per_account} license_cap={config.license_cap_per_account}"
    )

    batch = run_jobs_with_slots(
        job_specs=job_specs,
        max_slots=effective_slots,
        worker=lambda job_spec: _run_job_with_retry(
            config=config,
            state_store=state_store,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_run_dir=local_run_dir,
            job_spec=job_spec,
        ),
    )

    orphan_cleanup_error: _WorkflowError | None = None
    try:
        cleanup_orphan_sessions_for_run(config=config, run_id=run_id)
    except _WorkflowError as exc:
        orphan_cleanup_error = exc
        _log_stage(f"orphan cleanup failed run_id={run_id} reason={exc}")

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
        local_artifacts_dir=str(local_run_dir),
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
    local_run_dir: Path,
    job_spec: JobSpec,
) -> _JobRuntimeOutcome:
    last_result: RemoteJobAttemptResult | None = None
    for attempt in range(1, config.job_retry_count + 2):
        session_name = _build_session_name(run_id=run_id, job_index=job_spec.job_index, attempt=attempt)
        started_at = _utc_now_iso()
        _log_stage(f"job start job_id={job_spec.job_id} attempt={attempt} session={session_name}")

        state_store.update_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            state="SUBMITTED",
            session_name=session_name,
            started_at=started_at,
        )
        state_store.append_job_event(run_id=run_id, job_id=job_spec.job_id, attempt=attempt, event_type="SUBMITTED")
        state_store.update_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            state="RUNNING",
            session_name=session_name,
            started_at=started_at,
        )
        state_store.append_job_event(run_id=run_id, job_id=job_spec.job_id, attempt=attempt, event_type="RUNNING")

        try:
            result = run_remote_job_attempt(
                config=config,
                aedt_path=job_spec.aedt_path,
                remote_job_dir=_join_remote_root(remote_run_dir, job_spec.job_id),
                local_job_dir=local_run_dir / job_spec.job_id,
                session_name=session_name,
            )
        except Exception as exc:  # pragma: no cover - safety net for unexpected exceptions
            result = RemoteJobAttemptResult(
                success=False,
                exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                session_name=session_name,
                case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                message=f"unexpected exception: {exc}",
                failed_case_lines=[],
            )

        state_store.update_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            state="COLLECTING",
            session_name=session_name,
            started_at=started_at,
        )
        state_store.append_job_event(run_id=run_id, job_id=job_spec.job_id, attempt=attempt, event_type="COLLECTING")

        try:
            cleanup_orphan_session(config=config, session_name=session_name)
            state_store.append_job_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                attempt=attempt,
                event_type="ORPHAN_CLEANUP",
                details="session cleanup completed",
            )
        except _WorkflowError as cleanup_error:
            state_store.append_job_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                attempt=attempt,
                event_type="ORPHAN_CLEANUP_FAILED",
                details=str(cleanup_error),
            )

        finished_at = _utc_now_iso()
        if result.success:
            state_store.update_job(
                run_id=run_id,
                job_id=job_spec.job_id,
                attempt=attempt,
                state="SUCCEEDED",
                session_name=session_name,
                started_at=started_at,
                finished_at=finished_at,
                exit_code=EXIT_CODE_SUCCESS,
                failure_reason="",
            )
            state_store.append_job_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                attempt=attempt,
                event_type="SUCCEEDED",
                details=result.message,
            )
            return _JobRuntimeOutcome(
                job_id=job_spec.job_id,
                success=True,
                quarantined=False,
                exit_code=EXIT_CODE_SUCCESS,
                failed_case_lines=[],
                message=result.message,
            )

        last_result = result
        _log_stage(
            f"job failed job_id={job_spec.job_id} attempt={attempt} "
            f"exit_code={result.exit_code} reason={result.message}"
        )
        state_store.update_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            state="FAILED",
            session_name=session_name,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=result.exit_code,
            failure_reason=result.message,
        )
        state_store.append_job_event(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            event_type="FAILED",
            details=result.message,
        )

        if attempt <= config.job_retry_count:
            _log_stage(f"job retry scheduled job_id={job_spec.job_id} next_attempt={attempt + 1}")
            state_store.append_job_event(
                run_id=run_id,
                job_id=job_spec.job_id,
                attempt=attempt,
                event_type="RETRYING",
                details=f"attempt {attempt} failed",
            )
            continue

        state_store.quarantine_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            reason=result.message,
            exit_code=result.exit_code,
        )
        _log_stage(f"job quarantined job_id={job_spec.job_id} attempt={attempt}")
        state_store.update_job(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            state="QUARANTINED",
            session_name=session_name,
            started_at=started_at,
            finished_at=finished_at,
            exit_code=result.exit_code,
            failure_reason=result.message,
        )
        state_store.append_job_event(
            run_id=run_id,
            job_id=job_spec.job_id,
            attempt=attempt,
            event_type="QUARANTINED",
            details=result.message,
        )
        return _JobRuntimeOutcome(
            job_id=job_spec.job_id,
            success=False,
            quarantined=True,
            exit_code=result.exit_code,
            failed_case_lines=result.failed_case_lines,
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
        failed_case_lines=last_result.failed_case_lines,
        message=last_result.message,
    )


def _build_job_specs(aedt_files: list[Path]) -> list[JobSpec]:
    return [
        JobSpec(
            job_id=f"job_{index:04d}",
            job_index=index,
            aedt_path=aedt_file,
        )
        for index, aedt_file in enumerate(aedt_files, start=1)
    ]


def _build_session_name(*, run_id: str, job_index: int, attempt: int) -> str:
    return f"aedt_{run_id}_{job_index:02d}_a{attempt}"


def _ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _join_remote_root(remote_root: str, suffix: str) -> str:
    normalized = remote_root.rstrip("/")
    if normalized:
        return f"{normalized}/{suffix}"
    return suffix


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


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
