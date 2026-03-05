from __future__ import annotations

import os
import time
from pathlib import Path

from .pipeline import AccountConfig, PipelineConfig, run_pipeline


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_config() -> PipelineConfig:
    repo_root = Path(__file__).resolve().parent.parent
    input_queue_dir = os.getenv("PEETSFEA_INPUT_QUEUE_DIR", str(repo_root / "input_queue"))
    output_root_dir = os.getenv("PEETSFEA_OUTPUT_ROOT_DIR", str(repo_root / "output"))
    delete_failed_dir = os.getenv("PEETSFEA_DELETE_FAILED_DIR", str(repo_root / "output" / "_delete_failed"))
    metadata_db_path = os.getenv("PEETSFEA_DB_PATH", str(repo_root / "peetsfea_runner.duckdb"))

    account_id = os.getenv("PEETSFEA_ACCOUNT_ID", "account_01")
    host_alias = os.getenv("PEETSFEA_HOST_ALIAS", "gate1-harry")
    max_jobs = int(os.getenv("PEETSFEA_MAX_JOBS_PER_ACCOUNT", "10"))

    return PipelineConfig(
        input_queue_dir=input_queue_dir,
        output_root_dir=output_root_dir,
        delete_input_after_upload=_env_bool("PEETSFEA_DELETE_INPUT_AFTER_UPLOAD", True),
        delete_failed_quarantine_dir=delete_failed_dir,
        metadata_db_path=metadata_db_path,
        accounts_registry=(AccountConfig(account_id=account_id, host_alias=host_alias, max_jobs=max_jobs),),
        execute_remote=_env_bool("PEETSFEA_EXECUTE_REMOTE", True),
        partition=os.getenv("PEETSFEA_PARTITION", "cpu2"),
        remote_root=os.getenv("PEETSFEA_REMOTE_ROOT", "~/aedt_runs"),
    )


def run_user_worker_once() -> None:
    config = _build_config()
    result = run_pipeline(config)
    print(result.summary, flush=True)


def run_user_worker_loop() -> None:
    poll_seconds = int(os.getenv("PEETSFEA_POLL_SECONDS", "30"))
    if poll_seconds <= 0:
        raise ValueError("PEETSFEA_POLL_SECONDS must be > 0")

    while True:
        try:
            run_user_worker_once()
        except Exception as exc:  # pragma: no cover - runtime resilience path
            print(f"[peetsfea][worker] loop error: {exc}", flush=True)
        time.sleep(poll_seconds)
