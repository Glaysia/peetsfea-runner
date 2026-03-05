from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

from .pipeline import AccountConfig, PipelineConfig, run_pipeline
from .state_store import StateStore
from .web_status import start_status_server


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_accounts_from_env() -> tuple[AccountConfig, ...]:
    """
    Parse PEETSFEA_ACCOUNTS as comma-separated entries.
    Entry formats:
      - account_id@host_alias:max_jobs
      - account_id@host_alias
    Example:
      account_01@gate1-harry:10,account_02@gate1-dhj02:10,account_03@gate1-jji0930:10
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
        host_alias = host_and_jobs.strip()
        if ":" in host_and_jobs:
            host_alias, max_jobs_raw = host_and_jobs.rsplit(":", 1)
            host_alias = host_alias.strip()
            max_jobs = int(max_jobs_raw.strip())
        if not host_alias:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (empty host_alias): {entry}")
        if max_jobs <= 0:
            raise ValueError(f"Invalid PEETSFEA_ACCOUNTS entry (max_jobs must be > 0): {entry}")

        accounts.append(AccountConfig(account_id=account_id, host_alias=host_alias, max_jobs=max_jobs))

    if not accounts:
        raise ValueError("PEETSFEA_ACCOUNTS is set but no valid account entries were parsed.")
    return tuple(accounts)


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

    return PipelineConfig(
        input_queue_dir=input_queue_dir,
        output_root_dir=output_root_dir,
        delete_input_after_upload=_env_bool("PEETSFEA_DELETE_INPUT_AFTER_UPLOAD", True),
        delete_failed_quarantine_dir=delete_failed_dir,
        metadata_db_path=metadata_db_path,
        accounts_registry=accounts_registry,
        execute_remote=_env_bool("PEETSFEA_EXECUTE_REMOTE", True),
        partition=os.getenv("PEETSFEA_PARTITION", "cpu2"),
        remote_root=os.getenv("PEETSFEA_REMOTE_ROOT", "~/aedt_runs"),
        continuous_mode=_env_bool("PEETSFEA_CONTINUOUS_MODE", True),
        ingest_poll_seconds=int(os.getenv("PEETSFEA_INGEST_POLL_SECONDS", "30")),
        ready_sidecar_suffix=os.getenv("PEETSFEA_READY_SIDECAR_SUFFIX", ".ready"),
        run_rotation_hours=int(os.getenv("PEETSFEA_RUN_ROTATION_HOURS", "24")),
        pending_buffer_per_account=int(os.getenv("PEETSFEA_PENDING_BUFFER_PER_ACCOUNT", "3")),
        capacity_scope=os.getenv("PEETSFEA_CAPACITY_SCOPE", "all_user_jobs"),
        balance_metric=os.getenv("PEETSFEA_BALANCE_METRIC", "window_throughput"),
    )


def _start_embedded_web_if_enabled() -> None:
    if not _env_bool("PEETSFEA_EMBED_WEB", True):
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
    print(f"[peetsfea][web] embedded status server listening on http://{host}:{port}", flush=True)


def run_user_worker_once() -> None:
    config = _build_config()
    result = run_pipeline(config)
    print(result.summary, flush=True)


def run_user_worker_loop() -> None:
    poll_seconds = int(os.getenv("PEETSFEA_POLL_SECONDS", "30"))
    if poll_seconds <= 0:
        raise ValueError("PEETSFEA_POLL_SECONDS must be > 0")

    config = _build_config()
    _start_embedded_web_if_enabled()
    store = StateStore(Path(config.metadata_db_path))
    store.initialize()
    service_name = os.getenv("PEETSFEA_WORKER_SERVICE_NAME", "peetsfea-runner")
    host = socket.gethostname()
    pid = os.getpid()
    store.upsert_worker_heartbeat(
        service_name=service_name,
        host=host,
        pid=pid,
        run_id=None,
        status="HEALTHY",
    )
    store.append_event(
        run_id="__worker__",
        job_id="__worker__",
        level="INFO",
        stage="WORKER_LOOP_START",
        message=f"poll_seconds={poll_seconds}",
    )

    last_run_id: str | None = None
    while True:
        current_run_id: str | None = last_run_id
        try:
            store.upsert_worker_heartbeat(
                service_name=service_name,
                host=host,
                pid=pid,
                run_id=current_run_id,
                status="HEALTHY",
            )
            store.append_event(
                run_id=current_run_id or "__worker__",
                job_id="__worker__",
                level="INFO",
                stage="WORKER_LOOP_TICK",
                message="run_pipeline start",
            )
            result = run_pipeline(config)
            current_run_id = result.run_id
            last_run_id = current_run_id
            store.upsert_worker_heartbeat(
                service_name=service_name,
                host=host,
                pid=pid,
                run_id=current_run_id,
                status="HEALTHY",
            )
            store.append_event(
                run_id=current_run_id,
                job_id="__worker__",
                level="INFO",
                stage="WORKER_LOOP_OK",
                message=result.summary,
            )
        except Exception as exc:  # pragma: no cover - runtime resilience path
            print(f"[peetsfea][worker] loop error: {exc}", flush=True)
            store.upsert_worker_heartbeat(
                service_name=service_name,
                host=host,
                pid=pid,
                run_id=current_run_id,
                status="DEGRADED",
            )
            store.append_event(
                run_id=current_run_id or "__worker__",
                job_id="__worker__",
                level="ERROR",
                stage="WORKER_LOOP_ERROR",
                message=str(exc),
            )
        time.sleep(poll_seconds)
