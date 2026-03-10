import os
import threading
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, __version__, run_pipeline
from peetsfea_runner.web_status import start_status_server


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_config(workspace_root: Path) -> PipelineConfig:
    input_dir = Path(os.getenv("PEETSFEA_INPUT_QUEUE_DIR", str(workspace_root / "input_queue")))
    output_root = Path(os.getenv("PEETSFEA_OUTPUT_ROOT_DIR", str(workspace_root / "output")))
    delete_failed_dir = Path(
        os.getenv("PEETSFEA_DELETE_FAILED_DIR", str(workspace_root / "output" / "_delete_failed"))
    )
    metadata_db_path = os.getenv("PEETSFEA_DB_PATH", str(workspace_root / "peetsfea_runner.duckdb"))
    return PipelineConfig(
        input_queue_dir=str(input_dir),
        output_root_dir=str(output_root),
        delete_failed_quarantine_dir=str(delete_failed_dir),
        metadata_db_path=metadata_db_path,
        execute_remote=_env_bool("PEETSFEA_EXECUTE_REMOTE", True),
        partition=os.getenv("PEETSFEA_PARTITION", "cpu2"),
        cpus_per_job=int(os.getenv("PEETSFEA_CPUS_PER_JOB", "16")),
        mem=os.getenv("PEETSFEA_MEM", "960G"),
        time_limit=os.getenv("PEETSFEA_TIME_LIMIT", "05:00:00"),
        remote_root=os.getenv("PEETSFEA_REMOTE_ROOT", "~/aedt_runs"),
        continuous_mode=_env_bool("PEETSFEA_CONTINUOUS_MODE", True),
        ingest_poll_seconds=int(os.getenv("PEETSFEA_INGEST_POLL_SECONDS", "30")),
        ready_sidecar_suffix=os.getenv("PEETSFEA_READY_SIDECAR_SUFFIX", ".ready"),
        slots_per_job=int(os.getenv("PEETSFEA_SLOTS_PER_JOB", "4")),
        cores_per_slot=int(os.getenv("PEETSFEA_CORES_PER_SLOT", "4")),
        worker_requeue_limit=int(os.getenv("PEETSFEA_WORKER_REQUEUE_LIMIT", "1")),
        run_rotation_hours=int(os.getenv("PEETSFEA_RUN_ROTATION_HOURS", "24")),
        pending_buffer_per_account=int(os.getenv("PEETSFEA_PENDING_BUFFER_PER_ACCOUNT", "3")),
        capacity_scope=os.getenv("PEETSFEA_CAPACITY_SCOPE", "all_user_jobs"),
        balance_metric=os.getenv("PEETSFEA_BALANCE_METRIC", "slot_throughput"),
        accounts_registry=(
            AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),
            AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
            AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
        ),
    )


def main() -> None:
    workspace_root = Path(__file__).resolve().parent
    config = _build_config(workspace_root)
    web_server = None
    if os.getenv("PEETSFEA_EMBED_WEB", "true").strip().lower() in {"1", "true", "yes", "y", "on"}:
        web_host = os.getenv("PEETSFEA_WEB_HOST", "127.0.0.1")
        web_port = int(os.getenv("PEETSFEA_WEB_PORT", "8765"))
        web_server = start_status_server(
            db_path=str(workspace_root / "peetsfea_runner.duckdb"),
            host=web_host,
            port=web_port,
        )
        threading.Thread(target=web_server.serve_forever, daemon=True, name="peetsfea-web").start()
        print(f"[peetsfea][web] version={__version__} embedded status server listening on http://{web_host}:{web_port}")
    result = run_pipeline(config)
    print(f"version={result.version}")
    print(result.summary)
    print(f"success={result.success} exit_code={result.exit_code}")
    print(f"run_id={result.run_id}")
    print(f"remote_run_dir={result.remote_run_dir}")
    print(f"local_artifacts_dir={result.local_artifacts_dir}")
    if web_server is not None:
        web_server.shutdown()
        web_server.server_close()


if __name__ == "__main__":
    main()
