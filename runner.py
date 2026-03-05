import os
import threading
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline
from peetsfea_runner.web_status import start_status_server


def main() -> None:
    workspace_root = Path(__file__).resolve().parent
    input_dir = workspace_root / "input_queue"
    output_root = workspace_root / "output"
    config = PipelineConfig(
        input_queue_dir=str(input_dir),
        output_root_dir=str(output_root),
        delete_failed_quarantine_dir=str(workspace_root / "output" / "_delete_failed"),
        execute_remote=True,
        accounts_registry=(
            AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),
            AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
            AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
        ),
    )
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
        print(f"[peetsfea][web] embedded status server listening on http://{web_host}:{web_port}")
    result = run_pipeline(config)
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
