from __future__ import annotations

import os

from .web_status import start_status_server


def run_status_server() -> None:
    db_path = os.getenv("PEETSFEA_DB_PATH", "./peetsfea_runner.duckdb")
    host = os.getenv("PEETSFEA_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("PEETSFEA_WEB_PORT", "8765"))
    if port <= 0 or port > 65535:
        raise ValueError("PEETSFEA_WEB_PORT must be in 1..65535")

    server = start_status_server(db_path=db_path, host=host, port=port)
    print(f"[peetsfea][web] listening on http://{host}:{port}", flush=True)
    server.serve_forever()
