from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import duckdb


def _query(db_path: Path, sql: str, params: list[object] | None = None) -> list[tuple]:
    conn = duckdb.connect(str(db_path))
    try:
        return conn.execute(sql, params or []).fetchall()
    finally:
        conn.close()


def make_status_handler(*, db_path: Path):
    class StatusHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: object, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json({"ok": True})
                return

            if parsed.path == "/api/jobs":
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, job_id, account_id, status, input_path, output_path, updated_at
                    FROM jobs
                    ORDER BY updated_at DESC
                    LIMIT 500
                    """,
                )
                jobs = [
                    {
                        "run_id": row[0],
                        "job_id": row[1],
                        "account_id": row[2],
                        "status": row[3],
                        "input_path": row[4],
                        "output_path": row[5],
                        "updated_at": row[6],
                    }
                    for row in rows
                ]
                self._send_json({"jobs": jobs})
                return

            if parsed.path.startswith("/api/jobs/"):
                job_id = parsed.path.rsplit("/", 1)[-1]
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, job_id, account_id, status, input_path, output_path, created_at, updated_at, failure_reason
                    FROM jobs
                    WHERE job_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    [job_id],
                )
                if not rows:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                row = rows[0]
                attempts = _query(
                    db_path,
                    """
                    SELECT attempt_no, node, started_at, ended_at, exit_code, error
                    FROM attempts
                    WHERE run_id = ? AND job_id = ?
                    ORDER BY attempt_no
                    """,
                    [row[0], row[1]],
                )
                self._send_json(
                    {
                        "job": {
                            "run_id": row[0],
                            "job_id": row[1],
                            "account_id": row[2],
                            "status": row[3],
                            "input_path": row[4],
                            "output_path": row[5],
                            "created_at": row[6],
                            "updated_at": row[7],
                            "failure_reason": row[8],
                            "attempts": [
                                {
                                    "attempt_no": a[0],
                                    "node": a[1],
                                    "started_at": a[2],
                                    "ended_at": a[3],
                                    "exit_code": a[4],
                                    "error": a[5],
                                }
                                for a in attempts
                            ],
                        }
                    }
                )
                return

            if parsed.path == "/api/metrics/throughput":
                rows = _query(
                    db_path,
                    """
                    SELECT
                        COUNT(*) AS total_jobs,
                        SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_jobs,
                        SUM(CASE WHEN status IN ('FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS failed_jobs
                    FROM jobs
                    """
                )
                total, succeeded, failed = rows[0]
                self._send_json(
                    {
                        "metrics": {
                            "total_jobs": int(total or 0),
                            "succeeded_jobs": int(succeeded or 0),
                            "failed_jobs": int(failed or 0),
                        }
                    }
                )
                return

            self._send_json({"error": "not_found"}, status=404)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return StatusHandler


def start_status_server(*, db_path: str, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    handler = make_status_handler(db_path=Path(db_path).expanduser().resolve())
    server = ThreadingHTTPServer((host, port), handler)
    return server
