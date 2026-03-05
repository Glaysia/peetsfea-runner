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

        def _send_html(self, body: str, status: int = 200) -> None:
            encoded = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._send_html(_dashboard_html())
                return

            if parsed.path == "/health":
                self._send_json({"ok": True})
                return

            if parsed.path == "/api":
                self._send_json(
                    {
                        "endpoints": [
                            "/api/jobs",
                            "/api/jobs/{id}",
                            "/api/metrics/throughput",
                            "/api/runs/latest",
                            "/api/events/recent",
                            "/api/file-lifecycle/summary",
                        ]
                    }
                )
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

            if parsed.path == "/api/runs/latest":
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, started_at, finished_at, state, summary
                    FROM runs
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                )
                if not rows:
                    self._send_json({"run": None})
                    return
                row = rows[0]
                self._send_json(
                    {
                        "run": {
                            "run_id": row[0],
                            "started_at": row[1],
                            "finished_at": row[2],
                            "state": row[3],
                            "summary": row[4],
                        }
                    }
                )
                return

            if parsed.path == "/api/events/recent":
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, job_id, level, stage, message, ts
                    FROM events
                    ORDER BY ts DESC
                    LIMIT 200
                    """,
                )
                self._send_json(
                    {
                        "events": [
                            {
                                "run_id": row[0],
                                "job_id": row[1],
                                "level": row[2],
                                "stage": row[3],
                                "message": row[4],
                                "ts": row[5],
                            }
                            for row in rows
                        ]
                    }
                )
                return

            if parsed.path == "/api/file-lifecycle/summary":
                rows = _query(
                    db_path,
                    """
                    SELECT delete_final_state, COUNT(*)
                    FROM file_lifecycle
                    GROUP BY delete_final_state
                    ORDER BY delete_final_state
                    """,
                )
                self._send_json(
                    {
                        "file_lifecycle": [
                            {"state": row[0], "count": int(row[1])} for row in rows
                        ]
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


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Peets FEA Status</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #0b1220; color: #e5e7eb; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 20px; }
    h1 { margin: 0 0 10px; font-size: 24px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 16px 0; }
    .card { background: #111a2e; border: 1px solid #25324a; border-radius: 10px; padding: 12px; }
    .k { color: #9ca3af; font-size: 12px; margin-bottom: 4px; }
    .v { font-size: 24px; font-weight: 700; }
    table { width: 100%; border-collapse: collapse; background: #111a2e; border: 1px solid #25324a; border-radius: 10px; overflow: hidden; }
    th, td { text-align: left; padding: 10px; border-bottom: 1px solid #25324a; font-size: 13px; }
    th { background: #0f172a; color: #cbd5e1; position: sticky; top: 0; }
    .row { margin-top: 16px; }
    .muted { color: #9ca3af; font-size: 12px; }
    .status-RUNNING { color: #fbbf24; }
    .status-SUCCEEDED { color: #34d399; }
    .status-FAILED, .status-QUARANTINED { color: #f87171; }
    code { color: #93c5fd; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Peets FEA Status Dashboard</h1>
    <div class="muted">Refreshes every 5 seconds. API index: <code>/api</code></div>

    <div class="grid">
      <div class="card"><div class="k">Total Jobs</div><div class="v" id="m-total">-</div></div>
      <div class="card"><div class="k">Succeeded</div><div class="v" id="m-succ">-</div></div>
      <div class="card"><div class="k">Failed</div><div class="v" id="m-fail">-</div></div>
      <div class="card"><div class="k">Latest Run</div><div class="v" id="run-id" style="font-size:16px;">-</div></div>
    </div>

    <div class="row">
      <table>
        <thead><tr><th>job_id</th><th>account</th><th>status</th><th>input</th><th>output</th><th>updated_at</th></tr></thead>
        <tbody id="jobs-body"></tbody>
      </table>
    </div>
  </div>
  <script>
    async function fetchJson(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(url + " -> " + r.status);
      return r.json();
    }

    function esc(s) { return String(s ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;"); }

    async function refresh() {
      try {
        const [m, j, run] = await Promise.all([
          fetchJson('/api/metrics/throughput'),
          fetchJson('/api/jobs'),
          fetchJson('/api/runs/latest'),
        ]);
        document.getElementById('m-total').textContent = m.metrics.total_jobs;
        document.getElementById('m-succ').textContent = m.metrics.succeeded_jobs;
        document.getElementById('m-fail').textContent = m.metrics.failed_jobs;
        document.getElementById('run-id').textContent = run.run ? run.run.run_id : '-';

        const body = document.getElementById('jobs-body');
        body.innerHTML = '';
        for (const job of (j.jobs || [])) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${esc(job.job_id)}</td>
            <td>${esc(job.account_id)}</td>
            <td class="status-${esc(job.status)}">${esc(job.status)}</td>
            <td>${esc(job.input_path)}</td>
            <td>${esc(job.output_path)}</td>
            <td>${esc(job.updated_at)}</td>
          `;
          body.appendChild(tr);
        }
      } catch (e) {
        console.error(e);
      }
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
