from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb


def _query(db_path: Path, sql: str, params: list[object] | None = None) -> list[tuple]:
    conn = duckdb.connect(str(db_path))
    try:
        return conn.execute(sql, params or []).fetchall()
    finally:
        conn.close()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _age_seconds(value: str | None) -> int | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    now = datetime.now(tz=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0, int((now - parsed).total_seconds()))


def _worker_health_payload(db_path: Path, *, stale_threshold: int) -> dict[str, object]:
    hb_rows = _query(
        db_path,
        """
        SELECT service_name, host, pid, last_seen_ts, run_id, status
        FROM worker_heartbeat
        ORDER BY last_seen_ts DESC
        LIMIT 1
        """,
    )
    event_rows = _query(
        db_path,
        """
        SELECT ts
        FROM events
        ORDER BY ts DESC
        LIMIT 1
        """,
    )
    last_event_ts = event_rows[0][0] if event_rows else None
    last_event_age = _age_seconds(last_event_ts)

    if not hb_rows:
        return {
            "status": "STALE",
            "reason": "no heartbeat",
            "stale_threshold_seconds": stale_threshold,
            "last_heartbeat_ts": None,
            "heartbeat_age_seconds": None,
            "last_event_ts": last_event_ts,
            "last_event_age_seconds": last_event_age,
            "service_name": None,
            "host": None,
            "pid": None,
            "run_id": None,
        }

    row = hb_rows[0]
    heartbeat_age = _age_seconds(row[3])
    is_stale = heartbeat_age is None or heartbeat_age > stale_threshold
    if is_stale:
        status = "STALE"
        reason = "old heartbeat"
    elif row[5] == "DEGRADED":
        status = "DEGRADED"
        reason = "recent errors"
    else:
        status = "HEALTHY"
        reason = "ok"

    return {
        "status": status,
        "reason": reason,
        "stale_threshold_seconds": stale_threshold,
        "last_heartbeat_ts": row[3],
        "heartbeat_age_seconds": heartbeat_age,
        "last_event_ts": last_event_ts,
        "last_event_age_seconds": last_event_age,
        "service_name": row[0],
        "host": row[1],
        "pid": int(row[2]),
        "run_id": row[4],
    }


def make_status_handler(*, db_path: Path):
    stale_threshold = int(os.getenv("PEETSFEA_STALE_THRESHOLD_SECONDS", "90"))

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
            params = parse_qs(parsed.query)

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
                            "/api/worker/health",
                            "/api/jobs",
                            "/api/jobs/{id}",
                            "/api/jobs/{id}/timeline",
                            "/api/metrics/throughput",
                            "/api/runs/latest",
                            "/api/runs/{run_id}/summary",
                            "/api/runs/{run_id}/jobs",
                            "/api/events/recent",
                            "/api/file-lifecycle/summary",
                        ]
                    }
                )
                return

            if parsed.path == "/api/worker/health":
                self._send_json(_worker_health_payload(db_path, stale_threshold=stale_threshold))
                return

            if parsed.path == "/api/jobs":
                limit = int(params.get("limit", ["500"])[0])
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, job_id, account_id, status, input_path, output_path, updated_at
                    FROM jobs
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    [limit],
                )
                self._send_json(
                    {
                        "jobs": [
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
                    }
                )
                return

            if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/timeline"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                job_id = parts[2]
                job_rows = _query(
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
                if not job_rows:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                row = job_rows[0]
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
                events = _query(
                    db_path,
                    """
                    SELECT level, stage, message, ts
                    FROM events
                    WHERE run_id = ? AND job_id = ?
                    ORDER BY ts DESC
                    LIMIT 200
                    """,
                    [row[0], row[1]],
                )
                lifecycle = _query(
                    db_path,
                    """
                    SELECT input_deleted_at, delete_retry_count, delete_final_state, quarantine_path, updated_at
                    FROM file_lifecycle
                    WHERE run_id = ? AND job_id = ?
                    LIMIT 1
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
                        },
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
                        "events": [
                            {"level": e[0], "stage": e[1], "message": e[2], "ts": e[3]} for e in events
                        ],
                        "file_lifecycle": (
                            {
                                "input_deleted_at": lifecycle[0][0],
                                "delete_retry_count": lifecycle[0][1],
                                "delete_final_state": lifecycle[0][2],
                                "quarantine_path": lifecycle[0][3],
                                "updated_at": lifecycle[0][4],
                            }
                            if lifecycle
                            else None
                        ),
                    }
                )
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
                        SUM(CASE WHEN status IN ('FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS failed_jobs,
                        SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) AS active_jobs,
                        SUM(CASE WHEN status IN ('PENDING', 'SUBMITTED') THEN 1 ELSE 0 END) AS queue_jobs
                    FROM jobs
                    """
                )
                total, succeeded, failed, active_jobs, queue_jobs = rows[0]
                total_v = int(total or 0)
                failed_v = int(failed or 0)
                failed_ratio = float(failed_v / total_v) if total_v > 0 else 0.0
                self._send_json(
                    {
                        "metrics": {
                            "total_jobs": total_v,
                            "succeeded_jobs": int(succeeded or 0),
                            "failed_jobs": failed_v,
                            "active_jobs": int(active_jobs or 0),
                            "queue_jobs": int(queue_jobs or 0),
                            "failed_ratio": failed_ratio,
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
                health = _worker_health_payload(db_path, stale_threshold=stale_threshold)
                self._send_json(
                    {
                        "run": {
                            "run_id": row[0],
                            "started_at": row[1],
                            "finished_at": row[2],
                            "state": row[3],
                            "summary": row[4],
                            "stale_seconds": health["heartbeat_age_seconds"],
                            "is_stale": health["status"] == "STALE",
                            "last_event_ts": health["last_event_ts"],
                        }
                    }
                )
                return

            if parsed.path.startswith("/api/runs/") and parsed.path.endswith("/summary"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                run_id = parts[2]
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, started_at, finished_at, state, summary
                    FROM runs
                    WHERE run_id = ?
                    LIMIT 1
                    """,
                    [run_id],
                )
                if not rows:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                state_rows = _query(
                    db_path,
                    """
                    SELECT status, COUNT(*)
                    FROM jobs
                    WHERE run_id = ?
                    GROUP BY status
                    ORDER BY status
                    """,
                    [run_id],
                )
                event_rows = _query(
                    db_path,
                    """
                    SELECT level, stage, message, ts
                    FROM events
                    WHERE run_id = ?
                    ORDER BY ts DESC
                    LIMIT 20
                    """,
                    [run_id],
                )
                self._send_json(
                    {
                        "run": {
                            "run_id": rows[0][0],
                            "started_at": rows[0][1],
                            "finished_at": rows[0][2],
                            "state": rows[0][3],
                            "summary": rows[0][4],
                            "status_counts": [
                                {"status": status, "count": int(count)} for status, count in state_rows
                            ],
                            "recent_events": [
                                {"level": e[0], "stage": e[1], "message": e[2], "ts": e[3]} for e in event_rows
                            ],
                        }
                    }
                )
                return

            if parsed.path.startswith("/api/runs/") and parsed.path.endswith("/jobs"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                run_id = parts[2]
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, job_id, account_id, status, input_path, output_path, updated_at
                    FROM jobs
                    WHERE run_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1000
                    """,
                    [run_id],
                )
                self._send_json(
                    {
                        "jobs": [
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
    .wrap { max-width: 1400px; margin: 0 auto; padding: 20px; }
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
    .badge { display: inline-block; padding: 4px 8px; border-radius: 6px; font-size: 12px; font-weight: 700; }
    .badge-HEALTHY { background: #064e3b; color: #6ee7b7; }
    .badge-DEGRADED { background: #78350f; color: #fcd34d; }
    .badge-STALE { background: #7f1d1d; color: #fca5a5; }
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

    <div class="card" style="margin-top:12px;">
      <div class="k">Worker Health</div>
      <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
        <span id="health-badge" class="badge">-</span>
        <span class="muted">run: <code id="health-run">-</code></span>
        <span class="muted">last heartbeat age: <code id="health-age">-</code>s</span>
        <span class="muted">last event age: <code id="event-age">-</code>s</span>
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="k">Total</div><div class="v" id="m-total">-</div></div>
      <div class="card"><div class="k">Active</div><div class="v" id="m-active">-</div></div>
      <div class="card"><div class="k">Queued</div><div class="v" id="m-queue">-</div></div>
      <div class="card"><div class="k">Succeeded</div><div class="v" id="m-succ">-</div></div>
      <div class="card"><div class="k">Failed</div><div class="v" id="m-fail">-</div></div>
      <div class="card"><div class="k">Delete-Quarantined</div><div class="v" id="m-delq">-</div></div>
      <div class="card"><div class="k">Latest Run</div><div class="v" id="run-id" style="font-size:16px;">-</div></div>
    </div>

    <div class="row">
      <table>
        <thead><tr><th>job_id</th><th>account</th><th>status</th><th>input</th><th>output</th><th>updated_at</th></tr></thead>
        <tbody id="jobs-body"></tbody>
      </table>
    </div>

    <div class="row">
      <table>
        <thead><tr><th>ts</th><th>run</th><th>job</th><th>level</th><th>stage</th><th>message</th></tr></thead>
        <tbody id="events-body"></tbody>
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
        const [health, m, latest, jobs, ev, fl] = await Promise.all([
          fetchJson('/api/worker/health'),
          fetchJson('/api/metrics/throughput'),
          fetchJson('/api/runs/latest'),
          fetchJson('/api/jobs?limit=200'),
          fetchJson('/api/events/recent'),
          fetchJson('/api/file-lifecycle/summary'),
        ]);

        const badge = document.getElementById('health-badge');
        badge.textContent = health.status;
        badge.className = 'badge badge-' + health.status;
        document.getElementById('health-run').textContent = health.run_id || '-';
        document.getElementById('health-age').textContent = health.heartbeat_age_seconds ?? '-';
        document.getElementById('event-age').textContent = health.last_event_age_seconds ?? '-';

        document.getElementById('m-total').textContent = m.metrics.total_jobs;
        document.getElementById('m-active').textContent = m.metrics.active_jobs;
        document.getElementById('m-queue').textContent = m.metrics.queue_jobs;
        document.getElementById('m-succ').textContent = m.metrics.succeeded_jobs;
        document.getElementById('m-fail').textContent = m.metrics.failed_jobs;
        const dq = (fl.file_lifecycle || []).find(x => x.state === 'DELETE_QUARANTINED');
        document.getElementById('m-delq').textContent = dq ? dq.count : 0;
        document.getElementById('run-id').textContent = latest.run ? latest.run.run_id : '-';

        const jobsBody = document.getElementById('jobs-body');
        jobsBody.innerHTML = '';
        for (const job of (jobs.jobs || [])) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><a href="/api/jobs/${encodeURIComponent(job.job_id)}/timeline" target="_blank">${esc(job.job_id)}</a></td>
            <td>${esc(job.account_id)}</td>
            <td class="status-${esc(job.status)}">${esc(job.status)}</td>
            <td>${esc(job.input_path)}</td>
            <td>${esc(job.output_path)}</td>
            <td>${esc(job.updated_at)}</td>
          `;
          jobsBody.appendChild(tr);
        }

        const eventsBody = document.getElementById('events-body');
        eventsBody.innerHTML = '';
        for (const e of (ev.events || []).slice(0, 20)) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${esc(e.ts)}</td>
            <td>${esc(e.run_id)}</td>
            <td>${esc(e.job_id)}</td>
            <td>${esc(e.level)}</td>
            <td>${esc(e.stage)}</td>
            <td>${esc(e.message)}</td>
          `;
          eventsBody.appendChild(tr);
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
