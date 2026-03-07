from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

from peetsfea_runner.state_store import _GLOBAL_DUCKDB_LOCK


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        return default
    return max(minimum, value)


def _query(db_path: Path, sql: str, params: list[object] | None = None) -> list[tuple]:
    # Serialize DuckDB access across the embedded web server and the worker
    # loop to avoid in-process connection configuration conflicts.
    with _GLOBAL_DUCKDB_LOCK:
        conn = duckdb.connect(str(db_path))
        try:
            return conn.execute(sql, params or []).fetchall()
        finally:
            conn.close()


def _latest_run_id(db_path: Path) -> str | None:
    rows = _query(
        db_path,
        """
        SELECT run_id
        FROM runs
        ORDER BY started_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        return None
    value = rows[0][0]
    return str(value) if value is not None else None


def _first_str_param(params: dict[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    if not value:
        return None
    return value


def _first_int_param(params: dict[str, list[str]], key: str, default: int, minimum: int = 1) -> int:
    raw = _first_str_param(params, key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value < minimum:
        return minimum
    return value


def _window_state_counts(db_path: Path, *, run_id: str | None = None) -> dict[str, int]:
    if run_id is None:
        rows = _query(
            db_path,
            """
            SELECT state, COUNT(*)
            FROM window_tasks
            GROUP BY state
            """,
        )
    else:
        rows = _query(
            db_path,
            """
            SELECT state, COUNT(*)
            FROM window_tasks
            WHERE run_id = ?
            GROUP BY state
            """,
            [run_id],
        )
    return {str(row[0]): int(row[1]) for row in rows}


def _account_window_scores(db_path: Path, *, run_id: str | None) -> list[dict[str, object]]:
    if run_id is None:
        return []
    rows = _query(
        db_path,
        """
        SELECT
            account_id,
            SUM(CASE WHEN state IN ('SUCCEEDED', 'FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS completed_windows,
            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END) AS inflight_windows
        FROM window_tasks
        WHERE run_id = ? AND account_id IS NOT NULL
        GROUP BY account_id
        ORDER BY account_id
        """,
        [run_id],
    )
    scores: list[dict[str, object]] = []
    for row in rows:
        completed = int(row[1] or 0)
        inflight = int(row[2] or 0)
        scores.append(
            {
                "account_id": row[0],
                "completed_windows": completed,
                "inflight_windows": inflight,
                "score": completed + inflight,
            }
        )
    return scores


def _account_window_live_stats(db_path: Path, *, run_id: str | None) -> dict[str, dict[str, int]]:
    if run_id is None:
        return {}
    rows = _query(
        db_path,
        """
        SELECT
            account_id,
            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END) AS active_slots,
            SUM(CASE WHEN state IN ('SUCCEEDED', 'FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS completed_slots,
            SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_slots,
            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_slots,
            SUM(CASE WHEN state = 'QUARANTINED' THEN 1 ELSE 0 END) AS quarantined_slots
        FROM window_tasks
        WHERE run_id = ? AND account_id IS NOT NULL
        GROUP BY account_id
        ORDER BY account_id
        """,
        [run_id],
    )
    stats: dict[str, dict[str, int]] = {}
    for row in rows:
        stats[str(row[0])] = {
            "active_slots": int(row[1] or 0),
            "completed_slots": int(row[2] or 0),
            "succeeded_slots": int(row[3] or 0),
            "failed_slots": int(row[4] or 0),
            "quarantined_windows": int(row[5] or 0),
        }
    return stats


def _configured_account_worker_targets() -> dict[str, int]:
    raw_accounts = os.getenv("PEETSFEA_ACCOUNTS", "").strip()
    default_max_jobs = _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)
    targets: dict[str, int] = {}
    if not raw_accounts:
        return targets
    for chunk in raw_accounts.split(","):
        entry = chunk.strip()
        if not entry:
            continue
        account_part = entry
        max_jobs = default_max_jobs
        if ":" in entry:
            account_part, raw_max_jobs = entry.rsplit(":", 1)
            try:
                max_jobs = max(1, int(raw_max_jobs.strip()))
            except ValueError:
                max_jobs = default_max_jobs
        account_id = account_part.split("@", 1)[0].strip()
        if account_id:
            targets[account_id] = max_jobs
    return targets


def _latest_account_readiness(db_path: Path) -> dict[str, dict[str, object]]:
    rows = _query(
        db_path,
        """
        SELECT
            account_id,
            host,
            ready,
            status,
            reason,
            home_ok,
            runtime_path_ok,
            venv_ok,
            python_ok,
            module_ok,
            binaries_ok,
            ansys_ok,
            uv_ok,
            pyaedt_ok,
            ts
        FROM (
            SELECT
                account_id,
                host,
                ready,
                status,
                reason,
                home_ok,
                runtime_path_ok,
                venv_ok,
                python_ok,
                module_ok,
                binaries_ok,
                ansys_ok,
                uv_ok,
                pyaedt_ok,
                ts,
                ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ts DESC) AS rn
            FROM account_readiness_snapshots
        ) ranked
        WHERE rn = 1
        ORDER BY account_id
        """,
    )
    readiness: dict[str, dict[str, object]] = {}
    for row in rows:
        readiness[str(row[0])] = {
            "account_id": row[0],
            "host": row[1],
            "ready": bool(row[2]),
            "status": row[3],
            "reason": row[4],
            "checks": {
                "home_ok": bool(row[5]),
                "runtime_path_ok": bool(row[6]),
                "venv_ok": bool(row[7]),
                "python_ok": bool(row[8]),
                "module_ok": bool(row[9]),
                "binaries_ok": bool(row[10]),
                "ansys_ok": bool(row[11]),
                "uv_ok": bool(row[12]),
                "pyaedt_ok": bool(row[13]),
            },
            "ts": row[14],
        }
    return readiness


def _configured_capacity_targets() -> dict[str, int]:
    windows_per_job = _env_int("PEETSFEA_WINDOWS_PER_JOB", 4)
    raw_accounts = os.getenv("PEETSFEA_ACCOUNTS", "").strip()
    configured_accounts = 0
    configured_worker_jobs = 0
    if raw_accounts:
        for chunk in raw_accounts.split(","):
            entry = chunk.strip()
            if not entry:
                continue
            configured_accounts += 1
            max_jobs = 10
            if ":" in entry:
                try:
                    max_jobs = int(entry.rsplit(":", 1)[1].strip())
                except ValueError:
                    max_jobs = 10
            configured_worker_jobs += max(1, max_jobs)
    else:
        configured_accounts = 1
        configured_worker_jobs = _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)

    scaling_accounts = _env_int("PEETSFEA_SCALING_TARGET_ACCOUNTS", 5)
    scaling_jobs_per_account = _env_int("PEETSFEA_SCALING_TARGET_MAX_JOBS_PER_ACCOUNT", 10)
    return {
        "configured_accounts": configured_accounts,
        "configured_worker_jobs": configured_worker_jobs,
        "windows_per_job": windows_per_job,
        "configured_target_slots": configured_worker_jobs * windows_per_job,
        "scaling_accounts": scaling_accounts,
        "scaling_worker_jobs": scaling_accounts * scaling_jobs_per_account,
        "expansion_target_slots": scaling_accounts * scaling_jobs_per_account * windows_per_job,
    }


def _throughput_kpi_payload(
    *,
    queued_windows: int,
    active_windows: int,
    active_workers: int,
    pending_workers: int,
) -> dict[str, object]:
    targets = _configured_capacity_targets()
    demand_slots = queued_windows + active_windows
    configured_target_slots = int(targets["configured_target_slots"])
    configured_worker_jobs = int(targets["configured_worker_jobs"])
    effective_target_slots = min(configured_target_slots, demand_slots) if demand_slots > 0 else 0
    active_slot_shortfall = max(effective_target_slots - active_windows, 0)
    worker_shortfall = max(configured_worker_jobs - active_workers, 0)
    scheduled_worker_shortfall = max(configured_worker_jobs - (active_workers + pending_workers), 0)
    input_limited = demand_slots < configured_target_slots
    if demand_slots == 0:
        throughput_mode = "IDLE"
    elif active_slot_shortfall == 0 and input_limited:
        throughput_mode = "INPUT_LIMITED"
    elif active_slot_shortfall == 0:
        throughput_mode = "AT_CAPACITY"
    elif input_limited:
        throughput_mode = "INPUT_LIMITED"
    else:
        throughput_mode = "CAPACITY_SHORTFALL"

    return {
        **targets,
        "demand_slots": demand_slots,
        "effective_target_slots": effective_target_slots,
        "active_slot_shortfall": active_slot_shortfall,
        "active_workers": active_workers,
        "pending_workers": pending_workers,
        "worker_shortfall": worker_shortfall,
        "scheduled_worker_shortfall": scheduled_worker_shortfall,
        "recovery_backlog_windows": queued_windows,
        "input_limited": input_limited,
        "capacity_limited": (not input_limited) and active_slot_shortfall > 0,
        "throughput_mode": throughput_mode,
        "configured_slot_utilization": (float(active_windows) / configured_target_slots) if configured_target_slots else 0.0,
        "effective_slot_utilization": (float(active_windows) / effective_target_slots) if effective_target_slots else 0.0,
    }


def _account_live_status(
    *,
    readiness_ready: object,
    running_count: int,
    pending_count: int,
    target_workers: int,
) -> str:
    if readiness_ready is False:
        return "BLOCKED"
    if running_count >= target_workers:
        return "AT_TARGET"
    if (running_count + pending_count) >= target_workers:
        return "FILLING"
    if running_count > 0 or pending_count > 0:
        return "UNDER_CAPACITY"
    return "IDLE"


_ACCOUNT_RE = re.compile(r"account=(account_[0-9]+)")


def _extract_account_id(*, entity_id: str | None, message: str, source: str) -> str | None:
    if source == "JOB" and entity_id and entity_id.startswith("job_"):
        return None
    match = _ACCOUNT_RE.search(message)
    if match:
        return match.group(1)
    return None


def _classify_ops_event(*, stage: str, level: str, message: str, account_id: str | None) -> dict[str, object]:
    normalized_stage = stage.upper()
    normalized_level = level.upper()
    category = "info"
    alertable = False
    severity = normalized_level

    if normalized_stage in {"WORKER_LOOP_BLOCKED"} or "READINESS" in normalized_stage:
        category = "readiness"
        alertable = True
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"WORKER_LOOP_RECOVERING"} or "RECOVER" in normalized_stage:
        category = "recovery"
        alertable = True
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"QUARANTINED", "DELETE_QUARANTINED"}:
        category = "quarantine"
        alertable = True
        severity = "ERROR" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"WORKER_LOOP_ERROR"} or normalized_level == "ERROR":
        category = "failure"
        alertable = True
        severity = "ERROR"
    elif normalized_stage.startswith("FAILURE_") or "COLLECT" in normalized_stage or "CLEANUP" in normalized_stage:
        category = "failure"
        alertable = True
        severity = "ERROR" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"ACCOUNT_BOOTSTRAP_START", "ACCOUNT_BOOTSTRAP_OK"}:
        category = "bootstrap"
        alertable = False
    elif normalized_stage in {"DELETE_PENDING", "DELETE_RETAINED", "INPUT_DELETED"}:
        category = "lifecycle"
        alertable = normalized_stage == "DELETE_QUARANTINED"
    common_key = f"{category}:{account_id or 'global'}:{normalized_stage}"
    return {
        "category": category,
        "alertable": alertable,
        "severity": severity,
        "common_key": common_key,
    }


def _alertable_event_summary(
    *,
    events: list[dict[str, object]],
    throughput_kpi: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    alerts: list[dict[str, object]] = []
    if throughput_kpi and bool(throughput_kpi.get("capacity_limited")):
        alerts.append(
            {
                "alert_key": "CAPACITY_SHORTFALL",
                "severity": "WARN",
                "category": "capacity",
                "account_id": None,
                "message": (
                    f"worker_gap={throughput_kpi.get('scheduled_worker_shortfall', 0)} "
                    f"slot_shortfall={throughput_kpi.get('active_slot_shortfall', 0)} "
                    f"mode={throughput_kpi.get('throughput_mode')}"
                ),
            }
        )

    readiness_accounts = sorted(
        {
            str(event["account_id"])
            for event in events
            if event.get("category") == "readiness" and event.get("account_id")
        }
    )
    for account_id in readiness_accounts:
        alerts.append(
            {
                "alert_key": "READINESS_BLOCKED",
                "severity": "WARN",
                "category": "readiness",
                "account_id": account_id,
                "message": f"readiness blocked for {account_id}",
            }
        )

    quarantine_counts: dict[str, int] = {}
    for event in events:
        if event.get("category") != "quarantine":
            continue
        account_id = str(event.get("account_id") or "global")
        quarantine_counts[account_id] = quarantine_counts.get(account_id, 0) + 1
    for account_id, count in sorted(quarantine_counts.items()):
        if count < 2:
            continue
        alerts.append(
            {
                "alert_key": "QUARANTINE_BURST",
                "severity": "ERROR",
                "category": "quarantine",
                "account_id": None if account_id == "global" else account_id,
                "message": f"recent quarantine burst count={count}",
            }
        )

    return alerts


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
        FROM (
            SELECT ts FROM events
            UNION ALL
            SELECT ts FROM window_events
        ) AS all_events
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
            "worker_status": None,
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
    worker_status = row[5]
    heartbeat_age = _age_seconds(row[3])
    is_stale = heartbeat_age is None or heartbeat_age > stale_threshold
    if is_stale:
        status = "STALE"
        reason = "old heartbeat"
    elif worker_status == "DEGRADED":
        status = "DEGRADED"
        reason = "recent errors"
    elif worker_status == "BLOCKED":
        status = "HEALTHY"
        reason = "autorecovery blocked by readiness"
    elif worker_status == "RECOVERING":
        status = "HEALTHY"
        reason = "autorecovery active"
    else:
        status = "HEALTHY"
        reason = "ok"

    return {
        "status": status,
        "reason": reason,
        "worker_status": worker_status,
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
                            "/api/windows",
                            "/api/windows/{id}/timeline",
                            "/api/accounts/capacity",
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

            if parsed.path == "/api/windows":
                requested_run_id = _first_str_param(params, "run_id")
                run_id = requested_run_id or _latest_run_id(db_path)
                state_filter = _first_str_param(params, "status")
                limit = _first_int_param(params, "limit", default=500, minimum=1)
                if run_id is None:
                    self._send_json({"run_id": None, "windows": []})
                    return

                sql = """
                    SELECT run_id, window_id, job_id, account_id, input_path, output_path, state, attempt_no, updated_at
                    FROM window_tasks
                    WHERE run_id = ?
                """
                sql_params: list[object] = [run_id]
                if state_filter is not None:
                    sql += " AND state = ?"
                    sql_params.append(state_filter)
                sql += " ORDER BY updated_at DESC LIMIT ?"
                sql_params.append(limit)
                rows = _query(db_path, sql, sql_params)
                self._send_json(
                    {
                        "run_id": run_id,
                        "windows": [
                            {
                                "window_id": row[1],
                                "run_id": row[0],
                                "job_id": row[2],
                                "account_id": row[3],
                                "input_path": row[4],
                                "output_path": row[5],
                                "state": row[6],
                                "attempt_no": int(row[7] or 0),
                                "updated_at": row[8],
                            }
                            for row in rows
                        ],
                    }
                )
                return

            if parsed.path.startswith("/api/windows/") and parsed.path.endswith("/timeline"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                window_id = parts[2]
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, window_id, job_id, account_id, input_path, output_path, state, attempt_no, failure_reason, created_at, updated_at
                    FROM window_tasks
                    WHERE window_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    [window_id],
                )
                if not rows:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                row = rows[0]
                events = _query(
                    db_path,
                    """
                    SELECT level, stage, message, ts
                    FROM window_events
                    WHERE run_id = ? AND window_id = ?
                    ORDER BY ts DESC
                    LIMIT 300
                    """,
                    [row[0], row[1]],
                )
                lifecycle = _query(
                    db_path,
                    """
                    SELECT input_deleted_at, delete_retry_count, delete_final_state, quarantine_path, updated_at
                    FROM file_lifecycle
                    WHERE run_id = ? AND window_id = ?
                    LIMIT 1
                    """,
                    [row[0], row[1]],
                )
                attempt_summary_rows: list[tuple] = []
                if row[2] is not None:
                    attempt_summary_rows = _query(
                        db_path,
                        """
                        SELECT COUNT(*), MAX(attempt_no), MAX(node), MAX(ended_at)
                        FROM attempts
                        WHERE run_id = ? AND job_id = ?
                        """,
                        [row[0], row[2]],
                    )
                attempt_count = int(attempt_summary_rows[0][0] or 0) if attempt_summary_rows else 0
                latest_job_attempt = int(attempt_summary_rows[0][1] or 0) if attempt_summary_rows else 0
                latest_node = attempt_summary_rows[0][2] if attempt_summary_rows else None
                latest_attempt_ended_at = attempt_summary_rows[0][3] if attempt_summary_rows else None
                self._send_json(
                    {
                        "window_task": {
                            "run_id": row[0],
                            "window_id": row[1],
                            "job_id": row[2],
                            "account_id": row[3],
                            "input_path": row[4],
                            "output_path": row[5],
                            "state": row[6],
                            "attempt_no": int(row[7] or 0),
                            "failure_reason": row[8],
                            "created_at": row[9],
                            "updated_at": row[10],
                        },
                        "events": [{"level": e[0], "stage": e[1], "message": e[2], "ts": e[3]} for e in events],
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
                        "attempt_summary": {
                            "window_attempt_no": int(row[7] or 0),
                            "job_attempt_count": attempt_count,
                            "latest_job_attempt": latest_job_attempt,
                            "latest_job_node": latest_node,
                            "latest_job_attempt_ended_at": latest_attempt_ended_at,
                        },
                    }
                )
                return

            if parsed.path == "/api/accounts/capacity":
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                score_map = {item["account_id"]: item for item in _account_window_scores(db_path, run_id=run_id)}
                live_window_map = _account_window_live_stats(db_path, run_id=run_id)
                target_worker_map = _configured_account_worker_targets()
                windows_per_job = int(_configured_capacity_targets()["windows_per_job"])
                capacity_rows = _query(
                    db_path,
                    """
                    SELECT account_id, host, running_count, pending_count, allowed_submit, ts
                    FROM (
                        SELECT
                            account_id,
                            host,
                            running_count,
                            pending_count,
                            allowed_submit,
                            ts,
                            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ts DESC) AS rn
                        FROM account_capacity_snapshots
                    ) ranked
                    WHERE rn = 1
                    ORDER BY account_id
                    """,
                )
                readiness_map = _latest_account_readiness(db_path)
                capacity_map = {
                    str(row[0]): {
                        "account_id": row[0],
                        "host": row[1],
                        "running_count": int(row[2] or 0),
                        "pending_count": int(row[3] or 0),
                        "allowed_submit": int(row[4] or 0),
                        "ts": row[5],
                    }
                    for row in capacity_rows
                }
                account_ids = sorted(set(capacity_map) | set(readiness_map))
                self._send_json(
                    {
                        "run_id": run_id,
                        "accounts": [
                            {
                                "account_id": account_id,
                                "host": str(
                                    capacity_map.get(account_id, {}).get("host")
                                    or readiness_map.get(account_id, {}).get("host")
                                    or ""
                                ),
                                "running_count": int(capacity_map.get(account_id, {}).get("running_count", 0)),
                                "pending_count": int(capacity_map.get(account_id, {}).get("pending_count", 0)),
                                "allowed_submit": int(capacity_map.get(account_id, {}).get("allowed_submit", 0)),
                                "score": int(score_map.get(account_id, {}).get("score", 0)),
                                "configured_worker_jobs": int(target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10))),
                                "configured_target_slots": int(
                                    target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10))
                                )
                                * windows_per_job,
                                "active_slots": int(live_window_map.get(account_id, {}).get("active_slots", 0)),
                                "completed_slots": int(live_window_map.get(account_id, {}).get("completed_slots", 0)),
                                "succeeded_slots": int(live_window_map.get(account_id, {}).get("succeeded_slots", 0)),
                                "failed_slots": int(live_window_map.get(account_id, {}).get("failed_slots", 0)),
                                "quarantined_windows": int(
                                    live_window_map.get(account_id, {}).get("quarantined_windows", 0)
                                ),
                                "active_worker_shortfall": max(
                                    int(target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)))
                                    - int(capacity_map.get(account_id, {}).get("running_count", 0)),
                                    0,
                                ),
                                "scheduled_worker_shortfall": max(
                                    int(target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)))
                                    - (
                                        int(capacity_map.get(account_id, {}).get("running_count", 0))
                                        + int(capacity_map.get(account_id, {}).get("pending_count", 0))
                                    ),
                                    0,
                                ),
                                "active_slot_shortfall": max(
                                    int(target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)))
                                    * windows_per_job
                                    - int(live_window_map.get(account_id, {}).get("active_slots", 0)),
                                    0,
                                ),
                                "live_status": _account_live_status(
                                    readiness_ready=readiness_map.get(account_id, {}).get("ready"),
                                    running_count=int(capacity_map.get(account_id, {}).get("running_count", 0)),
                                    pending_count=int(capacity_map.get(account_id, {}).get("pending_count", 0)),
                                    target_workers=int(
                                        target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10))
                                    ),
                                ),
                                "readiness_ready": readiness_map.get(account_id, {}).get("ready"),
                                "readiness_status": readiness_map.get(account_id, {}).get("status"),
                                "readiness_reason": readiness_map.get(account_id, {}).get("reason"),
                                "readiness_checks": readiness_map.get(account_id, {}).get("checks"),
                                "ts": capacity_map.get(account_id, {}).get("ts")
                                or readiness_map.get(account_id, {}).get("ts"),
                            }
                            for account_id in account_ids
                        ],
                    }
                )
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
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                total_jobs = 0
                succeeded_jobs = 0
                failed_jobs = 0
                active_jobs = 0
                queue_jobs = 0
                if run_id is not None:
                    job_rows = _query(
                        db_path,
                        """
                        SELECT
                            COUNT(*) AS total_jobs,
                            SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_jobs,
                            SUM(CASE WHEN status IN ('FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS failed_jobs,
                            SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) AS active_jobs,
                            SUM(CASE WHEN status IN ('PENDING', 'SUBMITTED') THEN 1 ELSE 0 END) AS queue_jobs
                        FROM jobs
                        WHERE run_id = ?
                        """,
                        [run_id],
                    )
                    total, succeeded, failed, active_jobs_raw, queue_jobs_raw = job_rows[0]
                    total_jobs = int(total or 0)
                    succeeded_jobs = int(succeeded or 0)
                    failed_jobs = int(failed or 0)
                    active_jobs = int(active_jobs_raw or 0)
                    queue_jobs = int(queue_jobs_raw or 0)
                failed_ratio = float(failed_jobs / total_jobs) if total_jobs > 0 else 0.0

                total_windows = 0
                queued_windows = 0
                active_windows = 0
                succeeded_windows = 0
                failed_windows = 0
                quarantined_windows = 0
                delete_quarantined_windows = 0
                if run_id is not None:
                    window_rows = _query(
                        db_path,
                        """
                        SELECT
                            COUNT(*) AS total_windows,
                            SUM(CASE WHEN state IN ('QUEUED', 'RETRY_QUEUED') THEN 1 ELSE 0 END) AS queued_windows,
                            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END) AS active_windows,
                            SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_windows,
                            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_windows,
                            SUM(CASE WHEN state = 'QUARANTINED' THEN 1 ELSE 0 END) AS quarantined_windows
                        FROM window_tasks
                        WHERE run_id = ?
                        """,
                        [run_id],
                    )
                    (
                        total_windows,
                        queued_windows,
                        active_windows,
                        succeeded_windows,
                        failed_windows,
                        quarantined_windows,
                    ) = (int(item or 0) for item in window_rows[0])
                    delete_rows = _query(
                        db_path,
                        """
                        SELECT COUNT(*)
                        FROM file_lifecycle
                        WHERE run_id = ? AND delete_final_state = 'DELETE_QUARANTINED'
                        """,
                        [run_id],
                    )
                    delete_quarantined_windows = int(delete_rows[0][0] or 0)
                throughput_kpi = _throughput_kpi_payload(
                    queued_windows=queued_windows,
                    active_windows=active_windows,
                    active_workers=active_jobs,
                    pending_workers=queue_jobs,
                )

                self._send_json(
                    {
                        "metrics": {
                            "run_id": run_id,
                            "total_jobs": total_jobs,
                            "succeeded_jobs": succeeded_jobs,
                            "failed_jobs": failed_jobs,
                            "active_jobs": active_jobs,
                            "queue_jobs": queue_jobs,
                            "failed_ratio": failed_ratio,
                            "total_windows": total_windows,
                            "queued_windows": queued_windows,
                            "active_windows": active_windows,
                            "succeeded_windows": succeeded_windows,
                            "failed_windows": failed_windows,
                            "quarantined_windows": quarantined_windows,
                            "delete_quarantined_windows": delete_quarantined_windows,
                            "throughput_kpi": throughput_kpi,
                            "account_window_scores": _account_window_scores(db_path, run_id=run_id),
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
                window_state_rows = _query(
                    db_path,
                    """
                    SELECT state, COUNT(*)
                    FROM window_tasks
                    WHERE run_id = ?
                    GROUP BY state
                    ORDER BY state
                    """,
                    [run_id],
                )
                event_rows = _query(
                    db_path,
                    """
                    SELECT level, stage, message, ts, source
                    FROM (
                        SELECT level, stage, message, ts, 'JOB' AS source
                        FROM events
                        WHERE run_id = ?
                        UNION ALL
                        SELECT level, stage, message, ts, 'WINDOW' AS source
                        FROM window_events
                        WHERE run_id = ?
                    ) merged
                    ORDER BY ts DESC
                    LIMIT 20
                    """,
                    [run_id, run_id],
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
                            "window_state_counts": [
                                {"state": state, "count": int(count)} for state, count in window_state_rows
                            ],
                            "recent_events": [
                                {"level": e[0], "stage": e[1], "message": e[2], "ts": e[3], "source": e[4]}
                                for e in event_rows
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
                limit = _first_int_param(params, "limit", default=200, minimum=1)
                scope_run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                query = """
                    SELECT run_id, entity_id, level, stage, message, ts, source, account_id
                    FROM (
                        SELECT e.run_id, e.job_id AS entity_id, e.level, e.stage, e.message, e.ts, 'JOB' AS source, j.account_id
                        FROM events
                        AS e
                        LEFT JOIN jobs AS j
                        ON e.run_id = j.run_id AND e.job_id = j.job_id
                        UNION ALL
                        SELECT w.run_id, w.window_id AS entity_id, w.level, w.stage, w.message, w.ts, 'WINDOW' AS source, wt.account_id
                        FROM window_events
                        AS w
                        LEFT JOIN window_tasks AS wt
                        ON w.run_id = wt.run_id AND w.window_id = wt.window_id
                    ) AS merged
                """
                query_params: list[object] = []
                if scope_run_id is not None:
                    query += " WHERE run_id = ?"
                    query_params.append(scope_run_id)
                query += " ORDER BY ts DESC LIMIT ?"
                query_params.append(limit)
                rows = _query(
                    db_path,
                    query,
                    query_params,
                )
                throughput_kpi: dict[str, object] | None = None
                if scope_run_id is not None:
                    window_rows = _query(
                        db_path,
                        """
                        SELECT
                            SUM(CASE WHEN state IN ('QUEUED', 'RETRY_QUEUED') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END)
                        FROM window_tasks
                        WHERE run_id = ?
                        """,
                        [scope_run_id],
                    )
                    job_rows = _query(
                        db_path,
                        """
                        SELECT
                            SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status IN ('PENDING', 'SUBMITTED') THEN 1 ELSE 0 END)
                        FROM jobs
                        WHERE run_id = ?
                        """,
                        [scope_run_id],
                    )
                    queued_windows = int(window_rows[0][0] or 0)
                    active_windows = int(window_rows[0][1] or 0)
                    active_workers = int(job_rows[0][0] or 0)
                    pending_workers = int(job_rows[0][1] or 0)
                    throughput_kpi = _throughput_kpi_payload(
                        queued_windows=queued_windows,
                        active_windows=active_windows,
                        active_workers=active_workers,
                        pending_workers=pending_workers,
                    )

                events: list[dict[str, object]] = []
                for row in rows:
                    account_id = row[7] or _extract_account_id(
                        entity_id=row[1],
                        message=str(row[4] or ""),
                        source=str(row[6]),
                    )
                    classification = _classify_ops_event(
                        stage=str(row[3] or ""),
                        level=str(row[2] or ""),
                        message=str(row[4] or ""),
                        account_id=str(account_id) if account_id is not None else None,
                    )
                    events.append(
                        {
                            "run_id": row[0],
                            "entity_id": row[1],
                            "job_id": row[1] if row[6] == "JOB" else None,
                            "window_id": row[1] if row[6] == "WINDOW" else None,
                            "source": row[6],
                            "account_id": account_id,
                            "level": row[2],
                            "stage": row[3],
                            "message": row[4],
                            "ts": row[5],
                            **classification,
                        }
                    )
                self._send_json(
                    {
                        "run_id": scope_run_id,
                        "events": events,
                        "throughput_kpi": throughput_kpi,
                        "alerts": _alertable_event_summary(events=events, throughput_kpi=throughput_kpi),
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


def start_status_server(*, db_path: str, host: str = "127.0.0.1", port: int = 8765) -> HTTPServer:
    handler = make_status_handler(db_path=Path(db_path).expanduser().resolve())
    # Keep the status API single-threaded to avoid DuckDB file-handle conflicts
    # from concurrent dashboard polling requests.
    server = HTTPServer((host, port), handler)
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
    .wrap { max-width: 1500px; margin: 0 auto; padding: 20px; }
    h1 { margin: 0 0 8px; font-size: 24px; }
    .muted { color: #9ca3af; font-size: 12px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin: 14px 0; }
    .card { background: #111a2e; border: 1px solid #25324a; border-radius: 10px; padding: 12px; }
    .k { color: #9ca3af; font-size: 12px; margin-bottom: 4px; }
    .v { font-size: 22px; font-weight: 700; }
    .badge { display: inline-block; padding: 4px 8px; border-radius: 6px; font-size: 12px; font-weight: 700; }
    .badge-HEALTHY { background: #064e3b; color: #6ee7b7; }
    .badge-DEGRADED { background: #78350f; color: #fcd34d; }
    .badge-STALE { background: #7f1d1d; color: #fca5a5; }
    table { width: 100%; border-collapse: collapse; background: #111a2e; border: 1px solid #25324a; border-radius: 10px; overflow: hidden; }
    th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #25324a; font-size: 12px; vertical-align: top; }
    th { background: #0f172a; color: #cbd5e1; }
    .row { margin-top: 14px; }
    .two { display: grid; grid-template-columns: 2fr 1fr; gap: 12px; }
    @media (max-width: 980px) { .two { grid-template-columns: 1fr; } }
    .state-QUEUED, .state-RETRY_QUEUED { color: #93c5fd; }
    .state-RUNNING, .state-COLLECTING, .state-UPLOADING, .state-ASSIGNED { color: #fbbf24; }
    .state-SUCCEEDED { color: #34d399; }
    .state-FAILED, .state-QUARANTINED { color: #f87171; }
    code { color: #93c5fd; }
    a { color: #93c5fd; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Peets FEA Status Dashboard</h1>
    <div class="muted">Window(.aedt) 기준 대시보드. 5초마다 갱신. API: <code>/api</code></div>

    <div class="card" style="margin-top:12px;">
      <div class="k">Worker Health</div>
      <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
        <span id="health-badge" class="badge">-</span>
        <span class="muted">worker state: <code id="health-worker-state">-</code></span>
        <span class="muted">run: <code id="health-run">-</code></span>
        <span class="muted">heartbeat age: <code id="health-age">-</code>s</span>
        <span class="muted">last event age: <code id="event-age">-</code>s</span>
        <span class="muted">reason: <code id="health-reason">-</code></span>
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="k">Target Workers</div><div class="v" id="worker-target">-</div></div>
      <div class="card"><div class="k">Live Workers</div><div class="v" id="worker-live">-</div></div>
      <div class="card"><div class="k">Pending Workers</div><div class="v" id="worker-pending">-</div></div>
      <div class="card"><div class="k">Worker Gap</div><div class="v" id="worker-gap">-</div></div>
      <div class="card"><div class="k">Target Slots</div><div class="v" id="slot-target">-</div></div>
      <div class="card"><div class="k">Needed Slots</div><div class="v" id="slot-needed">-</div></div>
      <div class="card"><div class="k">Slot Shortfall</div><div class="v" id="slot-shortfall">-</div></div>
      <div class="card"><div class="k">Throughput Mode</div><div class="v" id="slot-mode" style="font-size:15px;">-</div></div>
      <div class="card"><div class="k">Queued Windows</div><div class="v" id="w-queued">-</div></div>
      <div class="card"><div class="k">Active Windows</div><div class="v" id="w-active">-</div></div>
      <div class="card"><div class="k">Succeeded Windows</div><div class="v" id="w-succ">-</div></div>
      <div class="card"><div class="k">Failed Windows</div><div class="v" id="w-fail">-</div></div>
      <div class="card"><div class="k">Quarantined Windows</div><div class="v" id="w-quar">-</div></div>
      <div class="card"><div class="k">Delete Quarantined</div><div class="v" id="w-delq">-</div></div>
      <div class="card"><div class="k">Active Jobs</div><div class="v" id="j-active">-</div></div>
      <div class="card"><div class="k">Queued Jobs</div><div class="v" id="j-queue">-</div></div>
      <div class="card"><div class="k">Latest Run</div><div class="v" id="run-id" style="font-size:15px;">-</div></div>
    </div>

    <div class="two">
      <div class="row">
        <table>
          <thead><tr><th>window_id</th><th>job_id</th><th>account</th><th>state</th><th>attempt</th><th>input</th><th>updated_at</th></tr></thead>
          <tbody id="windows-body"></tbody>
        </table>
      </div>
      <div class="row">
        <table>
          <thead><tr><th>account</th><th>host</th><th>ready</th><th>live</th><th>target</th><th>R</th><th>PD</th><th>allow</th><th>actS</th><th>doneS</th><th>quar</th><th>gapS</th><th>reason</th><th>ts</th></tr></thead>
          <tbody id="accounts-body"></tbody>
        </table>
      </div>
    </div>

    <div class="row">
      <table>
        <thead><tr><th>alert</th><th>severity</th><th>category</th><th>account</th><th>message</th></tr></thead>
        <tbody id="alerts-body"></tbody>
      </table>
    </div>

    <div class="row">
      <table>
        <thead><tr><th>ts</th><th>source</th><th>run</th><th>entity</th><th>level</th><th>stage</th><th>message</th></tr></thead>
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
        const health = await fetchJson('/api/worker/health');
        const metrics = await fetchJson('/api/metrics/throughput');
        const latest = await fetchJson('/api/runs/latest');
        const windows = await fetchJson('/api/windows?limit=300');
        const capacity = await fetchJson('/api/accounts/capacity');
        const events = await fetchJson('/api/events/recent?limit=80');

        const badge = document.getElementById('health-badge');
        badge.textContent = health.status;
        badge.className = 'badge badge-' + health.status;
        document.getElementById('health-worker-state').textContent = health.worker_status || '-';
        document.getElementById('health-run').textContent = health.run_id || '-';
        document.getElementById('health-age').textContent = health.heartbeat_age_seconds ?? '-';
        document.getElementById('event-age').textContent = health.last_event_age_seconds ?? '-';
        document.getElementById('health-reason').textContent = health.reason || '-';

        document.getElementById('worker-target').textContent = metrics.metrics.throughput_kpi.configured_worker_jobs;
        document.getElementById('worker-live').textContent = metrics.metrics.throughput_kpi.active_workers;
        document.getElementById('worker-pending').textContent = metrics.metrics.throughput_kpi.pending_workers;
        document.getElementById('worker-gap').textContent = metrics.metrics.throughput_kpi.scheduled_worker_shortfall;
        document.getElementById('slot-target').textContent = metrics.metrics.throughput_kpi.configured_target_slots;
        document.getElementById('slot-needed').textContent = metrics.metrics.throughput_kpi.effective_target_slots;
        document.getElementById('slot-shortfall').textContent = metrics.metrics.throughput_kpi.active_slot_shortfall;
        document.getElementById('slot-mode').textContent = metrics.metrics.throughput_kpi.throughput_mode;
        document.getElementById('w-queued').textContent = metrics.metrics.queued_windows;
        document.getElementById('w-active').textContent = metrics.metrics.active_windows;
        document.getElementById('w-succ').textContent = metrics.metrics.succeeded_windows;
        document.getElementById('w-fail').textContent = metrics.metrics.failed_windows;
        document.getElementById('w-quar').textContent = metrics.metrics.quarantined_windows;
        document.getElementById('w-delq').textContent = metrics.metrics.delete_quarantined_windows;
        document.getElementById('j-active').textContent = metrics.metrics.active_jobs;
        document.getElementById('j-queue').textContent = metrics.metrics.queue_jobs;
        document.getElementById('run-id').textContent = latest.run ? latest.run.run_id : '-';

        const windowsBody = document.getElementById('windows-body');
        windowsBody.innerHTML = '';
        for (const w of (windows.windows || [])) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><a href="/api/windows/${encodeURIComponent(w.window_id)}/timeline" target="_blank">${esc(w.window_id)}</a></td>
            <td>${esc(w.job_id)}</td>
            <td>${esc(w.account_id)}</td>
            <td class="state-${esc(w.state)}">${esc(w.state)}</td>
            <td>${esc(w.attempt_no)}</td>
            <td>${esc(w.input_path)}</td>
            <td>${esc(w.updated_at)}</td>
          `;
          windowsBody.appendChild(tr);
        }

        const accountsBody = document.getElementById('accounts-body');
        accountsBody.innerHTML = '';
        for (const a of (capacity.accounts || [])) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${esc(a.account_id)}</td>
            <td>${esc(a.host)}</td>
            <td>${esc(a.readiness_status || '-')}</td>
            <td>${esc(a.live_status || '-')}</td>
            <td>${esc(a.configured_worker_jobs)}</td>
            <td>${esc(a.running_count)}</td>
            <td>${esc(a.pending_count)}</td>
            <td>${esc(a.allowed_submit)}</td>
            <td>${esc(a.active_slots)}</td>
            <td>${esc(a.completed_slots)}</td>
            <td>${esc(a.quarantined_windows)}</td>
            <td>${esc(a.active_slot_shortfall)}</td>
            <td>${esc(a.readiness_reason || '-')}</td>
            <td>${esc(a.ts)}</td>
          `;
          accountsBody.appendChild(tr);
        }

        const alertsBody = document.getElementById('alerts-body');
        alertsBody.innerHTML = '';
        for (const a of (events.alerts || [])) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${esc(a.alert_key)}</td>
            <td>${esc(a.severity)}</td>
            <td>${esc(a.category)}</td>
            <td>${esc(a.account_id)}</td>
            <td>${esc(a.message)}</td>
          `;
          alertsBody.appendChild(tr);
        }

        const eventsBody = document.getElementById('events-body');
        eventsBody.innerHTML = '';
        for (const e of (events.events || []).slice(0, 40)) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${esc(e.ts)}</td>
            <td>${esc(e.source)}</td>
            <td>${esc(e.run_id)}</td>
            <td>${esc(e.entity_id)}</td>
            <td>${esc(e.level)}</td>
            <td>${esc(e.stage)}</td>
            <td>${esc(e.message)}</td>
          `;
          eventsBody.appendChild(tr);
        }
      } catch (error) {
        console.error(error);
      }
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
