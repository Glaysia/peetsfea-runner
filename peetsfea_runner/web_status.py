from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

from peetsfea_runner.state_store import StateStore, _GLOBAL_DUCKDB_LOCK
from peetsfea_runner.version import get_version


APP_VERSION = get_version()


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


def _slot_state_counts(db_path: Path, *, run_id: str | None = None) -> dict[str, int]:
    if run_id is None:
        rows = _query(
            db_path,
            """
            SELECT state, COUNT(*)
            FROM slot_tasks
            GROUP BY state
            """,
        )
    else:
        rows = _query(
            db_path,
            """
            SELECT state, COUNT(*)
            FROM slot_tasks
            WHERE run_id = ?
            GROUP BY state
            """,
            [run_id],
        )
    return {str(row[0]): int(row[1]) for row in rows}


def _account_slot_scores(db_path: Path, *, run_id: str | None) -> list[dict[str, object]]:
    if run_id is None:
        return []
    rows = _query(
        db_path,
        """
        SELECT
            account_id,
            SUM(CASE WHEN state IN ('SUCCEEDED', 'FAILED', 'QUARANTINED') THEN 1 ELSE 0 END) AS completed_slots,
            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END) AS inflight_slots
        FROM slot_tasks
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
                "completed_slots": completed,
                "inflight_slots": inflight,
                "score": completed + inflight,
            }
        )
    return scores


def _account_slot_live_stats(db_path: Path, *, run_id: str | None) -> dict[str, dict[str, int]]:
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
        FROM slot_tasks
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
            "quarantined_slots": int(row[5] or 0),
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
            storage_ready,
            storage_reason,
            inode_use_percent,
            free_mb,
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
                storage_ready,
                storage_reason,
                inode_use_percent,
                free_mb,
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
                "storage_ready": bool(row[14]),
                "storage_reason": row[15],
            },
            "inode_use_percent": int(row[16]) if row[16] is not None else None,
            "free_mb": int(row[17]) if row[17] is not None else None,
            "ts": row[18],
        }
    return readiness


def _configured_capacity_targets() -> dict[str, int]:
    slots_per_job = _env_int("PEETSFEA_SLOTS_PER_JOB", 4)
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
        "slots_per_job": slots_per_job,
        "configured_target_slots": configured_worker_jobs * slots_per_job,
        "scaling_accounts": scaling_accounts,
        "scaling_worker_jobs": scaling_accounts * scaling_jobs_per_account,
        "expansion_target_slots": scaling_accounts * scaling_jobs_per_account * slots_per_job,
    }


def _throughput_kpi_payload(
    *,
    queued_slots: int,
    active_slots: int,
    active_workers: int,
    pending_workers: int,
) -> dict[str, object]:
    targets = _configured_capacity_targets()
    demand_slots = queued_slots + active_slots
    configured_target_slots = int(targets["configured_target_slots"])
    configured_worker_jobs = int(targets["configured_worker_jobs"])
    effective_target_slots = min(configured_target_slots, demand_slots) if demand_slots > 0 else 0
    active_slot_shortfall = max(effective_target_slots - active_slots, 0)
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
        "queued_slots": queued_slots,
        "active_slots": active_slots,
        "demand_slots": demand_slots,
        "effective_target_slots": effective_target_slots,
        "active_slot_shortfall": active_slot_shortfall,
        "active_workers": active_workers,
        "pending_workers": pending_workers,
        "worker_shortfall": worker_shortfall,
        "scheduled_worker_shortfall": scheduled_worker_shortfall,
        "recovery_backlog_slots": queued_slots,
        "input_limited": input_limited,
        "capacity_limited": (not input_limited) and active_slot_shortfall > 0,
        "throughput_mode": throughput_mode,
        "configured_slot_utilization": (float(active_slots) / configured_target_slots) if configured_target_slots else 0.0,
        "effective_slot_utilization": (float(active_slots) / effective_target_slots) if effective_target_slots else 0.0,
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
    elif normalized_stage in {"SLURM_TRUTH_REFRESHED", "SLURM_TRUTH_LAG", "CAPACITY_MISMATCH"}:
        category = "capacity"
        alertable = normalized_stage in {"SLURM_TRUTH_LAG", "CAPACITY_MISMATCH"}
        severity = "WARN" if normalized_stage != "SLURM_TRUTH_REFRESHED" else normalized_level
    elif normalized_stage in {"CONTROL_TUNNEL_READY", "CONTROL_TUNNEL_HEARTBEAT"}:
        category = "recovery"
        alertable = False
        severity = normalized_level
    elif normalized_stage in {"CONTROL_TUNNEL_LOST", "CONTROL_TUNNEL_DEGRADED"}:
        category = "recovery"
        alertable = True
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"WORKER_LOOP_RECOVERING", "SLURM_WORKERS_REDISCOVERED", "WORKER_DEATH_DETECTED"} or "RECOVER" in normalized_stage:
        category = "recovery"
        alertable = normalized_stage != "CONTROL_TUNNEL_RECOVERED"
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {
        "CUTOVER_READY",
        "CUTOVER_BLOCKED",
        "CANARY_PASSED",
        "CANARY_FAILED",
        "ROLLBACK_TRIGGERED",
        "FULL_ROLLOUT_READY",
        "SERVICE_BOUNDARY",
    }:
        category = "lifecycle"
        alertable = normalized_stage in {"CUTOVER_BLOCKED", "CANARY_FAILED", "ROLLBACK_TRIGGERED"}
        if normalized_stage in {"CANARY_FAILED", "ROLLBACK_TRIGGERED"}:
            severity = "ERROR" if normalized_level == "INFO" else normalized_level
        elif normalized_stage == "CUTOVER_BLOCKED":
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
    seen_keys: set[str] = set()

    def add_alert(alert: dict[str, object]) -> None:
        alert_key = str(alert["alert_key"])
        if alert_key in seen_keys:
            return
        seen_keys.add(alert_key)
        alerts.append(alert)

    if throughput_kpi and bool(throughput_kpi.get("capacity_limited")):
        add_alert(
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
        add_alert(
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
        add_alert(
            {
                "alert_key": "QUARANTINE_BURST",
                "severity": "ERROR",
                "category": "quarantine",
                "account_id": None if account_id == "global" else account_id,
                "message": f"recent quarantine burst count={count}",
            }
        )

    for event in events:
        if not bool(event.get("alertable")):
            continue
        stage = str(event.get("stage") or "")
        category = str(event.get("category") or "ops")
        account_id = event.get("account_id")
        add_alert(
            {
                "alert_key": stage or str(event.get("common_key") or "OPS_ALERT"),
                "severity": str(event.get("severity") or event.get("level") or "WARN"),
                "category": category,
                "account_id": None if account_id in ("", None) else account_id,
                "message": str(event.get("message") or ""),
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
            SELECT ts FROM slot_events
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


def _is_loopback_client(client_host: str | None) -> bool:
    return client_host in {"127.0.0.1", "::1", "localhost"}


def _slurm_worker_payloads(db_path: Path, *, run_id: str | None) -> list[dict[str, object]]:
    sql = """
        SELECT
            run_id,
            worker_id,
            job_id,
            attempt_no,
            account_id,
            host_alias,
            slurm_job_id,
            worker_state,
            observed_node,
            slots_configured,
            backend,
            tunnel_session_id,
            tunnel_state,
            heartbeat_ts,
            degraded_reason,
            submitted_at,
            started_at,
            ended_at,
            last_seen_ts
        FROM slurm_workers
    """
    params: list[object] = []
    if run_id is not None:
        sql += " WHERE run_id = ?"
        params.append(run_id)
    sql += " ORDER BY last_seen_ts DESC, worker_id"
    rows = _query(db_path, sql, params)
    payloads: list[dict[str, object]] = []
    now = datetime.now(tz=timezone.utc)
    stale_threshold = _env_int("PEETSFEA_STALE_THRESHOLD_SECONDS", 90)
    for row in rows:
        heartbeat_age = _age_seconds(row[13])
        worker_state = str(row[7] or "")
        tunnel_state = str(row[12] or "")
        tunnel_requires_heartbeat = worker_state in {"RUNNING", "IDLE_DRAINING"} or tunnel_state in {
            "CONNECTED",
            "DEGRADED",
        }
        is_tunnel_stale = tunnel_requires_heartbeat and (
            heartbeat_age is None or heartbeat_age > stale_threshold
        )
        payloads.append(
            {
                "run_id": row[0],
                "worker_id": row[1],
                "job_id": row[2],
                "attempt_no": int(row[3] or 0),
                "account_id": row[4],
                "host_alias": row[5],
                "slurm_job_id": row[6],
                "worker_state": worker_state,
                "observed_node": row[8],
                "slots_configured": int(row[9] or 0),
                "backend": row[10],
                "tunnel_session_id": row[11],
                "tunnel_state": tunnel_state,
                "heartbeat_ts": row[13],
                "heartbeat_age_seconds": heartbeat_age,
                "degraded_reason": row[14],
                "submitted_at": row[15],
                "started_at": row[16],
                "ended_at": row[17],
                "last_seen_ts": row[18],
                "is_tunnel_stale": is_tunnel_stale,
                "ts": now.isoformat(),
            }
        )
    return payloads


def _latest_resource_summary_payload(db_path: Path, *, run_id: str | None) -> dict[str, object] | None:
    if run_id is None:
        return None
    rows = _query(
        db_path,
        """
        SELECT
            run_id,
            host,
            allocated_mem_mb,
            used_mem_mb,
            free_mem_mb,
            load_1,
            running_worker_count,
            active_slot_count,
            stale,
            ts
        FROM resource_summary_snapshots
        WHERE run_id = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        [run_id],
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "run_id": row[0],
        "host": row[1],
        "allocated_mem_mb": int(row[2]) if row[2] is not None else None,
        "used_mem_mb": int(row[3]) if row[3] is not None else None,
        "free_mem_mb": int(row[4]) if row[4] is not None else None,
        "load_1": float(row[5]) if row[5] is not None else None,
        "running_worker_count": int(row[6] or 0),
        "active_slot_count": int(row[7] or 0),
        "stale": bool(row[8]),
        "ts": row[9],
        "age_seconds": _age_seconds(row[9]),
    }


def _latest_node_resource_payload(db_path: Path, *, run_id: str | None) -> dict[str, object] | None:
    if run_id is None:
        return None
    rows = _query(
        db_path,
        """
        SELECT
            run_id,
            host,
            allocated_mem_mb,
            total_mem_mb,
            used_mem_mb,
            free_mem_mb,
            load_1,
            load_5,
            load_15,
            process_count,
            running_worker_count,
            active_slot_count,
            ts
        FROM node_resource_snapshots
        WHERE run_id = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        [run_id],
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "run_id": row[0],
        "host": row[1],
        "allocated_mem_mb": int(row[2]) if row[2] is not None else None,
        "total_mem_mb": int(row[3]) if row[3] is not None else None,
        "used_mem_mb": int(row[4]) if row[4] is not None else None,
        "free_mem_mb": int(row[5]) if row[5] is not None else None,
        "load_1": float(row[6]) if row[6] is not None else None,
        "load_5": float(row[7]) if row[7] is not None else None,
        "load_15": float(row[8]) if row[8] is not None else None,
        "process_count": int(row[9] or 0),
        "running_worker_count": int(row[10] or 0),
        "active_slot_count": int(row[11] or 0),
        "ts": row[12],
        "age_seconds": _age_seconds(row[12]),
    }


def _worker_mix_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    if run_id is None:
        return {
            "worker_count": 0,
            "busy_workers": 0,
            "idle_workers": 0,
            "configured_slots": 0,
            "active_slots": 0,
            "idle_slots": 0,
        }
    rows = _query(
        db_path,
        """
        SELECT
            COUNT(*),
            SUM(CASE WHEN active_slots > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN active_slots = 0 THEN 1 ELSE 0 END),
            SUM(configured_slots),
            SUM(active_slots),
            SUM(idle_slots)
        FROM (
            SELECT
                worker_id,
                configured_slots,
                active_slots,
                idle_slots
            FROM (
                SELECT
                    worker_id,
                    configured_slots,
                    active_slots,
                    idle_slots,
                    ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY ts DESC) AS rn
                FROM worker_resource_snapshots
                WHERE run_id = ?
            ) ranked
            WHERE rn = 1
        ) latest
        """,
        [run_id],
    )
    row = rows[0] if rows else (0, 0, 0, 0, 0, 0)
    return {
        "worker_count": int(row[0] or 0),
        "busy_workers": int(row[1] or 0),
        "idle_workers": int(row[2] or 0),
        "configured_slots": int(row[3] or 0),
        "active_slots": int(row[4] or 0),
        "idle_slots": int(row[5] or 0),
    }


def _overview_account_payloads(db_path: Path, *, run_id: str | None) -> list[dict[str, object]]:
    score_map = {item["account_id"]: item for item in _account_slot_scores(db_path, run_id=run_id)}
    live_slot_map = _account_slot_live_stats(db_path, run_id=run_id)
    target_worker_map = _configured_account_worker_targets()
    readiness_map = _latest_account_readiness(db_path)
    capacity_rows = _query(
        db_path,
        """
        SELECT
            account_id,
            host,
            running_count,
            pending_count,
            allowed_submit,
            ts
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
    account_ids = sorted(set(capacity_map) | set(readiness_map) | set(target_worker_map) | set(score_map) | set(live_slot_map))
    payloads: list[dict[str, object]] = []
    for account_id in account_ids:
        target = int(target_worker_map.get(account_id, _env_int("PEETSFEA_MAX_JOBS_PER_ACCOUNT", 10)))
        running = int(capacity_map.get(account_id, {}).get("running_count", 0))
        pending = int(capacity_map.get(account_id, {}).get("pending_count", 0))
        active_slots = int(live_slot_map.get(account_id, {}).get("active_slots", 0))
        payloads.append(
            {
                "account_id": account_id,
                "host": capacity_map.get(account_id, {}).get("host") or readiness_map.get(account_id, {}).get("host"),
                "target_workers": target,
                "running_count": running,
                "pending_count": pending,
                "allowed_submit": int(capacity_map.get(account_id, {}).get("allowed_submit", 0)),
                "active_slots": active_slots,
                "completed_slots": int(live_slot_map.get(account_id, {}).get("completed_slots", 0)),
                "live_status": _account_live_status(
                    readiness_ready=readiness_map.get(account_id, {}).get("ready"),
                    running_count=running,
                    pending_count=pending,
                    target_workers=target,
                ),
                "readiness_status": readiness_map.get(account_id, {}).get("status"),
                "readiness_reason": readiness_map.get(account_id, {}).get("reason"),
                "storage_ready": readiness_map.get(account_id, {}).get("checks", {}).get("storage_ready"),
                "storage_reason": readiness_map.get(account_id, {}).get("checks", {}).get("storage_reason"),
                "inode_use_percent": readiness_map.get(account_id, {}).get("inode_use_percent"),
                "free_mb": readiness_map.get(account_id, {}).get("free_mb"),
                "score": int(score_map.get(account_id, {}).get("score", 0)),
                "slot_gap": max(target * _env_int("PEETSFEA_SLOTS_PER_JOB", 4) - active_slots, 0),
            }
        )
    return payloads


def _recent_ops_events_payload(db_path: Path, *, run_id: str | None, limit: int) -> tuple[list[dict[str, object]], dict[str, object] | None]:
    query = """
        SELECT run_id, entity_id, level, stage, message, ts, source, account_id
        FROM (
            SELECT e.run_id, e.job_id AS entity_id, e.level, e.stage, e.message, e.ts, 'JOB' AS source, j.account_id
            FROM events AS e
            LEFT JOIN jobs AS j
            ON e.run_id = j.run_id AND e.job_id = j.job_id
            UNION ALL
            SELECT w.run_id, w.slot_id AS entity_id, w.level, w.stage, w.message, w.ts, 'SLOT' AS source, wt.account_id
            FROM slot_events AS w
            LEFT JOIN slot_tasks AS wt
            ON w.run_id = wt.run_id AND w.slot_id = wt.slot_id
        ) AS merged
    """
    params: list[object] = []
    if run_id is not None:
        query += " WHERE run_id = ?"
        params.append(run_id)
    query += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    rows = _query(db_path, query, params)
    throughput_kpi: dict[str, object] | None = None
    if run_id is not None:
        slot_rows = _query(
            db_path,
            """
            SELECT
                SUM(CASE WHEN state IN ('QUEUED', 'RETRY_QUEUED') THEN 1 ELSE 0 END),
                SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING', 'LEASED', 'DOWNLOADING') THEN 1 ELSE 0 END)
            FROM slot_tasks
            WHERE run_id = ?
            """,
            [run_id],
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
            [run_id],
        )
        queued_slots = int(slot_rows[0][0] or 0)
        active_slots = int(slot_rows[0][1] or 0)
        active_workers = int(job_rows[0][0] or 0)
        pending_workers = int(job_rows[0][1] or 0)
        throughput_kpi = _throughput_kpi_payload(
            queued_slots=queued_slots,
            active_slots=active_slots,
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
                "slot_id": row[1] if row[6] == "SLOT" else None,
                "source": row[6],
                "account_id": account_id,
                "level": row[2],
                "stage": row[3],
                "message": row[4],
                "ts": row[5],
                **classification,
            }
        )
    return events, throughput_kpi


def _overview_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    latest_run = None
    if run_id is not None:
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
        if rows:
            row = rows[0]
            latest_run = {
                "run_id": row[0],
                "started_at": row[1],
                "finished_at": row[2],
                "state": row[3],
                "summary": row[4],
            }
    health = _worker_health_payload(db_path, stale_threshold=int(os.getenv("PEETSFEA_STALE_THRESHOLD_SECONDS", "90")))
    events, throughput_kpi = _recent_ops_events_payload(db_path, run_id=run_id, limit=24)
    alerts = _alertable_event_summary(events=events, throughput_kpi=throughput_kpi)
    workers = _slurm_worker_payloads(db_path, run_id=run_id)
    resource_summary = _latest_resource_summary_payload(db_path, run_id=run_id)
    node_summary = _latest_node_resource_payload(db_path, run_id=run_id)
    worker_mix = _worker_mix_payload(db_path, run_id=run_id)
    tunnel_summary = {
        "connected_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "CONNECTED"),
        "degraded_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "DEGRADED"),
        "stale_workers": sum(1 for worker in workers if worker.get("is_tunnel_stale")),
    }
    return {
        "run_id": run_id,
        "health": health,
        "run": latest_run,
        "throughput_kpi": throughput_kpi,
        "resource_summary": resource_summary,
        "node_summary": node_summary,
        "worker_mix": worker_mix,
        "accounts": _overview_account_payloads(db_path, run_id=run_id),
        "alerts": alerts[:8],
        "recent_events": events[:12],
        "tunnel_summary": tunnel_summary,
        "rollout_status": _rollout_status_payload(db_path, run_id=run_id),
    }


def _rollout_status_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    if run_id is None:
        return {
            "run_id": None,
            "state": "IDLE",
            "full_rollout_ready": False,
            "fallback_active": False,
            "service_boundary": {
                "input_source_policy": os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only"),
                "public_storage_mode": os.getenv("PEETSFEA_PUBLIC_STORAGE_MODE", "disabled"),
                "runner_scope": "control_plane_orchestration",
                "storage_scope": "separate_service_boundary",
            },
        }
    rows = _query(
        db_path,
        """
        SELECT stage, level, message, ts
        FROM events
        WHERE run_id = ? AND job_id = '__worker__'
        ORDER BY ts DESC
        LIMIT 50
        """,
        [run_id],
    )
    seen_stages = [str(row[0]) for row in rows]
    service_boundary = {
        "input_source_policy": os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only"),
        "public_storage_mode": os.getenv("PEETSFEA_PUBLIC_STORAGE_MODE", "disabled"),
        "runner_scope": "control_plane_orchestration",
        "storage_scope": "separate_service_boundary",
    }
    boundary_event = next((row for row in rows if str(row[0]) == "SERVICE_BOUNDARY"), None)
    if boundary_event is not None:
        for token in str(boundary_event[2]).split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            if key in service_boundary:
                service_boundary[key] = value
    if "ROLLBACK_TRIGGERED" in seen_stages or "CANARY_FAILED" in seen_stages:
        state = "FALLBACK_ACTIVE"
    elif "FULL_ROLLOUT_READY" in seen_stages:
        state = "FULL_ROLLOUT_READY"
    elif "CUTOVER_BLOCKED" in seen_stages:
        state = "CUTOVER_BLOCKED"
    elif "CUTOVER_READY" in seen_stages:
        state = "CANARY_READY"
    else:
        state = "ACTIVE"
    return {
        "run_id": run_id,
        "state": state,
        "full_rollout_ready": "FULL_ROLLOUT_READY" in seen_stages,
        "fallback_active": "ROLLBACK_TRIGGERED" in seen_stages,
        "canary_passed": "CANARY_PASSED" in seen_stages,
        "service_boundary": service_boundary,
        "recent_stages": seen_stages[:8],
    }


def make_status_handler(*, db_path: Path):
    stale_threshold = int(os.getenv("PEETSFEA_STALE_THRESHOLD_SECONDS", "90"))
    state_store = StateStore(db_path)
    state_store.initialize()

    class StatusHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: object, status: int = 200) -> None:
            if isinstance(payload, dict) and "version" not in payload:
                payload = {"version": APP_VERSION, **payload}
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

        def _read_json_body(self) -> dict[str, object]:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def _reject_non_loopback(self) -> bool:
            client_host = self.client_address[0] if self.client_address else None
            if _is_loopback_client(client_host):
                return False
            self._send_json({"error": "forbidden"}, status=403)
            return True

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not parsed.path.startswith("/internal/"):
                self._send_json({"error": "not_found"}, status=404)
                return
            if self._reject_non_loopback():
                return
            try:
                payload = self._read_json_body()
            except json.JSONDecodeError:
                self._send_json({"error": "invalid_json"}, status=400)
                return

            run_id = str(payload.get("run_id") or "").strip()
            worker_id = str(payload.get("worker_id") or "").strip()
            if parsed.path.startswith("/internal/workers/"):
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                worker = state_store.get_slurm_worker(run_id=run_id, worker_id=worker_id)
                if worker is None:
                    self._send_json({"error": "worker_not_found"}, status=404)
                    return

            if parsed.path == "/internal/workers/register":
                tunnel_session_id = str(payload.get("tunnel_session_id") or "").strip() or None
                observed_node = str(payload.get("observed_node") or "").strip() or None
                state_store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="CONNECTED",
                    tunnel_session_id=tunnel_session_id,
                    observed_node=observed_node,
                )
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="CONTROL_TUNNEL_READY",
                    message=(
                        f"worker_id={worker_id} slurm_job_id={payload.get('slurm_job_id') or worker.get('slurm_job_id')} "
                        f"tunnel_session_id={tunnel_session_id or worker.get('tunnel_session_id') or 'unknown'}"
                    ),
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/heartbeat":
                state_store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="CONNECTED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/degraded":
                reason = str(payload.get("reason") or "tunnel degraded").strip()
                state_store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="DEGRADED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                    degraded_reason=reason,
                )
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="WARN",
                    stage="CONTROL_TUNNEL_LOST",
                    message=f"worker_id={worker_id} reason={reason}",
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/recovered":
                state_store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="CONNECTED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                    degraded_reason=None,
                )
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="CONTROL_TUNNEL_RECOVERED",
                    message=f"worker_id={worker_id}",
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/node":
                if not run_id:
                    self._send_json({"error": "run_id_required"}, status=400)
                    return
                host = str(payload.get("host") or "unknown").strip() or "unknown"
                state_store.record_node_resource_snapshot(
                    run_id=run_id,
                    host=host,
                    allocated_mem_mb=int(payload["allocated_mem_mb"]) if payload.get("allocated_mem_mb") is not None else None,
                    total_mem_mb=int(payload["total_mem_mb"]) if payload.get("total_mem_mb") is not None else None,
                    used_mem_mb=int(payload["used_mem_mb"]) if payload.get("used_mem_mb") is not None else None,
                    free_mem_mb=int(payload["free_mem_mb"]) if payload.get("free_mem_mb") is not None else None,
                    load_1=float(payload["load_1"]) if payload.get("load_1") is not None else None,
                    load_5=float(payload["load_5"]) if payload.get("load_5") is not None else None,
                    load_15=float(payload["load_15"]) if payload.get("load_15") is not None else None,
                    tmp_total_mb=int(payload["tmp_total_mb"]) if payload.get("tmp_total_mb") is not None else None,
                    tmp_used_mb=int(payload["tmp_used_mb"]) if payload.get("tmp_used_mb") is not None else None,
                    tmp_free_mb=int(payload["tmp_free_mb"]) if payload.get("tmp_free_mb") is not None else None,
                    process_count=int(payload.get("process_count") or 0),
                    running_worker_count=int(payload.get("running_worker_count") or 0),
                    active_slot_count=int(payload.get("active_slot_count") or 0),
                )
                state_store.record_resource_summary_snapshot(
                    run_id=run_id,
                    host=host,
                    allocated_mem_mb=int(payload["allocated_mem_mb"]) if payload.get("allocated_mem_mb") is not None else (
                        int(payload["total_mem_mb"]) if payload.get("total_mem_mb") is not None else None
                    ),
                    used_mem_mb=int(payload["used_mem_mb"]) if payload.get("used_mem_mb") is not None else None,
                    free_mem_mb=int(payload["free_mem_mb"]) if payload.get("free_mem_mb") is not None else None,
                    load_1=float(payload["load_1"]) if payload.get("load_1") is not None else None,
                    running_worker_count=int(payload.get("running_worker_count") or 0),
                    active_slot_count=int(payload.get("active_slot_count") or 0),
                    stale=bool(payload.get("stale") or False),
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/worker":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                host = str(payload.get("host") or worker.get("observed_node") or worker.get("host_alias") or "unknown")
                state_store.record_worker_resource_snapshot(
                    run_id=run_id,
                    worker_id=worker_id,
                    host=host,
                    slurm_job_id=str(payload.get("slurm_job_id") or worker.get("slurm_job_id") or "").strip() or None,
                    configured_slots=int(payload.get("configured_slots") or 0),
                    active_slots=int(payload.get("active_slots") or 0),
                    idle_slots=int(payload.get("idle_slots") or 0),
                    rss_mb=int(payload["rss_mb"]) if payload.get("rss_mb") is not None else None,
                    cpu_pct=float(payload["cpu_pct"]) if payload.get("cpu_pct") is not None else None,
                    tunnel_state=str(payload.get("tunnel_state") or worker.get("tunnel_state") or "").strip() or None,
                    process_count=int(payload.get("process_count") or 0),
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/slot":
                if not run_id:
                    self._send_json({"error": "run_id_required"}, status=400)
                    return
                slot_id = str(payload.get("slot_id") or "").strip()
                if not slot_id:
                    self._send_json({"error": "slot_id_required"}, status=400)
                    return
                state_store.record_slot_resource_snapshot(
                    run_id=run_id,
                    slot_id=slot_id,
                    worker_id=worker_id or (str(payload.get("worker_id") or "").strip() or None),
                    host=str(payload.get("host") or "").strip() or None,
                    allocated_mem_mb=int(payload["allocated_mem_mb"]) if payload.get("allocated_mem_mb") is not None else None,
                    used_mem_mb=int(payload["used_mem_mb"]) if payload.get("used_mem_mb") is not None else None,
                    load_1=float(payload["load_1"]) if payload.get("load_1") is not None else None,
                    rss_mb=int(payload["rss_mb"]) if payload.get("rss_mb") is not None else None,
                    cpu_pct=float(payload["cpu_pct"]) if payload.get("cpu_pct") is not None else None,
                    process_count=int(payload.get("process_count") or 0),
                    active_process_count=int(payload.get("active_process_count") or 0),
                    artifact_bytes=int(payload.get("artifact_bytes") or 0),
                    progress_ts=str(payload.get("progress_ts") or "").strip() or None,
                    state=str(payload.get("state") or "").strip() or None,
                )
                self._send_json({"ok": True})
                return

            self._send_json({"error": "not_found"}, status=404)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            if parsed.path == "/":
                self._send_html(_dashboard_html(version=APP_VERSION))
                return

            if parsed.path == "/health":
                self._send_json({"ok": True})
                return

            if parsed.path == "/api":
                self._send_json(
                    {
                        "endpoints": [
                            "/api/worker/health",
                            "/api/overview",
                            "/api/operations/rollout",
                            "/api/workers",
                            "/api/resources/summary",
                            "/api/slots",
                            "/api/slots/{id}/timeline",
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

            if parsed.path == "/api/overview":
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                self._send_json(_overview_payload(db_path, run_id=run_id))
                return

            if parsed.path == "/api/operations/rollout":
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                self._send_json(_rollout_status_payload(db_path, run_id=run_id))
                return

            if parsed.path == "/api/workers":
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                self._send_json({"run_id": run_id, "workers": _slurm_worker_payloads(db_path, run_id=run_id)})
                return

            if parsed.path == "/api/resources/summary":
                run_id = _first_str_param(params, "run_id") or _latest_run_id(db_path)
                self._send_json({"run_id": run_id, "summary": _latest_resource_summary_payload(db_path, run_id=run_id)})
                return

            if parsed.path == "/api/slots":
                requested_run_id = _first_str_param(params, "run_id")
                run_id = requested_run_id or _latest_run_id(db_path)
                state_filter = _first_str_param(params, "status")
                limit = _first_int_param(params, "limit", default=500, minimum=1)
                if run_id is None:
                    self._send_json({"run_id": None, "slots": []})
                    return

                sql = """
                    SELECT run_id, slot_id, job_id, account_id, input_path, output_path, state, attempt_no, updated_at
                    FROM slot_tasks
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
                        "slots": [
                            {
                                "slot_id": row[1],
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

            if parsed.path.startswith("/api/slots/") and parsed.path.endswith("/timeline"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                slot_id = parts[2]
                rows = _query(
                    db_path,
                    """
                    SELECT run_id, slot_id, job_id, account_id, input_path, output_path, state, attempt_no, failure_reason, created_at, updated_at
                    FROM slot_tasks
                    WHERE slot_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    [slot_id],
                )
                if not rows:
                    self._send_json({"error": "not_found"}, status=404)
                    return
                row = rows[0]
                events = _query(
                    db_path,
                    """
                    SELECT level, stage, message, ts
                    FROM slot_events
                    WHERE run_id = ? AND slot_id = ?
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
                    WHERE run_id = ? AND slot_id = ?
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
                        "slot_task": {
                            "run_id": row[0],
                            "slot_id": row[1],
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
                            "slot_attempt_no": int(row[7] or 0),
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
                score_map = {item["account_id"]: item for item in _account_slot_scores(db_path, run_id=run_id)}
                live_slot_map = _account_slot_live_stats(db_path, run_id=run_id)
                target_worker_map = _configured_account_worker_targets()
                slots_per_job = int(_configured_capacity_targets()["slots_per_job"])
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
                                * slots_per_job,
                                "active_slots": int(live_slot_map.get(account_id, {}).get("active_slots", 0)),
                                "completed_slots": int(live_slot_map.get(account_id, {}).get("completed_slots", 0)),
                                "succeeded_slots": int(live_slot_map.get(account_id, {}).get("succeeded_slots", 0)),
                                "failed_slots": int(live_slot_map.get(account_id, {}).get("failed_slots", 0)),
                                "quarantined_slots": int(
                                    live_slot_map.get(account_id, {}).get("quarantined_slots", 0)
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
                                    * slots_per_job
                                    - int(live_slot_map.get(account_id, {}).get("active_slots", 0)),
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
                                "storage_ready": readiness_map.get(account_id, {}).get("checks", {}).get("storage_ready"),
                                "storage_reason": readiness_map.get(account_id, {}).get("checks", {}).get("storage_reason"),
                                "inode_use_percent": readiness_map.get(account_id, {}).get("inode_use_percent"),
                                "free_mb": readiness_map.get(account_id, {}).get("free_mb"),
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

                total_slots = 0
                queued_slots = 0
                active_slots = 0
                succeeded_slots = 0
                failed_slots = 0
                quarantined_slots = 0
                delete_quarantined_slots = 0
                if run_id is not None:
                    slot_rows = _query(
                        db_path,
                        """
                        SELECT
                            COUNT(*) AS total_slots,
                            SUM(CASE WHEN state IN ('QUEUED', 'RETRY_QUEUED') THEN 1 ELSE 0 END) AS queued_slots,
                            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END) AS active_slots,
                            SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_slots,
                            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_slots,
                            SUM(CASE WHEN state = 'QUARANTINED' THEN 1 ELSE 0 END) AS quarantined_slots
                        FROM slot_tasks
                        WHERE run_id = ?
                        """,
                        [run_id],
                    )
                    (
                        total_slots,
                        queued_slots,
                        active_slots,
                        succeeded_slots,
                        failed_slots,
                        quarantined_slots,
                    ) = (int(item or 0) for item in slot_rows[0])
                    delete_rows = _query(
                        db_path,
                        """
                        SELECT COUNT(*)
                        FROM file_lifecycle
                        WHERE run_id = ? AND delete_final_state = 'DELETE_QUARANTINED'
                        """,
                        [run_id],
                    )
                    delete_quarantined_slots = int(delete_rows[0][0] or 0)
                throughput_kpi = _throughput_kpi_payload(
                    queued_slots=queued_slots,
                    active_slots=active_slots,
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
                            "total_slots": total_slots,
                            "queued_slots": queued_slots,
                            "active_slots": active_slots,
                            "succeeded_slots": succeeded_slots,
                            "failed_slots": failed_slots,
                            "quarantined_slots": quarantined_slots,
                            "delete_quarantined_slots": delete_quarantined_slots,
                            "throughput_kpi": throughput_kpi,
                            "account_slot_scores": _account_slot_scores(db_path, run_id=run_id),
                            "resource_summary": _latest_resource_summary_payload(db_path, run_id=run_id),
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
                slot_state_rows = _query(
                    db_path,
                    """
                    SELECT state, COUNT(*)
                    FROM slot_tasks
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
                        SELECT level, stage, message, ts, 'SLOT' AS source
                        FROM slot_events
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
                            "slot_state_counts": [
                                {"state": state, "count": int(count)} for state, count in slot_state_rows
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
                        SELECT w.run_id, w.slot_id AS entity_id, w.level, w.stage, w.message, w.ts, 'SLOT' AS source, wt.account_id
                        FROM slot_events
                        AS w
                        LEFT JOIN slot_tasks AS wt
                        ON w.run_id = wt.run_id AND w.slot_id = wt.slot_id
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
                    slot_rows = _query(
                        db_path,
                        """
                        SELECT
                            SUM(CASE WHEN state IN ('QUEUED', 'RETRY_QUEUED') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING') THEN 1 ELSE 0 END)
                        FROM slot_tasks
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
                    queued_slots = int(slot_rows[0][0] or 0)
                    active_slots = int(slot_rows[0][1] or 0)
                    active_workers = int(job_rows[0][0] or 0)
                    pending_workers = int(job_rows[0][1] or 0)
                    throughput_kpi = _throughput_kpi_payload(
                        queued_slots=queued_slots,
                        active_slots=active_slots,
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
                            "slot_id": row[1] if row[6] == "SLOT" else None,
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


def _dashboard_html(*, version: str) -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Peets FEA Status</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #0b1220; color: #e5e7eb; }
    .wrap { max-width: 1420px; margin: 0 auto; padding: 20px; }
    h1 { margin: 0 0 8px; font-size: 24px; }
    h2 { margin: 0 0 10px; font-size: 16px; }
    .muted { color: #9ca3af; font-size: 12px; }
    .hero { display:grid; grid-template-columns: 2fr 1fr; gap: 12px; margin-top: 14px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin: 14px 0; }
    .panel { background: #111a2e; border: 1px solid #25324a; border-radius: 12px; padding: 14px; }
    .k { color: #9ca3af; font-size: 12px; margin-bottom: 4px; }
    .v { font-size: 22px; font-weight: 700; }
    .badge { display: inline-block; padding: 4px 8px; border-radius: 6px; font-size: 12px; font-weight: 700; }
    .badge-HEALTHY { background: #064e3b; color: #6ee7b7; }
    .badge-DEGRADED { background: #78350f; color: #fcd34d; }
    .badge-STALE { background: #7f1d1d; color: #fca5a5; }
    .badge-BLOCKED { background: #7c2d12; color: #fdba74; }
    .section-grid { display:grid; grid-template-columns: 1.5fr 1fr; gap:12px; }
    .resource-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:10px; }
    table { width: 100%; border-collapse: collapse; background: #111a2e; border: 1px solid #25324a; border-radius: 10px; overflow: hidden; }
    th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #25324a; font-size: 12px; vertical-align: top; }
    th { background: #0f172a; color: #cbd5e1; }
    details { margin-top: 14px; }
    summary { cursor: pointer; color: #cbd5e1; font-weight: 700; }
    .list { display:grid; gap:8px; }
    .list-item { border:1px solid #25324a; border-radius:10px; padding:10px; background:#0f172a; }
    code { color: #93c5fd; }
    a { color: #93c5fd; }
    @media (max-width: 980px) { .hero, .section-grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Peets FEA Status Dashboard</h1>
    <div class="muted">Version <code>__APP_VERSION__</code> | Slot(.aedt) 기준 overview-first 운영 콘솔. overview는 5초, detail은 펼쳤을 때만 30초 주기로 갱신.</div>

    <div class="hero">
      <div class="panel">
        <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <span id="health-badge" class="badge">-</span>
          <span class="muted">worker state: <code id="health-worker-state">-</code></span>
          <span class="muted">run: <code id="health-run">-</code></span>
          <span class="muted">heartbeat age: <code id="health-age">-</code>s</span>
          <span class="muted">last event age: <code id="event-age">-</code>s</span>
          <span class="muted">reason: <code id="health-reason">-</code></span>
        </div>
        <div class="grid">
          <div class="panel"><div class="k">Worker Alive</div><div class="v" id="worker-live">-</div></div>
          <div class="panel"><div class="k">Slot Configured</div><div class="v" id="slot-target">-</div></div>
          <div class="panel"><div class="k">Slot Active</div><div class="v" id="w-active">-</div></div>
          <div class="panel"><div class="k">Slot Idle</div><div class="v" id="slot-idle">-</div></div>
          <div class="panel"><div class="k">Refill Lag</div><div class="v" id="refill-lag">-</div></div>
          <div class="panel"><div class="k">Tunnel Degraded</div><div class="v" id="tunnel-bad">-</div></div>
          <div class="panel"><div class="k">Tunnel Stale</div><div class="v" id="tunnel-stale">-</div></div>
          <div class="panel"><div class="k">Throughput Mode</div><div class="v" id="slot-mode" style="font-size:15px;">-</div></div>
        </div>
      </div>
      <div class="panel">
        <h2>Operator Flow</h2>
        <div class="muted">rollout: <code id="rollout-state">-</code></div>
        <div class="muted">boundary: <code id="rollout-boundary">-</code></div>
        <div class="muted">1. health / alert 확인</div>
        <div class="muted">2. account target / running / pending 확인</div>
        <div class="muted">3. memory / load / process count 확인</div>
        <div class="muted">4. refill lag / tunnel 상태 확인</div>
        <div class="muted">5. 필요할 때만 detail 펼치기</div>
      </div>
    </div>

    <div class="section-grid">
      <div class="panel">
        <h2>Resource Panel</h2>
        <div class="resource-grid">
          <div><div class="k">Allocated Memory</div><div class="v" id="res-alloc">-</div></div>
          <div><div class="k">Used Memory</div><div class="v" id="res-used">-</div></div>
          <div><div class="k">Free Memory</div><div class="v" id="res-free">-</div></div>
          <div><div class="k">Load</div><div class="v" id="res-load">-</div></div>
          <div><div class="k">Process Count</div><div class="v" id="res-proc">-</div></div>
          <div><div class="k">Worker Mix</div><div class="v" id="res-worker-mix" style="font-size:15px;">-</div></div>
          <div><div class="k">Slot Mix</div><div class="v" id="res-slot-mix" style="font-size:15px;">-</div></div>
        </div>
        <div class="muted" style="margin-top:10px;">slow snapshot age: <code id="res-age">-</code>s</div>
      </div>
      <div class="panel">
        <h2>Alerts</h2>
        <div id="alerts-list" class="list"></div>
      </div>
    </div>

    <div class="panel" style="margin-top:14px;">
      <h2>Account Overview</h2>
      <table>
        <thead><tr><th>account</th><th>host</th><th>target</th><th>R</th><th>PD</th><th>allow</th><th>active slots</th><th>live</th><th>readiness</th></tr></thead>
        <tbody id="accounts-body"></tbody>
      </table>
    </div>

    <details id="detail-panel">
      <summary>Detail Views</summary>
      <div class="section-grid" style="margin-top:12px;">
        <div class="panel">
          <h2>Recent Events</h2>
          <table>
            <thead><tr><th>ts</th><th>source</th><th>entity</th><th>stage</th><th>message</th></tr></thead>
            <tbody id="events-body"></tbody>
          </table>
        </div>
        <div class="panel">
          <h2>Workers</h2>
          <table>
            <thead><tr><th>worker</th><th>state</th><th>tunnel</th><th>node</th><th>configured</th><th>heartbeat</th></tr></thead>
            <tbody id="workers-body"></tbody>
          </table>
        </div>
      </div>
      <div class="panel" style="margin-top:12px;">
        <h2>Slot Detail</h2>
        <table>
          <thead><tr><th>slot_id</th><th>job_id</th><th>account</th><th>state</th><th>attempt</th><th>updated_at</th></tr></thead>
          <tbody id="slots-body"></tbody>
        </table>
      </div>
    </details>
  </div>
  <script>
    async function fetchJson(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(url + " -> " + r.status);
      return r.json();
    }
    function esc(s) { return String(s ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;"); }
    function fmtMb(value) {
      if (value == null) return '-';
      if (value >= 1024 * 1024) return (value / (1024 * 1024)).toFixed(1) + ' TB';
      if (value >= 1024) return (value / 1024).toFixed(1) + ' GB';
      return value + ' MB';
    }

    function renderAlerts(alerts) {
      const host = document.getElementById('alerts-list');
      host.innerHTML = '';
      if (!alerts.length) {
        host.innerHTML = '<div class="muted">no active alerts</div>';
        return;
      }
      for (const alert of alerts) {
        const div = document.createElement('div');
        div.className = 'list-item';
        div.innerHTML = `<div><strong>${esc(alert.alert_key)}</strong> <span class="muted">${esc(alert.severity)} / ${esc(alert.category)}</span></div><div class="muted">${esc(alert.message)}</div>`;
        host.appendChild(div);
      }
    }

    async function refreshOverview() {
      const overview = await fetchJson('/api/overview');
      const health = overview.health || {};
      const throughput = overview.throughput_kpi || {};
      const resource = overview.resource_summary || {};
      const node = overview.node_summary || {};
      const workerMix = overview.worker_mix || {};
      const tunnel = overview.tunnel_summary || {};
      const rollout = overview.rollout_status || {};
      const boundary = rollout.service_boundary || {};

      const badge = document.getElementById('health-badge');
      badge.textContent = health.status || '-';
      badge.className = 'badge badge-' + (health.status || 'STALE');
      document.getElementById('health-worker-state').textContent = health.worker_status || '-';
      document.getElementById('health-run').textContent = overview.run ? overview.run.run_id : '-';
      document.getElementById('health-age').textContent = health.heartbeat_age_seconds ?? '-';
      document.getElementById('event-age').textContent = health.last_event_age_seconds ?? '-';
      document.getElementById('health-reason').textContent = health.reason || '-';

      document.getElementById('worker-live').textContent = throughput.active_workers ?? '-';
      document.getElementById('slot-target').textContent = throughput.configured_target_slots ?? '-';
      document.getElementById('w-active').textContent = throughput.active_slots ?? '-';
      document.getElementById('slot-idle').textContent = Math.max((throughput.configured_target_slots || 0) - (throughput.active_slots || 0), 0);
      document.getElementById('refill-lag').textContent = throughput.recovery_backlog_slots ?? '-';
      document.getElementById('tunnel-bad').textContent = tunnel.degraded_workers ?? 0;
      document.getElementById('tunnel-stale').textContent = tunnel.stale_workers ?? 0;
      document.getElementById('slot-mode').textContent = throughput.throughput_mode || '-';
      document.getElementById('rollout-state').textContent = rollout.state || '-';
      document.getElementById('rollout-boundary').textContent =
        `${boundary.input_source_policy || '-'} / ${boundary.public_storage_mode || '-'}`;

      document.getElementById('res-alloc').textContent = fmtMb(node.allocated_mem_mb ?? resource.allocated_mem_mb);
      document.getElementById('res-used').textContent = fmtMb(node.used_mem_mb ?? resource.used_mem_mb);
      document.getElementById('res-free').textContent = fmtMb(node.free_mem_mb ?? resource.free_mem_mb);
      document.getElementById('res-load').textContent = (node.load_1 ?? resource.load_1) == null ? '-' : (node.load_1 ?? resource.load_1).toFixed(2);
      document.getElementById('res-proc').textContent = node.process_count ?? '-';
      document.getElementById('res-worker-mix').textContent = `${workerMix.busy_workers ?? 0} busy / ${workerMix.idle_workers ?? 0} idle`;
      document.getElementById('res-slot-mix').textContent = `${workerMix.active_slots ?? 0} active / ${workerMix.idle_slots ?? 0} idle`;
      document.getElementById('res-age').textContent = node.age_seconds ?? resource.age_seconds ?? '-';

      const accountsBody = document.getElementById('accounts-body');
      accountsBody.innerHTML = '';
      for (const account of (overview.accounts || [])) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${esc(account.account_id)}</td>
          <td>${esc(account.host)}</td>
          <td>${esc(account.target_workers)}</td>
          <td>${esc(account.running_count)}</td>
          <td>${esc(account.pending_count)}</td>
          <td>${esc(account.allowed_submit)}</td>
          <td>${esc(account.active_slots)}</td>
          <td>${esc(account.live_status)}</td>
          <td>${esc(account.readiness_status || '-')}</td>
        `;
        accountsBody.appendChild(tr);
      }
      renderAlerts(overview.alerts || []);
    }

    async function refreshDetails() {
      if (!document.getElementById('detail-panel').open) {
        return;
      }
      const [events, workers, slots] = await Promise.all([
        fetchJson('/api/events/recent?limit=24'),
        fetchJson('/api/workers'),
        fetchJson('/api/slots?limit=40'),
      ]);

      const eventsBody = document.getElementById('events-body');
      eventsBody.innerHTML = '';
      for (const event of (events.events || [])) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${esc(event.ts)}</td>
          <td>${esc(event.source)}</td>
          <td>${esc(event.entity_id)}</td>
          <td>${esc(event.stage)}</td>
          <td>${esc(event.message)}</td>
        `;
        eventsBody.appendChild(tr);
      }

      const workersBody = document.getElementById('workers-body');
      workersBody.innerHTML = '';
      for (const worker of (workers.workers || [])) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${esc(worker.worker_id)}</td>
          <td>${esc(worker.worker_state)}</td>
          <td>${esc(worker.tunnel_state)}</td>
          <td>${esc(worker.observed_node)}</td>
          <td>${esc(worker.slots_configured)}</td>
          <td>${esc(worker.heartbeat_age_seconds)}</td>
        `;
        workersBody.appendChild(tr);
      }

      const slotsBody = document.getElementById('slots-body');
      slotsBody.innerHTML = '';
      for (const slot of (slots.slots || [])) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><a href="/api/slots/${encodeURIComponent(slot.slot_id)}/timeline" target="_blank">${esc(slot.slot_id)}</a></td>
          <td>${esc(slot.job_id)}</td>
          <td>${esc(slot.account_id)}</td>
          <td>${esc(slot.state)}</td>
          <td>${esc(slot.attempt_no)}</td>
          <td>${esc(slot.updated_at)}</td>
        `;
        slotsBody.appendChild(tr);
      }
    }

    async function refresh() {
      try {
        await refreshOverview();
        await refreshDetails();
      } catch (error) {
        console.error(error);
      }
    }

    refresh();
    setInterval(refreshOverview, 5000);
    setInterval(refreshDetails, 30000);
    document.getElementById('detail-panel').addEventListener('toggle', () => { if (document.getElementById('detail-panel').open) refreshDetails(); });
  </script>
</body>
</html>
""".replace("__APP_VERSION__", version)
