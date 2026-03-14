from __future__ import annotations

import csv
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

from peetsfea_runner.state_store import StateStore, _GLOBAL_DUCKDB_LOCK
from peetsfea_runner.version import get_version


APP_VERSION = get_version()
_OUTPUT_VARIABLES_REQUIRED_COLUMNS = frozenset(
    {
        "source_aedt_path",
        "source_case_dir",
        "source_aedt_name",
        "coil_groups_0__count_mode",
        "coil_shape_inner_margin_x",
        "coil_spacing_tx_vertical_center_gap_mm",
        "ferrite_present",
        "tx_region_z_parts_dd_z_mm",
        "k_ratio",
        "Lrx_uH",
    }
)
_THROUGHPUT_BASELINE_SLOTS_PER_HOUR = 50.0
_THROUGHPUT_TARGET_SLOTS_PER_HOUR = 200.0
_BAD_NODE_TMP_FREE_THRESHOLD_MB = 65536
_REAL_INPUT_FLOW_RECENT_WINDOW_MINUTES = 60
_TONIGHT_TARGET_DEADLINE_KST = datetime(2026, 3, 13, 19, 0, tzinfo=timezone(timedelta(hours=9)))
_DISPATCH_MODE_ALLOWED = frozenset({"run", "drain"})
_SLOT_QUEUED_STATES = frozenset({"QUEUED", "RETRY_QUEUED"})
_SLOT_ACTIVE_STATES = frozenset({"ASSIGNED", "LEASED", "DOWNLOADING", "UPLOADING", "RUNNING", "COLLECTING"})
_SLOT_COMPLETED_STATES = frozenset({"SUCCEEDED", "FAILED", "QUARANTINED"})


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


def _slot_summary_payload(slot_state_counts: dict[str, int] | None) -> dict[str, int]:
    slot_state_counts = slot_state_counts or {}
    return {
        "queued_slots": sum(int(slot_state_counts.get(state, 0)) for state in _SLOT_QUEUED_STATES),
        "active_slots": sum(int(slot_state_counts.get(state, 0)) for state in _SLOT_ACTIVE_STATES),
        "completed_slots": sum(int(slot_state_counts.get(state, 0)) for state in _SLOT_COMPLETED_STATES),
        "succeeded_slots": int(slot_state_counts.get("SUCCEEDED", 0)),
        "failed_slots": int(slot_state_counts.get("FAILED", 0)),
        "quarantined_slots": int(slot_state_counts.get("QUARANTINED", 0)),
    }


def _classify_slot_input_source(input_path: str | None) -> str:
    if not input_path:
        return "other"
    repo_root = Path(__file__).resolve().parent.parent
    original_root = (repo_root / "original").resolve()
    sample_path = (repo_root / "examples" / "sample.aedt").resolve()
    path = Path(str(input_path)).expanduser()
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    if resolved == sample_path or resolved.name == "sample.aedt" or "tonight-canary" in resolved.parts:
        return "sample"
    if resolved == original_root or original_root in resolved.parents:
        return "original"
    return "other"


def _slot_source_mix_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    payload = {
        "sample_slots": 0,
        "original_slots": 0,
        "other_slots": 0,
        "sample_queued_slots": 0,
        "original_queued_slots": 0,
        "other_queued_slots": 0,
        "sample_active_slots": 0,
        "original_active_slots": 0,
        "other_active_slots": 0,
        "sample_completed_slots": 0,
        "original_completed_slots": 0,
        "other_completed_slots": 0,
        "sample_recent_completed_slots": 0,
        "original_recent_completed_slots": 0,
        "other_recent_completed_slots": 0,
        "sample_recent_succeeded_slots": 0,
        "original_recent_succeeded_slots": 0,
        "other_recent_succeeded_slots": 0,
        "sample_recent_failed_slots": 0,
        "original_recent_failed_slots": 0,
        "other_recent_failed_slots": 0,
        "dominant_source": "unknown",
    }
    if run_id is None:
        return payload
    recent_completed_cutoff = (datetime.now(tz=timezone.utc) - timedelta(minutes=_REAL_INPUT_FLOW_RECENT_WINDOW_MINUTES)).isoformat()
    rows = _query(
        db_path,
        """
        SELECT
            input_path,
            state,
            COUNT(*),
            SUM(
                CASE
                    WHEN state IN ('SUCCEEDED', 'FAILED', 'QUARANTINED') AND updated_at >= ?
                    THEN 1 ELSE 0
                END
            ) AS recent_completed_count
        FROM slot_tasks
        WHERE run_id = ?
        GROUP BY input_path, state
        """,
        [recent_completed_cutoff, run_id],
    )
    for input_path, state, count_value, recent_completed_value in rows:
        count = int(count_value or 0)
        source = _classify_slot_input_source(str(input_path or ""))
        payload[f"{source}_slots"] += count
        normalized_state = str(state or "")
        if normalized_state in _SLOT_QUEUED_STATES:
            payload[f"{source}_queued_slots"] += count
        elif normalized_state in _SLOT_ACTIVE_STATES:
            payload[f"{source}_active_slots"] += count
        elif normalized_state in _SLOT_COMPLETED_STATES:
            payload[f"{source}_completed_slots"] += count
            recent_completed_count = int(recent_completed_value or 0)
            payload[f"{source}_recent_completed_slots"] += recent_completed_count
            if normalized_state == "SUCCEEDED":
                payload[f"{source}_recent_succeeded_slots"] += recent_completed_count
            else:
                payload[f"{source}_recent_failed_slots"] += recent_completed_count
    dominant_source = max(("sample", "original", "other"), key=lambda key: int(payload[f"{key}_slots"]))
    payload["dominant_source"] = dominant_source if int(payload[f"{dominant_source}_slots"]) > 0 else "unknown"
    return payload


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
    rolling_throughput: dict[str, object] | None = None,
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
        **(rolling_throughput or {}),
    }


def _output_variables_csv_gate_reason(*, output_root: Path) -> str:
    csv_paths = _artifact_file_candidates(output_root, "output_variables.csv")
    if not csv_paths:
        return "output_variables_csv_missing"
    for csv_path in csv_paths:
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                header = next(csv.reader(handle), [])
        except (OSError, csv.Error):
            header = []
        normalized_header = {column.strip() for column in header if column.strip()}
        if _OUTPUT_VARIABLES_REQUIRED_COLUMNS.issubset(normalized_header):
            return "ok"
    return "output_variables_csv_schema_invalid"


def _artifact_file_candidates(output_root: Path, filename: str) -> list[Path]:
    direct_path = output_root / filename
    if direct_path.is_file():
        return [direct_path]
    if output_root.exists():
        return sorted(path for path in output_root.rglob(filename) if path.is_file())
    return []


def _first_resolved_path(paths: list[Path]) -> str | None:
    if not paths:
        return None
    return str(paths[0].resolve())


def _canary_artifact_status_payload(output_root: str | None) -> dict[str, object]:
    if not output_root:
        return {
            "canary_output_root_exists": None,
            "canary_materialized_output_present": None,
            "canary_output_variables_csv_gate": None,
            "canary_output_variables_csv_path": None,
            "canary_run_log_path": None,
            "canary_exit_code_path": None,
        }
    root = Path(output_root).expanduser()
    output_root_exists = root.exists()
    materialized_output_present = False
    csv_gate = "output_variables_csv_missing"
    csv_paths: list[Path] = []
    run_log_paths: list[Path] = []
    exit_code_paths: list[Path] = []
    if output_root_exists:
        csv_paths = _artifact_file_candidates(root, "output_variables.csv")
        run_log_paths = _artifact_file_candidates(root, "run.log")
        exit_code_paths = _artifact_file_candidates(root, "exit.code")
        materialized_output_present = bool(run_log_paths) and bool(exit_code_paths)
        csv_gate = _output_variables_csv_gate_reason(output_root=root)
    return {
        "canary_output_root_exists": output_root_exists,
        "canary_materialized_output_present": materialized_output_present,
        "canary_output_variables_csv_gate": csv_gate,
        "canary_output_variables_csv_path": _first_resolved_path(csv_paths),
        "canary_run_log_path": _first_resolved_path(run_log_paths),
        "canary_exit_code_path": _first_resolved_path(exit_code_paths),
    }


def _rolling_valid_csv_throughput_payload(
    db_path: Path,
    *,
    run_id: str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    payload = {
        "valid_succeeded_slots_30m": 0,
        "valid_succeeded_slots_60m": 0,
        "rolling_30m_throughput": 0.0,
        "rolling_60m_throughput": 0.0,
        "throughput_baseline_slots_per_hour": _THROUGHPUT_BASELINE_SLOTS_PER_HOUR,
        "throughput_target_slots_per_hour": _THROUGHPUT_TARGET_SLOTS_PER_HOUR,
        "throughput_measurement_basis": "valid_output_variables_csv",
    }
    if run_id is None:
        return payload

    reference_now = datetime.now(tz=timezone.utc) if now is None else now
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=timezone.utc)
    cutoff_60m = (reference_now - timedelta(minutes=60)).isoformat()
    rows = _query(
        db_path,
        """
        SELECT output_path, updated_at
        FROM slot_tasks
        WHERE run_id = ? AND state = 'SUCCEEDED' AND updated_at >= ?
        ORDER BY updated_at DESC
        """,
        [run_id, cutoff_60m],
    )

    valid_succeeded_slots_30m = 0
    valid_succeeded_slots_60m = 0
    for output_path, updated_at in rows:
        parsed_updated_at = _parse_iso(str(updated_at) if updated_at is not None else None)
        if parsed_updated_at is None:
            continue
        if parsed_updated_at.tzinfo is None:
            parsed_updated_at = parsed_updated_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (reference_now - parsed_updated_at).total_seconds())
        if age_seconds > 3600:
            continue
        if _output_variables_csv_gate_reason(output_root=Path(str(output_path)).expanduser()) != "ok":
            continue
        valid_succeeded_slots_60m += 1
        if age_seconds <= 1800:
            valid_succeeded_slots_30m += 1

    payload["valid_succeeded_slots_30m"] = valid_succeeded_slots_30m
    payload["valid_succeeded_slots_60m"] = valid_succeeded_slots_60m
    payload["rolling_30m_throughput"] = round(valid_succeeded_slots_30m * 2.0, 2)
    payload["rolling_60m_throughput"] = round(float(valid_succeeded_slots_60m), 2)
    return payload


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
    elif normalized_stage in {"ACCOUNT_COOLDOWN"}:
        category = "capacity"
        alertable = True
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"CONTROL_TUNNEL_READY", "CONTROL_TUNNEL_HEARTBEAT"}:
        category = "recovery"
        alertable = False
        severity = normalized_level
    elif normalized_stage in {
        "CONTROL_TUNNEL_LOST",
        "CONTROL_TUNNEL_DEGRADED",
        "RETURN_PATH_DNS_FAILURE",
        "RETURN_PATH_CONNECT_FAILURE",
        "RETURN_PATH_AUTH_FAILURE",
        "RETURN_PATH_PORT_MISMATCH",
    }:
        category = "recovery"
        alertable = True
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"WORKER_LOOP_RECOVERING", "SLURM_WORKERS_REDISCOVERED", "WORKER_DEATH_DETECTED"} or "RECOVER" in normalized_stage:
        category = "recovery"
        alertable = normalized_stage != "CONTROL_TUNNEL_RECOVERED"
        severity = "WARN" if normalized_level == "INFO" else normalized_level
    elif normalized_stage in {"RETRY_BACKOFF", "COLLECT_PROBE"}:
        category = "recovery"
        alertable = normalized_stage == "COLLECT_PROBE" and "marker_present=False" in message
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
    real_cutover: dict[str, object] | None = None,
    real_input_flow: dict[str, object] | None = None,
    now: datetime | None = None,
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
    throughput_feasibility = _throughput_feasibility_payload(_throughput_compare_payload(throughput_kpi), now=now)
    throughput_target_status = str(throughput_feasibility.get("status") or "")
    if throughput_target_status in {"OFF_TRACK", "MISSED"}:
        add_alert(
            {
                "alert_key": f"THROUGHPUT_TARGET_{throughput_target_status}",
                "severity": "ERROR" if throughput_target_status == "MISSED" else "WARN",
                "category": "throughput",
                "account_id": None,
                "message": str(throughput_feasibility.get("reason") or throughput_target_status),
            }
        )
    if real_cutover and bool(real_cutover.get("drift_active")):
        decision = str(real_cutover.get("decision") or "UNKNOWN")
        actual_input_source_policy = str(real_cutover.get("actual_input_source_policy") or "")
        add_alert(
            {
                "alert_key": "REAL_CUTOVER_DRIFT",
                "severity": "ERROR" if actual_input_source_policy == "allow_original" and decision != "GO" else "WARN",
                "category": "lifecycle",
                "account_id": None,
                "message": (
                    f"{real_cutover.get('alignment_reason') or 'real cutover drift'} "
                    f"decision={decision} action={real_cutover.get('action') or 'unknown'}"
                ),
            }
        )
    if real_input_flow and bool(real_input_flow.get("flow_absent")):
        add_alert(
            {
                "alert_key": "REAL_INPUT_FLOW_ABSENT",
                "severity": "WARN",
                "category": "lifecycle",
                "account_id": None,
                "message": str(real_input_flow.get("reason") or "real input flow absent"),
            }
        )
    if real_input_flow and str(real_input_flow.get("status") or "") == "RECENT_FAILED_ONLY":
        add_alert(
            {
                "alert_key": "REAL_INPUT_FLOW_FAILED_ONLY",
                "severity": "WARN",
                "category": "lifecycle",
                "account_id": None,
                "message": str(real_input_flow.get("reason") or "real input flow failed only"),
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


def _first_matching_event(
    events: list[dict[str, object]],
    *,
    stages: set[str] | None = None,
    message_fragment: str | None = None,
) -> dict[str, object] | None:
    normalized_stages = {stage.upper() for stage in stages} if stages is not None else None
    normalized_fragment = message_fragment.lower() if message_fragment is not None else None
    for event in events:
        stage = str(event.get("stage") or "").upper()
        message = str(event.get("message") or "")
        if normalized_stages is not None and stage not in normalized_stages:
            continue
        if normalized_fragment is not None and normalized_fragment not in message.lower():
            continue
        return event
    return None


def _ops_triage_payload(
    *,
    events: list[dict[str, object]],
    throughput_kpi: dict[str, object] | None,
    worker_mix: dict[str, object] | None,
    tunnel_summary: dict[str, object] | None,
    slot_state_counts: dict[str, int] | None,
    now: datetime | None = None,
) -> dict[str, object]:
    worker_mix = worker_mix or {}
    tunnel_summary = tunnel_summary or {}
    slot_state_counts = slot_state_counts or {}

    latest_canary_event = _first_matching_event(events, stages={"CANARY_FAILED", "CANARY_PASSED"})
    latest_restart_event = _first_matching_event(
        events,
        stages={"WORKER_LOOP_ERROR", "WORKER_LOOP_BLOCKED", "WORKER_LOOP_OK", "WORKER_LOOP_IDLE", "WORKER_LOOP_RECOVERING"},
    )
    latest_resource_event = _first_matching_event(
        events,
        stages={"BAD_NODE_REGISTERED", "BAD_NODES_INVALID", "BAD_NODE_EXCLUDE_ACTIVE"},
    )
    latest_no_space_event = _first_matching_event(events, message_fragment="No space left on device")
    latest_tunnel_event = _first_matching_event(events, stages={"CONTROL_TUNNEL_DEGRADED"})
    truth_refreshed = _first_matching_event(events, stages={"SLURM_TRUTH_REFRESHED"}) is not None

    csv_state = "UNKNOWN"
    if latest_canary_event is not None:
        csv_state = "RED" if str(latest_canary_event.get("stage")) == "CANARY_FAILED" else "GREEN"

    restart_state = "UNKNOWN"
    if latest_restart_event is not None:
        restart_stage = str(latest_restart_event.get("stage") or "")
        if restart_stage in {"WORKER_LOOP_ERROR", "WORKER_LOOP_BLOCKED"}:
            restart_state = "RED"
        elif restart_stage in {"WORKER_LOOP_OK", "WORKER_LOOP_IDLE"}:
            restart_state = "GREEN"
        else:
            restart_state = "DEGRADED"

    tunnel_degraded = bool(tunnel_summary.get("degraded_workers") or 0) or bool(tunnel_summary.get("stale_workers") or 0)
    resource_state = "UNKNOWN"
    if latest_resource_event is not None or latest_no_space_event is not None:
        resource_state = "RED"
    elif tunnel_degraded or latest_tunnel_event is not None:
        resource_state = "DEGRADED"
    elif truth_refreshed:
        resource_state = "GREEN"

    rolling_60m = float(throughput_kpi.get("rolling_60m_throughput") or 0.0) if throughput_kpi else 0.0
    baseline_60m = (
        float(throughput_kpi.get("throughput_baseline_slots_per_hour") or _THROUGHPUT_BASELINE_SLOTS_PER_HOUR)
        if throughput_kpi
        else _THROUGHPUT_BASELINE_SLOTS_PER_HOUR
    )
    idle_slots = int(worker_mix.get("idle_slots") or 0)
    failed_slots = int(slot_state_counts.get("FAILED", 0))
    quarantined_slots = int(slot_state_counts.get("QUARANTINED", 0))
    overload_signals = failed_slots + quarantined_slots + (1 if resource_state == "DEGRADED" else 0)
    throughput_state = "UNKNOWN"
    if throughput_kpi is not None:
        if str(throughput_kpi.get("throughput_mode") or "") == "IDLE":
            throughput_state = "IDLE"
        elif overload_signals > 0 and rolling_60m < baseline_60m:
            throughput_state = "OVERLOADED"
        elif rolling_60m >= baseline_60m:
            throughput_state = "RISING"
        elif idle_slots > 0:
            throughput_state = "STALLED"
        else:
            throughput_state = "LOW"
    throughput_feasibility = _throughput_feasibility_payload(_throughput_compare_payload(throughput_kpi), now=now)
    throughput_target_track_status = str(throughput_feasibility.get("status") or "UNKNOWN")
    throughput_target_track_reason = str(throughput_feasibility.get("reason") or "")

    priority = "none"
    status = "GO"
    headline = "green"
    reason = "csv/restart/resource gates are not red"
    if csv_state == "RED":
        priority = "csv"
        status = "NO_GO"
        headline = "CSV gate red"
        reason = str(latest_canary_event.get("message") or "CANARY_FAILED") if latest_canary_event is not None else "CANARY_FAILED"
    elif restart_state == "RED":
        priority = "restart"
        status = "NO_GO"
        headline = "restart gate red"
        reason = str(latest_restart_event.get("message") or "WORKER_LOOP_ERROR") if latest_restart_event is not None else "WORKER_LOOP_ERROR"
    elif resource_state == "RED":
        priority = "resource"
        status = "CONDITIONAL_NO_GO"
        headline = "resource triage required"
        resource_event = latest_no_space_event or latest_resource_event
        reason = (
            str(resource_event.get("message") or resource_event.get("stage") or "bad node/resource signal")
            if resource_event is not None
            else "bad node/resource signal"
        )
    elif throughput_state == "OVERLOADED":
        priority = "throughput"
        status = "ADJUST"
        headline = "throughput overloaded"
        reason = (
            f"rolling_60m={rolling_60m:.1f} baseline={baseline_60m:.1f} "
            f"failed_slots={failed_slots} quarantined_slots={quarantined_slots} resource_state={resource_state}"
        )
    elif throughput_state == "STALLED":
        priority = "throughput"
        status = "ADJUST"
        headline = "throughput stalled"
        reason = f"rolling_60m={rolling_60m:.1f} idle_slots={idle_slots} failed_slots={failed_slots}"
    elif throughput_target_track_status == "MISSED":
        priority = "throughput"
        status = "ADJUST"
        headline = "throughput target missed"
        reason = throughput_target_track_reason or "throughput target missed"
    elif throughput_target_track_status == "OFF_TRACK":
        priority = "throughput"
        status = "ADJUST"
        headline = "throughput off track"
        reason = throughput_target_track_reason or "throughput off track"

    return {
        "status": status,
        "priority": priority,
        "headline": headline,
        "reason": reason,
        "csv_state": csv_state,
        "restart_state": restart_state,
        "resource_state": resource_state,
        "throughput_state": throughput_state,
        "throughput_target_track_status": throughput_target_track_status,
        "throughput_target_track_reason": throughput_target_track_reason,
        "canary_stage": latest_canary_event.get("stage") if latest_canary_event is not None else None,
        "restart_stage": latest_restart_event.get("stage") if latest_restart_event is not None else None,
        "resource_stage": (
            (latest_no_space_event or latest_resource_event).get("stage")
            if (latest_no_space_event or latest_resource_event) is not None
            else None
        ),
        "truth_refreshed": truth_refreshed,
        "tunnel_degraded": tunnel_degraded,
        "rolling_60m_throughput": rolling_60m,
        "idle_slots": idle_slots,
        "failed_slots": failed_slots,
        "quarantined_slots": quarantined_slots,
        "overload_signals": overload_signals,
    }


def _ops_triage_with_real_cutover(
    ops_triage: dict[str, object] | None,
    *,
    real_cutover: dict[str, object] | None,
) -> dict[str, object]:
    payload = dict(ops_triage or {})
    real_cutover = real_cutover or {}
    drift_active = bool(real_cutover.get("drift_active"))
    payload["real_cutover_drift_active"] = drift_active
    payload["real_cutover_alignment_status"] = real_cutover.get("alignment_status")
    if not drift_active:
        return payload
    decision = str(real_cutover.get("decision") or "UNKNOWN")
    action = str(real_cutover.get("action") or "unknown")
    actual_input_source_policy = str(real_cutover.get("actual_input_source_policy") or "")
    alignment_reason = str(real_cutover.get("alignment_reason") or real_cutover.get("reason") or "real cutover drift")
    payload["priority"] = "cutover"
    payload["headline"] = "real cutover drift"
    payload["reason"] = f"{alignment_reason} decision={decision} action={action}"
    payload["status"] = "NO_GO" if actual_input_source_policy == "allow_original" and decision != "GO" else "ADJUST"
    return payload


def _ops_triage_with_real_input_flow(
    ops_triage: dict[str, object] | None,
    *,
    real_input_flow: dict[str, object] | None,
) -> dict[str, object]:
    payload = dict(ops_triage or {})
    real_input_flow = real_input_flow or {}
    flow_status = str(real_input_flow.get("status") or "UNKNOWN")
    flow_absent = bool(real_input_flow.get("flow_absent"))
    flow_failed_only = flow_status == "RECENT_FAILED_ONLY"
    payload["real_input_flow_status"] = flow_status
    payload["real_input_flow_absent"] = flow_absent
    payload["real_input_flow_failed_only"] = flow_failed_only
    if bool(payload.get("real_cutover_drift_active")) or str(payload.get("status") or "") == "NO_GO":
        return payload
    if flow_absent:
        payload["priority"] = "flow"
        payload["status"] = "ADJUST"
        payload["headline"] = "real input flow absent"
        payload["reason"] = str(real_input_flow.get("reason") or "real input flow absent")
    elif flow_failed_only:
        payload["priority"] = "flow"
        payload["status"] = "ADJUST"
        payload["headline"] = "real input flow failed only"
        payload["reason"] = str(real_input_flow.get("reason") or "real input flow failed only")
    return payload


def _real_input_flow_signal_payload(
    *,
    real_cutover: dict[str, object] | None,
    slot_source_mix: dict[str, object] | None,
) -> dict[str, object]:
    real_cutover = real_cutover or {}
    slot_source_mix = slot_source_mix or {}
    decision = str(real_cutover.get("decision") or "UNKNOWN")
    sample_active_slots = int(slot_source_mix.get("sample_active_slots") or 0)
    sample_queued_slots = int(slot_source_mix.get("sample_queued_slots") or 0)
    original_active_slots = int(slot_source_mix.get("original_active_slots") or 0)
    original_queued_slots = int(slot_source_mix.get("original_queued_slots") or 0)
    original_completed_slots = int(slot_source_mix.get("original_completed_slots") or 0)
    original_recent_completed_slots = int(slot_source_mix.get("original_recent_completed_slots") or 0)
    original_recent_succeeded_slots = int(slot_source_mix.get("original_recent_succeeded_slots") or 0)
    original_recent_failed_slots = int(slot_source_mix.get("original_recent_failed_slots") or 0)
    if decision != "GO":
        return {
            "status": "INACTIVE",
            "flow_absent": False,
            "reason": f"decision={decision}",
        }
    if original_active_slots > 0 or original_queued_slots > 0:
        return {
            "status": "PRESENT",
            "flow_absent": False,
            "reason": f"original_active_slots={original_active_slots} original_queued_slots={original_queued_slots}",
        }
    if original_recent_succeeded_slots > 0:
        return {
            "status": "RECENT",
            "flow_absent": False,
            "reason": (
                f"decision={decision} original_recent_succeeded_slots={original_recent_succeeded_slots} "
                f"original_recent_failed_slots={original_recent_failed_slots} "
                f"original_recent_completed_slots={original_recent_completed_slots} "
                f"original_completed_slots={original_completed_slots} "
                f"original_active_slots={original_active_slots} original_queued_slots={original_queued_slots}"
            ),
        }
    if original_recent_failed_slots > 0:
        return {
            "status": "RECENT_FAILED_ONLY",
            "flow_absent": False,
            "reason": (
                f"decision={decision} original_recent_succeeded_slots={original_recent_succeeded_slots} "
                f"original_recent_failed_slots={original_recent_failed_slots} "
                f"original_recent_completed_slots={original_recent_completed_slots} "
                f"original_completed_slots={original_completed_slots} "
                f"original_active_slots={original_active_slots} original_queued_slots={original_queued_slots}"
            ),
        }
    if sample_active_slots > 0 or sample_queued_slots > 0:
        return {
            "status": "ABSENT",
            "flow_absent": True,
            "reason": (
                f"decision={decision} original_active_slots={original_active_slots} "
                f"original_queued_slots={original_queued_slots} original_completed_slots={original_completed_slots} "
                f"original_recent_succeeded_slots={original_recent_succeeded_slots} "
                f"original_recent_failed_slots={original_recent_failed_slots} "
                f"sample_active_slots={sample_active_slots} "
                f"sample_queued_slots={sample_queued_slots}"
            ),
        }
    return {
        "status": "IDLE",
        "flow_absent": False,
        "reason": f"decision={decision} no queued_or_active_slots",
    }


def _real_cutover_decision_payload(
    *,
    ops_triage: dict[str, object] | None,
    rollout_status: dict[str, object] | None,
    service_boundary: dict[str, object] | None = None,
    real_input_flow: dict[str, object] | None = None,
    ops_controls: dict[str, object] | None = None,
) -> dict[str, object]:
    ops_triage = ops_triage or {}
    rollout_status = rollout_status or {}
    service_boundary = service_boundary or {}
    real_input_flow = real_input_flow or {}
    ops_controls = ops_controls or {}

    canary_status = str(rollout_status.get("canary_status") or "UNKNOWN")
    canary_gate = str(rollout_status.get("canary_gate") or rollout_status.get("canary_reason") or "UNKNOWN")
    canary_reason = str(rollout_status.get("canary_reason") or canary_gate or "UNKNOWN")
    csv_state = str(ops_triage.get("csv_state") or "UNKNOWN")
    restart_state = str(ops_triage.get("restart_state") or rollout_status.get("restart_status") or "UNKNOWN")
    rediscovery_status = str(rollout_status.get("rediscovery_status") or "UNKNOWN")
    resource_state = str(rollout_status.get("resource_status") or ops_triage.get("resource_state") or "UNKNOWN")
    throughput_state = str(ops_triage.get("throughput_state") or "UNKNOWN")
    full_rollout_ready = bool(rollout_status.get("full_rollout_ready"))
    bad_node_active = bool(rollout_status.get("bad_node_active"))
    actual_input_source_policy = str(service_boundary.get("input_source_policy") or "") or None
    real_input_flow_status = str(real_input_flow.get("status") or "UNKNOWN")
    real_input_flow_reason = str(real_input_flow.get("reason") or "real input flow unavailable")
    dispatch_mode = str(ops_controls.get("dispatch_mode") or "run")
    dispatch_mode_ready = bool(ops_controls.get("dispatch_mode_ready"))
    stop_gate_state = str(ops_controls.get("stop_gate_state") or "UNKNOWN")
    stop_gate_detail = str(ops_controls.get("stop_gate_detail") or "restart controls unavailable")
    restart_phase = str(ops_controls.get("restart_phase") or "UNKNOWN")
    restart_phase_detail = str(ops_controls.get("restart_phase_detail") or "restart phase unavailable")
    canary_progress_state = str(rollout_status.get("canary_progress_state") or "UNKNOWN")
    canary_progress_within_10m = rollout_status.get("canary_progress_within_10m")
    canary_completed_within_10m = rollout_status.get("canary_completed_within_10m")
    canary_duration_seconds = rollout_status.get("canary_duration_seconds")
    restart_bundle_ready = (
        restart_phase == "START_CONFIRMED"
        or (
            bool(rollout_status.get("worker_loop_started_after_canary"))
            and bool(rollout_status.get("restart_signal_after_canary"))
            and restart_state == "GREEN"
            and bool(rollout_status.get("truth_refreshed_after_canary"))
        )
    )

    drain_gate_state = "UNKNOWN"
    drain_gate_reason = "restart controls unavailable"
    if ops_controls:
        if dispatch_mode_ready:
            drain_gate_state = "GREEN"
            drain_gate_reason = f"dispatch_mode={dispatch_mode} dispatch_mode_ready=true"
        elif stop_gate_state == "PASS":
            drain_gate_state = "GREEN"
            drain_gate_reason = stop_gate_detail
        elif stop_gate_state == "FAIL":
            drain_gate_state = "RED"
            drain_gate_reason = stop_gate_detail
        elif canary_status in {"PASSED", "FAILED"} or canary_progress_state in {"RUNNING", "BLOCKED", "PASSED", "FAILED"} or full_rollout_ready:
            drain_gate_state = "RED"
            drain_gate_reason = f"stop_gate_state={stop_gate_state} {stop_gate_detail}"
        else:
            drain_gate_state = "PENDING"
            drain_gate_reason = f"stop_gate_state={stop_gate_state} {stop_gate_detail}"

    restart_bundle_state = "UNKNOWN"
    restart_bundle_reason = "restart bundle unavailable"
    if ops_controls and canary_status == "PASSED" and canary_gate == "ok" and full_rollout_ready:
        if restart_bundle_ready:
            restart_bundle_state = "GREEN"
            restart_bundle_reason = restart_phase_detail
        else:
            restart_bundle_state = "RED"
            restart_bundle_reason = f"restart_phase={restart_phase} {restart_phase_detail}"

    canary_sla_state = "UNKNOWN"
    canary_sla_reason = "canary timing unavailable"
    if canary_status == "PASSED" and canary_gate == "ok":
        if canary_completed_within_10m is True:
            canary_sla_state = "GREEN"
            canary_sla_reason = (
                f"canary_completed_within_10m={canary_completed_within_10m} "
                f"canary_duration_seconds={canary_duration_seconds if canary_duration_seconds is not None else '-'}"
            )
        else:
            canary_sla_state = "RED"
            canary_sla_reason = (
                f"canary_completed_within_10m={canary_completed_within_10m} "
                f"canary_duration_seconds={canary_duration_seconds if canary_duration_seconds is not None else '-'} "
                f"canary_progress_state={canary_progress_state} "
                f"canary_progress_within_10m={canary_progress_within_10m}"
            )

    csv_blocking_reasons = {"output_variables_csv_schema_invalid", "output_variables_csv_missing"}
    canary_blocking_reasons = csv_blocking_reasons | {"materialized_output_missing", "submit_missing"}

    def with_alignment(payload: dict[str, object]) -> dict[str, object]:
        desired_input_source_policy = "allow_original" if payload["decision"] == "GO" else "sample_only"
        if actual_input_source_policy is None:
            alignment_status = "UNKNOWN"
        elif actual_input_source_policy == desired_input_source_policy:
            alignment_status = "ALIGNED"
        else:
            alignment_status = "DRIFT"
        return {
            **payload,
            "desired_input_source_policy": desired_input_source_policy,
            "actual_input_source_policy": actual_input_source_policy,
            "alignment_status": alignment_status,
            "drift_active": alignment_status == "DRIFT",
            "drain_gate_state": drain_gate_state,
            "drain_gate_reason": drain_gate_reason,
            "restart_bundle_state": restart_bundle_state,
            "restart_bundle_reason": restart_bundle_reason,
            "canary_sla_state": canary_sla_state,
            "canary_sla_reason": canary_sla_reason,
            "alignment_reason": (
                f"desired_input_source_policy={desired_input_source_policy} "
                f"actual_input_source_policy={actual_input_source_policy or 'unknown'}"
            ),
        }

    if csv_state == "RED" or canary_gate in csv_blocking_reasons or canary_reason in csv_blocking_reasons:
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "04",
            "reason": f"csv_gate={canary_gate} canary_reason={canary_reason}",
        })
    if canary_sla_state == "RED":
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "01",
            "reason": f"canary_sla_state={canary_sla_state} {canary_sla_reason}",
        })
    if drain_gate_state == "RED":
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "01",
            "reason": f"drain_gate_state={drain_gate_state} {drain_gate_reason}",
        })
    if restart_state == "RED" or rediscovery_status == "RED":
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "01",
            "reason": f"restart_state={restart_state} rediscovery_status={rediscovery_status}",
        })
    if restart_bundle_state == "RED":
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "01",
            "reason": f"restart_bundle_state={restart_bundle_state} {restart_bundle_reason}",
        })
    if canary_status == "FAILED" or canary_gate in canary_blocking_reasons or canary_reason in canary_blocking_reasons:
        return with_alignment({
            "decision": "NO_GO",
            "action": "block_real_aedt",
            "workstream": "01",
            "reason": f"canary_status={canary_status} canary_reason={canary_reason}",
        })
    if canary_status == "PASSED" and canary_gate == "ok" and rediscovery_status == "GREEN" and full_rollout_ready:
        if resource_state == "RED" or bad_node_active:
            return with_alignment({
                "decision": "CONDITIONAL_GO",
                "action": "recheck_after_workstream",
                "workstream": "03",
                "reason": f"resource_state={resource_state} bad_node_active={bad_node_active}",
            })
        if throughput_state in {"STALLED", "LOW", "OVERLOADED"}:
            return with_alignment({
                "decision": "CONDITIONAL_GO",
                "action": "recheck_after_workstream",
                "workstream": "02",
                "reason": f"throughput_state={throughput_state} rolling_60m={ops_triage.get('rolling_60m_throughput') or 0}",
            })
        if real_input_flow_status == "RECENT_FAILED_ONLY":
            return with_alignment({
                "decision": "CONDITIONAL_GO",
                "action": "recheck_after_workstream",
                "workstream": "03",
                "reason": f"real_input_flow_status={real_input_flow_status} {real_input_flow_reason}",
            })
        return with_alignment({
            "decision": "GO",
            "action": "resume_real_aedt",
            "workstream": "-",
            "reason": (
                f"canary_status={canary_status} csv_gate={canary_gate} "
                f"rediscovery_status={rediscovery_status} full_rollout_ready={full_rollout_ready}"
            ),
        })
    return with_alignment({
        "decision": "PENDING",
        "action": "wait_validation_lane",
        "workstream": "01",
        "reason": (
            f"canary_status={canary_status} csv_gate={canary_gate} "
            f"rediscovery_status={rediscovery_status} full_rollout_ready={full_rollout_ready}"
        ),
    })


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _extract_token_value(text: str | None, key: str) -> str | None:
    if not text:
        return None
    prefix = f"{key}="
    for token in str(text).split():
        if token.startswith(prefix):
            return token[len(prefix) :]
    return None


def _dispatch_mode_control_path() -> Path:
    return Path(__file__).resolve().parent.parent / "tmp" / "runtime" / "dispatch.mode"


def _systemd_user_service_state_payload(service_name: str | None = None) -> dict[str, object]:
    raw_service_name = service_name or os.getenv("PEETSFEA_WORKER_SERVICE_NAME", "peetsfea-runner")
    unit_name = raw_service_name if raw_service_name.endswith(".service") else f"{raw_service_name}.service"
    cmd = [
        "systemctl",
        "--user",
        "show",
        unit_name,
        "--property=ActiveState",
        "--property=SubState",
        "--property=MainPID",
        "--property=LoadState",
        "--property=UnitFileState",
        "--property=Result",
    ]
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "observed": False,
            "service_name": raw_service_name,
            "unit_name": unit_name,
            "active_state": None,
            "sub_state": None,
            "main_pid": None,
            "load_state": None,
            "unit_file_state": None,
            "result": None,
            "detail": f"systemctl --user show failed reason={exc}",
        }

    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"returncode={completed.returncode}"
        return {
            "observed": False,
            "service_name": raw_service_name,
            "unit_name": unit_name,
            "active_state": None,
            "sub_state": None,
            "main_pid": None,
            "load_state": None,
            "unit_file_state": None,
            "result": None,
            "detail": f"systemctl --user show returncode={completed.returncode} reason={detail}",
        }

    fields: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        fields[key.strip()] = value.strip()
    main_pid_value = fields.get("MainPID", "")
    try:
        main_pid = int(main_pid_value)
    except ValueError:
        main_pid = None
    active_state = fields.get("ActiveState") or None
    sub_state = fields.get("SubState") or None
    return {
        "observed": bool(active_state or sub_state),
        "service_name": raw_service_name,
        "unit_name": unit_name,
        "active_state": active_state,
        "sub_state": sub_state,
        "main_pid": main_pid,
        "load_state": fields.get("LoadState") or None,
        "unit_file_state": fields.get("UnitFileState") or None,
        "result": fields.get("Result") or None,
        "detail": (
            f"ActiveState={active_state or 'unknown'} "
            f"SubState={sub_state or 'unknown'} "
            f"MainPID={main_pid if main_pid is not None else '-'}"
        ),
    }


def _ops_restart_controls_payload(
    *,
    db_path: Path,
    control_path: Path | None = None,
    health: dict[str, object] | None = None,
    rollout_status: dict[str, object] | None = None,
    service_state: dict[str, object] | None = None,
) -> dict[str, object]:
    health = health or {}
    rollout_status = rollout_status or {}
    service_state = _systemd_user_service_state_payload() if service_state is None else service_state
    dispatch_path = _dispatch_mode_control_path() if control_path is None else control_path
    dispatch_mode = "run"
    dispatch_warning = None
    try:
        raw_mode = dispatch_path.read_text(encoding="utf-8").strip().lower()
    except FileNotFoundError:
        raw_mode = ""
    except OSError as exc:
        raw_mode = ""
        dispatch_warning = f"dispatch.mode read failed path={dispatch_path} fallback=run reason={exc}"
    if raw_mode in _DISPATCH_MODE_ALLOWED:
        dispatch_mode = raw_mode
    elif raw_mode:
        dispatch_warning = f"dispatch.mode invalid value={raw_mode!r} path={dispatch_path} fallback=run"

    repo_root = Path(__file__).resolve().parent.parent
    health_status = str(health.get("status") or "UNKNOWN")
    worker_status = str(health.get("worker_status") or "") or "missing"
    last_heartbeat_ts = str(health.get("last_heartbeat_ts") or "")
    heartbeat_age_seconds = health.get("heartbeat_age_seconds")
    last_canary_ts = str(rollout_status.get("last_canary_ts") or "")
    systemd_observed = bool(service_state.get("observed"))
    systemd_unit_name = str(service_state.get("unit_name") or f"{os.getenv('PEETSFEA_WORKER_SERVICE_NAME', 'peetsfea-runner')}.service")
    systemd_active_state = str(service_state.get("active_state") or "")
    systemd_sub_state = str(service_state.get("sub_state") or "")
    systemd_main_pid = service_state.get("main_pid")
    systemd_detail = str(service_state.get("detail") or "systemd state unavailable")
    fallback_control_model = "status -> stop -> canary -> start" if dispatch_mode != "drain" else "drain -> restart -> run"
    if dispatch_mode == "drain":
        stop_gate_state = "PASS"
        stop_gate_detail = (
            f"control_model={fallback_control_model} dispatch_mode={dispatch_mode} "
            f"new_submit_gate=dispatch.mode systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    elif systemd_observed and systemd_active_state == "inactive":
        stop_gate_state = "PASS"
        stop_gate_detail = f"source=systemd {systemd_detail}"
    elif last_canary_ts and _timestamp_after(last_canary_ts, last_heartbeat_ts) and health_status == "STALE" and last_heartbeat_ts:
        stop_gate_state = "PASS"
        stop_gate_detail = (
            f"proxy=heartbeat_stale_before_canary last_heartbeat_ts={last_heartbeat_ts} "
            f"last_canary_ts={last_canary_ts} heartbeat_age_seconds={heartbeat_age_seconds if heartbeat_age_seconds is not None else '-'} "
            f"systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    elif last_canary_ts and health_status != "STALE":
        stop_gate_state = "FAIL"
        stop_gate_detail = (
            f"proxy=heartbeat_live_after_canary health={health_status} worker_status={worker_status} "
            f"last_heartbeat_ts={last_heartbeat_ts or 'missing'} last_canary_ts={last_canary_ts} "
            f"systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    elif last_canary_ts:
        stop_gate_state = "WARN"
        stop_gate_detail = (
            f"proxy=stale_without_order_proof health={health_status} "
            f"last_heartbeat_ts={last_heartbeat_ts or 'missing'} last_canary_ts={last_canary_ts} "
            f"systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    else:
        stop_gate_state = "WARN"
        stop_gate_detail = (
            f"proxy=pending_canary health={health_status} worker_status={worker_status} "
            f"last_heartbeat_ts={last_heartbeat_ts or 'missing'} systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    canary_status = str(rollout_status.get("canary_status") or "UNKNOWN")
    canary_gate = str(rollout_status.get("canary_gate") or rollout_status.get("canary_reason") or "UNKNOWN")
    restart_status = str(rollout_status.get("restart_status") or "UNKNOWN")
    restart_bundle_ready = (
        bool(rollout_status.get("worker_loop_started_after_canary"))
        and bool(rollout_status.get("restart_signal_after_canary"))
        and restart_status == "GREEN"
        and bool(rollout_status.get("truth_refreshed_after_canary"))
    )
    canary_progress_state = str(rollout_status.get("canary_progress_state") or "UNKNOWN")
    canary_progress_detail = str(rollout_status.get("canary_progress_detail") or f"canary_status={canary_status} canary_gate={canary_gate}")
    if restart_bundle_ready:
        restart_phase = "START_CONFIRMED"
        restart_phase_detail = (
            f"restart_status={restart_status} "
            f"last_restart_ts={rollout_status.get('last_restart_ts') or 'missing'} "
            f"truth_after_canary={bool(rollout_status.get('truth_refreshed_after_canary'))}"
        )
    elif canary_progress_state == "FAILED" or bool(rollout_status.get("fallback_active")):
        restart_phase = "CANARY_FAILED"
        restart_phase_detail = canary_progress_detail
    elif canary_progress_state == "PASSED":
        restart_phase = "CANARY_PASSED"
        restart_phase_detail = canary_progress_detail
    elif canary_progress_state == "BLOCKED":
        restart_phase = "CANARY_BLOCKED"
        restart_phase_detail = canary_progress_detail
    elif canary_progress_state == "RUNNING":
        restart_phase = "CANARY_RUNNING"
        restart_phase_detail = canary_progress_detail
    elif stop_gate_state == "PASS":
        restart_phase = "STOP_CONFIRMED"
        restart_phase_detail = stop_gate_detail
    elif systemd_observed or health_status != "UNKNOWN" or worker_status != "missing":
        restart_phase = "STATUS_CHECKED"
        restart_phase_detail = (
            f"health={health_status} worker_status={worker_status} "
            f"systemd={systemd_active_state or 'unknown'}/{systemd_sub_state or 'unknown'}"
        )
    else:
        restart_phase = "STATUS_UNKNOWN"
        restart_phase_detail = systemd_detail
    return {
        "dispatch_mode": dispatch_mode,
        "dispatch_mode_path": str(dispatch_path),
        "dispatch_mode_warning": dispatch_warning,
        "dispatch_mode_ready": dispatch_mode == "drain" and dispatch_warning is None,
        "fallback_control_model": fallback_control_model,
        "systemd_observed": systemd_observed,
        "systemd_unit_name": systemd_unit_name,
        "systemd_active_state": systemd_active_state or None,
        "systemd_sub_state": systemd_sub_state or None,
        "systemd_main_pid": systemd_main_pid,
        "systemd_detail": systemd_detail,
        "stop_gate_state": stop_gate_state,
        "stop_gate_detail": stop_gate_detail,
        "restart_phase": restart_phase,
        "restart_phase_detail": restart_phase_detail,
        "live_db_policy": "retain",
        "live_db_path": str(db_path.expanduser().resolve()),
        "canary_db_root": str((repo_root / "tmp" / "tonight-canary").resolve()),
        "db_reset_allowed": False,
        "db_reset_reason": "schema/state/ingest meaning change required",
    }


def _bad_nodes_control_path() -> Path:
    return Path(__file__).resolve().parent.parent / "tmp" / "runtime" / "bad_nodes.json"


def _canary_validation_root(*, input_dir: str | None, output_root: str | None, db_path: str | None) -> str | None:
    candidates: list[Path] = []
    for raw_path, expected_leaf in ((input_dir, "input"), (output_root, "output")):
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        candidates.append(path.parent if path.name == expected_leaf else path)
    if db_path:
        candidates.append(Path(db_path).expanduser().parent)
    if not candidates:
        return None
    normalized = [str(path.resolve()) for path in candidates]
    if all(value == normalized[0] for value in normalized[1:]):
        return normalized[0]
    return normalized[0]


def _canary_delete_failed_dir(*, delete_failed_dir: str | None, validation_root: str | None) -> str | None:
    if delete_failed_dir:
        return str(Path(delete_failed_dir).expanduser().resolve())
    if validation_root:
        return str((Path(validation_root).expanduser() / "delete_failed").resolve())
    return None


def _active_bad_node_entries(
    *,
    control_path: Path | None = None,
    now: datetime | None = None,
) -> tuple[list[dict[str, object]], list[str]]:
    path = _bad_nodes_control_path() if control_path is None else control_path
    reference = datetime.now(tz=timezone.utc) if now is None else now
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], []
    except OSError as exc:
        return [], [f"bad_nodes.json read failed path={path} reason={exc}"]
    except json.JSONDecodeError as exc:
        return [], [f"bad_nodes.json parse failed path={path} reason={exc}"]

    if isinstance(raw_payload, dict):
        raw_entries = raw_payload.get("bad_nodes")
        if not isinstance(raw_entries, list):
            return [], [f"bad_nodes.json invalid payload path={path} reason=missing_list"]
    elif isinstance(raw_payload, list):
        raw_entries = raw_payload
    else:
        return [], [f"bad_nodes.json invalid payload path={path} reason=unsupported_type"]

    active_entries: list[dict[str, object]] = []
    warnings: list[str] = []
    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} reason=not_object")
            continue
        node = str(entry.get("node", "")).strip()
        reason = str(entry.get("reason", "")).strip()
        first_seen_at = _parse_iso(str(entry.get("first_seen_at") or ""))
        expires_at = _parse_iso(str(entry.get("expires_at") or ""))
        if not node:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} reason=node_missing")
            continue
        if expires_at is None:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} node={node} reason=expires_at_missing")
            continue
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= reference:
            continue
        remaining_minutes = max(int((expires_at - reference).total_seconds() // 60), 0)
        active_entries.append(
            {
                "node": node,
                "reason": reason or None,
                "first_seen_at": first_seen_at.isoformat() if first_seen_at is not None else None,
                "expires_at": expires_at.isoformat(),
                "remaining_minutes": remaining_minutes,
            }
        )
    return active_entries, warnings


def _age_seconds(value: str | None) -> int | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    now = datetime.now(tz=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0, int((now - parsed).total_seconds()))


def _timestamp_after(candidate: str | None, baseline: str | None) -> bool:
    parsed_candidate = _parse_iso(candidate)
    parsed_baseline = _parse_iso(baseline)
    if parsed_candidate is None or parsed_baseline is None:
        return False
    if parsed_candidate.tzinfo is None:
        parsed_candidate = parsed_candidate.replace(tzinfo=timezone.utc)
    if parsed_baseline.tzinfo is None:
        parsed_baseline = parsed_baseline.replace(tzinfo=timezone.utc)
    return parsed_candidate > parsed_baseline


def _seconds_between(start: str | None, end: str | None) -> int | None:
    parsed_start = _parse_iso(start)
    parsed_end = _parse_iso(end)
    if parsed_start is None or parsed_end is None:
        return None
    if parsed_start.tzinfo is None:
        parsed_start = parsed_start.replace(tzinfo=timezone.utc)
    if parsed_end.tzinfo is None:
        parsed_end = parsed_end.replace(tzinfo=timezone.utc)
    return max(0, int((parsed_end - parsed_start).total_seconds()))


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
            tmp_total_mb,
            tmp_used_mb,
            tmp_free_mb,
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
    tmp_free_mb = int(row[8]) if row[8] is not None else None
    tmp_free_status = "UNKNOWN"
    tmp_free_shortfall_mb = None
    if tmp_free_mb is not None:
        if tmp_free_mb < _BAD_NODE_TMP_FREE_THRESHOLD_MB:
            tmp_free_status = "RED"
            tmp_free_shortfall_mb = _BAD_NODE_TMP_FREE_THRESHOLD_MB - tmp_free_mb
        else:
            tmp_free_status = "GREEN"
            tmp_free_shortfall_mb = 0
    return {
        "run_id": row[0],
        "host": row[1],
        "allocated_mem_mb": int(row[2]) if row[2] is not None else None,
        "total_mem_mb": int(row[3]) if row[3] is not None else None,
        "used_mem_mb": int(row[4]) if row[4] is not None else None,
        "free_mem_mb": int(row[5]) if row[5] is not None else None,
        "tmp_total_mb": int(row[6]) if row[6] is not None else None,
        "tmp_used_mb": int(row[7]) if row[7] is not None else None,
        "tmp_free_mb": tmp_free_mb,
        "tmp_free_threshold_mb": _BAD_NODE_TMP_FREE_THRESHOLD_MB,
        "tmp_free_status": tmp_free_status,
        "tmp_free_shortfall_mb": tmp_free_shortfall_mb,
        "load_1": float(row[9]) if row[9] is not None else None,
        "load_5": float(row[10]) if row[10] is not None else None,
        "load_15": float(row[11]) if row[11] is not None else None,
        "process_count": int(row[12] or 0),
        "running_worker_count": int(row[13] or 0),
        "active_slot_count": int(row[14] or 0),
        "ts": row[15],
        "age_seconds": _age_seconds(row[15]),
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


def _observed_node_summary_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    workers = _slurm_worker_payloads(db_path, run_id=run_id)
    nodes: list[str] = []
    for worker in workers:
        observed_node = str(worker.get("observed_node") or "").strip()
        if observed_node and observed_node not in nodes:
            nodes.append(observed_node)
    return {
        "primary_node": nodes[0] if nodes else None,
        "node_count": len(nodes),
        "nodes": nodes[:8],
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
        rolling_throughput = _rolling_valid_csv_throughput_payload(db_path, run_id=run_id)
        throughput_kpi = _throughput_kpi_payload(
            queued_slots=queued_slots,
            active_slots=active_slots,
            active_workers=active_workers,
            pending_workers=pending_workers,
            rolling_throughput=rolling_throughput,
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


def _throughput_compare_payload(throughput_kpi: dict[str, object] | None) -> dict[str, object]:
    if throughput_kpi is None:
        return {}
    rolling_30m = float(throughput_kpi.get("rolling_30m_throughput") or 0.0)
    rolling_60m = float(throughput_kpi.get("rolling_60m_throughput") or 0.0)
    baseline_60m = float(
        throughput_kpi.get("throughput_baseline_slots_per_hour") or _THROUGHPUT_BASELINE_SLOTS_PER_HOUR
    )
    target_60m = float(
        throughput_kpi.get("throughput_target_slots_per_hour") or _THROUGHPUT_TARGET_SLOTS_PER_HOUR
    )
    return {
        "rolling_30m_throughput": rolling_30m,
        "rolling_60m_throughput": rolling_60m,
        "baseline_60m": baseline_60m,
        "target_60m": target_60m,
        "baseline_gap_60m": round(rolling_60m - baseline_60m, 1),
        "target_gap_60m": round(target_60m - rolling_60m, 1),
        "baseline_status": "ABOVE_BASELINE" if rolling_60m >= baseline_60m else "BELOW_BASELINE",
        "target_status": "AT_OR_ABOVE_TARGET" if rolling_60m >= target_60m else "BELOW_TARGET",
    }


def _throughput_feasibility_payload(
    throughput_compare: dict[str, object] | None,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    if not throughput_compare:
        return {}
    reference = datetime.now(tz=_TONIGHT_TARGET_DEADLINE_KST.tzinfo) if now is None else now
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=_TONIGHT_TARGET_DEADLINE_KST.tzinfo)
    else:
        reference = reference.astimezone(_TONIGHT_TARGET_DEADLINE_KST.tzinfo)
    rolling_60m = float(throughput_compare.get("rolling_60m_throughput") or 0.0)
    baseline_60m = float(throughput_compare.get("baseline_60m") or _THROUGHPUT_BASELINE_SLOTS_PER_HOUR)
    target_60m = float(throughput_compare.get("target_60m") or _THROUGHPUT_TARGET_SLOTS_PER_HOUR)
    target_gap_60m = float(throughput_compare.get("target_gap_60m") or round(target_60m - rolling_60m, 1))
    hours_remaining = round(max((_TONIGHT_TARGET_DEADLINE_KST - reference).total_seconds() / 3600.0, 0.0), 1)
    if reference >= _TONIGHT_TARGET_DEADLINE_KST:
        status = "MET" if rolling_60m >= target_60m else "MISSED"
    elif rolling_60m >= target_60m:
        status = "ON_TRACK"
    elif target_gap_60m <= baseline_60m:
        status = "AT_RISK"
    else:
        status = "OFF_TRACK"
    return {
        "deadline_kst": _TONIGHT_TARGET_DEADLINE_KST.isoformat(),
        "hours_remaining": hours_remaining,
        "status": status,
        "rolling_60m_throughput": rolling_60m,
        "target_60m": target_60m,
        "target_gap_60m": target_gap_60m,
        "reason": (
            f"rolling_60m={rolling_60m:.1f} target={target_60m:.1f} "
            f"gap={target_gap_60m:.1f} baseline={baseline_60m:.1f} hours_remaining={hours_remaining:.1f}"
        ),
    }


def _overnight_ops_windows() -> list[dict[str, object]]:
    kst = _TONIGHT_TARGET_DEADLINE_KST.tzinfo
    return [
        {
            "label": "00:00",
            "start": datetime(2026, 3, 13, 0, 0, tzinfo=kst),
            "end": datetime(2026, 3, 13, 2, 0, tzinfo=kst),
            "goal": "CSV integrity hotfix, drain bootstrap",
            "checks": ["CSV schema", "restart rediscovery", "tunnel 상태"],
        },
        {
            "label": "02:00",
            "start": datetime(2026, 3, 13, 2, 0, tzinfo=kst),
            "end": datetime(2026, 3, 13, 4, 0, tzinfo=kst),
            "goal": "worker_bundle_multiplier=4 apply",
            "checks": ["CSV schema", "multiplier 적용 후 idle slot", "rolling throughput"],
        },
        {
            "label": "04:00",
            "start": datetime(2026, 3, 13, 4, 0, tzinfo=kst),
            "end": datetime(2026, 3, 13, 6, 0, tzinfo=kst),
            "goal": "telemetry exclude and bad-node triage",
            "checks": ["bad-node", "/tmp free", "observed node", "failed slots"],
        },
        {
            "label": "06:00",
            "start": datetime(2026, 3, 13, 6, 0, tzinfo=kst),
            "end": _TONIGHT_TARGET_DEADLINE_KST,
            "goal": "final hardening, revalidation, final config",
            "checks": [
                "CSV regression 재확인",
                "throughput 추세",
                "bad-node 누적 상태",
                "rolling 60m throughput",
                "idle slots",
                "failed_slots",
                "tunnel stale",
                "No space left on device",
            ],
        },
    ]


def _overnight_ops_window_payload(*, now: datetime | None = None) -> dict[str, object]:
    kst = _TONIGHT_TARGET_DEADLINE_KST.tzinfo
    reference = datetime.now(tz=kst) if now is None else now
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=kst)
    else:
        reference = reference.astimezone(kst)
    windows = _overnight_ops_windows()
    active_index = next(
        (index for index, window in enumerate(windows) if window["start"] <= reference < window["end"]),
        None,
    )
    if active_index is not None:
        window = windows[active_index]
        next_window = windows[active_index + 1]["label"] if active_index + 1 < len(windows) else None
        previous_window = windows[active_index - 1]["label"] if active_index > 0 else None
        return {
            "status": "ACTIVE",
            "label": window["label"],
            "goal": window["goal"],
            "checks": window["checks"],
            "start_kst": window["start"].isoformat(),
            "end_kst": window["end"].isoformat(),
            "previous_label": previous_window,
            "next_label": next_window,
            "deadline_kst": _TONIGHT_TARGET_DEADLINE_KST.isoformat(),
        }
    if reference < windows[0]["start"]:
        return {
            "status": "PRE_WINDOW",
            "label": windows[0]["label"],
            "goal": windows[0]["goal"],
            "checks": windows[0]["checks"],
            "start_kst": windows[0]["start"].isoformat(),
            "end_kst": windows[0]["end"].isoformat(),
            "previous_label": None,
            "next_label": windows[0]["label"],
            "deadline_kst": _TONIGHT_TARGET_DEADLINE_KST.isoformat(),
        }
    return {
        "status": "POST_WINDOW",
        "label": "19:00+",
        "goal": "overnight window complete",
        "checks": ["rolling 60m throughput", "failed_slots", "No space left on device"],
        "start_kst": windows[-1]["start"].isoformat(),
        "end_kst": _TONIGHT_TARGET_DEADLINE_KST.isoformat(),
        "previous_label": windows[-1]["label"],
        "next_label": None,
        "deadline_kst": _TONIGHT_TARGET_DEADLINE_KST.isoformat(),
    }


def _trend_direction(current: float | int | None, previous: float | int | None) -> str:
    if current is None or previous is None:
        return "UNKNOWN"
    if current > previous:
        return "UP"
    if current < previous:
        return "DOWN"
    return "FLAT"


def _per_hour_rate(count: int, minutes: float) -> float:
    if minutes <= 0.0:
        return 0.0
    return round(float(count) * 60.0 / minutes, 1)


def _worker_mix_snapshot_payload(
    db_path: Path,
    *,
    run_id: str | None,
    snapshot_ts: datetime,
    fallback_end_ts: datetime | None = None,
) -> dict[str, object] | None:
    if run_id is None:
        return None

    def query_snapshot(sql: str, params: list[object], source: str) -> dict[str, object] | None:
        rows = _query(db_path, sql, params)
        row = rows[0] if rows else (0, 0, 0, 0, 0, 0)
        worker_count = int(row[0] or 0)
        if worker_count <= 0:
            return None
        return {
            "worker_count": worker_count,
            "busy_workers": int(row[1] or 0),
            "idle_workers": int(row[2] or 0),
            "configured_slots": int(row[3] or 0),
            "active_slots": int(row[4] or 0),
            "idle_slots": int(row[5] or 0),
            "source": source,
        }

    snapshot_iso = snapshot_ts.astimezone(timezone.utc).isoformat()
    latest_before = query_snapshot(
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
                WHERE run_id = ? AND ts <= ?
            ) ranked
            WHERE rn = 1
        ) snapshot
        """,
        [run_id, snapshot_iso],
        "latest_at_or_before_window_start",
    )
    if latest_before is not None:
        return latest_before
    if fallback_end_ts is None:
        return None

    fallback_end_iso = fallback_end_ts.astimezone(timezone.utc).isoformat()
    return query_snapshot(
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
                    ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY ts ASC) AS rn
                FROM worker_resource_snapshots
                WHERE run_id = ? AND ts >= ? AND ts < ?
            ) ranked
            WHERE rn = 1
        ) snapshot
        """,
        [run_id, snapshot_iso, fallback_end_iso],
        "earliest_within_window",
    )


def _count_valid_succeeded_slots_between(
    db_path: Path,
    *,
    run_id: str | None,
    start_ts: datetime,
    end_ts: datetime,
) -> int:
    if run_id is None or end_ts <= start_ts:
        return 0
    rows = _query(
        db_path,
        """
        SELECT output_path
        FROM slot_tasks
        WHERE run_id = ? AND state = 'SUCCEEDED' AND updated_at >= ? AND updated_at < ?
        ORDER BY updated_at DESC
        """,
        [run_id, start_ts.astimezone(timezone.utc).isoformat(), end_ts.astimezone(timezone.utc).isoformat()],
    )
    valid_count = 0
    for output_path, in rows:
        if _output_variables_csv_gate_reason(output_root=Path(str(output_path)).expanduser()) != "ok":
            continue
        valid_count += 1
    return valid_count


def _count_terminal_slots_between(
    db_path: Path,
    *,
    run_id: str | None,
    start_ts: datetime,
    end_ts: datetime,
) -> int:
    if run_id is None or end_ts <= start_ts:
        return 0
    rows = _query(
        db_path,
        """
        SELECT COUNT(*)
        FROM slot_tasks
        WHERE run_id = ?
          AND state IN ('FAILED', 'QUARANTINED')
          AND updated_at >= ?
          AND updated_at < ?
        """,
        [run_id, start_ts.astimezone(timezone.utc).isoformat(), end_ts.astimezone(timezone.utc).isoformat()],
    )
    return int(rows[0][0] or 0) if rows else 0


def _count_node_error_events_between(
    db_path: Path,
    *,
    run_id: str | None,
    start_ts: datetime,
    end_ts: datetime,
) -> int:
    if run_id is None or end_ts <= start_ts:
        return 0
    rows = _query(
        db_path,
        """
        SELECT COUNT(*)
        FROM events
        WHERE run_id = ?
          AND ts >= ?
          AND ts < ?
          AND (
              stage IN ('BAD_NODE_REGISTERED', 'BAD_NODE_EXCLUDE_ACTIVE', 'BAD_NODES_INVALID')
              OR message LIKE '%No space left on device%'
          )
        """,
        [run_id, start_ts.astimezone(timezone.utc).isoformat(), end_ts.astimezone(timezone.utc).isoformat()],
    )
    return int(rows[0][0] or 0) if rows else 0


def _ops_window_delta_payload(
    db_path: Path,
    *,
    run_id: str | None,
    ops_window: dict[str, object],
    worker_mix: dict[str, object] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    worker_mix = worker_mix or {}
    label = str(ops_window.get("label") or "")
    window_status = str(ops_window.get("status") or "")
    windows = _overnight_ops_windows()
    current_index = next((index for index, window in enumerate(windows) if str(window["label"]) == label), None)
    if current_index is None or run_id is None:
        return {}

    kst = _TONIGHT_TARGET_DEADLINE_KST.tzinfo
    reference = datetime.now(tz=kst) if now is None else now
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=kst)
    else:
        reference = reference.astimezone(kst)

    current_window = windows[current_index]
    previous_window = windows[current_index - 1] if current_index > 0 else None
    window_start = current_window["start"]
    window_end = current_window["end"]
    effective_end = min(max(reference, window_start), window_end)
    elapsed_minutes = round(max((effective_end - window_start).total_seconds() / 60.0, 0.0), 1)
    baseline_end = min(window_start + timedelta(minutes=15), effective_end)
    baseline_mix = _worker_mix_snapshot_payload(
        db_path,
        run_id=run_id,
        snapshot_ts=window_start.astimezone(timezone.utc),
        fallback_end_ts=baseline_end.astimezone(timezone.utc) if baseline_end > window_start else None,
    )

    idle_slots_window_start = int(baseline_mix["idle_slots"]) if baseline_mix is not None else None
    idle_slots_now = int(worker_mix.get("idle_slots") or 0)
    idle_slots_delta = idle_slots_now - idle_slots_window_start if idle_slots_window_start is not None else None

    current_window_valid_succeeded_slots = _count_valid_succeeded_slots_between(
        db_path,
        run_id=run_id,
        start_ts=window_start.astimezone(timezone.utc),
        end_ts=effective_end.astimezone(timezone.utc),
    )
    current_window_failed_slots = _count_terminal_slots_between(
        db_path,
        run_id=run_id,
        start_ts=window_start.astimezone(timezone.utc),
        end_ts=effective_end.astimezone(timezone.utc),
    )
    current_window_node_error_events = _count_node_error_events_between(
        db_path,
        run_id=run_id,
        start_ts=window_start.astimezone(timezone.utc),
        end_ts=effective_end.astimezone(timezone.utc),
    )
    current_window_valid_succeeded_slots_per_hour = _per_hour_rate(current_window_valid_succeeded_slots, elapsed_minutes)

    previous_window_valid_succeeded_slots: int | None = None
    previous_window_failed_slots: int | None = None
    previous_window_node_error_events: int | None = None
    previous_window_valid_succeeded_slots_per_hour: float | None = None
    if previous_window is not None:
        previous_elapsed_minutes = (previous_window["end"] - previous_window["start"]).total_seconds() / 60.0
        previous_window_valid_succeeded_slots = _count_valid_succeeded_slots_between(
            db_path,
            run_id=run_id,
            start_ts=previous_window["start"].astimezone(timezone.utc),
            end_ts=previous_window["end"].astimezone(timezone.utc),
        )
        previous_window_failed_slots = _count_terminal_slots_between(
            db_path,
            run_id=run_id,
            start_ts=previous_window["start"].astimezone(timezone.utc),
            end_ts=previous_window["end"].astimezone(timezone.utc),
        )
        previous_window_node_error_events = _count_node_error_events_between(
            db_path,
            run_id=run_id,
            start_ts=previous_window["start"].astimezone(timezone.utc),
            end_ts=previous_window["end"].astimezone(timezone.utc),
        )
        previous_window_valid_succeeded_slots_per_hour = _per_hour_rate(
            previous_window_valid_succeeded_slots,
            previous_elapsed_minutes,
        )

    return {
        "window_label": label,
        "window_status": window_status,
        "window_start_kst": window_start.isoformat(),
        "window_end_kst": window_end.isoformat(),
        "elapsed_minutes": elapsed_minutes,
        "previous_window_label": previous_window["label"] if previous_window is not None else None,
        "idle_slots_window_start": idle_slots_window_start,
        "idle_slots_now": idle_slots_now,
        "idle_slots_delta": idle_slots_delta,
        "idle_slots_trend": _trend_direction(idle_slots_now, idle_slots_window_start),
        "idle_slots_baseline_source": baseline_mix.get("source") if baseline_mix is not None else None,
        "window_valid_succeeded_slots": current_window_valid_succeeded_slots,
        "window_valid_succeeded_slots_per_hour": current_window_valid_succeeded_slots_per_hour,
        "previous_window_valid_succeeded_slots": previous_window_valid_succeeded_slots,
        "previous_window_valid_succeeded_slots_per_hour": previous_window_valid_succeeded_slots_per_hour,
        "throughput_trend": _trend_direction(
            current_window_valid_succeeded_slots_per_hour,
            previous_window_valid_succeeded_slots_per_hour,
        ),
        "current_window_failed_slots": current_window_failed_slots,
        "previous_window_failed_slots": previous_window_failed_slots,
        "failed_slots_window_trend": _trend_direction(
            current_window_failed_slots,
            previous_window_failed_slots,
        ),
        "current_window_node_error_events": current_window_node_error_events,
        "previous_window_node_error_events": previous_window_node_error_events,
        "node_error_window_trend": _trend_direction(
            current_window_node_error_events,
            previous_window_node_error_events,
        ),
    }


def _ops_window_checklist_payload(
    *,
    ops_window: dict[str, object],
    ops_triage: dict[str, object],
    rollout_status: dict[str, object],
    throughput_compare: dict[str, object],
    throughput_feasibility: dict[str, object],
    ops_window_delta: dict[str, object] | None,
    node_summary: dict[str, object] | None,
    observed_node_summary: dict[str, object] | None,
    slot_summary: dict[str, int] | None,
    tunnel_summary: dict[str, object] | None,
) -> dict[str, object]:
    ops_window_delta = ops_window_delta or {}
    node_summary = node_summary or {}
    observed_node_summary = observed_node_summary or {}
    slot_summary = slot_summary or {}
    tunnel_summary = tunnel_summary or {}

    def item(name: str, state: str, detail: str) -> dict[str, str]:
        return {"name": name, "state": state, "detail": detail}

    csv_state = str(ops_triage.get("csv_state") or "UNKNOWN")
    restart_state = str(ops_triage.get("restart_state") or "UNKNOWN")
    resource_state = str(ops_triage.get("resource_state") or "UNKNOWN")
    throughput_state = str(ops_triage.get("throughput_state") or "UNKNOWN")
    target_track_status = str(ops_triage.get("throughput_target_track_status") or "UNKNOWN")
    idle_slots = int(ops_triage.get("idle_slots") or 0)
    failed_slots = int(slot_summary.get("failed_slots") or 0)
    bad_node_count = int(rollout_status.get("bad_node_count") or 0)
    no_space_active = bool(rollout_status.get("no_space_active"))
    tunnel_stale_workers = int(tunnel_summary.get("stale_workers") or 0)
    tunnel_degraded_workers = int(tunnel_summary.get("degraded_workers") or 0)
    rolling_60m = float(throughput_compare.get("rolling_60m_throughput") or 0.0)
    baseline_60m = float(throughput_compare.get("baseline_60m") or _THROUGHPUT_BASELINE_SLOTS_PER_HOUR)
    idle_slots_window_start = ops_window_delta.get("idle_slots_window_start")
    idle_slots_now = int(ops_window_delta.get("idle_slots_now") or idle_slots)
    idle_slots_delta = ops_window_delta.get("idle_slots_delta")
    idle_slots_trend = str(ops_window_delta.get("idle_slots_trend") or "UNKNOWN")
    window_valid_succeeded_slots_per_hour = float(ops_window_delta.get("window_valid_succeeded_slots_per_hour") or 0.0)
    previous_window_valid_succeeded_slots_per_hour_value = ops_window_delta.get("previous_window_valid_succeeded_slots_per_hour")
    previous_window_valid_succeeded_slots_per_hour = (
        float(previous_window_valid_succeeded_slots_per_hour_value)
        if previous_window_valid_succeeded_slots_per_hour_value is not None
        else None
    )
    throughput_trend = str(ops_window_delta.get("throughput_trend") or "UNKNOWN")
    current_window_failed_slots = int(ops_window_delta.get("current_window_failed_slots") or 0)
    previous_window_failed_slots_value = ops_window_delta.get("previous_window_failed_slots")
    previous_window_failed_slots = (
        int(previous_window_failed_slots_value) if previous_window_failed_slots_value is not None else None
    )
    items: list[dict[str, str]] = []
    label = str(ops_window.get("label") or "")

    def csv_item(name: str) -> dict[str, str]:
        state = "PASS" if csv_state == "GREEN" else ("FAIL" if csv_state == "RED" else "WARN")
        detail = str(rollout_status.get("canary_gate") or rollout_status.get("canary_reason") or csv_state)
        return item(name, state, detail)

    def restart_item(name: str) -> dict[str, str]:
        if restart_state == "GREEN" and str(rollout_status.get("rediscovery_status") or "") == "GREEN":
            state = "PASS"
        elif restart_state == "RED" or str(rollout_status.get("rediscovery_status") or "") == "RED":
            state = "FAIL"
        else:
            state = "WARN"
        detail = (
            f"restart={restart_state} rediscovery={rollout_status.get('rediscovery_status') or 'UNKNOWN'}"
        )
        return item(name, state, detail)

    if label == "00:00":
        items = [
            csv_item("CSV schema"),
            restart_item("restart rediscovery"),
            item(
                "tunnel 상태",
                "PASS" if tunnel_degraded_workers == 0 and tunnel_stale_workers == 0 else "WARN",
                f"degraded={tunnel_degraded_workers} stale={tunnel_stale_workers}",
            ),
        ]
    elif label == "02:00":
        if idle_slots_delta is None:
            idle_state = "WARN"
            idle_detail = f"idle_slots={idle_slots}"
        elif int(idle_slots_delta) < 0:
            idle_state = "PASS"
            idle_detail = f"start={idle_slots_window_start} now={idle_slots_now} delta={idle_slots_delta} trend={idle_slots_trend}"
        elif int(idle_slots_delta) == 0:
            idle_state = "WARN"
            idle_detail = f"start={idle_slots_window_start} now={idle_slots_now} delta={idle_slots_delta} trend={idle_slots_trend}"
        else:
            idle_state = "FAIL"
            idle_detail = f"start={idle_slots_window_start} now={idle_slots_now} delta={idle_slots_delta} trend={idle_slots_trend}"
        throughput_detail = str(throughput_feasibility.get("reason") or target_track_status)
        if previous_window_valid_succeeded_slots_per_hour is not None:
            throughput_detail = (
                f"window_pt_h={window_valid_succeeded_slots_per_hour:.1f} "
                f"prev_window_pt_h={previous_window_valid_succeeded_slots_per_hour:.1f} "
                f"trend={throughput_trend} / {throughput_detail}"
            )
        items = [
            csv_item("CSV schema"),
            item(
                "idle slot",
                idle_state,
                idle_detail,
            ),
            item(
                "rolling throughput",
                "PASS"
                if target_track_status == "ON_TRACK" or (throughput_trend == "UP" and window_valid_succeeded_slots_per_hour >= baseline_60m)
                else ("WARN" if target_track_status == "AT_RISK" or throughput_state == "RISING" else "FAIL"),
                throughput_detail,
            ),
        ]
    elif label == "04:00":
        failed_slot_detail = f"failed_slots={failed_slots}"
        if previous_window_failed_slots is not None:
            failed_slot_detail = f"window={current_window_failed_slots} prev_window={previous_window_failed_slots} total={failed_slots}"
        items = [
            item("bad-node", "FAIL" if bad_node_count > 0 or resource_state == "RED" else "PASS", f"bad_node_count={bad_node_count}"),
            item(
                "/tmp free",
                "PASS" if str(node_summary.get("tmp_free_status") or "") == "GREEN" else "FAIL",
                f"tmp_free_status={node_summary.get('tmp_free_status') or 'UNKNOWN'}",
            ),
            item(
                "observed node",
                "PASS" if observed_node_summary.get("primary_node") else "WARN",
                str(observed_node_summary.get("primary_node") or "missing"),
            ),
            item("failed slots", "PASS" if failed_slots == 0 else "WARN", failed_slot_detail),
        ]
    else:
        throughput_trend_detail = str(throughput_feasibility.get("reason") or target_track_status)
        if previous_window_valid_succeeded_slots_per_hour is not None:
            throughput_trend_detail = (
                f"window_pt_h={window_valid_succeeded_slots_per_hour:.1f} "
                f"prev_window_pt_h={previous_window_valid_succeeded_slots_per_hour:.1f} "
                f"trend={throughput_trend} / {throughput_trend_detail}"
            )
        items = [
            csv_item("CSV regression 재확인"),
            item(
                "throughput 추세",
                "PASS"
                if throughput_trend == "UP" or target_track_status == "ON_TRACK"
                else ("WARN" if throughput_trend in {"FLAT", "UNKNOWN"} or target_track_status == "AT_RISK" else "FAIL"),
                throughput_trend_detail,
            ),
            item(
                "bad-node 누적 상태",
                "FAIL" if bad_node_count > 0 or no_space_active else ("WARN" if resource_state == "DEGRADED" else "PASS"),
                f"bad_node_count={bad_node_count} no_space_active={no_space_active}",
            ),
            item(
                "rolling 60m throughput",
                "PASS" if rolling_60m >= baseline_60m else "WARN",
                f"rolling_60m={rolling_60m:.1f} baseline={baseline_60m:.1f}",
            ),
            item("idle slots", "PASS" if idle_slots == 0 else "WARN", f"idle_slots={idle_slots}"),
            item("failed_slots", "PASS" if failed_slots == 0 else "WARN", f"failed_slots={failed_slots}"),
            item("tunnel stale", "PASS" if tunnel_stale_workers == 0 else "WARN", f"stale_workers={tunnel_stale_workers}"),
            item("No space left on device", "FAIL" if no_space_active else "PASS", str(rollout_status.get("no_space_detail") or "absent")),
        ]

    overall_status = "PASS"
    if any(entry["state"] == "FAIL" for entry in items):
        overall_status = "FAIL"
    elif any(entry["state"] == "WARN" for entry in items):
        overall_status = "WARN"
    return {"status": overall_status, "items": items}


def _ops_trend_signals_payload(
    db_path: Path,
    *,
    run_id: str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    reference = datetime.now(tz=timezone.utc) if now is None else now
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)
    recent_start = (reference - timedelta(minutes=60)).isoformat()
    previous_start = (reference - timedelta(minutes=120)).isoformat()
    current_ts = reference.isoformat()

    slot_params: list[object] = [recent_start, current_ts, previous_start, recent_start]
    slot_filter = ""
    if run_id is not None:
        slot_filter = "AND run_id = ?"
        slot_params.append(run_id)
    slot_rows = _query(
        db_path,
        f"""
        SELECT
            SUM(CASE WHEN updated_at >= ? AND updated_at < ? THEN 1 ELSE 0 END) AS recent_failed_slots,
            SUM(CASE WHEN updated_at >= ? AND updated_at < ? THEN 1 ELSE 0 END) AS previous_failed_slots
        FROM slot_tasks
        WHERE state IN ('FAILED', 'QUARANTINED') {slot_filter}
        """,
        slot_params,
    )
    slot_row = slot_rows[0] if slot_rows else (0, 0)
    recent_failed_slots_60m = int(slot_row[0] or 0)
    previous_failed_slots_60m = int(slot_row[1] or 0)

    event_params: list[object] = [recent_start, current_ts, previous_start, recent_start]
    event_filter = ""
    if run_id is not None:
        event_filter = "AND run_id = ?"
        event_params.append(run_id)
    event_rows = _query(
        db_path,
        f"""
        SELECT
            SUM(
                CASE
                    WHEN ts >= ? AND ts < ?
                     AND (stage IN ('BAD_NODE_REGISTERED', 'BAD_NODE_EXCLUDE_ACTIVE') OR message LIKE '%No space left on device%')
                    THEN 1 ELSE 0
                END
            ) AS recent_node_errors,
            SUM(
                CASE
                    WHEN ts >= ? AND ts < ?
                     AND (stage IN ('BAD_NODE_REGISTERED', 'BAD_NODE_EXCLUDE_ACTIVE') OR message LIKE '%No space left on device%')
                    THEN 1 ELSE 0
                END
            ) AS previous_node_errors
        FROM events
        WHERE 1=1 {event_filter}
        """,
        event_params,
    )
    event_row = event_rows[0] if event_rows else (0, 0)
    recent_node_error_events_60m = int(event_row[0] or 0)
    previous_node_error_events_60m = int(event_row[1] or 0)
    return {
        "recent_failed_slots_60m": recent_failed_slots_60m,
        "previous_failed_slots_60m": previous_failed_slots_60m,
        "failed_slots_trend": _trend_direction(recent_failed_slots_60m, previous_failed_slots_60m),
        "recent_node_error_events_60m": recent_node_error_events_60m,
        "previous_node_error_events_60m": previous_node_error_events_60m,
        "node_error_trend": _trend_direction(recent_node_error_events_60m, previous_node_error_events_60m),
    }


def _ops_window_guardrails_payload(
    *,
    ops_window: dict[str, object],
    events: list[dict[str, object]],
    ops_triage: dict[str, object],
    rollout_status: dict[str, object],
    throughput_compare: dict[str, object] | None,
    ops_trends: dict[str, object] | None,
    ops_window_delta: dict[str, object] | None,
    slot_summary: dict[str, int] | None,
    real_input_flow: dict[str, object] | None = None,
) -> dict[str, object]:
    slot_summary = slot_summary or {}
    throughput_compare = throughput_compare or {}
    ops_trends = ops_trends or {}
    ops_window_delta = ops_window_delta or {}
    real_input_flow = real_input_flow or {}
    label = str(ops_window.get("label") or "")
    csv_state = str(ops_triage.get("csv_state") or "UNKNOWN")
    restart_state = str(ops_triage.get("restart_state") or "UNKNOWN")
    throughput_state = str(ops_triage.get("throughput_state") or "UNKNOWN")
    target_track_status = str(ops_triage.get("throughput_target_track_status") or "UNKNOWN")
    failed_slots = int(slot_summary.get("failed_slots") or 0)
    bad_node_count = int(rollout_status.get("bad_node_count") or 0)
    real_input_flow_status = str(real_input_flow.get("status") or "INACTIVE")
    real_input_flow_absent = bool(real_input_flow.get("flow_absent"))
    real_input_flow_reason = str(real_input_flow.get("reason") or "real input flow unavailable")
    real_cutover = rollout_status.get("real_cutover") or {}
    cutover_decision = str(real_cutover.get("decision") or "UNKNOWN")
    cutover_action = str(real_cutover.get("action") or "unknown")
    cutover_drift_active = bool(real_cutover.get("drift_active"))
    cutover_actual_input_source_policy = str(real_cutover.get("actual_input_source_policy") or "")
    cutover_alignment_detail = str(real_cutover.get("alignment_reason") or real_cutover.get("reason") or "real cutover alignment unavailable")
    unsafe_real_cutover = cutover_drift_active and cutover_actual_input_source_policy == "allow_original" and cutover_decision != "GO"
    pending_real_resume = cutover_drift_active and cutover_decision == "GO"
    no_space_active = bool(rollout_status.get("no_space_active"))
    last_restart_stage = str(rollout_status.get("last_restart_stage") or "")
    last_restart_ts = str(rollout_status.get("last_restart_ts") or "")
    last_canary_ts = str(rollout_status.get("last_canary_ts") or "")
    last_worker_loop_start_ts = str(rollout_status.get("last_worker_loop_start_ts") or "")
    worker_loop_started_after_canary = bool(rollout_status.get("worker_loop_started_after_canary"))
    last_worker_loop_active_ts = str(rollout_status.get("last_worker_loop_active_ts") or "")
    worker_loop_active_after_canary = bool(rollout_status.get("worker_loop_active_after_canary"))
    restart_signal_after_canary = bool(rollout_status.get("restart_signal_after_canary"))
    truth_refreshed = bool(rollout_status.get("truth_refreshed"))
    last_truth_ts = str(rollout_status.get("last_truth_ts") or "")
    truth_refreshed_after_canary = bool(rollout_status.get("truth_refreshed_after_canary"))
    rolling_30m = float(throughput_compare.get("rolling_30m_throughput") or 0.0)
    rolling_60m = float(throughput_compare.get("rolling_60m_throughput") or 0.0)
    baseline_60m = float(throughput_compare.get("baseline_60m") or _THROUGHPUT_BASELINE_SLOTS_PER_HOUR)
    recent_failed_slots_60m = int(ops_trends.get("recent_failed_slots_60m") or 0)
    previous_failed_slots_60m = int(ops_trends.get("previous_failed_slots_60m") or 0)
    failed_slots_trend = str(ops_trends.get("failed_slots_trend") or "FLAT")
    recent_node_error_events_60m = int(ops_trends.get("recent_node_error_events_60m") or 0)
    previous_node_error_events_60m = int(ops_trends.get("previous_node_error_events_60m") or 0)
    node_error_trend = str(ops_trends.get("node_error_trend") or "FLAT")
    elapsed_minutes = float(ops_window_delta.get("elapsed_minutes") or 0.0)
    idle_slots_delta = ops_window_delta.get("idle_slots_delta")
    current_window_valid_succeeded_slots_per_hour = float(ops_window_delta.get("window_valid_succeeded_slots_per_hour") or 0.0)
    previous_window_valid_succeeded_slots_per_hour_value = ops_window_delta.get("previous_window_valid_succeeded_slots_per_hour")
    previous_window_valid_succeeded_slots_per_hour = (
        float(previous_window_valid_succeeded_slots_per_hour_value)
        if previous_window_valid_succeeded_slots_per_hour_value is not None
        else None
    )
    window_throughput_trend = str(ops_window_delta.get("throughput_trend") or "UNKNOWN")
    current_window_failed_slots = int(ops_window_delta.get("current_window_failed_slots") or recent_failed_slots_60m)
    previous_window_failed_slots_value = ops_window_delta.get("previous_window_failed_slots")
    previous_window_failed_slots = (
        int(previous_window_failed_slots_value) if previous_window_failed_slots_value is not None else previous_failed_slots_60m
    )
    failed_slots_window_trend = str(ops_window_delta.get("failed_slots_window_trend") or failed_slots_trend)
    current_window_node_error_events = int(ops_window_delta.get("current_window_node_error_events") or recent_node_error_events_60m)
    previous_window_node_error_events_value = ops_window_delta.get("previous_window_node_error_events")
    previous_window_node_error_events = (
        int(previous_window_node_error_events_value)
        if previous_window_node_error_events_value is not None
        else previous_node_error_events_60m
    )
    node_error_window_trend = str(ops_window_delta.get("node_error_window_trend") or node_error_trend)

    def seen(stage: str) -> bool:
        return any(str(event.get("stage") or "") == stage for event in events)

    def item(name: str, state: str, detail: str) -> dict[str, str]:
        return {"name": name, "state": state, "detail": detail}

    def flow_absent_guard_state() -> str:
        if real_input_flow_absent:
            return "WARN"
        if real_input_flow_status in {"PRESENT", "RECENT", "INACTIVE"}:
            return "PASS"
        if real_input_flow_status == "RECENT_FAILED_ONLY":
            return "WARN"
        return "WARN"

    def original_slots_guard_state() -> str:
        if real_input_flow_status in {"PRESENT", "RECENT"}:
            return "PASS"
        if real_input_flow_status == "RECENT_FAILED_ONLY":
            return "WARN"
        if real_input_flow_absent:
            return "FAIL"
        if real_input_flow_status == "INACTIVE":
            return "PASS"
        return "WARN"

    no_go_items: list[dict[str, str]]
    start_items: list[dict[str, str]]
    if label == "00:00":
        worker_loop_active_confirmed = worker_loop_active_after_canary or (
            worker_loop_started_after_canary and restart_signal_after_canary and restart_state == "GREEN"
        )
        no_go_items = [
            item(
                "CANARY_FAILED absent",
                "FAIL" if str(rollout_status.get("canary_status") or "") == "FAILED" else "PASS",
                str(rollout_status.get("canary_reason") or rollout_status.get("canary_gate") or "ok"),
            ),
            item(
                "real input policy drift absent",
                "FAIL" if unsafe_real_cutover else ("WARN" if pending_real_resume else "PASS"),
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("real input flow absent", flow_absent_guard_state(), real_input_flow_reason),
        ]
        start_items = [
            item(
                "WORKER_LOOP_START",
                "PASS" if worker_loop_started_after_canary else "WARN",
                (
                    f"last_worker_loop_start_ts={last_worker_loop_start_ts or 'missing'} "
                    f"last_canary_ts={last_canary_ts or 'missing'} "
                    f"post_canary={worker_loop_started_after_canary}"
                ),
            ),
            item(
                "WORKER_LOOP_ACTIVE",
                "PASS" if worker_loop_active_confirmed else ("FAIL" if restart_state == "RED" else "WARN"),
                (
                    f"last_worker_loop_active_ts={last_worker_loop_active_ts or 'missing'} "
                    f"last_restart_stage={last_restart_stage or restart_state} "
                    f"post_canary={worker_loop_active_after_canary} inferred={worker_loop_active_confirmed and not worker_loop_active_after_canary}"
                ),
            ),
            item(
                "WORKER_LOOP_OK/IDLE",
                "PASS" if restart_state == "GREEN" and restart_signal_after_canary else ("FAIL" if restart_state == "RED" else "WARN"),
                (
                    f"last_restart_stage={last_restart_stage or restart_state} "
                    f"last_restart_ts={last_restart_ts or 'missing'} "
                    f"post_canary={restart_signal_after_canary}"
                ),
            ),
            item(
                "SLURM_TRUTH_REFRESHED",
                "PASS" if truth_refreshed_after_canary else ("FAIL" if str(rollout_status.get("rediscovery_status") or "") == "RED" else "WARN"),
                (
                    f"last_truth_ts={last_truth_ts or 'missing'} "
                    f"last_canary_ts={last_canary_ts or 'missing'} "
                    f"post_canary={truth_refreshed_after_canary} truth_refreshed={truth_refreshed}"
                ),
            ),
            item(
                "real input policy applied",
                "FAIL" if cutover_drift_active else "PASS",
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("original slots present", original_slots_guard_state(), real_input_flow_reason),
        ]
    elif label == "02:00":
        improving_window = (
            (idle_slots_delta is not None and int(idle_slots_delta) < 0)
            and (
                (
                    previous_window_valid_succeeded_slots_per_hour is not None
                    and current_window_valid_succeeded_slots_per_hour > previous_window_valid_succeeded_slots_per_hour
                )
                or current_window_valid_succeeded_slots_per_hour >= baseline_60m
            )
        )
        no_go_items = [
            item("CSV gate red absent", "FAIL" if csv_state == "RED" else "PASS", csv_state),
            item(
                "failed_slots surge",
                "FAIL"
                if failed_slots_window_trend == "UP" and current_window_failed_slots > previous_window_failed_slots
                else ("WARN" if failed_slots > 0 else "PASS"),
                f"window={current_window_failed_slots} prev_window={previous_window_failed_slots} trend={failed_slots_window_trend}",
            ),
            item(
                "throughput improvement present",
                "PASS"
                if improving_window
                else ("WARN" if elapsed_minutes < 20.0 or target_track_status == "AT_RISK" else "FAIL"),
                (
                    f"window_pt_h={current_window_valid_succeeded_slots_per_hour:.1f} "
                    f"prev_window_pt_h={previous_window_valid_succeeded_slots_per_hour if previous_window_valid_succeeded_slots_per_hour is not None else '-'} "
                    f"idle_delta={idle_slots_delta if idle_slots_delta is not None else '-'} "
                    f"rolling_30m={rolling_30m:.1f} rolling_60m={rolling_60m:.1f} target_track={target_track_status}"
                ),
            ),
            item(
                "real input policy drift absent",
                "FAIL" if unsafe_real_cutover else ("WARN" if pending_real_resume else "PASS"),
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("real input flow absent", flow_absent_guard_state(), real_input_flow_reason),
        ]
        start_items = [
            item("WORKER_LOOP_OK", "PASS" if restart_state == "GREEN" else "FAIL", last_restart_stage or restart_state),
            item("failed_slots stable", "WARN" if failed_slots > 0 else "PASS", f"failed_slots={failed_slots}"),
            item(
                "real input policy applied",
                "FAIL" if cutover_drift_active else "PASS",
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("original slots present", original_slots_guard_state(), real_input_flow_reason),
        ]
    elif label == "04:00":
        no_go_items = [
            item("No space left on device 지속", "FAIL" if no_space_active else "PASS", str(rollout_status.get("no_space_detail") or "absent")),
            item("canary red absent", "FAIL" if csv_state == "RED" else "PASS", csv_state),
            item("restart red absent", "FAIL" if restart_state == "RED" else "PASS", restart_state),
            item(
                "real input policy drift absent",
                "FAIL" if unsafe_real_cutover else ("WARN" if pending_real_resume else "PASS"),
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("real input flow absent", flow_absent_guard_state(), real_input_flow_reason),
        ]
        start_items = [
            item("WORKER_LOOP_OK", "PASS" if restart_state == "GREEN" else "FAIL", last_restart_stage or restart_state),
            item(
                "node 오류 감소 추세",
                "PASS"
                if current_window_node_error_events == 0 or node_error_window_trend == "DOWN"
                else ("FAIL" if node_error_window_trend == "UP" else "WARN"),
                f"window={current_window_node_error_events} prev_window={previous_window_node_error_events} trend={node_error_window_trend}",
            ),
            item(
                "real input policy applied",
                "FAIL" if cutover_drift_active else "PASS",
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("original slots present", original_slots_guard_state(), real_input_flow_reason),
        ]
    else:
        no_go_items = [
            item("WORKER_LOOP_ERROR absent", "FAIL" if last_restart_stage == "WORKER_LOOP_ERROR" else "PASS", last_restart_stage or "absent"),
            item("CSV red absent", "FAIL" if csv_state == "RED" else "PASS", csv_state),
            item("restart red absent", "FAIL" if restart_state == "RED" else "PASS", restart_state),
            item(
                "real input policy drift absent",
                "FAIL" if unsafe_real_cutover else ("WARN" if pending_real_resume else "PASS"),
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("real input flow absent", flow_absent_guard_state(), real_input_flow_reason),
        ]
        start_items = [
            item("WORKER_LOOP_OK/IDLE", "PASS" if restart_state == "GREEN" else "FAIL", last_restart_stage or restart_state),
            item("WORKER_LOOP_ERROR absent", "FAIL" if last_restart_stage == "WORKER_LOOP_ERROR" else "PASS", last_restart_stage or "absent"),
            item(
                "real input policy applied",
                "FAIL" if cutover_drift_active else "PASS",
                f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
            ),
            item("original slots present", original_slots_guard_state(), real_input_flow_reason),
        ]

    no_go_status = "BLOCKED" if any(entry["state"] == "FAIL" for entry in no_go_items) else "CLEAR"
    start_status = "PENDING"
    if all(entry["state"] == "PASS" for entry in start_items):
        start_status = "READY"
    elif any(entry["state"] == "FAIL" for entry in start_items):
        start_status = "BLOCKED"
    return {
        "no_go_status": no_go_status,
        "no_go_items": no_go_items,
        "start_status": start_status,
        "start_items": start_items,
    }


def _ops_window_runbook_payload(
    *,
    ops_window: dict[str, object],
    health: dict[str, object],
    ops_triage: dict[str, object],
    rollout_status: dict[str, object],
    ops_controls: dict[str, object] | None,
    node_summary: dict[str, object] | None,
    real_input_flow: dict[str, object] | None = None,
) -> dict[str, object]:
    ops_controls = ops_controls or {}
    node_summary = node_summary or {}
    real_input_flow = real_input_flow or {}

    def item(name: str, state: str, detail: str) -> dict[str, str]:
        return {"name": name, "state": state, "detail": detail}

    def status_for(items: list[dict[str, str]]) -> str:
        if any(entry["state"] == "FAIL" for entry in items):
            return "FAIL"
        if any(entry["state"] == "WARN" for entry in items):
            return "WARN"
        return "PASS"

    label = str(ops_window.get("label") or "")
    health_status = str(health.get("status") or "UNKNOWN")
    worker_status = str(health.get("worker_status") or "")
    csv_state = str(ops_triage.get("csv_state") or "UNKNOWN")
    canary_status = str(rollout_status.get("canary_status") or "UNKNOWN")
    canary_gate = str(rollout_status.get("canary_gate") or rollout_status.get("canary_reason") or "UNKNOWN")
    canary_reason = str(rollout_status.get("canary_reason") or canary_gate or "UNKNOWN")
    cutover_ready = bool(rollout_status.get("cutover_ready"))
    full_rollout_ready = bool(rollout_status.get("full_rollout_ready"))
    rollback_triggered = bool(rollout_status.get("rollback_triggered"))
    restart_status = str(rollout_status.get("restart_status") or "UNKNOWN")
    rediscovery_status = str(rollout_status.get("rediscovery_status") or "UNKNOWN")
    truth_refreshed = bool(rollout_status.get("truth_refreshed"))
    resource_status = str(rollout_status.get("resource_status") or ops_triage.get("resource_state") or "UNKNOWN")
    bad_node_count = int(rollout_status.get("bad_node_count") or 0)
    real_cutover = rollout_status.get("real_cutover") or {}
    has_real_cutover = bool(real_cutover)
    cutover_decision = str(real_cutover.get("decision") or "UNKNOWN")
    cutover_action = str(real_cutover.get("action") or "unknown")
    cutover_alignment_status = str(real_cutover.get("alignment_status") or "UNKNOWN")
    cutover_drift_active = bool(real_cutover.get("drift_active"))
    cutover_actual_input_source_policy = str(real_cutover.get("actual_input_source_policy") or "")
    cutover_alignment_detail = str(real_cutover.get("alignment_reason") or real_cutover.get("reason") or "real cutover alignment unavailable")
    unsafe_real_cutover = cutover_drift_active and cutover_actual_input_source_policy == "allow_original" and cutover_decision != "GO"
    pending_real_resume = cutover_drift_active and cutover_decision == "GO"
    real_input_flow_status = str(real_input_flow.get("status") or "INACTIVE")
    real_input_flow_reason = str(real_input_flow.get("reason") or "real input flow unavailable")
    configured_bundle_multiplier = _env_int("PEETSFEA_WORKER_BUNDLE_MULTIPLIER", 1)
    dispatch_mode = str(ops_controls.get("dispatch_mode") or "run")
    dispatch_mode_warning = str(ops_controls.get("dispatch_mode_warning") or "")
    systemd_observed = bool(ops_controls.get("systemd_observed"))
    systemd_unit_name = str(ops_controls.get("systemd_unit_name") or "unknown.service")
    systemd_detail = str(ops_controls.get("systemd_detail") or "systemd state unavailable")
    stop_gate_state = str(ops_controls.get("stop_gate_state") or "WARN")
    stop_gate_detail = str(ops_controls.get("stop_gate_detail") or "missing")
    live_db_policy = str(ops_controls.get("live_db_policy") or "retain")
    live_db_path = str(ops_controls.get("live_db_path") or "-")
    canary_db_root = str(ops_controls.get("canary_db_root") or "-")
    service_boundary = rollout_status.get("service_boundary") or {}
    boundary_detail = (
        f"{service_boundary.get('input_source_policy') or '-'} / "
        f"{service_boundary.get('public_storage_mode') or '-'} / "
        f"{service_boundary.get('runner_scope') or '-'}"
    )
    validation_window_detail = f"validation_window=10m canary_status={canary_status} gate={canary_gate}"
    canary_age_seconds = rollout_status.get("canary_age_seconds")
    canary_progress_state = str(rollout_status.get("canary_progress_state") or "UNKNOWN")
    canary_progress_started_ts = rollout_status.get("canary_progress_started_ts")
    canary_progress_age_seconds = rollout_status.get("canary_progress_age_seconds")
    canary_progress_within_10m = rollout_status.get("canary_progress_within_10m")
    canary_duration_seconds = rollout_status.get("canary_duration_seconds")
    canary_duration_within_10m = rollout_status.get("canary_completed_within_10m")
    last_canary_ts = _parse_iso(str(rollout_status.get("last_canary_ts") or ""))
    window_start = _parse_iso(str(ops_window.get("start_kst") or ""))
    window_end = _parse_iso(str(ops_window.get("end_kst") or ""))
    if window_start is None or window_end is None:
        window_definition = next((window for window in _overnight_ops_windows() if str(window["label"]) == label), None)
        if window_definition is not None:
            window_start = window_definition["start"]
            window_end = window_definition["end"]
    canary_in_current_window = False
    canary_completed_within_10m = False
    if last_canary_ts is not None and window_start is not None and window_end is not None:
        if last_canary_ts.tzinfo is None:
            last_canary_ts = last_canary_ts.replace(tzinfo=timezone.utc)
        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)
        else:
            window_end = window_end.astimezone(window_start.tzinfo)
        last_canary_window_tz = last_canary_ts.astimezone(window_start.tzinfo)
        canary_in_current_window = window_start <= last_canary_window_tz <= window_end
        canary_completed_within_10m = canary_in_current_window and last_canary_window_tz <= (window_start + timedelta(minutes=10))
    if canary_duration_within_10m is not None:
        canary_completed_within_10m = bool(canary_duration_within_10m)
    canary_window_detail = (
        f"last_canary_ts={rollout_status.get('last_canary_ts') or 'missing'} "
        f"started_ts={canary_progress_started_ts or 'missing'} "
        f"duration_seconds={canary_duration_seconds if canary_duration_seconds is not None else '-'} "
        f"age_seconds={canary_age_seconds if canary_age_seconds is not None else '-'} "
        f"within_10m={canary_completed_within_10m}"
    )
    if canary_progress_state in {"RUNNING", "BLOCKED"}:
        canary_window_detail = (
            f"progress_state={canary_progress_state} "
            f"started_ts={canary_progress_started_ts or 'missing'} "
            f"age_seconds={canary_progress_age_seconds if canary_progress_age_seconds is not None else '-'} "
            f"within_10m={canary_progress_within_10m}"
        )

    def canary_pass_state() -> str:
        if canary_status == "PASSED":
            return "PASS"
        if canary_status == "FAILED":
            return "FAIL"
        return "WARN"

    def gate_ok_state() -> str:
        if canary_gate == "ok":
            return "PASS"
        if canary_gate in {"UNKNOWN", ""}:
            return "WARN"
        return "FAIL"

    def canary_window_state() -> str:
        if canary_status == "PASSED" and canary_completed_within_10m:
            return "PASS"
        if canary_status in {"PASSED", "FAILED"} and canary_duration_within_10m is False:
            return "FAIL"
        if canary_progress_state in {"RUNNING", "BLOCKED"}:
            if canary_progress_age_seconds is not None and int(canary_progress_age_seconds) > 600:
                return "FAIL"
            return "WARN"
        if (canary_status == "FAILED" and canary_in_current_window) or (canary_in_current_window and not canary_completed_within_10m):
            return "FAIL"
        return "WARN"

    def real_input_flow_runbook_state() -> str:
        if real_input_flow_status in {"PRESENT", "RECENT", "INACTIVE"}:
            return "PASS"
        if real_input_flow_status == "RECENT_FAILED_ONLY":
            return "WARN"
        if real_input_flow_status == "ABSENT":
            return "WARN"
        return "WARN"

    def real_input_flow_canary_state() -> str:
        if real_input_flow_status in {"PRESENT", "RECENT", "INACTIVE"}:
            return "PASS"
        if real_input_flow_status == "RECENT_FAILED_ONLY":
            return "WARN"
        if real_input_flow_status == "ABSENT":
            return "FAIL"
        return "WARN"

    def validation_bundle_items() -> list[dict[str, str]]:
        return [
            item("CUTOVER_READY", "PASS" if cutover_ready else "WARN", f"cutover_ready={cutover_ready} state={rollout_status.get('state') or 'UNKNOWN'}"),
            item("SLURM_TRUTH_REFRESHED", "PASS" if truth_refreshed else "WARN", f"rediscovery_status={rediscovery_status}"),
            item("CANARY_PASSED", canary_pass_state(), validation_window_detail),
            item("FULL_ROLLOUT_READY", "PASS" if full_rollout_ready else "WARN", f"full_rollout_ready={full_rollout_ready}"),
            item("ROLLBACK_TRIGGERED absent", "FAIL" if rollback_triggered else "PASS", f"rollback_triggered={rollback_triggered}"),
        ]

    control_items = [
        item(
            "dispatch.mode 확인",
            "PASS" if dispatch_mode == "drain" and not dispatch_mode_warning else "WARN",
            (
                f"dispatch_mode={dispatch_mode} path={ops_controls.get('dispatch_mode_path') or '-'} "
                f"{dispatch_mode_warning or ''}".strip()
            ),
        ),
        item(
            "DB retain 판단",
            "PASS" if live_db_policy == "retain" else "FAIL",
            f"live_db_policy={live_db_policy} live_db_path={live_db_path} canary_db_root={canary_db_root}",
        ),
        item(
            "systemd user service state",
            "PASS" if systemd_observed else "WARN",
            f"unit={systemd_unit_name} {systemd_detail}",
        ),
        item("stop gate confirmation proxy", stop_gate_state, stop_gate_detail),
        item(
            "input source policy alignment",
            (
                "PASS"
                if not has_real_cutover or cutover_alignment_status == "ALIGNED"
                else ("FAIL" if unsafe_real_cutover else "WARN")
            ),
            f"{cutover_alignment_detail} decision={cutover_decision} action={cutover_action}",
        ),
        item(
            "original slots present when GO",
            real_input_flow_runbook_state(),
            real_input_flow_reason,
        ),
    ]

    if label == "00:00":
        stop_items = [
            *control_items,
            item(
                "service status visible",
                "PASS" if worker_status else ("WARN" if health_status == "STALE" else "FAIL"),
                f"health={health_status} worker_status={worker_status or 'missing'}",
            ),
            item("code/service boundary", "PASS" if service_boundary else "WARN", boundary_detail),
        ]
        canary_items = [
            item("10분 sample canary", canary_window_state(), canary_window_detail),
            *validation_bundle_items(),
            item("sentinel CSV", gate_ok_state(), f"canary_gate={canary_gate}"),
            item(
                "reason=ok",
                "PASS" if canary_reason == "ok" else ("WARN" if canary_reason in {"UNKNOWN", ""} else "FAIL"),
                f"canary_reason={canary_reason}",
            ),
            item("original slots present after GO", real_input_flow_canary_state(), real_input_flow_reason),
        ]
    elif label == "02:00":
        stop_items = [
            *control_items,
            item(
                "00:00 gate green 이력",
                "PASS" if full_rollout_ready and canary_status == "PASSED" and canary_gate == "ok" else "FAIL",
                f"full_rollout_ready={full_rollout_ready} canary_status={canary_status} gate={canary_gate}",
            ),
            item(
                "multiplier target 재확인",
                "PASS" if configured_bundle_multiplier >= 4 else "FAIL",
                f"configured_worker_bundle_multiplier={configured_bundle_multiplier} target=4",
            ),
        ]
        canary_items = [
            item("10분 sample canary", canary_window_state(), canary_window_detail),
            *validation_bundle_items(),
            item(
                "CSV green",
                "PASS" if canary_gate == "ok" and csv_state == "GREEN" else gate_ok_state(),
                f"csv_state={csv_state} canary_gate={canary_gate}",
            ),
            item(
                "multiplier target configured",
                "PASS" if configured_bundle_multiplier >= 4 else "FAIL",
                f"configured_worker_bundle_multiplier={configured_bundle_multiplier} target=4",
            ),
            item("original slots present after GO", real_input_flow_canary_state(), real_input_flow_reason),
        ]
    elif label == "04:00":
        stop_items = [
            *control_items,
            item(
                "bad-node 후보",
                "PASS" if resource_status != "UNKNOWN" else "WARN",
                f"bad_node_count={bad_node_count} resource_status={resource_status}",
            ),
            item(
                "/tmp 기준",
                "PASS"
                if str(node_summary.get("tmp_free_status") or "") == "GREEN"
                else ("FAIL" if str(node_summary.get("tmp_free_status") or "") == "RED" else "WARN"),
                f"tmp_free_status={node_summary.get('tmp_free_status') or 'UNKNOWN'}",
            ),
            item("exclude 대상 정리", "PASS", f"bad_node_count={bad_node_count}"),
        ]
        canary_items = [
            item("10분 sample canary", canary_window_state(), canary_window_detail),
            *validation_bundle_items(),
            item(
                "CSV green 유지",
                "PASS" if csv_state == "GREEN" and canary_gate == "ok" else gate_ok_state(),
                f"csv_state={csv_state} canary_gate={canary_gate}",
            ),
            item(
                "resource 변경 후 canary green",
                "PASS" if canary_status == "PASSED" and csv_state == "GREEN" else canary_pass_state(),
                validation_window_detail,
            ),
            item("original slots present after GO", real_input_flow_canary_state(), real_input_flow_reason),
        ]
    else:
        stop_items = [
            *control_items,
            item(
                "누적 변경 재확인",
                "PASS" if csv_state != "UNKNOWN" and restart_status != "UNKNOWN" else "WARN",
                f"csv_state={csv_state} restart_status={restart_status}",
            ),
            item(
                "final target 고정",
                "PASS" if configured_bundle_multiplier in {1, 2, 4} else "WARN",
                f"configured_worker_bundle_multiplier={configured_bundle_multiplier}",
            ),
        ]
        canary_items = [
            item("10분 sample canary", canary_window_state(), canary_window_detail),
            *validation_bundle_items(),
            item(
                "CSV regression 없음",
                "PASS" if csv_state == "GREEN" and canary_gate == "ok" else gate_ok_state(),
                f"csv_state={csv_state} canary_gate={canary_gate}",
            ),
            item("canary green", canary_pass_state(), validation_window_detail),
            item("original slots present after GO", real_input_flow_canary_state(), real_input_flow_reason),
        ]

    return {
        "stop_status": status_for(stop_items),
        "stop_items": stop_items,
        "canary_status": status_for(canary_items),
        "canary_items": canary_items,
        "canary_window_minutes": 10,
        "last_canary_ts": rollout_status.get("last_canary_ts"),
        "canary_age_seconds": canary_age_seconds,
        "canary_in_current_window": canary_in_current_window,
        "canary_completed_within_10m": canary_completed_within_10m,
        "rediscovery_status": rediscovery_status,
    }


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
    workers = _slurm_worker_payloads(db_path, run_id=run_id)
    resource_summary = _latest_resource_summary_payload(db_path, run_id=run_id)
    node_summary = _latest_node_resource_payload(db_path, run_id=run_id)
    worker_mix = _worker_mix_payload(db_path, run_id=run_id)
    observed_node_summary = _observed_node_summary_payload(db_path, run_id=run_id)
    slot_state_counts = _slot_state_counts(db_path, run_id=run_id)
    slot_summary = _slot_summary_payload(slot_state_counts)
    slot_source_mix = _slot_source_mix_payload(db_path, run_id=run_id)
    tunnel_summary = {
        "active_workers": len(workers),
        "connected_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "CONNECTED"),
        "degraded_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "DEGRADED"),
        "stale_workers": sum(1 for worker in workers if worker.get("is_tunnel_stale")),
        "live_return_path_workers": sum(
            1 for worker in workers if worker.get("tunnel_state") == "CONNECTED" and not worker.get("is_tunnel_stale")
        ),
    }
    ops_triage = _ops_triage_payload(
        events=events,
        throughput_kpi=throughput_kpi,
        worker_mix=worker_mix,
        tunnel_summary=tunnel_summary,
        slot_state_counts=slot_state_counts,
    )
    throughput_compare = _throughput_compare_payload(throughput_kpi)
    throughput_feasibility = _throughput_feasibility_payload(throughput_compare)
    ops_window = _overnight_ops_window_payload()
    ops_trends = _ops_trend_signals_payload(db_path, run_id=run_id)
    ops_window_delta = _ops_window_delta_payload(db_path, run_id=run_id, ops_window=ops_window, worker_mix=worker_mix)
    rollout_status = _rollout_status_payload(db_path, run_id=run_id)
    ops_controls = _ops_restart_controls_payload(db_path=db_path, health=health, rollout_status=rollout_status)
    # Compute flow from the base GO/NO-GO table first, then let final cutover consume it.
    base_real_cutover = _real_cutover_decision_payload(
        ops_triage=ops_triage,
        rollout_status=rollout_status,
        service_boundary=rollout_status.get("service_boundary"),
        ops_controls=ops_controls,
    )
    real_input_flow = _real_input_flow_signal_payload(real_cutover=base_real_cutover, slot_source_mix=slot_source_mix)
    real_cutover = _real_cutover_decision_payload(
        ops_triage=ops_triage,
        rollout_status=rollout_status,
        service_boundary=rollout_status.get("service_boundary"),
        real_input_flow=real_input_flow,
        ops_controls=ops_controls,
    )
    ops_triage = _ops_triage_with_real_cutover(ops_triage, real_cutover=real_cutover)
    ops_triage = _ops_triage_with_real_input_flow(ops_triage, real_input_flow=real_input_flow)
    alerts = _alertable_event_summary(
        events=events,
        throughput_kpi=throughput_kpi,
        real_cutover=real_cutover,
        real_input_flow=real_input_flow,
    )
    ops_window_runbook = _ops_window_runbook_payload(
        ops_window=ops_window,
        health=health,
        ops_triage=ops_triage,
        rollout_status=rollout_status,
        ops_controls=ops_controls,
        node_summary=node_summary,
        real_input_flow=real_input_flow,
    )
    ops_window_checklist = _ops_window_checklist_payload(
        ops_window=ops_window,
        ops_triage=ops_triage,
        rollout_status=rollout_status,
        throughput_compare=throughput_compare,
        throughput_feasibility=throughput_feasibility,
        ops_window_delta=ops_window_delta,
        node_summary=node_summary,
        observed_node_summary=observed_node_summary,
        slot_summary=slot_summary,
        tunnel_summary=tunnel_summary,
    )
    ops_window_guardrails = _ops_window_guardrails_payload(
        ops_window=ops_window,
        events=events,
        ops_triage=ops_triage,
        rollout_status=rollout_status,
        throughput_compare=throughput_compare,
        ops_trends=ops_trends,
        ops_window_delta=ops_window_delta,
        slot_summary=slot_summary,
        real_input_flow=real_input_flow,
    )
    return {
        "run_id": run_id,
        "health": health,
        "run": latest_run,
        "throughput_kpi": throughput_kpi,
        "throughput_compare": throughput_compare,
        "throughput_feasibility": throughput_feasibility,
        "ops_window": ops_window,
        "ops_trends": ops_trends,
        "ops_window_delta": ops_window_delta,
        "ops_controls": ops_controls,
        "ops_window_runbook": ops_window_runbook,
        "ops_window_checklist": ops_window_checklist,
        "ops_window_guardrails": ops_window_guardrails,
        "real_cutover": real_cutover,
        "real_input_flow": real_input_flow,
        "resource_summary": resource_summary,
        "node_summary": node_summary,
        "worker_mix": worker_mix,
        "observed_node_summary": observed_node_summary,
        "slot_summary": slot_summary,
        "slot_source_mix": slot_source_mix,
        "slot_state_counts": slot_state_counts,
        "ops_triage": ops_triage,
        "accounts": _overview_account_payloads(db_path, run_id=run_id),
        "alerts": alerts[:8],
        "recent_events": events[:12],
        "tunnel_summary": tunnel_summary,
        "rollout_status": rollout_status,
    }


def _rollout_status_payload(db_path: Path, *, run_id: str | None) -> dict[str, object]:
    if run_id is None:
        real_cutover = _real_cutover_decision_payload(
            ops_triage={},
            rollout_status={},
            service_boundary={"input_source_policy": os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only")},
        )
        return {
            "run_id": None,
            "state": "IDLE",
            "cutover_ready": False,
            "full_rollout_ready": False,
            "rollback_triggered": False,
            "fallback_active": False,
            "canary_status": "UNKNOWN",
            "canary_gate": None,
            "canary_reason": None,
            "canary_progress_state": "UNKNOWN",
            "canary_progress_detail": None,
            "canary_progress_started_ts": None,
            "canary_progress_age_seconds": None,
            "canary_progress_within_10m": None,
            "canary_input_dir": None,
            "canary_output_root": None,
            "canary_db_path": None,
            "canary_validation_root": None,
            "canary_delete_failed_dir": None,
            "canary_output_root_exists": None,
            "canary_materialized_output_present": None,
            "canary_output_variables_csv_gate": None,
            "canary_output_variables_csv_path": None,
            "canary_run_log_path": None,
            "canary_exit_code_path": None,
            "canary_duration_seconds": None,
            "canary_completed_within_10m": None,
            "last_canary_stage": None,
            "last_canary_ts": None,
            "canary_age_seconds": None,
            "restart_status": "UNKNOWN",
            "restart_reason": None,
            "last_restart_stage": None,
            "last_restart_ts": None,
            "last_worker_loop_start_ts": None,
            "worker_loop_started_after_canary": False,
            "last_worker_loop_active_ts": None,
            "worker_loop_active_after_canary": False,
            "restart_signal_after_canary": False,
            "rediscovery_status": "UNKNOWN",
            "rediscovery_reason": None,
            "truth_refreshed": False,
            "truth_stage": None,
            "last_truth_ts": None,
            "truth_refreshed_after_canary": False,
            "resource_status": "UNKNOWN",
            "resource_reason": None,
            "last_resource_stage": None,
            "bad_node_active": False,
            "bad_node_count": 0,
            "bad_nodes": [],
            "bad_node_warnings": [],
            "tunnel_degraded": False,
            "tunnel_degraded_workers": 0,
            "tunnel_stale_workers": 0,
            "service_boundary": {
                "input_source_policy": os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only"),
                "public_storage_mode": os.getenv("PEETSFEA_PUBLIC_STORAGE_MODE", "disabled"),
                "runner_scope": "control_plane_orchestration",
                "storage_scope": "separate_service_boundary",
            },
            "real_cutover": real_cutover,
            "real_input_flow": _real_input_flow_signal_payload(real_cutover=real_cutover, slot_source_mix={}),
            "slot_source_mix": {},
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
    run_rows = _query(
        db_path,
        """
        SELECT summary
        FROM runs
        WHERE run_id = ?
        LIMIT 1
        """,
        [run_id],
    )
    run_summary = str(run_rows[0][0]) if run_rows and run_rows[0][0] is not None else ""
    workers = _slurm_worker_payloads(db_path, run_id=run_id)
    active_bad_nodes, bad_node_warnings = _active_bad_node_entries()
    tunnel_degraded_workers = sum(1 for worker in workers if worker.get("tunnel_state") == "DEGRADED")
    tunnel_stale_workers = sum(1 for worker in workers if worker.get("is_tunnel_stale"))
    seen_stages = [str(row[0]) for row in rows]
    service_boundary = {
        "input_source_policy": os.getenv("PEETSFEA_INPUT_SOURCE_POLICY", "sample_only"),
        "public_storage_mode": os.getenv("PEETSFEA_PUBLIC_STORAGE_MODE", "disabled"),
        "runner_scope": "control_plane_orchestration",
        "storage_scope": "separate_service_boundary",
    }
    boundary_event = next((row for row in rows if str(row[0]) == "SERVICE_BOUNDARY"), None)
    canary_start_event = next((row for row in rows if str(row[0]) == "CANARY_STARTED"), None)
    canary_progress_event = next((row for row in rows if str(row[0]) in {"CUTOVER_READY", "CUTOVER_BLOCKED"}), None)
    canary_start_message = str(canary_start_event[2]) if canary_start_event is not None and canary_start_event[2] is not None else ""
    canary_input_dir = _extract_token_value(canary_start_message, "input_dir")
    canary_output_root = _extract_token_value(canary_start_message, "output_root")
    canary_db_path = _extract_token_value(canary_start_message, "db_path")
    canary_delete_failed_dir = _extract_token_value(canary_start_message, "delete_failed_dir")
    canary_validation_root = _canary_validation_root(
        input_dir=canary_input_dir,
        output_root=canary_output_root,
        db_path=canary_db_path,
    )
    canary_delete_failed_dir = _canary_delete_failed_dir(
        delete_failed_dir=canary_delete_failed_dir,
        validation_root=canary_validation_root,
    )
    canary_artifact_status = _canary_artifact_status_payload(canary_output_root)
    canary_progress_started_ts = (
        canary_start_event[3]
        if canary_start_event is not None
        else (canary_progress_event[3] if canary_progress_event is not None else None)
    )
    canary_progress_age_seconds = _age_seconds(canary_progress_started_ts)
    canary_progress_within_10m = (
        canary_progress_age_seconds is not None and canary_progress_age_seconds <= 600
    )
    if boundary_event is not None:
        for token in str(boundary_event[2]).split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            if key in service_boundary:
                service_boundary[key] = value
    canary_event = next((row for row in rows if str(row[0]) in {"CANARY_PASSED", "CANARY_FAILED"}), None)
    canary_status = "UNKNOWN"
    canary_gate = _extract_token_value(run_summary, "canary_gate")
    canary_reason = canary_gate
    last_canary_stage = None
    last_canary_ts = None
    if canary_event is not None:
        last_canary_stage = str(canary_event[0])
        last_canary_ts = canary_event[3]
        canary_status = "PASSED" if last_canary_stage == "CANARY_PASSED" else "FAILED"
        if canary_reason is None:
            canary_reason = _extract_token_value(str(canary_event[2]), "reason")
    if canary_reason is None and canary_status == "PASSED":
        canary_reason = "ok"
    if canary_gate is None:
        canary_gate = canary_reason
    canary_duration_seconds = _seconds_between(str(canary_progress_started_ts or ""), str(last_canary_ts or ""))
    canary_completed_within_10m = (
        canary_duration_seconds is not None and canary_duration_seconds <= 600 and canary_status in {"PASSED", "FAILED"}
    )
    restart_event = next(
        (
            row
            for row in rows
            if str(row[0]) in {"WORKER_LOOP_OK", "WORKER_LOOP_IDLE", "WORKER_LOOP_ERROR", "WORKER_LOOP_BLOCKED", "WORKER_LOOP_RECOVERING", "CONTROL_TUNNEL_DEGRADED"}
        ),
        None,
    )
    restart_status = "UNKNOWN"
    restart_reason = None
    last_restart_stage = None
    last_restart_ts = None
    if restart_event is not None:
        last_restart_stage = str(restart_event[0])
        restart_reason = str(restart_event[2]) if restart_event[2] is not None else None
        last_restart_ts = restart_event[3]
        if last_restart_stage in {"WORKER_LOOP_OK", "WORKER_LOOP_IDLE"}:
            restart_status = "GREEN"
        elif last_restart_stage in {"WORKER_LOOP_ERROR", "WORKER_LOOP_BLOCKED"}:
            restart_status = "RED"
        else:
            restart_status = "DEGRADED"
    worker_loop_start_event = next((row for row in rows if str(row[0]) == "WORKER_LOOP_START"), None)
    last_worker_loop_start_ts = worker_loop_start_event[3] if worker_loop_start_event is not None else None
    worker_loop_started_after_canary = _timestamp_after(str(last_worker_loop_start_ts or ""), str(last_canary_ts or ""))
    worker_loop_active_event = next((row for row in rows if str(row[0]) == "WORKER_LOOP_ACTIVE"), None)
    last_worker_loop_active_ts = worker_loop_active_event[3] if worker_loop_active_event is not None else None
    worker_loop_active_after_canary = _timestamp_after(str(last_worker_loop_active_ts or ""), str(last_canary_ts or ""))
    restart_signal_after_canary = _timestamp_after(str(last_restart_ts or ""), str(last_canary_ts or ""))
    truth_event = next((row for row in rows if str(row[0]) in {"SLURM_TRUTH_REFRESHED", "SLURM_TRUTH_LAG", "CAPACITY_MISMATCH"}), None)
    rediscovery_status = "UNKNOWN"
    rediscovery_reason = None
    truth_stage = None
    truth_refreshed = False
    last_truth_ts = truth_event[3] if truth_event is not None else None
    if truth_event is not None:
        truth_stage = str(truth_event[0])
        rediscovery_reason = str(truth_event[2]) if truth_event[2] is not None else None
        if truth_stage == "SLURM_TRUTH_REFRESHED":
            rediscovery_status = "GREEN"
            truth_refreshed = True
        elif truth_stage == "SLURM_TRUTH_LAG":
            rediscovery_status = "DEGRADED"
        else:
            rediscovery_status = "RED"
    truth_refreshed_after_canary = truth_refreshed and _timestamp_after(str(last_truth_ts or ""), str(last_canary_ts or ""))
    no_space_event = next((row for row in rows if "No space left on device" in str(row[2] or "")), None)
    no_space_bad_node = next(
        (entry for entry in active_bad_nodes if "No space left on device" in str(entry.get("reason") or "")),
        None,
    )
    no_space_active = False
    no_space_node = None
    no_space_detail = None
    no_space_source = None
    if no_space_event is not None:
        no_space_active = True
        no_space_node = _extract_token_value(str(no_space_event[2]), "node")
        no_space_detail = str(no_space_event[2]) if no_space_event[2] is not None else "No space left on device"
        no_space_source = "event"
    elif no_space_bad_node is not None:
        no_space_active = True
        no_space_node = str(no_space_bad_node.get("node") or "") or None
        no_space_detail = str(no_space_bad_node.get("reason") or "No space left on device")
        no_space_source = "bad_node_policy"
    resource_event = next(
        (
            row
            for row in rows
            if str(row[0]) in {"BAD_NODE_REGISTERED", "BAD_NODE_EXCLUDE_ACTIVE", "BAD_NODES_INVALID", "CONTROL_TUNNEL_DEGRADED"}
            or "No space left on device" in str(row[2] or "")
        ),
        None,
    )
    resource_status = "UNKNOWN"
    resource_reason = None
    last_resource_stage = None
    bad_node_active = bool(active_bad_nodes)
    tunnel_degraded = tunnel_degraded_workers > 0
    if resource_event is not None:
        last_resource_stage = str(resource_event[0])
        resource_reason = str(resource_event[2]) if resource_event[2] is not None else None
        if last_resource_stage in {"BAD_NODE_REGISTERED", "BAD_NODE_EXCLUDE_ACTIVE", "BAD_NODES_INVALID"} or "No space left on device" in str(resource_reason or ""):
            resource_status = "RED"
            bad_node_active = True
        else:
            resource_status = "DEGRADED"
            tunnel_degraded = True
    elif active_bad_nodes:
        resource_status = "RED"
        first_bad_node = active_bad_nodes[0]
        resource_reason = f"active_bad_node={first_bad_node['node']} reason={first_bad_node.get('reason') or 'unknown'}"
        last_resource_stage = "BAD_NODE_POLICY_ACTIVE"
    elif tunnel_stale_workers > 0 or tunnel_degraded_workers > 0:
        resource_status = "DEGRADED"
        resource_reason = f"tunnel_degraded_workers={tunnel_degraded_workers} tunnel_stale_workers={tunnel_stale_workers}"
        last_resource_stage = "TUNNEL_STALE" if tunnel_stale_workers > 0 else "CONTROL_TUNNEL_DEGRADED"
        tunnel_degraded = True
    elif truth_refreshed:
        resource_status = "GREEN"
    cutover_ready = "CUTOVER_READY" in seen_stages or "FULL_ROLLOUT_READY" in seen_stages
    full_rollout_ready = "FULL_ROLLOUT_READY" in seen_stages
    rollback_triggered = "ROLLBACK_TRIGGERED" in seen_stages
    fallback_active = rollback_triggered or "CANARY_FAILED" in seen_stages
    canary_passed = "CANARY_PASSED" in seen_stages
    if rollback_triggered or "CANARY_FAILED" in seen_stages:
        state = "FALLBACK_ACTIVE"
    elif full_rollout_ready:
        state = "FULL_ROLLOUT_READY"
    elif "CUTOVER_BLOCKED" in seen_stages:
        state = "CUTOVER_BLOCKED"
    elif "CUTOVER_READY" in seen_stages:
        state = "CANARY_READY"
    else:
        state = "ACTIVE"
    if canary_status == "FAILED":
        canary_progress_state = "FAILED"
        canary_progress_detail = f"reason={canary_reason or canary_gate or 'unknown'}"
    elif canary_status == "PASSED":
        canary_progress_state = "PASSED"
        canary_progress_detail = f"reason={canary_reason or canary_gate or 'ok'} last_canary_ts={last_canary_ts or 'missing'}"
    elif state == "CUTOVER_BLOCKED":
        canary_progress_state = "BLOCKED"
        canary_progress_detail = (
            f"validation lane blocked before canary completion "
            f"started_ts={canary_progress_started_ts or 'missing'} "
            f"age_seconds={canary_progress_age_seconds if canary_progress_age_seconds is not None else '-'} "
            f"within_10m={canary_progress_within_10m}"
        )
    elif state == "CANARY_READY":
        canary_progress_state = "RUNNING"
        canary_progress_detail = (
            f"validation lane running after CUTOVER_READY "
            f"started_ts={canary_progress_started_ts or 'missing'} "
            f"age_seconds={canary_progress_age_seconds if canary_progress_age_seconds is not None else '-'} "
            f"within_10m={canary_progress_within_10m}"
        )
    elif canary_start_event is not None:
        canary_progress_state = "RUNNING"
        canary_progress_detail = (
            f"validation lane started "
            f"started_ts={canary_progress_started_ts or 'missing'} "
            f"age_seconds={canary_progress_age_seconds if canary_progress_age_seconds is not None else '-'} "
            f"within_10m={canary_progress_within_10m}"
        )
    elif cutover_ready:
        canary_progress_state = "RUNNING"
        canary_progress_detail = (
            f"validation lane queued after CUTOVER_READY "
            f"started_ts={canary_progress_started_ts or 'missing'} "
            f"age_seconds={canary_progress_age_seconds if canary_progress_age_seconds is not None else '-'} "
            f"within_10m={canary_progress_within_10m}"
        )
    else:
        canary_progress_state = "UNKNOWN"
        canary_progress_detail = "no canary progress signal"
    _, throughput_kpi = _recent_ops_events_payload(db_path, run_id=run_id, limit=24)
    worker_mix = _worker_mix_payload(db_path, run_id=run_id)
    slot_state_counts = _slot_state_counts(db_path, run_id=run_id)
    slot_source_mix = _slot_source_mix_payload(db_path, run_id=run_id)
    tunnel_summary = {
        "active_workers": len(workers),
        "connected_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "CONNECTED"),
        "degraded_workers": tunnel_degraded_workers,
        "stale_workers": tunnel_stale_workers,
        "live_return_path_workers": sum(
            1 for worker in workers if worker.get("tunnel_state") == "CONNECTED" and not worker.get("is_tunnel_stale")
        ),
    }
    ops_events = [
        {
            "stage": str(row[0]),
            "level": str(row[1]),
            "message": str(row[2] or ""),
            "ts": row[3],
        }
        for row in rows
    ]
    ops_triage = _ops_triage_payload(
        events=ops_events,
        throughput_kpi=throughput_kpi,
        worker_mix=worker_mix,
        tunnel_summary=tunnel_summary,
        slot_state_counts=slot_state_counts,
    )
    health = _worker_health_payload(db_path, stale_threshold=int(os.getenv("PEETSFEA_STALE_THRESHOLD_SECONDS", "90")))
    rollout_gate_status = {
        "canary_status": canary_status,
        "canary_gate": canary_gate,
        "canary_reason": canary_reason,
        "canary_progress_state": canary_progress_state,
        "canary_progress_within_10m": canary_progress_within_10m,
        "canary_duration_seconds": canary_duration_seconds,
        "canary_completed_within_10m": canary_completed_within_10m if canary_duration_seconds is not None else None,
        "fallback_active": fallback_active,
        "restart_status": restart_status,
        "last_restart_ts": last_restart_ts,
        "last_canary_ts": last_canary_ts,
        "worker_loop_started_after_canary": worker_loop_started_after_canary,
        "restart_signal_after_canary": restart_signal_after_canary,
        "rediscovery_status": rediscovery_status,
        "truth_refreshed_after_canary": truth_refreshed_after_canary,
        "resource_status": resource_status,
        "full_rollout_ready": full_rollout_ready,
        "bad_node_active": bad_node_active,
    }
    ops_controls = _ops_restart_controls_payload(db_path=db_path, health=health, rollout_status=rollout_gate_status)
    base_real_cutover = _real_cutover_decision_payload(
        ops_triage=ops_triage,
        rollout_status=rollout_gate_status,
        service_boundary=service_boundary,
        ops_controls=ops_controls,
    )
    real_input_flow = _real_input_flow_signal_payload(real_cutover=base_real_cutover, slot_source_mix=slot_source_mix)
    real_cutover = _real_cutover_decision_payload(
        ops_triage=ops_triage,
        rollout_status=rollout_gate_status,
        service_boundary=service_boundary,
        real_input_flow=real_input_flow,
        ops_controls=ops_controls,
    )
    return {
        "run_id": run_id,
        "state": state,
        "cutover_ready": cutover_ready,
        "full_rollout_ready": full_rollout_ready,
        "rollback_triggered": rollback_triggered,
        "fallback_active": fallback_active,
        "canary_passed": canary_passed,
        "canary_status": canary_status,
        "canary_gate": canary_gate,
        "canary_reason": canary_reason,
        "canary_progress_state": canary_progress_state,
        "canary_progress_detail": canary_progress_detail,
        "canary_progress_started_ts": canary_progress_started_ts,
        "canary_progress_age_seconds": canary_progress_age_seconds,
        "canary_progress_within_10m": canary_progress_within_10m,
        "canary_input_dir": canary_input_dir,
        "canary_output_root": canary_output_root,
        "canary_db_path": canary_db_path,
        "canary_validation_root": canary_validation_root,
        "canary_delete_failed_dir": canary_delete_failed_dir,
        "canary_output_root_exists": canary_artifact_status["canary_output_root_exists"],
        "canary_materialized_output_present": canary_artifact_status["canary_materialized_output_present"],
        "canary_output_variables_csv_gate": canary_artifact_status["canary_output_variables_csv_gate"],
        "canary_output_variables_csv_path": canary_artifact_status["canary_output_variables_csv_path"],
        "canary_run_log_path": canary_artifact_status["canary_run_log_path"],
        "canary_exit_code_path": canary_artifact_status["canary_exit_code_path"],
        "canary_duration_seconds": canary_duration_seconds,
        "canary_completed_within_10m": canary_completed_within_10m if canary_duration_seconds is not None else None,
        "last_canary_stage": last_canary_stage,
        "last_canary_ts": last_canary_ts,
        "canary_age_seconds": _age_seconds(last_canary_ts),
        "restart_status": restart_status,
        "restart_reason": restart_reason,
        "last_restart_stage": last_restart_stage,
        "last_restart_ts": last_restart_ts,
        "last_worker_loop_start_ts": last_worker_loop_start_ts,
        "worker_loop_started_after_canary": worker_loop_started_after_canary,
        "last_worker_loop_active_ts": last_worker_loop_active_ts,
        "worker_loop_active_after_canary": worker_loop_active_after_canary,
        "restart_signal_after_canary": restart_signal_after_canary,
        "rediscovery_status": rediscovery_status,
        "rediscovery_reason": rediscovery_reason,
        "truth_refreshed": truth_refreshed,
        "truth_stage": truth_stage,
        "last_truth_ts": last_truth_ts,
        "truth_refreshed_after_canary": truth_refreshed_after_canary,
        "resource_status": resource_status,
        "resource_reason": resource_reason,
        "last_resource_stage": last_resource_stage,
        "no_space_active": no_space_active,
        "no_space_node": no_space_node,
        "no_space_detail": no_space_detail,
        "no_space_source": no_space_source,
        "bad_node_active": bad_node_active,
        "bad_node_count": len(active_bad_nodes),
        "bad_nodes": active_bad_nodes,
        "bad_node_warnings": bad_node_warnings,
        "tunnel_degraded": tunnel_degraded,
        "tunnel_degraded_workers": tunnel_degraded_workers,
        "tunnel_stale_workers": tunnel_stale_workers,
        "service_boundary": service_boundary,
        "real_cutover": real_cutover,
        "real_input_flow": real_input_flow,
        "slot_source_mix": slot_source_mix,
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
                stage = str(payload.get("stage") or "CONTROL_TUNNEL_LOST").strip().upper()
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
                    stage=stage,
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
                    rolling_throughput=_rolling_valid_csv_throughput_payload(db_path, run_id=run_id),
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
                    rolling_throughput = _rolling_valid_csv_throughput_payload(db_path, run_id=scope_run_id)
                    throughput_kpi = _throughput_kpi_payload(
                        queued_slots=queued_slots,
                        active_slots=active_slots,
                        active_workers=active_workers,
                        pending_workers=pending_workers,
                        rolling_throughput=rolling_throughput,
                    )
                worker_mix = _worker_mix_payload(db_path, run_id=scope_run_id)
                slot_state_counts = _slot_state_counts(db_path, run_id=scope_run_id)
                tunnel_summary = {
                    "degraded_workers": 0,
                    "stale_workers": 0,
                }
                if scope_run_id is not None:
                    workers = _slurm_worker_payloads(db_path, run_id=scope_run_id)
                    tunnel_summary = {
                        "degraded_workers": sum(1 for worker in workers if worker.get("tunnel_state") == "DEGRADED"),
                        "stale_workers": sum(1 for worker in workers if worker.get("is_tunnel_stale")),
                    }

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
                        "ops_triage": _ops_triage_payload(
                            events=events,
                            throughput_kpi=throughput_kpi,
                            worker_mix=worker_mix,
                            tunnel_summary=tunnel_summary,
                            slot_state_counts=slot_state_counts,
                        ),
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
          <div class="panel"><div class="k">Return Path Live</div><div class="v" id="return-live">-</div></div>
          <div class="panel"><div class="k">Slot Active</div><div class="v" id="w-active">-</div></div>
          <div class="panel"><div class="k">Slot Idle</div><div class="v" id="slot-idle">-</div></div>
          <div class="panel"><div class="k">Refill Lag</div><div class="v" id="refill-lag">-</div></div>
          <div class="panel"><div class="k">Tunnel Degraded</div><div class="v" id="tunnel-bad">-</div></div>
          <div class="panel"><div class="k">Tunnel Stale</div><div class="v" id="tunnel-stale">-</div></div>
          <div class="panel"><div class="k">Slot Succeeded</div><div class="v" id="slot-succeeded">-</div></div>
          <div class="panel"><div class="k">Slot Failed</div><div class="v" id="slot-failed">-</div></div>
          <div class="panel"><div class="k">Throughput Mode</div><div class="v" id="slot-mode" style="font-size:15px;">-</div></div>
        </div>
      </div>
      <div class="panel">
        <h2>Operator Flow</h2>
        <div class="muted">window: <code id="ops-window">-</code></div>
        <div class="muted">restart phase: <code id="ops-restart-phase">-</code></div>
        <div class="muted">phase detail: <code id="ops-restart-phase-detail">-</code></div>
        <div class="muted">checks: <code id="ops-window-checks">-</code></div>
        <div class="muted">window delta: <code id="ops-window-delta">-</code></div>
        <div class="muted">stop before: <code id="ops-window-stop-status">-</code></div>
        <div id="ops-window-stop-list" class="list" style="margin-top:8px;"></div>
        <div class="muted">canary check: <code id="ops-window-canary-status">-</code></div>
        <div id="ops-window-canary-list" class="list" style="margin-top:8px;"></div>
        <div class="muted">window status: <code id="ops-window-status">-</code></div>
        <div id="ops-window-checklist-list" class="list" style="margin-top:8px;"></div>
        <div class="muted">no-go: <code id="ops-window-no-go-status">-</code></div>
        <div id="ops-window-no-go-list" class="list" style="margin-top:8px;"></div>
        <div class="muted">start after: <code id="ops-window-start-status">-</code></div>
        <div id="ops-window-start-list" class="list" style="margin-top:8px;"></div>
        <div class="muted">real cutover: <code id="real-cutover-decision">-</code></div>
        <div class="muted">real cutover alignment: <code id="real-cutover-alignment">-</code></div>
        <div class="muted">real cutover reason: <code id="real-cutover-reason">-</code></div>
        <div class="muted">real input flow: <code id="real-input-flow">-</code></div>
        <div class="muted">slot sources: <code id="slot-source-mix">-</code></div>
        <div class="muted">decision: <code id="triage-status">-</code></div>
        <div class="muted">priority: <code id="triage-priority">-</code></div>
        <div class="muted">signals: <code id="triage-signals">-</code></div>
        <div class="muted">60m / idle / failed: <code id="triage-metrics">-</code></div>
        <div class="muted">30m / 60m / base / target gap: <code id="triage-throughput-compare">-</code></div>
        <div class="muted">target track: <code id="triage-target-track">-</code></div>
        <div class="muted">reason: <code id="triage-reason">-</code></div>
        <div class="muted">rollout: <code id="rollout-state">-</code></div>
        <div class="muted">canary: <code id="rollout-canary-status">-</code></div>
        <div class="muted">canary gate: <code id="rollout-canary-gate">-</code></div>
        <div class="muted">canary paths: <code id="rollout-canary-paths">-</code></div>
        <div class="muted">canary artifacts: <code id="rollout-canary-artifacts">-</code></div>
        <div class="muted">canary files: <code id="rollout-canary-files">-</code></div>
        <div class="muted">restart: <code id="rollout-restart-status">-</code></div>
        <div class="muted">rediscovery: <code id="rollout-truth-status">-</code></div>
        <div class="muted">resource: <code id="rollout-resource-status">-</code></div>
        <div class="muted">no space: <code id="rollout-no-space">-</code></div>
        <div class="muted">resource detail: <code id="rollout-resource-detail">-</code></div>
        <div class="muted">boundary: <code id="rollout-boundary">-</code></div>
        <div class="muted" style="margin-top:10px;">active bad nodes</div>
        <div id="bad-nodes-list" class="list" style="margin-top:8px;"></div>
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
          <div><div class="k">Tmp Free</div><div class="v" id="res-tmp-free">-</div></div>
          <div><div class="k">Tmp Gate</div><div class="v" id="res-tmp-status" style="font-size:15px;">-</div></div>
          <div><div class="k">Load</div><div class="v" id="res-load">-</div></div>
          <div><div class="k">Observed Node</div><div class="v" id="res-observed-node" style="font-size:15px;">-</div></div>
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

    function renderBadNodes(badNodes) {
      const host = document.getElementById('bad-nodes-list');
      host.innerHTML = '';
      if (!badNodes.length) {
        host.innerHTML = '<div class="muted">no active bad nodes</div>';
        return;
      }
      for (const item of badNodes) {
        const div = document.createElement('div');
        div.className = 'list-item';
        const remaining = item.remaining_minutes == null ? '-' : `${item.remaining_minutes}m`;
        div.innerHTML = `
          <div><strong>${esc(item.node)}</strong> <span class="muted">${esc(remaining)}</span></div>
          <div class="muted">${esc(item.reason || '-')}</div>
          <div class="muted">until ${esc(item.expires_at || '-')}</div>
        `;
        host.appendChild(div);
      }
    }

    function renderOpsItemList(containerId, items, emptyMessage) {
      const host = document.getElementById(containerId);
      host.innerHTML = '';
      if (!items.length) {
        host.innerHTML = `<div class="muted">${esc(emptyMessage)}</div>`;
        return;
      }
      for (const item of items) {
        const div = document.createElement('div');
        div.className = 'list-item';
        div.innerHTML = `
          <div><strong>${esc(item.name || '-')}</strong> <span class="muted">${esc(item.state || '-')}</span></div>
          <div class="muted">${esc(item.detail || '-')}</div>
        `;
        host.appendChild(div);
      }
    }

    let currentRunId = null;

    function renderRolloutDetail(rollout, realCutover, realInputFlow, slotSourceMix) {
      const badNodes = rollout.bad_nodes || [];
      const boundary = rollout.service_boundary || {};
      document.getElementById('real-cutover-decision').textContent =
        `${realCutover.decision || '-'} / ${realCutover.action || '-'} / ${realCutover.workstream || '-'}`;
      document.getElementById('real-cutover-alignment').textContent =
        `${realCutover.alignment_status || '-'} / desired=${realCutover.desired_input_source_policy || '-'} / actual=${realCutover.actual_input_source_policy || '-'}`;
      document.getElementById('real-cutover-reason').textContent = realCutover.reason || '-';
      document.getElementById('real-input-flow').textContent =
        `${realInputFlow.status || '-'} / ${realInputFlow.reason || '-'}`;
      document.getElementById('slot-source-mix').textContent =
        `active s=${slotSourceMix.sample_active_slots ?? 0} o=${slotSourceMix.original_active_slots ?? 0} x=${slotSourceMix.other_active_slots ?? 0} / queued s=${slotSourceMix.sample_queued_slots ?? 0} o=${slotSourceMix.original_queued_slots ?? 0} x=${slotSourceMix.other_queued_slots ?? 0} / dominant=${slotSourceMix.dominant_source || '-'}`;
      document.getElementById('rollout-state').textContent = rollout.state || '-';
      document.getElementById('rollout-canary-status').textContent = rollout.canary_status || '-';
      document.getElementById('rollout-canary-gate').textContent = rollout.canary_gate || rollout.canary_reason || '-';
      document.getElementById('rollout-canary-paths').textContent =
        rollout.canary_validation_root
          ? `${rollout.canary_validation_root} / db=${rollout.canary_db_path || '-'} / delete_failed=${rollout.canary_delete_failed_dir || '-'}`
          : '-';
      document.getElementById('rollout-canary-artifacts').textContent =
        rollout.canary_output_root_exists == null
          ? '-'
          : `root=${rollout.canary_output_root_exists ? 'present' : 'missing'} / materialized=${rollout.canary_materialized_output_present ? 'yes' : 'no'} / csv=${rollout.canary_output_variables_csv_gate || '-'}`;
      document.getElementById('rollout-canary-files').textContent =
        (rollout.canary_output_variables_csv_path || rollout.canary_run_log_path || rollout.canary_exit_code_path)
          ? `csv=${rollout.canary_output_variables_csv_path || '-'} / run.log=${rollout.canary_run_log_path || '-'} / exit.code=${rollout.canary_exit_code_path || '-'}`
          : '-';
      document.getElementById('rollout-restart-status').textContent = rollout.restart_status || '-';
      document.getElementById('rollout-truth-status').textContent = rollout.rediscovery_status || '-';
      document.getElementById('rollout-resource-status').textContent = rollout.resource_status || '-';
      document.getElementById('rollout-no-space').textContent = rollout.no_space_active
        ? `${rollout.no_space_node || '-'} / ${rollout.no_space_source || 'event'}`
        : '-';
      document.getElementById('rollout-resource-detail').textContent = badNodes.length
        ? `${badNodes.map(item => item.node).join(', ')} (${badNodes.length})`
        : (rollout.resource_reason || rollout.last_resource_stage || '-');
      document.getElementById('rollout-boundary').textContent =
        `${boundary.input_source_policy || '-'} / ${boundary.public_storage_mode || '-'}`;
      renderBadNodes(badNodes);
    }

    async function refreshOverview() {
      const overview = await fetchJson('/api/overview');
      const health = overview.health || {};
      const throughput = overview.throughput_kpi || {};
      const resource = overview.resource_summary || {};
      const node = overview.node_summary || {};
      const workerMix = overview.worker_mix || {};
      const observed = overview.observed_node_summary || {};
      const slotSummary = overview.slot_summary || {};
      const throughputCompare = overview.throughput_compare || {};
      const throughputFeasibility = overview.throughput_feasibility || {};
      const opsWindow = overview.ops_window || {};
      const opsWindowDelta = overview.ops_window_delta || {};
      const opsControls = overview.ops_controls || {};
      const opsWindowRunbook = overview.ops_window_runbook || {};
      const opsWindowChecklist = overview.ops_window_checklist || {};
      const opsWindowGuardrails = overview.ops_window_guardrails || {};
      const realCutover = overview.real_cutover || {};
      const tunnel = overview.tunnel_summary || {};
      const rollout = overview.rollout_status || {};
      const rolloutRealInputFlow = rollout.real_input_flow || {};
      const rolloutSlotSourceMix = rollout.slot_source_mix || {};
      const realInputFlow = Object.keys(rolloutRealInputFlow).length ? rolloutRealInputFlow : (overview.real_input_flow || {});
      const slotSourceMix = Object.keys(rolloutSlotSourceMix).length ? rolloutSlotSourceMix : (overview.slot_source_mix || {});
      const triage = overview.ops_triage || {};
      currentRunId = overview.run ? overview.run.run_id : null;

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
      document.getElementById('return-live').textContent = tunnel.live_return_path_workers ?? 0;
      document.getElementById('w-active').textContent = throughput.active_slots ?? '-';
      document.getElementById('slot-idle').textContent = Math.max((throughput.configured_target_slots || 0) - (throughput.active_slots || 0), 0);
      document.getElementById('refill-lag').textContent = throughput.recovery_backlog_slots ?? '-';
      document.getElementById('tunnel-bad').textContent = tunnel.degraded_workers ?? 0;
      document.getElementById('tunnel-stale').textContent = tunnel.stale_workers ?? 0;
      document.getElementById('slot-succeeded').textContent = slotSummary.succeeded_slots ?? '-';
      document.getElementById('slot-failed').textContent = slotSummary.failed_slots ?? triage.failed_slots ?? '-';
      document.getElementById('slot-mode').textContent = throughput.throughput_mode || '-';
      document.getElementById('ops-window').textContent =
        `${opsWindow.label || '-'} / ${opsWindow.goal || '-'}`;
      document.getElementById('ops-restart-phase').textContent = opsControls.restart_phase || '-';
      document.getElementById('ops-restart-phase-detail').textContent = opsControls.restart_phase_detail || '-';
      document.getElementById('ops-window-checks').textContent =
        Array.isArray(opsWindow.checks) ? opsWindow.checks.join(' / ') : '-';
      document.getElementById('ops-window-delta').textContent = opsWindowDelta.window_label
        ? `idle ${opsWindowDelta.idle_slots_window_start ?? '-'} -> ${opsWindowDelta.idle_slots_now ?? '-'} (${opsWindowDelta.idle_slots_trend || '-'}) / tp ${opsWindowDelta.window_valid_succeeded_slots_per_hour ?? '-'} vs ${opsWindowDelta.previous_window_valid_succeeded_slots_per_hour ?? '-'} / fail ${opsWindowDelta.current_window_failed_slots ?? '-'} vs ${opsWindowDelta.previous_window_failed_slots ?? '-'} / node ${opsWindowDelta.current_window_node_error_events ?? '-'} vs ${opsWindowDelta.previous_window_node_error_events ?? '-'}`
        : '-';
      document.getElementById('ops-window-stop-status').textContent = opsWindowRunbook.stop_status || '-';
      renderOpsItemList('ops-window-stop-list', opsWindowRunbook.stop_items || [], 'no stop-before checks');
      document.getElementById('ops-window-canary-status').textContent = opsWindowRunbook.canary_status || '-';
      renderOpsItemList('ops-window-canary-list', opsWindowRunbook.canary_items || [], 'no canary checks');
      document.getElementById('ops-window-status').textContent = opsWindowChecklist.status || '-';
      renderOpsItemList('ops-window-checklist-list', opsWindowChecklist.items || [], 'no window checklist');
      document.getElementById('ops-window-no-go-status').textContent = opsWindowGuardrails.no_go_status || '-';
      renderOpsItemList('ops-window-no-go-list', opsWindowGuardrails.no_go_items || [], 'no no-go conditions');
      document.getElementById('ops-window-start-status').textContent = opsWindowGuardrails.start_status || '-';
      renderOpsItemList('ops-window-start-list', opsWindowGuardrails.start_items || [], 'no start-after checks');
      document.getElementById('triage-status').textContent = triage.status || '-';
      document.getElementById('triage-priority').textContent = triage.priority || '-';
      document.getElementById('triage-signals').textContent =
        `${triage.csv_state || '-'} / ${triage.restart_state || '-'} / ${triage.resource_state || '-'} / ${triage.throughput_state || '-'}`;
      document.getElementById('triage-metrics').textContent =
        `${triage.rolling_60m_throughput ?? '-'} pt/h / ${triage.idle_slots ?? '-'} / ${triage.failed_slots ?? '-'}`;
      document.getElementById('triage-throughput-compare').textContent =
        `30m=${throughputCompare.rolling_30m_throughput ?? '-'} / 60m=${throughputCompare.rolling_60m_throughput ?? '-'} / base=${throughputCompare.baseline_60m ?? '-'} / gap=${throughputCompare.target_gap_60m ?? '-'}`;
      document.getElementById('triage-target-track').textContent =
        `${throughputFeasibility.status || '-'} / ${throughputFeasibility.hours_remaining ?? '-'}h / gap=${throughputFeasibility.target_gap_60m ?? '-'}`;
      document.getElementById('triage-reason').textContent = triage.reason || triage.headline || '-';
      renderRolloutDetail(rollout, realCutover, realInputFlow, slotSourceMix);

      document.getElementById('res-alloc').textContent = fmtMb(node.allocated_mem_mb ?? resource.allocated_mem_mb);
      document.getElementById('res-used').textContent = fmtMb(node.used_mem_mb ?? resource.used_mem_mb);
      document.getElementById('res-free').textContent = fmtMb(node.free_mem_mb ?? resource.free_mem_mb);
      document.getElementById('res-tmp-free').textContent = fmtMb(node.tmp_free_mb);
      document.getElementById('res-tmp-status').textContent = node.tmp_free_status == null
        ? '-'
        : (node.tmp_free_status === 'RED' && node.tmp_free_shortfall_mb != null
            ? `${node.tmp_free_status} / -${fmtMb(node.tmp_free_shortfall_mb)}`
            : node.tmp_free_status);
      document.getElementById('res-load').textContent = (node.load_1 ?? resource.load_1) == null ? '-' : (node.load_1 ?? resource.load_1).toFixed(2);
      document.getElementById('res-observed-node').textContent = observed.primary_node == null
        ? '-'
        : (observed.node_count > 1 ? `${observed.primary_node} +${observed.node_count - 1}` : observed.primary_node);
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
      const rolloutPath = currentRunId
        ? `/api/operations/rollout?run_id=${encodeURIComponent(currentRunId)}`
        : '/api/operations/rollout';
      const [events, workers, slots, rollout] = await Promise.all([
        fetchJson('/api/events/recent?limit=24'),
        fetchJson('/api/workers'),
        fetchJson('/api/slots?limit=40'),
        fetchJson(rolloutPath),
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
      for (const slot of ((slots || {}).slots || [])) {
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
      renderRolloutDetail(
        rollout || {},
        (rollout || {}).real_cutover || {},
        (rollout || {}).real_input_flow || {},
        (rollout || {}).slot_source_mix || {},
      );
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
