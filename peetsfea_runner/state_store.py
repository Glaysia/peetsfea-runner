from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

import duckdb


_GLOBAL_DUCKDB_LOCK = Lock()


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _run_prefix(namespace: str) -> str:
    normalized = namespace.strip()
    return f"{normalized}_" if normalized else ""


@dataclass(slots=True)
class StateStore:
    db_path: Path
    _lock: Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.db_path = self.db_path.expanduser().resolve()
        self._lock = _GLOBAL_DUCKDB_LOCK
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        state TEXT NOT NULL,
                        summary TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        input_path TEXT NOT NULL,
                        output_path TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        last_attempt_no INTEGER NOT NULL DEFAULT 0,
                        failure_reason TEXT,
                        PRIMARY KEY (run_id, job_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS attempts (
                        run_id TEXT NOT NULL,
                        attempt_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        attempt_no INTEGER NOT NULL,
                        node TEXT,
                        started_at TEXT NOT NULL,
                        ended_at TEXT,
                        exit_code INTEGER,
                        error TEXT,
                        PRIMARY KEY (run_id, attempt_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS artifacts (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        artifact_root TEXT NOT NULL,
                        size_bytes BIGINT,
                        checksum TEXT,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        level TEXT NOT NULL,
                        message TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS file_lifecycle (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        slot_id TEXT,
                        input_path TEXT NOT NULL,
                        input_deleted_at TEXT,
                        delete_retry_count INTEGER NOT NULL DEFAULT 0,
                        delete_final_state TEXT NOT NULL DEFAULT 'PENDING',
                        quarantine_path TEXT,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_heartbeat (
                        service_name TEXT NOT NULL,
                        host TEXT NOT NULL,
                        pid INTEGER NOT NULL,
                        last_seen_ts TEXT NOT NULL,
                        run_id TEXT,
                        status TEXT NOT NULL,
                        PRIMARY KEY (service_name, host, pid)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS slot_tasks (
                        run_id TEXT NOT NULL,
                        slot_id TEXT NOT NULL,
                        job_id TEXT,
                        account_id TEXT,
                        input_path TEXT NOT NULL,
                        output_path TEXT NOT NULL,
                        state TEXT NOT NULL,
                        attempt_no INTEGER NOT NULL DEFAULT 0,
                        failure_reason TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, slot_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS slot_events (
                        run_id TEXT NOT NULL,
                        slot_id TEXT NOT NULL,
                        level TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        message TEXT NOT NULL,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ingest_index (
                        input_path TEXT PRIMARY KEY,
                        ready_path TEXT NOT NULL,
                        ready_present BOOLEAN NOT NULL DEFAULT FALSE,
                        ready_mode TEXT NOT NULL DEFAULT 'SIDECAR',
                        ready_error TEXT,
                        ready_mtime_ns BIGINT,
                        file_size BIGINT NOT NULL,
                        file_mtime_ns BIGINT NOT NULL,
                        discovered_at TEXT NOT NULL,
                        last_rearmed_at TEXT,
                        state TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS account_capacity_snapshots (
                        account_id TEXT NOT NULL,
                        host TEXT NOT NULL,
                        running_count INTEGER NOT NULL,
                        pending_count INTEGER NOT NULL,
                        allowed_submit INTEGER NOT NULL,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS account_readiness_snapshots (
                        account_id TEXT NOT NULL,
                        host TEXT NOT NULL,
                        ready BOOLEAN NOT NULL,
                        status TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        home_ok BOOLEAN NOT NULL,
                        runtime_path_ok BOOLEAN NOT NULL,
                        env_ok BOOLEAN NOT NULL,
                        python_ok BOOLEAN NOT NULL,
                        module_ok BOOLEAN NOT NULL,
                        binaries_ok BOOLEAN NOT NULL,
                        ansys_ok BOOLEAN NOT NULL,
                        uv_ok BOOLEAN NOT NULL DEFAULT FALSE,
                        pyaedt_ok BOOLEAN NOT NULL DEFAULT FALSE,
                        storage_ready BOOLEAN NOT NULL DEFAULT TRUE,
                        storage_reason TEXT NOT NULL DEFAULT 'ok',
                        inode_use_percent INTEGER,
                        free_mb BIGINT,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS slurm_workers (
                        run_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        attempt_no INTEGER NOT NULL,
                        account_id TEXT NOT NULL,
                        host_alias TEXT NOT NULL,
                        slurm_job_id TEXT NOT NULL,
                        worker_state TEXT NOT NULL,
                        observed_node TEXT,
                        slots_configured INTEGER NOT NULL,
                        backend TEXT NOT NULL,
                        tunnel_session_id TEXT,
                        tunnel_state TEXT NOT NULL DEFAULT 'PENDING',
                        heartbeat_ts TEXT,
                        degraded_reason TEXT,
                        collect_probe_state TEXT,
                        marker_present BOOLEAN,
                        submitted_at TEXT,
                        started_at TEXT,
                        ended_at TEXT,
                        last_seen_ts TEXT NOT NULL,
                        PRIMARY KEY (run_id, worker_id)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS node_resource_snapshots (
                        run_id TEXT NOT NULL,
                        host TEXT NOT NULL,
                        allocated_mem_mb BIGINT,
                        total_mem_mb BIGINT,
                        used_mem_mb BIGINT,
                        free_mem_mb BIGINT,
                        load_1 DOUBLE,
                        load_5 DOUBLE,
                        load_15 DOUBLE,
                        tmp_total_mb BIGINT,
                        tmp_used_mb BIGINT,
                        tmp_free_mb BIGINT,
                        process_count INTEGER NOT NULL DEFAULT 0,
                        running_worker_count INTEGER NOT NULL DEFAULT 0,
                        active_slot_count INTEGER NOT NULL DEFAULT 0,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_resource_snapshots (
                        run_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        host TEXT NOT NULL,
                        slurm_job_id TEXT,
                        configured_slots INTEGER NOT NULL DEFAULT 0,
                        active_slots INTEGER NOT NULL DEFAULT 0,
                        idle_slots INTEGER NOT NULL DEFAULT 0,
                        rss_mb BIGINT,
                        cpu_pct DOUBLE,
                        tunnel_state TEXT,
                        process_count INTEGER NOT NULL DEFAULT 0,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS slot_resource_snapshots (
                        run_id TEXT NOT NULL,
                        slot_id TEXT NOT NULL,
                        worker_id TEXT,
                        host TEXT,
                        allocated_mem_mb BIGINT,
                        used_mem_mb BIGINT,
                        load_1 DOUBLE,
                        rss_mb BIGINT,
                        cpu_pct DOUBLE,
                        process_count INTEGER NOT NULL DEFAULT 0,
                        active_process_count INTEGER NOT NULL DEFAULT 0,
                        artifact_bytes BIGINT NOT NULL DEFAULT 0,
                        progress_ts TEXT,
                        state TEXT,
                        ts TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS resource_summary_snapshots (
                        run_id TEXT NOT NULL,
                        host TEXT NOT NULL,
                        allocated_mem_mb BIGINT,
                        used_mem_mb BIGINT,
                        free_mem_mb BIGINT,
                        load_1 DOUBLE,
                        running_worker_count INTEGER NOT NULL DEFAULT 0,
                        active_slot_count INTEGER NOT NULL DEFAULT 0,
                        stale BOOLEAN NOT NULL DEFAULT FALSE,
                        ts TEXT NOT NULL
                    )
                    """
                )
                # Backward-compatible tables retained for existing tooling/queries.
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_events (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        event_time TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        details TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS quarantine_jobs (
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        quarantined_at TEXT NOT NULL,
                        reason TEXT,
                        exit_code INTEGER
                    )
                    """
                )
                readiness_columns = {
                    str(row[1]).strip().lower()
                    for row in conn.execute("PRAGMA table_info('account_readiness_snapshots')").fetchall()
                }
                if "venv_ok" in readiness_columns and "env_ok" not in readiness_columns:
                    conn.execute("ALTER TABLE account_readiness_snapshots RENAME COLUMN venv_ok TO env_ok")
                    readiness_columns.remove("venv_ok")
                    readiness_columns.add("env_ok")
                if "env_ok" not in readiness_columns:
                    conn.execute("ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS env_ok BOOLEAN DEFAULT FALSE")
                conn.execute("ALTER TABLE file_lifecycle ADD COLUMN IF NOT EXISTS slot_id TEXT")
                conn.execute("ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS uv_ok BOOLEAN DEFAULT FALSE")
                conn.execute(
                    "ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS pyaedt_ok BOOLEAN DEFAULT FALSE"
                )
                conn.execute(
                    "ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS storage_ready BOOLEAN DEFAULT TRUE"
                )
                conn.execute(
                    "ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS storage_reason TEXT DEFAULT 'ok'"
                )
                conn.execute(
                    "ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS inode_use_percent INTEGER"
                )
                conn.execute("ALTER TABLE account_readiness_snapshots ADD COLUMN IF NOT EXISTS free_mb BIGINT")
                conn.execute("ALTER TABLE ingest_index ADD COLUMN IF NOT EXISTS ready_present BOOLEAN DEFAULT FALSE")
                conn.execute("ALTER TABLE ingest_index ADD COLUMN IF NOT EXISTS ready_mode TEXT DEFAULT 'SIDECAR'")
                conn.execute("ALTER TABLE ingest_index ADD COLUMN IF NOT EXISTS ready_error TEXT")
                conn.execute("ALTER TABLE ingest_index ADD COLUMN IF NOT EXISTS ready_mtime_ns BIGINT")
                conn.execute("ALTER TABLE ingest_index ADD COLUMN IF NOT EXISTS last_rearmed_at TEXT")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS tunnel_session_id TEXT")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS tunnel_state TEXT DEFAULT 'PENDING'")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS heartbeat_ts TEXT")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS degraded_reason TEXT")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS collect_probe_state TEXT")
                conn.execute("ALTER TABLE slurm_workers ADD COLUMN IF NOT EXISTS marker_present BOOLEAN")
                conn.execute("ALTER TABLE node_resource_snapshots ADD COLUMN IF NOT EXISTS allocated_mem_mb BIGINT")
                conn.execute("ALTER TABLE node_resource_snapshots ADD COLUMN IF NOT EXISTS process_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE slot_resource_snapshots ADD COLUMN IF NOT EXISTS allocated_mem_mb BIGINT")
                conn.execute("ALTER TABLE slot_resource_snapshots ADD COLUMN IF NOT EXISTS used_mem_mb BIGINT")
                conn.execute("ALTER TABLE slot_resource_snapshots ADD COLUMN IF NOT EXISTS load_1 DOUBLE")
                conn.execute("ALTER TABLE slot_resource_snapshots ADD COLUMN IF NOT EXISTS process_count INTEGER DEFAULT 0")
            finally:
                conn.close()

    def start_run(self, run_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    "INSERT INTO runs (run_id, started_at, state) VALUES (?, ?, ?)",
                    [run_id, now, "RUNNING"],
                )
            finally:
                conn.close()

    def ensure_continuous_run(self, *, rotation_hours: int, namespace: str = "") -> str:
        now = datetime.now(tz=timezone.utc)
        now_iso = now.isoformat()
        prefix = _run_prefix(namespace)
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                if prefix:
                    row = conn.execute(
                        """
                        SELECT run_id, started_at
                        FROM runs
                        WHERE state = 'RUNNING' AND run_id LIKE ?
                        ORDER BY started_at DESC
                        LIMIT 1
                        """,
                        [f"{prefix}%"],
                    ).fetchone()
                else:
                    row = conn.execute(
                        """
                        SELECT run_id, started_at
                        FROM runs
                        WHERE state = 'RUNNING'
                        ORDER BY started_at DESC
                        LIMIT 1
                        """
                    ).fetchone()
                if row is not None:
                    run_id = str(row[0])
                    started_at_raw = row[1]
                    started_at = datetime.fromisoformat(started_at_raw) if isinstance(started_at_raw, str) else now
                    if started_at.tzinfo is None:
                        started_at = started_at.replace(tzinfo=timezone.utc)
                    if now - started_at < timedelta(hours=rotation_hours):
                        return run_id
                    conn.execute(
                        """
                        UPDATE runs
                        SET finished_at = ?, state = ?, summary = ?
                        WHERE run_id = ?
                        """,
                        [now_iso, "ROLLED", f"auto-rolled after {rotation_hours}h", run_id],
                    )
                new_run_id = f"{prefix}{now.strftime('%Y%m%d_%H%M%S')}"
                conn.execute(
                    "INSERT INTO runs (run_id, started_at, state) VALUES (?, ?, ?)",
                    [new_run_id, now_iso, "RUNNING"],
                )
                return new_run_id
            finally:
                conn.close()

    def update_run_summary(self, *, run_id: str, summary: str) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute("UPDATE runs SET summary = ? WHERE run_id = ?", [summary, run_id])
            finally:
                conn.close()

    def finish_run(self, run_id: str, *, state: str, summary: str) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    "UPDATE runs SET finished_at = ?, state = ?, summary = ? WHERE run_id = ?",
                    [_utc_now_iso(), state, summary, run_id],
                )
            finally:
                conn.close()

    def create_job(self, *, run_id: str, job_id: str, input_path: str, output_path: str, account_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        run_id, job_id, input_path, output_path, account_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, input_path, output_path, account_id, "PENDING", now, now],
                )
                conn.execute(
                    """
                    INSERT INTO file_lifecycle (run_id, job_id, slot_id, input_path, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, None, input_path, now],
                )
            finally:
                conn.close()

    def create_slot_task(
        self,
        *,
        run_id: str,
        slot_id: str,
        input_path: str,
        output_path: str,
        account_id: str | None,
        state: str = "QUEUED",
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO slot_tasks (
                        run_id, slot_id, account_id, input_path, output_path, state, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, slot_id, account_id, input_path, output_path, state, now, now],
                )
                conn.execute(
                    """
                    INSERT INTO file_lifecycle (run_id, job_id, slot_id, input_path, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [run_id, slot_id, slot_id, input_path, now],
                )
            finally:
                conn.close()

    def update_slot_task(
        self,
        *,
        run_id: str,
        slot_id: str,
        state: str,
        attempt_no: int | None = None,
        job_id: str | None = None,
        account_id: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                fields: list[str] = ["state = ?", "updated_at = ?", "failure_reason = ?"]
                values: list[object] = [state, now, failure_reason]
                if attempt_no is not None:
                    fields.append("attempt_no = ?")
                    values.append(attempt_no)
                if job_id is not None:
                    fields.append("job_id = ?")
                    values.append(job_id)
                if account_id is not None:
                    fields.append("account_id = ?")
                    values.append(account_id)
                values.extend([run_id, slot_id])
                conn.execute(
                    f"UPDATE slot_tasks SET {', '.join(fields)} WHERE run_id = ? AND slot_id = ?",
                    values,
                )
            finally:
                conn.close()

    def get_slot_throughput_score(self, *, run_id: str, account_id: str) -> tuple[int, int]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                completed_row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM slot_tasks
                    WHERE run_id = ? AND account_id = ? AND state IN ('SUCCEEDED', 'FAILED', 'QUARANTINED')
                    """,
                    [run_id, account_id],
                ).fetchone()
                inflight_row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM slot_tasks
                    WHERE run_id = ? AND account_id = ? AND state IN ('ASSIGNED', 'UPLOADING', 'RUNNING', 'COLLECTING')
                    """,
                    [run_id, account_id],
                ).fetchone()
            finally:
                conn.close()
        completed = int(completed_row[0]) if completed_row is not None else 0
        inflight = int(inflight_row[0]) if inflight_row is not None else 0
        return completed, inflight

    def count_active_jobs_by_account(self, *, run_id: str) -> dict[str, int]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                rows = conn.execute(
                    """
                    SELECT account_id, COUNT(*)
                    FROM jobs
                    WHERE run_id = ? AND status IN ('PENDING', 'SUBMITTED', 'RUNNING')
                    GROUP BY account_id
                    ORDER BY account_id
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()
        return {str(row[0]): int(row[1]) for row in rows}

    def upsert_slurm_worker(
        self,
        *,
        run_id: str,
        worker_id: str,
        job_id: str,
        attempt_no: int,
        account_id: str,
        host_alias: str,
        slurm_job_id: str,
        worker_state: str,
        slots_configured: int,
        backend: str,
        observed_node: str | None = None,
        tunnel_session_id: str | None = None,
        tunnel_state: str | None = None,
        heartbeat_ts: str | None = None,
        degraded_reason: str | None = None,
        collect_probe_state: str | None = None,
        marker_present: bool | None = None,
    ) -> None:
        now = _utc_now_iso()
        normalized_state = worker_state.strip().upper()
        normalized_tunnel_state = tunnel_state.strip().upper() if tunnel_state is not None else None
        submitted_at = now if normalized_state in {"SUBMITTED", "PENDING", "RUNNING", "IDLE_DRAINING", "COMPLETED", "FAILED", "LOST"} else None
        started_at = now if normalized_state in {"RUNNING", "IDLE_DRAINING", "COMPLETED", "FAILED", "LOST"} else None
        ended_at = now if normalized_state in {"COMPLETED", "FAILED", "LOST"} else None
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                existing = conn.execute(
                    """
                    SELECT submitted_at, started_at, ended_at, tunnel_session_id, tunnel_state, heartbeat_ts, degraded_reason, collect_probe_state, marker_present
                    FROM slurm_workers
                    WHERE run_id = ? AND worker_id = ?
                    """,
                    [run_id, worker_id],
                ).fetchone()
                existing_submitted = existing[0] if existing is not None else None
                existing_started = existing[1] if existing is not None else None
                existing_ended = existing[2] if existing is not None else None
                existing_tunnel_session_id = existing[3] if existing is not None else None
                existing_tunnel_state = existing[4] if existing is not None else None
                existing_heartbeat_ts = existing[5] if existing is not None else None
                existing_degraded_reason = existing[6] if existing is not None else None
                existing_collect_probe_state = existing[7] if existing is not None else None
                existing_marker_present = existing[8] if existing is not None else None
                conn.execute(
                    """
                    INSERT INTO slurm_workers (
                        run_id, worker_id, job_id, attempt_no, account_id, host_alias,
                        slurm_job_id, worker_state, observed_node, slots_configured, backend,
                        tunnel_session_id, tunnel_state, heartbeat_ts, degraded_reason, collect_probe_state, marker_present,
                        submitted_at, started_at, ended_at, last_seen_ts
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (run_id, worker_id) DO UPDATE SET
                        job_id = EXCLUDED.job_id,
                        attempt_no = EXCLUDED.attempt_no,
                        account_id = EXCLUDED.account_id,
                        host_alias = EXCLUDED.host_alias,
                        slurm_job_id = EXCLUDED.slurm_job_id,
                        worker_state = EXCLUDED.worker_state,
                        observed_node = EXCLUDED.observed_node,
                        slots_configured = EXCLUDED.slots_configured,
                        backend = EXCLUDED.backend,
                        tunnel_session_id = EXCLUDED.tunnel_session_id,
                        tunnel_state = EXCLUDED.tunnel_state,
                        heartbeat_ts = EXCLUDED.heartbeat_ts,
                        degraded_reason = EXCLUDED.degraded_reason,
                        collect_probe_state = EXCLUDED.collect_probe_state,
                        marker_present = EXCLUDED.marker_present,
                        submitted_at = EXCLUDED.submitted_at,
                        started_at = EXCLUDED.started_at,
                        ended_at = EXCLUDED.ended_at,
                        last_seen_ts = EXCLUDED.last_seen_ts
                    """,
                    [
                        run_id,
                        worker_id,
                        job_id,
                        attempt_no,
                        account_id,
                        host_alias,
                        slurm_job_id,
                        normalized_state,
                        observed_node,
                        slots_configured,
                        backend,
                        tunnel_session_id if tunnel_session_id is not None else existing_tunnel_session_id,
                        normalized_tunnel_state if normalized_tunnel_state is not None else (existing_tunnel_state or "PENDING"),
                        heartbeat_ts if heartbeat_ts is not None else existing_heartbeat_ts,
                        degraded_reason if degraded_reason is not None else existing_degraded_reason,
                        collect_probe_state if collect_probe_state is not None else existing_collect_probe_state,
                        marker_present if marker_present is not None else existing_marker_present,
                        existing_submitted or submitted_at,
                        existing_started or started_at,
                        existing_ended or ended_at,
                        now,
                    ],
                )
            finally:
                conn.close()

    def get_slurm_worker(self, *, run_id: str, worker_id: str) -> dict[str, object] | None:
        rows = self.list_slurm_workers(run_id=run_id)
        for row in rows:
            if row["worker_id"] == worker_id:
                return row
        return None

    def update_slurm_worker_control_plane(
        self,
        *,
        run_id: str,
        worker_id: str,
        tunnel_state: str,
        tunnel_session_id: str | None = None,
        heartbeat_ts: str | None = None,
        observed_node: str | None = None,
        degraded_reason: str | None = None,
    ) -> None:
        now = heartbeat_ts or _utc_now_iso()
        normalized_tunnel_state = tunnel_state.strip().upper()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE slurm_workers
                    SET
                        tunnel_session_id = COALESCE(?, tunnel_session_id),
                        tunnel_state = ?,
                        heartbeat_ts = ?,
                        observed_node = COALESCE(?, observed_node),
                        degraded_reason = ?,
                        last_seen_ts = ?
                    WHERE run_id = ? AND worker_id = ?
                    """,
                    [
                        tunnel_session_id,
                        normalized_tunnel_state,
                        now,
                        observed_node,
                        degraded_reason,
                        now,
                        run_id,
                        worker_id,
                    ],
                )
            finally:
                conn.close()

    def list_active_slurm_workers(self, *, run_id: str) -> list[dict[str, object]]:
        return self.list_slurm_workers(
            run_id=run_id,
            worker_states=("SUBMITTED", "PENDING", "RUNNING", "IDLE_DRAINING"),
        )

    def list_slurm_workers(
        self,
        *,
        run_id: str,
        worker_states: tuple[str, ...] | None = None,
    ) -> list[dict[str, object]]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                if worker_states:
                    placeholders = ", ".join(["?"] * len(worker_states))
                    rows = conn.execute(
                        f"""
                        SELECT
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
                            collect_probe_state,
                            marker_present,
                            submitted_at,
                            started_at,
                            ended_at,
                            last_seen_ts
                        FROM slurm_workers
                        WHERE run_id = ? AND worker_state IN ({placeholders})
                        ORDER BY submitted_at, worker_id
                        """,
                        [run_id, *worker_states],
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT
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
                            collect_probe_state,
                            marker_present,
                            submitted_at,
                            started_at,
                            ended_at,
                            last_seen_ts
                        FROM slurm_workers
                        WHERE run_id = ?
                        ORDER BY submitted_at, worker_id
                        """,
                        [run_id],
                    ).fetchall()
            finally:
                conn.close()
        return [
            {
                "worker_id": str(row[0]),
                "job_id": str(row[1]),
                "attempt_no": int(row[2]),
                "account_id": str(row[3]),
                "host_alias": str(row[4]),
                "slurm_job_id": str(row[5]),
                "worker_state": str(row[6]),
                "observed_node": row[7],
                "slots_configured": int(row[8]),
                "backend": str(row[9]),
                "tunnel_session_id": row[10],
                "tunnel_state": str(row[11]) if row[11] is not None else None,
                "heartbeat_ts": row[12],
                "degraded_reason": row[13],
                "collect_probe_state": row[14],
                "marker_present": bool(row[15]) if row[15] is not None else None,
                "submitted_at": row[16],
                "started_at": row[17],
                "ended_at": row[18],
                "last_seen_ts": row[19],
            }
            for row in rows
        ]

    def list_schedulable_slot_tasks(self, *, run_id: str) -> list[tuple[str, str, str, int]]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                rows = conn.execute(
                    """
                    SELECT slot_id, input_path, output_path, attempt_no
                    FROM slot_tasks
                    WHERE run_id = ?
                      AND state IN ('QUEUED', 'RETRY_QUEUED')
                    ORDER BY created_at, slot_id
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()
        return [(str(row[0]), str(row[1]), str(row[2]), int(row[3] or 0)) for row in rows]

    def get_next_job_index(self, *, run_id: str) -> int:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                rows = conn.execute("SELECT job_id FROM jobs WHERE run_id = ?", [run_id]).fetchall()
            finally:
                conn.close()
        max_index = 0
        for row in rows:
            job_id = str(row[0])
            if not job_id.startswith("job_"):
                continue
            suffix = job_id[4:]
            if not suffix.isdigit():
                continue
            max_index = max(max_index, int(suffix))
        return max_index + 1

    def mark_ingest_state(self, *, input_path: str, state: str) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute("UPDATE ingest_index SET state = ? WHERE input_path = ?", [state, input_path])
            finally:
                conn.close()

    def append_slot_event(self, *, run_id: str, slot_id: str, level: str, stage: str, message: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO slot_events (run_id, slot_id, level, stage, message, ts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, slot_id, level, stage, message, now],
                )
            finally:
                conn.close()

    def count_slots_by_state(self, *, run_id: str) -> dict[str, int]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                rows = conn.execute(
                    """
                    SELECT state, COUNT(*)
                    FROM slot_tasks
                    WHERE run_id = ?
                    GROUP BY state
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()
        return {str(row[0]): int(row[1]) for row in rows}

    def register_ingest_candidate(
        self,
        *,
        input_path: str,
        ready_path: str,
        ready_present: bool,
        ready_mode: str,
        ready_error: str | None,
        ready_mtime_ns: int | None,
        file_size: int,
        file_mtime_ns: int,
    ) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                row = conn.execute(
                    """
                    SELECT
                        file_size,
                        file_mtime_ns,
                        ready_present,
                        ready_mode,
                        COALESCE(ready_error, ''),
                        state,
                        COALESCE(ready_mtime_ns, 0)
                    FROM ingest_index
                    WHERE input_path = ?
                    """,
                    [input_path],
                ).fetchone()
                if row is not None:
                    prev_size = int(row[0])
                    prev_mtime = int(row[1])
                    prev_ready_present = bool(row[2])
                    prev_ready_mode = str(row[3])
                    prev_ready_error = str(row[4]) or None
                    prev_state = str(row[5]).strip().upper()
                    prev_ready_mtime_ns = int(row[6] or 0)
                    if prev_size == file_size and prev_mtime == file_mtime_ns:
                        should_rearm = (
                            ready_present
                            and ready_mtime_ns is not None
                            and ready_mtime_ns > prev_ready_mtime_ns
                            and prev_state in {"FAILED", "QUARANTINED", "DELETE_QUARANTINED"}
                        )
                        if should_rearm:
                            conn.execute(
                                """
                                UPDATE ingest_index
                                SET ready_path = ?, ready_present = ?, ready_mode = ?, ready_error = ?,
                                    ready_mtime_ns = ?, discovered_at = ?, last_rearmed_at = ?, state = ?
                                WHERE input_path = ?
                                """,
                                [
                                    ready_path,
                                    ready_present,
                                    ready_mode,
                                    ready_error,
                                    ready_mtime_ns,
                                    now,
                                    now,
                                    "READY",
                                    input_path,
                                ],
                            )
                            return True
                        if (
                            prev_ready_present != ready_present
                            or prev_ready_mode != ready_mode
                            or prev_ready_error != ready_error
                            or prev_ready_mtime_ns != int(ready_mtime_ns or 0)
                        ):
                            conn.execute(
                                """
                                UPDATE ingest_index
                                SET ready_path = ?, ready_present = ?, ready_mode = ?, ready_error = ?,
                                    ready_mtime_ns = ?, discovered_at = ?
                                WHERE input_path = ?
                                """,
                                [ready_path, ready_present, ready_mode, ready_error, ready_mtime_ns, now, input_path],
                            )
                        return False
                    conn.execute(
                        """
                        UPDATE ingest_index
                        SET ready_path = ?, ready_present = ?, ready_mode = ?, ready_error = ?,
                            ready_mtime_ns = ?, file_size = ?, file_mtime_ns = ?, discovered_at = ?,
                            last_rearmed_at = NULL, state = ?
                        WHERE input_path = ?
                        """,
                        [
                            ready_path,
                            ready_present,
                            ready_mode,
                            ready_error,
                            ready_mtime_ns,
                            file_size,
                            file_mtime_ns,
                            now,
                            "READY",
                            input_path,
                        ],
                    )
                    return True

                conn.execute(
                    """
                    INSERT INTO ingest_index (
                        input_path, ready_path, ready_present, ready_mode, ready_error,
                        ready_mtime_ns, file_size, file_mtime_ns, discovered_at, last_rearmed_at, state
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        input_path,
                        ready_path,
                        ready_present,
                        ready_mode,
                        ready_error,
                        ready_mtime_ns,
                        file_size,
                        file_mtime_ns,
                        now,
                        None,
                        "READY",
                    ],
                )
                return True
            finally:
                conn.close()

    def update_job_status(
        self,
        *,
        run_id: str,
        job_id: str,
        status: str,
        attempt_no: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                if attempt_no is None:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?, updated_at = ?, failure_reason = ?
                        WHERE run_id = ? AND job_id = ?
                        """,
                        [status, now, failure_reason, run_id, job_id],
                    )
                else:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?, updated_at = ?, last_attempt_no = ?, failure_reason = ?
                        WHERE run_id = ? AND job_id = ?
                        """,
                        [status, now, attempt_no, failure_reason, run_id, job_id],
                    )
            finally:
                conn.close()

    def start_attempt(self, *, run_id: str, job_id: str, attempt_no: int, node: str | None = None) -> str:
        attempt_id = f"{job_id}_a{attempt_no}"
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO attempts (run_id, attempt_id, job_id, attempt_no, node, started_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, attempt_id, job_id, attempt_no, node, _utc_now_iso()],
                )
            finally:
                conn.close()
        return attempt_id

    def finish_attempt(
        self,
        *,
        run_id: str,
        attempt_id: str,
        exit_code: int,
        error: str | None = None,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE attempts
                    SET ended_at = ?, exit_code = ?, error = ?
                    WHERE run_id = ? AND attempt_id = ?
                    """,
                    [_utc_now_iso(), exit_code, error, run_id, attempt_id],
                )
            finally:
                conn.close()

    def record_artifact(
        self,
        *,
        run_id: str,
        job_id: str,
        artifact_root: str,
        size_bytes: int | None = None,
        checksum: str | None = None,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO artifacts (run_id, job_id, artifact_root, size_bytes, checksum, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, artifact_root, size_bytes, checksum, _utc_now_iso()],
                )
            finally:
                conn.close()

    def append_event(self, *, run_id: str, job_id: str, level: str, stage: str, message: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO events (run_id, job_id, level, message, stage, ts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, level, message, stage, now],
                )
                conn.execute(
                    """
                    INSERT INTO job_events (run_id, job_id, attempt, event_time, event_type, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, 0, now, stage, message],
                )
            finally:
                conn.close()

    def mark_input_deleted(self, *, run_id: str, job_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET input_deleted_at = ?, delete_retry_count = ?, delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND job_id = ?
                    """,
                    [now, retry_count, "DELETED", now, run_id, job_id],
                )
            finally:
                conn.close()

    def mark_slot_input_deleted(self, *, run_id: str, slot_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET input_deleted_at = ?, delete_retry_count = ?, delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND slot_id = ?
                    """,
                    [now, retry_count, "DELETED", now, run_id, slot_id],
                )
            finally:
                conn.close()

    def mark_slot_delete_pending(self, *, run_id: str, slot_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND slot_id = ?
                    """,
                    ["DELETE_PENDING", now, run_id, slot_id],
                )
            finally:
                conn.close()

    def mark_slot_delete_retained(self, *, run_id: str, slot_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND slot_id = ?
                    """,
                    ["RETAINED", now, run_id, slot_id],
                )
            finally:
                conn.close()

    def mark_delete_retrying(self, *, run_id: str, job_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_retry_count = ?, delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND job_id = ?
                    """,
                    [retry_count, "DELETE_RETRYING", now, run_id, job_id],
                )
            finally:
                conn.close()

    def mark_slot_delete_retrying(self, *, run_id: str, slot_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_retry_count = ?, delete_final_state = ?, updated_at = ?
                    WHERE run_id = ? AND slot_id = ?
                    """,
                    [retry_count, "DELETE_RETRYING", now, run_id, slot_id],
                )
            finally:
                conn.close()

    def mark_delete_quarantined(
        self,
        *,
        run_id: str,
        job_id: str,
        retry_count: int,
        quarantine_path: str,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_retry_count = ?, delete_final_state = ?, quarantine_path = ?, updated_at = ?
                    WHERE run_id = ? AND job_id = ?
                    """,
                    [retry_count, "DELETE_QUARANTINED", quarantine_path, now, run_id, job_id],
                )
            finally:
                conn.close()

    def mark_slot_delete_quarantined(
        self,
        *,
        run_id: str,
        slot_id: str,
        retry_count: int,
        quarantine_path: str,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE file_lifecycle
                    SET delete_retry_count = ?, delete_final_state = ?, quarantine_path = ?, updated_at = ?
                    WHERE run_id = ? AND slot_id = ?
                    """,
                    [retry_count, "DELETE_QUARANTINED", quarantine_path, now, run_id, slot_id],
                )
            finally:
                conn.close()

    def quarantine_job(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt: int,
        reason: str,
        exit_code: int,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO quarantine_jobs (run_id, job_id, attempt, quarantined_at, reason, exit_code)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, attempt, _utc_now_iso(), reason, exit_code],
                )
            finally:
                conn.close()

    def upsert_worker_heartbeat(
        self,
        *,
        service_name: str,
        host: str,
        pid: int,
        run_id: str | None,
        status: str,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO worker_heartbeat (service_name, host, pid, last_seen_ts, run_id, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (service_name, host, pid) DO UPDATE SET
                        last_seen_ts = EXCLUDED.last_seen_ts,
                        run_id = EXCLUDED.run_id,
                        status = EXCLUDED.status
                    """,
                    [service_name, host, pid, now, run_id, status],
                )
            finally:
                conn.close()

    def record_account_capacity_snapshot(
        self,
        *,
        account_id: str,
        host: str,
        running_count: int,
        pending_count: int,
        allowed_submit: int,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO account_capacity_snapshots (
                        account_id, host, running_count, pending_count, allowed_submit, ts
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [account_id, host, running_count, pending_count, allowed_submit, _utc_now_iso()],
                )
            finally:
                conn.close()

    def record_account_readiness_snapshot(
        self,
        *,
        account_id: str,
        host: str,
        ready: bool,
        status: str,
        reason: str,
        home_ok: bool,
        runtime_path_ok: bool,
        env_ok: bool,
        python_ok: bool,
        module_ok: bool,
        binaries_ok: bool,
        ansys_ok: bool,
        uv_ok: bool = False,
        pyaedt_ok: bool = False,
        storage_ready: bool = True,
        storage_reason: str = "ok",
        inode_use_percent: int | None = None,
        free_mb: int | None = None,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO account_readiness_snapshots (
                        account_id,
                        host,
                        ready,
                        status,
                        reason,
                        home_ok,
                        runtime_path_ok,
                        env_ok,
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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        account_id,
                        host,
                        ready,
                        status,
                        reason,
                        home_ok,
                        runtime_path_ok,
                        env_ok,
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
                        _utc_now_iso(),
                    ],
                )
            finally:
                conn.close()

    def record_node_resource_snapshot(
        self,
        *,
        run_id: str,
        host: str,
        allocated_mem_mb: int | None,
        total_mem_mb: int | None,
        used_mem_mb: int | None,
        free_mem_mb: int | None,
        load_1: float | None,
        load_5: float | None,
        load_15: float | None,
        tmp_total_mb: int | None,
        tmp_used_mb: int | None,
        tmp_free_mb: int | None,
        process_count: int,
        running_worker_count: int,
        active_slot_count: int,
        ts: str | None = None,
    ) -> None:
        snapshot_ts = ts or _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO node_resource_snapshots (
                        run_id, host, allocated_mem_mb, total_mem_mb, used_mem_mb, free_mem_mb,
                        load_1, load_5, load_15, tmp_total_mb, tmp_used_mb, tmp_free_mb,
                        process_count, running_worker_count, active_slot_count, ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        host,
                        allocated_mem_mb,
                        total_mem_mb,
                        used_mem_mb,
                        free_mem_mb,
                        load_1,
                        load_5,
                        load_15,
                        tmp_total_mb,
                        tmp_used_mb,
                        tmp_free_mb,
                        process_count,
                        running_worker_count,
                        active_slot_count,
                        snapshot_ts,
                    ],
                )
            finally:
                conn.close()

    def list_latest_low_tmp_nodes(self, *, tmp_free_threshold_mb: int) -> list[tuple[str, int, str]]:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                rows = conn.execute(
                    """
                    WITH ranked AS (
                        SELECT
                            host,
                            tmp_free_mb,
                            ts,
                            ROW_NUMBER() OVER (PARTITION BY host ORDER BY ts DESC) AS row_no
                        FROM node_resource_snapshots
                        WHERE tmp_free_mb IS NOT NULL
                    )
                    SELECT host, tmp_free_mb, ts
                    FROM ranked
                    WHERE row_no = 1 AND tmp_free_mb < ?
                    ORDER BY host
                    """,
                    [tmp_free_threshold_mb],
                ).fetchall()
            finally:
                conn.close()
        return [(str(row[0]), int(row[1]), str(row[2])) for row in rows]

    def record_worker_resource_snapshot(
        self,
        *,
        run_id: str,
        worker_id: str,
        host: str,
        slurm_job_id: str | None,
        configured_slots: int,
        active_slots: int,
        idle_slots: int,
        rss_mb: int | None,
        cpu_pct: float | None,
        tunnel_state: str | None,
        process_count: int,
        ts: str | None = None,
    ) -> None:
        snapshot_ts = ts or _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO worker_resource_snapshots (
                        run_id, worker_id, host, slurm_job_id, configured_slots, active_slots,
                        idle_slots, rss_mb, cpu_pct, tunnel_state, process_count, ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        worker_id,
                        host,
                        slurm_job_id,
                        configured_slots,
                        active_slots,
                        idle_slots,
                        rss_mb,
                        cpu_pct,
                        tunnel_state,
                        process_count,
                        snapshot_ts,
                    ],
                )
            finally:
                conn.close()

    def record_slot_resource_snapshot(
        self,
        *,
        run_id: str,
        slot_id: str,
        worker_id: str | None,
        host: str | None,
        allocated_mem_mb: int | None,
        used_mem_mb: int | None,
        load_1: float | None,
        rss_mb: int | None,
        cpu_pct: float | None,
        process_count: int,
        active_process_count: int,
        artifact_bytes: int,
        progress_ts: str | None,
        state: str | None,
        ts: str | None = None,
    ) -> None:
        snapshot_ts = ts or _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO slot_resource_snapshots (
                        run_id, slot_id, worker_id, host, allocated_mem_mb, used_mem_mb, load_1,
                        rss_mb, cpu_pct, process_count, active_process_count, artifact_bytes, progress_ts, state, ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        slot_id,
                        worker_id,
                        host,
                        allocated_mem_mb,
                        used_mem_mb,
                        load_1,
                        rss_mb,
                        cpu_pct,
                        process_count,
                        active_process_count,
                        artifact_bytes,
                        progress_ts,
                        state,
                        snapshot_ts,
                    ],
                )
            finally:
                conn.close()

    def record_resource_summary_snapshot(
        self,
        *,
        run_id: str,
        host: str,
        allocated_mem_mb: int | None,
        used_mem_mb: int | None,
        free_mem_mb: int | None,
        load_1: float | None,
        running_worker_count: int,
        active_slot_count: int,
        stale: bool,
        ts: str | None = None,
    ) -> None:
        snapshot_ts = ts or _utc_now_iso()
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO resource_summary_snapshots (
                        run_id, host, allocated_mem_mb, used_mem_mb, free_mem_mb,
                        load_1, running_worker_count, active_slot_count, stale, ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        host,
                        allocated_mem_mb,
                        used_mem_mb,
                        free_mem_mb,
                        load_1,
                        running_worker_count,
                        active_slot_count,
                        stale,
                        snapshot_ts,
                    ],
                )
            finally:
                conn.close()

    def count_active_slots(self, *, run_id: str) -> int:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM slot_tasks
                    WHERE run_id = ? AND state IN ('ASSIGNED', 'LEASED', 'DOWNLOADING', 'UPLOADING', 'RUNNING', 'COLLECTING')
                    """,
                    [run_id],
                ).fetchone()
            finally:
                conn.close()
        return int(row[0] or 0)
