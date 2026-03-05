from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import duckdb


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(slots=True)
class StateStore:
    db_path: Path
    _lock: Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.db_path = self.db_path.expanduser().resolve()
        self._lock = Lock()
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
                        input_path TEXT NOT NULL,
                        input_deleted_at TEXT,
                        delete_retry_count INTEGER NOT NULL DEFAULT 0,
                        delete_final_state TEXT NOT NULL DEFAULT 'PENDING',
                        quarantine_path TEXT,
                        updated_at TEXT NOT NULL
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
                    INSERT INTO file_lifecycle (run_id, job_id, input_path, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [run_id, job_id, input_path, now],
                )
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
