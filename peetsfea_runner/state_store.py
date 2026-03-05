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
                        attempt INTEGER NOT NULL,
                        aedt_path TEXT NOT NULL,
                        session_name TEXT,
                        state TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        exit_code INTEGER,
                        failure_reason TEXT,
                        PRIMARY KEY (run_id, job_id)
                    )
                    """
                )
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
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    "INSERT INTO runs (run_id, started_at, state) VALUES (?, ?, ?)",
                    [run_id, _utc_now_iso(), "RUNNING"],
                )
            finally:
                conn.close()

    def finish_run(self, run_id: str, *, state: str, summary: str) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE runs
                    SET finished_at = ?, state = ?, summary = ?
                    WHERE run_id = ?
                    """,
                    [_utc_now_iso(), state, summary, run_id],
                )
            finally:
                conn.close()

    def create_job(self, *, run_id: str, job_id: str, aedt_path: str) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        run_id, job_id, attempt, aedt_path, state
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, 0, aedt_path, "PENDING"],
                )
            finally:
                conn.close()

    def update_job(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt: int,
        state: str,
        session_name: str | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        exit_code: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    UPDATE jobs
                    SET
                        attempt = ?,
                        session_name = ?,
                        state = ?,
                        started_at = ?,
                        finished_at = ?,
                        exit_code = ?,
                        failure_reason = ?
                    WHERE run_id = ? AND job_id = ?
                    """,
                    [
                        attempt,
                        session_name,
                        state,
                        started_at,
                        finished_at,
                        exit_code,
                        failure_reason,
                        run_id,
                        job_id,
                    ],
                )
            finally:
                conn.close()

    def append_job_event(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt: int,
        event_type: str,
        details: str = "",
    ) -> None:
        with self._lock:
            conn = duckdb.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO job_events (run_id, job_id, attempt, event_time, event_type, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [run_id, job_id, attempt, _utc_now_iso(), event_type, details],
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
