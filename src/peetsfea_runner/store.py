from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import duckdb

from peetsfea_runner.state import JobState


class JobStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(self._db_path))
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _table_exists(self, table_name: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        return row is not None

    def _table_columns(self, table_name: str) -> set[str]:
        if not self._table_exists(table_name):
            return set()
        rows = self.connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]) for row in rows}

    def _ensure_jobs_table(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                task_id TEXT,
                filename TEXT NOT NULL,
                source_path TEXT NOT NULL,
                pending_path TEXT,
                uploaded_path TEXT,
                remote_account_id TEXT,
                remote_inbox_path TEXT,
                state TEXT NOT NULL,
                aedt_retention TEXT NOT NULL DEFAULT 'delete_after_done',
                local_aedt_deleted_ts TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                error_code TEXT,
                error_message TEXT
            )
            """
        )

        columns = self._table_columns("jobs")
        if "task_id" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN task_id TEXT")
            columns.add("task_id")

        if "pending_path" not in columns:
            if "staging_path" in columns:
                self.connection.execute("ALTER TABLE jobs RENAME COLUMN staging_path TO pending_path")
            else:
                self.connection.execute("ALTER TABLE jobs ADD COLUMN pending_path TEXT")
            columns.add("pending_path")

        if "uploaded_path" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN uploaded_path TEXT")
            columns.add("uploaded_path")

        if "remote_account_id" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN remote_account_id TEXT")
            columns.add("remote_account_id")

        if "remote_inbox_path" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN remote_inbox_path TEXT")
            columns.add("remote_inbox_path")

        if "aedt_retention" not in columns:
            self.connection.execute(
                "ALTER TABLE jobs ADD COLUMN aedt_retention TEXT DEFAULT 'delete_after_done'"
            )
            columns.add("aedt_retention")

        if "local_aedt_deleted_ts" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN local_aedt_deleted_ts TIMESTAMP")
            columns.add("local_aedt_deleted_ts")

        if "error_code" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN error_code TEXT")
            columns.add("error_code")

        if "error_message" not in columns:
            self.connection.execute("ALTER TABLE jobs ADD COLUMN error_message TEXT")
            columns.add("error_message")

        # Backfill task_id for old rows that predate task_id column.
        self.connection.execute(
            """
            UPDATE jobs
            SET task_id = filename
            WHERE task_id IS NULL
            """
        )

        self.connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_task_id ON jobs(task_id)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")

    def _ensure_events_table(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS job_events (
                event_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                error_code TEXT,
                message TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_events_task_created ON job_events(task_id, created_at)"
        )

    def initialize_schema(self) -> None:
        self._ensure_jobs_table()
        self._ensure_events_table()

    def task_exists(self, task_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM jobs WHERE task_id = ? LIMIT 1",
            [task_id],
        ).fetchone()
        return row is not None

    def insert_job(
        self,
        *,
        task_id: str,
        filename: str,
        source_path: str,
        pending_path: str,
        state: JobState,
        uploaded_path: str | None = None,
        remote_account_id: str | None = None,
        remote_inbox_path: str | None = None,
        aedt_retention: str = "delete_after_done",
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> bool:
        try:
            self.connection.execute(
                """
                INSERT INTO jobs(
                    task_id,
                    filename,
                    source_path,
                    pending_path,
                    uploaded_path,
                    remote_account_id,
                    remote_inbox_path,
                    state,
                    aedt_retention,
                    error_code,
                    error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    filename,
                    source_path,
                    pending_path,
                    uploaded_path,
                    remote_account_id,
                    remote_inbox_path,
                    state.value,
                    aedt_retention,
                    error_code,
                    error_message,
                ],
            )
        except duckdb.ConstraintException:
            return False
        return True

    def update_state_by_task_id(
        self,
        *,
        task_id: str,
        state: JobState,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET state = ?, error_code = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?
            """,
            [state.value, error_code, error_message, task_id],
        )

    def mark_local_aedt_deleted(self, *, task_id: str) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET local_aedt_deleted_ts = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?
            """,
            [task_id],
        )

    def record_event(
        self,
        *,
        task_id: str,
        event_type: str,
        error_code: str | None = None,
        message: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO job_events(event_id, task_id, event_type, error_code, message)
            VALUES (?, ?, ?, ?, ?)
            """,
            [str(uuid4()), task_id, event_type, error_code, message],
        )

    def get_job_state(self, task_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT state FROM jobs WHERE task_id = ?",
            [task_id],
        ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def get_job_row(self, task_id: str) -> tuple[str, str, str, str, str] | None:
        row = self.connection.execute(
            """
            SELECT task_id, filename, source_path, pending_path, state
            FROM jobs
            WHERE task_id = ?
            """,
            [task_id],
        ).fetchone()
        if row is None:
            return None
        return (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))

    def get_job_error(self, task_id: str) -> tuple[str | None, str | None] | None:
        row = self.connection.execute(
            """
            SELECT error_code, error_message
            FROM jobs
            WHERE task_id = ?
            """,
            [task_id],
        ).fetchone()
        if row is None:
            return None
        return (row[0], row[1])

    def list_pending_upload_jobs(self) -> list[tuple[str, str, str, str, str, str]]:
        rows = self.connection.execute(
            """
            SELECT task_id, filename, pending_path, uploaded_path, remote_account_id, remote_inbox_path
            FROM jobs
            WHERE state = ?
            ORDER BY created_at, task_id
            """,
            [JobState.PENDING.value],
        ).fetchall()
        return [
            (
                str(row[0]),
                str(row[1]),
                str(row[2]) if row[2] is not None else "",
                str(row[3]) if row[3] is not None else "",
                str(row[4]) if row[4] is not None else "",
                str(row[5]) if row[5] is not None else "",
            )
            for row in rows
        ]

    def mark_uploaded(
        self,
        *,
        task_id: str,
        uploaded_path: str,
        remote_account_id: str,
        remote_inbox_path: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET state = ?,
                uploaded_path = ?,
                remote_account_id = ?,
                remote_inbox_path = ?,
                error_code = NULL,
                error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?
            """,
            [JobState.UPLOADED.value, uploaded_path, remote_account_id, remote_inbox_path, task_id],
        )

    def list_jobs_by_state(self, state: JobState) -> list[tuple[str, str]]:
        rows = self.connection.execute(
            """
            SELECT task_id, pending_path
            FROM jobs
            WHERE state = ?
            """,
            [state.value],
        ).fetchall()
        return [(str(row[0]), str(row[1]) if row[1] is not None else "") for row in rows]

    def refresh_job_paths(
        self,
        *,
        task_id: str,
        filename: str,
        source_path: str,
        pending_path: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET filename = ?,
                source_path = ?,
                pending_path = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?
            """,
            [filename, source_path, pending_path, task_id],
        )

    def get_task_events(self, task_id: str) -> list[tuple[str, str | None, str | None]]:
        rows = self.connection.execute(
            """
            SELECT event_type, error_code, message
            FROM job_events
            WHERE task_id = ?
            ORDER BY created_at, event_id
            """,
            [task_id],
        ).fetchall()
        return [(str(row[0]), row[1], row[2]) for row in rows]

    def can_promote_done(
        self,
        *,
        task_id: str,
        required_events: tuple[str, ...] = ("AEDT_DELETE_LOCAL_DONE",),
    ) -> bool:
        if not required_events:
            return True
        placeholders = ",".join("?" for _ in required_events)
        row = self.connection.execute(
            f"""
            SELECT COUNT(DISTINCT event_type)
            FROM job_events
            WHERE task_id = ?
              AND event_type IN ({placeholders})
            """,
            [task_id, *required_events],
        ).fetchone()
        matched = int(row[0]) if row and row[0] is not None else 0
        return matched == len(set(required_events))
