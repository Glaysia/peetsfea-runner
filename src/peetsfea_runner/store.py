from __future__ import annotations

from pathlib import Path

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

    def initialize_schema(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL UNIQUE,
                source_path TEXT NOT NULL,
                staging_path TEXT,
                state TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT
            )
            """
        )

    def filename_exists(self, filename: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM jobs WHERE filename = ? LIMIT 1",
            [filename],
        ).fetchone()
        return row is not None

    def insert_job(
        self,
        *,
        job_id: str,
        filename: str,
        source_path: str,
        staging_path: str,
        state: JobState,
        error_message: str | None = None,
    ) -> bool:
        try:
            self.connection.execute(
                """
                INSERT INTO jobs(job_id, filename, source_path, staging_path, state, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [job_id, filename, source_path, staging_path, state.value, error_message],
            )
        except duckdb.ConstraintException:
            return False
        return True

    def update_state_by_job_id(
        self,
        *,
        job_id: str,
        state: JobState,
        error_message: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET state = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
            """,
            [state.value, error_message, job_id],
        )

    def update_state_by_filename(
        self,
        *,
        filename: str,
        state: JobState,
        error_message: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            UPDATE jobs
            SET state = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE filename = ?
            """,
            [state.value, error_message, filename],
        )

    def get_job_state(self, filename: str) -> str | None:
        row = self.connection.execute(
            "SELECT state FROM jobs WHERE filename = ?",
            [filename],
        ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def get_job_row(self, filename: str) -> tuple[str, str, str, str, str] | None:
        row = self.connection.execute(
            """
            SELECT job_id, filename, source_path, staging_path, state
            FROM jobs
            WHERE filename = ?
            """,
            [filename],
        ).fetchone()
        if row is None:
            return None
        return (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
