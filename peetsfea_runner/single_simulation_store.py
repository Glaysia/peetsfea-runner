from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import duckdb


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dataclass(slots=True)
class SingleSimulationResultStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path = Path(self.db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with duckdb.connect(str(self.db_path)) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS single_simulation_results (
                    request_id VARCHAR PRIMARY KEY,
                    account_id VARCHAR,
                    host_alias VARCHAR,
                    remote_job_id VARCHAR,
                    remote_api_session_id VARCHAR,
                    input_toml_hash VARCHAR,
                    peetsfea_version VARCHAR,
                    mode VARCHAR,
                    seed BIGINT,
                    design_id VARCHAR,
                    point_hash VARCHAR,
                    dimension_count BIGINT,
                    free_owner_paths_json VARCHAR,
                    point_values_json VARCHAR,
                    terminal_state VARCHAR,
                    started_at VARCHAR,
                    finished_at VARCHAR,
                    setup_pass_counts_json VARCHAR,
                    solve_telemetry_json VARCHAR,
                    csv_text_by_report_json VARCHAR,
                    csv_paths_json VARCHAR,
                    artifact_references_json VARCHAR,
                    error_stage VARCHAR,
                    error_type VARCHAR,
                    error_message VARCHAR,
                    result_json VARCHAR,
                    envelope_json VARCHAR
                )
                """
            )

    def record_envelope(self, envelope: Mapping[str, Any]) -> None:
        self.initialize()
        result = _mapping(envelope.get("result"))
        error = _mapping(envelope.get("error"))
        request_id = str(envelope.get("request_id") or "")
        if not request_id:
            raise ValueError("simulation envelope is missing request_id")
        row = {
            "request_id": request_id,
            "account_id": str(envelope.get("account_id") or ""),
            "host_alias": str(envelope.get("host_alias") or ""),
            "remote_job_id": str(envelope.get("remote_job_id") or ""),
            "remote_api_session_id": str(envelope.get("api_session_id") or ""),
            "input_toml_hash": str(envelope.get("input_toml_hash") or ""),
            "peetsfea_version": str(envelope.get("peetsfea_version") or ""),
            "mode": str(envelope.get("mode") or result.get("mode") or ""),
            "seed": int(envelope.get("seed") or result.get("seed") or 0),
            "design_id": str(result.get("design_id") or ""),
            "point_hash": str(result.get("point_hash") or ""),
            "dimension_count": int(result.get("dimension_count") or 0),
            "free_owner_paths_json": _json_dumps(result.get("free_owner_paths") or []),
            "point_values_json": _json_dumps(result.get("point_values") or {}),
            "terminal_state": str(envelope.get("terminal_state") or "failed"),
            "started_at": str(envelope.get("started_at") or ""),
            "finished_at": str(envelope.get("finished_at") or ""),
            "setup_pass_counts_json": _json_dumps(result.get("setup_pass_counts") or {}),
            "solve_telemetry_json": _json_dumps(result.get("solve_telemetry") or {}),
            "csv_text_by_report_json": _json_dumps(result.get("csv_text_by_report") or {}),
            "csv_paths_json": _json_dumps(result.get("csv_paths") or {}),
            "artifact_references_json": _json_dumps(result.get("artifact_references") or {}),
            "error_stage": str(error.get("stage") or ""),
            "error_type": str(error.get("type") or ""),
            "error_message": str(error.get("message") or ""),
            "result_json": _json_dumps(result),
            "envelope_json": _json_dumps(envelope),
        }
        columns = tuple(row)
        placeholders = ", ".join("?" for _ in columns)
        with duckdb.connect(str(self.db_path)) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("DELETE FROM single_simulation_results WHERE request_id = ?", [request_id])
                connection.execute(
                    f"INSERT INTO single_simulation_results ({', '.join(columns)}) VALUES ({placeholders})",
                    [row[column] for column in columns],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def fetch_result(self, request_id: str) -> dict[str, Any] | None:
        self.initialize()
        with duckdb.connect(str(self.db_path)) as connection:
            result = connection.execute(
                "SELECT * FROM single_simulation_results WHERE request_id = ?",
                [request_id],
            )
            row = result.fetchone()
            if row is None:
                return None
            columns = [column[0] for column in result.description]
            return dict(zip(columns, row, strict=True))


__all__ = ["SingleSimulationResultStore"]
