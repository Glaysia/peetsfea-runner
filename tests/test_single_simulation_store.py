from __future__ import annotations

import json
from pathlib import Path

from peetsfea_runner.single_simulation_store import SingleSimulationResultStore


def test_single_simulation_result_store_records_peetsfea_result_envelope(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(tmp_path / "results.duckdb")
    envelope = {
        "request_id": "req-001",
        "account_id": "account_01",
        "host_alias": "gate1-harry261",
        "remote_job_id": "12345",
        "api_session_id": "session-01",
        "input_toml_hash": "abc",
        "peetsfea_version": "0.3.1",
        "mode": "semi_dry",
        "seed": 7,
        "terminal_state": "success",
        "started_at": "2026-06-14T00:00:00+00:00",
        "finished_at": "2026-06-14T00:01:00+00:00",
        "result": {
            "design_id": "ssw_abc",
            "point_hash": "hash-001",
            "dimension_count": 2,
            "free_owner_paths": ["a", "b"],
            "point_values": {"a": 1, "b": 2.5},
            "setup_pass_counts": {"maximum_passes": 5},
            "solve_telemetry": {"sample_count": 3},
            "csv_text_by_report": {"Results1_Pass": "x,y\n1,2\n"},
            "csv_paths": {"Results1_Pass": "/remote/Results1_Pass.csv"},
        },
    }

    store.record_envelope(envelope)

    row = store.fetch_result("req-001")
    assert row is not None
    assert row["request_id"] == "req-001"
    assert row["host_alias"] == "gate1-harry261"
    assert row["peetsfea_version"] == "0.3.1"
    assert row["terminal_state"] == "success"
    assert row["design_id"] == "ssw_abc"
    assert row["point_hash"] == "hash-001"
    assert json.loads(row["point_values_json"]) == {"a": 1, "b": 2.5}
    assert json.loads(row["csv_text_by_report_json"]) == {"Results1_Pass": "x,y\n1,2\n"}


def test_single_simulation_result_store_records_structured_failure(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(tmp_path / "results.duckdb")
    envelope = {
        "request_id": "req-failed",
        "account_id": "account_01",
        "host_alias": "gate1-harry261",
        "terminal_state": "failed",
        "error": {"stage": "simulate", "type": "RuntimeError", "message": "boom"},
        "result": {},
    }

    store.record_envelope(envelope)

    row = store.fetch_result("req-failed")
    assert row is not None
    assert row["terminal_state"] == "failed"
    assert row["error_stage"] == "simulate"
    assert row["error_type"] == "RuntimeError"
    assert row["error_message"] == "boom"
