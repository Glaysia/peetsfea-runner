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


def test_keep_best_success_not_overwritten_by_failure(tmp_path: Path) -> None:
    # 고질병 수정: 잡 재시작이 같은 seed(request_id)를 재탐색하다 실패해도 누적 성공이 유실되면 안 된다.
    store = SingleSimulationResultStore(tmp_path / "results.duckdb")
    store.record_envelope({
        "request_id": "base-7", "terminal_state": "success",
        "result": {"design_id": "d7", "point_values": {"a": 1}},
    })
    # 같은 id로 실패가 들어와도 성공 행을 덮어쓰지 않는다.
    store.record_envelope({
        "request_id": "base-7", "terminal_state": "failed",
        "error": {"stage": "simulate", "type": "GrpcApiError", "message": "boom"}, "result": {},
    })
    row = store.fetch_result("base-7")
    assert row is not None and row["terminal_state"] == "success" and row["design_id"] == "d7"
    # 단, 성공이 새 성공으로는 갱신된다(최신 데이터 반영).
    store.record_envelope({
        "request_id": "base-7", "terminal_state": "success",
        "result": {"design_id": "d7b", "point_values": {"a": 2}},
    })
    assert store.fetch_result("base-7")["design_id"] == "d7b"  # type: ignore[index]


def test_timeseries_buckets_success_fail_gpu(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(tmp_path / "results.duckdb")
    def rec(rid: str, fin: str, state: str, gpu: bool) -> None:
        store.record_envelope({
            "request_id": rid, "terminal_state": state, "finished_at": fin,
            "result": {"design_id": rid, "solve_telemetry": {"gpu_used": gpu, "elapsed_ms": 600000}},
        })
    # 두 개의 15분 버킷
    rec("a", "2026-06-16T10:02:00+00:00", "success", True)
    rec("b", "2026-06-16T10:09:00+00:00", "success", False)
    rec("c", "2026-06-16T10:11:00+00:00", "failed", False)
    rec("d", "2026-06-16T10:20:00+00:00", "success", True)
    ts = store.timeseries(bucket_minutes=15)
    assert len(ts) == 2
    b0, b1 = ts
    assert b0["success"] == 2 and b0["failed"] == 1 and b0["gpu"] == 1 and b0["total"] == 3
    assert b1["success"] == 1 and b1["gpu"] == 1
    # since 필터
    assert len(store.timeseries(bucket_minutes=15, since="2026-06-16T10:15:00+00:00")) == 1
