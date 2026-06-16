from __future__ import annotations

import threading
import urllib.request
from pathlib import Path
from typing import Any

from peetsfea_runner.edt_dashboard import (
    build_failures,
    build_sim_detail,
    build_summary,
    rows_to_csv,
    start_dashboard_server,
)
from peetsfea_runner.single_simulation_store import SingleSimulationResultStore


def _seed(store: SingleSimulationResultStore, rid: str, k_in: float, passes: int) -> None:
    store.record_envelope(
        {
            "request_id": rid,
            "terminal_state": "success",
            "started_at": "2026-06-16T00:00:00",
            "finished_at": "2026-06-16T00:10:00",
            "account_id": "account_01",
            "seed": 1,
            "mode": "full",
            "peetsfea_version": "0.3.4",
            "result": {
                "design_id": f"d_{rid}",
                "point_values": {"coil_w": k_in, "ferrite": 1},
                "setup_pass_counts": {"Setup1": passes},
                "solve_telemetry": {"gpu_used": True, "solver_cores": 4, "solve_seconds": 812.5, "elapsed_ms": 600000},
                "csv_text_by_report": {"S11": f"freq,db\n1e9,-12.3\n2e9,-15.1  # {rid}\n"},
            },
        }
    )


def test_fetch_rows_and_csv_flattens_inputs(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "r0", 1.5, 12)
    _seed(store, "r1", 2.5, 20)

    rows = store.fetch_rows()
    assert len(rows) == 2
    csv_text = rows_to_csv(rows)
    header = csv_text.splitlines()[0]
    assert "request_id" in header and "in_coil_w" in header and "in_ferrite" in header and "pass_Setup1" in header
    # 값이 행에 들어갔는지
    assert "1.5" in csv_text and "2.5" in csv_text and "r0" in csv_text


def test_csv_includes_full_output_dataset(tmp_path: Path) -> None:
    # 요구사항: 무거운 .aedt만 빼고 입출력 데이터셋 전부 포함(telemetry + 리포트 출력 데이터).
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "r0", 1.5, 12)

    csv_text = rows_to_csv(store.fetch_rows())
    header = csv_text.splitlines()[0]
    # 출력 telemetry 컬럼(gpu/solver_cores/시간) — 자동 GPU 벤치마크의 원천.
    assert "tel_gpu_used" in header and "tel_solver_cores" in header and "tel_solve_seconds" in header
    # 출력 리포트 데이터셋(csv_text_by_report)이 손실 없이 임베드.
    assert "reports_json" in header
    assert "S11" in csv_text and "-12.3" in csv_text  # 실제 리포트 출력값이 CSV에 있음
    assert "4" in csv_text and "812.5" in csv_text  # solver_cores, solve_seconds


def test_fetch_rows_filter_terminal_state(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "ok", 1.0, 10)
    store.record_envelope({"request_id": "bad", "terminal_state": "failed", "result": {}})
    assert len(store.fetch_rows(terminal_state="success")) == 1
    assert len(store.fetch_rows(terminal_state="failed")) == 1
    assert len(store.fetch_rows(limit=1)) == 1


def test_dashboard_server_serves_csv_and_health(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "r0", 3.0, 15)

    server = start_dashboard_server(store=store, host="127.0.0.1", port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as resp:
            assert resp.status == 200
            assert b'"count"' in resp.read()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/results.csv", timeout=5) as resp:
            body = resp.read().decode("utf-8")
            assert resp.headers["Content-Type"] == "text/csv"
            assert "in_coil_w" in body and "r0" in body
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as resp:
            assert b"results.csv" in resp.read()
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _seed_failed(store: SingleSimulationResultStore, rid: str, etype: str) -> None:
    store.record_envelope({
        "request_id": rid, "terminal_state": "failed", "partition": "gpu1", "node": "n003",
        "started_at": "2026-06-16T00:00:00", "finished_at": "2026-06-16T00:00:05",
        "error": {"stage": "attach", "type": etype, "message": f"boom {rid}"},
        "result": {},
    })


def test_build_summary_counts_and_metrics(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "s0", 1.5, 12)
    _seed(store, "s1", 2.5, 20)
    _seed_failed(store, "f0", "GrpcApiError")
    s = build_summary(store)
    assert s["total"] == 3 and s["success"] == 2 and s["failed"] == 1
    assert s["success_rate"] == round(2 / 3 * 100, 1)
    assert s["gpu_used_pct"] == 100.0  # _seed telemetry has gpu_used=True
    assert s["avg_solve_min"] is not None
    assert "solve_min_hist" in s


def test_build_sim_detail_parses_report_curves(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "s0", 1.5, 12)  # csv_text_by_report has S11: freq,db rows
    d = build_sim_detail(store, "s0")
    assert d is not None
    assert d["inputs"]["coil_w"] == 1.5
    assert d["telemetry"]["solver_cores"] == 4
    assert "S11" in d["reports"]
    rep = d["reports"]["S11"]
    assert rep["columns"][0] == "freq" and len(rep["rows"]) >= 1
    assert isinstance(rep["rows"][0][0], float)  # 숫자로 파싱
    assert build_sim_detail(store, "nope") is None


def test_build_failures_groups_by_type(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed_failed(store, "f0", "GrpcApiError")
    _seed_failed(store, "f1", "GrpcApiError")
    _seed_failed(store, "f2", "AssertionError")
    f = build_failures(store)
    assert f["by_type"]["GrpcApiError"] == 2 and f["by_type"]["AssertionError"] == 1
    assert len(f["recent"]) == 3


def test_dashboard_api_endpoints_and_resources(tmp_path: Path) -> None:
    import json
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "s0", 1.5, 12)
    snap = {"ok": True, "jobs": [{"id": "1", "state": "RUNNING", "node": "n003", "partition": "gpu1"}],
            "nodes": {"n003": {"cpuload": 4.0, "cputot": 48, "memfree_mb": 1, "memtotal_mb": 2}},
            "license": {"mine": 5}, "counts": {"running": 1, "pending": 0}}
    server = start_dashboard_server(store=store, port=0, resource_provider=lambda: snap)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        page = urllib.request.urlopen(base + "/", timeout=3).read().decode()
        assert "운영 대시보드" in page and "컨테이너 부하" in page
        summ = json.load(urllib.request.urlopen(base + "/api/summary", timeout=3))
        assert summ["success"] == 1
        res = json.load(urllib.request.urlopen(base + "/api/resources", timeout=3))
        assert res["counts"]["running"] == 1 and res["nodes"]["n003"]["cpuload"] == 4.0
        results = json.load(urllib.request.urlopen(base + "/api/results?state=success", timeout=3))
        assert results["rows"][0]["request_id"] == "s0" and "in_coil_w" in results["rows"][0]
    finally:
        server.shutdown()
        t.join(timeout=5)
