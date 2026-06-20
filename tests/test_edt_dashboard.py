from __future__ import annotations

import threading
import urllib.request
from pathlib import Path
from typing import Any

import duckdb

from peetsfea_runner.edt_dashboard import (
    build_failures,
    build_sim_detail,
    build_summary,
    start_dashboard_server,
)
from peetsfea_runner.edt_toml_registry import TomlRegistryService
from peetsfea_runner.single_simulation_store import DbTomlRegistry, SingleSimulationResultStore


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


def _toml_registry(store: SingleSimulationResultStore) -> TomlRegistryService:
    service = TomlRegistryService(
        registry=DbTomlRegistry(store=store),
        builtin_toml_text="spec_version = 'builtin'\n",
        clock=lambda: 10.0,
    )
    service.initialize()
    return service


def test_export_parquet_filters_and_excludes(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "r0", 1.5, 12)
    _seed(store, "r1", 2.5, 20)

    out = tmp_path / "export.parquet"
    store.export_parquet(out, peetsfea_version="0.3.4")
    con = duckdb.connect()
    cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{out}'").fetchall()]
    assert con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0] == 2
    # 무거운 중복 컬럼 제외, 데이터 JSON 컬럼은 포함(손실 없이; 입력·telemetry·리포트).
    assert "envelope_json" not in cols and "result_json" not in cols
    for c in ("request_id", "peetsfea_version", "point_values_json", "solve_telemetry_json", "csv_text_by_report_json"):
        assert c in cols
    pv = con.execute(f"SELECT point_values_json FROM '{out}' ORDER BY request_id").fetchall()
    assert "coil_w" in pv[0][0] and "1.5" in pv[0][0]  # 입력값 보존
    # state 필터
    store.record_envelope({"request_id": "bad", "terminal_state": "failed", "peetsfea_version": "0.3.4", "result": {}})
    out2 = tmp_path / "succ.parquet"
    store.export_parquet(out2, peetsfea_version="0.3.4", terminal_state="success")
    assert con.execute(f"SELECT count(*) FROM '{out2}'").fetchone()[0] == 2  # success만


def test_results_parquet_endpoint_removed(tmp_path: Path) -> None:
    # /results.parquet 전체 덤프 엔드포인트는 제거됨(데이터플레인 갈아엎기) → 404.
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "r0", 1.5, 12)

    server = start_dashboard_server(store=store, host="127.0.0.1", port=0)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        import urllib.error

        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/results.parquet", timeout=5)
            raise AssertionError("expected 404")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
    finally:
        server.shutdown()


def test_fetch_rows_filter_terminal_state(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "ok", 1.0, 10)
    store.record_envelope({"request_id": "bad", "terminal_state": "failed", "result": {}})
    assert len(store.fetch_rows(terminal_state="success")) == 1
    assert len(store.fetch_rows(terminal_state="failed")) == 1
    assert len(store.fetch_rows(limit=1)) == 1


def test_dashboard_server_serves_health(tmp_path: Path) -> None:
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
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as resp:
            assert resp.status == 200  # 대시보드 UI(전체 덤프 링크 제거됨)
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


def test_dashboard_toml_cards_and_api_proxy(tmp_path: Path) -> None:
    import json

    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    registry = _toml_registry(store)
    server = start_dashboard_server(store=store, port=0, toml_registry=registry)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        page = urllib.request.urlopen(base + "/", timeout=3).read().decode()
        assert 'data-t="toml"' in page
        assert "TOML registry" in page
        assert "입력큐" not in page

        listed = json.load(urllib.request.urlopen(base + "/api/tomls", timeout=3))
        assert listed["active_count"] == 1
        req = urllib.request.Request(
            base + "/api/tomls/custom",
            data=json.dumps({"name": "narrow", "toml_text": "spec_version = 'narrow'\n"}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        created = json.load(urllib.request.urlopen(req, timeout=3))
        custom_id = created["toml"]["id"]
        req = urllib.request.Request(
            base + "/api/tomls/ratios",
            data=json.dumps({"ratios": {"builtin-widest": 60, custom_id: 40}}).encode(),
            method="PUT",
            headers={"Content-Type": "application/json"},
        )
        json.load(urllib.request.urlopen(req, timeout=3))
        listed = json.load(urllib.request.urlopen(base + "/api/tomls", timeout=3))
        assert listed["active_count"] == 2 and listed["ratios_set"] is True
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_dashboard_timeseries_and_history_endpoints(tmp_path: Path) -> None:
    import json
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    _seed(store, "s0", 1.5, 12)  # finished_at 2026-06-16T00:10:00
    hist = [{"ts": 1.0, "running": 8, "pending": 1, "lic_mine": 80, "lic_inuse": 120,
             "load": 30.0, "cpus": 320, "mem_used_mb": 1000, "mem_total_mb": 4000}]
    server = start_dashboard_server(store=store, port=0, history_provider=lambda since_ts=None: hist)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        page = urllib.request.urlopen(base + "/", timeout=3).read().decode()
        assert "추세" in page  # 시계열 탭 존재
        ts = json.load(urllib.request.urlopen(base + "/api/timeseries?bucket=30", timeout=3))
        assert ts["bucket_minutes"] == 30 and ts["points"][0]["success"] == 1
        h = json.load(urllib.request.urlopen(base + "/api/resources/history", timeout=3))
        assert h["points"][0]["running"] == 8 and h["points"][0]["lic_mine"] == 80
    finally:
        server.shutdown()
        t.join(timeout=5)
