from __future__ import annotations

import threading
import urllib.request
from pathlib import Path
from typing import Any

from peetsfea_runner.edt_dashboard import rows_to_csv, start_dashboard_server
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
                "solve_telemetry": {"gpu_used": True, "solver_cores": 4, "solve_seconds": 812.5},
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
