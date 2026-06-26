"""데이터플레인 갈아엎기 검증: seq 증분 커서 + read API(Arrow IPC 스트림).

seq 트리거는 Postgres 전용이라, 로컬 PG(:5433)가 닿을 때만 실행한다(없으면 skip).
임시 DB를 만들어 격리 테스트 후 삭제한다.
"""

from __future__ import annotations

import os
import uuid

import pyarrow as pa
import pytest

psycopg = pytest.importorskip("psycopg")

_ADMIN_DSN = os.environ.get("EDT_PG_TEST_ADMIN_DSN", "host=127.0.0.1 port=5433 dbname=peetsfea user=peets")


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(_ADMIN_DSN, autocommit=True, connect_timeout=3):
            return True
    except Exception:  # noqa: BLE001
        return False


pytestmark = pytest.mark.skipif(not _pg_reachable(), reason="local Postgres(:5433) 없음")


@pytest.fixture()
def pg_store():  # type: ignore[no-untyped-def]
    from peetsfea_runner.postgres_store import PostgresResultStore

    name = f"peetsfea_pytest_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(_ADMIN_DSN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    dsn = " ".join(p if not p.startswith("dbname=") else f"dbname={name}" for p in _ADMIN_DSN.split())
    store = PostgresResultStore(dsn=dsn)
    store.initialize()
    try:
        yield store
    finally:
        # 전용 연결 캐시 정리 후 DROP.
        from peetsfea_runner import postgres_store as ps

        conn = ps._PG_CONNS.pop(dsn, None)
        if conn is not None:
            conn.close()
        with psycopg.connect(_ADMIN_DSN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def _rec(store, rid: str, state: str = "success") -> None:  # type: ignore[no-untyped-def]
    store.record_envelope(
        {"request_id": rid, "terminal_state": state, "finished_at": "2026-06-20T10:00",
         "result": {"point_values": {"x": 1}, "design_id": rid}, "error": {}}
    )


def _seq(store, rid: str):  # type: ignore[no-untyped-def]
    with store._locked_connect() as c:
        row = c.execute("SELECT seq FROM single_simulation_results WHERE request_id=%s", [rid]).fetchone()
        return row[0] if row else None


def test_seq_bumps_on_insert_and_update_not_on_noop(pg_store) -> None:  # type: ignore[no-untyped-def]
    _rec(pg_store, "r1", "success")
    _rec(pg_store, "r2", "failed")
    s1, s2 = _seq(pg_store, "r1"), _seq(pg_store, "r2")
    assert s1 is not None and s2 is not None and s2 > s1  # insert마다 부여

    _rec(pg_store, "r1", "success")  # update(success→success, WHERE 통과) → bump
    s1b = _seq(pg_store, "r1")
    assert s1b > s1

    _rec(pg_store, "r1", "failed")  # no-op(failed over success, WHERE false) → 불변
    assert _seq(pg_store, "r1") == s1b


def test_stream_results_since_incremental(pg_store) -> None:  # type: ignore[no-untyped-def]
    for i in range(5):
        _rec(pg_store, f"d-{i}")
    all_rows = list(pg_store.stream_results_since(since=0))
    assert len(all_rows) == 5
    seqs = [r["seq"] for r in all_rows]
    assert seqs == sorted(seqs)  # seq 오름차순
    last = max(seqs)

    _rec(pg_store, "d-0")  # 갱신
    _rec(pg_store, "d-new")  # 신규
    delta = list(pg_store.stream_results_since(since=last))
    assert {r["request_id"] for r in delta} == {"d-0", "d-new"}  # 변경분만


def test_data_api_arrow_roundtrip_and_incremental(pg_store) -> None:  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from peetsfea_runner.edt_data_api import create_data_api_app

    for i in range(3000):  # 배치 경계(2000) 넘김
        _rec(pg_store, f"x-{i:04d}")
    client = TestClient(create_data_api_app(store=pg_store))

    r = client.get("/api/results?since=0&limit=100000")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.apache.arrow.stream"
    tbl = pa.ipc.open_stream(r.content).read_all()
    assert tbl.num_rows == 3000
    assert "solve_telemetry_json" not in tbl.column_names  # lean 기본
    seqs = tbl.column("seq").to_pylist()
    assert seqs == sorted(seqs)
    last = max(seqs)

    _rec(pg_store, "x-0000")  # 갱신
    _rec(pg_store, "x-9999")  # 신규
    r2 = client.get(f"/api/results?since={last}&limit=100000")
    t2 = pa.ipc.open_stream(r2.content).read_all()
    assert sorted(t2.column("request_id").to_pylist()) == ["x-0000", "x-9999"]  # 변경분 2행만

    rf = client.get("/api/results?since=0&limit=3&full=1")
    tf = pa.ipc.open_stream(rf.content).read_all()
    assert "solve_telemetry_json" in tf.column_names and "envelope_json" in tf.column_names  # full=1


# ---------------------------------------------------------------- v2 데이터플레인 계약


def _mk_csv(header, rows):  # type: ignore[no-untyped-def]
    import csv
    import io

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([f'"{h}"' for h in header])  # 실데이터처럼 따옴표 포함(파서가 strip)
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def _rec_full(store, rid, *, state="success", n_sweep=5):  # type: ignore[no-untyped-def]
    """실측 리포트 구조(Results1_Pass/3_Freq/2_Last)를 갖춘 success 엔벌롭."""
    z = ["re_Z11", "im_Z11", "re_Z22", "im_Z22", "re_Z12", "im_Z12"]
    p_header = ["Pass", "Ltx", "Lrx", "M", "k", "Qtx", "Qrx", "FOM", *z]
    p_rows = [[1, 0, 0, 0, 0, 0, 0, 0, 9, -19, 2.9, -3.9, 0.4, 1.1],
              [2, 0, 0, 0, 0, 0, 0, 0, 10.0, -20.0, 3.0, -4.0, 0.5, 1.2]]  # 마지막행=동작점
    f_header = ["Freq_kHz", "Ltx", "Lrx", "M", "k", "Qtx", "Qrx", "FOM", *z]
    f_rows = [[100.0 * (i + 1), 0, 0, 0, 0, 0, 0, 0, 1.0 + i, 14.0 + i, 4.0, 36.0, 0.1, 0.5] for i in range(n_sweep)]
    l_header = ["Freq_MHz", "loss_W_tx_coil_ssw_copper", "loss_W_tx_pcb_1_fr4", "loss_W_tx_mull_ferrite_sheet",
                "loss_W_rx_coil_ssw_copper", "loss_W_rx_pcb_1_fr4", "loss_W_Region_Abs_2000mm"]
    l_rows = [[6.78, 0.01, 0.02, 0.03, 0.04, 0.05, 99.0]]
    reports = {"Results1_Pass": _mk_csv(p_header, p_rows),
               "Results3_Freq": _mk_csv(f_header, f_rows),
               "Results2_Last": _mk_csv(l_header, l_rows)}
    store.record_envelope({
        "request_id": rid, "terminal_state": state, "peetsfea_version": "0.3.9.5", "node": "n100",
        "started_at": "2026-06-25T10:00:00+00:00", "finished_at": "2026-06-25T10:15:00+00:00",
        "result": {"design_id": rid, "point_hash": f"ph-{rid}", "point_values": {"a": 1.5, "b": 2.0},
                   "csv_text_by_report": reports, "solve_telemetry": {"elapsed_ms": 900000.0, "gpu_used": True}},
        "error": {},
    })


def test_dataplane_extraction_columns_and_sweep(pg_store) -> None:  # type: ignore[no-untyped-def]
    _rec_full(pg_store, "d-1")
    with pg_store._locked_connect() as c:
        row = c.execute(
            "SELECT op_freq_hz, op_re_z11, op_im_z11, op_re_z22, op_im_z22, op_re_z12, op_im_z12, max_mag_delta_s, "
            "loss_w_copper_tx, loss_w_fr4_tx, loss_w_ferrite_tx, loss_w_copper_rx, loss_w_fr4_rx, loss_w_ferrite_rx "
            "FROM single_simulation_results WHERE request_id='d-1'"
        ).fetchone()
    (op_freq, re11, im11, re22, im22, re12, im12, dlt, cu_tx, fr4_tx, fe_tx, cu_rx, fr4_rx, fe_rx) = row
    assert op_freq == 6.78e6
    assert (re11, im11, re22, im22, re12, im12) == (10.0, -20.0, 3.0, -4.0, 0.5, 1.2)  # Results1_Pass 마지막행
    assert dlt is None  # 소스 없음 → null
    assert (cu_tx, fr4_tx, fe_tx, cu_rx, fr4_rx) == (0.01, 0.02, 0.03, 0.04, 0.05)
    assert fe_rx is None  # rx ferrite 없음 → null
    with pg_store._locked_connect() as c:
        sw = c.execute("SELECT freq_hz, re_z11, im_z11 FROM freq_sweep WHERE point_hash='ph-d-1' ORDER BY freq_hz").fetchall()
    assert len(sw) == 5
    assert sw[0][0] == 100.0 * 1e3 and sw[0][1] == 1.0  # kHz->Hz, re_z11
    assert [s[0] for s in sw] == sorted(s[0] for s in sw)


def test_v2_results_schema_types_and_cursor(pg_store) -> None:  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from peetsfea_runner.edt_data_api import create_data_api_app

    _rec_full(pg_store, "ok-1")
    _rec(pg_store, "fail-1", "failed")  # success 필터로 걸러질 행(커서는 전진해야)
    _rec_full(pg_store, "ok-2")
    client = TestClient(create_data_api_app(store=pg_store))

    r = client.get("/api/v2/results?since=0&limit=10000&state=success")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.apache.arrow.stream"
    tbl = pa.ipc.open_stream(r.content).read_all()  # ZSTD 자동 해제
    sch = tbl.schema
    assert sch.field("started_at").type == pa.timestamp("us", tz="UTC")
    assert sch.field("point_values").type == pa.map_(pa.string(), pa.float64())
    assert pa.types.is_dictionary(sch.field("peetsfea_version").type)
    assert sch.field("elapsed_ms").type == pa.int64()
    assert "csv_text_by_report_json" not in tbl.column_names  # 핫 경로 CSV 0
    rids = tbl.column("request_id").to_pylist()
    assert rids == ["ok-1", "ok-2"]  # success만, seq 오름차순
    rec = {k: v for k, v in zip(tbl.column_names, [col[0].as_py() for col in tbl.columns])}
    assert rec["op_re_z11"] == 10.0 and rec["elapsed_ms"] == 900000
    assert dict(rec["point_values"]) == {"a": 1.5, "b": 2.0}
    # 커서: 필터로 걸러진 fail-1 까지 스캔했으니 X-Next-Since 는 최대 seq(=ok-2)
    last_seq = max(tbl.column("seq").to_pylist())
    assert int(r.headers["X-Next-Since"]) >= last_seq
    assert r.headers["X-Has-More"] == "false"

    # 증분: 신규 0건 → 빈 스트림 + has_more false
    nxt = r.headers["X-Next-Since"]
    r0 = client.get(f"/api/v2/results?since={nxt}&state=any")
    assert pa.ipc.open_stream(r0.content).read_all().num_rows == 0
    assert r0.headers["X-Has-More"] == "false"


def test_v2_results_projection(pg_store) -> None:  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from peetsfea_runner.edt_data_api import create_data_api_app

    _rec_full(pg_store, "p-1")
    client = TestClient(create_data_api_app(store=pg_store))
    r = client.get("/api/v2/results?columns=op_re_z11,op_im_z12")
    tbl = pa.ipc.open_stream(r.content).read_all()
    assert set(tbl.column_names) == {"seq", "op_re_z11", "op_im_z12"}  # seq 항상 포함


def test_v2_sweeps_and_health(pg_store) -> None:  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from peetsfea_runner.edt_data_api import create_data_api_app

    _rec_full(pg_store, "s-1", n_sweep=4)
    _rec_full(pg_store, "s-2", n_sweep=6)
    client = TestClient(create_data_api_app(store=pg_store))

    r = client.get("/api/v2/sweeps?since=0")
    tbl = pa.ipc.open_stream(r.content).read_all()
    assert tbl.num_rows == 10  # 4 + 6
    pairs = list(zip(tbl.column("result_seq").to_pylist(), tbl.column("freq_hz").to_pylist()))
    assert pairs == sorted(pairs)  # (result_seq, freq_hz) 오름차순
    assert r.headers["X-Has-More"] == "false"

    # result_seqs 지정 모드
    seq2 = max(tbl.column("result_seq").to_pylist())
    r2 = client.get(f"/api/v2/sweeps?result_seqs={seq2}")
    t2 = pa.ipc.open_stream(r2.content).read_all()
    assert set(t2.column("result_seq").to_pylist()) == {seq2}

    # since 와 result_seqs 상호배타 → 400
    assert client.get("/api/v2/sweeps?since=5&result_seqs=1").status_code == 400

    h = client.get("/api/v2/health").json()
    assert h["status"] == "ok" and h["schema_version"] == "2.0"
    assert h["results_rows"] == 2 and h["sweeps_max_result_seq"] == seq2


def test_v2_sweeps_design_boundary_paging(pg_store) -> None:  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from peetsfea_runner.edt_data_api import create_data_api_app

    _rec_full(pg_store, "b-1", n_sweep=4)
    _rec_full(pg_store, "b-2", n_sweep=4)
    client = TestClient(create_data_api_app(store=pg_store))
    # limit=4 → 첫 설계(4행) 채우고 둘째 설계 경계에서 멈춤(설계 분할 금지) → 다음 페이지로
    r = client.get("/api/v2/sweeps?since=0&limit=4")
    t = pa.ipc.open_stream(r.content).read_all()
    assert t.num_rows == 4 and r.headers["X-Has-More"] == "true"
    nxt = r.headers["X-Next-Since"]
    r2 = client.get(f"/api/v2/sweeps?since={nxt}&limit=5")
    t2 = pa.ipc.open_stream(r2.content).read_all()
    assert t2.num_rows == 4 and r2.headers["X-Has-More"] == "false"


def test_backfill_dataplane(pg_store) -> None:  # type: ignore[no-untyped-def]
    _rec_full(pg_store, "bf-1")
    # 추출 컬럼/스윕을 비워 '구 데이터' 상태로 되돌린 뒤 백필이 복구하는지 검증
    with pg_store._locked_connect() as c:
        c.execute("UPDATE single_simulation_results SET op_re_z11=NULL, op_freq_hz=NULL")
        c.execute("DELETE FROM freq_sweep")
    out = pg_store.backfill_dataplane()
    assert out["processed"] == 1 and out["sweep_rows"] == 5
    with pg_store._locked_connect() as c:
        v = c.execute("SELECT op_re_z11 FROM single_simulation_results WHERE request_id='bf-1'").fetchone()[0]
        n = c.execute("SELECT count(*) FROM freq_sweep WHERE point_hash='ph-bf-1'").fetchone()[0]
    assert v == 10.0 and n == 5


def _rec_design(store, rid, ph, state="success"):  # type: ignore[no-untyped-def]
    store.record_envelope({
        "request_id": rid, "terminal_state": state, "finished_at": "2026-06-26T10:00:00+00:00",
        "result": {"design_id": ph, "point_hash": ph, "point_values": {"x": 1.0}}, "error": {},
    })


def test_different_designs_same_request_id_both_survive(pg_store) -> None:  # type: ignore[no-untyped-def]
    # 버그 재현 방지: 같은 request_id(카드슬롯 재사용)라도 다른 point_hash는 서로 삭제/덮어쓰지 않는다.
    _rec_design(pg_store, "toml-custom-3-5", "designA")
    _rec_design(pg_store, "toml-custom-3-5", "designB")  # 같은 request_id, 다른 설계
    with pg_store._locked_connect() as c:
        phs = {r[0] for r in c.execute(
            "SELECT point_hash FROM single_simulation_results WHERE point_hash IN ('designA','designB')").fetchall()}
    assert phs == {"designA", "designB"}  # 예전엔 designB가 designA를 덮어썼음 → 이제 둘 다 생존


def test_same_design_resolve_keepbest(pg_store) -> None:  # type: ignore[no-untyped-def]
    # 같은 point_hash 재시도는 1행 유지(keep-best), 실패가 success를 덮지 않음.
    _rec_design(pg_store, "r1", "D", "success")
    _rec_design(pg_store, "r2", "D", "failed")  # 같은 설계 다른 request_id, failed
    with pg_store._locked_connect() as c:
        rows = c.execute("SELECT terminal_state FROM single_simulation_results WHERE point_hash='D'").fetchall()
    assert len(rows) == 1 and rows[0][0] == "success"  # 1행·success 불변


def test_failures_without_point_hash_dedup_by_request_id(pg_store) -> None:  # type: ignore[no-untyped-def]
    # point_hash 없는 실패행은 request_id로 keep-best(누적 안 됨).
    _rec(pg_store, "f1", "failed")
    _rec(pg_store, "f1", "failed")  # 같은 request_id 재인제스트
    with pg_store._locked_connect() as c:
        n = c.execute("SELECT count(*) FROM single_simulation_results WHERE request_id='f1'").fetchone()[0]
    assert n == 1  # 중복 안 생김
