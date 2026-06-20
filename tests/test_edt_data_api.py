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
