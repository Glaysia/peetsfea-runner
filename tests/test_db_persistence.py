from __future__ import annotations

from pathlib import Path

from peetsfea_runner.single_simulation_store import DbPriorityQueue, SingleSimulationResultStore


# --- 라이선스/자원 시계열 영속 -------------------------------------------------

def test_resource_snapshot_record_and_fetch(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    for ts in (100.0, 200.0, 300.0):
        store.record_resource_snapshot({
            "ts": ts, "running": 9, "pending": 1, "lic_mine": 34, "lic_inuse": 101,
            "load": 12.3, "cpus": 432, "mem_used_mb": 5000, "mem_total_mb": 9000,
        })
    rows = store.fetch_resource_history()
    assert [r["ts"] for r in rows] == [100.0, 200.0, 300.0]  # 오래된→최신
    assert rows[0]["lic_mine"] == 34 and rows[0]["load"] == 12.3 and rows[0]["cpus"] == 432
    # since로 범위 제한
    assert [r["ts"] for r in store.fetch_resource_history(since_ts=200.0)] == [200.0, 300.0]


def test_resource_snapshot_survives_restart(tmp_path: Path) -> None:
    db = tmp_path / "r.duckdb"
    SingleSimulationResultStore(db_path=db).record_resource_snapshot({"ts": 1.0, "lic_mine": 7})
    # 새 인스턴스(=web 재시작) → 데이터 살아있음
    rows = SingleSimulationResultStore(db_path=db).fetch_resource_history()
    assert len(rows) == 1 and rows[0]["lic_mine"] == 7


def test_resource_prune(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    for ts in (10.0, 20.0, 30.0):
        store.record_resource_snapshot({"ts": ts})
    assert store.prune_resource_snapshots(before_ts=25.0) == 2
    assert [r["ts"] for r in store.fetch_resource_history()] == [30.0]


# --- 우선순위 큐 영속 ----------------------------------------------------------

def _sweep(store: SingleSimulationResultStore, rid: str, count: int, *, seed: int = 0, now: float = 1.0) -> None:
    store.priority_enqueue_sweep(request_id=rid, sweep_toml_text=f"# {rid}", seed=seed, count=count, mode="full", now=now)


def test_priority_chunk_lease_decrements_remaining(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    _sweep(store, "sweep-1", 50, seed=100, now=1.0)
    assert store.priority_depth() == 50  # 대기 후보 = remaining 합
    c1 = store.priority_lease_chunk(16)
    assert c1["request_id"] == "sweep-1" and c1["count"] == 16 and c1["seed_base"] == 100  # offset 0
    assert store.priority_depth() == 34
    c2 = store.priority_lease_chunk(16)
    assert c2["count"] == 16 and c2["seed_base"] == 116  # offset 16 → 다른 후보 seed대역
    assert store.priority_depth() == 18


def test_priority_chunk_fifo_across_sweeps_and_drain(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    _sweep(store, "sweep-1", 2, now=1.0)
    _sweep(store, "sweep-2", 3, now=2.0)
    assert store.priority_lease_chunk(10)["request_id"] == "sweep-1"  # 오래된 것 먼저
    assert store.priority_lease_chunk(10)["request_id"] == "sweep-2"  # sweep-1 소진 → 다음
    assert store.priority_depth() == 0
    assert store.priority_lease_chunk(10) is None  # 비면 None


def test_priority_enqueue_sweep_dedup(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    _sweep(store, "sweep-1", 5, now=1.0)
    _sweep(store, "sweep-1", 99, now=2.0)  # 같은 request_id → 무시
    assert store.priority_depth() == 5


def test_priority_list_shows_remaining_no_toml(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    _sweep(store, "sweep-1", 10, now=5.0)
    store.priority_lease_chunk(4)
    listed = store.priority_list()
    assert listed[0]["request_id"] == "sweep-1" and listed[0]["total_count"] == 10 and listed[0]["remaining_count"] == 6
    assert "sweep_toml_text" not in listed[0]  # 무거운 본문 제외
    assert store.priority_depth() == 6  # 조회는 차감 안 함


def test_priority_lineage_links_results_by_prefix(tmp_path: Path) -> None:
    # 입력큐 계보: 결과 request_id=`{sweep}-{seed}`를 prefix로 sweep에 연결, baseline(base-*)은 제외.
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    _sweep(store, "userA", 10, now=1.0)
    _sweep(store, "userB", 5, now=2.0)
    store.priority_lease_chunk(3)  # 가장 오래된(userA) 3 차감
    for rid, state in [("userA-1000", "success"), ("userA-1001", "failed"), ("base-99-0", "success")]:
        store.record_envelope({"request_id": rid, "terminal_state": state, "seed": 0, "mode": "full"})
    lin = store.priority_lineage()
    assert lin["submitted"] == 15 and lin["waiting"] == 12 and lin["leased"] == 3
    assert lin["done"] == 2 and lin["ok"] == 1 and lin["fail"] == 1 and lin["inflight"] == 1  # base-* 제외됨
    a = next(i for i in lin["items"] if i["request_id"] == "userA")
    assert a["total"] == 10 and a["leased"] == 3 and a["done"] == 2 and a["inflight"] == 1


def test_priority_sweep_survives_restart(tmp_path: Path) -> None:
    db = tmp_path / "r.duckdb"
    _sweep(SingleSimulationResultStore(db_path=db), "sweep-9", 7, now=1.0)
    store2 = SingleSimulationResultStore(db_path=db)  # web 재시작
    assert store2.priority_depth() == 7
    assert store2.priority_lease_chunk(7)["count"] == 7


# --- DbPriorityQueue 드롭인 어댑터(IntakeService/lease 서버용) ------------------

def test_db_priority_queue_dropin(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    clock = iter([1.0, 2.0])
    q = DbPriorityQueue(store=store, clock=lambda: next(clock))
    q.enqueue_sweep(request_id="sweep-1", sweep_toml_text="# s1", seed=0, count=20)  # IntakeService가 호출
    q.enqueue_sweep(request_id="sweep-2", sweep_toml_text="# s2", seed=0, count=5)
    assert q.depth() == 25
    chunk = q.lease_chunk(8)  # lease 서버가 호출
    assert chunk["request_id"] == "sweep-1" and chunk["count"] == 8
    assert q.depth() == 17
    assert [r["request_id"] for r in q.list_requests()] == ["sweep-1", "sweep-2"]
