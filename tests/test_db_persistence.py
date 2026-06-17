from __future__ import annotations

from pathlib import Path

from peetsfea_runner.edt_queue import QueueItem
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

def _items(*ids: str) -> list[QueueItem]:
    return [QueueItem(request_id=i, candidate_toml_text=f"# {i}", seed=1, mode="full") for i in ids]


def test_priority_enqueue_lease_fifo_and_depth(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.priority_enqueue(_items("a", "b", "c"), now=1000.0)
    assert store.priority_depth() == 3
    leased = store.priority_lease(2)
    assert [it.request_id for it in leased] == ["a", "b"]  # FIFO(created_at 순)
    assert store.priority_depth() == 1
    assert [it.request_id for it in store.priority_lease(10)] == ["c"]  # 남은 만큼만
    assert store.priority_lease(5) == []  # 비면 빈 리스트


def test_priority_enqueue_dedup_request_id(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.priority_enqueue(_items("a"), now=1.0)
    store.priority_enqueue(_items("a", "b"), now=2.0)  # a는 중복 → 무시
    assert store.priority_depth() == 2
    assert {it.request_id for it in store.priority_lease(10)} == {"a", "b"}


def test_priority_queue_survives_restart(tmp_path: Path) -> None:
    db = tmp_path / "r.duckdb"
    SingleSimulationResultStore(db_path=db).priority_enqueue(_items("x", "y"), now=1.0)
    # 새 인스턴스(web 재시작) → 미처리 우선순위 항목 보존
    store2 = SingleSimulationResultStore(db_path=db)
    assert store2.priority_depth() == 2
    assert [it.request_id for it in store2.priority_lease(2)] == ["x", "y"]


# --- DbPriorityQueue 드롭인 어댑터(IntakeService/lease 서버용) ------------------

def test_db_priority_queue_dropin(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    clock = iter([1.0, 2.0, 3.0, 4.0])
    q = DbPriorityQueue(store=store, clock=lambda: next(clock))
    q.extend_priority(_items("a", "b"))   # IntakeService가 호출
    q.put_priority(QueueItem(request_id="c", candidate_toml_text="# c"))
    assert q.depths() == (3, 0)           # (priority, baseline) — baseline은 컨테이너 자기공급이라 0
    leased = q.lease_priority(2)          # lease 서버가 호출
    assert [it.request_id for it in leased] == ["a", "b"]
    assert q.depths() == (1, 0)
