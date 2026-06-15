from __future__ import annotations

from peetsfea_runner.edt_queue import QueueItem, TwoLaneQueue


def _item(rid: str) -> QueueItem:
    return QueueItem(request_id=rid, candidate_toml_text=f"x = {rid}\n")


def test_85_15_split_when_both_lanes_full() -> None:
    q = TwoLaneQueue(baseline_floor_percent=15)
    q.extend_priority(_item(f"P{i}") for i in range(100))
    q.seed_baseline(_item(f"B{i}") for i in range(100))

    drawn = [q.get() for _ in range(100)]
    lanes = ["B" if it.request_id.startswith("B") else "P" for it in drawn if it is not None]  # type: ignore[union-attr]
    assert lanes.count("B") == 15  # 15% 하드 플로어
    assert lanes.count("P") == 85


def test_priority_is_fifo_and_consumed_first() -> None:
    q = TwoLaneQueue(baseline_floor_percent=0)  # 플로어 0 → 순수 priority 우선
    q.extend_priority([_item("P0"), _item("P1"), _item("P2")])
    q.seed_baseline([_item("B0")])
    assert q.get().request_id == "P0"  # type: ignore[union-attr]
    assert q.get().request_id == "P1"  # type: ignore[union-attr]
    assert q.get().request_id == "P2"  # type: ignore[union-attr]
    assert q.get().request_id == "B0"  # priority 비면 baseline  # type: ignore[union-attr]
    assert q.get() is None


def test_priority_empty_serves_all_baseline() -> None:
    q = TwoLaneQueue()
    q.seed_baseline(_item(f"B{i}") for i in range(20))
    drawn = [q.get() for _ in range(20)]
    assert all(it is not None and it.request_id.startswith("B") for it in drawn)
    assert q.get() is None


def test_baseline_empty_serves_priority_even_on_floor_tick() -> None:
    # baseline 비고 sampler 없음 → 플로어 틱이어도 priority 서빙.
    q = TwoLaneQueue(baseline_floor_percent=100)  # 모든 틱이 baseline 우선이지만 재고 없음
    q.extend_priority([_item("P0"), _item("P1")])
    assert q.get().request_id == "P0"  # type: ignore[union-attr]
    assert q.get().request_id == "P1"  # type: ignore[union-attr]


def test_baseline_refill_when_low() -> None:
    calls = {"n": 0}

    def sampler() -> list[QueueItem]:
        calls["n"] += 1
        base = calls["n"] * 1000
        return [_item(f"B{base + i}") for i in range(1000)]

    q = TwoLaneQueue(baseline_sampler=sampler, baseline_low_watermark=200, baseline_floor_percent=100)
    # 첫 get에서 baseline 0 < 200 → 리필(1000).
    first = q.get()
    assert first is not None and first.request_id.startswith("B")
    assert calls["n"] == 1
    _prio_depth, base_depth = q.depths()
    assert base_depth == 999  # 1000 리필 - 1 소비
    # 800개 더 빼면 199 < 200 → 다음 get에서 재리필.
    for _ in range(800):
        q.get()
    q.get()  # 트리거
    assert calls["n"] == 2
