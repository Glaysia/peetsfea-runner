from __future__ import annotations

import threading

from peetsfea_runner.edt_queue import BaselineRefiller, QueueItem, TwoLaneQueue


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


def test_get_never_samples_inline() -> None:
    # 핵심 회귀: get()은 절대 샘플링하지 않는다(샘플은 느린 geometry 빌드 → 슬롯 기아 유발).
    q = TwoLaneQueue(baseline_floor_percent=100)
    assert q.get() is None  # baseline 비어도 인라인 샘플 없이 즉시 None


def test_baseline_refiller_tops_up_in_background() -> None:
    calls = {"n": 0}
    lock = threading.Lock()

    def sampler() -> list[QueueItem]:
        with lock:
            calls["n"] += 1
            base = calls["n"] * 100
        return [_item(f"B{base + i}") for i in range(8)]  # 작은 청크

    q = TwoLaneQueue(baseline_floor_percent=0)
    refiller = BaselineRefiller(queue=q, sampler=sampler, low_watermark=16, poll_seconds=0.01)
    refiller.start()
    try:
        # 백그라운드 워커가 저수위(16)까지 채운다(청크 8 → 최소 2회 호출).
        for _ in range(200):
            _, depth = q.depths()
            if depth >= 16:
                break
            threading.Event().wait(0.01)
        _, depth = q.depths()
        assert depth >= 16
        assert calls["n"] >= 2
        assert q.get() is not None  # 슬롯이 즉시 소비 가능
    finally:
        refiller.stop()


def test_baseline_refiller_survives_sampler_errors() -> None:
    state = {"n": 0}

    def flaky() -> list[QueueItem]:
        state["n"] += 1
        if state["n"] <= 2:
            raise RuntimeError("sample failed")
        return [_item(f"B{state['n']}")]

    q = TwoLaneQueue(baseline_floor_percent=0)
    refiller = BaselineRefiller(queue=q, sampler=flaky, low_watermark=1, poll_seconds=0.01)
    refiller.start()
    try:
        for _ in range(200):
            if q.depths()[1] >= 1:
                break
            threading.Event().wait(0.01)
        assert q.depths()[1] >= 1  # 초기 실패를 견디고 결국 채움
    finally:
        refiller.stop()
