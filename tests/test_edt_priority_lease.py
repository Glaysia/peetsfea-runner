from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path

from peetsfea_runner.edt_priority_lease import PriorityPuller, start_priority_lease_server
from peetsfea_runner.edt_queue import QueueItem, TwoLaneQueue
from peetsfea_runner.single_simulation_store import DbPriorityQueue, SingleSimulationResultStore


def _item(rid: str) -> QueueItem:
    return QueueItem(request_id=rid, candidate_toml_text=f"# toml {rid}\n", seed=7, mode="full")


def _control(tmp_path: Path) -> DbPriorityQueue:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    return DbPriorityQueue(store=store, clock=lambda: 1.0)


def _fake_sampler(sweep: str, count: int, seed: int) -> list[str]:
    return [f"# cand seed={seed + i}\n" for i in range(count)]


def _serve(server) -> threading.Thread:
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return t


def test_two_lane_blend_priority_85_baseline_15() -> None:
    # 워커 로컬 큐의 블렌드(컨테이너가 샘플한 우선순위 85% / baseline 15%)는 그대로 유지.
    q = TwoLaneQueue()
    q.extend_priority([_item(f"p{i}") for i in range(1000)])
    q.seed_baseline([_item(f"b{i}") for i in range(1000)])
    import collections

    c = collections.Counter(q.get().request_id[0] for _ in range(400))  # type: ignore[union-attr]
    assert c["p"] == 340 and c["b"] == 60  # 85% / 15%


def test_lease_server_hands_out_chunk(tmp_path: Path) -> None:
    control = _control(tmp_path)
    control.enqueue_sweep(request_id="sweep-1", sweep_toml_text="# sw", seed=100, count=10)
    server = start_priority_lease_server(queue=control, host="127.0.0.1", port=0)
    _serve(server)
    try:
        port = server.server_address[1]
        health = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/health"))
        assert health == {"status": "ok", "priority_depth": 10}  # 대기 후보 = remaining 합
        chunk = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/lease?n=4"))
        assert chunk["request_id"] == "sweep-1" and chunk["count"] == 4 and chunk["seed_base"] == 100
        assert chunk["sweep_toml_text"] == "# sw"
        json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/lease?n=100"))  # 남은 6 소진
        empty = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/lease"))
        assert empty == {}  # chunk 없으면 빈 객체
    finally:
        server.shutdown()


def test_puller_samples_chunk_into_local_lane(tmp_path: Path) -> None:
    # 컨트롤플레인은 sweep만 들고, **워커(puller)가 컨테이너에서 샘플링**해 로컬 레인에 적재.
    control = _control(tmp_path)
    control.enqueue_sweep(request_id="sweep-1", sweep_toml_text="# sw", seed=0, count=20)
    server = start_priority_lease_server(queue=control, host="127.0.0.1", port=0)
    _serve(server)
    try:
        port = server.server_address[1]
        local = TwoLaneQueue()
        puller = PriorityPuller(
            queue=local,
            lease_url=f"http://127.0.0.1:{port}/lease",
            sampler=_fake_sampler,
            batch=5,
            low_watermark=4,
            poll_seconds=0.05,
        )
        puller.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and local.depths()[0] == 0:
            time.sleep(0.05)
        puller.stop()
        prio, base = local.depths()
        assert prio > 0  # 컨테이너에서 샘플링해 로컬 우선순위 레인 적재
        assert base == 0  # baseline은 안 건드림
        item = local.get()
        assert item is not None and item.candidate_toml_text.startswith("# cand")
    finally:
        server.shutdown()


def test_puller_backoff_when_remote_empty(tmp_path: Path) -> None:
    control = _control(tmp_path)  # 비어 있음(sweep 없음)
    server = start_priority_lease_server(queue=control, host="127.0.0.1", port=0)
    _serve(server)
    try:
        port = server.server_address[1]
        local = TwoLaneQueue()
        puller = PriorityPuller(
            queue=local, lease_url=f"http://127.0.0.1:{port}/lease", sampler=_fake_sampler, poll_seconds=0.05
        )
        puller.start()
        time.sleep(0.3)
        puller.stop()
        assert local.depths() == (0, 0)  # 당겨올 chunk 없으면 비어 있음(에러 없이 backoff)
    finally:
        server.shutdown()
