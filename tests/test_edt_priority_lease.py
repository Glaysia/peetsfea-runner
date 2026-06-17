from __future__ import annotations

import threading
import time

from peetsfea_runner.edt_priority_lease import PriorityPuller, start_priority_lease_server
from peetsfea_runner.edt_queue import QueueItem, TwoLaneQueue


def _item(rid: str) -> QueueItem:
    return QueueItem(request_id=rid, candidate_toml_text=f"# toml {rid}\n", seed=7, mode="full")


def test_lease_priority_pops_priority_only_and_respects_n() -> None:
    q = TwoLaneQueue()
    q.extend_priority([_item("p0"), _item("p1"), _item("p2")])
    q.seed_baseline([_item("b0"), _item("b1")])

    leased = q.lease_priority(2)
    assert [it.request_id for it in leased] == ["p0", "p1"]  # FIFO, priority만
    assert q.depths() == (1, 2)  # baseline 그대로

    rest = q.lease_priority(10)  # n이 재고보다 커도 있는 만큼만
    assert [it.request_id for it in rest] == ["p2"]
    assert q.lease_priority(5) == []  # 비면 빈 리스트
    assert q.depths() == (0, 2)


def _serve(server) -> threading.Thread:
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return t


def test_lease_server_hands_out_and_drains() -> None:
    q = TwoLaneQueue()
    q.extend_priority([_item("p0"), _item("p1")])
    server = start_priority_lease_server(queue=q, host="127.0.0.1", port=0)
    _serve(server)
    try:
        import json
        import urllib.request

        port = server.server_address[1]
        health = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/health"))
        assert health == {"status": "ok", "priority_depth": 2}

        payload = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/lease?n=5"))
        ids = [it["request_id"] for it in payload["items"]]
        assert ids == ["p0", "p1"]
        assert payload["items"][0]["candidate_toml_text"] == "# toml p0\n"
        # 비었으면 빈 items
        assert json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/lease"))["items"] == []
    finally:
        server.shutdown()


def test_puller_refills_local_priority_lane_from_server() -> None:
    # 컨트롤플레인 큐(원격) + 워커 로컬 큐. 풀러가 원격을 당겨 로컬 우선순위 레인에 적재.
    control = TwoLaneQueue()
    control.extend_priority([_item(f"p{i}") for i in range(20)])
    server = start_priority_lease_server(queue=control, host="127.0.0.1", port=0)
    _serve(server)
    try:
        port = server.server_address[1]
        local = TwoLaneQueue()
        puller = PriorityPuller(
            queue=local,
            lease_url=f"http://127.0.0.1:{port}/lease",
            batch=5,
            low_watermark=4,
            poll_seconds=0.05,
        )
        puller.start()
        # 로컬이 워터마크 아래라 풀러가 당겨와야 함.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and local.depths()[0] == 0:
            time.sleep(0.05)
        puller.stop()
        prio, base = local.depths()
        assert prio > 0  # 원격에서 당겨옴
        assert base == 0  # baseline은 안 건드림
        # 당겨온 항목은 블렌드로 소비 가능(priority 우선).
        item = local.get()
        assert item is not None and item.request_id.startswith("p")
    finally:
        server.shutdown()


def test_puller_backoff_when_remote_empty() -> None:
    control = TwoLaneQueue()  # 비어 있음
    server = start_priority_lease_server(queue=control, host="127.0.0.1", port=0)
    _serve(server)
    try:
        port = server.server_address[1]
        local = TwoLaneQueue()
        puller = PriorityPuller(queue=local, lease_url=f"http://127.0.0.1:{port}/lease", poll_seconds=0.05)
        puller.start()
        time.sleep(0.3)
        puller.stop()
        assert local.depths() == (0, 0)  # 당겨올 게 없으면 비어 있음(에러 없이 backoff)
    finally:
        server.shutdown()
