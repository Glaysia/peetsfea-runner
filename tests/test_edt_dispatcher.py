from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from peetsfea_runner.edt_dispatcher import SlotDispatcher
from peetsfea_runner.edtmgr import AedtSession, EdtBackend, EdtManager
from peetsfea_runner.edt_queue import QueueItem, TomlQueue


class FakeBackend(EdtBackend):
    def __init__(self, base_port: int) -> None:
        self.alive = False
        self.starts = 0
        self.kills = 0
        self.reclaims = 0
        self._pid = base_port
        self._port = base_port

    def start(self) -> AedtSession:
        self.starts += 1
        self.alive = True
        return AedtSession(pid=self._pid, grpc_port=self._port)

    def lend(self) -> AedtSession:
        return AedtSession(pid=self._pid, grpc_port=self._port)

    def reclaim(self) -> None:
        self.reclaims += 1

    def is_alive(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.kills += 1
        self.alive = False


def _slots(n: int) -> list[EdtManager]:
    return [
        EdtManager(backend=FakeBackend(50000 + i), clock=time.monotonic, slot_id=f"slot_{i}")
        for i in range(n)
    ]


def _dispatcher(slots: list[EdtManager], queue: TomlQueue, primitive: Any, tmp_path: Path, **kw: Any) -> tuple[SlotDispatcher, list[dict[str, Any]]]:
    recorded: list[dict[str, Any]] = []
    disp = SlotDispatcher(
        slots=slots,
        queue=queue,
        primitive=primitive,
        output_root=tmp_path / "out",
        record=lambda env: recorded.append(dict(env)),
        version_loader=lambda: "0.3.2",
        **kw,
    )
    return disp, recorded


def test_processes_all_items_across_slots(tmp_path: Path) -> None:
    seen_ports: list[int] = []

    def primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
        seen_ports.append(grpc_port)
        return {"k_ratio": 1.0, "echo_port": grpc_port}

    queue = TomlQueue()
    queue.extend(QueueItem(f"req_{i}", f"x = {i}\n", seed=i) for i in range(6))
    disp, recorded = _dispatcher(_slots(3), queue, primitive, tmp_path)

    processed = disp.run()

    assert processed == 6
    assert len(recorded) == 6
    assert {e["request_id"] for e in recorded} == {f"req_{i}" for i in range(6)}
    assert all(e["terminal_state"] == "success" for e in recorded)
    assert all(e["peetsfea_version"] == "0.3.2" for e in recorded)
    # 프리미티브가 edtmgr가 준 grpc_port를 실제로 받았다.
    assert len(seen_ports) == 6 and all(p >= 50000 for p in seen_ports)


def test_failure_records_failed_and_recovers(tmp_path: Path) -> None:
    def primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
        raise RuntimeError("boom")

    queue = TomlQueue()
    queue.put(QueueItem("req_fail", "x = 1\n"))
    slots = _slots(1)
    disp, recorded = _dispatcher(slots, queue, primitive, tmp_path)

    disp.run()

    assert len(recorded) == 1
    env = recorded[0]
    assert env["terminal_state"] == "failed"
    assert env["error"]["type"] == "RuntimeError"
    # 살아있는 백엔드 → recover는 reclaim(재부착)으로 처리.
    backend = slots[0].backend
    assert isinstance(backend, FakeBackend)
    assert backend.reclaims == 1 and backend.kills == 0


def test_backstop_aborts_hung_sim(tmp_path: Path) -> None:
    def primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
        time.sleep(0.6)  # 백스톱(0.3s)보다 오래 → 미반환
        return {"k_ratio": 0.0}

    queue = TomlQueue()
    queue.put(QueueItem("req_hang", "x = 1\n"))
    slots = _slots(1)
    disp, recorded = _dispatcher(slots, queue, primitive, tmp_path, backstop_seconds=0.3)

    disp.run()

    assert len(recorded) == 1
    env = recorded[0]
    assert env["terminal_state"] == "aborted"
    assert env["error"]["type"] == "BackstopTimeout"
    backend = slots[0].backend
    assert isinstance(backend, FakeBackend)
    assert backend.kills == 1  # 강제 종료 후 재기동
