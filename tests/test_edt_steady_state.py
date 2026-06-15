from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from peetsfea_runner.edt_dispatcher import SlotDispatcher
from peetsfea_runner.edt_load import AdmissionController, LoadSample
from peetsfea_runner.edt_queue import QueueItem, TwoLaneQueue
from peetsfea_runner.edtmgr import AedtSession, EdtBackend, EdtManager
from peetsfea_runner.edt_service import EdtServiceConfig, build_steady_state_service


class FakeBackend(EdtBackend):
    def __init__(self) -> None:
        self.alive = False

    def start(self) -> AedtSession:
        self.alive = True
        return AedtSession(pid=1, grpc_port=2)

    def lend(self) -> AedtSession:
        return AedtSession(pid=1, grpc_port=2)

    def reclaim(self) -> None:
        return None

    def is_alive(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.alive = False


def _slots(n: int) -> list[EdtManager]:
    return [EdtManager(backend=FakeBackend(), clock=time.monotonic, slot_id=f"slot_{i}") for i in range(n)]


def _headroom_admission() -> AdmissionController:
    return AdmissionController(
        load_sampler=lambda: LoadSample(cpu_percent=10.0, mem_percent=10.0),
        clock=time.monotonic,
        min_start_interval_seconds=0.0,
        ewma_alpha=1.0,
    )


def test_two_lane_dispatcher_processes_both_lanes_under_admission(tmp_path: Path) -> None:
    def primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
        return {"ok": True}

    queue = TwoLaneQueue(baseline_floor_percent=15)
    queue.extend_priority(QueueItem(f"P{i}", f"x={i}\n") for i in range(10))
    queue.seed_baseline(QueueItem(f"B{i}", f"y={i}\n") for i in range(10))

    recorded: list[dict[str, Any]] = []
    disp = SlotDispatcher(
        slots=_slots(3),
        queue=queue,
        primitive=primitive,
        output_root=tmp_path / "out",
        record=lambda e: recorded.append(dict(e)),
        version_loader=lambda: "test",
        admission=_headroom_admission(),  # 부하 여유 → 모두 통과
        drain=True,
    )
    processed = disp.run()
    assert processed == 20  # priority 10 + baseline 10 전부
    rids = {e["request_id"] for e in recorded}
    assert any(r.startswith("P") for r in rids) and any(r.startswith("B") for r in rids)
    assert all(e["terminal_state"] == "success" for e in recorded)


def test_build_steady_state_service_wires_lb_queue_intake(tmp_path: Path) -> None:
    config = EdtServiceConfig(
        output_root=tmp_path / "out",
        db_path=tmp_path / "r.duckdb",
        reference_sweep_text=None,  # baseline 휴면(peetsfea 미사용)
        enable_load_balancer=True,
    )
    svc = build_steady_state_service(
        config,
        slots=_slots(1),  # RealEdtBackend 생성 회피
        primitive=lambda *a, **k: {"ok": True},
        load_sampler=lambda: LoadSample(10.0, 10.0),
    )
    assert isinstance(svc.queue, TwoLaneQueue)
    assert svc.admission is not None  # 로드밸런서 활성
    assert svc.dispatcher.queue is svc.queue
    assert svc.dispatcher.admission is svc.admission
    assert svc.dispatcher.drain is False  # 상시 가동
    assert svc.intake.queue is svc.queue  # intake가 같은 큐 우선순위 레인에 적재


def test_build_steady_state_service_lb_disabled(tmp_path: Path) -> None:
    config = EdtServiceConfig(
        output_root=tmp_path / "out",
        db_path=tmp_path / "r.duckdb",
        enable_load_balancer=False,
    )
    svc = build_steady_state_service(config, slots=_slots(1), primitive=lambda *a, **k: {"ok": True})
    assert svc.admission is None
    assert svc.dispatcher.admission is None
