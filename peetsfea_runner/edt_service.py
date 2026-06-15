"""Phase 1 와이어링 — 슬롯 10개(RealEdtBackend) + 큐 + 디스패처 + 결과 DB.

systemd user 서비스(또는 컨테이너 진입점)가 호출하는 빌더. 큐는 디렉토리에서 수동 시드한다
(7875 인테이크는 Phase 4). 실제 시뮬은 peetsfea 0.3.2 프리미티브를 grpc_port와 함께 호출한다.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import SLOTS_PER_CONTAINER
from .edt_aedt_backend import RealEdtBackend, default_ansysedt_executable
from .edt_dispatcher import SimulationPrimitive, SlotDispatcher
from .edtmgr import EdtManager
from .edt_queue import TomlQueue, load_queue_items_from_dir
from .single_simulation_store import SingleSimulationResultStore


def _default_primitive() -> SimulationPrimitive:
    from peetsfea.ssw_random_sample_reports import run_ssw_random_sample_reports_from_toml_text

    primitive: SimulationPrimitive = run_ssw_random_sample_reports_from_toml_text
    return primitive


@dataclass(slots=True)
class EdtServiceConfig:
    output_root: Path
    db_path: Path
    queue_dir: Path | None = None
    slot_count: int = SLOTS_PER_CONTAINER
    executable: Path | None = None
    account_id: str = "account_01"
    host_alias: str = "gate1-harry261"
    work_dir: Path | None = None
    drain: bool = True


def build_slots(config: EdtServiceConfig) -> list[EdtManager]:
    executable = config.executable or default_ansysedt_executable()
    slots: list[EdtManager] = []
    for index in range(config.slot_count):
        slot_id = f"slot_{index:02d}"
        backend = RealEdtBackend(slot_id=slot_id, executable=executable, work_dir=config.work_dir)
        slots.append(EdtManager(backend=backend, clock=time.monotonic, slot_id=slot_id))
    return slots


def build_dispatcher(
    config: EdtServiceConfig,
    *,
    primitive: SimulationPrimitive | None = None,
    slots: list[EdtManager] | None = None,
    queue: TomlQueue | None = None,
) -> tuple[SlotDispatcher, TomlQueue, SingleSimulationResultStore]:
    store = SingleSimulationResultStore(db_path=config.db_path)
    store.initialize()
    work_queue = queue if queue is not None else TomlQueue()
    if config.queue_dir is not None:
        work_queue.extend(load_queue_items_from_dir(config.queue_dir))

    def record(envelope: Mapping[str, Any]) -> None:
        store.record_envelope(envelope)

    dispatcher = SlotDispatcher(
        slots=slots if slots is not None else build_slots(config),
        queue=work_queue,
        primitive=primitive if primitive is not None else _default_primitive(),
        output_root=config.output_root,
        record=record,
        account_id=config.account_id,
        host_alias=config.host_alias,
        drain=config.drain,
    )
    return dispatcher, work_queue, store


def run_edt_service(config: EdtServiceConfig, *, primitive: SimulationPrimitive | None = None) -> int:
    """슬롯을 띄워 큐를 처리한다. 반환: 처리 건수."""

    dispatcher, _queue, _store = build_dispatcher(config, primitive=primitive)
    return dispatcher.run()


__all__ = ["EdtServiceConfig", "build_dispatcher", "build_slots", "run_edt_service"]
