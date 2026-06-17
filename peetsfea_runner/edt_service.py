"""Phase 1 와이어링 — 슬롯 10개(RealEdtBackend) + 큐 + 디스패처 + 결과 DB.

systemd user 서비스(또는 컨테이너 진입점)가 호출하는 빌더. 큐는 디렉토리에서 수동 시드한다
(7875 인테이크는 Phase 4). 실제 시뮬은 peetsfea 0.3.2 프리미티브를 grpc_port와 함께 호출한다.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import SLOTS_PER_CONTAINER
from .edt_aedt_backend import RealEdtBackend, default_ansysedt_executable
from .edt_dispatcher import SimulationPrimitive, SlotDispatcher
from .edt_intake import IntakeService, make_baseline_sampler
from .edt_load import AdmissionController, LoadSampler, psutil_load_sampler
from .edtmgr import EdtManager
from .edt_priority_lease import PriorityPuller
from .edt_queue import BaselineRefiller, TomlQueue, TwoLaneQueue, load_queue_items_from_dir
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
    # Phase 3/4: baseline 샘플러용 기준 sweep 텍스트(없으면 baseline 휴면), 로드밸런서 on/off.
    reference_sweep_text: str | None = None
    enable_load_balancer: bool = True
    # baseline 청크 크기(작게! 샘플은 후보마다 geometry를 빌드해 느리다 — 백그라운드 워커가 청크 단위로
    # 채워 슬롯이 즉시 solve 시작하게 한다)와 저수위 임계.
    baseline_batch_size: int = 16
    baseline_low_watermark: int = 64
    # 워커별 seed 시작값(process-per-slot): 워커마다 다른 seed 범위 → request_id 충돌 방지 + 설계공간 분산.
    baseline_seed_start: int = 0
    partition: str = ""  # 자동 벤치마크용: 잡이 떠 있는 파티션/노드 기록
    node: str = ""


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


@dataclass(slots=True)
class SteadyStateService:
    """Phase 3+4 컨테이너-측 서비스: 2-레인 큐 + admission + 디스패처 + intake + 결과 sink.

    `store`는 로컬 단독 모드에서만 채워진다. 컨테이너에선 결과를 ingest(:7876)로 push하므로
    DuckDB를 만들지 않고 `store`는 None. `baseline_refiller`는 baseline 레인을 백그라운드로 채운다.
    """

    dispatcher: SlotDispatcher
    queue: TwoLaneQueue
    store: SingleSimulationResultStore | None
    intake: IntakeService
    admission: AdmissionController | None
    baseline_refiller: BaselineRefiller | None = None
    priority_puller: "PriorityPuller | None" = None  # 컨트롤플레인 우선순위 lease를 당겨 로컬 레인 보충(entrypoint가 주입).


def build_steady_state_service(
    config: EdtServiceConfig,
    *,
    slots: list[EdtManager] | None = None,
    primitive: SimulationPrimitive | None = None,
    load_sampler: LoadSampler | None = None,
    record: Callable[[Mapping[str, Any]], None] | None = None,
) -> SteadyStateService:
    """정상상태 컨테이너 서비스(Phase 3+4) 와이어링.

    - **2-레인 큐**: baseline(기준 sweep 풀샘플 리필) + 우선순위(Intake).
    - **admission(Phase 3)**: CPU/mem 부하 게이트(`enable_load_balancer`면 활성).
    - **Intake**: 우선순위 레인에 적재(서버 기동은 호출자가 `start_intake_server`로).
    - **record sink**: 외부 `record`가 주어지면(컨테이너=ingest push) DuckDB를 만들지 않는다.
      없으면 로컬 DuckDB에 직접 기록(단독/테스트).
    드레인하지 않고(`drain=False`) stop()까지 상시 가동.
    """
    store: SingleSimulationResultStore | None = None
    if record is None:
        store = SingleSimulationResultStore(db_path=config.db_path)
        store.initialize()
        record = store.record_envelope

    queue = TwoLaneQueue()
    # baseline refill은 **백그라운드 워커**로(디스패치 경로에서 분리). 샘플은 후보마다 geometry를 빌드해
    # 느리므로 슬롯에서 동기 호출하면 안 된다(문제 1). 청크는 작게, 저수위 때만 채운다.
    baseline_refiller: BaselineRefiller | None = None
    if config.reference_sweep_text:
        sampler = make_baseline_sampler(
            config.reference_sweep_text,
            batch_size=config.baseline_batch_size,
            seed_start=config.baseline_seed_start,
        )
        baseline_refiller = BaselineRefiller(
            queue=queue, sampler=sampler, low_watermark=config.baseline_low_watermark
        )
        baseline_refiller.start()

    admission = (
        AdmissionController(load_sampler=load_sampler or psutil_load_sampler, clock=time.monotonic)
        if config.enable_load_balancer
        else None
    )

    dispatcher = SlotDispatcher(
        slots=slots if slots is not None else build_slots(config),
        queue=queue,
        primitive=primitive if primitive is not None else _default_primitive(),
        output_root=config.output_root,
        record=record,
        account_id=config.account_id,
        host_alias=config.host_alias,
        partition=config.partition,
        node=config.node,
        admission=admission,
        drain=False,
    )
    return SteadyStateService(
        dispatcher=dispatcher,
        queue=queue,
        store=store,
        intake=IntakeService(queue=queue),
        admission=admission,
        baseline_refiller=baseline_refiller,
    )


__all__ = [
    "EdtServiceConfig",
    "SteadyStateService",
    "build_dispatcher",
    "build_slots",
    "build_steady_state_service",
    "run_edt_service",
]
