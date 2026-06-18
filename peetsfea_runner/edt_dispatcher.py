"""슬롯 디스패처 — 대기 큐의 fixed toml을 슬롯에 순차 디스패치 (Phase 1).

각 슬롯(`EdtManager`)은 자기 스레드에서 큐를 당겨 `acquire` → peetsfea 프리미티브 실행
(edtmgr가 준 `grpc_port`에 접속) → 결과 기록 → `release` 를 반복한다. 컨테이너 안에서 슬롯 10개가
동시에 돌아 항상 N개 시뮬이 진행된다.

타이밍: 프리미티브는 60분에 스스로 abort(마지막 패스 리포트)하고, 그 위에 edtmgr 백스톱이
65분(`backstop_seconds`) 미반환 시 ansysedt를 SIGKILL+재기동한다. 여기서는 프리미티브를
슬롯별 단일 워커 executor에 제출해 `backstop_seconds` 타임아웃으로 백스톱을 강제한다.
"""

from __future__ import annotations

import hashlib
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import EDTMGR_BACKSTOP_KILL_SECONDS, SIM_HARD_ABORT_SECONDS
from .edt_load import AdmissionController
from .edtmgr import EdtManager
from .edt_queue import QueueItem, QueueLike

# (candidate_toml_text, *, output_dir, seed, mode, grpc_port, aedt_pid) -> result mapping
SimulationPrimitive = Callable[..., Mapping[str, Any]]
VersionLoader = Callable[[], str]


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _default_version_loader() -> str:
    import peetsfea

    return str(peetsfea.__version__)


@dataclass
class SlotDispatcher:
    slots: list[EdtManager]
    queue: QueueLike
    primitive: SimulationPrimitive
    output_root: Path
    record: Callable[[Mapping[str, Any]], None]
    account_id: str = "account_01"
    host_alias: str = "gate1-harry261"
    partition: str = ""  # 잡이 떠 있는 SLURM 파티션(자동 벤치마크용 기록)
    node: str = ""  # 잡이 떠 있는 노드 hostname
    version_loader: VersionLoader = _default_version_loader
    backstop_seconds: float = float(EDTMGR_BACKSTOP_KILL_SECONDS)
    # peetsfea가 스스로 마지막 패스 리포트를 남기고 abort할 한도(90분). backstop(98분)보다 짧아야 함.
    solve_hard_abort_seconds: float = float(SIM_HARD_ABORT_SECONDS)
    now_iso: Callable[[], str] = _utc_now_iso
    drain: bool = True
    idle_sleep_seconds: float = 1.0
    # 로드밸런서(Phase 3): None이면 게이트 없음(Phase 1 동작). 설정하면 새 시뮬 시작을 부하로 게이팅(ramp-up).
    admission: AdmissionController | None = None
    admission_poll_seconds: float = 1.0
    # 라이선스 제어(전역): None이면 게이트 없음. 설정하면 솔브 직전 permit 획득(상한 100), 솔브 중 heartbeat로
    # abort 지령(150 초과 시 youngest kill) 수신. acquire()/release()/heartbeat(started_epoch)를 가진 클라이언트.
    permit_client: Any = None
    heartbeat_seconds: float = 20.0
    # 연속 솔브 실패 시 슬롯 AEDT를 새로 띄운다(손상된 warm 세션 재사용 → 실패 폭주 차단). 1회 실패는 reclaim(싸게),
    # 연속 N회면 force_restart로 깨끗한 세션. project_name=None·STEP import 빈결과 등 세션 손상이 fleet 동시 폭주를
    # 일으켜 동시 solve(=라이선스)가 급락하던 문제의 근본 대응. (recover()가 살아있는 손상 데스크톱을 재부착하던 게 원인.)
    force_restart_after_failures: int = 2
    _fail_streak: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _processed: int = field(default=0, init=False)
    _processed_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.output_root = Path(self.output_root).expanduser().resolve()
        self.output_root.mkdir(parents=True, exist_ok=True)

    @property
    def processed(self) -> int:
        with self._processed_lock:
            return self._processed

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        """모든 슬롯 스레드를 띄워 큐를 처리한다. 반환: 처리한 건수.

        `drain=True`면 큐가 비는 즉시 각 슬롯이 종료한다(Phase 1 수용 테스트).
        `drain=False`면 stop() 전까지 idle 폴링하며 대기한다.
        """
        threads = [
            threading.Thread(target=self._slot_loop, args=(slot,), name=f"edt-{slot.slot_id}", daemon=True)
            for slot in self.slots
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return self.processed

    def _slot_loop(self, slot: EdtManager) -> None:
        # 슬롯 시작 시 ansysedt를 미리 warm으로 띄워 둔다(상시 기동).
        slot.ensure_warm()
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"sim-{slot.slot_id}") as executor:
            while not self._stop.is_set():
                item = self.queue.get()
                if item is None:
                    if self.drain:
                        return
                    self._stop.wait(self.idle_sleep_seconds)
                    continue
                # ramp-up 게이트: (라이선스 permit AND CPU 여유)일 때까지 새 시뮬 시작을 보류(item은 손에 쥔 채).
                if not self._await_admission():
                    break
                try:
                    envelope = self._run_one(slot, item, executor)
                finally:
                    if self.permit_client is not None:
                        self.permit_client.release()  # 솔브 1건 끝 → 라이선스 자리 반납
                self._safe_record(envelope)
                with self._processed_lock:
                    self._processed += 1

    def _await_admission(self) -> bool:
        """(라이선스 permit AND CPU admission) 둘 다일 때 승인. permit은 전역 상한, CPU는 로컬 로드밸런싱.

        permit을 먼저 잡고 CPU가 아직이면 permit을 반납하고 재시도(permit 누수 방지). stop 시 False.
        """
        while not self._stop.is_set():
            # 1) 라이선스 permit (전역 상한 100)
            if self.permit_client is not None and not self.permit_client.acquire():
                self._stop.wait(self.admission_poll_seconds)
                continue
            # 2) CPU 로드밸런싱 (로컬 — 그대로 유지)
            if self.admission is not None and not self.admission.can_admit():
                if self.permit_client is not None:
                    self.permit_client.release()  # CPU 아직 → 잡은 permit 반납
                self._stop.wait(self.admission_poll_seconds)
                continue
            return True
        if self.permit_client is not None:
            self.permit_client.release()  # stop: 혹시 잡은 permit 반납(멱등)
        return False

    def _run_one(self, slot: EdtManager, item: QueueItem, executor: ThreadPoolExecutor) -> dict[str, Any]:
        grant = slot.acquire()
        started_at = self.now_iso()
        started_epoch = time.time()
        job_output_dir = self.output_root / item.request_id
        future = executor.submit(
            self.primitive,
            item.candidate_toml_text,
            output_dir=job_output_dir,
            seed=item.seed,
            mode=item.mode,
            grpc_port=grant.grpc_port,
            aedt_pid=grant.pid,
            solve_hard_abort_seconds=self.solve_hard_abort_seconds,
        )
        # 라이선스 제어 watcher: 솔브 중 heartbeat → abort 지령(youngest kill) 시 AEDT를 죽여 솔브 중단.
        aborted = {"v": False}
        watch_stop = threading.Event()
        watcher: threading.Thread | None = None
        if self.permit_client is not None:
            watcher = threading.Thread(
                target=self._heartbeat_watch, args=(slot, started_epoch, watch_stop, aborted), daemon=True
            )
            watcher.start()
        try:
            result = future.result(timeout=self.backstop_seconds)
        except FutureTimeoutError:
            # 65분 백스톱: 미반환 시뮬을 강제 종료하고 슬롯 재기동.
            slot.force_restart()
            self._fail_streak[slot.slot_id] = 0  # 새 AEDT 세션 — 스트릭 리셋
            return self._envelope(
                item,
                slot,
                grant.grpc_port,
                started_at,
                terminal_state="aborted",
                error={"stage": "backstop", "type": "BackstopTimeout", "message": f"sim exceeded {self.backstop_seconds:.0f}s"},
            )
        except Exception as exc:  # 시뮬 실패(또는 라이선스 abort로 AEDT kill): 살아있으면 재부착, 아니면 재기동.
            if aborted["v"]:
                # 제어기가 AEDT를 죽인 abort: 손상 아님(프로세스 dead면 recover가 알아서 재기동). 스트릭 리셋.
                slot.recover()
                self._fail_streak[slot.slot_id] = 0
                return self._envelope(
                    item, slot, grant.grpc_port, started_at, terminal_state="aborted",
                    error={"stage": "license_abort", "type": "LicenseAbort", "message": "killed by license controller (youngest, lic>ceiling)"},
                )
            # 진짜 솔브 실패. 살아있는 손상 세션을 재부착하면 다음 솔브도 실패(폭주) → 연속 N회면 새 AEDT로 치유.
            streak = self._fail_streak.get(slot.slot_id, 0) + 1
            if streak >= self.force_restart_after_failures:
                slot.force_restart()
                self._fail_streak[slot.slot_id] = 0
            else:
                slot.recover()
                self._fail_streak[slot.slot_id] = streak
            return self._envelope(
                item,
                slot,
                grant.grpc_port,
                started_at,
                terminal_state="failed",
                error={"stage": "simulate", "type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()},
            )
        else:
            slot.release()
            self._fail_streak[slot.slot_id] = 0  # 성공 → 스트릭 리셋
            return self._envelope(item, slot, grant.grpc_port, started_at, terminal_state="success", result=result)
        finally:
            watch_stop.set()

    def _heartbeat_watch(
        self, slot: EdtManager, started_epoch: float, watch_stop: threading.Event, aborted: dict[str, bool]
    ) -> None:
        """솔브 중 제어기에 heartbeat. abort 지령이면 AEDT를 죽여(grpc 끊김) 솔브를 중단시킨다."""
        while not watch_stop.is_set() and not self._stop.is_set():
            try:
                if self.permit_client is not None and self.permit_client.heartbeat(started_epoch):
                    aborted["v"] = True
                    slot.backend.kill()  # AEDT 죽임 → primitive grpc 실패 → future 예외 → except에서 'aborted' 기록
                    return
            except Exception:  # noqa: BLE001 — watcher가 솔브를 죽이면 안 된다.
                pass
            if watch_stop.wait(self.heartbeat_seconds):
                return

    def _envelope(
        self,
        item: QueueItem,
        slot: EdtManager,
        grpc_port: int,
        started_at: str,
        *,
        terminal_state: str,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "request_id": item.request_id,
            "terminal_state": terminal_state,
            "started_at": started_at,
            "finished_at": self.now_iso(),
            "account_id": self.account_id,
            "host_alias": self.host_alias,
            "partition": self.partition,
            "node": self.node,
            "remote_job_id": "",
            "api_session_id": slot.slot_id,
            "slot_id": slot.slot_id,
            "grpc_port": grpc_port,
            "input_toml_hash": item.input_toml_hash(),
            "peetsfea_version": self._safe_version(),
            "mode": item.mode,
            "seed": item.seed,
            "output_dir": str(self.output_root / item.request_id),
            "result": _jsonable(result) if result is not None else {},
            **({"error": dict(error)} if error is not None else {}),
        }

    def _safe_version(self) -> str:
        try:
            return self.version_loader()
        except Exception:
            return ""

    def _safe_record(self, envelope: Mapping[str, Any]) -> None:
        try:
            self.record(envelope)
        except Exception:
            # 기록 실패가 슬롯 루프를 죽이면 안 된다.
            pass


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


__all__ = ["SimulationPrimitive", "SlotDispatcher", "VersionLoader"]
