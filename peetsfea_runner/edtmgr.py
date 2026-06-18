"""edtmgr — ansysedt warm 유지 + 라이선스 점유 + 대여 프로토콜 (Phase 1).

ansysedt는 켜고 끄는 비용이 크고 끄면 EDT 라이선스를 놓친다. 그래서 edtmgr는 별도 시뮬이
아니라 **관리용 pyaedt 세션**을 상시 물고 있어 (1) ansysedt를 warm 유지하고 (2) 라이선스
점유를 유지한다. 실제 시뮬이 붙을 때만 잠깐 release(`acquire`)해 `(pid, grpc_port)`를 넘기고,
끝나면 다시 붙인다(`release`).

이 모듈은 **상태기계와 타이밍 로직만** 담고, ansysedt/pyaedt 실제 조작은 `EdtBackend`
프로토콜 뒤로 분리한다(테스트는 fake backend, 실서비스는 `edt_aedt_backend.RealEdtBackend`).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from .constants import EDTMGR_BACKSTOP_KILL_SECONDS

# 단조(monotonic) 초 단위 시계. 테스트에서 가짜 시계를 주입할 수 있게 분리한다.
Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class AedtSession:
    """edtmgr가 warm으로 유지하는 ansysedt의 좌표."""

    pid: int
    grpc_port: int


class SlotState(Enum):
    IDLE = "idle"  # ansysedt 없음
    WARM = "warm"  # ansysedt 기동 + 관리 세션 부착, 대여 대기
    LENT = "lent"  # 시뮬에 대여됨


class EdtManagerError(RuntimeError):
    """edtmgr 상태 전이 위반."""


class EdtBackend:
    """한 슬롯의 ansysedt 하나를 소유하는 어댑터(stateful) 인터페이스.

    구현체는 ansysedt를 `-ng -grpcsrv <port>`로 띄우고 관리 pyaedt 세션을
    `close_on_exit=False`로 붙여 둔다. 본 클래스는 타입/문서용 베이스이며,
    `RealEdtBackend`(실 AEDT)과 테스트용 fake가 이를 구현한다.
    """

    def start(self) -> AedtSession:
        """ansysedt 기동 + 관리 세션 부착. 좌표 반환."""
        raise NotImplementedError

    def lend(self) -> AedtSession:
        """관리 세션 점유만 잠깐 놓고(ansysedt는 살림) 좌표 반환."""
        raise NotImplementedError

    def reclaim(self) -> None:
        """관리 세션을 다시 부착."""
        raise NotImplementedError

    def is_alive(self) -> bool:
        """현재 ansysedt 프로세스 생존 여부."""
        raise NotImplementedError

    def kill(self) -> None:
        """현재 ansysedt(+관리 세션) SIGKILL."""
        raise NotImplementedError


@dataclass
class EdtManager:
    """슬롯 하나의 edtmgr: ansysedt warm 유지 + 대여(acquire/release) + 65분 백스톱."""

    backend: EdtBackend
    clock: Clock
    slot_id: str = "slot"
    backstop_seconds: float = float(EDTMGR_BACKSTOP_KILL_SECONDS)
    state: SlotState = field(default=SlotState.IDLE)
    _session: AedtSession | None = field(default=None, init=False, repr=False)
    _lent_at: float | None = field(default=None, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    @property
    def session(self) -> AedtSession | None:
        return self._session

    def ensure_warm(self) -> AedtSession:
        """warm 보장. 없거나 죽었으면 (재)기동."""
        with self._lock:
            if self.state is SlotState.LENT:
                raise EdtManagerError(f"{self.slot_id}: cannot warm while lent")
            if self.state is SlotState.WARM and self._session is not None and self.backend.is_alive():
                return self._session
            session = self.backend.start()
            self._session = session
            self.state = SlotState.WARM
            return session

    def acquire(self) -> AedtSession:
        """warm 보장 후 대여. `(pid, grpc_port)` 반환."""
        with self._lock:
            if self.state is SlotState.LENT:
                raise EdtManagerError(f"{self.slot_id}: already lent")
            self.ensure_warm()
            session = self.backend.lend()
            self._session = session
            self._lent_at = self.clock()
            self.state = SlotState.LENT
            return session

    def release(self) -> None:
        """정상 완료 반환: 사용한 ansysedt를 **죽이고 새 깨끗한 세션을 띄워 둔다(1솔브=1AEDT)**.

        원래는 reclaim(관리세션 재부착)으로 warm 재사용해 콜드스타트(~분)를 아꼈으나, AEDT는 close_project로
        점유 메모리를 OS에 반환하지 않아(프로세스 종료 시에만 해제) 재사용할수록 누수가 누적된다 →
        메모리 압박 → 할당 실패(geometry import 빈 결과·project_name=None) → 손상 세션 재사용 실패 폭주 →
        동시 solve(라이선스) 급락. 매 솔브마다 새 프로세스로 교체해 누수·세션 손상을 **구조적으로 차단**한다.
        (대여 중이 아니면 무시. ensure_warm으로 띄운 새 AEDT가 다음 요청까지 idle 대기.)"""
        with self._lock:
            if self.state is not SlotState.LENT:
                return
            self.force_restart()

    def force_restart(self) -> AedtSession:
        """현재 ansysedt를 SIGKILL하고 다시 warm 기동(새 프로세스 = 누수/손상 0에서 시작)."""
        with self._lock:
            try:
                self.backend.kill()
            finally:
                self._session = None
                self._lent_at = None
                self.state = SlotState.IDLE
            return self.ensure_warm()

    def recover(self) -> None:
        """실패/이상 반환: 사용한(손상·누수 가능) ansysedt를 죽이고 새 세션으로 교체. release와 동일 정책."""
        with self._lock:
            if self.state is not SlotState.LENT:
                return
            self.force_restart()

    def lease_age_seconds(self) -> float | None:
        """대여 경과 시간(초). 대여 중이 아니면 None."""
        with self._lock:
            if self.state is not SlotState.LENT or self._lent_at is None:
                return None
            return self.clock() - self._lent_at

    def check_liveness(self) -> bool:
        """대여 중이 아닌(WARM/IDLE) 슬롯의 ansysedt가 죽었으면 재기동. 재기동했으면 True.

        LENT 슬롯의 백스톱/사망은 디스패처가 소유하므로 워치독은 건드리지 않는다(경합 회피).
        """
        with self._lock:
            if self.state is SlotState.LENT:
                return False
            if self.state is SlotState.WARM and not self.backend.is_alive():
                self.force_restart()
                return True
            return False

    def poll(self) -> bool:
        """백스톱 + liveness 점검(워치독용). 강제 재기동했으면 True.

        - 대여한 채 `backstop_seconds` 초과 → SIGKILL + 재기동.
        - WARM/LENT인데 ansysedt가 죽었으면 → 재기동.
        """
        with self._lock:
            age = self.lease_age_seconds()
            if age is not None and age >= self.backstop_seconds:
                self.force_restart()
                return True
            if self.state in (SlotState.WARM, SlotState.LENT) and not self.backend.is_alive():
                self.force_restart()
                return True
            return False


__all__ = [
    "AedtSession",
    "Clock",
    "EdtBackend",
    "EdtManager",
    "EdtManagerError",
    "SlotState",
]
