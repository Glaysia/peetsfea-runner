"""9잡 오케스트레이터 (Phase 2, MASTER_PLAN §1/§4).

단일 계정에서 **9개 SLURM 잡**(= enroot 컨테이너 = 슬롯 서비스)을 상시 유지한다.
- 죽은 잡은 즉시 재기동.
- **10h 만료** 잡은 진행 중 시뮬을 **그냥 폐기**(드레인 없음, Q8)하고 재기동.
상태기계/타이밍만 담고, 실제 sbatch/squeue/scancel는 `JobLauncher`로 분리(테스트는 fake,
실서비스는 `scheduler.py`의 sbatch 패턴을 쓰는 SLURM 런처).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from .constants import JOB_MAX_LIFETIME_SECONDS, JOBS_PER_ACCOUNT

Clock = Callable[[], float]

# 막힌 PENDING 판정용. squeue REASON이 이 집합이면 "곧 시작/스케줄 중" → 계속 대기.
# 그 외(Resources, Priority, ReqNodeNotAvail 등)는 그 노드가 한동안 안 비는 것 → 취소하고 다른 노드로.
_OK_PENDING_REASONS = frozenset({"", "none", "null", "(null)", "pending"})


def _pending_is_stuck(reason: str) -> bool:
    return reason.strip().lower() not in _OK_PENDING_REASONS


@dataclass(frozen=True, slots=True)
class JobHandle:
    """제출된 잡 1개의 핸들."""

    job_index: int
    slurm_id: str
    started_at: float
    node: str = ""  # node_based 제출 시 핀한 노드(막힌 PENDING 취소 후 그 노드를 회피하는 데 사용).


class JobLauncher(Protocol):
    """잡 1개의 제출/생존/종료. 구현체가 컨테이너 안에서 슬롯 서비스를 돌린다."""

    def submit(self, job_index: int) -> JobHandle: ...
    def is_alive(self, handle: JobHandle) -> bool: ...
    def kill(self, handle: JobHandle) -> None: ...
    # 선택: 순차 램프(sequential_ramp)는 `is_running`(RUNNING/PENDING 구분)을 쓴다. 구현이 없으면 is_alive로
    # 폴백하므로 Protocol에 필수로 선언하지 않는다(스텁 상속 시 None 반환 폴백이 깨지는 것을 피함).


@dataclass
class JobOrchestrator:
    """계정 내 9잡을 상시 유지. `ensure_running()`으로 부팅, `poll()`을 주기 호출."""

    launcher: JobLauncher
    clock: Clock
    job_count: int = JOBS_PER_ACCOUNT
    max_lifetime_seconds: float = float(JOB_MAX_LIFETIME_SECONDS)
    # sequential_ramp=True면 **한 번에 한 잡씩**: 직전 잡이 RUNNING 된 뒤에야 다음 잡을 제출(노드 과적재·동시
    # 콜드스타트 폭주 방지). poll/ensure_running 호출당 최대 1개 제출, 가용 노드 없으면 그냥 대기.
    sequential_ramp: bool = False
    submitted: int = field(default=0, init=False)
    restarts: int = field(default=0, init=False)  # 죽어서 재기동한 횟수
    expiries: int = field(default=0, init=False)  # 10h 만료로 폐기·재기동한 횟수
    cancellations: int = field(default=0, init=False)  # 막힌 PENDING(Resources/Priority 등) 취소 후 다른 노드로 넘긴 횟수
    _jobs: dict[int, JobHandle] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def _submit(self, job_index: int) -> None:
        self._jobs[job_index] = self.launcher.submit(job_index)
        self.submitted += 1

    def ensure_running(self) -> None:
        """비어 있는 잡 슬롯을 채운다(콜드 스타트/누락 보충)."""
        if self.sequential_ramp:
            self._ramp_poll()
            return
        with self._lock:
            for i in range(self.job_count):
                if i not in self._jobs:
                    self._submit(i)

    def poll(self) -> None:
        """전 잡 1회 점검: 죽었으면 재기동, 10h 만료면 폐기 후 재기동."""
        if self.sequential_ramp:
            self._ramp_poll()
            return
        with self._lock:
            now = self.clock()
            for i in range(self.job_count):
                handle = self._jobs.get(i)
                if handle is None:
                    self._submit(i)
                    continue
                if not self.launcher.is_alive(handle):
                    self._submit(i)
                    self.restarts += 1
                elif (now - handle.started_at) >= self.max_lifetime_seconds:
                    self.launcher.kill(handle)
                    self._submit(i)
                    self.expiries += 1

    def _is_running(self, handle: JobHandle) -> bool:
        fn = getattr(self.launcher, "is_running", None)
        return fn(handle) if callable(fn) else self.launcher.is_alive(handle)

    def _pending_reason(self, handle: JobHandle) -> str:
        fn = getattr(self.launcher, "pending_reason", None)
        return fn(handle) if callable(fn) else ""

    def _avoid_node(self, handle: JobHandle) -> None:
        fn = getattr(self.launcher, "avoid_node", None)
        node = getattr(handle, "node", "")
        if callable(fn) and node:
            fn(node)

    def _cancel_pending(self, handle: JobHandle) -> None:
        # PENDING 취소는 plain scancel(launcher.cancel). kill(--signal=TERM)은 PENDING엔 no-op이라 좀비로 남는다.
        fn = getattr(self.launcher, "cancel", None)
        (fn if callable(fn) else self.launcher.kill)(handle)

    def _ramp_poll(self) -> None:
        """순차 램프 1회: 죽은/만료/막힌 PENDING 정리 후, 대기 잡이 없고 목표 미달이면 **한 개만** 제출."""
        with self._lock:
            now = self.clock()
            for i in list(self._jobs):
                handle = self._jobs[i]
                if not self.launcher.is_alive(handle):
                    del self._jobs[i]
                    self.restarts += 1  # 죽음 → 다음 램프에서 재제출
                elif (now - handle.started_at) >= self.max_lifetime_seconds:
                    self.launcher.kill(handle)
                    del self._jobs[i]
                    self.expiries += 1
            # 막힌 PENDING 취소: REASON이 None/곧시작이 아니라 Resources/Priority 등이면 그 노드는 한동안 안 빈다
            # → 잡을 취소하고 그 노드를 회피 등록 → 다음 제출에서 다른 노드로 넘어간다.
            for i in list(self._jobs):
                handle = self._jobs[i]
                if self.launcher.is_alive(handle) and not self._is_running(handle):
                    if _pending_is_stuck(self._pending_reason(handle)):
                        self._cancel_pending(handle)  # plain scancel(PENDING은 TERM no-op)
                        self._avoid_node(handle)
                        del self._jobs[i]
                        self.cancellations += 1
            if len(self._jobs) >= self.job_count:
                return
            # 아직 (정상) RUNNING 대기 중인 잡이 있으면 그게 뜰 때까지 대기 — 한 번에 하나씩.
            if any(self.launcher.is_alive(h) and not self._is_running(h) for h in self._jobs.values()):
                return
            for i in range(self.job_count):
                if i not in self._jobs:
                    try:
                        self._submit(i)
                    except Exception:  # noqa: BLE001 — 가용 노드 없음 등: 이번 사이클은 건너뛰고 다음에 재시도.
                        pass
                    return

    def running_count(self) -> int:
        with self._lock:
            return sum(1 for h in self._jobs.values() if self.launcher.is_alive(h))

    def handles(self) -> list[JobHandle]:
        with self._lock:
            return [self._jobs[i] for i in sorted(self._jobs)]

    def shutdown(self) -> None:
        """모든 잡 종료(서비스 정지). 진행 중 시뮬은 폐기."""
        with self._lock:
            for handle in self._jobs.values():
                self.launcher.kill(handle)
            self._jobs.clear()


__all__ = ["Clock", "JobHandle", "JobLauncher", "JobOrchestrator"]
