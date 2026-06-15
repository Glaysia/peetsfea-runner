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


@dataclass(frozen=True, slots=True)
class JobHandle:
    """제출된 잡 1개의 핸들."""

    job_index: int
    slurm_id: str
    started_at: float


class JobLauncher(Protocol):
    """잡 1개의 제출/생존/종료. 구현체가 컨테이너 안에서 슬롯 서비스를 돌린다."""

    def submit(self, job_index: int) -> JobHandle: ...
    def is_alive(self, handle: JobHandle) -> bool: ...
    def kill(self, handle: JobHandle) -> None: ...


@dataclass
class JobOrchestrator:
    """계정 내 9잡을 상시 유지. `ensure_running()`으로 부팅, `poll()`을 주기 호출."""

    launcher: JobLauncher
    clock: Clock
    job_count: int = JOBS_PER_ACCOUNT
    max_lifetime_seconds: float = float(JOB_MAX_LIFETIME_SECONDS)
    submitted: int = field(default=0, init=False)
    restarts: int = field(default=0, init=False)  # 죽어서 재기동한 횟수
    expiries: int = field(default=0, init=False)  # 10h 만료로 폐기·재기동한 횟수
    _jobs: dict[int, JobHandle] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def _submit(self, job_index: int) -> None:
        self._jobs[job_index] = self.launcher.submit(job_index)
        self.submitted += 1

    def ensure_running(self) -> None:
        """비어 있는 잡 슬롯을 채운다(콜드 스타트/누락 보충)."""
        with self._lock:
            for i in range(self.job_count):
                if i not in self._jobs:
                    self._submit(i)

    def poll(self) -> None:
        """전 잡 1회 점검: 죽었으면 재기동, 10h 만료면 폐기 후 재기동."""
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
