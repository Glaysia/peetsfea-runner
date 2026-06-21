"""SLURM 잡 인프라 유지 루프.

잡은 **고정 인프라**다 — 제어 목적으로 죽이지 않는다. 누수 회수는 컨테이너 PID-ns 격리가 전담하고
(`PLANS/leak_reclaim_test.html`), 처리량 제어는 `ContainerScheduler`의 적분제어가 컨테이너 수로
actuate한다(`PLANS/integral_container_control.html`). 여기서는 job_count개 잡을 살아있게 유지하고,
죽으면 재기동, max_lifetime 경과 시 교체(드리프트/잔류 방지)만 한다.

구 정책(2분 홀짝 submit4/kill1, solve→N LUT, 가장-늙은-잡 종료, squeue-stuck 회복, 12분 포화제어)은
모두 폐지됐다 — 제어가 잡-죽임을 겸직하던 구조가 통째 출렁임(리플)을 만들었기 때문.
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
    node: str = ""  # 제출 시 핀한 노드(런처가 노드 회피 등에 사용).


class JobLauncher(Protocol):
    """잡 1개의 제출/생존/종료. 구현체가 컨테이너 안에서 슬롯 서비스를 돌린다."""

    def submit(self, job_index: int) -> JobHandle: ...
    def is_alive(self, handle: JobHandle) -> bool: ...
    def kill(self, handle: JobHandle) -> None: ...


@dataclass
class JobOrchestrator:
    """계정 내 고정 SLURM 잡(인프라)을 안정 유지한다.

    잡은 제어 목적으로 죽이지 않는다 — 처리량은 ContainerScheduler 적분제어가 컨테이너 수로 조절한다.
    여기선 job_count개를 살려두고(죽으면 재기동), max_lifetime 경과 잡만 교체한다(드리프트/잔류 방지).
    """

    launcher: JobLauncher
    clock: Clock
    job_count: int = JOBS_PER_ACCOUNT
    max_lifetime_seconds: float = float(JOB_MAX_LIFETIME_SECONDS)
    # 관측/상태용(제어 자체는 ContainerScheduler가 담당). control_plane이 주입.
    solve_provider: Callable[[], int] | None = None
    submitted: int = field(default=0, init=False)
    restarts: int = field(default=0, init=False)  # 죽어서 재기동한 횟수
    expiries: int = field(default=0, init=False)  # max_lifetime 만료로 교체한 횟수
    _jobs: dict[int, JobHandle] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def _submit(self, job_index: int) -> None:
        self._jobs[job_index] = self.launcher.submit(job_index)
        self.submitted += 1

    def ensure_running(self) -> None:
        """초기 기동: job_count개 잡 슬롯을 채운다."""
        with self._lock:
            for i in range(self.job_count):
                if i not in self._jobs:
                    self._submit(i)

    def poll(self) -> None:
        """고정 잡 유지: 빈/죽은 슬롯 재기동, max_lifetime 경과 잡 교체(즉시 재채움)."""
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
