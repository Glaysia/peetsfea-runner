"""SLURM 잡 출생 제어 루프.

현행 정책은 HTML 문서(`PLANS/job_birth_controller.html`)가 기준이다. 관리 루프는 잡 수 구간을 나누지
않고 4분마다 가장 오래된 RUNNING 잡을 하나 내린 뒤 새 잡 4개를 요청한다. 전체 squeue 상한은 15개이며,
과포화(solve > 150)는 12분마다 가장 오래된 RUNNING 잡 하나를 추가로 내리는 방식으로 감압한다.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from .constants import JOB_MAX_LIFETIME_SECONDS, JOBS_PER_ACCOUNT

Clock = Callable[[], float]

# 막힌 PENDING 판정용. squeue REASON이 이 집합이면 "곧 시작/스케줄 중"으로 본다.
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
    # 선택 API: 관리 루프는 `is_running`(RUNNING/PENDING 구분), `pending_reason`, `cancel`, `avoid_node`를
    # 구현체가 제공하면 사용한다. 없으면 is_alive/kill 기반으로 폴백한다.


@dataclass
class JobOrchestrator:
    """계정 내 SLURM 잡 출생/사망을 제어한다."""

    launcher: JobLauncher
    clock: Clock
    job_count: int = JOBS_PER_ACCOUNT
    max_lifetime_seconds: float = float(JOB_MAX_LIFETIME_SECONDS)
    # 기존 설정명 호환용. True면 HTML 기준 관리 루프를 사용한다.
    sequential_ramp: bool = False
    control_period_seconds: float = 240.0
    submit_batch_jobs: int = 4
    squeue_cap: int = 15
    running_target: int = 10
    pending_target: int = 5
    saturation_every_ticks: int = 3
    saturation_solve_ceiling: int = 150
    solve_provider: Callable[[], int] | None = None
    submitted: int = field(default=0, init=False)
    restarts: int = field(default=0, init=False)  # 죽어서 재기동한 횟수
    expiries: int = field(default=0, init=False)  # TTL 만료/제어 루프 종료로 폐기한 횟수
    cancellations: int = field(default=0, init=False)  # PENDING/비RUNNING 잡 취소 횟수
    saturation_reductions: int = field(default=0, init=False)
    _jobs: dict[int, JobHandle] = field(default_factory=dict, init=False, repr=False)
    _last_control: float = field(default=float("-inf"), init=False)
    _control_ticks: int = field(default=0, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def _submit(self, job_index: int) -> None:
        self._jobs[job_index] = self.launcher.submit(job_index)
        self.submitted += 1

    def _submit_empty_slot(self, *, limit: int | None = None) -> bool:
        """가장 낮은 빈 슬롯 1개에 새 잡 제출. 제출하면 True(가용 노드 없음 등 실패 시 False)."""
        upper = self.job_count if limit is None else max(self.job_count, limit)
        for i in range(upper):
            if i not in self._jobs:
                try:
                    self._submit(i)
                    return True
                except Exception:  # noqa: BLE001 — 가용 노드 없음 등: 다음 사이클 재시도.
                    return False
        return False

    def _submit_until(self, target: int, max_submissions: int) -> int:
        """target까지, 단 이번 tick에서는 max_submissions개까지만 제출한다."""
        submitted = 0
        while len(self._jobs) < target and submitted < max_submissions:
            if not self._submit_empty_slot(limit=target):
                break
            submitted += 1
        return submitted

    def ensure_running(self) -> None:
        """초기 기동. 관리 루프는 첫 tick을 즉시 실행해 새 잡 4개를 요청한다."""
        if self.sequential_ramp:
            self._managed_poll(force_tick=True)
            return
        with self._lock:
            for i in range(self.job_count):
                if i not in self._jobs:
                    self._submit(i)

    def poll(self) -> None:
        """전 잡 1회 점검. 관리 루프는 4분 tick에서만 출생률을 바꾼다."""
        if self.sequential_ramp:
            self._managed_poll()
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

    def _active_count(self) -> int:
        return len(self._jobs)

    def _running_items(self) -> list[tuple[int, JobHandle]]:
        return [
            (i, h) for i, h in self._jobs.items()
            if self.launcher.is_alive(h) and self._is_running(h)
        ]

    def _oldest_running(self) -> tuple[int, JobHandle] | None:
        running = self._running_items()
        if not running:
            return None
        return min(running, key=lambda ih: ih[1].started_at)

    def _stop_oldest_running(self) -> bool:
        victim = self._oldest_running()
        if victim is None:
            return False
        i, handle = victim
        self.launcher.kill(handle)
        del self._jobs[i]
        self.expiries += 1
        return True

    def _queue_is_stuck(self) -> bool:
        for handle in self._jobs.values():
            if self.launcher.is_alive(handle) and not self._is_running(handle):
                if _pending_is_stuck(self._pending_reason(handle)):
                    return True
        return False

    def _cancel_stuck_pending_jobs(self) -> int:
        """PENDING reason이 None 계열이 아니면 cap과 무관하게 즉시 큐에서 제거한다."""
        cancelled = 0
        for i in list(self._jobs):
            handle = self._jobs[i]
            if not self.launcher.is_alive(handle) or self._is_running(handle):
                continue
            if not _pending_is_stuck(self._pending_reason(handle)):
                continue
            self._cancel_pending(handle)
            self._avoid_node(handle)
            del self._jobs[i]
            self.cancellations += 1
            cancelled += 1
        return cancelled

    def _current_solve(self) -> int:
        if self.solve_provider is None:
            return 0
        try:
            return int(self.solve_provider())
        except Exception:  # noqa: BLE001 — solve 관측 실패가 제어 루프를 죽이면 안 된다.
            return 0

    def _cleanup_finished_and_expired(self, now: float) -> None:
        for i in list(self._jobs):
            handle = self._jobs[i]
            if not self.launcher.is_alive(handle):
                del self._jobs[i]
                self.restarts += 1
                continue
            if (now - handle.started_at) < self.max_lifetime_seconds:
                continue
            if self._is_running(handle):
                self.launcher.kill(handle)
            else:
                self._cancel_pending(handle)
                self.cancellations += 1
            del self._jobs[i]
            self.expiries += 1

    def _managed_poll(self, *, force_tick: bool = False) -> None:
        """HTML 기준 제어 1회. 4분마다 oldest stop + submit 4, squeue cap은 15."""
        with self._lock:
            now = self.clock()
            self._cleanup_finished_and_expired(now)
            self._cancel_stuck_pending_jobs()
            if not force_tick and (now - self._last_control) < self.control_period_seconds:
                return

            stuck_at_cap = self._active_count() >= self.squeue_cap and self._queue_is_stuck()
            if stuck_at_cap:
                self._stop_oldest_running()
                self._submit_until(self.squeue_cap, 1)
            else:
                self._stop_oldest_running()
                self._submit_until(self.squeue_cap, self.submit_batch_jobs)
            self._last_control = now
            self._control_ticks += 1

            if (
                self.saturation_every_ticks > 0
                and self._control_ticks % self.saturation_every_ticks == 0
                and self._current_solve() > self.saturation_solve_ceiling
            ):
                if self._stop_oldest_running():
                    self.saturation_reductions += 1

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
