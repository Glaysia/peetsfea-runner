"""로드 텔레메트리 + admission control (Phase 3, MASTER_PLAN §2.3).

CPU·메모리 사용률을 주기 샘플링(EWMA 평활)하고, **새 시뮬을 지금 시작해도 되는지**를
admission control로 결정한다. 기법: watermark 하드 게이트 + AIMD(과부하 시 multiplicative
감소, 여유 시 additive 증가) + 시작 스태거(최소 간격). ramp-up은 이 게이트의 자연스러운 결과 —
부하 여유가 있을 때만 새 시뮬이 들어오므로 무거운 패스 구간이 시간축에 분산된다.

로직(상태기계/타이밍)만 담고, 실제 CPU/mem 측정은 `LoadSampler`로 분리(테스트는 fake, 실서비스는
`psutil_load_sampler`).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field

Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class LoadSample:
    """한 시점의 노드 부하(%). 0~100."""

    cpu_percent: float
    mem_percent: float


LoadSampler = Callable[[], LoadSample]


def psutil_load_sampler() -> LoadSample:
    """실 노드 부하(psutil). cpu_percent는 직전 호출 이후 평균이라 주기 호출 전제."""
    import psutil

    return LoadSample(
        cpu_percent=float(psutil.cpu_percent(interval=None)),
        mem_percent=float(psutil.virtual_memory().percent),
    )


@dataclass
class AdmissionController:
    """새 시뮬 시작 허용 여부 게이트. `can_admit()`가 True면 1건 시작을 승인(소비)한다."""

    load_sampler: LoadSampler
    clock: Clock
    cpu_high: float = 85.0  # watermark(%) — 넘으면 과부하
    mem_high: float = 85.0
    ewma_alpha: float = 0.3  # 0~1, 클수록 최근 가중
    min_start_interval_seconds: float = 2.0  # 시작 스태거(최소 간격)
    aimd_increase: float = 1.0  # 여유 시 budget += (additive)
    aimd_decrease: float = 0.5  # 과부하 시 budget *= (multiplicative)
    max_budget: float = 16.0  # budget 상한(컨테이너 슬롯 상한과 정렬)
    _ewma_cpu: float | None = field(default=None, init=False, repr=False)
    _ewma_mem: float | None = field(default=None, init=False, repr=False)
    _budget: float = field(default=1.0, init=False, repr=False)
    _last_admit: float | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _update_ewma(self, sample: LoadSample) -> tuple[float, float]:
        a = self.ewma_alpha
        self._ewma_cpu = sample.cpu_percent if self._ewma_cpu is None else a * sample.cpu_percent + (1 - a) * self._ewma_cpu
        self._ewma_mem = sample.mem_percent if self._ewma_mem is None else a * sample.mem_percent + (1 - a) * self._ewma_mem
        return self._ewma_cpu, self._ewma_mem

    @property
    def smoothed(self) -> LoadSample | None:
        with self._lock:
            if self._ewma_cpu is None or self._ewma_mem is None:
                return None
            return LoadSample(cpu_percent=self._ewma_cpu, mem_percent=self._ewma_mem)

    def can_admit(self) -> bool:
        with self._lock:
            cpu, mem = self._update_ewma(self.load_sampler())
            overloaded = cpu >= self.cpu_high or mem >= self.mem_high
            if overloaded:
                # AIMD: multiplicative decrease + 즉시 보류.
                self._budget = max(0.0, self._budget * self.aimd_decrease)
                return False
            # 여유: additive increase.
            self._budget = min(self.max_budget, self._budget + self.aimd_increase)
            # 시작 스태거.
            now = self.clock()
            if self._last_admit is not None and (now - self._last_admit) < self.min_start_interval_seconds:
                return False
            # AIMD 토큰: 1건 이상 모였을 때만 승인.
            if self._budget < 1.0:
                return False
            self._budget -= 1.0
            self._last_admit = now
            return True


__all__ = ["AdmissionController", "Clock", "LoadSample", "LoadSampler", "psutil_load_sampler"]
