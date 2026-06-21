"""적분(I) 컨테이너 제어 — 유효 AEDT를 지령(120)으로 수렴시키는 처리량 제어기.

설계: `PLANS/integral_container_control.html`. 누수 회수는 컨테이너 PID-ns 격리가 전담하므로
(`PLANS/leak_reclaim_test.html`) 이 제어기는 **처리량만** 담당한다. 잡은 고정 인프라(안 죽임)고,
actuation은 **총 컨테이너 수 N_total**을 매 tick 적분 피드백으로 ±소량 조정해 10잡에 분배하는 것이다.

왜 적분인가: 비례항만 쓰면 정상상태 옵셋이 남는다. 적분은 오차를 누적해 평균을 정확히 120으로 맞춘다.
왜 컨테이너 단위인가: 잡 단위(±12 AEDT)보다 분해능이 4~12배 미세(±1~3) → 리플 최소.

이 모듈은 **순수 제어 상태/산술만** 담는다(관측·actuation은 호출측이 주입). 단위테스트 가능.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .constants import JOBS_PER_ACCOUNT


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


@dataclass
class ContainerController:
    """총 컨테이너 수 N을 적분 피드백으로 제어. tick마다 유효 AEDT 관측을 받아 N을 갱신한다.

    파라미터는 전부 config(설계 문서 §6의 확정 대상). 기본값은 보수적 시작점.
    """

    target_aedt: int = 120          # 지령: 동시 솔브(elec_solve_hfss) 수
    ki: float = 0.4                 # 적분 게인 (0.3~0.5). 클수록 빠르지만 출렁임↑
    dn_max: int = 3                 # tick당 N 변화 상한(±) — 매끈한 출생률 유지
    n_min: int = 80                 # 컨테이너 수 하한
    n_max: int = 150                # 컨테이너 수 상한
    job_count: int = JOBS_PER_ACCOUNT  # 분배 대상 고정 잡 수(10)
    n_total: int = field(default=-1)   # 적분 상태(총 컨테이너). -1이면 target으로 초기화.

    def __post_init__(self) -> None:
        if self.n_total < 0:
            self.n_total = _clamp(self.target_aedt, self.n_min, self.n_max)

    def tick(self, effective_aedt: int) -> int:
        """유효 AEDT 1회 관측 → 적분 갱신 → 새 N_total 반환.

        err = 지령 - 관측. step = clamp(round(Ki·err), ±dn_max). N += step (n_min..n_max 클램프).
        """
        err = self.target_aedt - int(effective_aedt)
        step = _clamp(round(self.ki * err), -self.dn_max, self.dn_max)
        self.n_total = _clamp(self.n_total + step, self.n_min, self.n_max)
        return self.n_total

    def distribute(self) -> list[int]:
        """N_total을 job_count개 잡에 균등 분배(나머지는 앞 잡부터 라운드로빈). 합 = N_total."""
        if self.job_count <= 0:
            return []
        base, rem = divmod(self.n_total, self.job_count)
        return [base + (1 if i < rem else 0) for i in range(self.job_count)]

    def plan_for(self, job_index: int) -> int:
        """잡 i의 현재 컨테이너 목표(/job_plan 서빙용). 범위 밖 인덱스는 0."""
        dist = self.distribute()
        return dist[job_index] if 0 <= job_index < len(dist) else 0


__all__ = ["ContainerController"]
