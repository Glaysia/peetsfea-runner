from __future__ import annotations

from peetsfea_runner.edt_orchestrator import JobHandle, JobLauncher, JobOrchestrator


class FakeLauncher(JobLauncher):
    def __init__(self) -> None:
        self.submits = 0
        self.kills = 0
        self.alive: dict[str, bool] = {}
        self._clock_ref: FakeClock | None = None

    def bind_clock(self, clock: FakeClock) -> None:
        self._clock_ref = clock

    def submit(self, job_index: int) -> JobHandle:
        self.submits += 1
        sid = f"slurm-{job_index}-{self.submits}"
        self.alive[sid] = True
        started = self._clock_ref.t if self._clock_ref is not None else 0.0
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=started)

    def is_alive(self, handle: JobHandle) -> bool:
        return self.alive.get(handle.slurm_id, False)

    def kill(self, handle: JobHandle) -> None:
        self.kills += 1
        self.alive[handle.slurm_id] = False


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _orch(job_count: int = 9, lifetime: float = 36000.0) -> tuple[JobOrchestrator, FakeLauncher, FakeClock]:
    launcher = FakeLauncher()
    clock = FakeClock()
    launcher.bind_clock(clock)
    return JobOrchestrator(launcher=launcher, clock=clock, job_count=job_count, max_lifetime_seconds=lifetime), launcher, clock


def test_ensure_running_submits_all_jobs() -> None:
    orch, launcher, _ = _orch(job_count=9)
    orch.ensure_running()
    assert launcher.submits == 9
    assert orch.running_count() == 9
    orch.ensure_running()  # 이미 다 떠 있으면 추가 제출 없음
    assert launcher.submits == 9


def test_poll_restarts_dead_job() -> None:
    orch, launcher, _ = _orch(job_count=3)
    orch.ensure_running()
    dead = orch.handles()[1]
    launcher.alive[dead.slurm_id] = False  # 잡 1개 사망
    orch.poll()
    assert orch.restarts == 1
    assert launcher.submits == 4  # 3 + 재기동 1
    assert orch.running_count() == 3  # 다시 3개


def test_poll_expires_and_resubmits_after_lifetime() -> None:
    orch, launcher, clock = _orch(job_count=2, lifetime=36000.0)  # 10h
    orch.ensure_running()
    clock.t = 36000.0  # 10h 경과
    orch.poll()
    assert orch.expiries == 2  # 둘 다 만료 폐기
    assert launcher.kills == 2  # 폐기 시 kill(드레인 없음)
    assert launcher.submits == 4  # 2 + 재제출 2
    assert orch.running_count() == 2


def test_shutdown_kills_all() -> None:
    orch, launcher, _ = _orch(job_count=4)
    orch.ensure_running()
    orch.shutdown()
    assert launcher.kills == 4
    assert orch.running_count() == 0


# --- 순차 램프(sequential_ramp): 한 잡이 RUNNING 된 뒤에야 다음 제출 ---------------

class SeqFakeLauncher(JobLauncher):
    """RUNNING/PENDING을 구분하는 fake. 새 잡은 PENDING으로 시작, mark_running()으로 RUNNING 전환."""

    def __init__(self) -> None:
        self.alive: dict[str, bool] = {}
        self.running: dict[str, bool] = {}
        self.reason: dict[str, str] = {}
        self.submits = 0
        self.kills = 0
        self.fail_submit = False
        self.avoided: list[str] = []

    def submit(self, job_index: int) -> JobHandle:
        if self.fail_submit:
            raise RuntimeError("no available node")
        self.submits += 1
        sid = f"s-{job_index}-{self.submits}"
        self.alive[sid] = True
        self.running[sid] = False  # 처음엔 PENDING
        self.reason[sid] = "None"  # 곧 시작
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=0.0, node=f"node{self.submits}")

    def is_alive(self, handle: JobHandle) -> bool:
        return self.alive.get(handle.slurm_id, False)

    def is_running(self, handle: JobHandle) -> bool:
        return self.running.get(handle.slurm_id, False)

    def pending_reason(self, handle: JobHandle) -> str:
        return self.reason.get(handle.slurm_id, "")

    def avoid_node(self, node: str) -> None:
        self.avoided.append(node)

    def kill(self, handle: JobHandle) -> None:
        self.kills += 1
        self.alive[handle.slurm_id] = False

    def mark_running(self) -> None:
        for sid, alive in self.alive.items():
            if alive:
                self.running[sid] = True


def test_sequential_ramp_one_at_a_time() -> None:
    launcher = SeqFakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=3, sequential_ramp=True)
    orch.ensure_running()
    assert launcher.submits == 1  # 한 개만 제출
    orch.poll()
    assert launcher.submits == 1  # 직전 잡 아직 PENDING → 다음 제출 안 함
    launcher.mark_running()  # 잡0 RUNNING
    orch.poll()
    assert launcher.submits == 2  # 이제 다음 제출
    launcher.mark_running()
    orch.poll()
    assert launcher.submits == 3
    launcher.mark_running()
    orch.poll()
    assert launcher.submits == 3  # 목표 도달 → 더 제출 안 함
    assert orch.running_count() == 3


def test_sequential_ramp_waits_when_no_node() -> None:
    launcher = SeqFakeLauncher()
    launcher.fail_submit = True  # 가용 노드 없음(submit 예외)
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=2, sequential_ramp=True)
    orch.ensure_running()
    assert launcher.submits == 0 and orch.running_count() == 0  # 예외 삼키고 대기


def test_sequential_ramp_refills_after_death() -> None:
    launcher = SeqFakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=2, sequential_ramp=True)
    orch.ensure_running(); launcher.mark_running()
    orch.poll(); launcher.mark_running()
    orch.poll()
    assert orch.running_count() == 2
    dead = orch.handles()[0]
    launcher.alive[dead.slurm_id] = False  # 잡 1개 사망
    orch.poll()  # 죽은 잡 drop + 새로 1개 제출
    assert orch.restarts == 1
    assert launcher.submits == 3


def test_sequential_ramp_cancels_stuck_pending_and_moves_on() -> None:
    launcher = SeqFakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=2, sequential_ramp=True)
    orch.ensure_running()
    assert launcher.submits == 1
    stuck = orch.handles()[0]
    launcher.reason[stuck.slurm_id] = "Resources"  # 노드가 한동안 안 비는 막힌 PENDING
    orch.poll()  # 막힌 잡 취소 + 노드 회피 + 다른 노드로 재제출
    assert orch.cancellations == 1
    assert launcher.kills == 1
    assert stuck.node in launcher.avoided          # 취소한 노드 회피 등록
    assert launcher.submits == 2                    # 다음 노드로 새로 제출


def test_sequential_ramp_keeps_waiting_on_none_reason() -> None:
    launcher = SeqFakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=2, sequential_ramp=True)
    orch.ensure_running()
    orch.poll()  # reason='None'(곧 시작) → 취소 안 하고 대기
    assert orch.cancellations == 0
    assert launcher.submits == 1  # 직전 잡 대기 중 → 추가 제출 없음
