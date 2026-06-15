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
