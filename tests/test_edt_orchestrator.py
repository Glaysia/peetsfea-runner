from __future__ import annotations

from peetsfea_runner.edt_orchestrator import JobHandle, JobLauncher, JobOrchestrator


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class FakeLauncher(JobLauncher):
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock
        self.submits = 0
        self.kills = 0
        self.alive: dict[str, bool] = {}

    def submit(self, job_index: int) -> JobHandle:
        self.submits += 1
        sid = f"s-{job_index}-{self.submits}"
        self.alive[sid] = True
        started = self.clock.t if self.clock is not None else float(self.submits)
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=started)

    def is_alive(self, handle: JobHandle) -> bool:
        return self.alive.get(handle.slurm_id, False)

    def kill(self, handle: JobHandle) -> None:
        self.kills += 1
        self.alive[handle.slurm_id] = False


def test_ensure_running_fills_fixed_job_slots() -> None:
    launcher = FakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=10)
    orch.ensure_running()
    assert launcher.submits == 10
    assert orch.running_count() == 10
    # 멱등: 이미 차 있으면 추가 제출 없음.
    orch.ensure_running()
    assert launcher.submits == 10


def test_poll_restarts_dead_jobs_keeping_count() -> None:
    launcher = FakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=5)
    orch.ensure_running()
    # 잡 2개가 죽음(누수 아닌 사유) → poll이 그 슬롯만 재기동, 제어 목적 종료 없음.
    dead = orch.handles()[:2]
    for h in dead:
        launcher.alive[h.slurm_id] = False
    orch.poll()
    assert orch.running_count() == 5
    assert orch.restarts == 2
    assert launcher.kills == 0  # 죽은 잡을 또 kill하지 않음


def test_poll_replaces_jobs_past_max_lifetime() -> None:
    clock = FakeClock()
    launcher = FakeLauncher(clock)
    orch = JobOrchestrator(launcher=launcher, clock=clock, job_count=4, max_lifetime_seconds=100.0)
    orch.ensure_running()
    assert launcher.submits == 4
    # 수명 전: 교체 없음.
    clock.t = 99.0
    orch.poll()
    assert launcher.kills == 0 and launcher.submits == 4
    # 수명 경과: kill + 즉시 재채움(고정 잡 유지). running_count 보존.
    clock.t = 100.0
    orch.poll()
    assert launcher.kills == 4
    assert launcher.submits == 8
    assert orch.expiries == 4
    assert orch.running_count() == 4


def test_shutdown_kills_all() -> None:
    launcher = FakeLauncher()
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=6)
    orch.ensure_running()
    orch.shutdown()
    assert launcher.kills == 6
    assert orch.running_count() == 0
