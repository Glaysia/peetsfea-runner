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
        self.cancels = 0
        self.alive: dict[str, bool] = {}
        self.running: dict[str, bool] = {}
        self.reason: dict[str, str] = {}
        self.avoided: list[str] = []

    def submit(self, job_index: int) -> JobHandle:
        self.submits += 1
        sid = f"s-{job_index}-{self.submits}"
        self.alive[sid] = True
        self.running[sid] = False
        self.reason[sid] = "None"
        started = self.clock.t + (self.submits * 0.001) if self.clock is not None else float(self.submits)
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=started, node=f"node{self.submits}")

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

    def cancel(self, handle: JobHandle) -> None:
        self.cancels += 1
        self.alive[handle.slurm_id] = False

    def mark_running(self) -> None:
        for sid, alive in self.alive.items():
            if alive:
                self.running[sid] = True


def _managed(clock: FakeClock | None = None, *, solve: int = 0) -> tuple[JobOrchestrator, FakeLauncher, FakeClock]:
    clk = clock or FakeClock()
    launcher = FakeLauncher(clk)
    orch = JobOrchestrator(
        launcher=launcher,
        clock=clk,
        job_count=9,
        sequential_ramp=True,
        solve_provider=lambda: solve,
    )
    return orch, launcher, clk


def test_legacy_keepalive_mode_still_fills_configured_slots() -> None:
    clock = FakeClock()
    launcher = FakeLauncher(clock)
    orch = JobOrchestrator(launcher=launcher, clock=clock, job_count=3)
    orch.ensure_running()
    assert launcher.submits == 3
    assert orch.running_count() == 3


def test_managed_startup_submits_four_jobs_immediately() -> None:
    orch, launcher, _ = _managed()
    orch.ensure_running()
    assert launcher.submits == 4
    assert launcher.kills == 0
    assert orch.running_count() == 4


def test_managed_loop_waits_until_four_minute_tick() -> None:
    orch, launcher, clock = _managed()
    orch.ensure_running()
    launcher.mark_running()
    clock.t = 239.0
    orch.poll()
    assert launcher.submits == 4
    assert launcher.kills == 0
    clock.t = 240.0
    orch.poll()
    assert launcher.kills == 1
    assert launcher.submits == 8
    assert orch.running_count() == 7


def test_managed_loop_caps_squeue_at_fifteen() -> None:
    orch, launcher, clock = _managed()
    orch.ensure_running()
    for tick in range(1, 6):
        launcher.mark_running()
        clock.t = tick * orch.control_period_seconds
        orch.poll()
    assert launcher.submits == 20
    assert orch.running_count() == 15


def test_managed_loop_cancels_non_none_pending_reason_every_poll() -> None:
    orch, launcher, clock = _managed()
    orch.ensure_running()
    stuck = orch.handles()[0]
    launcher.reason[stuck.slurm_id] = "Priority"

    clock.t = 10.0
    orch.poll()

    assert launcher.cancels == 1
    assert launcher.kills == 0
    assert stuck.node in launcher.avoided
    assert launcher.alive[stuck.slurm_id] is False
    assert launcher.submits == 4
    assert orch.running_count() == 3


def test_managed_loop_stuck_at_cap_replaces_one_job() -> None:
    orch, launcher, clock = _managed()
    orch.ensure_running()
    for tick in range(1, 5):
        launcher.mark_running()
        clock.t = tick * orch.control_period_seconds
        orch.poll()
    assert orch.running_count() == 15

    pending = orch.handles()[-1]
    launcher.running[pending.slurm_id] = False
    launcher.reason[pending.slurm_id] = "Resources"
    before_submits = launcher.submits
    before_kills = launcher.kills
    clock.t += orch.control_period_seconds
    orch.poll()

    assert launcher.kills == before_kills + 1
    assert launcher.cancels == 1
    assert launcher.submits == before_submits + 2
    assert orch.running_count() == 15


def test_managed_loop_saturation_every_third_tick_stops_extra_oldest_job() -> None:
    clock = FakeClock()
    holder = {"solve": 151}
    launcher = FakeLauncher(clock)
    orch = JobOrchestrator(
        launcher=launcher,
        clock=clock,
        job_count=9,
        sequential_ramp=True,
        solve_provider=lambda: holder["solve"],
    )
    orch.ensure_running()
    launcher.mark_running()
    clock.t = orch.control_period_seconds
    orch.poll()
    before_kills = launcher.kills
    launcher.mark_running()
    clock.t = 2 * orch.control_period_seconds
    orch.poll()
    assert launcher.kills == before_kills + 2
    assert orch.saturation_reductions == 1


def test_managed_loop_uses_oldest_running_job_as_victim() -> None:
    orch, launcher, clock = _managed()
    orch.ensure_running()
    launcher.mark_running()
    victim = orch.handles()[0]
    clock.t = orch.control_period_seconds
    orch.poll()
    assert launcher.alive[victim.slurm_id] is False


def test_managed_loop_ttl_kills_expired_jobs_without_immediate_refill() -> None:
    clock = FakeClock()
    launcher = FakeLauncher(clock)
    orch = JobOrchestrator(
        launcher=launcher,
        clock=clock,
        job_count=9,
        max_lifetime_seconds=10.0,
        sequential_ramp=True,
    )
    orch.ensure_running()
    launcher.mark_running()
    clock.t = 11.0
    orch.poll()
    assert launcher.kills == 4
    assert launcher.submits == 4
    assert orch.running_count() == 0
