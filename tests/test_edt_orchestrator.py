from __future__ import annotations

from peetsfea_runner.edt_orchestrator import JobHandle, JobInfo, JobLauncher, JobOrchestrator


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
        self.fail_after: int | None = None

    def submit(self, job_index: int) -> JobHandle:
        if self.fail_after is not None and self.submits >= self.fail_after:
            raise RuntimeError("no available node")
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


def test_submit_failure_does_not_crash_or_drop_existing_jobs() -> None:
    launcher = FakeLauncher()
    launcher.fail_after = 3
    orch = JobOrchestrator(launcher=launcher, clock=FakeClock(), job_count=5)

    orch.ensure_running()

    assert launcher.submits == 3
    assert orch.running_count() == 3
    assert orch.submit_failures == 2
    assert "no available node" in orch.last_submit_error

    launcher.fail_after = None
    orch.poll()

    assert launcher.submits == 5
    assert orch.running_count() == 5


# ------------------------------------------------------------------ B2: 스냅샷 기반 reconcile


class SnapFake(JobLauncher):
    """list_active_by_index/cancel/avoid_node 지원 런처(키퍼 스냅샷 경로 검증용)."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.slurm: dict[str, dict] = {}  # slurm_id -> {index,state,node,reason}
        self.counter = 0
        self.next_node = "n200"
        self.cancelled: list[str] = []
        self.avoided: list[str] = []

    def add(self, sid: str, index: int, state: str, node: str = "", reason: str = "None") -> None:
        self.slurm[sid] = {"index": index, "state": state, "node": node, "reason": reason}

    def submit(self, job_index: int) -> JobHandle:
        self.counter += 1
        sid = f"{1000 + self.counter}"
        self.slurm[sid] = {"index": job_index, "state": "RUNNING", "node": self.next_node, "reason": "None"}
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=self.clock.t, node=self.next_node)

    def is_alive(self, handle: JobHandle) -> bool:
        return handle.slurm_id in self.slurm

    def kill(self, handle: JobHandle) -> None:
        self.slurm.pop(handle.slurm_id, None)

    def cancel(self, handle: JobHandle) -> None:
        self.cancelled.append(handle.slurm_id)
        self.slurm.pop(handle.slurm_id, None)

    def avoid_node(self, node: str) -> None:
        self.avoided.append(node)

    def list_active_by_index(self) -> dict[int, list[JobInfo]]:
        out: dict[int, list[JobInfo]] = {}
        for sid, j in self.slurm.items():
            node = j["node"] if j["state"] == "RUNNING" else ""  # PENDING은 %N 비어있음(실제 동작)
            out.setdefault(j["index"], []).append(JobInfo(sid, j["state"], node, j["reason"]))
        return out


def test_reconcile_adopts_existing_jobs_no_duplicate_submit() -> None:
    # 키퍼 재시작 직후: squeue엔 이미 잡이 있고 in-memory state는 비었다 → 채택(재제출 금지).
    clock = FakeClock()
    fake = SnapFake(clock)
    for i in range(3):
        fake.add(f"{2000 + i}", i, "RUNNING", node="n10")
    orch = JobOrchestrator(launcher=fake, clock=clock, job_count=3)
    orch.ensure_running()
    assert fake.counter == 0  # 새 제출 0
    assert orch.adopted == 3
    assert orch.running_count() == 3
    assert {h.slurm_id for h in orch.handles()} == {"2000", "2001", "2002"}


def test_reconcile_cancels_duplicate_jobs_per_slot() -> None:
    clock = FakeClock()
    fake = SnapFake(clock)
    fake.add("3000", 0, "RUNNING", node="n10")
    fake.add("3001", 0, "PENDING", reason="Resources")  # 같은 슬롯 중복(구버그 잔재)
    orch = JobOrchestrator(launcher=fake, clock=clock, job_count=1)
    orch.poll()
    assert orch.dedup_cancels == 1
    assert fake.cancelled == ["3001"]  # RUNNING 유지, PENDING 중복 취소
    assert orch.handles()[0].slurm_id == "3000"


def test_poll_replaces_stuck_pending_and_avoids_node() -> None:
    clock = FakeClock()
    fake = SnapFake(clock)
    fake.add("4000", 0, "PENDING", reason="ReqNodeNotAvail, UnavailableNodes:n113")
    orch = JobOrchestrator(launcher=fake, clock=clock, job_count=1)
    orch.poll()
    assert "n113" in fake.avoided  # 사유의 노드 회피
    assert "4000" in fake.cancelled  # 막힌 잡 취소
    assert orch.stuck_replacements == 1
    assert fake.counter == 1  # 다른 노드로 재제출
    assert orch.handles()[0].slurm_id != "4000"


def test_poll_leaves_normal_pending_until_stuck_threshold() -> None:
    clock = FakeClock()
    fake = SnapFake(clock)
    fake.add("5000", 0, "PENDING", reason="Resources")  # 정상 대기(클러스터 혼잡)
    orch = JobOrchestrator(launcher=fake, clock=clock, job_count=1, stuck_pending_seconds=600.0)
    orch.poll()  # 처음 PENDING 관측(t=0)
    assert orch.stuck_replacements == 0 and not fake.cancelled
    clock.t = 599.0
    orch.poll()
    assert orch.stuck_replacements == 0  # 임계 전: 그대로 대기
    clock.t = 600.0
    orch.poll()
    assert orch.stuck_replacements == 1 and "5000" in fake.cancelled  # 장기 정체 → 재배치
