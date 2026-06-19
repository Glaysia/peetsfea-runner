from __future__ import annotations

import time
from pathlib import Path

from peetsfea_runner.edt_control_plane import ControlPlaneConfig, build_control_plane
from peetsfea_runner.edt_intake import IntakeService
from peetsfea_runner.edt_orchestrator import JobHandle, JobLauncher


class FakeLauncher(JobLauncher):
    def __init__(self) -> None:
        self.submits = 0
        self.alive: dict[str, bool] = {}

    def submit(self, job_index: int) -> JobHandle:
        self.submits += 1
        sid = f"j{job_index}-{self.submits}"
        self.alive[sid] = True
        # 컨트롤플레인 오케스트레이터는 time.monotonic을 쓰므로 started_at도 현재 monotonic(만료 오판 방지).
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=time.monotonic())

    def is_alive(self, handle: JobHandle) -> bool:
        return self.alive.get(handle.slurm_id, False)

    def kill(self, handle: JobHandle) -> None:
        self.alive[handle.slurm_id] = False


def test_build_control_plane_wires_orchestrator_store_intake(tmp_path: Path) -> None:
    config = ControlPlaneConfig(db_path=tmp_path / "r.duckdb", archive_root=tmp_path / "arch", job_count=9)
    launcher = FakeLauncher()
    cp = build_control_plane(config, launcher=launcher)

    assert cp.orchestrator.job_count == 9
    assert isinstance(cp.intake, IntakeService)
    assert cp.dashboard_port == 8080 and cp.intake_port == 7875
    assert config.job_command.endswith("orchestrator.sh")

    # HTML 기준 관리 루프: 시작 시 4개를 요청하고, 2분 홀짝 tick으로 squeue 15를 넘기지 않는다.
    assert cp.orchestrator.sequential_ramp is True
    cp.orchestrator.control_period_seconds = 0.0
    cp.orchestrator.ensure_running()
    assert launcher.submits == 4
    for _ in range(8):
        cp.orchestrator.poll()
    assert cp.orchestrator.running_count() == 15

    # 죽은 잡은 정리되지만 출생은 tick 정책과 cap을 따른다.
    victim = cp.orchestrator.handles()[0]
    launcher.alive[victim.slurm_id] = False
    cp.orchestrator.poll()
    assert cp.orchestrator.restarts == 1
    assert cp.orchestrator.running_count() <= 15
    cp.orchestrator.shutdown()
