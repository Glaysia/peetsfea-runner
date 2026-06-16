from __future__ import annotations

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
        return JobHandle(job_index=job_index, slurm_id=sid, started_at=0.0)

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

    # 오케스트레이터가 9잡을 띄운다(연속 가동 시작점).
    cp.orchestrator.ensure_running()
    assert launcher.submits == 9
    assert cp.orchestrator.running_count() == 9

    # 잡 1개 죽으면 poll에서 재기동.
    victim = cp.orchestrator.handles()[0]
    launcher.alive[victim.slurm_id] = False
    cp.orchestrator.poll()
    assert cp.orchestrator.restarts == 1
    assert cp.orchestrator.running_count() == 9
    cp.orchestrator.shutdown()
