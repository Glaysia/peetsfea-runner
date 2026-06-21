from __future__ import annotations

import time
from pathlib import Path

from peetsfea_runner.edt_control_plane import ControlPlaneConfig, build_control_plane
from peetsfea_runner.edt_toml_registry import TomlRegistryService
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
    config = ControlPlaneConfig(db_path=tmp_path / "r.duckdb", job_count=9)
    launcher = FakeLauncher()
    cp = build_control_plane(config, launcher=launcher)

    assert cp.orchestrator.job_count == 9
    assert isinstance(cp.toml_registry, TomlRegistryService)
    assert cp.dashboard_port == 8080 and cp.intake_port == 7875
    assert config.job_command.endswith("orchestrator.sh")

    # 잡은 고정 인프라: ensure_running이 job_count(9)개를 채우고, poll은 그 수를 유지한다(홀짝/cap 폐지).
    cp.orchestrator.ensure_running()
    assert launcher.submits == 9
    assert cp.orchestrator.running_count() == 9

    # 죽은 잡은 그 슬롯만 재기동 → 고정 9 유지.
    victim = cp.orchestrator.handles()[0]
    launcher.alive[victim.slurm_id] = False
    cp.orchestrator.poll()
    assert cp.orchestrator.restarts == 1
    assert cp.orchestrator.running_count() == 9
    cp.orchestrator.shutdown()
