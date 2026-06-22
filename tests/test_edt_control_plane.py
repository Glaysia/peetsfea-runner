from __future__ import annotations

import time
from pathlib import Path

from peetsfea_runner.edt_control_plane import AccountConfig, ControlPlaneConfig, build_control_plane
from peetsfea_runner.edt_toml_registry import TomlRegistryService
from peetsfea_runner.edt_orchestrator import JobHandle, JobLauncher
from peetsfea_runner.edt_slurm_launcher import SlurmJobLauncher


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

    # 단일계정(accounts 비움): 계정 1개로 합성, 레거시 동작 보존.
    assert len(cp.accounts) == 1
    orch = cp.accounts[0].orchestrator
    assert orch.job_count == 9
    assert isinstance(cp.toml_registry, TomlRegistryService)
    assert cp.dashboard_port == 8080 and cp.intake_port == 7875
    assert config.job_command.endswith("orchestrator.sh")

    # 잡은 고정 인프라: ensure_running이 job_count(9)개를 채우고, poll은 그 수를 유지한다(홀짝/cap 폐지).
    orch.ensure_running()
    assert launcher.submits == 9
    assert orch.running_count() == 9

    # 죽은 잡은 그 슬롯만 재기동 → 고정 9 유지.
    victim = orch.handles()[0]
    launcher.alive[victim.slurm_id] = False
    orch.poll()
    assert orch.restarts == 1
    assert orch.running_count() == 9
    orch.shutdown()


def test_build_control_plane_single_account_legacy_ports(tmp_path: Path) -> None:
    """accounts 비움 → 계정 1개로 합성, 레거시 단일 포트 유지."""
    config = ControlPlaneConfig(db_path=tmp_path / "r.duckdb", job_count=7)
    cp = build_control_plane(config, launcher=FakeLauncher())

    assert len(cp.accounts) == 1
    acct = cp.accounts[0]
    assert acct.account_id == "account_01"
    assert acct.host_alias == config.ssh_host
    assert acct.ingest_port == 7876
    assert acct.priority_lease_port == 7878
    assert acct.license_ctrl_port == 7879
    assert acct.resource_port == 7882
    assert acct.orchestrator.job_count == 7


def test_build_control_plane_two_accounts_distinct_ports(tmp_path: Path) -> None:
    """2계정 config → AccountRuntime 2개, 계정별 ingest/license/resource 포트 분리."""
    config = ControlPlaneConfig(
        db_path=tmp_path / "r.duckdb",
        accounts=(
            AccountConfig(
                account_id="harry261", host_alias="gate1-harry261", ssh_host="gate1-harry261",
                account_index=0, job_count=10,
                ingest_port=7876, priority_lease_port=7878, license_ctrl_port=7879, resource_port=7882,
            ),
            AccountConfig(
                account_id="hmlee31", host_alias="gate1-hmlee31", ssh_host="gate1-hmlee31",
                account_index=1, job_count=5,
                ingest_port=7886, priority_lease_port=7888, license_ctrl_port=7889, resource_port=7892,
            ),
        ),
    )
    cp = build_control_plane(config)

    assert len(cp.accounts) == 2
    a0, a1 = cp.accounts
    assert a0.account_id == "harry261" and a1.account_id == "hmlee31"
    # 포트가 계정별로 분리(같은 로컬 머신 공존).
    assert {a0.ingest_port, a1.ingest_port} == {7876, 7886}
    assert {a0.license_ctrl_port, a1.license_ctrl_port} == {7879, 7889}
    assert {a0.resource_port, a1.resource_port} == {7882, 7892}
    assert a0.priority_lease_port == 7878 and a1.priority_lease_port == 7888
    assert a0.orchestrator.job_count == 10 and a1.orchestrator.job_count == 5
    # 명시 launcher 없으면 계정마다 SlurmJobLauncher(계정 ssh_host/account_id 태깅).
    assert isinstance(a0.orchestrator.launcher, SlurmJobLauncher)
    assert a0.orchestrator.launcher.ssh_host == "gate1-harry261"
    assert a1.orchestrator.launcher.ssh_host == "gate1-hmlee31"


def test_account_launcher_emits_account_id_in_sbatch() -> None:
    """각 계정 런처의 sbatch 스크립트에 EDT_ACCOUNT_ID=<id>가 들어간다(단일 DB 태깅)."""
    launcher = SlurmJobLauncher(
        ssh_host="gate1-hmlee31", account_id="hmlee31", host_alias="gate1-hmlee31",
    )
    script = launcher._sbatch_script(0, "cpu2", 64)
    assert "export EDT_ACCOUNT_ID=hmlee31\n" in script
    assert "export EDT_HOST_ALIAS=gate1-hmlee31\n" in script
