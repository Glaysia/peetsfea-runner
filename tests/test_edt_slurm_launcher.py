from __future__ import annotations

import pytest

from peetsfea_runner.edt_orchestrator import JobHandle
from peetsfea_runner.edt_slurm_launcher import CommandResult, SlurmJobLauncher, SlurmLauncherError


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str | None]] = []
        self.responses: dict[str, CommandResult] = {}

    def __call__(self, argv: list[str], input_text: str | None = None) -> CommandResult:
        self.calls.append((argv, input_text))
        remote = argv[-1]
        for key, resp in self.responses.items():
            if key in remote:
                return resp
        return CommandResult(0, "", "")

    def remotes(self) -> list[str]:
        return [argv[-1] for argv, _ in self.calls]


def _launcher(runner: FakeRunner, partitions: tuple[str, ...] = ("cpu2",)) -> SlurmJobLauncher:
    return SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0, partitions=partitions)


def test_submit_parses_slurm_id_and_sends_sbatch_script() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 777\n", "")
    handle = _launcher(runner).submit(3)
    assert handle.slurm_id == "777" and handle.job_index == 3
    # sbatch에 스크립트가 stdin으로 전달되고 SBATCH 헤더/명령 포함.
    argv, script = runner.calls[0]
    assert argv[-1] == "sbatch" and script is not None
    assert "#SBATCH --partition=cpu2" in script
    assert "#SBATCH --cpus-per-task=64" in script  # cpu2 → 64코어(QOS 하드캡)
    assert "#SBATCH --mem=480G" in script
    assert "export EDT_JOB_INDEX=3" in script
    assert "export EDT_PARTITION=cpu2" in script
    assert "EDT_ORCH_SSHD_PORT" not in script  # 디버그 sshd 기본 비활성


def test_debug_sshd_port_injected_per_job_and_account() -> None:
    # 잡별 sshd 역터널: 게이트 포트 = base + stride*account + job_index. 결정적 → ssh -J로 그 잡 노드 진입.
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    launcher = SlurmJobLauncher(
        command_runner=runner, clock=lambda: 0.0,
        debug_sshd_base=7900, debug_account_stride=50, account_index=1,  # hmlee31
    )
    launcher.submit(3)
    _, script = runner.calls[0]
    # 게이트 포트(역터널 바인드)와 노드-로컬 sshd 포트 둘 다 주입, 각각 (계정×잡)별 유일.
    assert "export EDT_ORCH_SSHD_PORT=7953" in script   # gate  = 7900 + 50*1 + 3
    assert "export EDT_DEBUG_LOCAL_SSHD=2253" in script  # local = 2200 + 50*1 + 3
    assert launcher.debug_sshd_port(0) == 7950 and launcher.debug_local_sshd_port(0) == 2250
    assert launcher.debug_sshd_port(8) == 7958 and launcher.debug_local_sshd_port(8) == 2258
    # account 0(harry261)은 같은 잡이라도 다른 포트(같은 노드 co-locate 충돌 회피).
    l0 = SlurmJobLauncher(debug_sshd_base=7900, account_index=0)
    assert l0.debug_sshd_port(3) == 7903 and l0.debug_local_sshd_port(3) == 2203


def test_debug_sshd_disabled_by_default_returns_none() -> None:
    assert SlurmJobLauncher().debug_sshd_port(0) is None  # base=0 → 비활성
    assert SlurmJobLauncher().debug_local_sshd_port(0) is None


def test_seed_epoch_injected_from_provider() -> None:
    # 재램프 재탕 방지: 런처가 used-seed 프런티어+1을 EDT_BASELINE_SEED_EPOCH로 sbatch에 주입.
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0, seed_epoch_provider=lambda: 415000000)
    launcher.submit(0)
    assert "export EDT_BASELINE_SEED_EPOCH=415000001" in runner.calls[-1][1]  # type: ignore[operator]


def test_seed_epoch_defaults_zero_without_provider() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    _launcher(runner).submit(0)
    assert "export EDT_BASELINE_SEED_EPOCH=0" in runner.calls[-1][1]  # type: ignore[operator]


def test_seed_epoch_provider_failure_falls_back_to_zero() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    def boom() -> int:
        raise RuntimeError("store down")
    SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0, seed_epoch_provider=boom).submit(0)
    assert "export EDT_BASELINE_SEED_EPOCH=0" in runner.calls[-1][1]  # type: ignore[operator]


def test_cpus_per_partition_cpu2_64_other_24() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    # cpu2 → 64 (QOS 하드캡)
    _launcher(runner, partitions=("cpu2",)).submit(0)
    assert "#SBATCH --cpus-per-task=64" in runner.calls[-1][1]  # type: ignore[operator]
    # 그 외(gpu4) → 24
    _launcher(runner, partitions=("gpu4",)).submit(0)
    assert "#SBATCH --cpus-per-task=24" in runner.calls[-1][1]  # type: ignore[operator]
    assert "#SBATCH --partition=gpu4" in runner.calls[-1][1]  # type: ignore[operator]
    assert "#SBATCH --mem=384G" in runner.calls[-1][1]  # type: ignore[operator]


def test_gpu_partition_requests_gres_cpu2_does_not() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    # gpu* → --gres=gpu:1 요청 + EDT_GPU_COUNT=1 (백필 가능성 우선)
    _launcher(runner, partitions=("gpu1",)).submit(0)
    gpu_script = runner.calls[-1][1]
    assert "#SBATCH --gres=gpu:1" in gpu_script  # type: ignore[operator]
    assert "export EDT_GPU_COUNT=1" in gpu_script  # type: ignore[operator]
    # cpu2 → gres 없음(GPU 없는 노드), EDT_GPU_COUNT=0
    _launcher(runner, partitions=("cpu2",)).submit(0)
    cpu_script = runner.calls[-1][1]
    assert "--gres" not in cpu_script  # type: ignore[operator]
    assert "export EDT_GPU_COUNT=0" in cpu_script  # type: ignore[operator]


def test_random_partition_distribution() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    # 전 파티션을 chooser로 순회 — 분배가 파티션 인자로 반영되는지.
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0)
    seen = set()
    seq = iter(["cpu2", "gpu1", "gpu2", "gpu3"])
    launcher.partition_chooser = lambda parts: next(seq)
    for _ in range(4):
        launcher.submit(0)
        seen.add(runner.calls[-1][1].split("--partition=")[1].split("\n")[0])  # type: ignore[union-attr]
    assert {"cpu2", "gpu1", "gpu2", "gpu3"} <= seen  # chooser가 명시 지정하면 gpu도 제출 가능(메커니즘)
    # 기본 후보는 cpu2 전용으로 변경(gpu 노드는 GPU 미사용+코어 적어 느림 → 폐기).
    assert set(SlurmJobLauncher().partitions) == {"cpu2"}


def test_mem_override_applies_to_all_partitions_for_verify_scripts() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0, partitions=("gpu1",), mem="32G")
    launcher.submit(0)
    assert "#SBATCH --mem=32G" in runner.calls[-1][1]  # type: ignore[operator]


def test_submit_failure_raises() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(1, "", "sbatch: error")
    with pytest.raises(SlurmLauncherError):
        _launcher(runner).submit(0)


def test_submit_unparseable_raises() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "weird output", "")
    with pytest.raises(SlurmLauncherError):
        _launcher(runner).submit(0)


def test_is_alive_states() -> None:
    runner = FakeRunner()
    launcher = _launcher(runner)
    handle = JobHandle(job_index=0, slurm_id="42", started_at=0.0)

    runner.responses["squeue"] = CommandResult(0, "RUNNING\n", "")
    assert launcher.is_alive(handle) is True
    runner.responses["squeue"] = CommandResult(0, "PENDING\n", "")
    assert launcher.is_alive(handle) is True
    runner.responses["squeue"] = CommandResult(0, "", "")  # 큐에 없음 = 종료
    assert launcher.is_alive(handle) is False
    runner.responses["squeue"] = CommandResult(0, "COMPLETED\n", "")  # 비활성 상태
    assert launcher.is_alive(handle) is False


def test_kill_calls_scancel() -> None:
    runner = FakeRunner()
    launcher = _launcher(runner)
    launcher.kill(JobHandle(job_index=0, slurm_id="99", started_at=0.0))
    # graceful SIGTERM(자동 KILL 없음) → trap이 /enroot 청소. raw `scancel 99`(KILL 폴백) 아님.
    assert any("scancel --full --signal=TERM 99" in r for r in runner.remotes())


def test_cancel_uses_plain_scancel_for_pending() -> None:
    runner = FakeRunner()
    launcher = _launcher(runner)
    launcher.cancel(JobHandle(job_index=0, slurm_id="77", started_at=0.0))
    # PENDING 취소는 plain `scancel 77`(큐에서 제거). --signal=TERM은 PENDING엔 no-op이라 쓰면 안 됨.
    assert any(r == "scancel 77" for r in runner.remotes())
    assert not any("--signal=TERM 77" in r for r in runner.remotes())


# --- node_based 제출 (특정 노드 핀 + 빈 노드 발견 + 가중 파티션) ---------------------

def _node_runner(sinfo_out: str, *, busy: str = "", sbatch_id: str = "500") -> FakeRunner:
    runner = FakeRunner()
    runner.responses["sinfo"] = CommandResult(0, sinfo_out, "")
    runner.responses["squeue -h --me"] = CommandResult(0, busy, "")  # busy nodes
    runner.responses["sbatch"] = CommandResult(0, f"Submitted batch job {sbatch_id}\n", "")
    return runner


def _last_script(runner: FakeRunner) -> str:
    return [s for a, s in runner.calls if a[-1] == "sbatch" and s][-1]


def test_node_based_pins_nodelist_and_excludes_busy() -> None:
    sinfo = "n001 cpu2 idle\nn002 cpu2 idle\nn010 gpu1 idle\n"
    runner = _node_runner(sinfo, busy="n001\n")  # n001은 내 잡이 점유 → 제외
    launcher = SlurmJobLauncher(
        command_runner=runner, clock=lambda: 0.0, partitions=("cpu2", "gpu1"),
        node_based=True, cpu2_weight=1.0,  # cpu2 강제
    )
    handle = launcher.submit(2)
    assert handle.slurm_id == "500"
    script = _last_script(runner)
    assert "#SBATCH --nodelist=n002" in script  # n001 busy 제외 → 다음 idle cpu2 노드
    assert "#SBATCH --partition=cpu2" in script
    assert "--nodelist=n001" not in script


def test_node_based_weighted_cpu2_vs_gpu() -> None:
    sinfo = "n001 cpu2 idle\nn010 gpu1 idle\n"
    # rng<cpu2_weight → cpu2
    r1 = _node_runner(sinfo)
    SlurmJobLauncher(command_runner=r1, clock=lambda: 0.0, partitions=("cpu2", "gpu1"),
                     node_based=True, cpu2_weight=0.7, rng=lambda: 0.1).submit(0)
    assert "--nodelist=n001" in _last_script(r1) and "--partition=cpu2" in _last_script(r1)
    # rng>=cpu2_weight → gpu (gres 포함)
    r2 = _node_runner(sinfo)
    SlurmJobLauncher(command_runner=r2, clock=lambda: 0.0, partitions=("cpu2", "gpu1"),
                     node_based=True, cpu2_weight=0.7, rng=lambda: 0.9).submit(0)
    s2 = _last_script(r2)
    assert "--nodelist=n010" in s2 and "--partition=gpu1" in s2 and "--gres=gpu:1" in s2


def test_node_based_prefers_idle_over_mix() -> None:
    sinfo = "n001 cpu2 mix\nn002 cpu2 idle\n"  # idle 우선
    runner = _node_runner(sinfo)
    SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0, partitions=("cpu2",),
                     node_based=True, cpu2_weight=1.0).submit(0)
    assert "--nodelist=n002" in _last_script(runner)


def test_node_based_no_available_node_raises() -> None:
    runner = _node_runner("", busy="")  # sinfo 빈 결과 = 가용 노드 없음
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0,
                                partitions=("cpu2", "gpu1"), node_based=True)
    with pytest.raises(SlurmLauncherError):
        launcher.submit(0)


def test_node_based_all_busy_raises() -> None:
    runner = _node_runner("n001 cpu2 idle\n", busy="n001\n")  # 유일 후보가 busy
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0,
                                partitions=("cpu2",), node_based=True)
    with pytest.raises(SlurmLauncherError):
        launcher.submit(0)


def test_is_running_only_true_for_running() -> None:
    runner = FakeRunner()
    launcher = _launcher(runner)
    handle = JobHandle(job_index=0, slurm_id="42", started_at=0.0)
    runner.responses["squeue -j"] = CommandResult(0, "RUNNING\n", "")
    assert launcher.is_running(handle) is True
    runner.responses["squeue -j"] = CommandResult(0, "PENDING\n", "")
    assert launcher.is_running(handle) is False  # PENDING은 아직 아님
    assert launcher.is_alive(handle) is True     # 단 살아있음


def test_node_based_handle_records_node() -> None:
    runner = _node_runner("n001 cpu2 idle\n")
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0,
                                partitions=("cpu2",), node_based=True, cpu2_weight=1.0)
    handle = launcher.submit(0)
    assert handle.node == "n001"  # 핀한 노드가 핸들에 기록(취소 시 회피용)


def test_pending_reason_parses_squeue_r() -> None:
    runner = FakeRunner()
    runner.responses["-o %r"] = CommandResult(0, "Resources\n", "")
    launcher = _launcher(runner)
    assert launcher.pending_reason(JobHandle(0, "5", 0.0)) == "Resources"


def test_node_based_avoids_cancelled_node() -> None:
    runner = _node_runner("n001 cpu2 idle\nn002 cpu2 idle\n")
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0,
                                partitions=("cpu2",), node_based=True, cpu2_weight=1.0)
    h1 = launcher.submit(0)
    assert h1.node == "n001"
    launcher.avoid_node("n001")           # 막힌 PENDING으로 취소됐다고 가정
    h2 = launcher.submit(1)
    assert h2.node == "n002"              # n001 회피 → 다음 노드


def test_node_based_reserves_newly_submitted_node_for_burst_spread() -> None:
    runner = _node_runner("n001 cpu2 idle\nn002 cpu2 idle\n", busy="")
    launcher = SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0,
                                partitions=("cpu2",), node_based=True, cpu2_weight=1.0)
    h1 = launcher.submit(0)
    h2 = launcher.submit(1)
    assert (h1.node, h2.node) == ("n001", "n002")
