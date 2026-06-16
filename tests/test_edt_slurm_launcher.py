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
    assert "#SBATCH --cpus-per-task=48" in script  # cpu2 → 48코어
    assert "#SBATCH --mem=480G" in script
    assert "export EDT_JOB_INDEX=3" in script
    assert "export EDT_PARTITION=cpu2" in script


def test_cpus_per_partition_cpu2_48_other_32() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    # cpu2 → 48
    _launcher(runner, partitions=("cpu2",)).submit(0)
    assert "#SBATCH --cpus-per-task=48" in runner.calls[-1][1]  # type: ignore[operator]
    # 그 외(gpu4) → 32
    _launcher(runner, partitions=("gpu4",)).submit(0)
    assert "#SBATCH --cpus-per-task=32" in runner.calls[-1][1]  # type: ignore[operator]
    assert "#SBATCH --partition=gpu4" in runner.calls[-1][1]  # type: ignore[operator]


def test_gpu_partition_requests_gres_cpu2_does_not() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 1\n", "")
    # gpu* → --gres=gpu:4 요청 + EDT_GPU_COUNT=4 (peetsfea 자동 GPU + 워커 핀닝 활성화)
    _launcher(runner, partitions=("gpu1",)).submit(0)
    gpu_script = runner.calls[-1][1]
    assert "#SBATCH --gres=gpu:4" in gpu_script  # type: ignore[operator]
    assert "export EDT_GPU_COUNT=4" in gpu_script  # type: ignore[operator]
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
    seq = iter(["cpu2", "gpu1", "gpu2", "gpu6"])
    launcher.partition_chooser = lambda parts: next(seq)
    for _ in range(4):
        launcher.submit(0)
        seen.add(runner.calls[-1][1].split("--partition=")[1].split("\n")[0])  # type: ignore[union-attr]
    assert {"cpu2", "gpu1", "gpu2", "gpu6"} <= seen
    # 기본 파티션 후보에서 cpu1·gpu5 제외 확인
    assert "cpu1" not in SlurmJobLauncher().partitions and "gpu5" not in SlurmJobLauncher().partitions


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
