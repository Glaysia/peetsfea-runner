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


def _launcher(runner: FakeRunner) -> SlurmJobLauncher:
    return SlurmJobLauncher(command_runner=runner, clock=lambda: 0.0)


def test_submit_parses_slurm_id_and_sends_sbatch_script() -> None:
    runner = FakeRunner()
    runner.responses["sbatch"] = CommandResult(0, "Submitted batch job 777\n", "")
    handle = _launcher(runner).submit(3)
    assert handle.slurm_id == "777" and handle.job_index == 3
    # sbatch에 스크립트가 stdin으로 전달되고 SBATCH 헤더/명령 포함.
    argv, script = runner.calls[0]
    assert argv[-1] == "sbatch" and script is not None
    assert "#SBATCH --partition=cpu2" in script
    assert "export EDT_JOB_INDEX=3" in script


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
    assert any("scancel 99" in r for r in runner.remotes())
