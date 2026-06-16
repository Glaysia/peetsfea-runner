"""실 SLURM JobLauncher (Phase 2) — sbatch/squeue/scancel로 잡 lifecycle 구현.

`JobOrchestrator`가 쓰는 `JobLauncher`의 실 구현. 게이트노드에 ssh로 붙어 잡을 제출/모니터/종료한다.
잡이 실제로 돌리는 명령(`job_command`)은 보통 enroot 컨테이너에서 `python -m peetsfea_runner.edt_entrypoint`
이며, lifecycle 검증 시엔 짧은 placeholder(예: `sleep`)로 둘 수 있다.

명령 실행은 `CommandRunner`로 분리(테스트는 fake, 실서비스는 subprocess+ssh).
"""

from __future__ import annotations

import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import random
from collections.abc import Sequence

from .edt_orchestrator import JobHandle

Clock = Callable[[], float]

# 활성으로 간주하는 SLURM 상태(squeue -h -o %T).
_ACTIVE_STATES = frozenset({"RUNNING", "PENDING", "CONFIGURING", "COMPLETING", "RESIZING", "REQUEUED"})
_SUBMITTED_RE = re.compile(r"Submitted batch job (\d+)")

# 잡을 랜덤 분배하는 파티션(MASTER_PLAN §2.10 / Q5: 파티션별 성능 통계 자연 수집).
# cpu1·gpu5는 제외(포화/문제 파티션).
DEFAULT_PARTITIONS: tuple[str, ...] = ("cpu2", "gpu1", "gpu2", "gpu3", "gpu4", "gpu6")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


# (argv, input_text) -> CommandResult
CommandRunner = Callable[[list[str], str | None], CommandResult]


def subprocess_runner(argv: list[str], input_text: str | None = None) -> CommandResult:
    proc = subprocess.run(argv, input=input_text, capture_output=True, text=True, timeout=120)
    return CommandResult(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)


class SlurmLauncherError(RuntimeError):
    pass


@dataclass
class SlurmJobLauncher:
    """게이트노드 ssh + sbatch/squeue/scancel로 잡 1개를 제출/모니터/종료."""

    ssh_host: str = "gate1-harry261"
    # 잡을 무작위 분배할 파티션들(MASTER_PLAN §2.10). partition_chooser로 잡마다 1개 선택.
    partitions: tuple[str, ...] = DEFAULT_PARTITIONS
    time_limit: str = "10:00:00"
    # cpus-per-task: cpu2 노드는 256코어지만 QOS cpu2_limit이 노드당 cpu=64로 하드캡(MaxTRESPerNode).
    # 48로 운영(여유). 그 외 파티션(QOS normal)은 무제한이라 32.
    cpus_cpu2: int = 48
    cpus_other: int = 32
    # gpu* 파티션은 노드당 GPU 4개. --gres=gpu:N을 요청해야 컨테이너가 GPU를 보고(peetsfea 0.3.6 자동감지),
    # 안 그러면 0 GPU 할당 → CPU fallback(느림). cpu2는 GPU 없으니 요청 안 한다.
    # 2로 둔다: gpu:4를 요구하면 공유 클러스터에서 한 노드에 GPU 4개 동시확보가 어려워 잡이 무한 PENDING(백필 불가).
    # gpu:2면 mix 노드의 빈 GPU에 백필돼 잡이 실제로 뜬다. 워커는 CUDA_VISIBLE_DEVICES=idx%2로 두 GPU에 분산.
    gres_gpu_count: int = 2
    mem: str = "480G"  # 전 파티션 노드 ≥768GB라 적재 가능
    job_name_prefix: str = "peetsfea-edt"
    job_command: str = "echo placeholder-job; sleep 60"  # 실서비스는 enroot+entrypoint로 교체
    partition_chooser: Callable[[Sequence[str]], str] = random.choice
    clock: Clock = time.monotonic
    command_runner: CommandRunner = subprocess_runner

    def _ssh(self, remote: str, *, input_text: str | None = None) -> CommandResult:
        argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", self.ssh_host, remote]
        return self.command_runner(argv, input_text)

    def _cpus_for(self, partition: str) -> int:
        return self.cpus_cpu2 if partition == "cpu2" else self.cpus_other

    def _gpu_count_for(self, partition: str) -> int:
        return self.gres_gpu_count if partition.startswith("gpu") else 0

    def _sbatch_script(self, job_index: int, partition: str, cpus: int) -> str:
        gpus = self._gpu_count_for(partition)
        gres_line = f"#SBATCH --gres=gpu:{gpus}\n" if gpus > 0 else ""
        return (
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self.job_name_prefix}-{job_index}\n"
            f"#SBATCH --partition={partition}\n"
            f"#SBATCH --time={self.time_limit}\n"
            "#SBATCH --nodes=1 --ntasks=1\n"
            f"#SBATCH --cpus-per-task={cpus}\n"
            f"{gres_line}"
            f"#SBATCH --mem={self.mem}\n"
            f"export EDT_JOB_INDEX={job_index}\n"
            f"export EDT_PARTITION={partition}\n"
            # EDT_GPU_COUNT: 컨테이너 supervisor가 워커별 CUDA_VISIBLE_DEVICES=index%N 핀닝에 사용(GPU 분산).
            f"export EDT_GPU_COUNT={gpus}\n"
            f"{self.job_command}\n"
        )

    def submit(self, job_index: int) -> JobHandle:
        partition = self.partition_chooser(self.partitions)
        cpus = self._cpus_for(partition)
        result = self._ssh("sbatch", input_text=self._sbatch_script(job_index, partition, cpus))
        if result.returncode != 0:
            raise SlurmLauncherError(f"sbatch failed (rc={result.returncode}): {result.stderr.strip()}")
        match = _SUBMITTED_RE.search(result.stdout)
        if match is None:
            raise SlurmLauncherError(f"could not parse sbatch output: {result.stdout.strip()!r}")
        return JobHandle(job_index=job_index, slurm_id=match.group(1), started_at=self.clock())

    def is_alive(self, handle: JobHandle) -> bool:
        result = self._ssh(f"squeue -j {handle.slurm_id} -h -o %T")
        if result.returncode != 0:
            return False
        state = result.stdout.strip().splitlines()[0].strip() if result.stdout.strip() else ""
        return state in _ACTIVE_STATES

    def kill(self, handle: JobHandle) -> None:
        # graceful SIGTERM만 보낸다(자동 SIGKILL 없음): supervisor/워커가 TERM에 깨끗이 종료하고
        # slot_service trap이 /enroot 노드 로컬 스크래치를 청소한다. raw `scancel`(KILL 폴백)을 쓰면
        # SIGKILL이 trap보다 빨라 /enroot 잔재가 남을 수 있다(공용 공간). 강제 종료가 필요하면 safe_scancel.sh.
        self._ssh(f"scancel --full --signal=TERM {handle.slurm_id}")


__all__ = ["CommandResult", "CommandRunner", "SlurmJobLauncher", "SlurmLauncherError", "subprocess_runner"]
