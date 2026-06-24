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
# 최근 2시간(2026-06-19) 관측상 평균 RUNNING job이 4~5개에 묶였다. job당 AEDT 수는 충분하므로
# cpu2/gpu1에 더해 MIX 여유가 보이는 gpu2/gpu3까지 후보를 넓혀 평균 RUNNING job 9개를 목표로 한다.
# cpu2 전용: gpu 노드는 AEDT가 GPU를 실제로 안 써(nvidia-smi 0%·gpu_used는 거짓 config 플래그) CPU 솔브인데
# 코어까지 적어 cpu2보다 느렸다(gpu 14~15분 vs cpu2 11분). → gpu 파티션 제거, cpu2만 사용.
DEFAULT_PARTITIONS: tuple[str, ...] = ("cpu2",)


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
    # 하드캡까지 꽉 채워 64로 운영(GPU 폐기로 cpu2에 집중 → 잡당 코어 최대화).
    cpus_cpu2: int = 64
    cpus_other: int = 24
    # gpu* 파티션은 노드당 GPU 4개. --gres=gpu:N을 요청해야 컨테이너가 GPU를 보고(peetsfea 0.3.6 자동감지),
    # 안 그러면 0 GPU 할당 → CPU fallback(느림). cpu2는 GPU 없으니 요청 안 한다.
    # gpu:2도 MIX 노드에서 자주 PENDING이 된다. 평균 RUNNING job 확보가 우선이라 gpu:1로 낮춰 백필 폭을 넓힌다.
    gres_gpu_count: int = 1
    mem_cpu2: str = "480G"
    mem_other: str = "384G"
    # 호환 override: 검증 스크립트가 mem="32G"처럼 직접 지정하면 파티션별 기본값보다 우선한다.
    mem: str | None = None
    job_name_prefix: str = "peetsfea-edt"
    job_command: str = "echo placeholder-job; sleep 60"  # 실서비스는 enroot+entrypoint로 교체
    partition_chooser: Callable[[Sequence[str]], str] = random.choice
    # node_based=True면 파티션에 던지지 않고 **빈 노드를 골라 --nodelist로 핀**한다. sinfo로 idle/mix 노드를
    # 찾고, 이미 내 잡이 도는 노드는 제외(노드당 1잡 = 과적재 방지). 오케스트레이터의 순차 램프와 짝.
    node_based: bool = False
    # 노드 선택 시 파티션 가중: cpu2와 gpu가 둘 다 가용이면 cpu2를 이 확률로 고른다(나머지는 gpu).
    cpu2_weight: float = 0.7
    rng: Callable[[], float] = random.random  # 가중 선택용(테스트 주입 가능)
    # 막힌 PENDING으로 취소된 노드를 이 시간 동안 후보에서 제외(그 노드가 한동안 안 비므로). clock 단위.
    avoid_cooldown_seconds: float = 300.0
    # 같은 control tick에서 연속 제출할 때 squeue 반영 지연으로 같은 노드를 다시 고르지 않게 하는 짧은 로컬 예약.
    submit_reservation_seconds: float = 60.0
    clock: Clock = time.monotonic
    command_runner: CommandRunner = subprocess_runner
    # 이미 사용한 baseline seed 프런티어를 반환(store.max_baseline_seed). 제출 시 이 위로 seed epoch를 잡아
    # 재램프해도 워커가 새 설계공간을 탐색(이미 푼 seed 재탕 방지). None이면 epoch=0(기존 동작).
    seed_epoch_provider: Callable[[], int] | None = None
    # 롤링 라이프사이클: 잡 출생 시 제어기가 LUT로 결정한 컨테이너 수 N을 주입(EDT_JOB_CONTAINERS).
    # None이면 미주입(orchestrator가 /job_plan 조회 또는 기본값). job_ttl_seconds=잡 수명(orchestrator self-exit).
    container_count_provider: Callable[[], int] | None = None
    job_ttl_seconds: int = 14400  # 잡 수명 4시간(orchestrator self-exit)
    # 잡별 디버그 sshd: 잡 노드에 **우리 소유 sshd**를 노드-로컬 포트에 띄우고(클러스터 22 미사용), 그 포트를
    # 게이트 결정적 포트로 역터널한다. 두 포트 모두 (계정×잡)별로 유일해야 충돌이 없다:
    #   게이트 포트  EDT_ORCH_SSHD_PORT  = debug_sshd_base  + stride*account + job  (게이트는 1대 = 전 잡 공유)
    #   노드 로컬   EDT_DEBUG_LOCAL_SSHD = debug_local_base + stride*account + job  (한 노드에 두 잡 co-locate 대비)
    # base=0이면 비활성(프로덕션 기본). PLANS/per_job_debug_access.html.
    debug_sshd_base: int = 0
    debug_local_base: int = 2200
    debug_account_stride: int = 50
    account_index: int = 0
    # 다계정 태깅: 잡이 띄우는 컨테이너가 결과를 어느 계정으로 ingest했는지 단일 DB에서 구분하게 한다.
    # sbatch 스크립트가 EDT_ACCOUNT_ID/EDT_HOST_ALIAS를 export → orchestrator.sh가 컨테이너로 전달.
    account_id: str = "account_01"
    host_alias: str = "gate1-harry261"
    # 다계정 백채널 포트: orchestrator가 EDT_*_PORT로 받아 **자기 계정**의 ingest/lease/컨트롤러(:license_ctrl_port)에
    # 연결한다. 미주입이면 orchestrator 기본(7876/7878/7879=account_01)을 써서 비-primary 계정(hmlee31)이
    # primary 컨트롤러를 따라가 /job_plan 피드백 루프가 끊긴다(컨테이너·솔브 저조). → sbatch에 계정별 포트 export.
    ingest_port: int = 7876
    priority_lease_port: int = 7878
    license_ctrl_port: int = 7879
    _avoid: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _submitted_nodes: dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def _seed_epoch(self) -> int:
        """다음 잡의 baseline seed 시작 오프셋. 사용한 최대 seed 바로 위(프런티어+1)에서 시작 → 재탕 0."""
        if self.seed_epoch_provider is None:
            return 0
        try:
            return max(0, int(self.seed_epoch_provider()) + 1)
        except Exception:  # noqa: BLE001 — store 조회 실패가 잡 제출을 막으면 안 됨(0으로 폴백).
            return 0

    def _ssh(self, remote: str, *, input_text: str | None = None) -> CommandResult:
        argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", self.ssh_host, remote]
        return self.command_runner(argv, input_text)

    def debug_sshd_port(self, job_index: int) -> int | None:
        """잡 i의 디버그 sshd **게이트** 포트(역터널 바인드, 결정적). 비활성(base=0)이면 None.

        로컬에서 `ssh -J <gate> -p <port> -i edt_debug <user>@127.0.0.1`로 그 잡 노드에 진입(→ enroot exec).
        """
        if self.debug_sshd_base <= 0:
            return None
        return self.debug_sshd_base + self.debug_account_stride * self.account_index + job_index

    def debug_local_sshd_port(self, job_index: int) -> int | None:
        """잡 i의 우리 sshd가 **노드 로컬**에 listen하는 포트(결정적). 같은 노드 co-locate 충돌 회피."""
        if self.debug_sshd_base <= 0:
            return None
        return self.debug_local_base + self.debug_account_stride * self.account_index + job_index

    def _cpus_for(self, partition: str) -> int:
        return self.cpus_cpu2 if partition == "cpu2" else self.cpus_other

    def _gpu_count_for(self, partition: str) -> int:
        return self.gres_gpu_count if partition.startswith("gpu") else 0

    def _mem_for(self, partition: str) -> str:
        if self.mem is not None:
            return self.mem
        return self.mem_cpu2 if partition == "cpu2" else self.mem_other

    def _sbatch_script(self, job_index: int, partition: str, cpus: int, *, node: str | None = None) -> str:
        gpus = self._gpu_count_for(partition)
        mem = self._mem_for(partition)
        gres_line = f"#SBATCH --gres=gpu:{gpus}\n" if gpus > 0 else ""
        # node_based: 특정 노드에 핀(과적재 방지). 파티션은 노드가 속한 파티션으로 함께 지정.
        nodelist_line = f"#SBATCH --nodelist={node}\n" if node else ""
        seed_epoch = self._seed_epoch()  # 사용한 seed 위로 시작점 advance(재램프 재탕 방지)
        # 롤링: 잡 출생 시 제어기 LUT로 N(컨테이너 수) 결정 → 주입. 실패 시 미주입(orchestrator 폴백).
        n_line = ""
        if self.container_count_provider is not None:
            try:
                n = int(self.container_count_provider())
                if n > 0:
                    n_line = f"export EDT_JOB_CONTAINERS={n}\n"
            except Exception:  # noqa: BLE001 — 제어기 조회 실패가 잡 제출을 막으면 안 됨.
                n_line = ""
        # 잡별 디버그 sshd 포트(결정적, 게이트+노드로컬 둘 다 유일). 활성 시 orchestrator가 우리 sshd를
        # 노드 로컬에 띄우고 게이트 포트로 -R 역터널.
        gate_sshd = self.debug_sshd_port(job_index)
        local_sshd = self.debug_local_sshd_port(job_index)
        sshd_line = (
            f"export EDT_ORCH_SSHD_PORT={gate_sshd}\nexport EDT_DEBUG_LOCAL_SSHD={local_sshd}\n"
            if gate_sshd else ""
        )
        return (
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self.job_name_prefix}-{job_index}\n"
            f"#SBATCH --partition={partition}\n"
            f"{nodelist_line}"
            f"#SBATCH --time={self.time_limit}\n"
            "#SBATCH --nodes=1 --ntasks=1\n"
            # 노드 배타 점유: 한 노드에 한 계정의 한 잡만 → 두 계정 AEDT가 같은 노드에 안 섞인다.
            # (co-locate 시 cross-account OpenMP 'Can't open SHM'로 늦게 램프한 계정 컨테이너가 rc=134 abort.)
            "#SBATCH --exclusive\n"
            f"#SBATCH --cpus-per-task={cpus}\n"
            f"{gres_line}"
            f"#SBATCH --mem={mem}\n"
            f"export EDT_JOB_INDEX={job_index}\n"
            f"export EDT_PARTITION={partition}\n"
            # 다계정 태깅: 컨테이너가 결과 ingest 시 account_id로 단일 DB에 구분 기록.
            f"export EDT_ACCOUNT_ID={self.account_id}\n"
            f"export EDT_HOST_ALIAS={self.host_alias}\n"
            # 계정별 백채널 포트 — orchestrator가 자기 계정 ingest/lease/컨트롤러에 연결(피드백 루프 분리).
            f"export EDT_INGEST_PORT={self.ingest_port}\n"
            f"export EDT_PRIORITY_LEASE_PORT={self.priority_lease_port}\n"
            f"export EDT_LICENSE_CTRL_PORT={self.license_ctrl_port}\n"
            # EDT_GPU_COUNT: 컨테이너 supervisor가 워커별 CUDA_VISIBLE_DEVICES=index%N 핀닝에 사용(GPU 분산).
            f"export EDT_GPU_COUNT={gpus}\n"
            # EDT_BASELINE_SEED_EPOCH: entrypoint가 seed_base에 더해 재램프마다 새 설계공간을 탐색(재탕 방지).
            f"export EDT_BASELINE_SEED_EPOCH={seed_epoch}\n"
            # 롤링 라이프사이클: orchestrator가 N개 컨테이너를 stagger 가동 후 TTL에 self-exit.
            f"export EDT_JOB_TTL_SEC={self.job_ttl_seconds}\n"
            f"{n_line}"
            f"{sshd_line}"
            f"{self.job_command}\n"
        )

    def _busy_nodes(self) -> set[str]:
        """내 잡(RUNNING/PENDING)이 이미 점유한 노드 — 노드당 1잡을 위해 제외 대상."""
        result = self._ssh("squeue -h --me -t RUNNING,PENDING -o '%N'")
        if result.returncode != 0:
            return set()
        nodes: set[str] = set()
        for line in result.stdout.splitlines():
            name = line.strip()
            if name and not name.startswith("("):  # PENDING 사유는 '(Resources)' 형태 → 제외
                nodes.add(name)
        return nodes

    def _candidate_nodes(self) -> list[tuple[str, str]]:
        """대상 파티션의 가용 노드 (node, partition). idle 우선, 그다음 mix."""
        result = self._ssh(f"sinfo -h -N -p {','.join(self.partitions)} -o '%N %P %t'")
        if result.returncode != 0:
            return []
        idle: list[tuple[str, str]] = []
        mixed: list[tuple[str, str]] = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            node, partition, state = parts[0], parts[1].rstrip("*"), parts[2].lower()
            if partition not in self.partitions:
                continue
            if state == "idle":
                idle.append((node, partition))
            elif state in ("mix", "mixed"):
                mixed.append((node, partition))
        return idle + mixed

    def _select_partition(self, available: set[str]) -> str:
        """가용 파티션 중 가중 선택: cpu2/gpu 둘 다면 cpu2를 cpu2_weight 확률로."""
        has_cpu2 = "cpu2" in available
        gpus = sorted(p for p in available if p.startswith("gpu"))
        if has_cpu2 and gpus:
            return "cpu2" if self.rng() < self.cpu2_weight else self.partition_chooser(gpus)
        if has_cpu2:
            return "cpu2"
        if gpus:
            return self.partition_chooser(gpus)
        return self.partition_chooser(sorted(available))

    def avoid_node(self, node: str) -> None:
        """막힌 PENDING으로 취소된 노드를 쿨다운 동안 후보에서 제외 등록."""
        if node:
            self._avoid[node] = self.clock()

    def _avoided_nodes(self) -> set[str]:
        now = self.clock()
        return {n for n, t in self._avoid.items() if (now - t) < self.avoid_cooldown_seconds}

    def _reserved_nodes(self) -> set[str]:
        now = self.clock()
        return {n for n, t in self._submitted_nodes.items() if (now - t) < self.submit_reservation_seconds}

    def pending_reason(self, handle: JobHandle) -> str:
        """PENDING 사유(squeue %r). 'None'/빈값=곧 시작, 'Resources'/'Priority' 등=한동안 대기."""
        result = self._ssh(f"squeue -j {handle.slurm_id} -h -o %r")
        if result.returncode != 0 or not result.stdout.strip():
            return ""
        return result.stdout.strip().splitlines()[0].strip()

    def _ordered_candidates(self) -> list[tuple[str, str]]:
        """제출 후보 노드 순서: 빈 노드 중 가중 선택된 파티션을 앞에(나머지는 fallback). busy·회피 노드 제외."""
        skip = self._busy_nodes() | self._avoided_nodes() | self._reserved_nodes()
        cands = [(n, p) for (n, p) in self._candidate_nodes() if n not in skip]
        if not cands:
            return []
        by_part: dict[str, list[tuple[str, str]]] = {}
        for n, p in cands:
            by_part.setdefault(p, []).append((n, p))
        chosen = self._select_partition(set(by_part))
        head = by_part.pop(chosen, [])
        rest = [c for lst in by_part.values() for c in lst]
        return head + rest

    def submit(self, job_index: int) -> JobHandle:
        if not self.node_based:
            partition = self.partition_chooser(self.partitions)
            cpus = self._cpus_for(partition)
            result = self._ssh("sbatch", input_text=self._sbatch_script(job_index, partition, cpus))
            return self._parse_submit(result, job_index)
        # node_based: 빈 노드(가중 선택)에 핀. 레이스로 한 노드 제출이 실패하면 다음 후보로.
        candidates = self._ordered_candidates()
        if not candidates:
            raise SlurmLauncherError(f"no available node in {self.partitions} (job {job_index})")
        last_err = ""
        for node, partition in candidates:
            cpus = self._cpus_for(partition)
            result = self._ssh("sbatch", input_text=self._sbatch_script(job_index, partition, cpus, node=node))
            if result.returncode == 0 and _SUBMITTED_RE.search(result.stdout):
                return self._parse_submit(result, job_index, node=node)
            last_err = result.stderr.strip() or result.stdout.strip()
        raise SlurmLauncherError(f"sbatch failed on all candidate nodes (job {job_index}): {last_err}")

    def _parse_submit(self, result: CommandResult, job_index: int, *, node: str = "") -> JobHandle:
        if result.returncode != 0:
            raise SlurmLauncherError(f"sbatch failed (rc={result.returncode}): {result.stderr.strip()}")
        match = _SUBMITTED_RE.search(result.stdout)
        if match is None:
            raise SlurmLauncherError(f"could not parse sbatch output: {result.stdout.strip()!r}")
        if node:
            self._submitted_nodes[node] = self.clock()
        return JobHandle(job_index=job_index, slurm_id=match.group(1), started_at=self.clock(), node=node)

    def is_alive(self, handle: JobHandle) -> bool:
        result = self._ssh(f"squeue -j {handle.slurm_id} -h -o %T")
        if result.returncode != 0:
            return False
        state = result.stdout.strip().splitlines()[0].strip() if result.stdout.strip() else ""
        return state in _ACTIVE_STATES

    def is_running(self, handle: JobHandle) -> bool:
        """RUNNING 상태인지(순차 램프 게이트용). PENDING/CONFIGURING은 False."""
        result = self._ssh(f"squeue -j {handle.slurm_id} -h -o %T")
        if result.returncode != 0:
            return False
        state = result.stdout.strip().splitlines()[0].strip() if result.stdout.strip() else ""
        return state == "RUNNING"

    def kill(self, handle: JobHandle) -> None:
        # graceful SIGTERM만 보낸다(자동 SIGKILL 없음): supervisor/워커가 TERM에 깨끗이 종료하고
        # slot_service trap이 /enroot 노드 로컬 스크래치를 청소한다. raw `scancel`(KILL 폴백)을 쓰면
        # SIGKILL이 trap보다 빨라 /enroot 잔재가 남을 수 있다(공용 공간). 강제 종료가 필요하면 safe_scancel.sh.
        # 주의: `--signal=TERM`은 **RUNNING 잡 전용** — PENDING 잡엔 시그널 받을 step이 없어 no-op이라 안 꺼진다.
        self._ssh(f"scancel --full --signal=TERM {handle.slurm_id}")

    def cancel(self, handle: JobHandle) -> None:
        # PENDING 잡 취소용: plain `scancel`로 큐에서 즉시 제거(컨테이너 미기동 → /enroot 청소 불필요).
        # 막힌 PENDING(Resources/ReqNodeNotAvail 등)은 kill(--signal=TERM)으론 안 빠지므로 이걸 쓴다.
        self._ssh(f"scancel {handle.slurm_id}")


__all__ = ["CommandResult", "CommandRunner", "SlurmJobLauncher", "SlurmLauncherError", "subprocess_runner"]
