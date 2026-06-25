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

from .edt_orchestrator import JobHandle, JobInfo

Clock = Callable[[], float]

# 활성으로 간주하는 SLURM 상태(squeue -h -o %T).
_ACTIVE_STATES = frozenset({"RUNNING", "PENDING", "CONFIGURING", "COMPLETING", "RESIZING", "REQUEUED"})
_SUBMITTED_RE = re.compile(r"Submitted batch job (\d+)")

# 잡 배치 파티션 — **cpu2 최우선, 포화 시 gpu 폴백**(B5, PLANS/project_overhaul_plan.html).
# _select_partition이 cpu2를 엄격 우선(가중랜덤 아님)하므로 gpu1/2/3은 **cpu2에 자리가 없을 때만** 쓴다.
# gpu 노드는 AEDT가 GPU를 실제로 안 써(CPU 솔브) 코어 적어 cpu2보다 느리지만(gpu 14~15분 vs cpu2 11분),
# cpu2가 타유저로 포화돼 잡이 자리를 못 찾을 때 처리량을 흘려보내는 폴백으로만 쓴다(평시엔 cpu2만).
DEFAULT_PARTITIONS: tuple[str, ...] = ("cpu2", "gpu1", "gpu2", "gpu3")


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
    # (B5 이전 가중선택용 — 이제 _select_partition이 cpu2 엄격 우선이라 미사용. 구성 호환 위해 필드 유지.)
    cpu2_weight: float = 0.7
    rng: Callable[[], float] = random.random
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
    # 계정간 노드 분리: **상대 계정들**의 SLURM username(자기 제외). control plane이 주입한다. _peer_nodes가
    # squeue -u로 상대 계정 점유 노드를 보고 **항상 배제** → 한 노드엔 한 계정만(cross-account OpenMP SHM 충돌
    # 방지). 같은 클러스터(gate1)라 squeue -u <peer> 조회 가능. 비면 단일계정(배제 없음).
    peer_users: tuple[str, ...] = ()
    # 노드당 **같은 계정** 잡 수 상한(packing). 1잡/노드(spread)면 한 계정이 가용 노드를 다 먹어 상대 계정이
    # 빈 노드를 못 찾고 co-locate→SHM 충돌. 2잡/노드로 몰아 담으면 절반 노드만 써 상대 계정 자리를 남긴다.
    max_jobs_per_node: int = 2
    _avoid: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _submitted_nodes: dict[str, list[float]] = field(default_factory=dict, init=False, repr=False)

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
            # 노드 배타는 --exclusive 안 씀(노드 전체 256cpu 요구 → QOS cpu2_limit 64cpu/node와 충돌, QOSMaxCpuPerNode).
            # 대신 _busy_nodes가 양 계정 점유 노드(squeue -u)를 제외 → node_based 핀으로 계정간 노드 분리.
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

    def _nodes_of(self, who: str) -> list[str]:
        """squeue 인자 who(--me 또는 -u <users>)로 RUNNING/PENDING 잡의 노드 리스트(잡당 1줄, 중복=노드당 잡수)."""
        result = self._ssh(f"squeue -h {who} -t RUNNING,PENDING -o '%N'")
        if result.returncode != 0:
            return []
        return [n.strip() for n in result.stdout.splitlines() if n.strip() and not n.strip().startswith("(")]

    def _peer_nodes(self) -> set[str]:
        """상대 계정(peer_users)이 점유한 노드 — 항상 배제(cross-account SHM 충돌 방지). 한 노드엔 한 계정만."""
        peers = tuple(u for u in self.peer_users if u)
        if not peers:
            return set()
        return set(self._nodes_of(f"-u {','.join(peers)}"))

    def _own_counts(self) -> dict[str, int]:
        """내 계정 잡의 노드별 개수(squeue 반영분) — max_jobs_per_node 캡 + packing 판정용."""
        counts: dict[str, int] = {}
        for n in self._nodes_of("--me"):
            counts[n] = counts.get(n, 0) + 1
        return counts

    def _candidate_nodes(self) -> list[tuple[str, str]]:
        """대상 파티션의 가용 노드 (node, partition). idle 우선, 그다음 mix.

        sinfo %C(=A/I/O/T)가 있으면 **idle 코어가 우리 잡 cpus 미만인 노드는 제외**(B3 free-core 프리필터):
        --nodelist 핀은 코어가 모자라도 sbatch가 PENDING(ReqNodeNotAvail)으로 받아줘 고착되므로, 애초에
        안 들어가는 노드를 후보에서 뺀다 → cpu2가 진짜 포화면 후보 0 → gpu 폴백이 즉시 작동(B5)."""
        result = self._ssh(f"sinfo -h -N -p {','.join(self.partitions)} -o '%N %P %t %C'")
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
            if state not in ("idle", "mix", "mixed"):
                continue
            # free-core 필터: %C가 있을 때만(없으면 통과 — 구 테스트/포맷 호환). idle 코어 < 요청 cpus면 제외.
            if len(parts) >= 4 and "/" in parts[3]:
                try:
                    idle_cores = int(parts[3].split("/")[1])
                except (ValueError, IndexError):
                    idle_cores = None
                if idle_cores is not None and idle_cores < self._cpus_for(partition):
                    continue
            (idle if state == "idle" else mixed).append((node, partition))
        return idle + mixed

    def _select_partition(self, available: set[str]) -> str:
        """**cpu2 엄격 우선, gpu 폴백 전용**(B5): cpu2에 후보 노드가 있으면 무조건 cpu2를 먼저 채우고,
        cpu2가 포화(후보 0)일 때만 gpu로 흘린다. 가중랜덤(cpu2_weight) 폐지 — gpu는 평시 미사용 폴백."""
        if "cpu2" in available:
            return "cpu2"
        gpus = sorted(p for p in available if p.startswith("gpu"))
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

    def _reserved_counts(self) -> dict[str, int]:
        """최근 제출한 노드별 로컬 예약 수(squeue 반영 전 race 방지). packing 위해 노드당 여러 건 카운트."""
        now = self.clock()
        counts: dict[str, int] = {}
        for n, ts_list in self._submitted_nodes.items():
            recent = sum(1 for t in ts_list if (now - t) < self.submit_reservation_seconds)
            if recent:
                counts[n] = recent
        return counts

    def pending_reason(self, handle: JobHandle) -> str:
        """PENDING 사유(squeue %r). 'None'/빈값=곧 시작, 'Resources'/'Priority' 등=한동안 대기."""
        result = self._ssh(f"squeue -j {handle.slurm_id} -h -o %r")
        if result.returncode != 0 or not result.stdout.strip():
            return ""
        return result.stdout.strip().splitlines()[0].strip()

    def list_active_by_index(self) -> dict[int, list[JobInfo]]:
        """squeue --me 1회로 내 활성 잡을 job_index(잡이름 접미사)별로 그룹화 — state·node·reason 포함.
        키퍼가 ① 중복제거 ② 막힌 PENDING 재배치 ③ 재시작 후 채택에 쓴다(슬롯당 잡 1개 truth)."""
        states = ",".join(sorted(_ACTIVE_STATES))
        result = self._ssh(f"squeue --me -h -t {states} -o '%i|%j|%T|%N|%r'")
        out: dict[int, list[JobInfo]] = {}
        if result.returncode != 0:
            return out
        prefix = f"{self.job_name_prefix}-"
        for line in result.stdout.splitlines():
            parts = line.split("|")
            if len(parts) < 5:
                continue
            sid, name, state, node, reason = (p.strip() for p in parts[:5])
            if not name.startswith(prefix):
                continue
            suffix = name[len(prefix):]
            if not suffix.isdigit():
                continue
            # PENDING은 %N이 비거나 '(reason)' 형태 → 노드 미상으로 둔다(회피는 reason의 노드명으로).
            node = "" if (not node or node.startswith("(")) else node
            out.setdefault(int(suffix), []).append(JobInfo(slurm_id=sid, state=state, node=node, reason=reason))
        return out

    def _ordered_candidates(self) -> list[tuple[str, str]]:
        """제출 후보 노드 순서: ① 상대 계정 노드 항상 배제(SHM) ② 내 잡 수(squeue+예약)가 max 이상인 노드 배제
        ③ **packing**: 내가 이미 쓰는(아직 max 미만) 노드를 빈 노드보다 앞에 둬, 절반 노드만 쓰고 상대 자리를 남긴다."""
        peer = self._peer_nodes()
        own = self._own_counts()
        reserved = self._reserved_counts()
        avoided = self._avoided_nodes()

        def load(n: str) -> int:
            return own.get(n, 0) + reserved.get(n, 0)

        cands = [(n, p) for (n, p) in self._candidate_nodes()
                 if n not in peer and n not in avoided and load(n) < self.max_jobs_per_node]
        if not cands:
            return []
        # packing: 내가 이미 잡을 올린(부분 점유) 노드 우선 → 그 노드를 max까지 채운 뒤 새 노드로.
        cands.sort(key=lambda np: 0 if load(np[0]) > 0 else 1)
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
            self._submitted_nodes.setdefault(node, []).append(self.clock())
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
