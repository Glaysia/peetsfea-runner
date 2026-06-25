"""SLURM 잡 인프라 유지 루프.

잡은 **고정 인프라**다 — 제어 목적으로 죽이지 않는다. 누수 회수는 컨테이너 PID-ns 격리가 전담하고
(`PLANS/leak_reclaim_test.html`), 처리량 제어는 `ContainerScheduler`의 적분제어가 컨테이너 수로
actuate한다(`PLANS/integral_container_control.html`). 여기서는 job_count개 잡을 살아있게 유지하고,
죽으면 재기동, max_lifetime 경과 시 교체(드리프트/잔류 방지)만 한다.

구 정책(2분 홀짝 submit4/kill1, solve→N LUT, 가장-늙은-잡 종료, squeue-stuck 회복, 12분 포화제어)은
모두 폐지됐다 — 제어가 잡-죽임을 겸직하던 구조가 통째 출렁임(리플)을 만들었기 때문.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol

from .constants import JOB_MAX_LIFETIME_SECONDS, JOBS_PER_ACCOUNT

Clock = Callable[[], float]

# PENDING 사유에서 문제 노드 추출용(예 "ReqNodeNotAvail, UnavailableNodes:n113").
_NODE_RE = re.compile(r"\bn\d+\b")
# 즉시 회피·교체할 PENDING 사유(핀한 노드가 영영 안 뜸). Resources/Priority는 정상 대기라 제외.
_HARD_PENDING = ("ReqNodeNotAvail", "BadConstraints", "InvalidNode", "InvalidQOS", "PartitionNodeLimit")


@dataclass(frozen=True, slots=True)
class JobHandle:
    """제출된 잡 1개의 핸들."""

    job_index: int
    slurm_id: str
    started_at: float
    node: str = ""  # 제출 시 핀한 노드(런처가 노드 회피 등에 사용).


class JobInfo(NamedTuple):
    """squeue 1회 스냅샷의 잡 1건(키퍼 reconcile/stuck 판정용)."""

    slurm_id: str
    state: str
    node: str
    reason: str


class JobLauncher(Protocol):
    """잡 1개의 제출/생존/종료. 구현체가 컨테이너 안에서 슬롯 서비스를 돌린다.

    선택 능력(있으면 키퍼가 reconcile/stuck 처리에 사용, 없으면 legacy is_alive 경로):
      list_active_by_index() -> dict[int, list[JobInfo]]   # squeue --me 1회 스냅샷
      pending_reason(handle) -> str   avoid_node(node)   cancel(handle)
    """

    def submit(self, job_index: int) -> JobHandle: ...
    def is_alive(self, handle: JobHandle) -> bool: ...
    def kill(self, handle: JobHandle) -> None: ...


@dataclass
class JobOrchestrator:
    """계정 내 고정 SLURM 잡(인프라)을 안정 유지한다.

    잡은 제어 목적으로 죽이지 않는다 — 처리량은 ContainerScheduler 적분제어가 컨테이너 수로 조절한다.
    여기선 job_count개를 살려두고(죽으면 재기동), max_lifetime 경과 잡만 교체한다(드리프트/잔류 방지).
    """

    launcher: JobLauncher
    clock: Clock
    job_count: int = JOBS_PER_ACCOUNT
    max_lifetime_seconds: float = float(JOB_MAX_LIFETIME_SECONDS)
    # PENDING이 이 시간 이상 지속되면(사유 무관) 다른 노드로 재배치. hard 사유(ReqNodeNotAvail 등)는 즉시.
    # Resources/Priority는 정상 대기라, 길게 막힌 경우만 재-pick(핀한 노드가 계속 바쁜 케이스 구제).
    stuck_pending_seconds: float = 600.0
    # 관측/상태용(제어 자체는 ContainerScheduler가 담당). control_plane이 주입.
    solve_provider: Callable[[], int] | None = None
    submitted: int = field(default=0, init=False)
    restarts: int = field(default=0, init=False)  # 죽어서 재기동한 횟수
    expiries: int = field(default=0, init=False)  # max_lifetime 만료로 교체한 횟수
    adopted: int = field(default=0, init=False)  # squeue에 이미 있던 잡을 채택(재시작 후 중복제출 방지)
    dedup_cancels: int = field(default=0, init=False)  # 같은 슬롯 중복 잡을 취소한 횟수
    stuck_replacements: int = field(default=0, init=False)  # 막힌 PENDING을 다른 노드로 재배치한 횟수
    submit_failures: int = field(default=0, init=False)  # 가용 노드/ssh/sbatch 일시 실패 횟수
    last_submit_error: str = field(default="", init=False)
    _jobs: dict[int, JobHandle] = field(default_factory=dict, init=False, repr=False)
    _pending_since: dict[int, float] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def _submit(self, job_index: int) -> bool:
        try:
            self._jobs[job_index] = self.launcher.submit(job_index)
        except Exception as exc:  # noqa: BLE001 - Slurm availability and ssh failures are transient.
            self.submit_failures += 1
            self.last_submit_error = f"{type(exc).__name__}: {exc}"
            print(f"[orchestrator] submit failed job={job_index}: {self.last_submit_error}", flush=True)
            return False
        self.submitted += 1
        return True

    def _snapshot(self) -> dict[int, list[JobInfo]] | None:
        """런처가 지원하면 squeue --me 1회 스냅샷(job_index→잡들). 미지원이면 None(legacy 경로)."""
        lister = getattr(self.launcher, "list_active_by_index", None)
        if lister is None:
            return None
        try:
            snap = lister()
        except Exception as exc:  # noqa: BLE001 — squeue 일시 실패가 유지루프를 막으면 안 됨.
            print(f"[orchestrator] snapshot failed: {type(exc).__name__}: {exc}", flush=True)
            return None
        return snap if isinstance(snap, dict) else None

    def ensure_running(self) -> None:
        """초기 기동: 슬롯을 채운다. squeue에 이미 우리 잡이 있으면 **채택**(재시작 후 중복제출 방지)."""
        with self._lock:
            snap = self._snapshot()
            now = self.clock()
            for i in range(self.job_count):
                if snap is not None:
                    self._reconcile_slot(i, snap.get(i, []), now)
                elif i not in self._jobs:
                    self._submit(i)

    def poll(self) -> None:
        """고정 잡 유지: squeue 1회 스냅샷으로 ① 중복제거 ② 막힌 PENDING 재배치 ③ 빈/죽은 슬롯 재기동
        ④ max_lifetime 교체. 런처가 스냅샷 미지원이면 legacy is_alive 경로."""
        with self._lock:
            now = self.clock()
            snap = self._snapshot()
            if snap is None:
                self._poll_legacy(now)
                return
            for i in range(self.job_count):
                self._reconcile_slot(i, snap.get(i, []), now)

    def _reconcile_slot(self, i: int, infos: list[JobInfo], now: float) -> None:
        """슬롯 i를 squeue 진실과 일치시킨다: 중복취소→채택→stuck 재배치→수명교체. 빈 슬롯이면 제출."""
        canceller = getattr(self.launcher, "cancel", None)
        if len(infos) > 1:  # ③ 중복 방지: RUNNING(없으면 가장 오래된) 하나만 남기고 나머지 취소
            infos = self._dedup(i, infos, canceller)
        if not infos:
            had = self._jobs.pop(i, None)
            self._pending_since.pop(i, None)
            if self._submit(i) and had is not None:
                self.restarts += 1
            return
        keep = infos[0]
        cur = self._jobs.get(i)
        started = cur.started_at if (cur is not None and cur.slurm_id == keep.slurm_id) else now
        if cur is None or cur.slurm_id != keep.slurm_id:
            self.adopted += 1  # squeue에 있던 잡 채택(재시작 직후 등) → 같은 인덱스 재제출 안 함
        handle = JobHandle(job_index=i, slurm_id=keep.slurm_id, started_at=started, node=keep.node)
        self._jobs[i] = handle
        if keep.state == "PENDING":
            if self._maybe_replace_stuck(i, keep, handle, now, canceller):
                return
        else:
            self._pending_since.pop(i, None)
        if (now - started) >= self.max_lifetime_seconds:  # ④ 수명 교체
            self.launcher.kill(handle)
            self._jobs.pop(i, None)
            if self._submit(i):
                self.expiries += 1

    def _dedup(self, i: int, infos: list[JobInfo], canceller: Callable[[JobHandle], None] | None) -> list[JobInfo]:
        def _key(info: JobInfo) -> tuple[int, int]:
            try:
                sid = int(info.slurm_id)
            except ValueError:
                sid = 0
            return (0 if info.state == "RUNNING" else 1, sid)  # RUNNING 우선, 그다음 가장 오래된(작은 id)

        ordered = sorted(infos, key=_key)
        keep = ordered[0]
        for dup in ordered[1:]:
            if canceller is not None:
                try:
                    canceller(JobHandle(job_index=i, slurm_id=dup.slurm_id, started_at=self.clock(), node=dup.node))
                except Exception:  # noqa: BLE001
                    pass
            self.dedup_cancels += 1
        return [keep]

    def _maybe_replace_stuck(
        self, i: int, info: JobInfo, handle: JobHandle, now: float, canceller: Callable[[JobHandle], None] | None
    ) -> bool:
        """막힌 PENDING이면 핀 노드 avoid + 취소 + 다른 노드 재제출. 처리했으면 True."""
        reason = info.reason or ""
        hard = any(h in reason for h in _HARD_PENDING)
        first = self._pending_since.setdefault(i, now)
        if not (hard or (now - first) >= self.stuck_pending_seconds):
            return False
        avoid = getattr(self.launcher, "avoid_node", None)
        if avoid is not None:
            bad = {handle.node} if handle.node else set()
            bad |= set(_NODE_RE.findall(reason))  # 사유에 박힌 UnavailableNodes도 회피(채택잡은 handle.node 없음)
            for n in bad:
                if n:
                    try:
                        avoid(n)
                    except Exception:  # noqa: BLE001
                        pass
        if canceller is not None:
            try:
                canceller(handle)
            except Exception:  # noqa: BLE001
                pass
        self._jobs.pop(i, None)
        self._pending_since.pop(i, None)
        if self._submit(i):
            self.stuck_replacements += 1
        return True

    def _poll_legacy(self, now: float) -> None:
        """런처가 스냅샷 미지원(테스트 fake 등): 기존 is_alive 기반 유지."""
        for i in range(self.job_count):
            handle = self._jobs.get(i)
            if handle is None:
                self._submit(i)
                continue
            if not self.launcher.is_alive(handle):
                if self._submit(i):
                    self.restarts += 1
            elif (now - handle.started_at) >= self.max_lifetime_seconds:
                self.launcher.kill(handle)
                if self._submit(i):
                    self.expiries += 1

    def running_count(self) -> int:
        with self._lock:
            return sum(1 for h in self._jobs.values() if self.launcher.is_alive(h))

    def handles(self) -> list[JobHandle]:
        with self._lock:
            return [self._jobs[i] for i in sorted(self._jobs)]

    def shutdown(self) -> None:
        """모든 잡 종료(서비스 정지). 진행 중 시뮬은 폐기."""
        with self._lock:
            for handle in self._jobs.values():
                self.launcher.kill(handle)
            self._jobs.clear()


__all__ = ["Clock", "JobHandle", "JobInfo", "JobLauncher", "JobOrchestrator"]
