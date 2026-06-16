"""컨트롤 플레인 — 정상상태 동시 ~100 연속 가동 (MASTER_PLAN 운영 목표).

제어 호스트(systemd `--user` 서비스)가 돌리는 상시 루프:
- **오케스트레이터:** 9개 SLURM 잡(=컨테이너=슬롯 서비스)을 상시 유지. 각 잡은 entrypoint로
  `build_steady_state_service`를 돌려 **baseline 전역 샘플링으로 슬롯을 자기공급** → 요청이 없어도
  계속 시뮬(동시 ~100). 죽으면 재기동, 10h 만료 폐기·재기동.
- **Intake :7875:** sweep 우선순위 요청 수신(우선순위 분배는 후속 — 현재는 baseline 자기공급이 핵심).
- **대시보드 :8080:** 결과 DB read-only 조회 + `results.csv`.
- **아카이브:** 완료 project_dir를 20GB 묶음 압축, 2TB FIFO.

`run_control_plane()`가 SIGTERM(=`systemctl stop`)까지 `poll()` 루프를 돈다.
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import FrameType

# 종료 시 아카이브 flush(대용량 tar.gz 압축)에 줄 최대 시간. 초과하면 다음 기동에 위임(systemd 'failed' 방지).
SHUTDOWN_FLUSH_DEADLINE_SECONDS = 45.0

from .constants import ARCHIVE_BUFFER_BYTES, JOBS_PER_ACCOUNT
from .edt_archive import ArchiveStore
from .edt_bulk_transfer import DEFAULT_BULK_PORT, start_bulk_transfer_server
from .edt_dashboard import start_dashboard_server
from .edt_intake import IntakeService, start_intake_server
from .edt_orchestrator import JobLauncher, JobOrchestrator
from .edt_queue import TwoLaneQueue
from .edt_resources import ResourcePoller
from .edt_result_ingest import DEFAULT_INGEST_PORT, start_result_ingest_server
from .edt_slurm_launcher import SlurmJobLauncher
from .edt_ssh_tunnel import SshTunnel, reverse_tunnel_argv
from .single_simulation_store import SingleSimulationResultStore


@dataclass(slots=True)
class ControlPlaneConfig:
    db_path: Path
    archive_root: Path
    archive_buffer_bytes: int = ARCHIVE_BUFFER_BYTES  # 로컬 디스크에 맞춰 조정(FIFO 상한).
    ssh_host: str = "gate1-harry261"
    job_count: int = JOBS_PER_ACCOUNT
    # 잡 컨테이너가 돌릴 production 슬롯 서비스 스크립트(baseline 자기공급, 무한).
    job_command: str = "bash $HOME/edt-deploy/slot_service.sh"
    dashboard_port: int = 8080
    intake_port: int = 7875
    ingest_port: int = DEFAULT_INGEST_PORT  # 슈퍼컴 전용 결과 백채널(역터널로만 도달).
    bulk_port: int = DEFAULT_BULK_PORT  # 슈퍼컴 전용 대용량 산출물 백채널(역터널로만 도달).
    poll_interval_seconds: float = 60.0
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전). 예: "0.3.7".


@dataclass
class ControlPlane:
    orchestrator: JobOrchestrator
    store: SingleSimulationResultStore
    intake: IntakeService
    archive_store: ArchiveStore
    dashboard_port: int
    intake_port: int
    poll_interval_seconds: float
    ssh_host: str = "gate1-harry261"
    ingest_port: int = DEFAULT_INGEST_PORT
    bulk_port: int = DEFAULT_BULK_PORT
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전).
    resource_poller: ResourcePoller | None = None  # 컨테이너별 실시간 부하(대시보드 /api/resources).
    enable_ingest_tunnel: bool = True  # 테스트에선 ssh 역터널 비활성.
    # 역할 분리: web=대시보드/intake/ingest/bulk/터널/아카이브/폴러(DB 보유), keeper=오케스트레이터(9잡 유지, DB 무관).
    # 둘 다 True면 단일 프로세스(기존 동작). 분리 운영 시 web 재시작이 컨테이너를 건드리지 않는다(scancel은 keeper만).
    run_web: bool = True
    run_keeper: bool = True
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _servers: list[ThreadingHTTPServer] = field(default_factory=list, init=False, repr=False)
    _tunnels: list[SshTunnel] = field(default_factory=list, init=False, repr=False)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        """서버 기동 + 오케스트레이터 상시 유지 루프. SIGTERM/SIGINT까지."""
        # systemd는 메인 스레드에서 돈다. 비메인 스레드(테스트 등)에선 시그널 등록이 불가하므로 무시.
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._on_signal)
        except ValueError:
            pass

        # --- web 데이터 플레인(대시보드/intake/ingest/bulk/터널/폴러) — DB 보유 측 ---
        if self.run_web:
            # 컨테이너별 실시간 부하 폴러 시작(있으면) → 대시보드 /api/resources.
            provider = None
            history = None
            if self.resource_poller is not None:
                self.resource_poller.start()
                provider = self.resource_poller.snapshot
                history = self.resource_poller.history  # 시계열(추세 탭) ring buffer
            dashboard = start_dashboard_server(
                store=self.store, port=self.dashboard_port, resource_provider=provider, history_provider=history,
                peetsfea_version=self.dashboard_peetsfea_version,
            )
            intake_server = start_intake_server(service=self.intake, port=self.intake_port)
            # 결과 ingest(:7876): 슈퍼컴 컨테이너가 역터널로 push → 로컬 단일 DB(대시보드와 동일 store).
            ingest_server = start_result_ingest_server(store=self.store, port=self.ingest_port)
            # 대용량 산출물(:7877): project_dir tar.gz 스트림 수신 → 추출 → ArchiveStore(20GB 묶음/2TB FIFO).
            bulk_server = start_bulk_transfer_server(archive_store=self.archive_store, port=self.bulk_port)
            for server in (dashboard, intake_server, ingest_server, bulk_server):
                self._servers.append(server)
                threading.Thread(target=server.serve_forever, daemon=True, name=type(server).__name__).start()

            # gate 경유 역터널 상시 유지: gate loopback:{7876,7877} → 로컬 ingest/bulk. 둘 다 슈퍼컴 전용.
            if self.enable_ingest_tunnel:
                for port, name in ((self.ingest_port, "edt-ingest-rtunnel"), (self.bulk_port, "edt-bulk-rtunnel")):
                    tunnel = SshTunnel(argv=reverse_tunnel_argv(self.ssh_host, port=port), name=name)
                    tunnel.start()
                    self._tunnels.append(tunnel)

        # --- keeper 컨트롤 플레인(오케스트레이터) — 컨테이너 유지 측. scancel은 여기서만 발생 ---
        if self.run_keeper:
            self.orchestrator.ensure_running()
        try:
            while not self._stop.is_set():
                if self.run_keeper:
                    self.orchestrator.poll()  # 죽은 잡 재기동, 10h 만료 폐기·재기동
                self._stop.wait(self.poll_interval_seconds)
        finally:
            # 각 단계는 독립적으로 best-effort: 하나가 실패/지연해도 나머지 teardown을 막지 않는다.
            def _safe(label: str, fn: "Callable[[], object]") -> None:
                try:
                    fn()
                except Exception:  # noqa: BLE001 — teardown은 절대 예외로 중단되면 안 된다.
                    pass

            # scancel은 keeper 역할에서만: web 단독 재시작이 컨테이너를 죽이지 않게 한다(분리 운영의 핵심).
            if self.run_keeper:
                _safe("orchestrator", self.orchestrator.shutdown)  # 잡 scancel(TERM); ssh ConnectTimeout=10
            if self.run_web:
                if self.resource_poller is not None:
                    _safe("poller", self.resource_poller.stop)
                for tunnel in self._tunnels:
                    _safe("tunnel", tunnel.stop)
                for server in self._servers:
                    _safe("server", server.shutdown)
                # 아카이브 flush(대기 묶음 tar.gz 압축)는 수 분 걸릴 수 있어 systemd TimeoutStopSec를 넘기면
                # 서비스가 'failed'로 죽는다. 별도 스레드 + 시한으로 감싸 시한 초과 시 다음 기동에 위임(유실 아님).
                flusher = threading.Thread(target=lambda: _safe("flush", self.archive_store.flush), daemon=True)
                flusher.start()
                flusher.join(timeout=SHUTDOWN_FLUSH_DEADLINE_SECONDS)

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        self.stop()


def build_control_plane(
    config: ControlPlaneConfig,
    *,
    launcher: JobLauncher | None = None,
    run_web: bool = True,
    run_keeper: bool = True,
) -> ControlPlane:
    store = SingleSimulationResultStore(db_path=config.db_path)
    # DB 초기화(스키마 보장)는 web 역할에서만 — keeper는 DB를 안 쓰므로 부팅 시 writer 락 경합을 피한다.
    if run_web:
        store.initialize()
    archive_store = ArchiveStore(archive_root=config.archive_root, buffer_limit_bytes=config.archive_buffer_bytes)
    job_launcher = launcher if launcher is not None else SlurmJobLauncher(
        ssh_host=config.ssh_host,
        job_command=config.job_command,
    )
    orchestrator = JobOrchestrator(launcher=job_launcher, clock=time.monotonic, job_count=config.job_count)
    # 호스트측 Intake 큐(우선순위 분배는 후속; 여기선 수신·검증·샘플까지).
    intake = IntakeService(queue=TwoLaneQueue())
    return ControlPlane(
        orchestrator=orchestrator,
        store=store,
        intake=intake,
        archive_store=archive_store,
        dashboard_port=config.dashboard_port,
        intake_port=config.intake_port,
        poll_interval_seconds=config.poll_interval_seconds,
        ssh_host=config.ssh_host,
        ingest_port=config.ingest_port,
        bulk_port=config.bulk_port,
        dashboard_peetsfea_version=config.dashboard_peetsfea_version,
        resource_poller=ResourcePoller(ssh_host=config.ssh_host),
        run_web=run_web,
        run_keeper=run_keeper,
    )


def run_control_plane(config: ControlPlaneConfig, *, run_web: bool = True, run_keeper: bool = True) -> None:
    build_control_plane(config, run_web=run_web, run_keeper=run_keeper).run()


def main() -> int:
    """systemd `--user` 진입점. 설정은 환경변수로(EDT_DB_PATH 필수).

    `EDT_ROLE`로 역할 선택: `all`(기본, 단일 프로세스) / `web`(대시보드·데이터 플레인) / `keeper`(컨테이너 유지).
    분리 운영 시 web 유닛은 scancel 안 함 → web 재시작이 컨테이너를 건드리지 않는다.
    """
    import os

    role = os.environ.get("EDT_ROLE", "all").strip().lower()
    if role not in {"all", "web", "keeper"}:
        raise ValueError(f"EDT_ROLE must be one of all|web|keeper, got {role!r}")
    run_web = role in {"all", "web"}
    run_keeper = role in {"all", "keeper"}

    config = ControlPlaneConfig(
        db_path=Path(os.environ.get("EDT_DB_PATH", "~/edt-deploy/results.duckdb")).expanduser(),
        archive_root=Path(os.environ.get("EDT_ARCHIVE_ROOT", "~/edt-archive")).expanduser(),
        archive_buffer_bytes=int(os.environ.get("EDT_ARCHIVE_BUFFER_BYTES", str(ARCHIVE_BUFFER_BYTES))),
        ssh_host=os.environ.get("EDT_SSH_HOST", "gate1-harry261"),
        job_count=int(os.environ.get("EDT_JOB_COUNT", str(JOBS_PER_ACCOUNT))),
        dashboard_port=int(os.environ.get("EDT_DASHBOARD_PORT", "8080")),
        intake_port=int(os.environ.get("EDT_INTAKE_PORT", "7875")),
        ingest_port=int(os.environ.get("EDT_INGEST_PORT", str(DEFAULT_INGEST_PORT))),
        bulk_port=int(os.environ.get("EDT_BULK_PORT", str(DEFAULT_BULK_PORT))),
        dashboard_peetsfea_version=os.environ.get("EDT_DASHBOARD_PEETSFEA_VERSION", "").strip(),
    )
    run_control_plane(config, run_web=run_web, run_keeper=run_keeper)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ControlPlane", "ControlPlaneConfig", "build_control_plane", "main", "run_control_plane"]
