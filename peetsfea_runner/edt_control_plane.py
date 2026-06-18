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
from .edt_license_ctrl import DEFAULT_LICENSE_CTRL_PORT, LicenseController, start_license_ctrl_server
from .edt_orchestrator import JobLauncher, JobOrchestrator
from .edt_priority_lease import DEFAULT_PRIORITY_LEASE_PORT, start_priority_lease_server
from .edt_resources import ResourcePoller
from .edt_result_ingest import DEFAULT_INGEST_PORT, start_result_ingest_server
from .edt_slurm_launcher import SlurmJobLauncher
from .edt_ssh_tunnel import SshTunnel, reverse_tunnel_argv
from .single_simulation_store import DbPriorityQueue, SingleSimulationResultStore


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
    priority_lease_port: int = DEFAULT_PRIORITY_LEASE_PORT  # 슈퍼컴 전용 우선순위 분배 백채널(역터널로만 도달).
    poll_interval_seconds: float = 60.0
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전). 예: "0.3.7".
    # 잡 제출 전략: 노드 기반(빈 노드에 --nodelist 핀, 내 잡 도는 노드 제외) + 순차 램프(한 잡 RUNNING 후 다음).
    node_based_jobs: bool = True
    sequential_ramp: bool = True
    # 라이선스 피드백 제어(:7879): 전역 동시 솔브를 target~ceiling 밴드로. lic_mine은 poller에서.
    license_ctrl_port: int = DEFAULT_LICENSE_CTRL_PORT
    license_target: int = 100  # permit 상한(<100이면 더 솔브)
    license_ceiling: int = 150  # 초과 시 youngest abort
    license_poll_seconds: float = 60.0  # 솔브 ~14분이라 1분이면 충분


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
    priority_lease_port: int = DEFAULT_PRIORITY_LEASE_PORT
    license_ctrl_port: int = DEFAULT_LICENSE_CTRL_PORT
    license_target: int = 100
    license_ceiling: int = 150
    license_poll_seconds: float = 60.0
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전).
    resource_poller: ResourcePoller | None = None  # 컨테이너별 실시간 부하(대시보드 /api/resources).
    enable_ingest_tunnel: bool = True  # 테스트에선 ssh 역터널 비활성.
    # 역할 분리: web=대시보드/intake/ingest/bulk/터널/아카이브/폴러(DB 보유), keeper=오케스트레이터(9잡 유지, DB 무관).
    # 둘 다 True면 단일 프로세스(기존 동작). 분리 운영 시 web 재시작이 컨테이너를 건드리지 않는다(scancel은 keeper만).
    run_web: bool = True
    run_keeper: bool = True
    license_controller: LicenseController | None = field(default=None, init=False, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _servers: list[ThreadingHTTPServer] = field(default_factory=list, init=False, repr=False)
    _tunnels: list[SshTunnel] = field(default_factory=list, init=False, repr=False)

    def stop(self) -> None:
        self._stop.set()

    def _lic_mine(self) -> int:
        """제어기 lic_provider — 내 **솔브 라이선스 사용수**(elec_solve_hfss). poller 스냅샷에서.

        electronics_desktop(=열린 데스크톱)가 아니라 solve feature를 기준으로 묶는다:
        데스크톱은 450까지 오버슛해도 되고, 매 순간 솔브를 target~ceiling(100~150)에 둔다.
        solve 필드가 없으면(구버전/파싱실패) 0 → max(0, len(_active))로 _active가 가드.
        """
        if self.resource_poller is None:
            return 0
        lic = self.resource_poller.snapshot().get("license") or {}
        try:
            return int(lic.get("solve_mine") or 0)
        except (TypeError, ValueError):
            return 0

    def _license_poll_loop(self) -> None:
        while not self._stop.is_set():
            if self.license_controller is not None:
                try:
                    self.license_controller.poll()
                except Exception:  # noqa: BLE001 — 제어 루프가 데몬을 죽이면 안 된다.
                    pass
            self._stop.wait(self.license_poll_seconds)

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
            # 우선순위 분배(:7878): intake(:7875)가 채운 우선순위 레인을 슈퍼컴 워커들에 lease로 분배.
            lease_server = start_priority_lease_server(queue=self.intake.queue, port=self.priority_lease_port)
            # 라이선스 제어(:7879): lic_mine을 읽어 전역 동시 솔브를 target~ceiling 밴드로. 워커가 permit/heartbeat.
            controller = LicenseController(
                lic_provider=self._lic_mine, target=self.license_target, ceiling=self.license_ceiling,
            )
            self.license_controller = controller
            # 추세의 AEDT 명목(열린 데스크톱)/유효(솔브중)는 _history_point가 lmstat 실측으로 채운다.
            # (제어기 내부 ping 집계는 솔브 사이 idle 워커를 놓쳐 과소계상되므로 extra_provider로 덮지 않는다.)
            if self.resource_poller is not None:
                self.resource_poller.aedt_provider = controller.per_job  # 컨테이너(잡)별 pyaedt 수 → 부하 탭
            license_server = start_license_ctrl_server(controller=controller, port=self.license_ctrl_port)
            for server in (dashboard, intake_server, ingest_server, bulk_server, lease_server, license_server):
                self._servers.append(server)
                threading.Thread(target=server.serve_forever, daemon=True, name=type(server).__name__).start()
            # 제어 루프(1분): lic_mine 갱신 + ceiling 초과 시 youngest abort 표시.
            threading.Thread(target=self._license_poll_loop, daemon=True, name="license-ctrl-poll").start()

            # gate 경유 역터널 상시 유지: gate loopback:{7876,7877,7878,7879} → 로컬 ingest/bulk/lease/lic. 전부 슈퍼컴 전용.
            if self.enable_ingest_tunnel:
                for port, name in (
                    (self.ingest_port, "edt-ingest-rtunnel"),
                    (self.bulk_port, "edt-bulk-rtunnel"),
                    (self.priority_lease_port, "edt-priority-rtunnel"),
                    (self.license_ctrl_port, "edt-license-rtunnel"),
                ):
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
        node_based=config.node_based_jobs,
    )
    orchestrator = JobOrchestrator(
        launcher=job_launcher,
        clock=time.monotonic,
        job_count=config.job_count,
        sequential_ramp=config.sequential_ramp,
    )
    # 호스트측 Intake 큐 — DB 영속(web 재시작에도 미처리 우선순위 항목 보존). lease :7878이 같은 큐를 분배.
    intake = IntakeService(queue=DbPriorityQueue(store=store))
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
        priority_lease_port=config.priority_lease_port,
        license_ctrl_port=config.license_ctrl_port,
        license_target=config.license_target,
        license_ceiling=config.license_ceiling,
        license_poll_seconds=config.license_poll_seconds,
        dashboard_peetsfea_version=config.dashboard_peetsfea_version,
        # 라이선스/자원 시계열을 DB에 영속(web 재시작·12h ring buffer 넘어 보존) + 7일 넘은 건 자동 prune.
        resource_poller=ResourcePoller(
            ssh_host=config.ssh_host,
            history_sink=store.record_resource_snapshot,
            history_prune=lambda cutoff: store.prune_resource_snapshots(before_ts=cutoff),
        ),
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
        priority_lease_port=int(os.environ.get("EDT_PRIORITY_LEASE_PORT", str(DEFAULT_PRIORITY_LEASE_PORT))),
        license_ctrl_port=int(os.environ.get("EDT_LICENSE_CTRL_PORT", str(DEFAULT_LICENSE_CTRL_PORT))),
        license_target=int(os.environ.get("EDT_LICENSE_TARGET", "100")),
        license_ceiling=int(os.environ.get("EDT_LICENSE_CEILING", "150")),
        license_poll_seconds=float(os.environ.get("EDT_LICENSE_POLL_SECONDS", "60")),
        dashboard_peetsfea_version=os.environ.get("EDT_DASHBOARD_PEETSFEA_VERSION", "").strip(),
    )
    run_control_plane(config, run_web=run_web, run_keeper=run_keeper)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ControlPlane", "ControlPlaneConfig", "build_control_plane", "main", "run_control_plane"]
