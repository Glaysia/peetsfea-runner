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
from dataclasses import dataclass, field
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import FrameType

from .constants import ARCHIVE_BUFFER_BYTES, JOBS_PER_ACCOUNT
from .edt_archive import ArchiveStore
from .edt_bulk_transfer import DEFAULT_BULK_PORT, start_bulk_transfer_server
from .edt_dashboard import start_dashboard_server
from .edt_intake import IntakeService, start_intake_server
from .edt_orchestrator import JobLauncher, JobOrchestrator
from .edt_queue import TwoLaneQueue
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
    enable_ingest_tunnel: bool = True  # 테스트에선 ssh 역터널 비활성.
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

        dashboard = start_dashboard_server(store=self.store, port=self.dashboard_port)
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

        self.orchestrator.ensure_running()
        try:
            while not self._stop.is_set():
                self.orchestrator.poll()  # 죽은 잡 재기동, 10h 만료 폐기·재기동
                self._stop.wait(self.poll_interval_seconds)
        finally:
            self.orchestrator.shutdown()
            for tunnel in self._tunnels:
                tunnel.stop()
            for server in self._servers:
                server.shutdown()
            self.archive_store.flush()  # 종료 시 대기 중인 묶음 강제 압축(유실 방지).

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        self.stop()


def build_control_plane(config: ControlPlaneConfig, *, launcher: JobLauncher | None = None) -> ControlPlane:
    store = SingleSimulationResultStore(db_path=config.db_path)
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
    )


def run_control_plane(config: ControlPlaneConfig) -> None:
    build_control_plane(config).run()


def main() -> int:
    """systemd `--user` 진입점. 설정은 환경변수로(EDT_DB_PATH 필수)."""
    import os

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
    )
    run_control_plane(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ControlPlane", "ControlPlaneConfig", "build_control_plane", "main", "run_control_plane"]
