"""컨트롤 플레인 — 정상상태 동시 ~100 연속 가동.

제어 호스트(systemd `--user` 서비스)가 돌리는 상시 루프:
- **오케스트레이터:** HTML 정책 기준으로 2분마다 홀수 tick은 새 잡 제출, 짝수 tick은 oldest 종료를 수행한다.
  잡 내부에서는 `deploy/orchestrator.sh`가 1솔브 단명 enroot 컨테이너 N개를 stagger 가동한다.
- **Adaptive TOML Registry :7875:** built-in + custom TOML active pool과 ratio를 영속 관리한다.
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
from typing import Any

# 종료 시 아카이브 flush(대용량 tar.gz 압축)에 줄 최대 시간. 초과하면 다음 기동에 위임(systemd 'failed' 방지).
SHUTDOWN_FLUSH_DEADLINE_SECONDS = 45.0

from .constants import JOBS_PER_ACCOUNT
from .edt_data_api import create_data_api_app
from .edt_dashboard import start_dashboard_server
from .edt_license_ctrl import (
    DEFAULT_LICENSE_CTRL_PORT,
    ContainerScheduler,
    LicenseController,
    start_license_ctrl_server,
)
from .edt_orchestrator import JobLauncher, JobOrchestrator
from .edt_priority_lease import DEFAULT_PRIORITY_LEASE_PORT, start_priority_lease_server
from .edt_resources import (
    DEFAULT_RESOURCE_PORT,
    RemoteResourceProvider,
    ResourcePoller,
    start_resource_server,
)
from .edt_result_ingest import DEFAULT_INGEST_PORT, start_result_ingest_server
from .edt_slurm_launcher import SlurmJobLauncher
from .edt_ssh_tunnel import SshTunnel, reverse_tunnel_argv
from .edt_toml_registry import TomlRegistryService, load_default_builtin_toml_text, start_toml_registry_server
from .single_simulation_store import DbTomlRegistry, SingleSimulationResultStore, make_result_store


@dataclass(slots=True)
class AccountConfig:
    """다계정: 한 계정의 식별·게이트·포트·지령. ControlPlaneConfig.accounts에 담긴다.

    같은 control plane 프로세스가 계정 리스트를 돌려 각 계정의 스택(런처/오케스트레이터/폴러/
    라이선스·스케줄러/ingest·lease·license 서버 + 역터널)을 띄운다. 결과는 단일 DB에 account_id로 태깅.
    포트는 계정별로 분리(harry261=7876대, hmlee31=7886대)해 같은 로컬 머신에서 충돌 없이 공존.
    """

    account_id: str = "account_01"
    host_alias: str = "gate1-harry261"
    ssh_host: str = "gate1-harry261"
    account_index: int = 0
    job_count: int = JOBS_PER_ACCOUNT
    ingest_port: int = DEFAULT_INGEST_PORT  # 7876
    priority_lease_port: int = DEFAULT_PRIORITY_LEASE_PORT  # 7878
    license_ctrl_port: int = DEFAULT_LICENSE_CTRL_PORT  # 7879
    resource_port: int = DEFAULT_RESOURCE_PORT  # 7882
    license_target: int = 100
    license_ceiling: int = 220


@dataclass
class AccountRuntime:
    """한 계정의 런타임 스택. ControlPlane.accounts에 담겨 run()이 계정마다 돌린다.

    공유(단일): store / toml_registry / dashboard / intake / resources_store.
    계정별: launcher→orchestrator, resource_poller, LicenseController, ContainerScheduler,
    license/job_plan 서버(license_ctrl_port), ingest 서버(ingest_port), lease 서버(priority_lease_port),
    resource 서버(resource_port) + ingest/lease/license 역터널(ssh_host).
    """

    account_id: str
    host_alias: str
    ssh_host: str
    orchestrator: JobOrchestrator
    resource_poller: ResourcePoller | None
    ingest_port: int
    priority_lease_port: int
    license_ctrl_port: int
    resource_port: int
    license_target: int
    license_ceiling: int
    license_controller: LicenseController | None = field(default=None, init=False)
    container_scheduler: ContainerScheduler | None = field(default=None, init=False)


@dataclass(slots=True)
class ControlPlaneConfig:
    db_path: Path
    ssh_host: str = "gate1-harry261"
    job_count: int = JOBS_PER_ACCOUNT
    # 다계정: 비면 아래 단일계정 필드(ssh_host·*_port·job_count·account_index)로 계정 1개를 만든다(기존 동작).
    # 채우면 각 AccountConfig마다 계정 스택을 띄운다(harry261 + hmlee31 …).
    accounts: tuple[AccountConfig, ...] = ()
    # 잡 내부 서브 오케스트레이터: 1솔브 단명 enroot 컨테이너 N개를 띄우고 respawn 없이 종료.
    job_command: str = "bash $HOME/edt-deploy/orchestrator.sh"
    dashboard_port: int = 8080
    intake_port: int = 7875  # historical env name; this now serves Adaptive TOML Registry.
    ingest_port: int = DEFAULT_INGEST_PORT  # 슈퍼컴 전용 결과 백채널(역터널로만 도달).
    priority_lease_port: int = DEFAULT_PRIORITY_LEASE_PORT  # 슈퍼컴 전용 우선순위 분배 백채널(역터널로만 도달).
    data_api_port: int = 7884  # read 평면: 학습 주체용 증분 Arrow 스트림 API(EDT_ROLE=data, uvicorn).
    poll_interval_seconds: float = 60.0
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전). 예: "0.3.7".
    # 잡 제출 전략: 노드 기반(빈 노드에 --nodelist 핀, 내 잡 도는 노드 제외). 잡은 고정 인프라(적분제어).
    node_based_jobs: bool = True
    # 잡별 디버그 sshd: 잡 노드 22를 게이트 결정적 포트로 역터널(0=비활성, 7900 권장). 계정 stride로 다계정
    # 충돌 회피(PLANS/per_job_debug_access.html). EDT_DEBUG_SSHD_BASE / EDT_ACCOUNT_INDEX env로 설정.
    debug_sshd_base: int = 0
    debug_account_stride: int = 50
    account_index: int = 0
    # 라이선스 피드백 제어(:7879): 전역 동시 솔브를 target~ceiling 밴드로. lic_mine은 poller에서.
    license_ctrl_port: int = DEFAULT_LICENSE_CTRL_PORT
    license_target: int = 100  # permit 상한(<100이면 더 솔브)
    license_ceiling: int = 150  # 초과 시 youngest abort
    license_poll_seconds: float = 60.0  # 솔브 ~14분이라 1분이면 충분
    # 자원 백채널(control→web): 폴러를 무거운 데이터플레인(web)과 분리해 control(keeper)에서 돌리고
    # 대시보드는 이 포트로 프록시한다. 같은 호스트의 로컬 루프백.
    resource_port: int = DEFAULT_RESOURCE_PORT
    resources_db_path: Path | None = None  # control측 자원 시계열 전용 소형 DB(미지정 시 db_path 옆 .resources).
    builtin_toml_path: Path | None = None


@dataclass
class ControlPlane:
    store: SingleSimulationResultStore
    toml_registry: TomlRegistryService
    dashboard_port: int
    intake_port: int
    poll_interval_seconds: float
    # 다계정: 계정별 런타임 스택 리스트. run()이 각 계정마다 서버·터널·오케스트레이터를 띄운다.
    accounts: list[AccountRuntime] = field(default_factory=list)
    data_api_port: int = 7884
    license_poll_seconds: float = 60.0
    dashboard_peetsfea_version: str = ""  # 대시보드 표시 버전 필터(빈 값=전 버전).
    resources_store: SingleSimulationResultStore | None = None  # control측: 자원 시계열 전용 소형 DB.
    resource_provider: RemoteResourceProvider | None = None  # web측: control의 자원 엔드포인트 프록시.
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

    @staticmethod
    def _lic_mine(acct: AccountRuntime) -> int:
        """제어기 lic_provider — 그 계정의 **솔브 라이선스 사용수**(elec_solve_hfss). poller 스냅샷에서.

        electronics_desktop(=열린 데스크톱)가 아니라 solve feature를 기준으로 묶는다:
        데스크톱은 450까지 오버슛해도 되고, 매 순간 솔브를 target~ceiling(100~150)에 둔다.
        solve 필드가 없으면(구버전/파싱실패) 0 → max(0, len(_active))로 _active가 가드.
        """
        if acct.resource_poller is None:
            return 0
        lic = acct.resource_poller.snapshot().get("license") or {}
        try:
            return int(lic.get("solve_mine") or 0)
        except (TypeError, ValueError):
            return 0

    def _license_poll_loop(self) -> None:
        # 컨테이너 스케줄러는 ~10s마다 tick(폴러 캐시 스냅샷 읽기, 가벼움). 라이선스 제어기 poll(lmstat ssh, 무거움)은
        # license_poll_seconds마다. 콜드스타트 lag을 감안해 step·tick은 보수적(과spawn 방지) — 라이브 튜닝 대상.
        import time as _t
        last_ctrl = 0.0
        while not self._stop.is_set():
            now = _t.monotonic()
            ctrl_due = (now - last_ctrl) >= self.license_poll_seconds
            for acct in self.accounts:
                if acct.license_controller is not None and ctrl_due:
                    try:
                        acct.license_controller.poll()
                    except Exception:  # noqa: BLE001 — 제어 루프가 데몬을 죽이면 안 된다.
                        pass
                if acct.container_scheduler is not None:
                    try:
                        acct.container_scheduler.tick()
                    except Exception:  # noqa: BLE001
                        pass
            if ctrl_due:
                last_ctrl = now
            self._stop.wait(10.0)

    def run(self) -> None:
        """서버 기동 + 오케스트레이터 상시 유지 루프. SIGTERM/SIGINT까지."""
        # systemd는 메인 스레드에서 돈다. 비메인 스레드(테스트 등)에선 시그널 등록이 불가하므로 무시.
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._on_signal)
        except ValueError:
            pass

        # --- control 플레인(keeper측): 폴러 + 라이선스 제어기 — 경량, 무거운 데이터플레인과 격리 ---
        # 폴러를 web과 분리해 여기서 돌리면, web이 OOM/fd-고갈로 허덕여도 텔레메트리가 안 끊긴다(차트 빈 구간 해소).
        # 다계정: 계정마다 폴러/제어기/스케줄러 + license 서버·resource 서버 + license 역터널을 띄운다.
        if self.run_keeper:
            for acct in self.accounts:
                if acct.resource_poller is not None:
                    acct.resource_poller.start()
                # 라이선스 제어(:license_ctrl_port): solve 수를 읽어 동시 솔브를 target~ceiling 밴드로.
                controller = LicenseController(
                    lic_provider=(lambda a=acct: self._lic_mine(a)),
                    target=acct.license_target,
                    ceiling=acct.license_ceiling,
                )
                acct.license_controller = controller
                if acct.resource_poller is not None:
                    acct.resource_poller.aedt_provider = controller.per_job  # 컨테이너(잡)별 pyaedt 수 → 부하 탭
                    # 적분 컨테이너 제어: 잡은 고정, 잡별 컨테이너 목표를 /job_plan으로 동적 서빙.
                    acct.container_scheduler = ContainerScheduler(
                        snapshot_provider=acct.resource_poller.snapshot,
                    )
                    # container_count_provider 미주입(None) → orchestrator가 /job_plan?job=i를 주기
                    # 재조회해 respawn-to-N. solve_provider는 관측/상태용으로 유지.
                    if acct.orchestrator is not None:
                        acct.orchestrator.solve_provider = acct.container_scheduler.current_solve
                license_server = start_license_ctrl_server(
                    controller=controller, scheduler=acct.container_scheduler, port=acct.license_ctrl_port
                )
                self._servers.append(license_server)
                threading.Thread(target=license_server.serve_forever, daemon=True, name="LicenseServer").start()
                # 자원 백채널(:resource_port): web 대시보드가 폴러 스냅샷 + 전용 자원 DB 시계열을 HTTP 프록시로 읽는다.
                if acct.resource_poller is not None:
                    _rstore = self.resources_store
                    history_fetch: Callable[[float | None], list[dict[str, Any]]] | None = None
                    if _rstore is not None:
                        def history_fetch(since_ts: float | None = None, _rstore=_rstore) -> list[dict[str, Any]]:
                            return _rstore.fetch_resource_history(since_ts=since_ts)

                    resource_server = start_resource_server(
                        poller=acct.resource_poller,
                        port=acct.resource_port,
                        history_fetch=history_fetch,
                    )
                    self._servers.append(resource_server)
                    threading.Thread(
                        target=resource_server.serve_forever, daemon=True, name="ResourceServer"
                    ).start()
                # 라이선스 역터널만 control측: gate loopback:license_ctrl_port → 로컬 제어기.
                if self.enable_ingest_tunnel:
                    tunnel = SshTunnel(
                        argv=reverse_tunnel_argv(acct.ssh_host, port=acct.license_ctrl_port),
                        name="edt-license-rtunnel",
                    )
                    tunnel.start()
                    self._tunnels.append(tunnel)
            # 라이선스/스케줄러 백채널 루프(전 계정을 한 스레드가 순회).
            threading.Thread(target=self._license_poll_loop, daemon=True, name="license-ctrl-poll").start()

        # --- web 데이터 플레인(대시보드/intake/ingest/bulk) — 무거운 DB·I/O 측 ---
        if self.run_web:
            # 대시보드/intake는 단일(공유). 자원 프로바이더는 primary(첫) 계정 폴러 기준.
            primary = self.accounts[0] if self.accounts else None
            primary_poller = primary.resource_poller if primary is not None else None
            # 자원 프로바이더: 같은 프로세스에 폴러가 있으면(all 모드) 라이브는 폴러 직접·추세는 자원DB 영속,
            # 없으면(web 전용) control(:resource_port)에 HTTP 프록시. history는 둘 다 since_ts를 받는다.
            provider: Callable[[], dict[str, Any]] | None
            history: Callable[[float | None], list[dict[str, Any]]] | None
            if primary_poller is not None:
                provider = primary_poller.snapshot
                _rs, _rp = self.resources_store, primary_poller
                if _rs is not None:
                    def history(since_ts: float | None = None) -> list[dict[str, Any]]:
                        return _rs.fetch_resource_history(since_ts=since_ts)
                else:
                    def history(since_ts: float | None = None) -> list[dict[str, Any]]:
                        return _rp.history()
            elif self.resource_provider is not None:
                provider, history = self.resource_provider.snapshot, self.resource_provider.history
            else:
                provider = history = None
            dashboard = start_dashboard_server(
                store=self.store, port=self.dashboard_port, resource_provider=provider, history_provider=history,
                peetsfea_version=self.dashboard_peetsfea_version, toml_registry=self.toml_registry,
            )
            toml_registry_server = start_toml_registry_server(service=self.toml_registry, port=self.intake_port)
            for server in (dashboard, toml_registry_server):
                self._servers.append(server)
                threading.Thread(target=server.serve_forever, daemon=True, name=type(server).__name__).start()
            # 계정별 ingest/lease 서버 + 역터널. 결과는 단일 store로(account_id 태깅).
            for acct in self.accounts:
                # 결과 ingest(:ingest_port): 슈퍼컴 컨테이너가 역터널로 push → 로컬 단일 DB.
                ingest_server = start_result_ingest_server(store=self.store, port=acct.ingest_port)
                # TOML lease(:priority_lease_port): registry(:7875)의 active pool에서 ratio 기반으로 워커에 분배.
                lease_server = start_priority_lease_server(queue=self.toml_registry, port=acct.priority_lease_port)
                # bulk(:7877) 산출물 sink 제거됨 — 학습은 DB 결과 행만 쓰고, raw 아카이브는 out-of-band.
                # (구 단일-락 압축 sink가 스레드/소켓 누수로 FD死를 일으켰음. PLANS/data_plane_overhaul.html)
                for server in (ingest_server, lease_server):
                    self._servers.append(server)
                    threading.Thread(target=server.serve_forever, daemon=True, name=type(server).__name__).start()
                # 역터널: ingest/lease(라이선스는 control측). 전부 슈퍼컴 전용.
                if self.enable_ingest_tunnel:
                    for port, name in (
                        (acct.ingest_port, "edt-ingest-rtunnel"),
                        (acct.priority_lease_port, "edt-priority-rtunnel"),
                    ):
                        tunnel = SshTunnel(argv=reverse_tunnel_argv(acct.ssh_host, port=port), name=name)
                        tunnel.start()
                        self._tunnels.append(tunnel)

        # --- keeper 오케스트레이터 기동(컨테이너 유지). scancel은 keeper 역할에서만 ---
        if self.run_keeper:
            for acct in self.accounts:
                acct.orchestrator.ensure_running()
        try:
            while not self._stop.is_set():
                for acct in self.accounts:
                    if self.run_keeper:
                        acct.orchestrator.poll()
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
                for acct in self.accounts:
                    _safe("orchestrator", acct.orchestrator.shutdown)  # 잡 scancel(TERM); ssh ConnectTimeout=10
                    if acct.resource_poller is not None:
                        _safe("poller", acct.resource_poller.stop)  # 폴러는 control(keeper)측.
            # 터널·서버는 양쪽 역할에서 생길 수 있으니(이 프로세스가 만든 것만 _tunnels/_servers에 있음) 모두 정리.
            for tunnel in self._tunnels:
                _safe("tunnel", tunnel.stop)
            for server in self._servers:
                _safe("server", server.shutdown)

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        self.stop()


def build_control_plane(
    config: ControlPlaneConfig,
    *,
    launcher: JobLauncher | None = None,
    run_web: bool = True,
    run_keeper: bool = True,
) -> ControlPlane:
    store = make_result_store(config.db_path)  # EDT_STORE_BACKEND=postgres면 PostgresResultStore.
    # 결과 DB(11GB)는 web 역할(대시보드/ingest/intake/lease)만 초기화·소유 — 단일 writer 락 경합 회피.
    if run_web:
        store.initialize()
        # 레거시 priority_sweeps 정리. 새 Adaptive TOML registry는 finite intake 큐가 아니므로 정리 대상이 아니다.
        try:
            store.settle_priority_sweeps()
        except Exception:  # noqa: BLE001 — 정리 실패가 기동을 막으면 안 된다.
            pass
    # 다계정: config.accounts가 비면 레거시 단일계정 필드로 계정 1개를 합성(기존 동작 보존).
    account_configs: tuple[AccountConfig, ...] = config.accounts
    if not account_configs:
        account_configs = (
            AccountConfig(
                account_id="account_01",
                host_alias=config.ssh_host,
                ssh_host=config.ssh_host,
                account_index=config.account_index,
                job_count=config.job_count,
                ingest_port=config.ingest_port,
                priority_lease_port=config.priority_lease_port,
                license_ctrl_port=config.license_ctrl_port,
                resource_port=config.resource_port,
                license_target=config.license_target,
                license_ceiling=config.license_ceiling,
            ),
        )
    # control(keeper)측 폴러는 결과 DB가 아니라 **전용 소형 자원 DB**에 시계열을 영속한다(공유).
    # 그래야 web의 무거운 쿼리/락과 분리되고, keeper 프로세스가 11GB DB를 안 연다.
    resources_store = None
    resource_provider = None
    if run_keeper:
        resources_db = config.resources_db_path or config.db_path.with_suffix(".resources.duckdb")
        resources_store = make_result_store(resources_db)  # PG 백엔드면 같은 PG(resource_snapshots 테이블).
        resources_store.initialize()
    elif run_web:
        # web 전용 프로세스: 폴러가 없으니 control(keeper)의 자원 엔드포인트를 HTTP로 프록시(primary 계정 포트).
        resource_provider = RemoteResourceProvider(
            base_url=f"http://127.0.0.1:{account_configs[0].resource_port}"
        )

    accounts: list[AccountRuntime] = []
    for acct_cfg in account_configs:
        # 테스트/단일계정: launcher를 명시 주입하면 그걸 쓴다(계정이 1개일 때만).
        if launcher is not None and len(account_configs) == 1:
            acct_launcher: JobLauncher = launcher
        else:
            acct_launcher = SlurmJobLauncher(
                ssh_host=acct_cfg.ssh_host,
                job_command=config.job_command,
                node_based=config.node_based_jobs,
                # 재램프 시 이미 푼 baseline seed를 재탕하지 않게: store의 used-seed 프런티어 위로 seed epoch advance.
                seed_epoch_provider=store.max_baseline_seed,
                # 잡별 디버그 sshd 역터널(0=비활성). 잡 노드에 ssh -J로 직접 진입.
                debug_sshd_base=config.debug_sshd_base,
                debug_account_stride=config.debug_account_stride,
                account_index=acct_cfg.account_index,
                account_id=acct_cfg.account_id,
                host_alias=acct_cfg.host_alias,
            )
        acct_orchestrator = JobOrchestrator(
            launcher=acct_launcher,
            clock=time.monotonic,
            job_count=acct_cfg.job_count,
        )
        acct_poller: ResourcePoller | None = None
        if run_keeper and resources_store is not None:
            acct_poller = ResourcePoller(
                ssh_host=acct_cfg.ssh_host,
                history_sink=resources_store.record_resource_snapshot,
                history_prune=lambda cutoff: resources_store.prune_resource_snapshots(before_ts=cutoff),
            )
        accounts.append(
            AccountRuntime(
                account_id=acct_cfg.account_id,
                host_alias=acct_cfg.host_alias,
                ssh_host=acct_cfg.ssh_host,
                orchestrator=acct_orchestrator,
                resource_poller=acct_poller,
                ingest_port=acct_cfg.ingest_port,
                priority_lease_port=acct_cfg.priority_lease_port,
                license_ctrl_port=acct_cfg.license_ctrl_port,
                resource_port=acct_cfg.resource_port,
                license_target=acct_cfg.license_target,
                license_ceiling=acct_cfg.license_ceiling,
            )
        )
    builtin_toml_text = (
        load_default_builtin_toml_text(config.builtin_toml_path)
        if run_web
        else "spec_version = 'keeper-only-placeholder'\n"
    )
    toml_registry = TomlRegistryService(
        registry=DbTomlRegistry(store=store),
        builtin_toml_text=builtin_toml_text,
    )
    if run_web:
        toml_registry.initialize()
    return ControlPlane(
        store=store,
        toml_registry=toml_registry,
        dashboard_port=config.dashboard_port,
        intake_port=config.intake_port,
        poll_interval_seconds=config.poll_interval_seconds,
        accounts=accounts,  # 계정별 런타임 스택(런처/오케스트레이터/폴러/포트).
        data_api_port=config.data_api_port,
        license_poll_seconds=config.license_poll_seconds,
        dashboard_peetsfea_version=config.dashboard_peetsfea_version,
        resources_store=resources_store,  # keeper측 자원 시계열 DB(영속 history 서빙용, 공유).
        resource_provider=resource_provider,  # web 전용 프로세스에서 control 자원 엔드포인트 프록시.
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
    if role not in {"all", "web", "keeper", "data"}:
        raise ValueError(f"EDT_ROLE must be one of all|web|keeper|data, got {role!r}")

    # read 평면(data): 학습 주체용 증분 Arrow 스트림 API를 uvicorn으로 단독 서빙(write 평면과 격리).
    # 무상태·읽기전용이라 ControlPlane 풀스택 불필요 — store만 열고 FastAPI/uvicorn으로 끝.
    if role == "data":
        import uvicorn

        store = make_result_store(Path(os.environ.get("EDT_DB_PATH", "~/edt-deploy/results.duckdb")).expanduser())
        app = create_data_api_app(store=store)
        uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("EDT_DATA_API_PORT", "7884")))
        return 0

    run_web = role in {"all", "web"}
    run_keeper = role in {"all", "keeper"}

    # 다계정: EDT_ACCOUNTS="ssh_host:account_id:account_index,..." 면 계정 튜플을 만든다.
    # 포트는 account_index에서 파생(idx*10 오프셋)해 같은 로컬 머신에서 충돌 없이 공존:
    #   ingest=7876+10*idx, priority_lease=7878+10*idx, license_ctrl=7879+10*idx, resource=7882+10*idx.
    # 미설정이면 accounts=()로 두어 레거시 단일계정 경로(아래 ControlPlaneConfig 단일 필드)를 그대로 탄다.
    accounts: tuple[AccountConfig, ...] = ()
    accounts_spec = os.environ.get("EDT_ACCOUNTS", "").strip()
    if accounts_spec:
        lic_target = int(os.environ.get("EDT_LICENSE_TARGET", "100"))
        lic_ceiling = int(os.environ.get("EDT_LICENSE_CEILING", "220"))
        job_count = int(os.environ.get("EDT_JOB_COUNT", str(JOBS_PER_ACCOUNT)))
        built: list[AccountConfig] = []
        for spec in accounts_spec.split(","):
            spec = spec.strip()
            if not spec:
                continue
            parts = spec.split(":")
            ssh_host = parts[0].strip()
            account_id = parts[1].strip() if len(parts) > 1 and parts[1].strip() else f"account_{len(built) + 1:02d}"
            idx = int(parts[2].strip()) if len(parts) > 2 and parts[2].strip() else len(built)
            built.append(
                AccountConfig(
                    account_id=account_id,
                    host_alias=ssh_host,
                    ssh_host=ssh_host,
                    account_index=idx,
                    job_count=job_count,
                    ingest_port=DEFAULT_INGEST_PORT + 10 * idx,  # 7876+
                    priority_lease_port=DEFAULT_PRIORITY_LEASE_PORT + 10 * idx,  # 7878+
                    license_ctrl_port=DEFAULT_LICENSE_CTRL_PORT + 10 * idx,  # 7879+
                    resource_port=DEFAULT_RESOURCE_PORT + 10 * idx,  # 7882+
                    license_target=lic_target,
                    license_ceiling=lic_ceiling,
                )
            )
        accounts = tuple(built)

    config = ControlPlaneConfig(
        accounts=accounts,
        db_path=Path(os.environ.get("EDT_DB_PATH", "~/edt-deploy/results.duckdb")).expanduser(),
        ssh_host=os.environ.get("EDT_SSH_HOST", "gate1-harry261"),
        job_count=int(os.environ.get("EDT_JOB_COUNT", str(JOBS_PER_ACCOUNT))),
        dashboard_port=int(os.environ.get("EDT_DASHBOARD_PORT", "8080")),
        intake_port=int(os.environ.get("EDT_INTAKE_PORT", "7875")),
        ingest_port=int(os.environ.get("EDT_INGEST_PORT", str(DEFAULT_INGEST_PORT))),
        priority_lease_port=int(os.environ.get("EDT_PRIORITY_LEASE_PORT", str(DEFAULT_PRIORITY_LEASE_PORT))),
        license_ctrl_port=int(os.environ.get("EDT_LICENSE_CTRL_PORT", str(DEFAULT_LICENSE_CTRL_PORT))),
        license_target=int(os.environ.get("EDT_LICENSE_TARGET", "100")),
        license_ceiling=int(os.environ.get("EDT_LICENSE_CEILING", "150")),
        license_poll_seconds=float(os.environ.get("EDT_LICENSE_POLL_SECONDS", "60")),
        debug_sshd_base=int(os.environ.get("EDT_DEBUG_SSHD_BASE", "0")),
        account_index=int(os.environ.get("EDT_ACCOUNT_INDEX", "0")),
        dashboard_peetsfea_version=os.environ.get("EDT_DASHBOARD_PEETSFEA_VERSION", "").strip(),
        resource_port=int(os.environ.get("EDT_RESOURCE_PORT", str(DEFAULT_RESOURCE_PORT))),
        builtin_toml_path=Path(os.environ["EDT_BUILTIN_TOML_PATH"]).expanduser()
        if os.environ.get("EDT_BUILTIN_TOML_PATH")
        else None,
    )
    run_control_plane(config, run_web=run_web, run_keeper=run_keeper)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ControlPlane", "ControlPlaneConfig", "build_control_plane", "main", "run_control_plane"]
