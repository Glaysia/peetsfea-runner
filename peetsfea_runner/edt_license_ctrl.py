"""라이선스 피드백 제어기 :7879 — 슈퍼컴 전용 백채널 (전역 동시 솔브를 밴드로 묶음).

슬롯을 50×9=450 열어둬도 실제 동시 솔브(=라이선스 사용)는 **목표 100**에 맞춘다. 워커가 솔브
직전 permit을 받고(상한), 솔브 중 heartbeat로 abort 지령을 받는다(150 초과 시 youngest kill).

토폴로지는 lease(:7878)/ingest(:7876)와 동일: gate 경유 ssh 터널 + loopback 바인딩.

- **제어기(web, `LicenseController` + `start_license_ctrl_server`)**: lic_mine(lmstat)을 1분 주기로 읽고
  `effective = max(lic_mine, 발급수)`로 permit을 TARGET(100)에 묶는다(발급수 회계 → herd 오버슈트 차단).
  lic_mine > CEILING(150)이면 in-flight 중 youngest(가장 최근 시작) 솔브를 abort 표시.
- **워커(`LicensePermitClient`)**: solve 직전 `acquire()`(grant면 솔브), solve 후 `release()`, solve 중
  `heartbeat(started_at)` → abort 여부. 제어기 불가 시 fail-closed(grant=False → 워커가 대기·재시도).

abort는 best-effort: youngest라 버리는 컴퓨트 최소. 평상시 상한 회계가 막아 abort는 드물다.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .edt_container_control import ContainerController

DEFAULT_LICENSE_CTRL_PORT = 7879


def _job_index(worker_id: str) -> str:
    """worker_id `j{jidx}-w{widx}-{host}-{pid}` → 잡 인덱스 문자열('0' 등). 형식 불일치 시 '?'."""
    head = worker_id.split("-", 1)[0]
    return head[1:] if head[:1] == "j" and head[1:].isdigit() else "?"


@dataclass
class LicenseController:
    """전역 동시 솔브 제어기. permit 상한(target) + youngest-kill(ceiling)로 밴드 수렴.

    `lic_provider()`는 lmstat로 측정한 내 라이선스 사용수(lic_mine). `poll()`을 주기(1분) 호출하면
    lic_mine을 갱신하고 ceiling 초과 시 youngest를 abort 표시한다.
    """

    lic_provider: Callable[[], int]
    target: int = 100
    ceiling: int = 150
    permit_ttl_seconds: float = 180.0  # heartbeat 끊긴 permit 만료(워커 사망 누수 방지)
    nominal_ttl_seconds: float = 120.0  # 이 안에 ping(permit/heartbeat)한 워커 = 명목 AEDT(켜져 있는 것)
    clock: Callable[[], float] = time.time
    _lic_cached: int = field(default=0, init=False)
    _active: dict[str, dict[str, float]] = field(default_factory=dict, init=False, repr=False)  # 솔브중(=유효 AEDT)
    _seen: dict[str, float] = field(default_factory=dict, init=False, repr=False)  # 최근 ping한 전체 워커(=명목 AEDT)
    _abort: set[str] = field(default_factory=set, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _touch(self, worker_id: str, now: float) -> None:
        self._seen[worker_id] = now  # 명목 AEDT 카운트용(솔브 안 해도 ping하면 켜져 있는 것)

    def _gc(self, now: float) -> None:
        dead = [w for w, s in self._active.items() if (now - s["last_seen"]) > self.permit_ttl_seconds]
        for w in dead:
            self._active.pop(w, None)
            self._abort.discard(w)
        for w in [w for w, t in self._seen.items() if (now - t) > self.nominal_ttl_seconds]:
            self._seen.pop(w, None)

    def nominal(self) -> int:
        """명목 AEDT = 최근 nominal_ttl 안에 ping한 워커 수(켜놓은 것 포함; idle도)."""
        with self._lock:
            now = self.clock()
            return sum(1 for t in self._seen.values() if (now - t) <= self.nominal_ttl_seconds)

    def _per_job_locked(self, now: float) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for w in self._active:  # 솔브중(=유효 AEDT)
            out.setdefault(_job_index(w), {"active": 0, "nominal": 0})["active"] += 1
        for w, t in self._seen.items():  # 최근 ping한 전체(=명목 AEDT, idle 포함)
            if (now - t) <= self.nominal_ttl_seconds:
                out.setdefault(_job_index(w), {"active": 0, "nominal": 0})["nominal"] += 1
        return out

    def per_job(self) -> dict[str, dict[str, int]]:
        """컨테이너(잡 인덱스)별 pyaedt 수 — {jidx: {active(솔브중), nominal(켜진)}}."""
        with self._lock:
            return self._per_job_locked(self.clock())

    def permit(self, worker_id: str) -> bool:
        """솔브 1건 허가 여부. effective < target이면 grant하고 active에 등록."""
        with self._lock:
            now = self.clock()
            self._gc(now)
            self._touch(worker_id, now)  # 명목 AEDT: permit 거절돼도 워커가 켜져 있다는 신호
            effective = max(self._lic_cached, len(self._active))
            if effective >= self.target:
                return False
            self._active[worker_id] = {"granted_at": now, "solve_started_at": now, "last_seen": now}
            return True

    def release(self, worker_id: str) -> None:
        with self._lock:
            self._touch(worker_id, self.clock())
            self._active.pop(worker_id, None)
            self._abort.discard(worker_id)

    def heartbeat(self, worker_id: str, solve_started_at: float) -> bool:
        """솔브 중 워커가 호출. abort 표시됐으면 True. permit 없이 온 경우(재시작)도 등록."""
        with self._lock:
            now = self.clock()
            self._touch(worker_id, now)
            state = self._active.get(worker_id)
            if state is None:
                self._active[worker_id] = {"granted_at": now, "solve_started_at": solve_started_at, "last_seen": now}
            else:
                state["last_seen"] = now
                state["solve_started_at"] = solve_started_at
            return worker_id in self._abort

    def poll(self) -> None:
        """제어 루프 1회(1분): lic_mine 갱신 + ceiling 초과 시 youngest 1개 abort 표시."""
        try:
            lic = int(self.lic_provider())
        except Exception:  # noqa: BLE001 — 측정 실패가 제어기를 죽이면 안 된다.
            lic = self._lic_cached
        with self._lock:
            now = self.clock()
            self._gc(now)
            self._lic_cached = lic
            self._abort.clear()
            # 150 초과면 가장 최근 시작(youngest = solve_started_at 최대) 1개를 끈다(한 번에 하나).
            if lic > self.ceiling and self._active:
                youngest = max(self._active.items(), key=lambda kv: kv[1]["solve_started_at"])[0]
                self._abort.add(youngest)

    def status(self) -> dict[str, Any]:
        with self._lock:
            now = self.clock()
            nominal = sum(1 for t in self._seen.values() if (now - t) <= self.nominal_ttl_seconds)
            return {
                "lic_mine": self._lic_cached,
                "active_permits": len(self._active),  # 솔브중 = 유효 AEDT
                "nominal_aedt": nominal,  # 켜져 있는 전체 = 명목 AEDT
                "effective": max(self._lic_cached, len(self._active)),
                "target": self.target,
                "ceiling": self.ceiling,
                "abort_marked": len(self._abort),
                "aedt_per_job": self._per_job_locked(now),  # 컨테이너당 pyaedt(솔브중/켜짐)
            }


@dataclass
class ContainerScheduler:
    """유효 AEDT(동시 솔브)를 지령으로 수렴시키는 **적분 컨테이너 제어**.

    잡은 고정 인프라(JobOrchestrator가 10개 안정 유지)고, 처리량 제어는 매 tick(control_period)마다
    `ContainerController`가 `err=120-solve`로 총 컨테이너 N을 적분 갱신 → `plan_for(job)`이 잡별 목표를
    돌려준다. orchestrator가 `/job_plan?job=i`를 주기 재조회해 respawn-to-N. (구 LUT·드레인 폐지 —
    `PLANS/integral_container_control.html`.) /orch_report는 관측/대시보드용 실측 N 피드백.
    """

    snapshot_provider: Callable[[], Mapping[str, Any]]   # poller.snapshot
    controller: ContainerController = field(default_factory=ContainerController)
    control_period_seconds: float = 45.0  # 적분 tick 주기(키퍼 루프보다 길게 — 슬램 방지)
    report_ttl_seconds: float = 60.0
    clock: Callable[[], float] = time.monotonic
    _reports: dict[str, dict[str, Any]] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _last_control: float = field(default=float("-inf"), init=False, repr=False)

    def current_solve(self) -> int:
        try:
            snap = self.snapshot_provider() or {}
        except Exception:  # noqa: BLE001 — 스냅샷 실패가 제어기를 죽이면 안 됨.
            return 0
        return int((snap.get("license") or {}).get("solve_mine") or 0)

    def decide_n(self, solve: int | None = None) -> int:
        """현재 총 컨테이너 목표 N_total(적분 상태). 관측/호환용 — 잡별 목표는 plan_for(i)."""
        with self._lock:
            return self.controller.n_total

    def _live_by_job_locked(self, now: float) -> dict[int, int]:
        """fresh orch_report에서 잡인덱스→실측 live(가장 최근 보고 우선). 로드밸런싱 가중치용."""
        out: dict[int, int] = {}
        best_ts: dict[int, float] = {}
        for r in self._reports.values():
            if (now - r["ts"]) > self.report_ttl_seconds:
                continue
            j = r.get("job")
            if j is None:
                continue
            try:
                ji = int(j)
            except (TypeError, ValueError):
                continue
            if ji not in best_ts or r["ts"] > best_ts[ji]:
                best_ts[ji] = r["ts"]
                out[ji] = int(r.get("live", 0))
        return out

    def plan_for(self, job_index: int) -> int:
        """잡 i의 현재 컨테이너 목표(/job_plan?job=i 서빙). 적분 N_total을 잡별 실측 live에 **비례 재분배**
        (로드밸런싱): 못 채우는 한가한 잡의 잉여를 잘 도는 잡에 더 준다. 보고 없으면 균등 폴백."""
        with self._lock:
            live_by_job = self._live_by_job_locked(self.clock())
            return self.controller.plan_for(job_index, live_by_job)

    def report(self, slurm_id: str, live: int, **extra: Any) -> None:
        """orchestrator가 살아있는 컨테이너 수 보고."""
        sid = str(slurm_id or "")
        if not sid:
            return
        with self._lock:
            self._reports[sid] = {"live": int(live), "ts": self.clock(), **extra}

    def live_count(self, slurm_id: str) -> int:
        """slurm_id의 살아있는 컨테이너 수. 미보고/만료면 -1(아직 모름 → 교체 후보 제외용)."""
        with self._lock:
            r = self._reports.get(str(slurm_id))
            if not r or (self.clock() - r["ts"]) > self.report_ttl_seconds:
                return -1
            return int(r["live"])

    def tick(self) -> None:
        """control_period마다 적분 1회(err=120-solve로 N_total 갱신) + 오래된 보고 정리.

        키퍼 루프는 빠르게(수초) 돌므로, 적분 tick은 control_period_seconds로 율제한해야 ±dn_max가
        매 루프 누적돼 한계로 슬램되지 않는다. solve 관측은 락 밖에서(스냅샷이 느릴 수 있음).
        """
        now = self.clock()
        do_control = False
        with self._lock:
            if (now - self._last_control) >= self.control_period_seconds:
                do_control = True
                self._last_control = now
        if do_control:
            solve = self.current_solve()
            with self._lock:
                self.controller.tick(solve)
        with self._lock:
            for sid in [s for s, r in self._reports.items() if now - r["ts"] > self.report_ttl_seconds]:
                self._reports.pop(sid, None)

    def status(self) -> dict[str, Any]:
        with self._lock:
            reps = {s: r["live"] for s, r in self._reports.items()}
            n_total = self.controller.n_total
        return {
            "solve": self.current_solve(), "n_total": n_total,
            "target_aedt": self.controller.target_aedt, "reports": reps,
        }


def start_license_ctrl_server(
    *, controller: LicenseController, scheduler: "ContainerScheduler | None" = None,
    host: str = "127.0.0.1", port: int = DEFAULT_LICENSE_CTRL_PORT
) -> ThreadingHTTPServer:
    """제어기 백채널. POST /permit·/release·/heartbeat. GET /health. GET /container_plan?job=N (스케줄러)."""

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-license-ctrl"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def _body(self) -> dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b"{}"
                payload = json.loads(raw.decode("utf-8"))
                return payload if isinstance(payload, dict) else {}
            except Exception:  # noqa: BLE001
                return {}

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                st = {"status": "ok", **controller.status()}
                if scheduler is not None:
                    st["scheduler"] = scheduler.status()
                _write_json(self, 200, st)
                return
            if parsed.path in ("/job_plan", "/container_plan"):
                # 잡별 컨테이너 목표(적분 N_total을 고정 잡들에 분배). job 미지정이면 총 N_total.
                if scheduler is None:
                    n = 0
                else:
                    job_q = parse_qs(parsed.query).get("job", [""])[0]
                    n = scheduler.plan_for(int(job_q)) if job_q.isdigit() else scheduler.decide_n()
                _write_json(self, 200, {"n": n})
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            # orchestrator 보고(form-encoded): 살아있는 컨테이너 수를 관측/대시보드용으로 저장.
            if path == "/orch_report":
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    q = parse_qs(self.rfile.read(length).decode("utf-8")) if length else {}
                    if scheduler is not None:
                        scheduler.report(
                            q.get("slurm", [""])[0], int(q.get("live", ["0"])[0] or 0),
                            target=int(q.get("target", ["0"])[0] or 0), age=int(q.get("age", ["0"])[0] or 0),
                        )
                except Exception:  # noqa: BLE001
                    pass
                _write_json(self, 200, {"ok": True})
                return
            body = self._body()
            worker_id = str(body.get("worker_id") or "")
            if not worker_id:
                _write_json(self, 400, {"error": "worker_id required"})
                return
            if path == "/permit":
                _write_json(self, 200, {"grant": controller.permit(worker_id)})
            elif path == "/release":
                controller.release(worker_id)
                _write_json(self, 200, {"ok": True})
            elif path == "/heartbeat":
                started = float(body.get("solve_started_at") or 0.0)
                _write_json(self, 200, {"abort": controller.heartbeat(worker_id, started)})
            else:
                _write_json(self, 404, {"error": "not_found"})

    return ThreadingHTTPServer((host, port), Handler)


@dataclass
class LicensePermitClient:
    """워커측 제어기 클라이언트. 제어기 불가 시 fail-closed(grant=False → 워커가 대기·재시도)."""

    ctrl_url: str  # 예: http://127.0.0.1:7879
    worker_id: str
    timeout_seconds: float = 10.0

    def _post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        url = self.ctrl_url.rstrip("/") + path
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                parsed = json.loads(response.read().decode("utf-8"))
                return parsed if isinstance(parsed, dict) else None
        except Exception:  # noqa: BLE001 — 네트워크/터널 단절.
            return None

    def acquire(self) -> bool:
        resp = self._post("/permit", {"worker_id": self.worker_id})
        return bool(resp.get("grant")) if resp else False  # fail-closed

    def release(self) -> None:
        self._post("/release", {"worker_id": self.worker_id})

    def heartbeat(self, solve_started_at: float) -> bool:
        resp = self._post("/heartbeat", {"worker_id": self.worker_id, "solve_started_at": float(solve_started_at)})
        return bool(resp.get("abort")) if resp else False


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, object]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionResetError):
        pass


__all__ = [
    "ContainerScheduler",
    "DEFAULT_LICENSE_CTRL_PORT",
    "LicenseController",
    "LicensePermitClient",
    "start_license_ctrl_server",
]
