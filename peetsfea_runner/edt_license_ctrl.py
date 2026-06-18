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
    """컨테이너 수 제어기 — 동시 solve(lmstat solve_mine)를 target~ceiling 밴드로 묶되 actuator가
    permit이 아니라 **컨테이너 spawn/안전종료**다.

      solve < target  → 가장 한가한 노드의 잡 target↑ (LB: CPU 낮은 노드 우선; 다 90%↑면 free-RAM 큰 노드)
      solve > ceiling → 가장 바쁜 노드의 잡 target↓ (→ 오케스트레이터가 youngest 컨테이너를 SIGTERM 안전종료)

    잡당 max_per_job(20) 상한. 1컨테이너=1솔브=종료라 누수 0; 오버슈팅은 컨테이너 수로 흡수(콜드스타트 듀티 보상)."""

    snapshot_provider: Callable[[], Mapping[str, Any]]   # poller.snapshot
    target: int = 100
    ceiling: int = 150
    max_per_job: int = 20
    min_per_job: int = 2
    step: int = 3
    _job_target: dict[int, int] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @staticmethod
    def _job_node_map(snap: Mapping[str, Any]) -> dict[int, str]:
        out: dict[int, str] = {}
        for j in snap.get("jobs") or []:
            if j.get("state") != "RUNNING":
                continue
            try:
                ji = int(str(j.get("name") or "").rsplit("-", 1)[-1])
            except ValueError:
                continue
            out[ji] = str(j.get("node") or "")
        return out

    @staticmethod
    def _cpu(node: str, nodes: Mapping[str, Any]) -> float:
        nd = nodes.get(node) or {}
        ct = nd.get("cputot") or 0
        return (nd.get("cpuload") or 0.0) / ct if ct else 0.0

    @staticmethod
    def _freeram(node: str, nodes: Mapping[str, Any]) -> int:
        return (nodes.get(node) or {}).get("memfree_mb") or 0

    def tick(self) -> None:
        try:
            snap = self.snapshot_provider() or {}
        except Exception:  # noqa: BLE001 — 스냅샷 실패가 스케줄러를 죽이면 안 됨.
            return
        jn = self._job_node_map(snap)
        nodes = snap.get("nodes") or {}
        eff = int((snap.get("license") or {}).get("solve_mine") or 0)
        with self._lock:
            for ji in jn:
                self._job_target.setdefault(ji, self.min_per_job)
            for ji in [j for j in self._job_target if j not in jn]:
                self._job_target.pop(ji, None)
            # cap 하향 시 기존 target도 즉시 클램프(orchestrator가 youngest 컨테이너 안전종료로 흡수).
            for ji in list(self._job_target):
                if self._job_target[ji] > self.max_per_job:
                    self._job_target[ji] = self.max_per_job
            if not jn:
                return
            if eff < self.target:
                cands = [ji for ji in jn if self._job_target[ji] < self.max_per_job]
                if cands:
                    if all(self._cpu(jn[ji], nodes) > 0.9 for ji in cands):
                        best = max(cands, key=lambda ji: self._freeram(jn[ji], nodes))
                    else:
                        best = min(cands, key=lambda ji: self._cpu(jn[ji], nodes))
                    self._job_target[best] = min(self.max_per_job, self._job_target[best] + self.step)
            elif eff > self.ceiling:
                cands = [ji for ji in jn if self._job_target[ji] > self.min_per_job]
                if cands:
                    worst = max(cands, key=lambda ji: self._cpu(jn[ji], nodes))
                    self._job_target[worst] = max(self.min_per_job, self._job_target[worst] - self.step)

    def plan(self, job_index: int) -> int:
        with self._lock:
            return self._job_target.get(int(job_index), self.min_per_job)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"job_target": dict(self._job_target), "total": sum(self._job_target.values()),
                    "target": self.target, "ceiling": self.ceiling, "max_per_job": self.max_per_job}


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
            if parsed.path == "/container_plan":
                if scheduler is None:
                    _write_json(self, 200, {"target": 0})
                    return
                raw = parse_qs(parsed.query).get("job", ["0"])[0]
                ji = int(raw) if raw.lstrip("-").isdigit() else 0
                _write_json(self, 200, {"job": ji, "target": scheduler.plan(ji)})
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            path = urlparse(self.path).path
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
    "DEFAULT_LICENSE_CTRL_PORT",
    "LicenseController",
    "LicensePermitClient",
    "start_license_ctrl_server",
]
