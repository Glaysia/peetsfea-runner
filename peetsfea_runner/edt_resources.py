"""운영 리소스 폴러 — 컨테이너(잡)별 실시간 부하 + 라이선스/잡 상태 (대시보드 :8080용).

컨테이너는 노드 1개 = 잡 1개이므로, **컨테이너별 실시간 부하**는 그 잡이 도는 노드의 SLURM 텔레메트리로
얻는다. 데몬이 `ssh <gate>`로 한 번에 `squeue`(잡·상태·노드·경과) + `scontrol show node`(CPULoad·메모리) +
`lmstat`(electronics_desktop 사용량)을 주기 폴링(기본 20s)해 스냅샷을 캐시하고, 대시보드가 `/api/resources`로 노출한다.
read-only(시뮬 무간섭).
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

# argv -> (returncode, stdout)
CommandRunner = Callable[[list[str]], "tuple[int, str]"]

DEFAULT_RESOURCE_PORT = 7882  # control 프로세스가 자원 스냅샷/시계열을 web(대시보드)에 제공하는 로컬 백채널.

# gate에서 한 번에 잡/노드/라이선스를 뽑는 원격 스크립트(섹션 마커로 구분).
_REMOTE = r"""
P=peetsfea-edt
echo '###JOBS'
squeue --me -h -o '%i|%j|%T|%M|%N|%P|%C' 2>/dev/null | awk -F'|' -v p="$P" '$2 ~ p'
echo '###NODES'
for n in $(squeue --me -h -o '%N %T %j' 2>/dev/null | awk -v p="$P" '$2=="RUNNING" && $3 ~ p{print $1}' | sort -u); do
  # 필드별 독립 추출(순서 무관). 하나의 순서고정 정규식은 노드마다 필드 순서/유무가 달라 대부분 실패했다.
  L=$(scontrol show node "$n" -o 2>/dev/null)
  g() { printf '%s' "$L" | grep -oP "$1=\K[^ ]+" | head -1; }
  echo "$n|$(g CPULoad)|$(g CPUAlloc)|$(g CPUTot)|$(g FreeMem)|$(g RealMemory)"
done
echo '###LIC'
LM=/opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil
ME=$(whoami)
# lmstat 1회 호출 후 두 feature를 추출: electronics_desktop(=열린 데스크톱)·elec_solve_hfss(=실제 솔브중).
# 제어기는 solve(=elec_solve_hfss)를 100~150 밴드로 묶는다(데스크톱은 오버슛 허용). 대시보드는 둘 다 표시.
LMALL=$("$LM" lmstat -a -c 1055@license-server 2>/dev/null)
sect() { printf '%s\n' "$LMALL" | awk -v feat="$1" '/Users of /{f=($0 ~ ("Users of " feat ":"))?1:0} f{print}'; }
D=$(sect electronics_desktop)
S=$(sect elec_solve_hfss)
iss=$(printf '%s' "$D" | grep -oP 'Total of \K[0-9]+(?= licenses issued)' | head -1)
use=$(printf '%s' "$D" | grep -oP 'Total of \K[0-9]+(?= licenses in use)' | head -1)
mine=$(printf '%s' "$D" | grep -c "$ME")
suse=$(printf '%s' "$S" | grep -oP 'Total of \K[0-9]+(?= licenses in use)' | head -1)
smine=$(printf '%s' "$S" | grep -c "$ME")
echo "${iss:-0}|${use:-0}|${mine:-0}|${suse:-0}|${smine:-0}"
"""


def _ssh_runner(argv: list[str]) -> tuple[int, str]:
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=45)
    return proc.returncode, proc.stdout


def _empty_snapshot() -> dict[str, Any]:
    return {"ts": 0.0, "ok": False, "jobs": [], "nodes": {}, "license": {}, "counts": {"running": 0, "pending": 0}}


def parse_remote(text: str) -> dict[str, Any]:
    """원격 출력을 스냅샷 dict로 파싱(섹션: ###JOBS/###NODES/###LIC)."""
    snap = _empty_snapshot()
    section = ""
    for raw in text.splitlines():
        line = raw.strip()
        if line in ("###JOBS", "###NODES", "###LIC"):
            section = line
            continue
        if not line:
            continue
        if section == "###JOBS":
            parts = line.split("|")
            if len(parts) >= 7:
                jid, name, state, t, node, part, cpus = parts[:7]
                snap["jobs"].append(
                    {"id": jid, "name": name, "state": state, "time": t, "node": node, "partition": part, "cpus": cpus}
                )
        elif section == "###NODES":
            parts = line.split("|")
            if len(parts) >= 6:
                node, load, cpualloc, cputot, freemem, realmem = parts[:6]
                snap["nodes"][node] = {
                    "cpuload": _f(load),
                    "cpualloc": _i(cpualloc),
                    "cputot": _i(cputot),
                    "memfree_mb": _i(freemem),
                    "memtotal_mb": _i(realmem),
                }
        elif section == "###LIC":
            parts = line.split("|")
            if len(parts) >= 3:
                lic = {"feature": "electronics_desktop", "issued": _i(parts[0]), "in_use": _i(parts[1]), "mine": _i(parts[2])}
                # solve feature(elec_solve_hfss): 실제 솔브중 수. 제어기 기준값 + 대시보드 '유효 AEDT'.
                if len(parts) >= 5:
                    lic["solve_feature"] = "elec_solve_hfss"
                    lic["solve_in_use"] = _i(parts[3])
                    lic["solve_mine"] = _i(parts[4])
                snap["license"] = lic
    snap["counts"] = {
        "running": sum(1 for j in snap["jobs"] if j["state"] == "RUNNING"),
        "pending": sum(1 for j in snap["jobs"] if j["state"] == "PENDING"),
    }
    snap["ok"] = True
    return snap


def _f(s: str) -> float:
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _i(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def _history_point(snap: dict[str, Any]) -> dict[str, Any]:
    """스냅샷을 시계열 1포인트로 압축: 잡 카운트·라이선스·집계 CPU부하/메모리(우리 노드 합)."""
    nodes = snap.get("nodes") or {}
    jobs = snap.get("jobs") or []
    lic = snap.get("license") or {}
    counts = snap.get("counts") or {}
    load = sum(float(n.get("cpuload") or 0) for n in nodes.values())
    cpus = sum(int(j.get("cpus") or 0) for j in jobs if j.get("state") == "RUNNING")
    mem_used = sum((int(n.get("memtotal_mb") or 0) - int(n.get("memfree_mb") or 0)) for n in nodes.values())
    mem_total = sum(int(n.get("memtotal_mb") or 0) for n in nodes.values())
    return {
        "ts": snap.get("ts", 0.0),
        "running": int(counts.get("running") or 0),
        "pending": int(counts.get("pending") or 0),
        "lic_mine": int(lic.get("mine") or 0),
        "lic_inuse": int(lic.get("in_use") or 0),
        "load": round(load, 1),       # 우리 노드들의 node-wide CPULoad 합
        "cpus": cpus,                 # 우리 잡 할당 코어 합(부하 분모)
        "mem_used_mb": mem_used,
        "mem_total_mb": mem_total,
        # AEDT 추세는 lmstat 실측 기준: 명목=열린 데스크톱(electronics_desktop), 유효=솔브중(elec_solve_hfss).
        # (제어기 내부 ping 집계는 솔브 사이 idle 워커를 놓쳐 과소계상되므로 실측을 쓴다.)
        "nominal_aedt": int(lic.get("mine") or 0),
        "effective_aedt": int(lic.get("solve_mine") or 0),
    }


@dataclass
class ResourcePoller:
    """gate를 주기 폴링해 컨테이너(잡)별 부하·라이선스 스냅샷을 캐시한다."""

    ssh_host: str = "gate1-harry261"
    refresh_seconds: float = 20.0
    history_maxlen: int = 2160  # 20s × 2160 = 12h 시계열 ring buffer(인메모리; DB 영속은 history_sink로 별도)
    runner: CommandRunner | None = None
    clock: Callable[[], float] = time.time
    history_sink: Callable[[dict[str, Any]], None] | None = None  # 각 포인트 영속(store.record_resource_snapshot)
    extra_provider: Callable[[], dict[str, Any]] | None = None  # 포인트에 합칠 추가 필드(예: 라이선스 제어기 nominal/effective AEDT)
    aedt_provider: Callable[[], dict[str, Any]] | None = None  # 스냅샷에 합칠 컨테이너(잡)별 pyaedt 수(제어기 per_job)
    # DB 영속 시계열 보존기간(기본 7일). 주기적으로 그보다 오래된 스냅샷을 prune해 무한 성장 방지.
    history_retention_seconds: float = 7 * 24 * 3600.0
    history_prune_interval_seconds: float = 3600.0  # prune 검사 주기(매 폴링마다 DELETE는 낭비라 1h마다)
    history_prune: Callable[[float], Any] | None = None  # (cutoff_ts) -> None; store.prune_resource_snapshots 와이어링
    _last_prune: float = field(default=0.0, init=False, repr=False)
    _snapshot: dict[str, Any] = field(default_factory=_empty_snapshot, init=False, repr=False)
    _history: "deque[dict[str, Any]]" = field(default_factory=deque, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.runner is None:
            self.runner = _ssh_runner
        self._history = deque(maxlen=max(1, self.history_maxlen))

    def start(self) -> None:
        if self._thread is not None:
            return
        self.poll_once()  # 첫 스냅샷 즉시
        self._thread = threading.Thread(target=self._loop, name="edt-resource-poller", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self.refresh_seconds)
            if self._stop.is_set():
                return
            self.poll_once()

    def poll_once(self) -> dict[str, Any]:
        assert self.runner is not None
        argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=12", self.ssh_host, _REMOTE]
        try:
            rc, out = self.runner(argv)
            snap = parse_remote(out) if rc == 0 else _empty_snapshot()
        except Exception:  # noqa: BLE001 — 폴링 실패가 데몬/대시보드를 죽이면 안 된다.
            snap = _empty_snapshot()
        snap["ts"] = self.clock()
        if self.aedt_provider is not None:
            try:
                snap["aedt_per_job"] = self.aedt_provider() or {}  # 컨테이너당 pyaedt(솔브중/켜짐)
            except Exception:  # noqa: BLE001 — 추가 필드 실패가 폴러를 죽이면 안 된다.
                snap["aedt_per_job"] = {}
        point = _history_point(snap)
        if self.extra_provider is not None:
            try:
                point.update(self.extra_provider() or {})  # 라이선스 제어기 nominal/effective AEDT 등
            except Exception:  # noqa: BLE001 — 추가 필드 실패가 폴러를 죽이면 안 된다.
                pass
        with self._lock:
            self._snapshot = snap
            self._history.append(point)
        if self.history_sink is not None:
            try:
                self.history_sink(point)  # DB 영속(실패해도 폴링 지속).
            except Exception:  # noqa: BLE001 — 영속 실패가 폴러를 죽이면 안 된다.
                pass
        self._maybe_prune(snap["ts"])
        return snap

    def _maybe_prune(self, now: float) -> None:
        """7일 넘은 DB 자원 스냅샷을 1시간마다 한 번 prune(무한 성장 방지)."""
        if self.history_prune is None:
            return
        if (now - self._last_prune) < self.history_prune_interval_seconds:
            return
        self._last_prune = now
        try:
            self.history_prune(now - self.history_retention_seconds)
        except Exception:  # noqa: BLE001 — prune 실패가 폴러를 죽이면 안 된다.
            pass

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._snapshot)

    def history(self) -> list[dict[str, Any]]:
        """시간순 압축 포인트 목록(대시보드 /api/resources/history). 오래된→최신."""
        with self._lock:
            return list(self._history)

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)


def start_resource_server(
    *,
    poller: ResourcePoller,
    history_fetch: Callable[[float | None], list[dict[str, Any]]] | None = None,
    host: str = "127.0.0.1",
    port: int = DEFAULT_RESOURCE_PORT,
) -> ThreadingHTTPServer:
    """control 프로세스의 자원 백채널. GET /resources(스냅샷) · /resources/history?since= · /health.

    폴러를 무거운 데이터플레인(web)과 분리해 별도 프로세스에서 돌리고, web 대시보드는
    `RemoteResourceProvider`로 이 엔드포인트를 프록시한다. 그래야 web이 OOM/fd-고갈로
    허덕여도 텔레메트리(폴러)가 질식하지 않는다(차트 빈 구간 해소).

    `history_fetch`(전용 자원 DB의 fetch_resource_history)가 주어지면 영속 시계열을 서빙하고,
    없으면 인메모리 ring(폴러)로 폴백한다 → 대시보드 추세가 재시작/12h를 넘어 보존된다.
    """

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-resources"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def _json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/resources":
                self._json(200, poller.snapshot())
            elif path == "/resources/history":
                since_raw = parse_qs(parsed.query).get("since", [None])[0]
                try:
                    since_ts = float(since_raw) if since_raw else None
                except (TypeError, ValueError):
                    since_ts = None
                points = history_fetch(since_ts) if history_fetch is not None else None
                if not points:
                    points = poller.history()  # 영속이 비었으면(or 미설정) 인메모리 폴백.
                self._json(200, {"points": points})
            elif path == "/health":
                self._json(200, {"status": "ok"})
            else:
                self._json(404, {"error": "not_found"})

    return ThreadingHTTPServer((host, port), Handler)


@dataclass
class RemoteResourceProvider:
    """web 대시보드측 클라이언트 — control 프로세스의 자원 엔드포인트를 프록시. 실패 시 빈값(fail-safe)."""

    base_url: str  # 예: http://127.0.0.1:7882
    timeout_seconds: float = 8.0

    def _get(self, path: str) -> Any:
        url = self.base_url.rstrip("/") + path
        try:
            with urllib.request.urlopen(url, timeout=self.timeout_seconds) as resp:  # noqa: S310 — 로컬 신뢰 URL
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError, TimeoutError):
            return None

    def snapshot(self) -> dict[str, Any]:
        data = self._get("/resources")
        return data if isinstance(data, dict) else _empty_snapshot()

    def history(self, since_ts: float | None = None) -> list[dict[str, Any]]:
        path = "/resources/history"
        if since_ts is not None:
            path += "?" + urlencode({"since": since_ts})
        data = self._get(path)
        if isinstance(data, dict) and isinstance(data.get("points"), list):
            return data["points"]
        return []


__all__ = [
    "CommandRunner",
    "ResourcePoller",
    "RemoteResourceProvider",
    "parse_remote",
    "start_resource_server",
    "DEFAULT_RESOURCE_PORT",
]
