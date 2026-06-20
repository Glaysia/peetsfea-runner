"""결과 대시보드 + parquet export + 운영 텔레메트리 (Phase 5, MASTER_PLAN §2.7).

`localhost:8080`에서 결과 DB(DuckDB)를 **읽기 전용**으로 시각화/조회한다. 시뮬에 영향 주는 입력은 받지 않는다.
성공/데이터 중심으로 보여주고(실패는 on-demand), **컨테이너(잡)별 실시간 부하**는 `ResourcePoller` 스냅샷으로 띄운다.

엔드포인트:
- `GET /` — 대시보드(개요·컨테이너 실시간 부하·입출력 데이터셋·실패 on-demand).
- `GET /api/summary` — 누적/성공/실패/처리량/평균 solve/ GPU.
- `GET /api/results?state=&since=&limit=` — 입출력 데이터셋 행(JSON, 기본 success).
- `GET /api/sim/<request_id>` — 단건 상세(입력 + telemetry + 출력 리포트 곡선).
- `GET /api/failures?limit=` — 실패 요약(error_type별) + 최근 실패(on-demand).
- `GET /api/resources` — 컨테이너별 실시간 부하(노드 CPULoad/mem) + 라이선스 + 잡 상태.
- 전체 DB 덤프(`/results.parquet`)는 제거 — 학습은 read 평면 data API(:7884 `/api/results?since=`) 증분 Arrow 스트림 사용.
- `GET /health` — 상태.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .single_simulation_store import SingleSimulationResultStore
from .edt_toml_registry import TomlRegistryRequestError, TomlRegistryService
from .version_filter import normalize_peetsfea_version_filter

# 운영 리소스 스냅샷 제공자(없으면 빈 스냅샷). edt_resources.ResourcePoller.snapshot 와이어링.
ResourceProvider = Callable[[], Mapping[str, Any]]


def _loads(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _flatten_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# --- API payload 빌더 (read-only) --------------------------------------------

def _row_to_io(r: Mapping[str, Any]) -> dict[str, Any]:
    """한 결과 행 → 입출력 데이터셋 dict(테이블/JSON용; 무거운 리포트 원문 제외)."""
    pv = _loads(r.get("point_values_json"))
    tel = _loads(r.get("solve_telemetry_json"))
    out: dict[str, Any] = {
        "request_id": r.get("request_id"), "terminal_state": r.get("terminal_state"),
        "partition": r.get("partition"), "node": r.get("node"), "finished_at": r.get("finished_at"),
        "peetsfea_version": r.get("peetsfea_version"),
        "design_id": r.get("design_id"), "seed": r.get("seed"),
        "gpu_used": tel.get("gpu_used"), "solver_cores": tel.get("solver_cores"),
        "elapsed_min": round(tel["elapsed_ms"] / 60000, 1) if isinstance(tel.get("elapsed_ms"), (int, float)) else None,
    }
    for k, v in pv.items():
        out[f"in_{k}"] = _flatten_scalar(v)
    return out


def _histogram(values: list[float], bins: int = 12) -> dict[str, Any]:
    if not values:
        return {"edges": [], "counts": []}
    lo, hi = min(values), max(values)
    if hi <= lo:
        return {"edges": [lo, lo + 1], "counts": [len(values)]}
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        i = min(bins - 1, int((v - lo) / width))
        counts[i] += 1
    edges = [round(lo + i * width, 1) for i in range(bins + 1)]
    return {"edges": edges, "counts": counts}


def build_summary(store: SingleSimulationResultStore, *, peetsfea_version: str | None = None) -> dict[str, Any]:
    counts = store.state_counts(peetsfea_version=peetsfea_version)
    total = sum(counts.values())
    success = counts.get("success", 0)
    srows = store.fetch_solve_telemetry(terminal_state="success", peetsfea_version=peetsfea_version, limit=5000)
    mins: list[float] = []
    gpu = 0
    by_partition: dict[str, list[float]] = {}
    for r in srows:
        e = r.get("elapsed_ms")  # 전용 컬럼(ingest/백필이 채움) — 거대 JSON 재파싱 제거
        if isinstance(e, (int, float)):
            m = e / 60000
            mins.append(m)
            by_partition.setdefault(str(r.get("partition") or "?"), []).append(m)
        if r.get("gpu_used"):
            gpu += 1
    now = datetime.datetime.now(datetime.timezone.utc)
    since_1h = (now - datetime.timedelta(hours=1)).isoformat()
    return {
        "total": total, "counts": counts, "success": success,
        "failed": counts.get("failed", 0), "aborted": counts.get("aborted", 0),
        "success_rate": round(success / total * 100, 1) if total else 0.0,
        "avg_solve_min": round(sum(mins) / len(mins), 1) if mins else None,
        "gpu_used_count": gpu, "gpu_used_pct": round(gpu / len(srows) * 100, 1) if srows else 0.0,
        "throughput_1h": store.count_since(since_1h, peetsfea_version=peetsfea_version),
        "solve_min_hist": _histogram(mins),
        "by_partition_avg_min": {p: round(sum(v) / len(v), 1) for p, v in by_partition.items() if v},
        "version_filter": peetsfea_version or "",
    }


def build_sim_detail(store: SingleSimulationResultStore, request_id: str, *, peetsfea_version: str | None = None) -> dict[str, Any] | None:
    r = store.fetch_result(request_id, peetsfea_version=peetsfea_version)
    if r is None:
        return None
    reports_raw = _loads(r.get("csv_text_by_report_json"))
    reports: dict[str, Any] = {}
    for name, text in reports_raw.items():
        if not isinstance(text, str):
            continue
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            continue
        header = [c.strip() for c in lines[0].split(",")]
        data: list[list[Any]] = []
        for ln in lines[1:2000]:  # 곡선 플롯용(상한)
            cells = ln.split(",")
            data.append([_num(c) for c in cells])
        reports[name] = {"columns": header, "rows": data}
    return {
        "request_id": r.get("request_id"), "terminal_state": r.get("terminal_state"),
        "partition": r.get("partition"), "node": r.get("node"), "finished_at": r.get("finished_at"),
        "inputs": _loads(r.get("point_values_json")),
        "telemetry": {k: v for k, v in _loads(r.get("solve_telemetry_json")).items() if k != "samples"},
        "pass_counts": _loads(r.get("setup_pass_counts_json")),
        "error": {"stage": r.get("error_stage"), "type": r.get("error_type"), "message": r.get("error_message")},
        "reports": reports,
    }


def build_failures(store: SingleSimulationResultStore, limit: int = 50, *, peetsfea_version: str | None = None) -> dict[str, Any]:
    rows = store.fetch_rows(terminal_state="failed", peetsfea_version=peetsfea_version, limit=limit)
    by_type: dict[str, int] = {}
    recent: list[dict[str, Any]] = []
    for r in rows:
        t = str(r.get("error_type") or "?")
        by_type[t] = by_type.get(t, 0) + 1
        recent.append({
            "request_id": r.get("request_id"), "partition": r.get("partition"), "node": r.get("node"),
            "error_type": r.get("error_type"), "error_message": (str(r.get("error_message") or "")[:200]),
            "finished_at": r.get("finished_at"),
        })
    return {"by_type": by_type, "recent": recent}


def _num(s: str) -> Any:
    s = s.strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return s


def start_dashboard_server(
    *,
    store: SingleSimulationResultStore,
    host: str = "127.0.0.1",
    port: int = 8080,
    resource_provider: ResourceProvider | None = None,
    history_provider: Callable[[float | None], list[dict[str, Any]]] | None = None,
    peetsfea_version: str | None = None,
    toml_registry: TomlRegistryService | None = None,
) -> ThreadingHTTPServer:
    # 표시 버전 필터(설정 시 모든 결과 뷰가 이 버전만 노출). 빈 값이면 전 버전. `/api/versions`는 항상 전 분포.
    version_filter = normalize_peetsfea_version_filter(peetsfea_version)

    def _query_rows(query: dict[str, list[str]], default_state: str | None = None) -> list[dict[str, Any]]:
        since = query.get("since", [None])[0]
        state = query.get("state", query.get("terminal_state", [default_state]))[0]
        limit_raw = query.get("limit", [None])[0]
        limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None
        origin = query.get("origin", [None])[0]  # 출처 접두(base-/sweep-/prio-) — 데이터셋 탭 필터
        return store.fetch_rows(
            since=since, terminal_state=state, peetsfea_version=version_filter,
            request_id_prefix=origin or None, limit=limit,
        )

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-dashboard"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/health":
                self._json(200, {"status": "ok", "count": sum(store.state_counts(peetsfea_version=version_filter).values()), "version_filter": version_filter or ""})
            elif path == "/" or path == "/index.html":
                self._send(200, "text/html; charset=utf-8", _PAGE.encode("utf-8"))
            elif path == "/api/versions":
                # 전 버전 분포(필터 무관) + 현재 적용 중인 표시 필터.
                self._json(200, {"counts": store.version_counts(), "filter": version_filter or ""})
            elif path == "/api/summary":
                self._json(200, build_summary(store, peetsfea_version=version_filter))
            elif path == "/api/resources":
                self._json(200, dict(resource_provider()) if resource_provider else {"ok": False, "jobs": [], "nodes": {}, "license": {}, "counts": {}})
            elif path == "/api/resources/history":
                # 추세는 control(keeper)의 전용 자원 DB(history_provider)를 우선 — 분리 후 결과 DB엔 자원 시계열이 없다.
                # history_provider가 비면 결과 DB의 옛 자원 스냅샷(분리 전 데이터)으로 폴백.
                since_raw = query.get("since", [None])[0]
                try:
                    since_ts = float(since_raw) if since_raw else None
                except ValueError:
                    since_ts = None
                points = history_provider(since_ts) if history_provider else []
                if not points:
                    points = store.fetch_resource_history(since_ts=since_ts)
                self._json(200, {"points": points})
            elif path == "/api/timeseries":
                bucket = int(query.get("bucket", ["15"])[0] or 15)
                since = query.get("since", [None])[0]
                self._json(200, {"bucket_minutes": bucket, "points": store.timeseries(bucket_minutes=bucket, since=since, peetsfea_version=version_filter)})
            elif path == "/api/results":
                limit = int(query.get("limit", ["200"])[0] or 200)
                rows = _query_rows({**query, "limit": [str(limit)]}, default_state="success")
                self._json(200, {"rows": [_row_to_io(r) for r in rows]})
            elif path == "/api/tomls":
                if toml_registry is None:
                    self._json(503, {"error": "toml_registry_unavailable"})
                    return
                self._json(200, toml_registry.list_tomls())
            elif path == "/api/queue":
                # 레거시 sweep queue 조회. 새 운영 화면은 /api/tomls 카드 UI를 사용한다.
                limit = int(query.get("limit", ["300"])[0] or 300)
                self._json(200, {"depth": store.priority_depth(), "items": store.priority_list(limit=limit)})
            elif path == "/api/queue/lineage":
                # 레거시 sweep queue lineage. 새 운영 화면은 /api/tomls 카드 UI를 사용한다.
                limit = int(query.get("limit", ["200"])[0] or 200)
                self._json(200, store.priority_lineage(limit=limit))
            elif path == "/api/failures":
                self._json(200, build_failures(store, limit=int(query.get("limit", ["60"])[0] or 60), peetsfea_version=version_filter))
            elif path.startswith("/api/sim/"):
                detail = build_sim_detail(store, path[len("/api/sim/"):], peetsfea_version=version_filter)
                self._json(200 if detail else 404, detail or {"error": "not_found"})
            else:
                # /results.parquet(전체 DB 덤프) 제거됨 — 학습은 read 평면 data API(:7884 /api/results?since=)로
                # 증분 Arrow 스트림만 받는다(PLANS/data_plane_overhaul.html). FD死/전체재페치 부류 제거.
                self._json(404, {"error": "not_found"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if toml_registry is not None and parsed.path == "/api/tomls/custom":
                self._handle_toml(lambda: toml_registry.register_custom(self._read_json()))
                return
            self._json(404, {"error": "not_found"})

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            prefix = "/api/tomls/custom/"
            if toml_registry is not None and parsed.path.startswith(prefix):
                toml_id = parsed.path[len(prefix):]
                self._handle_toml(lambda: toml_registry.unregister_custom(toml_id))
                return
            self._json(404, {"error": "not_found"})

        def do_PATCH(self) -> None:
            parsed = urlparse(self.path)
            prefix = "/api/tomls/"
            suffix = "/active"
            if toml_registry is not None and parsed.path.startswith(prefix) and parsed.path.endswith(suffix):
                toml_id = parsed.path[len(prefix):-len(suffix)]
                self._handle_toml(lambda: toml_registry.set_active(toml_id, self._read_json()))
                return
            self._json(404, {"error": "not_found"})

        def do_PUT(self) -> None:
            parsed = urlparse(self.path)
            if toml_registry is not None and parsed.path == "/api/tomls/ratios":
                self._handle_toml(lambda: toml_registry.set_ratios(self._read_json()))
                return
            self._json(404, {"error": "not_found"})

        def _json(self, status: int, payload: Mapping[str, Any]) -> None:
            self._send(status, "application/json", json.dumps(payload, default=str).encode("utf-8"))

        def _read_json(self) -> Mapping[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise TomlRegistryRequestError("Content-Length must be an integer") from exc
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise TomlRegistryRequestError(f"invalid JSON body: {exc}") from exc
            if not isinstance(payload, Mapping):
                raise TomlRegistryRequestError("JSON body must be an object")
            return payload

        def _handle_toml(self, fn: Callable[[], Mapping[str, Any]]) -> None:
            try:
                self._json(200, fn())
            except TomlRegistryRequestError as exc:
                self._json(exc.status, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._json(500, {"error": type(exc).__name__, "message": str(exc)})

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # 클라이언트가 응답 도중 끊으면 정상 상황 — 트레이스백 스팸을 막는다.
                pass

    return ThreadingHTTPServer((host, port), Handler)


_PAGE = r"""<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<title>peetsfea 운영 대시보드</title>
<style>
:root{--bg:#0d1117;--pan:#161b22;--ln:#30363d;--fg:#e6edf3;--mut:#8b949e;--acc:#58a6ff;--ok:#3fb950;--warn:#d29922;--bad:#f85149}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 -apple-system,"Noto Sans KR",Segoe UI,sans-serif}
.wrap{max-width:1280px;margin:0 auto;padding:18px}
h1{font-size:18px;margin:0 0 2px}.sub{color:var(--mut);font-size:12px;margin-bottom:14px}
.tabs{display:flex;gap:6px;margin:8px 0 16px;border-bottom:1px solid var(--ln)}
.tab{padding:8px 16px;cursor:pointer;color:var(--mut);border-bottom:2px solid transparent;font-weight:600}
.tab.on{color:var(--fg);border-color:var(--acc)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:16px}
.card{background:var(--pan);border:1px solid var(--ln);border-radius:10px;padding:12px 14px}
.card .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.4px}
.card .v{font-size:24px;font-weight:700;margin-top:3px}
.tomlgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}
.tomlcard{background:var(--pan);border:1px solid var(--ln);border-radius:10px;padding:13px 14px;display:flex;flex-direction:column;gap:10px}
.tomlcard.off{opacity:.66}.tomlhead{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.tomlname{font-weight:700;font-size:15px;word-break:break-word}.pill{display:inline-block;border:1px solid var(--ln);border-radius:999px;padding:1px 7px;font-size:11px;color:var(--mut);white-space:nowrap}
.pill.on{color:var(--ok);border-color:#2f5e3a}.pill.lock{color:var(--warn);border-color:#5e4a2f}
.tomlmeta{display:grid;grid-template-columns:auto 1fr;gap:3px 10px;font-size:12px}.tomlmeta span:nth-child(odd){color:var(--mut)}
.tomlactions{display:flex;gap:6px;flex-wrap:wrap;align-items:center}.tomlratio{width:92px}
textarea{background:#0d1117;color:var(--fg);border:1px solid var(--ln);border-radius:8px;padding:9px 10px;font:12.5px/1.45 "SF Mono",Consolas,monospace;width:100%;min-height:280px}
.tomlpreview{background:#0d1117;border:1px solid var(--ln);border-radius:8px;padding:8px;max-height:96px;overflow:hidden;font:11px/1.35 "SF Mono",Consolas,monospace;color:var(--mut);white-space:pre-wrap}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}
.grid.c2{grid-template-columns:repeat(2,1fr)}
.cont{background:var(--pan);border:1px solid var(--ln);border-radius:10px;padding:12px 14px}
.cont .top{display:flex;justify-content:space-between;align-items:baseline}
.cont .node{font-weight:700}.cont .meta{color:var(--mut);font-size:12px}
.bar{height:9px;background:#0d1117;border:1px solid var(--ln);border-radius:6px;overflow:hidden;margin:4px 0 2px}
.bar>i{display:block;height:100%}
.lbl{display:flex;justify-content:space-between;font-size:11px;color:var(--mut)}
.st{display:inline-block;font-size:10px;padding:1px 7px;border-radius:10px;font-weight:700}
.st.RUNNING{background:rgba(63,185,80,.16);color:var(--ok)}.st.PENDING{background:rgba(210,153,34,.16);color:var(--warn)}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th,td{border-bottom:1px solid var(--ln);padding:6px 9px;text-align:left;white-space:nowrap}
th{color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--bg)}
tbody tr{cursor:pointer}tbody tr:hover{background:#1c2430}
.tools{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
input,select,button{background:var(--pan);color:var(--fg);border:1px solid var(--ln);border-radius:7px;padding:6px 10px;font:inherit}
button{cursor:pointer}button:hover{border-color:var(--acc)}
a{color:var(--acc)}.muted{color:var(--mut)}.hide{display:none}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:center;justify-content:center;z-index:9}
.modal .box{background:var(--pan);border:1px solid var(--ln);border-radius:12px;max-width:860px;width:92%;max-height:88vh;overflow:auto;padding:20px}
.kv{display:grid;grid-template-columns:auto 1fr;gap:2px 14px;font-size:12.5px}.kv .k{color:var(--mut)}
svg{display:block}.gpu{color:var(--bad);font-weight:700}.cpuonly{color:var(--mut)}
.bigbad{color:var(--bad);font-weight:700}
</style></head><body><div class="wrap">
<h1>peetsfea 운영 대시보드</h1>
<div class="sub" id="sub">read-only · 자동 새로고침</div>
<div class="tabs">
  <div class="tab on" data-t="overview">개요</div>
  <div class="tab" data-t="containers">컨테이너 부하</div>
  <div class="tab" data-t="trends">추세</div>
  <div class="tab" data-t="dataset">입출력 데이터셋</div>
  <div class="tab" data-t="toml">TOML</div>
  <div class="tab" data-t="failures">실패</div>
</div>

<div id="overview" class="page">
  <div class="cards" id="ovcards"></div>
  <div class="card"><div class="k">solve 시간 분포 (분)</div><div id="hist"></div></div>
  <div class="card" style="margin-top:10px"><div class="k">파티션별 평균 solve (분) · GPU vs CPU 자동 벤치마크</div><div id="bypart"></div></div>
</div>

<div id="containers" class="page hide">
  <div class="sub" id="contsub"></div>
  <div class="grid" id="contgrid"></div>
</div>

<div id="trends" class="page hide">
  <div class="tools">
    <span class="muted">범위</span>
    <select id="tswin"><option value="30" selected>최근 30분</option><option value="60">최근 1시간</option><option value="180">최근 3시간</option><option value="360">최근 6시간</option><option value="720">최근 12시간</option></select>
    <button onclick="trends()">새로고침</button>
    <span class="muted" id="tssub"></span>
  </div>
  <div class="card"><div class="k">처리량 — 성공 / 실패 (적층) · GPU 사용(선)</div><div id="tsThroughput"></div></div>
  <div class="grid c2" style="margin-top:10px">
    <div class="card"><div class="k">CPU 부하 — 노드 전체(전체유저) / 내 프로세스 / 할당 코어 (선)</div><div id="tsCpu"></div></div>
    <div class="card"><div class="k">메모리 — 우리 노드 사용 GB (선)</div><div id="tsMem"></div></div>
    <div class="card"><div class="k">동시 잡 — RUNNING / PENDING (선)</div><div id="tsJobs"></div></div>
    <div class="card"><div class="k">라이선스 — 내 점유 / 전체 사용 (선)</div><div id="tsLic"></div></div>
    <div class="card"><div class="k">AEDT — 명목(켜놓은 전체) / 유효(솔브중=라이선스) (선)</div><div id="tsAedt"></div></div>
  </div>
</div>

<div id="dataset" class="page hide">
  <div class="tools">
    <select id="dstate"><option value="success">success</option><option value="">전체</option><option value="aborted">aborted</option></select>
    <select id="dorigin"><option value="">전체 출처</option><option value="toml-">TOML registry</option><option value="base-">legacy baseline</option><option value="sweep-">legacy sweep</option><option value="prio-">static</option></select>
    <input id="dsearch" placeholder="검색(노드/설계id/입력값)"/>
    <button onclick="loadDataset()">새로고침</button>
    <span class="muted" id="dcount"></span>
  </div>
  <div style="overflow:auto;max-height:70vh"><table id="dtable"><thead></thead><tbody></tbody></table></div>
</div>

<div id="toml" class="page hide">
  <div class="tools">
    <button onclick="loadTomls()">새로고침</button>
    <button onclick="openTomlRegister()">custom TOML 등록</button>
    <button onclick="saveTomlRatios()">ratio 저장</button>
    <button onclick="resetTomlRatios()">ratio 균등</button>
    <span class="muted" id="tomlcount"></span>
  </div>
  <div class="tomlgrid" id="tomlgrid"></div>
</div>

<div id="failures" class="page hide">
  <div class="sub">실패는 전면에 두지 않습니다. 필요할 때만 조회.</div>
  <button onclick="loadFailures()">실패 불러오기</button>
  <div id="failtype" style="margin:12px 0"></div>
  <div style="overflow:auto;max-height:60vh"><table id="ftable"><thead></thead><tbody></tbody></table></div>
</div>
</div>

<div class="modal" id="modal" onclick="if(event.target.id=='modal')this.style.display='none'"><div class="box" id="mbox"></div></div>

<script>
const $=s=>document.querySelector(s), $$=s=>[...document.querySelectorAll(s)];
const f=(p)=>fetch(p).then(r=>r.json());
async function api(p,opt={}){const r=await fetch(p,{headers:{'Content-Type':'application/json'},...opt});
  const d=await r.json().catch(()=>({}));if(!r.ok)throw new Error(d.message||d.error||('HTTP '+r.status));return d}
let cur='overview';
$$('.tab').forEach(t=>t.onclick=()=>{cur=t.dataset.t;$$('.tab').forEach(x=>x.classList.toggle('on',x===t));
  $$('.page').forEach(p=>p.classList.add('hide'));$('#'+cur).classList.remove('hide');tick()});

function bar(v,max,col){const p=max>0?Math.min(100,v/max*100):0;
  return `<div class="bar"><i style="width:${p}%;background:${col}"></i></div>`}
function esc(s){return String(s==null?'':s).replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}

async function overview(){const s=await f('/api/summary');
  $('#sub').textContent='read-only · 자동 새로고침'+(s.version_filter?(' · 버전 '+s.version_filter+'만 표시'):'');
  const c=[['총 시뮬',s.total],['성공',s.success],['성공률',s.success_rate+'%'],
    ['평균 solve',s.avg_solve_min!=null?s.avg_solve_min+'분':'—'],['최근1h 처리량',s.throughput_1h],
    ['GPU 사용',s.gpu_used_pct+'%'],['실패(보존)',s.failed]];
  $('#ovcards').innerHTML=c.map(([k,v])=>`<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('');
  $('#hist').innerHTML=svgHist(s.solve_min_hist);
  const bp=s.by_partition_avg_min||{};
  const mx=Math.max(1,...Object.values(bp));
  $('#bypart').innerHTML=Object.keys(bp).length?Object.entries(bp).map(([p,v])=>
    `<div class="lbl"><span>${p}</span><span>${v}분</span></div>${bar(v,mx,'#58a6ff')}`).join(''):'<span class="muted">성공 데이터 누적되면 표시</span>';
}
function svgHist(h){if(!h.counts||!h.counts.length)return '<span class="muted">데이터 없음</span>';
  const W=560,H=120,n=h.counts.length,mx=Math.max(...h.counts),bw=W/n;
  let b='';for(let i=0;i<n;i++){const ht=mx>0?h.counts[i]/mx*(H-20):0;
    b+=`<rect x="${i*bw+1}" y="${H-ht-16}" width="${bw-2}" height="${ht}" fill="#3fb950"/>`}
  return `<svg width="${W}" height="${H}">${b}<text x="0" y="${H-2}" fill="#8b949e" font-size="10">${h.edges[0]}분</text>`+
    `<text x="${W-30}" y="${H-2}" fill="#8b949e" font-size="10">${h.edges[h.edges.length-1]}분</text></svg>`}

async function containers(){const r=await f('/api/resources');
  const lic=r.license||{}, c=r.counts||{};
  $('#contsub').innerHTML=r.ok?`RUNNING ${c.running||0} · PENDING ${c.pending||0} · `+
    `라이선스 데스크톱 <b>${lic.mine||0}</b>/${lic.in_use||0} (총 ${lic.issued||0}) · 솔브 <b style="color:#3fb950">${lic.solve_mine||0}</b>/${lic.solve_in_use||0}`:
    '<span class="bigbad">리소스 폴링 응답 없음(서비스/게이트 확인)</span>';
  const jobs=(r.jobs||[]).filter(j=>j.state==='RUNNING');
  const pend=(r.jobs||[]).filter(j=>j.state==='PENDING');
  const apj=r.aedt_per_job||{};
  $('#contgrid').innerHTML=jobs.map(j=>{const nd=(r.nodes||{})[j.node]||{};
    const load=nd.cpuload||0,memU=(nd.memtotal_mb||0)-(nd.memfree_mb||0),memT=nd.memtotal_mb||1;
    const mycpu=nd.mycpu||0, others=Math.max(0,load-mycpu);     // 내 프로세스 CPU(코어) vs 타유저(노드부하-내것)
    const ae=apj[String(j.name||'').split('-').pop()]||{};   // 잡 이름 peetsfea-edt-{jidx} → 제어기 per_job
    const ct=nd.cputot||0,alloc=nd.cpualloc||0;                 // ct=물리 전체, alloc=노드 전체 할당(공유)
    const mine=parseInt(j.cpus||'0',10)||0;                     // 이 잡(컨테이너)에 할당된 코어
    // 내 CPU 막대 기준은 '우리 잡 할당 코어'(mine) — 우리 컨테이너의 체감 가동률. 노드 막대는 물리(ct).
    const base=mine||ct||1;
    const mc=mycpu/base>1.05?'#f85149':mycpu/base>.7?'#d29922':'#3fb950';
    const lc=load/(ct||base)>0.9?'#f85149':load/(ct||base)>.6?'#d29922':'#3fb950';
    return `<div class="cont"><div class="top"><span class="node">${esc(j.node||'?')}</span>
      <span class="meta">${esc(j.partition)} · ${esc(j.time)} · ${esc(j.name)}</span></div>
      <div class="lbl"><span>pyaedt (솔브중 / 켜짐)</span><span><b style="color:#3fb950">${nd.solve!=null?nd.solve:(ae.active||0)}</b> / ${nd.desktop!=null?nd.desktop:(ae.nominal||0)}</span></div>
      <div class="lbl"><span>내 CPU (내 프로세스) / 할당</span><span><b style="color:#3fb950">${mycpu.toFixed(1)}</b> / ${mine} 코어</span></div>${bar(mycpu,base,mc)}
      <div class="lbl muted"><span>노드 전체 / 타유저</span><span>${load.toFixed(1)} / ${others.toFixed(1)} 코어 · 물리 ${ct}</span></div>${bar(load,ct||base,lc)}
      <div class="lbl"><span>메모리(노드)</span><span>${(memU/1024).toFixed(0)} / ${(memT/1024).toFixed(0)} GB</span></div>${bar(memU,memT,'#58a6ff')}
      </div>`}).join('')||'<span class="muted">RUNNING 컨테이너 없음</span>';
  if(pend.length)$('#contgrid').innerHTML+=`<div class="cont muted">+ PENDING ${pend.length}개 (${pend.map(p=>p.partition).join(', ')})</div>`;
}

function dorigin(rid){rid=String(rid||'');
  if(rid.startsWith('toml-'))return 'toml';
  if(rid.startsWith('base-'))return 'legacy baseline';
  if(rid.startsWith('sweep-'))return 'legacy sweep';
  if(rid.startsWith('prio-'))return 'static';
  return '?';}
async function loadDataset(){const st=$('#dstate').value,q=$('#dsearch').value.toLowerCase(),og=$('#dorigin').value;
  const d=await f('/api/results?limit=500&state='+st+(og?'&origin='+og:''));let rows=d.rows;
  if(q)rows=rows.filter(r=>JSON.stringify(r).toLowerCase().includes(q));
  $('#dcount').textContent=rows.length+'행';
  const inks=[...new Set(rows.flatMap(r=>Object.keys(r).filter(k=>k.startsWith('in_'))))].slice(0,8);
  const cols=['출처','request_id','partition','node','gpu_used','solver_cores','elapsed_min',...inks];
  $('#dtable thead').innerHTML='<tr>'+cols.map(c=>`<th>${c.replace('in_','')}</th>`).join('')+'</tr>';
  $('#dtable tbody').innerHTML=rows.map(r=>`<tr onclick="detail('${r.request_id}')">`+
    cols.map(c=>`<td>${c==='출처'?dorigin(r.request_id):esc(r[c])}</td>`).join('')+'</tr>').join('');
}
async function detail(id){const d=await f('/api/sim/'+id);if(!d||d.error)return;
  const ins=Object.entries(d.inputs||{}).map(([k,v])=>`<div class="k">${k}</div><div>${esc(v)}</div>`).join('');
  const tel=Object.entries(d.telemetry||{}).map(([k,v])=>`<div class="k">${k}</div><div>${esc(v)}</div>`).join('');
  let rep='';for(const[name,rd]of Object.entries(d.reports||{})){rep+=`<h4>${name}</h4>`+svgLine(rd)}
  $('#mbox').innerHTML=`<h3>${id} <span class="st ${d.terminal_state}">${d.terminal_state}</span></h3>
    <div class="muted">${d.partition} · ${d.node} · ${d.finished_at}</div>
    <h4>입력 설계점</h4><div class="kv">${ins||'<span class="muted">—</span>'}</div>
    <h4>출력 telemetry</h4><div class="kv">${tel}</div>
    <h4>출력 리포트</h4>${rep||'<span class="muted">없음</span>'}`;
  $('#modal').style.display='flex';
}
function svgLine(rd){const rows=rd.rows||[];if(rows.length<2)return '<span class="muted">데이터 부족</span>';
  const xs=rows.map(r=>+r[0]),W=760,H=180,P=30;const cols=rd.columns||[];
  let svg=`<svg width="${W}" height="${H}">`;const xmin=Math.min(...xs),xmax=Math.max(...xs);
  for(let c=1;c<(rows[0]||[]).length;c++){const ys=rows.map(r=>+r[c]).filter(v=>!isNaN(v));
    if(ys.length<2)continue;const ymin=Math.min(...ys),ymax=Math.max(...ys);
    const col=`hsl(${c*67%360},70%,60%)`;let p='';
    rows.forEach(r=>{const x=P+(+r[0]-xmin)/((xmax-xmin)||1)*(W-2*P);
      const y=H-P-(+r[c]-ymin)/((ymax-ymin)||1)*(H-2*P);if(!isNaN(x)&&!isNaN(y))p+=(p?'L':'M')+x.toFixed(1)+' '+y.toFixed(1)});
    svg+=`<path d="${p}" fill="none" stroke="${col}" stroke-width="1.4"/>`+
      `<text x="${W-P-70}" y="${20+c*12}" fill="${col}" font-size="10">${esc(cols[c]||'s'+c)}</text>`}
  return svg+`<text x="${P}" y="${H-4}" fill="#8b949e" font-size="10">${xmin.toExponential(1)}</text>`+
    `<text x="${W-P-40}" y="${H-4}" fill="#8b949e" font-size="10">${xmax.toExponential(1)}</text></svg>`}

async function loadFailures(){const d=await f('/api/failures?limit=80');
  $('#failtype').innerHTML='<b>error_type별:</b> '+Object.entries(d.by_type).map(([t,n])=>`${t}: ${n}`).join(' · ');
  $('#ftable thead').innerHTML='<tr><th>request_id</th><th>partition</th><th>error_type</th><th>message</th><th>시각</th></tr>';
  $('#ftable tbody').innerHTML=d.recent.map(r=>`<tr><td>${esc(r.request_id)}</td><td>${esc(r.partition)}</td>
    <td class="bigbad">${esc(r.error_type)}</td><td>${esc(r.error_message)}</td><td>${esc(r.finished_at)}</td></tr>`).join('');
}
// 추세 탭: 상대시간(지금 기준) 시간비례 축. 범위 [now-win, now]에 모든 차트를 동일 기준으로 맞춘다.
// 절대시간(HH:MM) 대신 now 기준 상대 라벨(-30m … now). x는 인덱스가 아니라 실제 시각 비례(공백도 비례).
let TS_WIN_MIN=30, TS_BUCKET_MIN=1, TS_NOW=0;
function relX(epochSec,W,P){const start=TS_NOW-TS_WIN_MIN*60;const fr=(epochSec-start)/((TS_WIN_MIN*60)||1);
  return P+(W-P-8)*Math.max(0,Math.min(1,fr));}
function relTicks(W,P,H){let g='';[0,.5,1].forEach(fr=>{const x=P+(W-P-8)*fr;const ago=Math.round(TS_WIN_MIN*(1-fr));
  const lbl=fr===1?'now':(TS_WIN_MIN>90?'-'+(+(ago/60).toFixed(1))+'h':'-'+ago+'m');
  g+=`<text x="${(x-10).toFixed(1)}" y="${H-4}" fill="#8b949e" font-size="9">${lbl}</text>`;});return g;}
function tsLines(id,points,series,xkey){const e=$('#'+id);
  if(!points||!points.length){e.innerHTML='<span class="muted">데이터 없음 (선택 범위 내 자원 포인트 없음)</span>';return;}
  const W=560,H=170,P=34;let mx=0;
  series.forEach(s=>points.forEach(p=>{const v=+p[s.key]||0;if(v>mx)mx=v;}));mx=mx||1;
  const Y=v=>H-P-(H-P-14)*v/mx;let g='';
  [0,.5,1].forEach(fr=>{const y=Y(mx*fr);g+=`<line x1="${P}" y1="${y}" x2="${W-8}" y2="${y}" stroke="#2c313c"/><text x="2" y="${y+4}" fill="#8b949e" font-size="9">${Math.round(mx*fr)}</text>`;});
  series.forEach(s=>{let d='';points.forEach(p=>{const x=relX(+p[xkey]||0,W,P);d+=(d?'L':'M')+x.toFixed(1)+' '+Y(+p[s.key]||0).toFixed(1);});g+=`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="1.6"/>`;});
  g+=relTicks(W,P,H);
  const lg=series.map(s=>`<span style="color:${s.color}">■ ${s.label}</span>`).join(' &nbsp;');
  e.innerHTML=`<svg width="100%" viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">${g}</svg><div style="font-size:11px;margin-top:3px">${lg}</div>`;}
function tsStack(id,points){const e=$('#'+id);
  if(!points||!points.length){e.innerHTML='<span class="muted">데이터 없음 (선택 범위 내 결과 없음)</span>';return;}
  const W=1080,H=210,P=30;
  const mx=Math.max(1,...points.map(p=>p.success+p.failed)),gmx=Math.max(1,...points.map(p=>p.gpu));
  const Y=v=>(H-P-16)*v/mx;
  const bw=Math.max(2,(W-P-8)*TS_BUCKET_MIN/(TS_WIN_MIN||1)-1);let g='';  // 버킷 폭 = 시간 비례
  [0,.5,1].forEach(fr=>{const y=H-P-Y(mx*fr);g+=`<line x1="${P}" y1="${y}" x2="${W-6}" y2="${y}" stroke="#2c313c"/><text x="2" y="${y+4}" fill="#8b949e" font-size="9">${Math.round(mx*fr)}</text>`;});
  let gd='';points.forEach(p=>{const te=Date.parse(p.t+':00Z')/1000;const x=relX(te,W,P),sh=Y(p.success),fh=Y(p.failed);
    g+=`<rect x="${x.toFixed(1)}" y="${(H-P-sh).toFixed(1)}" width="${bw.toFixed(1)}" height="${sh.toFixed(1)}" fill="#3fb950" opacity=".85"/>`;
    g+=`<rect x="${x.toFixed(1)}" y="${(H-P-sh-fh).toFixed(1)}" width="${bw.toFixed(1)}" height="${fh.toFixed(1)}" fill="#f85149" opacity=".85"/>`;
    gd+=(gd?'L':'M')+(x+bw/2).toFixed(1)+' '+(H-P-(H-P-16)*p.gpu/gmx).toFixed(1);});
  g+=`<path d="${gd}" fill="none" stroke="#b392f0" stroke-width="1.6"/>`;
  g+=relTicks(W,P,H);
  e.innerHTML=`<svg width="100%" viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">${g}</svg>
    <div style="font-size:11px;margin-top:2px"><span style="color:#3fb950">■ 성공</span> <span style="color:#f85149">■ 실패</span> <span style="color:#b392f0">— GPU 사용</span></div>`;}
const TS_WIN_LABEL={30:'최근 30분',60:'최근 1시간',180:'최근 3시간',360:'최근 6시간',720:'최근 12시간'};
async function trends(){TS_WIN_MIN=+(($('#tswin')&&$('#tswin').value)||30);
  TS_BUCKET_MIN=Math.max(1,Math.min(60,Math.round(TS_WIN_MIN/40)));  // 범위에 맞춰 ~40버킷
  TS_NOW=Date.now()/1000;const startSec=TS_NOW-TS_WIN_MIN*60;
  const since=new Date(startSec*1000).toISOString();
  const ts=await f(`/api/timeseries?bucket=${TS_BUCKET_MIN}&since=${encodeURIComponent(since)}`).catch(()=>({points:[]}));
  const hist=await f(`/api/resources/history?since=${startSec.toFixed(0)}`).catch(()=>({points:[]}));
  const tp=(ts.points||[]).filter(p=>Date.parse(p.t+':00Z')/1000>=startSec-TS_BUCKET_MIN*60);
  const hp=(hist.points||[]).filter(p=>(+p.ts||0)>=startSec);
  $('#tssub').textContent=`${TS_WIN_LABEL[TS_WIN_MIN]||TS_WIN_MIN+'분'} · 버킷 ${TS_BUCKET_MIN}분 · 결과 ${tp.length} · 자원 ${hp.length} (지금 기준 상대시간)`;
  tsStack('tsThroughput',tp);
  tsLines('tsCpu',hp,[{key:'load',color:'#58a6ff',label:'노드부하합(전체)'},{key:'myload',color:'#3fb950',label:'내부하합'},{key:'cpus',color:'#d29922',label:'할당코어합'}],'ts');
  tsLines('tsMem',hp.map(p=>({ts:p.ts,gb:Math.round((p.mem_used_mb||0)/1024)})),[{key:'gb',color:'#58a6ff',label:'사용 GB'}],'ts');
  tsLines('tsJobs',hp,[{key:'running',color:'#3fb950',label:'RUNNING'},{key:'pending',color:'#d29922',label:'PENDING'}],'ts');
  tsLines('tsLic',hp,[{key:'lic_mine',color:'#b392f0',label:'내 점유'},{key:'lic_inuse',color:'#58a6ff',label:'전체 사용'}],'ts');
  tsLines('tsAedt',hp,[{key:'nominal_aedt',color:'#d29922',label:'명목(켜놓은 전체)'},{key:'effective_aedt',color:'#3fb950',label:'유효(솔브중)'}],'ts');}
$('#tswin')&&($('#tswin').onchange=trends);
let TOMLS=[];
async function loadTomls(){try{const d=await f('/api/tomls');TOMLS=d.tomls||[];
    const active=TOMLS.filter(t=>t.active);$('#tomlcount').textContent=`active ${active.length} · custom ${TOMLS.filter(t=>t.kind==='custom').length}/6 · ratio ${d.ratios_set?'설정됨':'균등'}`;
    $('#tomlgrid').innerHTML=TOMLS.map(tomlCard).join('')||'<div class="muted">TOML registry 응답 없음</div>';
  }catch(e){$('#tomlcount').textContent='TOML registry 응답 없음';$('#tomlgrid').innerHTML=`<div class="cont bigbad">${esc(e.message||e)}</div>`;}}
function tomlCard(t){const locked=t.kind==='built_in',active=!!t.active,ratio=t.ratio==null?'':Number(t.ratio).toFixed(3).replace(/\.?0+$/,'');
  const preview=String(t.toml_text||'').slice(0,520);
  return `<div class="tomlcard ${active?'':'off'}">
    <div class="tomlhead"><div><div class="tomlname">${esc(t.name||t.id)}</div><div class="muted">${esc(t.id)}</div></div>
      <div>${locked?'<span class="pill lock">built-in</span>':`<span class="pill">custom</span>`} ${active?'<span class="pill on">active</span>':'<span class="pill">inactive</span>'}</div></div>
    <div class="tomlmeta"><span>ratio</span><span><input class="tomlratio" data-ratio="${esc(t.id)}" type="number" min="0" step="0.001" value="${esc(ratio)}" ${active?'':'disabled'}> %</span>
      <span>next seed</span><span>${esc(t.next_seed||0)}</span><span>updated</span><span>${fmtTs(t.updated_at)}</span></div>
    <div class="tomlpreview">${esc(preview)}${String(t.toml_text||'').length>520?'\n...':''}</div>
    <div class="tomlactions">
      <button onclick='showToml(${JSON.stringify(t.id)})'>보기</button>
      ${locked?'<button disabled>항상 active</button>':`<button onclick='toggleToml(${JSON.stringify(t.id)},${active?'false':'true'})'>${active?'비활성':'활성'}</button><button onclick='deleteToml(${JSON.stringify(t.id)})'>삭제</button>`}
    </div></div>`;}
function fmtTs(v){const n=+v||0;if(!n)return '—';return new Date(n*1000).toLocaleString();}
function showToml(id){const t=TOMLS.find(x=>x.id===id);if(!t)return;
  $('#mbox').innerHTML=`<h3>${esc(t.name||t.id)}</h3><div class="muted">${esc(t.id)} · ${esc(t.kind)} · ${t.active?'active':'inactive'}</div><textarea readonly>${esc(t.toml_text||'')}</textarea>`;
  $('#modal').style.display='flex';}
function openTomlRegister(){$('#mbox').innerHTML=`<h3>custom TOML 등록</h3>
  <div class="tools"><input id="newTomlName" placeholder="name"><label><input id="newTomlActive" type="checkbox" checked> active</label></div>
  <textarea id="newTomlText" placeholder="spec_version = ..."></textarea>
  <div class="tools" style="margin-top:10px"><button onclick="registerToml()">등록</button><span class="muted" id="newTomlMsg"></span></div>`;
  $('#modal').style.display='flex';}
async function registerToml(){try{await api('/api/tomls/custom',{method:'POST',body:JSON.stringify({name:$('#newTomlName').value,toml_text:$('#newTomlText').value,active:$('#newTomlActive').checked})});
    $('#modal').style.display='none';await loadTomls();}catch(e){$('#newTomlMsg').textContent=e.message||e;}}
async function toggleToml(id,active){try{await api('/api/tomls/'+encodeURIComponent(id)+'/active',{method:'PATCH',body:JSON.stringify({active})});await loadTomls();}catch(e){alert(e.message||e);}}
async function deleteToml(id){if(!confirm(id+' 삭제?'))return;try{await api('/api/tomls/custom/'+encodeURIComponent(id),{method:'DELETE'});await loadTomls();}catch(e){alert(e.message||e);}}
async function saveTomlRatios(){const ratios={};$$('input[data-ratio]').forEach(i=>{if(!i.disabled&&i.value!=='')ratios[i.dataset.ratio]=Number(i.value);});
  try{await api('/api/tomls/ratios',{method:'PUT',body:JSON.stringify({ratios})});await loadTomls();}catch(e){alert(e.message||e);}}
async function resetTomlRatios(){try{await api('/api/tomls/ratios',{method:'PUT',body:JSON.stringify({ratios:null})});await loadTomls();}catch(e){alert(e.message||e);}}
function tick(){if(cur==='overview')overview();else if(cur==='containers')containers();else if(cur==='trends')trends();else if(cur==='toml')loadTomls();}
$('#dsearch').oninput=()=>{if(cur==='dataset')loadDataset()};
$('#dorigin').onchange=()=>{if(cur==='dataset')loadDataset()};
$('#dstate').onchange=()=>{if(cur==='dataset')loadDataset()};
tick();loadDataset();setInterval(()=>{if(cur==='overview'||cur==='containers'||cur==='trends'||cur==='toml')tick()},8000);
</script></body></html>"""


__all__ = [
    "build_failures", "build_sim_detail", "build_summary", "start_dashboard_server",
]
