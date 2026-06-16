"""결과 대시보드 + CSV export (Phase 5, MASTER_PLAN §2.7).

`localhost:8080`에서 결과 DB(DuckDB)를 **읽기 전용**으로 시각화/조회한다. 시뮬에 영향 주는 입력은
받지 않는다.
- `GET /health` — 상태.
- `GET /` — 누적 건수·최근 결과 요약(HTML).
- `GET /results.csv` — 누적 결과 CSV(입력 파라미터 + 시간/패스/상태). 쿼리: `?since=`, `?terminal_state=`,
  `?limit=`. 한 행 = 한 시뮬.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .single_simulation_store import SingleSimulationResultStore

# CSV 기본(스칼라) 컬럼 — 그 뒤에 입력 파라미터(in_*)/패스(pass_*)가 붙는다.
_BASE_COLUMNS = (
    "request_id",
    "terminal_state",
    "started_at",
    "finished_at",
    "account_id",
    "host_alias",
    "partition",
    "node",
    "peetsfea_version",
    "mode",
    "seed",
    "design_id",
    "point_hash",
    "dimension_count",
)


def _loads(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def rows_to_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    """결과 행들을 CSV 텍스트로. 입력 파라미터(point_values)는 `in_<key>`, 패스는 `pass_<setup>`로 평탄화."""
    in_keys = sorted({k for r in rows for k in _loads(r.get("point_values_json"))})
    pass_keys = sorted({k for r in rows for k in _loads(r.get("setup_pass_counts_json"))})
    fieldnames = [*_BASE_COLUMNS, *(f"in_{k}" for k in in_keys), *(f"pass_{k}" for k in pass_keys)]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        point_values = _loads(r.get("point_values_json"))
        pass_counts = _loads(r.get("setup_pass_counts_json"))
        row: dict[str, Any] = {col: r.get(col) for col in _BASE_COLUMNS}
        for k in in_keys:
            row[f"in_{k}"] = point_values.get(k)
        for k in pass_keys:
            row[f"pass_{k}"] = pass_counts.get(k)
        writer.writerow(row)
    return buffer.getvalue()


def start_dashboard_server(
    *,
    store: SingleSimulationResultStore,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> ThreadingHTTPServer:
    def _query_rows(query: dict[str, list[str]]) -> list[dict[str, Any]]:
        since = query.get("since", [None])[0]
        terminal_state = query.get("terminal_state", [None])[0]
        limit_raw = query.get("limit", [None])[0]
        limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None
        return store.fetch_rows(since=since, terminal_state=terminal_state, limit=limit)

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-dashboard"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/health":
                self._json(200, {"status": "ok", "count": len(store.fetch_rows())})
            elif parsed.path == "/results.csv":
                self._csv(rows_to_csv(_query_rows(query)))
            elif parsed.path == "/":
                self._html(_query_rows({"limit": ["20"]}))
            else:
                self._json(404, {"error": "not_found"})

        def _json(self, status: int, payload: Mapping[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self._send(status, "application/json", body)

        def _csv(self, text: str) -> None:
            self._send(200, "text/csv", text.encode("utf-8"))

        def _html(self, rows: list[dict[str, Any]]) -> None:
            total = len(store.fetch_rows())
            lines = "".join(
                f"<tr><td>{r.get('request_id')}</td><td>{r.get('terminal_state')}</td>"
                f"<td>{r.get('finished_at')}</td></tr>"
                for r in rows
            )
            html = (
                "<html><head><meta charset='utf-8'><title>peetsfea results</title></head><body>"
                f"<h2>peetsfea 결과 대시보드 — 누적 {total}건</h2>"
                "<p><a href='/results.csv'>results.csv</a> (read-only)</p>"
                "<table border=1><tr><th>request_id</th><th>state</th><th>finished_at</th></tr>"
                f"{lines}</table></body></html>"
            )
            self._send(200, "text/html; charset=utf-8", html.encode("utf-8"))

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return ThreadingHTTPServer((host, port), Handler)


__all__ = ["rows_to_csv", "start_dashboard_server"]
