"""결과 ingest :7876 — 슈퍼컴 전용 백채널 (단일 로컬 DB로 결과 수렴).

토폴로지: **슈퍼컴엔 DB 없음, 진실의 원천은 로컬 단일 DuckDB.** 컴퓨트노드 컨테이너는 로컬로 직접
push가 안 되지만 gate로는 ssh가 되므로, gate를 경유 TCP 허브로 쓴다:

    [컨테이너] 127.0.0.1:7876 --ssh -L--> [gate] 127.0.0.1:7876 <--ssh -R-- [로컬] 127.0.0.1:7876
                                                                              (ingest 서버, 단일 DB)

- **로컬(`start_result_ingest_server`)**: `127.0.0.1:7876` HTTP 서버. `POST /ingest`로 받은 결과
  envelope를 로컬 `SingleSimulationResultStore`에 record. loopback 바인딩 + 역터널로만 도달 →
  외부 7875 클라이언트엔 안 보임(슈퍼컴 전용).
- **컨테이너(`HttpResultSink`)**: `SlotDispatcher`의 record sink. 결과 envelope를 `127.0.0.1:7876`로
  POST. 14분짜리 시뮬 결과가 터널 일시 단절로 유실되지 않게, 실패 시 `fallback_dir`에 JSON으로 떨궈둔다.

7875 = 공개 sweep intake, 7876 = 7875 사용자가 모르는 슈퍼컴 전용 결과 통로.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .single_simulation_store import SingleSimulationResultStore

DEFAULT_INGEST_PORT = 7876


def start_result_ingest_server(
    *,
    store: SingleSimulationResultStore,
    host: str = "127.0.0.1",
    port: int = DEFAULT_INGEST_PORT,
    on_record: Callable[[Mapping[str, Any]], None] | None = None,
) -> ThreadingHTTPServer:
    """결과 ingest 서버. `POST /ingest`(JSON envelope) → 로컬 DB record. `GET /health`."""

    lock = threading.Lock()

    def _record(envelope: Mapping[str, Any]) -> None:
        with lock:  # DuckDB 단일 커넥션 직렬화(여러 컨테이너가 동시에 POST).
            store.record_envelope(envelope)
        if on_record is not None:
            on_record(envelope)

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-ingest"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            if urlparse(self.path).path == "/health":
                _write_json(self, 200, {"status": "ok", "count": len(store.fetch_rows())})
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/ingest":
                _write_json(self, 404, {"error": "not_found"})
                return
            try:
                envelope = _read_json(self)
            except ValueError as exc:
                _write_json(self, 400, {"error": "bad_request", "message": str(exc)})
                return
            try:
                _record(envelope)
            except Exception as exc:  # noqa: BLE001
                _write_json(self, 500, {"error": type(exc).__name__, "message": str(exc)})
                return
            _write_json(self, 200, {"status": "ok", "request_id": envelope.get("request_id", "")})

    return ThreadingHTTPServer((host, port), Handler)


@dataclass
class HttpResultSink:
    """컨테이너측 record sink: 결과 envelope를 로컬 데몬(:7876, 터널)으로 POST.

    `.record(envelope)`는 `SlotDispatcher.record` 시그니처(`Callable[[Mapping], None]`)에 맞다.
    POST 실패 시 재시도 후, 그래도 안 되면 `fallback_dir`에 JSON으로 보존(유실 방지). 절대 raise 안 함
    — 디스패처 슬롯 루프를 죽이면 안 되므로.
    """

    url: str = f"http://127.0.0.1:{DEFAULT_INGEST_PORT}/ingest"
    timeout_seconds: float = 10.0
    retries: int = 3
    retry_sleep_seconds: float = 2.0
    fallback_dir: Path | None = None
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def record(self, envelope: Mapping[str, Any]) -> None:
        body = json.dumps(dict(envelope)).encode("utf-8")
        for attempt in range(1, self.retries + 1):
            try:
                self._post(body)
                return
            except Exception:  # noqa: BLE001 — 네트워크/터널 단절 등.
                if attempt < self.retries:
                    self.sleep(self.retry_sleep_seconds)
        self._spill(envelope)

    def _post(self, body: bytes) -> None:
        request = urllib.request.Request(self.url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            if response.status != 200:
                raise urllib.error.HTTPError(self.url, response.status, "ingest rejected", response.headers, None)

    def _spill(self, envelope: Mapping[str, Any]) -> None:
        """터널이 끝내 안 되면 결과를 로컬 파일로 보존(나중에 재전송/복구용)."""
        if self.fallback_dir is None:
            return
        with self._lock:
            self.fallback_dir.mkdir(parents=True, exist_ok=True)
            rid = str(envelope.get("request_id") or "unknown")
            slot = str(envelope.get("slot_id") or "")
            target = self.fallback_dir / f"{rid}.{slot}.json"
            tmp = target.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(dict(envelope)), encoding="utf-8")
            tmp.rename(target)


def replay_spilled(sink_url: str, fallback_dir: Path, *, timeout_seconds: float = 10.0) -> int:
    """`fallback_dir`에 쌓인(터널 복구 후) envelope들을 ingest로 재전송. 반환: 성공 건수."""
    if not fallback_dir.exists():
        return 0
    sent = 0
    for path in sorted(fallback_dir.glob("*.json")):
        envelope = json.loads(path.read_text(encoding="utf-8"))
        body = json.dumps(envelope).encode("utf-8")
        request = urllib.request.Request(
            sink_url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                if response.status != 200:
                    continue
        except Exception:  # noqa: BLE001
            continue
        path.unlink()
        sent += 1
    return sent


def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise ValueError("Content-Length must be an integer") from exc
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    return payload


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


__all__ = [
    "DEFAULT_INGEST_PORT",
    "HttpResultSink",
    "replay_spilled",
    "start_result_ingest_server",
]
