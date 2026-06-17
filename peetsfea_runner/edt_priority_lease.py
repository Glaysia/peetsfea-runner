"""우선순위 lease :7878 — 슈퍼컴 전용 백채널 (컨트롤플레인 우선순위 큐 → 컨테이너 워커).

토폴로지는 결과 ingest(:7876)와 동일: gate 경유 ssh 터널 + loopback 바인딩(외부 도달 불가, 인증 불필요).
7878도 7875 사용자가 모르는 슈퍼컴 전용 통로다(7875=공개 sweep intake가 채우고, 7878=워커가 당겨감).

- **컨트롤플레인(web, `start_priority_lease_server`)**: `127.0.0.1:7878`. `GET /lease?n=K`로 우선순위
  레인에서 최대 K건 pop해 반환(drain). intake(:7875)가 채운 큐를 슈퍼컴 워커들에 분배한다.
- **컨테이너 워커(`PriorityPuller`)**: 로컬 우선순위 레인이 저수위면 lease를 당겨 `extend_priority`.
  블렌드(85:15)는 워커 로컬 `TwoLaneQueue.get()`이 수행 — 우선순위 재고가 있으면 85%, 없으면 100% baseline.

lease 의미론은 best-effort(at-most-once): 당긴 워커가 solve 전에 죽으면 그 건은 유실(재배달 없음).
우선순위는 sweep 샘플이라 유실 허용. 배치/저수위를 작게 잡아 in-flight 유실을 줄인다.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .edt_queue import QueueItem, TwoLaneQueue

DEFAULT_PRIORITY_LEASE_PORT = 7878
_MAX_LEASE_BATCH = 256  # 한 번에 분배할 상한(워커 폭주/유실 폭 제한).


def start_priority_lease_server(
    *, queue: TwoLaneQueue, host: str = "127.0.0.1", port: int = DEFAULT_PRIORITY_LEASE_PORT
) -> ThreadingHTTPServer:
    """우선순위 레인 분배 서버. `GET /lease?n=K` → 최대 K건 pop, `GET /health` → 깊이."""

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-priority-lease"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                priority_depth, _ = queue.depths()
                _write_json(self, 200, {"status": "ok", "priority_depth": priority_depth})
                return
            if parsed.path == "/lease":
                raw = parse_qs(parsed.query).get("n", ["1"])[0]
                n = int(raw) if raw.isdigit() else 1
                n = max(1, min(n, _MAX_LEASE_BATCH))
                items = queue.lease_priority(n)
                _write_json(self, 200, {"items": [_item_to_json(it) for it in items]})
                return
            _write_json(self, 404, {"error": "not_found"})

    return ThreadingHTTPServer((host, port), Handler)


def _item_to_json(item: QueueItem) -> dict[str, object]:
    return {
        "request_id": item.request_id,
        "candidate_toml_text": item.candidate_toml_text,
        "seed": item.seed,
        "mode": item.mode,
    }


def _item_from_json(raw: Mapping[str, object]) -> QueueItem | None:
    text = raw.get("candidate_toml_text")
    if not isinstance(text, str) or not text:
        return None
    return QueueItem(
        request_id=str(raw.get("request_id") or ""),
        candidate_toml_text=text,
        seed=int(raw.get("seed") or 0) if isinstance(raw.get("seed"), int) else 0,
        mode=str(raw.get("mode") or "full"),
    )


@dataclass
class PriorityPuller:
    """컨테이너 워커측: 로컬 우선순위 레인이 저수위면 컨트롤플레인 lease를 당겨 보충하는 백그라운드 워커.

    baseline 자기공급([[edt_queue.BaselineRefiller]])과 짝을 이룬다 — 이쪽은 우선순위를 원격에서 당겨오고,
    블렌드는 `TwoLaneQueue.get()`이 처리한다. lease가 비면 backoff(우선순위 없음 = 100% baseline로 자연 강등).
    """

    queue: TwoLaneQueue
    lease_url: str
    batch: int = 16
    low_watermark: int = 8
    poll_seconds: float = 2.0
    timeout_seconds: float = 10.0
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="edt-priority-pull", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            priority_depth, _ = self.queue.depths()
            if priority_depth >= self.low_watermark:
                self._stop.wait(self.poll_seconds)
                continue
            items = self._lease()
            if items:
                self.queue.extend_priority(items)
            else:
                self._stop.wait(self.poll_seconds)  # 우선순위 없음/오류 → backoff(baseline로 강등).

    def _lease(self) -> list[QueueItem]:
        sep = "&" if "?" in self.lease_url else "?"
        url = f"{self.lease_url}{sep}n={int(self.batch)}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:  # noqa: BLE001 — 네트워크/터널 단절은 정상(backoff).
            return []
        raw_items = payload.get("items") if isinstance(payload, Mapping) else None
        if not isinstance(raw_items, list):
            return []
        out: list[QueueItem] = []
        for raw in raw_items:
            if isinstance(raw, Mapping):
                item = _item_from_json(raw)
                if item is not None:
                    out.append(item)
        return out

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10)


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
    "DEFAULT_PRIORITY_LEASE_PORT",
    "PriorityPuller",
    "start_priority_lease_server",
]
