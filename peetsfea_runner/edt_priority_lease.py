"""TOML lease :7878 — 슈퍼컴 전용 백채널 (컨트롤플레인 registry → 컨테이너 워커).

토폴로지는 결과 ingest(:7876)와 동일: gate 경유 ssh 터널 + loopback 바인딩(외부 도달 불가, 인증 불필요).
7878도 7875 사용자가 모르는 슈퍼컴 전용 통로다(7875=공개 sweep intake가 채우고, 7878=워커가 당겨감).

**무거운 cadquery 샘플링은 web이 아니라 컨테이너 워커가 한다**(메모리 정상화의 핵심; baseline과 대칭):
- **컨트롤플레인(web, `start_priority_lease_server`)**: `127.0.0.1:7878`. `GET /lease?n=K`로 가장 오래된
  sweep 요청에서 **chunk(최대 K개)** 를 분배(remaining_count 차감). web은 toml 문자열만 들고 있다.
- **컨테이너 워커(`PriorityPuller`)**: 로컬 우선순위 레인이 저수위면 chunk를 lease → **컨테이너 안에서
  count개 샘플링**(geometry 빌드 = 어차피 솔브 때 하는 일) → `extend_priority`. 블렌드(85:15)는
  워커 로컬 `TwoLaneQueue.get()`이 수행 — 우선순위 재고가 있으면 85%, 없으면 100% baseline.

lease 의미론은 best-effort(at-most-once): chunk를 당긴(remaining 차감된) 워커가 솔브 전에 죽으면 그
chunk는 유실(재배달 없음). 우선순위는 sweep 샘플이라 유실 허용. chunk/저수위를 작게 잡아 유실 폭을 줄인다.
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

from .edt_queue import QueueItem, TwoLaneQueue

DEFAULT_PRIORITY_LEASE_PORT = 7878
_MAX_LEASE_BATCH = 256  # 한 번에 분배할 chunk 상한(워커 폭주/유실 폭 제한).

# 컨테이너에서 sweep을 count개 후보 toml로 샘플링: (sweep_text, count, seed) -> list[toml text].
ChunkSampler = Callable[[str, int, int], list[str]]


def start_priority_lease_server(
    *, queue: Any, host: str = "127.0.0.1", port: int = DEFAULT_PRIORITY_LEASE_PORT
) -> ThreadingHTTPServer:
    """TOML chunk 분배 서버. `GET /lease?n=K` → chunk(최대 K), `GET /health` → 상태.

    `queue`는 `lease_chunk(n) -> dict|None`를 가진 객체다. 새 런타임은 DbTomlRegistry를,
    레거시 테스트/호환 경로는 DbPriorityQueue를 넣을 수 있다.
    """

    def health_payload() -> dict[str, object]:
        active_count = getattr(queue, "active_count", None)
        if callable(active_count):
            return {"status": "ok", "active_tomls": int(active_count())}
        depth = getattr(queue, "depth", None)
        if callable(depth):
            return {"status": "ok", "priority_depth": int(depth())}
        return {"status": "ok"}

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-priority-lease"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                _write_json(self, 200, health_payload())
                return
            if parsed.path == "/lease":
                raw = parse_qs(parsed.query).get("n", ["1"])[0]
                n = int(raw) if raw.isdigit() else 1
                n = max(1, min(n, _MAX_LEASE_BATCH))
                chunk = queue.lease_chunk(n)
                _write_json(self, 200, chunk or {})  # chunk 없으면 빈 객체
                return
            _write_json(self, 404, {"error": "not_found"})

    return ThreadingHTTPServer((host, port), Handler)


@dataclass
class PriorityPuller:
    """컨테이너 워커측: 로컬 우선순위 레인이 저수위면 chunk를 lease → **컨테이너에서 샘플링** → 보충.

    baseline 자기공급([[edt_queue.BaselineRefiller]])과 짝을 이룬다 — 둘 다 컨테이너에서 cadquery 샘플링.
    chunk가 없으면 backoff(우선순위 없음 = 100% baseline로 자연 강등).
    """

    queue: TwoLaneQueue  # 로컬(컨테이너) 큐
    lease_url: str
    sampler: ChunkSampler  # 컨테이너 peetsfea 샘플러(baseline과 동일 함수 주입)
    batch: int = 8
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
            chunk = self._lease()
            if chunk is None:
                self._stop.wait(self.poll_seconds)  # chunk 없음/오류 → backoff(baseline로 강등).
                continue
            items = self._sample(chunk)
            if items:
                self.queue.extend_priority(items)

    def _lease(self) -> dict[str, Any] | None:
        sep = "&" if "?" in self.lease_url else "?"
        url = f"{self.lease_url}{sep}n={int(self.batch)}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:  # noqa: BLE001 — 네트워크/터널 단절은 정상(backoff).
            return None
        if not isinstance(payload, Mapping) or not payload.get("sweep_toml_text"):
            return None  # 빈 객체 = chunk 없음
        return dict(payload)

    def _sample(self, chunk: Mapping[str, Any]) -> list[QueueItem]:
        sweep = chunk.get("sweep_toml_text")
        count = int(chunk.get("count") or 0)
        seed_base = int(chunk.get("seed_base") or 0)
        rid = str(chunk.get("request_id") or "sweep")
        mode = str(chunk.get("mode") or "full")
        if not isinstance(sweep, str) or not sweep or count <= 0:
            return []
        try:
            texts = self.sampler(sweep, count, seed_base)  # cadquery 샘플링(컨테이너에서; 무거움)
        except Exception:  # noqa: BLE001 — 샘플 실패가 풀러를 죽이면 안 된다.
            return []
        return [
            QueueItem(request_id=f"{rid}-{seed_base + i}", candidate_toml_text=text, seed=seed_base + i, mode=mode)
            for i, text in enumerate(texts)
        ]

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
    "ChunkSampler",
    "PriorityPuller",
    "start_priority_lease_server",
]
