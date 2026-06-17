"""Intake 서비스 :7875 + baseline 샘플러 (Phase 4, MASTER_PLAN §2.6 / §2.6.1).

- **Intake `:7875`:** 온전한 sweep toml + 개수 N 수신 → peetsfea로 **범위 검증**(기준 이내인지,
  넓으면 거절) → **랜덤 샘플 fixed × N** → `TwoLaneQueue` **우선순위 레인** 적재.
- **baseline 샘플러:** 전체 기준 sweep을 풀스페이스 랜덤 샘플해 baseline 레인을 리필(배치마다
  **seed 롤링**). 슬롯이 놀지 않게 하는 상시 공급원.

peetsfea(0.3.4) 호출은 `validator`/`sampler`로 분리(테스트는 fake, 실서비스는 peetsfea
`validate_sweep_toml_text` / `sample_fixed_candidates_from_toml_text`).
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .edt_queue import BaselineSampler, QueueItem

# (sweep_text) -> None. 기준 범위를 벗어나면 예외.
SweepValidator = Callable[[str], None]
# (sweep_text, count, seed) -> fixed candidate toml 텍스트 N개.
SweepSampler = Callable[[str, int, int], list[str]]


class IntakeRequestError(ValueError):
    pass


def _light_validator(sweep_text: str) -> None:
    """가벼운 검증 — toml 파싱만(geometry 빌드 없음). web 메모리 정상화의 핵심.

    범위/기하 타당성은 워커 샘플러의 rejection sampling이 어차피 거른다(불가능 설계는 후보가 안 나옴).
    무거운 `validate_sweep_toml_text`(cadquery geometry 빌드 → web 메모리 누수)는 요청 경로에서 뺀다.
    """
    import tomllib

    try:
        parsed = tomllib.loads(sweep_text)
    except Exception as exc:  # noqa: BLE001 — 파싱 실패는 입력 오류.
        raise IntakeRequestError(f"sweep_toml parse failed: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise IntakeRequestError("sweep_toml must be a non-empty TOML table")


def _peetsfea_validator(sweep_text: str) -> None:
    """무거운 geometry 기반 범위 검증(cadquery 빌드). web 핫패스엔 부적합 — 기본 아님."""
    from peetsfea.ssw_design_space import validate_sweep_toml_text

    validate_sweep_toml_text(sweep_text)


def _peetsfea_sampler(sweep_text: str, count: int, seed: int) -> list[str]:
    from peetsfea.ssw_design_space import sample_fixed_candidates_from_toml_text

    return list(sample_fixed_candidates_from_toml_text(sweep_text, count, seed))


def make_baseline_sampler(
    reference_sweep_text: str,
    *,
    batch_size: int = 1000,
    sampler: SweepSampler | None = None,
    seed_start: int = 0,
) -> BaselineSampler:
    """전체 기준 sweep을 풀샘플하는 baseline 리필 샘플러(배치마다 seed 롤링)."""

    sample = sampler if sampler is not None else _peetsfea_sampler
    state = {"seed": seed_start}

    def refill() -> list[QueueItem]:
        seed = state["seed"]
        state["seed"] += 1
        fixed = sample(reference_sweep_text, batch_size, seed)
        return [
            QueueItem(request_id=f"base-{seed}-{i}", candidate_toml_text=text, seed=seed, mode="full")
            for i, text in enumerate(fixed)
        ]

    return refill


@dataclass
class IntakeService:
    """7875 인입 처리: 가벼운 검증 → **sweep 요청 1줄 적재**(샘플링 없음).

    무거운 cadquery 샘플링은 web이 아니라 컨테이너 워커가 lease 후 수행(baseline과 대칭). `queue`는
    `enqueue_sweep(request_id, sweep_toml_text, seed, count, mode)`를 가진 객체(예: DbPriorityQueue).
    """

    queue: Any  # enqueue_sweep(...)를 가진 큐(DbPriorityQueue)
    validator: SweepValidator = _light_validator  # 기본은 경량(toml 파싱). geometry 검증은 워커가 함
    _seq: int = field(default=0, init=False)
    _seq_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def submit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        sweep_text = payload.get("sweep_toml_text")
        if not isinstance(sweep_text, str) or sweep_text.strip() == "":
            raise IntakeRequestError("sweep_toml_text must be a non-empty string")
        raw_count = payload.get("count")
        if isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count <= 0:
            raise IntakeRequestError("count must be a positive integer")
        raw_seed = payload.get("seed", 0)
        if isinstance(raw_seed, bool) or not isinstance(raw_seed, int):
            raise IntakeRequestError("seed must be an integer")

        # 기준 범위 검증(넓으면 거절 → 400). geometry 빌드 없음(샘플링은 컨테이너로 내림).
        try:
            self.validator(sweep_text)
        except IntakeRequestError:
            raise
        except Exception as exc:
            raise IntakeRequestError(f"sweep range validation failed: {exc}") from exc
        # sweep 요청 1줄만 적재 → 즉시 반환(샘플링은 워커가 chunk lease 후 수행).
        batch = self._next_seq()
        request_id = f"sweep-{batch}"
        self.queue.enqueue_sweep(
            request_id=request_id, sweep_toml_text=sweep_text, seed=raw_seed, count=raw_count, mode="full"
        )
        return {"accepted": raw_count, "request_id": request_id, "batch": batch}


def start_intake_server(*, service: IntakeService, host: str = "127.0.0.1", port: int = 7875) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-intake"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            if urlparse(self.path).path == "/health":
                _write_json(self, 200, {"status": "ok", "priority_depth": service.queue.depth()})
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/submit":
                _write_json(self, 404, {"error": "not_found"})
                return
            try:
                payload = _read_json(self)
                _write_json(self, 200, service.submit(payload))
            except IntakeRequestError as exc:
                _write_json(self, 400, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                _write_json(self, 500, {"error": type(exc).__name__, "message": str(exc)})

    return ThreadingHTTPServer((host, port), Handler)


def _read_json(handler: BaseHTTPRequestHandler) -> Mapping[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise IntakeRequestError("Content-Length must be an integer") from exc
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise IntakeRequestError(f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise IntakeRequestError("JSON body must be an object")
    return payload


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


__all__ = [
    "IntakeRequestError",
    "IntakeService",
    "SweepSampler",
    "SweepValidator",
    "make_baseline_sampler",
    "start_intake_server",
]
