from __future__ import annotations

import hashlib
import json
import threading
import traceback
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_SINGLE_ACCOUNT_ID = "account_01"
DEFAULT_SINGLE_HOST_ALIAS = "gate1-harry261"
EXPECTED_PEETSFEA_VERSION = "0.3.5"

SimulationPrimitive = Callable[..., Mapping[str, Any]]
VersionLoader = Callable[[], str]


class SimulationRequestError(ValueError):
    pass


class SimulationBusyError(RuntimeError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _load_peetsfea_version() -> str:
    import peetsfea

    return str(peetsfea.__version__)


def _load_default_primitive() -> SimulationPrimitive:
    from peetsfea.ssw_random_sample_reports import run_ssw_random_sample_reports_from_toml_text

    return run_ssw_random_sample_reports_from_toml_text


@dataclass(slots=True)
class SingleSimulationService:
    output_root: Path
    account_id: str = DEFAULT_SINGLE_ACCOUNT_ID
    host_alias: str = DEFAULT_SINGLE_HOST_ALIAS
    remote_job_id: str = ""
    api_session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    primitive: SimulationPrimitive | None = None
    version_loader: VersionLoader = _load_peetsfea_version
    _busy_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _busy: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.output_root = Path(self.output_root).expanduser().resolve()
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.account_id = self.account_id.strip() or DEFAULT_SINGLE_ACCOUNT_ID
        self.host_alias = self.host_alias.strip() or DEFAULT_SINGLE_HOST_ALIAS

    @property
    def busy(self) -> bool:
        return self._busy

    def health(self) -> dict[str, Any]:
        peetsfea_version = self.version_loader()
        return {
            "status": "ok",
            "busy": self.busy,
            "account_id": self.account_id,
            "host_alias": self.host_alias,
            "remote_job_id": self.remote_job_id,
            "api_session_id": self.api_session_id,
            "peetsfea_version": peetsfea_version,
            "expected_peetsfea_version": EXPECTED_PEETSFEA_VERSION,
            "peetsfea_version_ok": peetsfea_version == EXPECTED_PEETSFEA_VERSION,
        }

    def simulate(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        request_id = self._request_id(payload)
        candidate_toml_text = self._candidate_toml_text(payload)
        seed = self._seed(payload)
        mode = self._mode(payload)
        if not self._busy_lock.acquire(blocking=False):
            raise SimulationBusyError("single simulation API is busy")
        self._busy = True
        started_at = _utc_now_iso()
        try:
            input_toml_hash = _sha256_text(candidate_toml_text)
            job_output_dir = self.output_root / request_id
            primitive = self.primitive or _load_default_primitive()
            result = primitive(
                candidate_toml_text,
                output_dir=job_output_dir,
                seed=seed,
                mode=mode,
            )
            finished_at = _utc_now_iso()
            return {
                "request_id": request_id,
                "terminal_state": "success",
                "started_at": started_at,
                "finished_at": finished_at,
                "account_id": self.account_id,
                "host_alias": self.host_alias,
                "remote_job_id": self.remote_job_id,
                "api_session_id": self.api_session_id,
                "input_toml_hash": input_toml_hash,
                "peetsfea_version": self.version_loader(),
                "mode": mode,
                "seed": seed,
                "output_dir": str(job_output_dir),
                "result": _jsonable(result),
            }
        except Exception as exc:
            finished_at = _utc_now_iso()
            return {
                "request_id": request_id or self._fallback_request_id(),
                "terminal_state": "failed",
                "started_at": started_at,
                "finished_at": finished_at,
                "account_id": self.account_id,
                "host_alias": self.host_alias,
                "remote_job_id": self.remote_job_id,
                "api_session_id": self.api_session_id,
                "input_toml_hash": _sha256_text(candidate_toml_text) if candidate_toml_text else "",
                "peetsfea_version": self._safe_version(),
                "mode": str(payload.get("mode") or "full"),
                "seed": payload.get("seed"),
                "result": {},
                "error": {
                    "stage": "simulate",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
            }
        finally:
            self._busy = False
            self._busy_lock.release()

    def _safe_version(self) -> str:
        try:
            return self.version_loader()
        except Exception:
            return ""

    @staticmethod
    def _fallback_request_id() -> str:
        return f"request-{uuid.uuid4().hex}"

    def _request_id(self, payload: Mapping[str, Any]) -> str:
        raw = payload.get("request_id")
        if raw is None or str(raw).strip() == "":
            return self._fallback_request_id()
        request_id = str(raw).strip()
        if "/" in request_id or request_id in {".", ".."}:
            raise SimulationRequestError("request_id must be a simple path segment")
        return request_id

    @staticmethod
    def _candidate_toml_text(payload: Mapping[str, Any]) -> str:
        raw = payload.get("candidate_toml_text")
        if not isinstance(raw, str) or raw.strip() == "":
            raise SimulationRequestError("candidate_toml_text must be a non-empty string")
        return raw

    @staticmethod
    def _seed(payload: Mapping[str, Any]) -> int:
        raw = payload.get("seed", 0)
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise SimulationRequestError("seed must be an integer")
        return raw

    @staticmethod
    def _mode(payload: Mapping[str, Any]) -> str:
        raw = str(payload.get("mode") or "full").strip()
        if raw not in {"full", "semi_dry"}:
            raise SimulationRequestError("mode must be 'full' or 'semi_dry'")
        return raw


def start_single_simulation_api_server(
    *,
    service: SingleSimulationService,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-single-simulation"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path != "/health":
                _write_json(self, 404, {"error": "not_found"})
                return
            try:
                _write_json(self, 200, service.health())
            except Exception as exc:
                _write_json(self, 500, _error_payload(exc, stage="health"))

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path == "/simulate":
                self._handle_simulate()
                return
            if path == "/shutdown":
                _write_json(self, 200, {"status": "shutdown"})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
            _write_json(self, 404, {"error": "not_found"})

        def _handle_simulate(self) -> None:
            try:
                payload = _read_json(self)
                envelope = service.simulate(payload)
                status = 200 if envelope.get("terminal_state") == "success" else 500
                _write_json(self, status, envelope)
            except SimulationBusyError as exc:
                _write_json(self, 409, _error_payload(exc, stage="simulate"))
            except SimulationRequestError as exc:
                _write_json(self, 400, _error_payload(exc, stage="request"))
            except Exception as exc:
                _write_json(self, 500, _error_payload(exc, stage="simulate"))

    return ThreadingHTTPServer((host, port), Handler)


def _read_json(handler: BaseHTTPRequestHandler) -> Mapping[str, Any]:
    raw_length = handler.headers.get("Content-Length", "0")
    try:
        length = int(raw_length)
    except ValueError as exc:
        raise SimulationRequestError("Content-Length must be an integer") from exc
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SimulationRequestError(f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise SimulationRequestError("JSON body must be an object")
    return payload


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]) -> None:
    body = json.dumps(_jsonable(payload), sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _error_payload(exc: Exception, *, stage: str) -> dict[str, Any]:
    return {
        "terminal_state": "failed",
        "error": {
            "stage": stage,
            "type": type(exc).__name__,
            "message": str(exc),
        },
    }


__all__ = [
    "DEFAULT_SINGLE_ACCOUNT_ID",
    "DEFAULT_SINGLE_HOST_ALIAS",
    "EXPECTED_PEETSFEA_VERSION",
    "SingleSimulationService",
    "SimulationBusyError",
    "SimulationRequestError",
    "start_single_simulation_api_server",
]
