"""Adaptive TOML registry API (:7875).

The server owns a persistent pool of active TOML sources. Workers lease small
chunks from that pool and sample fixed candidates inside the container. The
built-in widest TOML is always present and cannot be disabled or deleted;
custom TOMLs are capped at six.
"""

from __future__ import annotations

import json
import time
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

BUILTIN_TOML_ID = "builtin-widest"
MAX_CUSTOM_TOMLS = 6

TomlValidator = Callable[[str], None]


class TomlRegistryRequestError(ValueError):
    def __init__(self, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


class TomlRegistryBackend(Protocol):
    def bootstrap_builtin_toml(self, *, toml_text: str, now: float, name: str = "built-in widest TOML") -> None: ...

    def list_tomls(self) -> list[dict[str, Any]]: ...

    def add_custom_toml(self, *, name: str, toml_text: str, active: bool, now: float) -> dict[str, Any]: ...

    def delete_custom_toml(self, toml_id: str) -> bool: ...

    def set_toml_active(self, toml_id: str, active: bool, *, now: float) -> bool: ...

    def set_toml_ratios(self, ratios: Mapping[str, float] | None, *, now: float) -> None: ...

    def active_count(self) -> int: ...

    def lease_chunk(self, k: int) -> dict[str, Any] | None: ...


def light_toml_validator(toml_text: str) -> None:
    try:
        parsed = tomllib.loads(toml_text)
    except Exception as exc:  # noqa: BLE001 - input validation boundary.
        raise TomlRegistryRequestError(f"TOML parse failed: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise TomlRegistryRequestError("TOML must be a non-empty table")


def load_default_builtin_toml_text(path: Path | None = None) -> str:
    if path is not None:
        return path.expanduser().read_text(encoding="utf-8")

    import peetsfea

    data_dir = Path(peetsfea.__file__).resolve().parent / "data"
    candidates = sorted(data_dir.glob("*_sweep.toml"))
    if not candidates:
        raise FileNotFoundError(f"no *_sweep.toml found in {data_dir}")
    return candidates[-1].read_text(encoding="utf-8")


@dataclass
class TomlRegistryService:
    registry: TomlRegistryBackend
    builtin_toml_text: str
    validator: TomlValidator = light_toml_validator
    clock: Callable[[], float] = time.time
    builtin_name: str = "built-in widest TOML"
    _initialized: bool = field(default=False, init=False, repr=False)

    def initialize(self) -> None:
        if self._initialized:
            return
        self.validator(self.builtin_toml_text)
        self.registry.bootstrap_builtin_toml(
            toml_text=self.builtin_toml_text, now=self.clock(), name=self.builtin_name
        )
        self._initialized = True

    def list_tomls(self) -> dict[str, Any]:
        self.initialize()
        tomls = self.registry.list_tomls()
        active = [t for t in tomls if bool(t.get("active"))]
        ratios_set = bool(active) and all(t.get("ratio") is not None for t in active)
        return {"tomls": tomls, "active_count": len(active), "ratios_set": ratios_set}

    def register_custom(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        toml_text = payload.get("toml_text")
        if not isinstance(toml_text, str) or toml_text.strip() == "":
            raise TomlRegistryRequestError("toml_text must be a non-empty string")
        self.validator(toml_text)
        raw_name = payload.get("name")
        name = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else "custom TOML"
        raw_active = payload.get("active", True)
        if not isinstance(raw_active, bool):
            raise TomlRegistryRequestError("active must be a boolean")
        try:
            entry = self.registry.add_custom_toml(
                name=name, toml_text=toml_text, active=raw_active, now=self.clock()
            )
        except ValueError as exc:
            raise TomlRegistryRequestError(str(exc), status=409) from exc
        return {"toml": entry}

    def unregister_custom(self, toml_id: str) -> dict[str, Any]:
        self.initialize()
        if toml_id == BUILTIN_TOML_ID:
            raise TomlRegistryRequestError("built-in TOML cannot be unregistered", status=409)
        if not self.registry.delete_custom_toml(toml_id):
            raise TomlRegistryRequestError("custom TOML not found", status=404)
        return {"deleted": toml_id}

    def set_active(self, toml_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        if toml_id == BUILTIN_TOML_ID:
            raise TomlRegistryRequestError("built-in TOML is always active", status=409)
        raw_active = payload.get("active")
        if not isinstance(raw_active, bool):
            raise TomlRegistryRequestError("active must be a boolean")
        if not self.registry.set_toml_active(toml_id, raw_active, now=self.clock()):
            raise TomlRegistryRequestError("TOML not found", status=404)
        return {"id": toml_id, "active": raw_active}

    def set_ratios(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        raw_ratios = payload.get("ratios")
        if raw_ratios is None:
            self.registry.set_toml_ratios(None, now=self.clock())
            return {"ratios_set": False}
        if not isinstance(raw_ratios, Mapping):
            raise TomlRegistryRequestError("ratios must be an object or null")

        active_ids = {str(t["id"]) for t in self.registry.list_tomls() if bool(t.get("active"))}
        provided: dict[str, float] = {}
        for key, value in raw_ratios.items():
            toml_id = str(key)
            if toml_id not in active_ids:
                raise TomlRegistryRequestError(f"ratio target is not active: {toml_id}")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TomlRegistryRequestError(f"ratio must be numeric: {toml_id}")
            ratio = float(value)
            if ratio <= 0:
                raise TomlRegistryRequestError(f"ratio must be positive: {toml_id}")
            provided[toml_id] = ratio
        missing = sorted(active_ids - set(provided))
        if missing:
            raise TomlRegistryRequestError(f"missing active TOML ratios: {', '.join(missing)}")
        total = sum(provided.values())
        if abs(total - 100.0) > 1e-6:
            raise TomlRegistryRequestError(f"active TOML ratios must sum to 100, got {total:g}")
        self.registry.set_toml_ratios(provided, now=self.clock())
        return {"ratios_set": True, "ratios": provided}

    def lease_chunk(self, k: int) -> dict[str, Any] | None:
        self.initialize()
        return self.registry.lease_chunk(k)

    def active_count(self) -> int:
        self.initialize()
        return self.registry.active_count()


def start_toml_registry_server(
    *, service: TomlRegistryService, host: str = "127.0.0.1", port: int = 7875
) -> ThreadingHTTPServer:
    service.initialize()

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-toml-registry"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                _write_json(
                    self,
                    200,
                    {"status": "ok", "active_tomls": service.registry.active_count()},
                )
                return
            if parsed.path == "/api/tomls":
                _write_json(self, 200, service.list_tomls())
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/tomls/custom":
                self._handle(lambda: service.register_custom(_read_json(self)))
                return
            if parsed.path == "/submit":
                _write_json(
                    self,
                    410,
                    {"error": "gone", "message": "use /api/tomls custom registration and ratios"},
                )
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_DELETE(self) -> None:
            prefix = "/api/tomls/custom/"
            parsed = urlparse(self.path)
            if parsed.path.startswith(prefix):
                toml_id = parsed.path[len(prefix):]
                self._handle(lambda: service.unregister_custom(toml_id))
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_PATCH(self) -> None:
            parsed = urlparse(self.path)
            suffix = "/active"
            prefix = "/api/tomls/"
            if parsed.path.startswith(prefix) and parsed.path.endswith(suffix):
                toml_id = parsed.path[len(prefix):-len(suffix)]
                self._handle(lambda: service.set_active(toml_id, _read_json(self)))
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_PUT(self) -> None:
            if urlparse(self.path).path == "/api/tomls/ratios":
                self._handle(lambda: service.set_ratios(_read_json(self)))
                return
            _write_json(self, 404, {"error": "not_found"})

        def _handle(self, fn: Callable[[], Mapping[str, Any]]) -> None:
            try:
                _write_json(self, 200, fn())
            except TomlRegistryRequestError as exc:
                _write_json(self, exc.status, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                _write_json(self, 500, {"error": type(exc).__name__, "message": str(exc)})

    return ThreadingHTTPServer((host, port), Handler)


def _read_json(handler: BaseHTTPRequestHandler) -> Mapping[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise TomlRegistryRequestError("Content-Length must be an integer") from exc
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise TomlRegistryRequestError(f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise TomlRegistryRequestError("JSON body must be an object")
    return payload


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


__all__ = [
    "BUILTIN_TOML_ID",
    "MAX_CUSTOM_TOMLS",
    "TomlRegistryBackend",
    "TomlRegistryRequestError",
    "TomlRegistryService",
    "TomlValidator",
    "light_toml_validator",
    "load_default_builtin_toml_text",
    "start_toml_registry_server",
]
