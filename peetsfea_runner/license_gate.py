from __future__ import annotations

import logging
import re
import subprocess
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Callable

from .license_policy import (
    LICENSE_FEATURE,
    LICENSE_FEATURE_CEILING,
    LICENSE_FEATURE_POLL_TTL_SECONDS,
    LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS,
    LICENSE_POLL_COMMAND,
    LICENSE_POLL_ENV,
    LICENSE_POLL_SOURCE_HOST,
)


_LICENSE_FEATURE_PATTERN = re.compile(
    rf"Users of {re.escape(LICENSE_FEATURE)}:\s+\(Total of \d+ licenses issued;\s+Total of (\d+) licenses in use\)",
    re.IGNORECASE,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class LicenseGateResult:
    is_open: bool
    license_in_use: int | None
    license_ceiling: int
    status: str
    license_feature: str = LICENSE_FEATURE
    license_gate: str | None = None
    error: str | None = None
    source: str = "fresh"
    checked_at_monotonic: float = 0.0
    expires_at_monotonic: float = 0.0

    def as_web_status(self) -> dict[str, object]:
        return {
            "is_open": self.is_open,
            "license_gate": self.license_gate,
            "license_feature": self.license_feature,
            "license_in_use": self.license_in_use,
            "license_ceiling": self.license_ceiling,
            "status": self.status,
            "error": self.error,
            "source": self.source,
        }


class LicenseGate:
    def __init__(
        self,
        *,
        ttl_seconds: int = LICENSE_FEATURE_POLL_TTL_SECONDS,
        ceiling: int = LICENSE_FEATURE_CEILING,
        timeout_seconds: int = LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS,
        source_host: str = LICENSE_POLL_SOURCE_HOST,
        poll_env: str = LICENSE_POLL_ENV,
        poll_command: str = LICENSE_POLL_COMMAND,
        clock: Callable[[], float] = time.monotonic,
        query_func: Callable[..., Any] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        if ceiling <= 0:
            raise ValueError("ceiling must be > 0")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        self._ttl_seconds = int(ttl_seconds)
        self._ceiling = int(ceiling)
        self._timeout_seconds = int(timeout_seconds)
        self._source_host = str(source_host).strip()
        self._poll_env = str(poll_env).strip()
        self._poll_command = str(poll_command).strip()
        self._clock = clock
        self._query_func = query_func
        self._refresh_lock = threading.Lock()
        self._cached_result: LicenseGateResult | None = None
        if not self._source_host:
            raise ValueError("source_host must not be empty")
        if not self._poll_command:
            raise ValueError("poll_command must not be empty")

    def check(
        self,
        *,
        ssh_config_path: str = "",
        timeout_seconds: int | None = None,
        force_refresh: bool = False,
    ) -> LicenseGateResult:
        now = self._clock()
        cached = self._cached_result
        if not force_refresh and cached is not None and now < cached.expires_at_monotonic:
            return replace(cached, source="cache")

        with self._refresh_lock:
            now = self._clock()
            cached = self._cached_result
            if not force_refresh and cached is not None and now < cached.expires_at_monotonic:
                return replace(cached, source="cache")

            result = self._refresh(
                now=now,
                ssh_config_path=ssh_config_path,
                timeout_seconds=timeout_seconds or self._timeout_seconds,
            )
            self._cached_result = result
            return result

    def reset(self) -> None:
        with self._refresh_lock:
            self._cached_result = None

    def _refresh(
        self,
        *,
        now: float,
        ssh_config_path: str,
        timeout_seconds: int,
    ) -> LicenseGateResult:
        try:
            snapshot = self._query(ssh_config_path=ssh_config_path, timeout_seconds=timeout_seconds)
            license_in_use = _extract_license_in_use(snapshot)
            error = _extract_error(snapshot)
            if _snapshot_failed(snapshot):
                LOGGER.warning("License gate fail-open: %s", error or "license query failed")
                return self._open_result(
                    now=now,
                    status="FAIL_OPEN",
                    error=error or "license query failed",
                )
            if license_in_use is None:
                error = error or f"missing {LICENSE_FEATURE} usage"
                LOGGER.warning("License gate fail-open: %s", error)
                return self._open_result(now=now, status="FAIL_OPEN", error=error)
            if license_in_use >= self._ceiling:
                return LicenseGateResult(
                    is_open=False,
                    license_in_use=license_in_use,
                    license_ceiling=self._ceiling,
                    status="CLOSED",
                    license_gate="license_closed",
                    checked_at_monotonic=now,
                    expires_at_monotonic=now + self._ttl_seconds,
                )
            return LicenseGateResult(
                is_open=True,
                license_in_use=license_in_use,
                license_ceiling=self._ceiling,
                status="OPEN",
                checked_at_monotonic=now,
                expires_at_monotonic=now + self._ttl_seconds,
            )
        except Exception as exc:
            LOGGER.warning("License gate fail-open: %s", exc)
            return self._open_result(now=now, status="FAIL_OPEN", error=str(exc))

    def _query(self, *, ssh_config_path: str, timeout_seconds: int) -> Any:
        if self._query_func is not None:
            return self._query_func(ssh_config_path=ssh_config_path, timeout_seconds=timeout_seconds)

        return query_license_gate_usage(
            ssh_config_path=ssh_config_path,
            timeout_seconds=timeout_seconds,
            source_host=self._source_host,
            poll_env=self._poll_env,
            poll_command=self._poll_command,
            ceiling=self._ceiling,
        )

    def _open_result(self, *, now: float, status: str, error: str | None) -> LicenseGateResult:
        return LicenseGateResult(
            is_open=True,
            license_in_use=None,
            license_ceiling=self._ceiling,
            status=status,
            error=error,
            checked_at_monotonic=now,
            expires_at_monotonic=now + self._ttl_seconds,
        )


def query_license_gate_usage(
    *,
    ssh_config_path: str = "",
    timeout_seconds: int = LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS,
    source_host: str = LICENSE_POLL_SOURCE_HOST,
    poll_env: str = LICENSE_POLL_ENV,
    poll_command: str = LICENSE_POLL_COMMAND,
    ceiling: int = LICENSE_FEATURE_CEILING,
) -> dict[str, object]:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5"]
    normalized_ssh_config_path = str(ssh_config_path).strip()
    if normalized_ssh_config_path:
        command.extend(["-F", normalized_ssh_config_path])
    command.extend([source_host, f"{poll_env} {poll_command}".strip()])
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "FAILED",
            "license_feature": LICENSE_FEATURE,
            "license_in_use": None,
            "license_ceiling": ceiling,
            "error": f"timeout={timeout_seconds}s",
        }
    output = "\n".join(part for part in ((completed.stdout or ""), (completed.stderr or "")) if part).strip()
    if completed.returncode != 0:
        return {
            "status": "FAILED",
            "license_feature": LICENSE_FEATURE,
            "license_in_use": None,
            "license_ceiling": ceiling,
            "error": output or f"return code={completed.returncode}",
        }
    match = _LICENSE_FEATURE_PATTERN.search(output)
    if match is None:
        return {
            "status": "FAILED",
            "license_feature": LICENSE_FEATURE,
            "license_in_use": None,
            "license_ceiling": ceiling,
            "error": f"missing {LICENSE_FEATURE} usage",
        }
    return {
        "status": "OK",
        "license_feature": LICENSE_FEATURE,
        "license_in_use": int(match.group(1)),
        "license_ceiling": ceiling,
        "error": None,
    }


def _extract_license_in_use(snapshot: Any) -> int | None:
    if isinstance(snapshot, int):
        return max(0, snapshot)
    if isinstance(snapshot, dict):
        for key in ("license_in_use", "electronics_desktop_in_use"):
            value = snapshot.get(key)
            if value is not None:
                return max(0, int(value))
        return None
    for attr in ("license_in_use", "electronics_desktop_in_use"):
        value = getattr(snapshot, attr, None)
        if value is not None:
            return max(0, int(value))
    return None


def _snapshot_failed(snapshot: Any) -> bool:
    status = None
    if isinstance(snapshot, dict):
        status = snapshot.get("status")
    else:
        status = getattr(snapshot, "status", None)
    return str(status or "").upper() in {"FAILED", "ERROR", "TIMEOUT"}


def _extract_error(snapshot: Any) -> str | None:
    if isinstance(snapshot, dict):
        error = snapshot.get("error")
    else:
        error = getattr(snapshot, "error", None)
    if error is None:
        return None
    return str(error)


_GATES_LOCK = threading.Lock()
_GATES: dict[tuple[object, ...], LicenseGate] = {}


def check_license_gate(
    *,
    ssh_config_path: str = "",
    timeout_seconds: int | None = None,
    ttl_seconds: int = LICENSE_FEATURE_POLL_TTL_SECONDS,
    ceiling: int = LICENSE_FEATURE_CEILING,
    source_host: str = LICENSE_POLL_SOURCE_HOST,
    poll_env: str = LICENSE_POLL_ENV,
    poll_command: str = LICENSE_POLL_COMMAND,
    force_refresh: bool = False,
) -> LicenseGateResult:
    effective_timeout = int(timeout_seconds or LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS)
    key = (
        str(ssh_config_path).strip(),
        int(ttl_seconds),
        int(ceiling),
        effective_timeout,
        str(source_host).strip(),
        str(poll_env).strip(),
        str(poll_command).strip(),
    )
    with _GATES_LOCK:
        gate = _GATES.get(key)
        if gate is None:
            gate = LicenseGate(
                ttl_seconds=int(ttl_seconds),
                ceiling=int(ceiling),
                timeout_seconds=effective_timeout,
                source_host=str(source_host).strip(),
                poll_env=str(poll_env).strip(),
                poll_command=str(poll_command).strip(),
            )
            _GATES[key] = gate
    return gate.check(
        ssh_config_path=ssh_config_path,
        timeout_seconds=timeout_seconds,
        force_refresh=force_refresh,
    )
