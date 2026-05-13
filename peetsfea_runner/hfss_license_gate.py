from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Callable

from . import license_policy


HFSS_LICENSE_GATE_TTL_SECONDS: int = 10
HFSS_LICENSE_CEILING: int = 530
HFSS_LICENSE_QUERY_TIMEOUT_SECONDS: int = 30
HFSS_LICENSE_SOURCE_HOST: str = "gate1-harry261"
HFSS_LICENSE_POLL_ENV: str = "ANSYSLMD_LICENSE_FILE=1055@172.16.10.81"
HFSS_LICENSE_POLL_COMMAND: str = "/opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil lmstat -a"

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class HfssLicenseGateResult:
    is_open: bool
    hfss_in_use: int | None
    license_ceiling: int
    status: str
    license_gate: str | None = None
    error: str | None = None
    source: str = "fresh"
    checked_at_monotonic: float = 0.0
    expires_at_monotonic: float = 0.0

    def as_web_status(self) -> dict[str, object]:
        return {
            "is_open": self.is_open,
            "license_gate": self.license_gate,
            "hfss_in_use": self.hfss_in_use,
            "license_ceiling": self.license_ceiling,
            "status": self.status,
            "error": self.error,
            "source": self.source,
        }


class HfssLicenseGate:
    def __init__(
        self,
        *,
        ttl_seconds: int = HFSS_LICENSE_GATE_TTL_SECONDS,
        ceiling: int = HFSS_LICENSE_CEILING,
        timeout_seconds: int = HFSS_LICENSE_QUERY_TIMEOUT_SECONDS,
        source_host: str = HFSS_LICENSE_SOURCE_HOST,
        poll_env: str = HFSS_LICENSE_POLL_ENV,
        poll_command: str = HFSS_LICENSE_POLL_COMMAND,
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
        self._cached_result: HfssLicenseGateResult | None = None
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
    ) -> HfssLicenseGateResult:
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
    ) -> HfssLicenseGateResult:
        try:
            snapshot = self._query(ssh_config_path=ssh_config_path, timeout_seconds=timeout_seconds)
            hfss_in_use = _extract_hfss_in_use(snapshot)
            error = _extract_error(snapshot)
            if _snapshot_failed(snapshot):
                LOGGER.warning("HFSS license gate fail-open: %s", error or "license query failed")
                return self._open_result(
                    now=now,
                    status="FAIL_OPEN",
                    error=error or "license query failed",
                )
            if hfss_in_use is None:
                LOGGER.warning("HFSS license gate fail-open: %s", error or "missing elec_solve_hfss usage")
                return self._open_result(
                    now=now,
                    status="FAIL_OPEN",
                    error=error or "missing elec_solve_hfss usage",
                )
            if hfss_in_use >= self._ceiling:
                return HfssLicenseGateResult(
                    is_open=False,
                    hfss_in_use=hfss_in_use,
                    license_ceiling=self._ceiling,
                    status="CLOSED",
                    license_gate="hfss_closed",
                    checked_at_monotonic=now,
                    expires_at_monotonic=now + self._ttl_seconds,
                )
            return HfssLicenseGateResult(
                is_open=True,
                hfss_in_use=hfss_in_use,
                license_ceiling=self._ceiling,
                status="OPEN",
                checked_at_monotonic=now,
                expires_at_monotonic=now + self._ttl_seconds,
            )
        except Exception as exc:
            LOGGER.warning("HFSS license gate fail-open: %s", exc)
            return self._open_result(now=now, status="FAIL_OPEN", error=str(exc))

    def _query(self, *, ssh_config_path: str, timeout_seconds: int) -> Any:
        if self._query_func is not None:
            return self._query_func(ssh_config_path=ssh_config_path, timeout_seconds=timeout_seconds)

        query_hfss = getattr(license_policy, "query_hfss_license_usage", None)
        if callable(query_hfss):
            return query_hfss(
                ssh_config_path=ssh_config_path,
                timeout_seconds=timeout_seconds,
                source_host=self._source_host,
                poll_env=self._poll_env,
                poll_command=self._poll_command,
                ceiling=self._ceiling,
            )

        return license_policy.query_license_usage(
            ssh_config_path=ssh_config_path,
            timeout_seconds=timeout_seconds,
        )

    def _open_result(self, *, now: float, status: str, error: str | None) -> HfssLicenseGateResult:
        return HfssLicenseGateResult(
            is_open=True,
            hfss_in_use=None,
            license_ceiling=self._ceiling,
            status=status,
            error=error,
            checked_at_monotonic=now,
            expires_at_monotonic=now + self._ttl_seconds,
        )


def _extract_hfss_in_use(snapshot: Any) -> int | None:
    if isinstance(snapshot, int):
        return max(0, snapshot)
    if isinstance(snapshot, dict):
        for key in ("hfss_in_use", "elec_solve_hfss_in_use"):
            value = snapshot.get(key)
            if value is not None:
                return max(0, int(value))
        return None
    for attr in ("hfss_in_use", "elec_solve_hfss_in_use"):
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
_GATES: dict[tuple[object, ...], HfssLicenseGate] = {}


def check_hfss_license_gate(
    *,
    ssh_config_path: str = "",
    timeout_seconds: int | None = None,
    ttl_seconds: int = HFSS_LICENSE_GATE_TTL_SECONDS,
    ceiling: int = HFSS_LICENSE_CEILING,
    source_host: str = HFSS_LICENSE_SOURCE_HOST,
    poll_env: str = HFSS_LICENSE_POLL_ENV,
    poll_command: str = HFSS_LICENSE_POLL_COMMAND,
    force_refresh: bool = False,
) -> HfssLicenseGateResult:
    key = (
        str(ssh_config_path).strip(),
        int(ttl_seconds),
        int(ceiling),
        int(timeout_seconds or HFSS_LICENSE_QUERY_TIMEOUT_SECONDS),
        str(source_host).strip(),
        str(poll_env).strip(),
        str(poll_command).strip(),
    )
    with _GATES_LOCK:
        gate = _GATES.get(key)
        if gate is None:
            gate = HfssLicenseGate(
                ttl_seconds=int(ttl_seconds),
                ceiling=int(ceiling),
                timeout_seconds=int(timeout_seconds or HFSS_LICENSE_QUERY_TIMEOUT_SECONDS),
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
