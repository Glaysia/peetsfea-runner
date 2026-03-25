from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path


LICENSE_POLL_SOURCE_HOST: str = "gate1-harry261"
LICENSE_POLL_INTERVAL_SECONDS: int = 100
LICENSE_ACCOUNT_STATE_TTL_SECONDS: int = 120
LICENSE_CEILING: int = 520
LICENSE_POLL_ENV: str = "ANSYSLMD_LICENSE_FILE=1055@172.16.10.81"
LICENSE_POLL_COMMAND: str = (
    "/opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil lmstat -a"
)

_LEVEL1_PATTERN = re.compile(
    r"Users of elec_solve_level1:\s+\(Total of \d+ licenses issued;\s+Total of (\d+) licenses in use\)",
    re.IGNORECASE,
)
_LEVEL2_PATTERN = re.compile(
    r"Users of elec_solve_level2:\s+\(Total of \d+ licenses issued;\s+Total of (\d+) licenses in use\)",
    re.IGNORECASE,
)


@dataclass(slots=True, frozen=True)
class LicenseUsageSnapshot:
    source_host: str
    level1_in_use: int | None
    level2_in_use: int | None
    effective_in_use: int | None
    ceiling: int
    status: str
    error: str | None = None


@dataclass(slots=True, frozen=True)
class LicenseAccountState:
    run_id: str
    account_id: str
    host: str
    ready: bool
    queued_slots: int
    active_slots: int
    max_active_slots: int
    ts: str


@dataclass(slots=True, frozen=True)
class LicenseTargetPlan:
    target_slots_by_account: dict[str, int]
    dispatchable_account_ids: tuple[str, ...]
    total_active_slots: int
    total_queued_slots: int
    frozen: bool


@dataclass(slots=True, frozen=True)
class _AggregatedAccountState:
    account_id: str
    ready: bool
    queued_slots: int
    active_slots: int
    max_active_slots: int


def parse_license_usage(text: str, *, source_host: str = LICENSE_POLL_SOURCE_HOST) -> LicenseUsageSnapshot:
    level1_match = _LEVEL1_PATTERN.search(text or "")
    level2_match = _LEVEL2_PATTERN.search(text or "")
    if level1_match is None or level2_match is None:
        missing: list[str] = []
        if level1_match is None:
            missing.append("elec_solve_level1")
        if level2_match is None:
            missing.append("elec_solve_level2")
        return LicenseUsageSnapshot(
            source_host=source_host,
            level1_in_use=None,
            level2_in_use=None,
            effective_in_use=None,
            ceiling=LICENSE_CEILING,
            status="FAILED",
            error=f"missing_license_lines={','.join(missing)}",
        )
    level1_in_use = int(level1_match.group(1))
    level2_in_use = int(level2_match.group(1))
    return LicenseUsageSnapshot(
        source_host=source_host,
        level1_in_use=level1_in_use,
        level2_in_use=level2_in_use,
        effective_in_use=max(level1_in_use, level2_in_use),
        ceiling=LICENSE_CEILING,
        status="OK",
        error=None,
    )


def query_license_usage(*, ssh_config_path: str = "", timeout_seconds: int = 30) -> LicenseUsageSnapshot:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5"]
    normalized_ssh_config_path = str(ssh_config_path).strip()
    if normalized_ssh_config_path:
        command.extend(["-F", normalized_ssh_config_path])
    command.extend(
        [
            LICENSE_POLL_SOURCE_HOST,
            f"{LICENSE_POLL_ENV} {LICENSE_POLL_COMMAND}",
        ]
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return LicenseUsageSnapshot(
            source_host=LICENSE_POLL_SOURCE_HOST,
            level1_in_use=None,
            level2_in_use=None,
            effective_in_use=None,
            ceiling=LICENSE_CEILING,
            status="FAILED",
            error=f"timeout={timeout_seconds}s",
        )
    output = "\n".join(part for part in ((completed.stdout or ""), (completed.stderr or "")) if part).strip()
    if completed.returncode != 0:
        details = output or f"return code={completed.returncode}"
        return LicenseUsageSnapshot(
            source_host=LICENSE_POLL_SOURCE_HOST,
            level1_in_use=None,
            level2_in_use=None,
            effective_in_use=None,
            ceiling=LICENSE_CEILING,
            status="FAILED",
            error=details,
        )
    snapshot = parse_license_usage(output, source_host=LICENSE_POLL_SOURCE_HOST)
    if snapshot.status != "OK":
        return snapshot
    return snapshot


def next_desired_total_active_slots(
    *,
    current_desired_total_active_slots: int,
    effective_in_use: int | None,
    dispatchable_account_count: int,
    total_active_slots: int,
    total_queued_slots: int,
    ceiling: int = LICENSE_CEILING,
) -> int:
    desired_total = max(0, current_desired_total_active_slots)
    if effective_in_use is None or effective_in_use >= ceiling:
        return desired_total
    if dispatchable_account_count <= 0 or total_queued_slots <= 0:
        return desired_total
    useful_cap = max(0, total_active_slots + total_queued_slots)
    return min(desired_total + dispatchable_account_count, useful_cap)


def compute_license_target_plan(
    *,
    account_states: list[LicenseAccountState],
    desired_total_active_slots: int,
    effective_in_use: int | None,
    ceiling: int = LICENSE_CEILING,
) -> LicenseTargetPlan:
    aggregated_by_account: dict[str, _AggregatedAccountState] = {}
    for item in account_states:
        existing = aggregated_by_account.get(item.account_id)
        if existing is None:
            aggregated_by_account[item.account_id] = _AggregatedAccountState(
                account_id=item.account_id,
                ready=bool(item.ready),
                queued_slots=max(0, int(item.queued_slots)),
                active_slots=max(0, int(item.active_slots)),
                max_active_slots=max(0, int(item.max_active_slots)),
            )
            continue
        aggregated_by_account[item.account_id] = _AggregatedAccountState(
            account_id=item.account_id,
            ready=existing.ready or bool(item.ready),
            queued_slots=existing.queued_slots + max(0, int(item.queued_slots)),
            active_slots=existing.active_slots + max(0, int(item.active_slots)),
            max_active_slots=max(existing.max_active_slots, max(0, int(item.max_active_slots))),
        )

    current_active_slots_by_account = {
        account_id: item.active_slots for account_id, item in aggregated_by_account.items()
    }
    target_slots_by_account = dict(current_active_slots_by_account)
    dispatchable_states = [
        item
        for item in aggregated_by_account.values()
        if item.ready and item.queued_slots > 0
    ]
    total_active_slots = sum(item.active_slots for item in aggregated_by_account.values())
    queued_by_run: dict[str, int] = {}
    for item in account_states:
        queued_by_run[item.run_id] = max(queued_by_run.get(item.run_id, 0), max(0, int(item.queued_slots)))
    total_queued_slots = sum(queued_by_run.values())
    if (effective_in_use is not None and effective_in_use >= ceiling) or not dispatchable_states:
        return LicenseTargetPlan(
            target_slots_by_account=target_slots_by_account,
            dispatchable_account_ids=tuple(sorted(item.account_id for item in dispatchable_states)),
            total_active_slots=total_active_slots,
            total_queued_slots=total_queued_slots,
            frozen=(effective_in_use is not None and effective_in_use >= ceiling),
        )

    dispatchable_states = sorted(dispatchable_states, key=lambda item: item.account_id)
    base = max(0, desired_total_active_slots) // len(dispatchable_states)
    remainder = max(0, desired_total_active_slots) % len(dispatchable_states)
    for index, item in enumerate(dispatchable_states):
        desired_slots = base + (1 if index < remainder else 0)
        target_slots_by_account[item.account_id] = min(max(0, desired_slots), max(0, item.max_active_slots))
    return LicenseTargetPlan(
        target_slots_by_account=target_slots_by_account,
        dispatchable_account_ids=tuple(item.account_id for item in dispatchable_states),
        total_active_slots=total_active_slots,
        total_queued_slots=total_queued_slots,
        frozen=False,
    )


def normalize_license_account_states(rows: list[dict[str, object]]) -> list[LicenseAccountState]:
    normalized: list[LicenseAccountState] = []
    for row in rows:
        normalized.append(
            LicenseAccountState(
                run_id=str(row.get("run_id") or ""),
                account_id=str(row.get("account_id") or ""),
                host=str(row.get("host") or ""),
                ready=bool(row.get("ready")),
                queued_slots=max(0, int(row.get("queued_slots") or 0)),
                active_slots=max(0, int(row.get("active_slots") or 0)),
                max_active_slots=max(0, int(row.get("max_active_slots") or 0)),
                ts=str(row.get("ts") or ""),
            )
        )
    return normalized
