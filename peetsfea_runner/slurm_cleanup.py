from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence


_SSH_CONNECT_TIMEOUT_SECONDS = 5


class AccountLike(Protocol):
    account_id: str
    host_alias: str


class CommandRunner(Protocol):
    def __call__(self, command: list[str]) -> tuple[int, str, str]:
        ...


def _normalize_host_alias(host_alias: str | None) -> str:
    return (host_alias or "").strip()


def _normalize_job_id(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    text = str(value).strip()
    return text if text.isdigit() else None


def group_active_worker_rows_by_host(
    *,
    active_worker_rows: Sequence[Mapping[str, object]],
) -> dict[str, list[Mapping[str, object]]]:
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in active_worker_rows:
        host_alias = _normalize_host_alias(str(row.get("host_alias", "")))
        if not host_alias:
            continue
        grouped.setdefault(host_alias, []).append(row)
    return grouped


def dedupe_numeric_slurm_job_ids(
    *,
    active_worker_rows: Sequence[Mapping[str, object]],
) -> list[str]:
    unique_job_ids: list[str] = []
    seen: set[str] = set()
    for row in active_worker_rows:
        job_id = _normalize_job_id(row.get("slurm_job_id"))
        if job_id is None or job_id in seen:
            continue
        seen.add(job_id)
        unique_job_ids.append(job_id)
    return unique_job_ids


def _build_ssh_command(
    *,
    host_alias: str,
    ssh_config_path: str = "",
) -> list[str]:
    host_alias = _normalize_host_alias(host_alias)
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={_SSH_CONNECT_TIMEOUT_SECONDS}",
    ]
    ssh_config_path = _normalize_host_alias(ssh_config_path)
    if ssh_config_path:
        command.extend(["-F", ssh_config_path])
    command.append(host_alias)
    return command


def build_scancel_command(
    *,
    host_alias: str,
    slurm_job_ids: Sequence[str],
    ssh_config_path: str = "",
) -> list[str]:
    command = _build_ssh_command(host_alias=host_alias, ssh_config_path=ssh_config_path)
    command.extend(["scancel", *slurm_job_ids])
    return command


def build_squeue_command(
    *,
    host_alias: str,
    ssh_config_path: str = "",
) -> list[str]:
    command = _build_ssh_command(host_alias=host_alias, ssh_config_path=ssh_config_path)
    command.extend(["bash", "-lc", 'squeue -h -u "$USER" -o "%A %j"'])
    return command


def _is_runner_owned_job_name(job_name: str) -> bool:
    return job_name.startswith("peetsfea-worker_") or job_name == "remote_pull_sbatch.sh"


def parse_runner_owned_job_ids(output: str) -> list[str]:
    job_ids: list[str] = []
    seen: set[str] = set()
    for raw in output.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=1)
        if len(parts) != 2:
            continue
        job_id, job_name = parts
        parsed_id = _normalize_job_id(job_id)
        if parsed_id is None or not _is_runner_owned_job_name(job_name):
            continue
        if parsed_id in seen:
            continue
        seen.add(parsed_id)
        job_ids.append(parsed_id)
    return job_ids


@dataclass(slots=True)
class _HostCleanupResult:
    host_alias: str
    attempted: bool
    cancelled: bool
    failed: bool


def _execute_command(
    *,
    command: list[str],
    run_command: CommandRunner,
) -> tuple[bool, int, str, str]:
    try:
        return_code, stdout, stderr = run_command(command)
    except Exception as exc:  # best effort: never surface shutdown command failures
        return False, -1, "", str(exc)
    return return_code == 0, return_code, stdout, stderr


def run_host_cleanup(
    *,
    account: AccountLike,
    active_worker_rows: Sequence[Mapping[str, object]],
    ssh_config_path: str,
    run_command: CommandRunner,
) -> _HostCleanupResult:
    host_alias = _normalize_host_alias(account.host_alias)
    if not host_alias:
        return _HostCleanupResult(host_alias=host_alias, attempted=False, cancelled=False, failed=False)

    attempted = False
    attempted_cancel = False
    cancelled = False
    failed = False

    deduped_job_ids = dedupe_numeric_slurm_job_ids(active_worker_rows=active_worker_rows)
    if deduped_job_ids:
        attempted = True
        scancel_command = build_scancel_command(
            host_alias=host_alias,
            slurm_job_ids=deduped_job_ids,
            ssh_config_path=ssh_config_path,
        )
        scancel_ok, _, _, _ = _execute_command(command=scancel_command, run_command=run_command)
        failed = failed or (not scancel_ok)
        attempted_cancel = attempted_cancel or bool(deduped_job_ids)
        cancelled = cancelled or scancel_ok

    attempted_sweep = False
    sweep_command = build_squeue_command(host_alias=host_alias, ssh_config_path=ssh_config_path)
    sweep_ok, _, stdout, _ = _execute_command(command=sweep_command, run_command=run_command)
    attempted = True
    if sweep_ok:
        sweep_job_ids = parse_runner_owned_job_ids(output=stdout)
        if sweep_job_ids:
            attempted_sweep = True
            attempted = True
            sweep_cancel_command = build_scancel_command(
                host_alias=host_alias,
                slurm_job_ids=sweep_job_ids,
                ssh_config_path=ssh_config_path,
            )
            sweep_cancel_ok, _, _, _ = _execute_command(command=sweep_cancel_command, run_command=run_command)
            failed = failed or (not sweep_cancel_ok)
            attempted_cancel = attempted_cancel or True
            cancelled = cancelled or sweep_cancel_ok
    else:
        failed = True

    if sweep_ok:
        attempted = attempted or attempted_sweep

    return _HostCleanupResult(
        host_alias=host_alias,
        attempted=attempted,
        cancelled=attempted_cancel and not failed and cancelled,
        failed=failed,
    )


def cleanup_slurm_workers(
    *,
    accounts: Sequence[AccountLike],
    ssh_config_path: str,
    active_worker_rows: Sequence[Mapping[str, object]],
    run_command: CommandRunner,
) -> dict[str, list[str] | int]:
    rows_by_host = group_active_worker_rows_by_host(active_worker_rows=active_worker_rows)

    attempted_hosts: list[str] = []
    cancelled_hosts: list[str] = []
    failed_hosts: list[str] = []

    seen_hosts: set[str] = set()
    for account in accounts:
        host_alias = _normalize_host_alias(account.host_alias)
        if not host_alias or host_alias in seen_hosts:
            continue
        seen_hosts.add(host_alias)
        host_rows = rows_by_host.get(host_alias, [])
        result = run_host_cleanup(
            account=account,
            active_worker_rows=host_rows,
            ssh_config_path=ssh_config_path,
            run_command=run_command,
        )
        if not result.attempted:
            continue
        attempted_hosts.append(result.host_alias)
        if result.cancelled:
            cancelled_hosts.append(result.host_alias)
        if result.failed:
            failed_hosts.append(result.host_alias)

    return {
        "attempted_hosts": attempted_hosts,
        "cancelled_hosts": cancelled_hosts,
        "failed_hosts": failed_hosts,
        "attempted_count": len(attempted_hosts),
        "cancelled_count": len(cancelled_hosts),
        "failed_count": len(failed_hosts),
    }
