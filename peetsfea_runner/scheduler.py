from __future__ import annotations

import shlex
import subprocess
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Generic, Protocol, Sequence, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class JobSpec:
    job_id: str
    job_index: int
    input_path: Path
    relative_path: Path
    output_dir: Path
    account_id: str
    host_alias: str


@dataclass(slots=True)
class ScheduledBatchResult(Generic[T]):
    results: list[T]
    max_inflight: int


class AccountConfigLike(Protocol):
    account_id: str
    host_alias: str
    max_jobs: int
    platform: str
    scheduler: str


@dataclass(slots=True, frozen=True)
class SlotTaskRef:
    run_id: str
    slot_id: str
    input_path: Path
    relative_path: Path
    output_dir: Path
    attempt_no: int = 1


@dataclass(slots=True, frozen=True)
class BundleSpec:
    job_id: str
    job_index: int
    account_id: str
    host_alias: str
    slot_inputs: tuple[SlotTaskRef, ...]
    platform: str = "linux"
    scheduler: str = "slurm"

    @property
    def slot_count(self) -> int:
        return len(self.slot_inputs)


@dataclass(slots=True, frozen=True)
class AccountCapacitySnapshot:
    account_id: str
    host_alias: str
    running_count: int
    pending_count: int
    allowed_submit: int


@dataclass(slots=True, frozen=True)
class AccountReadinessSnapshot:
    account_id: str
    host_alias: str
    ready: bool
    status: str
    reason: str
    home_ok: bool
    runtime_path_ok: bool
    venv_ok: bool
    python_ok: bool
    module_ok: bool
    binaries_ok: bool
    ansys_ok: bool
    uv_ok: bool = False
    pyaedt_ok: bool = False


@dataclass(slots=True)
class BalancedBatchResult(Generic[T]):
    results: list[T]
    max_inflight_jobs: int
    submitted_jobs: int
    terminal_jobs: int = 0
    replacement_jobs: int = 0


@dataclass(slots=True, frozen=True)
class SlotWorkerControllerSnapshot:
    queued_slots: int
    pending_slots: int
    inflight_slots: int
    inflight_jobs: int
    submitted_jobs: int


RUNNING_STATES = frozenset({"R", "CG", "RUNNING", "COMPLETING"})
PENDING_STATES = frozenset({"PD", "PENDING"})

_READINESS_MARKER = "__PEETSFEA_READY__:"
_PREFLIGHT_MARKER = "__PEETSFEA_PREFLIGHT__:"
_BOOTSTRAP_MARKER = "__PEETSFEA_BOOTSTRAP__:ok"


def _account_platform(account: AccountConfigLike) -> str:
    return getattr(account, "platform", "linux").strip().lower()


def _account_scheduler(account: AccountConfigLike) -> str:
    default = "slurm" if _account_platform(account) == "linux" else "none"
    return getattr(account, "scheduler", default).strip().lower()


def _windows_ssh_command(account: AccountConfigLike, command: str) -> list[str]:
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=5",
        account.host_alias,
        command,
    ]


def parse_squeue_state_counts(lines: Sequence[str]) -> tuple[int, int, dict[str, int]]:
    state_counts: dict[str, int] = {}
    for raw in lines:
        state = raw.strip().upper()
        if not state:
            continue
        state_counts[state] = state_counts.get(state, 0) + 1
    running_count = sum(count for state, count in state_counts.items() if state in RUNNING_STATES)
    pending_count = sum(count for state, count in state_counts.items() if state in PENDING_STATES)
    return running_count, pending_count, state_counts


def query_account_capacity(
    *,
    account: AccountConfigLike,
    pending_buffer_per_account: int,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None = None,
    ssh_connect_timeout_seconds: int = 5,
    command_timeout_seconds: int = 8,
) -> AccountCapacitySnapshot:
    if (_account_platform(account), _account_scheduler(account)) == ("windows", "none"):
        return AccountCapacitySnapshot(
            account_id=account.account_id,
            host_alias=account.host_alias,
            running_count=0,
            pending_count=0,
            allowed_submit=max(0, account.max_jobs),
        )
    if (_account_platform(account), _account_scheduler(account)) != ("linux", "slurm"):
        raise RuntimeError(
            f"unsupported account provider account={account.account_id} "
            f"platform={_account_platform(account)} scheduler={_account_scheduler(account)}"
        )
    if pending_buffer_per_account < 0:
        raise ValueError("pending_buffer_per_account must be >= 0")
    if ssh_connect_timeout_seconds <= 0:
        raise ValueError("ssh_connect_timeout_seconds must be > 0")
    if command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be > 0")

    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        "squeue -u $USER -h -o \"%T\"",
    ]
    if run_command is None:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"capacity query timed out account={account.account_id} host={account.host_alias} "
                f"timeout={command_timeout_seconds}s"
            ) from exc
        return_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        return_code, stdout, stderr = run_command(command)

    if return_code != 0:
        details = (stderr or stdout).strip()
        if not details:
            details = f"return code={return_code}"
        raise RuntimeError(f"capacity query failed account={account.account_id}: {details}")

    running_count, pending_count, _ = parse_squeue_state_counts(stdout.splitlines())
    allow_running = max(0, account.max_jobs - running_count)
    allow_pending = max(0, pending_buffer_per_account - pending_count)
    allowed_submit = allow_running + allow_pending
    return AccountCapacitySnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        running_count=running_count,
        pending_count=pending_count,
        allowed_submit=allowed_submit,
    )


def _parse_readiness_marker(*, account: AccountConfigLike, text: str) -> AccountReadinessSnapshot:
    marker_line = next((line.strip() for line in text.splitlines() if line.strip().startswith(_READINESS_MARKER)), None)
    if marker_line is None:
        raise RuntimeError(f"readiness check failed account={account.account_id}: missing readiness marker")

    values: dict[str, bool] = {}
    for chunk in marker_line[len(_READINESS_MARKER) :].split():
        if "=" not in chunk:
            continue
        key, raw_value = chunk.split("=", 1)
        values[key.strip()] = raw_value.strip() == "1"

    home_ok = values.get("home", False)
    runtime_path_ok = values.get("runtime", False)
    venv_ok = values.get("venv", False)
    python_ok = values.get("python", False)
    module_ok = values.get("module", False)
    binaries_ok = values.get("binaries", False)
    ansys_ok = values.get("ansys", False)
    bootstrap_needed = not runtime_path_ok or not venv_ok or not python_ok or not binaries_ok
    hard_blocked = not home_ok or not module_ok or not ansys_ok
    failed_checks = [
        check_name
        for check_name, check_ok in (
            ("home", home_ok),
            ("runtime_path", runtime_path_ok),
            ("venv", venv_ok),
            ("python", python_ok),
            ("module", module_ok),
            ("binaries", binaries_ok),
            ("ansys", ansys_ok),
        )
        if not check_ok
    ]
    ready = not failed_checks
    if ready:
        status = "READY"
        reason = "ok"
    elif bootstrap_needed and not hard_blocked:
        status = "BOOTSTRAP_REQUIRED"
        reason = ",".join(failed_checks)
    else:
        status = "DISABLED_FOR_DISPATCH"
        reason = ",".join(failed_checks)
    return AccountReadinessSnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        ready=ready,
        status=status,
        reason=reason,
        home_ok=home_ok,
        runtime_path_ok=runtime_path_ok,
        venv_ok=venv_ok,
        python_ok=python_ok,
        module_ok=module_ok,
        binaries_ok=binaries_ok,
        ansys_ok=ansys_ok,
    )


def _build_windows_readiness_script() -> str:
    return r"""
$ErrorActionPreference = 'Stop'
$HomeOk = 1
$ModuleOk = 1
$MinicondaDir = Join-Path $HOME 'miniconda3'
$VenvDir = Join-Path $HOME '.peetsfea-runner-venv'
$CondaEnvName = 'peetsfea-runner-py312'
$CondaPython = Join-Path $MinicondaDir ('envs\' + $CondaEnvName + '\python.exe')
$RuntimeOk = [int](Test-Path $MinicondaDir)
$VenvOk = [int](Test-Path (Join-Path $VenvDir 'Scripts\python.exe'))
$PythonOk = [int](Test-Path $CondaPython)
$BinariesOk = [int]($RuntimeOk -and $VenvOk -and $PythonOk)
$AnsysOk = 0
if (Get-Command ansysedt.exe -ErrorAction SilentlyContinue) { $AnsysOk = 1 }
elseif (Test-Path 'C:\Program Files\ANSYS Inc') { $AnsysOk = 1 }
Write-Output ('__PEETSFEA_READY__:home={0} runtime={1} venv={2} python={3} module={4} binaries={5} ansys={6}' -f $HomeOk,$RuntimeOk,$VenvOk,$PythonOk,$ModuleOk,$BinariesOk,$AnsysOk)
"""


def _build_windows_preflight_script() -> str:
    return r"""
$ErrorActionPreference = 'Stop'
$HomeOk = 1
$ModuleOk = 1
$MinicondaDir = Join-Path $HOME 'miniconda3'
$VenvDir = Join-Path $HOME '.peetsfea-runner-venv'
$CondaEnvName = 'peetsfea-runner-py312'
$CondaPython = Join-Path $MinicondaDir ('envs\' + $CondaEnvName + '\python.exe')
$RuntimeOk = [int](Test-Path $MinicondaDir)
$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'
$VenvOk = [int](Test-Path $VenvPython)
$PythonOk = [int](Test-Path $CondaPython)
$BinariesOk = [int]($RuntimeOk -and $VenvOk -and $PythonOk)
$AnsysOk = 0
if (Get-Command ansysedt.exe -ErrorAction SilentlyContinue) { $AnsysOk = 1 }
elseif (Test-Path 'C:\Program Files\ANSYS Inc') { $AnsysOk = 1 }
$UvOk = 0
if ($VenvOk) {
  & $VenvPython -m uv --version *> $null
  if ($LASTEXITCODE -eq 0) { $UvOk = 1 }
}
$PyaedtOk = 0
if ($VenvOk) {
  & $VenvPython -c "import ansys.aedt.core" *> $null
  if ($LASTEXITCODE -eq 0) { $PyaedtOk = 1 }
}
Write-Output ('__PEETSFEA_PREFLIGHT__:home={0} runtime={1} venv={2} python={3} module={4} binaries={5} ansys={6} uv={7} pyaedt={8}' -f $HomeOk,$RuntimeOk,$VenvOk,$PythonOk,$ModuleOk,$BinariesOk,$AnsysOk,$UvOk,$PyaedtOk)
"""


def _run_windows_ssh_script(
    *,
    account: AccountConfigLike,
    script: str,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None,
    ssh_connect_timeout_seconds: int,
    command_timeout_seconds: int,
    stage: str,
) -> tuple[int, str, str]:
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        f"powershell -NoProfile -NonInteractive -Command {shlex.quote(script)}",
    ]
    if run_command is not None:
        return run_command(command)
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=command_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{stage} timed out account={account.account_id} host={account.host_alias} "
            f"timeout={command_timeout_seconds}s"
        ) from exc
    return completed.returncode, completed.stdout or "", completed.stderr or ""


def _query_windows_account_readiness(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None,
    ssh_connect_timeout_seconds: int,
    command_timeout_seconds: int,
) -> AccountReadinessSnapshot:
    return_code, stdout, stderr = _run_windows_ssh_script(
        account=account,
        script=_build_windows_readiness_script(),
        run_command=run_command,
        ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        stage="windows readiness check",
    )
    combined_output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if combined_output:
        snapshot = _parse_readiness_marker(account=account, text=combined_output)
        if return_code != 0 and snapshot.ready:
            raise RuntimeError(
                f"readiness check failed account={account.account_id}: "
                f"{(stderr or stdout).strip() or f'return code={return_code}'}"
            )
        return snapshot
    details = (stderr or stdout).strip() or f"return code={return_code}"
    raise RuntimeError(f"readiness check failed account={account.account_id}: {details}")


def _query_windows_account_preflight(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None,
    ssh_connect_timeout_seconds: int,
    command_timeout_seconds: int,
) -> AccountReadinessSnapshot:
    return_code, stdout, stderr = _run_windows_ssh_script(
        account=account,
        script=_build_windows_preflight_script(),
        run_command=run_command,
        ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        stage="windows preflight check",
    )
    combined_output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if combined_output:
        snapshot = _parse_preflight_marker(account=account, text=combined_output)
        if return_code != 0 and snapshot.ready:
            raise RuntimeError(
                f"preflight check failed account={account.account_id}: "
                f"{(stderr or stdout).strip() or f'return code={return_code}'}"
            )
        return snapshot
    details = (stderr or stdout).strip() or f"return code={return_code}"
    raise RuntimeError(f"preflight check failed account={account.account_id}: {details}")


def query_account_readiness(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None = None,
    ssh_connect_timeout_seconds: int = 5,
    command_timeout_seconds: int = 12,
) -> AccountReadinessSnapshot:
    if (_account_platform(account), _account_scheduler(account)) == ("windows", "none"):
        return _query_windows_account_readiness(
            account=account,
            run_command=run_command,
            ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
            command_timeout_seconds=command_timeout_seconds,
        )
    if (_account_platform(account), _account_scheduler(account)) != ("linux", "slurm"):
        raise RuntimeError(
            f"unsupported account provider account={account.account_id} "
            f"platform={_account_platform(account)} scheduler={_account_scheduler(account)}"
        )
    if ssh_connect_timeout_seconds <= 0:
        raise ValueError("ssh_connect_timeout_seconds must be > 0")
    if command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be > 0")

    remote_script = """
set +e
HOME_OK=0
RUNTIME_OK=0
VENV_OK=0
PYTHON_OK=0
MODULE_OK=0
BINARIES_OK=0
ANSYS_OK=0
if [ -n "${HOME:-}" ] && [ -d "$HOME" ] && [ -w "$HOME" ]; then
  HOME_OK=1
  RUNTIME_OK=1
fi
if [ -d "$HOME/.peetsfea-runner-venv" ]; then
  VENV_OK=1
fi
if [ -x "$HOME/.peetsfea-runner-venv/bin/python" ]; then
  PYTHON_OK=1
fi
if command -v bash >/dev/null 2>&1 && command -v tar >/dev/null 2>&1 && command -v base64 >/dev/null 2>&1; then
  BINARIES_OK=1
fi
if [ -r /etc/profile.d/modules.sh ]; then
  . /etc/profile.d/modules.sh >/dev/null 2>&1 || true
fi
if command -v module >/dev/null 2>&1; then
  module load ansys-electronics/v252 >/dev/null 2>&1 && MODULE_OK=1
fi
if command -v ansysedt >/dev/null 2>&1 || command -v ansysedtsv >/dev/null 2>&1 || command -v electronicsdesktop >/dev/null 2>&1; then
  ANSYS_OK=1
fi
printf '__PEETSFEA_READY__:home=%s runtime=%s venv=%s python=%s module=%s binaries=%s ansys=%s\\n' "$HOME_OK" "$RUNTIME_OK" "$VENV_OK" "$PYTHON_OK" "$MODULE_OK" "$BINARIES_OK" "$ANSYS_OK"
"""
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        f"bash -lc {shlex.quote(remote_script)}",
    ]
    if run_command is None:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"readiness check timed out account={account.account_id} host={account.host_alias} "
                f"timeout={command_timeout_seconds}s"
            ) from exc
        return_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        return_code, stdout, stderr = run_command(command)

    combined_output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if combined_output:
        snapshot = _parse_readiness_marker(account=account, text=combined_output)
        if return_code != 0 and snapshot.ready:
            raise RuntimeError(
                f"readiness check failed account={account.account_id}: "
                f"{(stderr or stdout).strip() or f'return code={return_code}'}"
            )
        return snapshot

    details = (stderr or stdout).strip()
    if not details:
        details = f"return code={return_code}"
    raise RuntimeError(f"readiness check failed account={account.account_id}: {details}")


def _parse_preflight_marker(*, account: AccountConfigLike, text: str) -> AccountReadinessSnapshot:
    marker_line = next((line.strip() for line in text.splitlines() if line.strip().startswith(_PREFLIGHT_MARKER)), None)
    if marker_line is None:
        raise RuntimeError(f"preflight check failed account={account.account_id}: missing preflight marker")

    values: dict[str, bool] = {}
    for chunk in marker_line[len(_PREFLIGHT_MARKER) :].split():
        if "=" not in chunk:
            continue
        key, raw_value = chunk.split("=", 1)
        values[key.strip()] = raw_value.strip() == "1"

    home_ok = values.get("home", False)
    runtime_path_ok = values.get("runtime", False)
    venv_ok = values.get("venv", False)
    python_ok = values.get("python", False)
    module_ok = values.get("module", False)
    binaries_ok = values.get("binaries", False)
    ansys_ok = values.get("ansys", False)
    uv_ok = values.get("uv", False)
    pyaedt_ok = values.get("pyaedt", False)
    failed_checks = [
        check_name
        for check_name, check_ok in (
            ("home", home_ok),
            ("runtime_path", runtime_path_ok),
            ("venv", venv_ok),
            ("python", python_ok),
            ("module", module_ok),
            ("binaries", binaries_ok),
            ("ansys", ansys_ok),
            ("uv", uv_ok),
            ("pyaedt", pyaedt_ok),
        )
        if not check_ok
    ]
    ready = not failed_checks
    return AccountReadinessSnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        ready=ready,
        status="READY" if ready else "PREFLIGHT_FAILED",
        reason="ok" if ready else ",".join(failed_checks),
        home_ok=home_ok,
        runtime_path_ok=runtime_path_ok,
        venv_ok=venv_ok,
        python_ok=python_ok,
        module_ok=module_ok,
        binaries_ok=binaries_ok,
        ansys_ok=ansys_ok,
        uv_ok=uv_ok,
        pyaedt_ok=pyaedt_ok,
    )


def query_account_preflight(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None = None,
    ssh_connect_timeout_seconds: int = 5,
    command_timeout_seconds: int = 20,
) -> AccountReadinessSnapshot:
    if (_account_platform(account), _account_scheduler(account)) == ("windows", "none"):
        return _query_windows_account_preflight(
            account=account,
            run_command=run_command,
            ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
            command_timeout_seconds=command_timeout_seconds,
        )
    if (_account_platform(account), _account_scheduler(account)) != ("linux", "slurm"):
        raise RuntimeError(
            f"unsupported account provider account={account.account_id} "
            f"platform={_account_platform(account)} scheduler={_account_scheduler(account)}"
        )
    if ssh_connect_timeout_seconds <= 0:
        raise ValueError("ssh_connect_timeout_seconds must be > 0")
    if command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be > 0")

    remote_script = """
set +e
HOME_OK=0
RUNTIME_OK=0
VENV_OK=0
PYTHON_OK=0
MODULE_OK=0
BINARIES_OK=0
ANSYS_OK=0
UV_OK=0
PYAEDT_OK=0
VENV_DIR="$HOME/.peetsfea-runner-venv"
if [ -n "${HOME:-}" ] && [ -d "$HOME" ] && [ -w "$HOME" ]; then
  HOME_OK=1
  RUNTIME_OK=1
fi
if [ -d "$VENV_DIR" ]; then
  VENV_OK=1
fi
if [ -x "$VENV_DIR/bin/python" ]; then
  PYTHON_OK=1
fi
if command -v bash >/dev/null 2>&1 && command -v tar >/dev/null 2>&1 && command -v base64 >/dev/null 2>&1; then
  BINARIES_OK=1
fi
if [ -r /etc/profile.d/modules.sh ]; then
  . /etc/profile.d/modules.sh >/dev/null 2>&1 || true
fi
if command -v module >/dev/null 2>&1; then
  module load ansys-electronics/v252 >/dev/null 2>&1 && MODULE_OK=1
fi
if command -v ansysedt >/dev/null 2>&1 || command -v ansysedtsv >/dev/null 2>&1 || [ -x /opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt ]; then
  ANSYS_OK=1
fi
if [ "$PYTHON_OK" -eq 1 ] && "$VENV_DIR/bin/python" -m uv --version >/dev/null 2>&1; then
  UV_OK=1
fi
if [ "$PYTHON_OK" -eq 1 ] && "$VENV_DIR/bin/python" -c "import ansys.aedt.core" >/dev/null 2>&1; then
  PYAEDT_OK=1
fi
printf '__PEETSFEA_PREFLIGHT__:home=%s runtime=%s venv=%s python=%s module=%s binaries=%s ansys=%s uv=%s pyaedt=%s\\n' "$HOME_OK" "$RUNTIME_OK" "$VENV_OK" "$PYTHON_OK" "$MODULE_OK" "$BINARIES_OK" "$ANSYS_OK" "$UV_OK" "$PYAEDT_OK"
"""
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        f"bash -lc {shlex.quote(remote_script)}",
    ]
    if run_command is None:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"preflight check timed out account={account.account_id} host={account.host_alias} "
                f"timeout={command_timeout_seconds}s"
            ) from exc
        return_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        return_code, stdout, stderr = run_command(command)

    combined_output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if combined_output:
        snapshot = _parse_preflight_marker(account=account, text=combined_output)
        if return_code != 0 and snapshot.ready:
            raise RuntimeError(
                f"preflight check failed account={account.account_id}: "
                f"{(stderr or stdout).strip() or f'return code={return_code}'}"
            )
        return snapshot

    details = (stderr or stdout).strip()
    if not details:
        details = f"return code={return_code}"
    raise RuntimeError(f"preflight check failed account={account.account_id}: {details}")


def bootstrap_account_runtime(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None = None,
    ssh_connect_timeout_seconds: int = 5,
    command_timeout_seconds: int = 900,
) -> str:
    if (_account_platform(account), _account_scheduler(account)) == ("windows", "none"):
        return _bootstrap_windows_account_runtime(
            account=account,
            run_command=run_command,
            ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
            command_timeout_seconds=command_timeout_seconds,
        )
    if (_account_platform(account), _account_scheduler(account)) != ("linux", "slurm"):
        raise RuntimeError(
            f"unsupported account provider account={account.account_id} "
            f"platform={_account_platform(account)} scheduler={_account_scheduler(account)}"
        )
    if ssh_connect_timeout_seconds <= 0:
        raise ValueError("ssh_connect_timeout_seconds must be > 0")
    if command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be > 0")

    remote_script = """
set -euo pipefail
VENV_DIR="$HOME/.peetsfea-runner-venv"
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="peetsfea-runner-py312"
CONDA_PYTHON_PATH="$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python"
DEPS_READY_MARKER="$VENV_DIR/.peets_deps_ready"
if [ -r /etc/profile.d/modules.sh ]; then
  . /etc/profile.d/modules.sh >/dev/null 2>&1 || true
fi
download_miniconda_installer() {
  installer_path="$1"
  url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$installer_path"
    return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "$installer_path" "$url"
    return 0
  fi
  echo "[ERROR] curl or wget is required to install Miniconda3" >&2
  exit 1
}
ensure_miniconda() {
  if [ -x "$MINICONDA_DIR/bin/conda" ]; then
    return 0
  fi
  if [ -e "$MINICONDA_DIR" ]; then
    rm -rf "$MINICONDA_DIR"
  fi
  installer_path=$(mktemp /tmp/miniconda_installer.XXXXXX.sh)
  download_miniconda_installer "$installer_path"
  bash "$installer_path" -b -p "$MINICONDA_DIR"
  rm -f "$installer_path"
}
ensure_conda_python312() {
  if "$MINICONDA_DIR/bin/conda" tos --help >/dev/null 2>&1; then
    "$MINICONDA_DIR/bin/conda" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true
    "$MINICONDA_DIR/bin/conda" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r >/dev/null 2>&1 || true
  fi
  if [ -x "$CONDA_PYTHON_PATH" ]; then
    major_minor="$($CONDA_PYTHON_PATH -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    if [ "$major_minor" = "3.12" ]; then
      return 0
    fi
  fi
  "$MINICONDA_DIR/bin/conda" create -y -n "$CONDA_ENV_NAME" python=3.12
}
ensure_runner_venv() {
  recreate=0
  if [ -d "$VENV_DIR" ]; then
    if [ ! -x "$VENV_DIR/bin/python" ]; then
      recreate=1
    else
      major_minor="$($VENV_DIR/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
      if [ "$major_minor" != "3.12" ]; then
        recreate=1
      fi
    fi
  else
    recreate=1
  fi
  if [ "$recreate" -eq 1 ]; then
    rm -rf "$VENV_DIR"
    "$CONDA_PYTHON_PATH" -m venv "$VENV_DIR"
  fi
}
ensure_miniconda
ensure_conda_python312
ensure_runner_venv
"$VENV_DIR/bin/python" -m ensurepip --upgrade || true
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/python" -m pip install uv
"$VENV_DIR/bin/python" -m uv pip install pyaedt==0.25.1
touch "$DEPS_READY_MARKER"
printf '__PEETSFEA_BOOTSTRAP__:ok\\n'
"""
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        f"bash -lc {shlex.quote(remote_script)}",
    ]
    if run_command is None:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"bootstrap timed out account={account.account_id} host={account.host_alias} "
                f"timeout={command_timeout_seconds}s"
            ) from exc
        return_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        return_code, stdout, stderr = run_command(command)

    output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if return_code != 0 or _BOOTSTRAP_MARKER not in output:
        details = output or f"return code={return_code}"
        raise RuntimeError(f"bootstrap failed account={account.account_id}: {details}")
    return output


def _bootstrap_windows_account_runtime(
    *,
    account: AccountConfigLike,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None,
    ssh_connect_timeout_seconds: int,
    command_timeout_seconds: int,
) -> str:
    remote_script = r"""
$ErrorActionPreference = 'Stop'
$VenvDir = Join-Path $HOME '.peetsfea-runner-venv'
$MinicondaDir = Join-Path $HOME 'miniconda3'
$CondaEnvName = 'peetsfea-runner-py312'
$CondaPythonPath = Join-Path $MinicondaDir ('envs\' + $CondaEnvName + '\python.exe')
$CondaExe = Join-Path $MinicondaDir 'Scripts\conda.exe'
$InstallerPath = Join-Path $env:TEMP 'Miniconda3-latest-Windows-x86_64.exe'
function Ensure-Miniconda {
  if (Test-Path $CondaExe) { return }
  if (Test-Path $MinicondaDir) { Remove-Item -Recurse -Force $MinicondaDir }
  Invoke-WebRequest -UseBasicParsing -Uri 'https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe' -OutFile $InstallerPath
  Start-Process -FilePath $InstallerPath -ArgumentList '/InstallationType=JustMe','/RegisterPython=0','/S',('/D=' + $MinicondaDir) -Wait
  Remove-Item -Force $InstallerPath
}
function Ensure-CondaPython312 {
  & $CondaExe tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main *> $null
  & $CondaExe tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r *> $null
  if (Test-Path $CondaPythonPath) {
    $ver = & $CondaPythonPath -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if ($ver -eq '3.12') { return }
  }
  & $CondaExe create -y -n $CondaEnvName python=3.12
}
function Ensure-RunnerVenv {
  $VenvPython = Join-Path $VenvDir 'Scripts\python.exe'
  $recreate = $true
  if (Test-Path $VenvPython) {
    $ver = & $VenvPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if ($ver -eq '3.12') { $recreate = $false }
  }
  if ($recreate) {
    if (Test-Path $VenvDir) { Remove-Item -Recurse -Force $VenvDir }
    & $CondaPythonPath -m venv $VenvDir
  }
}
Ensure-Miniconda
Ensure-CondaPython312
Ensure-RunnerVenv
$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'
& $VenvPython -m pip install --upgrade pip
& $VenvPython -m pip install uv
& $VenvPython -m uv pip install pyaedt==0.25.1
Write-Output '__PEETSFEA_BOOTSTRAP__:ok'
"""
    return_code, stdout, stderr = _run_windows_ssh_script(
        account=account,
        script=remote_script,
        run_command=run_command,
        ssh_connect_timeout_seconds=ssh_connect_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        stage="windows bootstrap",
    )
    output = "\n".join(part for part in (stdout, stderr) if part).strip()
    if return_code != 0 or _BOOTSTRAP_MARKER not in output:
        details = output or f"return code={return_code}"
        raise RuntimeError(f"bootstrap failed account={account.account_id}: {details}")
    return output


def pick_balanced_account(
    *,
    capacities: Sequence[AccountCapacitySnapshot],
    completed_slots_by_account: dict[str, int],
    inflight_slots_by_account: dict[str, int],
) -> AccountCapacitySnapshot | None:
    eligible = [item for item in capacities if item.allowed_submit > 0]
    if not eligible:
        return None

    def _sort_key(item: AccountCapacitySnapshot) -> tuple[int, int, str]:
        score = completed_slots_by_account.get(item.account_id, 0) + inflight_slots_by_account.get(
            item.account_id, 0
        )
        return score, item.running_count, item.account_id

    return min(eligible, key=_sort_key)


def run_slot_bundles(
    *,
    slot_queue: list[SlotTaskRef],
    accounts: list[AccountConfigLike],
    slots_per_job: int,
    pending_buffer_per_account: int,
    worker: Callable[[BundleSpec], T],
    capacity_lookup: Callable[..., AccountCapacitySnapshot] = query_account_capacity,
    initial_completed_slots: dict[str, int] | None = None,
    job_index_start: int = 1,
    max_workers: int | None = None,
    idle_sleep_seconds: float = 1.0,
    on_capacity_snapshot: Callable[[AccountCapacitySnapshot], None] | None = None,
    on_bundle_submitted: Callable[[BundleSpec], None] | None = None,
    on_capacity_error: Callable[[AccountConfigLike, Exception], None] | None = None,
) -> BalancedBatchResult[T]:
    if slots_per_job <= 0:
        raise ValueError("slots_per_job must be > 0")
    if pending_buffer_per_account < 0:
        raise ValueError("pending_buffer_per_account must be >= 0")
    if idle_sleep_seconds <= 0:
        raise ValueError("idle_sleep_seconds must be > 0")
    if not accounts:
        raise ValueError("accounts must not be empty")
    if job_index_start <= 0:
        raise ValueError("job_index_start must be > 0")

    queue = deque(slot_queue)
    completed_slots: dict[str, int] = dict(initial_completed_slots or {})
    inflight_slots: dict[str, int] = {}
    results: list[T] = []
    max_inflight_jobs = 0
    submitted_jobs = 0
    job_counter = job_index_start - 1

    if max_workers is None:
        max_workers = max(1, sum(max(1, account.max_jobs) for account in accounts))
    else:
        max_workers = max(1, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        inflight_futures: dict[Future[T], tuple[str, int]] = {}

        while queue or inflight_futures:
            done_futures = [future for future in tuple(inflight_futures) if future.done()]
            for future in done_futures:
                account_id, slot_count = inflight_futures.pop(future)
                try:
                    results.append(future.result())
                finally:
                    inflight_slots[account_id] = max(0, inflight_slots.get(account_id, 0) - slot_count)
                    completed_slots[account_id] = completed_slots.get(account_id, 0) + slot_count

            if not queue:
                if inflight_futures:
                    wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                continue

            if len(inflight_futures) >= max_workers:
                wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                continue

            snapshots: list[AccountCapacitySnapshot] = []
            for account in accounts:
                try:
                    snapshot = capacity_lookup(account=account, pending_buffer_per_account=pending_buffer_per_account)
                except Exception as exc:
                    if on_capacity_error is not None:
                        on_capacity_error(account, exc)
                    continue
                snapshots.append(snapshot)
                if on_capacity_snapshot is not None:
                    on_capacity_snapshot(snapshot)

            selected = pick_balanced_account(
                capacities=snapshots,
                completed_slots_by_account=completed_slots,
                inflight_slots_by_account=inflight_slots,
            )
            if selected is None:
                if inflight_futures:
                    wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                    continue
                raise RuntimeError("No eligible account available for bundle submission.")

            bundle_inputs: list[SlotTaskRef] = []
            while queue and len(bundle_inputs) < slots_per_job:
                bundle_inputs.append(queue.popleft())
            if not bundle_inputs:
                continue

            job_counter += 1
            bundle = BundleSpec(
                job_id=f"job_{job_counter:04d}",
                job_index=job_counter,
                account_id=selected.account_id,
                host_alias=selected.host_alias,
                slot_inputs=tuple(bundle_inputs),
                platform=_account_platform(selected),
                scheduler=_account_scheduler(selected),
            )
            future = executor.submit(worker, bundle)
            inflight_futures[future] = (selected.account_id, len(bundle_inputs))
            inflight_slots[selected.account_id] = inflight_slots.get(selected.account_id, 0) + len(bundle_inputs)
            submitted_jobs += 1
            max_inflight_jobs = max(max_inflight_jobs, len(inflight_futures))
            if on_bundle_submitted is not None:
                on_bundle_submitted(bundle)

            # Keep loop responsive for freshly completed futures.
            if inflight_futures:
                wait(tuple(inflight_futures), timeout=0)

    return BalancedBatchResult(
        results=results,
        max_inflight_jobs=max_inflight_jobs,
        submitted_jobs=submitted_jobs,
    )


class SlotWorkerController(Generic[T]):
    def __init__(
        self,
        *,
        accounts: list[AccountConfigLike],
        slots_per_job: int,
        pending_buffer_per_account: int,
        worker: Callable[[BundleSpec], T],
        capacity_lookup: Callable[..., AccountCapacitySnapshot] = query_account_capacity,
        initial_completed_slots: dict[str, int] | None = None,
        job_index_start: int = 1,
        max_workers: int | None = None,
        idle_sleep_seconds: float = 1.0,
        on_capacity_snapshot: Callable[[AccountCapacitySnapshot], None] | None = None,
        on_bundle_submitted: Callable[[BundleSpec], None] | None = None,
        on_capacity_error: Callable[[AccountConfigLike, Exception], None] | None = None,
        recovery_slots_lookup: Callable[[BundleSpec, T], Sequence[SlotTaskRef]] | None = None,
        terminal_bundle_lookup: Callable[[BundleSpec, T], bool] | None = None,
    ) -> None:
        if slots_per_job <= 0:
            raise ValueError("slots_per_job must be > 0")
        if pending_buffer_per_account < 0:
            raise ValueError("pending_buffer_per_account must be >= 0")
        if idle_sleep_seconds <= 0:
            raise ValueError("idle_sleep_seconds must be > 0")
        if not accounts:
            raise ValueError("accounts must not be empty")
        if job_index_start <= 0:
            raise ValueError("job_index_start must be > 0")

        self._accounts = list(accounts)
        self._slots_per_job = slots_per_job
        self._pending_buffer_per_account = pending_buffer_per_account
        self._worker = worker
        self._capacity_lookup = capacity_lookup
        self._completed_slots = dict(initial_completed_slots or {})
        self._idle_sleep_seconds = idle_sleep_seconds
        self._on_capacity_snapshot = on_capacity_snapshot
        self._on_bundle_submitted = on_bundle_submitted
        self._on_capacity_error = on_capacity_error
        self._recovery_slots_lookup = recovery_slots_lookup
        self._terminal_bundle_lookup = terminal_bundle_lookup

        self._source_queue: deque[SlotTaskRef] = deque()
        self._pending_bundles_by_account: dict[str, deque[list[SlotTaskRef]]] = {}
        self._pending_seed_bundles: list[list[SlotTaskRef]] = []
        self._inflight_slots: dict[str, int] = {}
        self._inflight_workers: dict[str, int] = {}
        self._target_workers_by_account: dict[str, int] = {}
        self._seeded_workers_by_account: dict[str, int] = {}
        self._results: list[T] = []
        self._max_inflight_jobs = 0
        self._submitted_jobs = 0
        self._terminal_jobs = 0
        self._replacement_jobs = 0
        self._job_counter = job_index_start - 1
        self._seed_account_cursor = 0
        self._backlog_account_cursor = 0

        if max_workers is None:
            max_workers = max(1, sum(max(1, account.max_jobs) for account in self._accounts))
        else:
            max_workers = max(1, max_workers)
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._future_to_bundle: dict[Future[T], BundleSpec] = {}

    def enqueue_slots(self, slots: Sequence[SlotTaskRef]) -> None:
        if not slots:
            return
        self._source_queue.extend(slots)
        self._materialize_pending_bundles()

    def has_work(self) -> bool:
        return bool(self._source_queue) or any(self._pending_bundles_by_account.values()) or bool(self._future_to_bundle)

    def snapshot(self) -> SlotWorkerControllerSnapshot:
        pending_slots = sum(
            len(bundle)
            for queue in self._pending_bundles_by_account.values()
            for bundle in queue
        )
        inflight_slots = sum(self._inflight_slots.values())
        return SlotWorkerControllerSnapshot(
            queued_slots=len(self._source_queue),
            pending_slots=pending_slots,
            inflight_slots=inflight_slots,
            inflight_jobs=len(self._future_to_bundle),
            submitted_jobs=self._submitted_jobs,
        )

    def step(self, *, wait_for_progress: bool = False) -> bool:
        progressed = self._collect_done_futures()
        self._materialize_pending_bundles()

        snapshots: list[AccountCapacitySnapshot] = []
        for account in self._accounts:
            try:
                snapshot = self._capacity_lookup(
                    account=account,
                    pending_buffer_per_account=self._pending_buffer_per_account,
                )
            except Exception as exc:
                if self._on_capacity_error is not None:
                    self._on_capacity_error(account, exc)
                continue
            snapshots.append(snapshot)
            if self._on_capacity_snapshot is not None:
                self._on_capacity_snapshot(snapshot)

        if any(queue for queue in self._pending_bundles_by_account.values()) and not snapshots and not self._future_to_bundle:
            raise RuntimeError("No account capacity snapshots available for worker startup.")

        if self._submit_pending_bundles(snapshots):
            progressed = True

        if progressed:
            return True

        if wait_for_progress and self._future_to_bundle:
            wait(tuple(self._future_to_bundle), return_when=FIRST_COMPLETED, timeout=self._idle_sleep_seconds)
            return self._collect_done_futures()

        if wait_for_progress:
            time.sleep(self._idle_sleep_seconds)
        return False

    def finalize(self) -> BalancedBatchResult[T]:
        try:
            while self.has_work():
                self.step(wait_for_progress=True)
        finally:
            self._executor.shutdown(wait=True)
        return BalancedBatchResult(
            results=list(self._results),
            max_inflight_jobs=self._max_inflight_jobs,
            submitted_jobs=self._submitted_jobs,
            terminal_jobs=self._terminal_jobs,
            replacement_jobs=self._replacement_jobs,
        )

    def _collect_done_futures(self) -> bool:
        progressed = False
        done_futures = [future for future in tuple(self._future_to_bundle) if future.done()]
        for future in done_futures:
            progressed = True
            bundle = self._future_to_bundle.pop(future)
            result = future.result()
            self._results.append(result)
            self._inflight_slots[bundle.account_id] = max(
                0,
                self._inflight_slots.get(bundle.account_id, 0) - bundle.slot_count,
            )
            self._inflight_workers[bundle.account_id] = max(
                0,
                self._inflight_workers.get(bundle.account_id, 0) - 1,
            )
            self._completed_slots[bundle.account_id] = self._completed_slots.get(bundle.account_id, 0) + bundle.slot_count
            if self._terminal_bundle_lookup is not None and self._terminal_bundle_lookup(bundle, result):
                self._terminal_jobs += 1
            if self._recovery_slots_lookup is not None:
                recovery_slots = list(self._recovery_slots_lookup(bundle, result))
                if recovery_slots:
                    account = next((item for item in self._accounts if item.account_id == bundle.account_id), None)
                    if account is not None:
                        self._replacement_jobs += self._queue_bundles_for_account(account=account, slots=recovery_slots)
        return progressed

    def _submit_pending_bundles(self, snapshots: Sequence[AccountCapacitySnapshot]) -> bool:
        submitted_any = False
        snapshot_by_account = {snapshot.account_id: snapshot for snapshot in snapshots}
        for account in self._accounts:
            queue = self._pending_bundles_by_account.get(account.account_id)
            if not queue:
                continue
            snapshot = snapshot_by_account.get(account.account_id)
            if snapshot is None:
                continue
            target_workers = self._target_workers_by_account.get(account.account_id, 0)
            if target_workers <= 0:
                continue
            external_occupied = max(
                0,
                max(0, snapshot.running_count) + max(0, snapshot.pending_count) - self._inflight_workers.get(account.account_id, 0),
            )
            target_available_workers = max(
                0,
                target_workers - external_occupied - self._inflight_workers.get(account.account_id, 0),
            )
            allowed_submit_workers = max(
                0,
                snapshot.allowed_submit - self._inflight_workers.get(account.account_id, 0),
            )
            available_workers = min(target_available_workers, allowed_submit_workers)
            while queue and available_workers > 0 and len(self._future_to_bundle) < self._max_workers:
                bundle_slots = queue.popleft()
                if bundle_slots in self._pending_seed_bundles:
                    self._pending_seed_bundles.remove(bundle_slots)
                self._job_counter += 1
                bundle = BundleSpec(
                    job_id=f"job_{self._job_counter:04d}",
                    job_index=self._job_counter,
                    account_id=account.account_id,
                    host_alias=account.host_alias,
                    slot_inputs=tuple(bundle_slots),
                    platform=_account_platform(account),
                    scheduler=_account_scheduler(account),
                )
                future = self._executor.submit(self._worker, bundle)
                self._future_to_bundle[future] = bundle
                self._inflight_slots[bundle.account_id] = self._inflight_slots.get(bundle.account_id, 0) + bundle.slot_count
                self._inflight_workers[bundle.account_id] = self._inflight_workers.get(bundle.account_id, 0) + 1
                self._submitted_jobs += 1
                self._max_inflight_jobs = max(self._max_inflight_jobs, len(self._future_to_bundle))
                available_workers -= 1
                submitted_any = True
                if self._on_bundle_submitted is not None:
                    self._on_bundle_submitted(bundle)
        return submitted_any

    def _next_seed_account(self) -> AccountConfigLike | None:
        if not self._accounts:
            return None
        account_count = len(self._accounts)
        for offset in range(account_count):
            index = (self._seed_account_cursor + offset) % account_count
            account = self._accounts[index]
            seeded = self._seeded_workers_by_account.get(account.account_id, 0)
            if seeded >= account.max_jobs:
                continue
            self._seed_account_cursor = (index + 1) % account_count
            return account
        return None

    def _queue_bundle(self, *, account: AccountConfigLike, assigned: Sequence[SlotTaskRef]) -> list[SlotTaskRef] | None:
        if not assigned:
            return None
        bundle_slots = list(assigned)
        self._pending_bundles_by_account.setdefault(account.account_id, deque()).append(bundle_slots)
        return bundle_slots

    def _materialize_pending_bundles(self) -> None:
        while self._source_queue:
            account = self._next_seed_account()
            if account is None:
                break
            if not self._source_queue:
                break
            assigned = [self._source_queue.popleft()]
            self._seeded_workers_by_account[account.account_id] = self._seeded_workers_by_account.get(account.account_id, 0) + 1
            self._target_workers_by_account[account.account_id] = self._target_workers_by_account.get(account.account_id, 0) + 1
            bundle_slots = self._queue_bundle(account=account, assigned=assigned)
            if bundle_slots is not None:
                self._pending_seed_bundles.append(bundle_slots)

        while self._source_queue and self._pending_seed_bundles:
            progressed = False
            for bundle_slots in list(self._pending_seed_bundles):
                if not self._source_queue:
                    break
                if len(bundle_slots) >= self._slots_per_job:
                    continue
                bundle_slots.append(self._source_queue.popleft())
                progressed = True
            self._pending_seed_bundles = [
                bundle_slots for bundle_slots in self._pending_seed_bundles if len(bundle_slots) < self._slots_per_job
            ]
            if not progressed:
                break

        while self._source_queue:
            account = self._accounts[self._backlog_account_cursor % len(self._accounts)]
            self._backlog_account_cursor += 1
            assigned: list[SlotTaskRef] = []
            while self._source_queue and len(assigned) < self._slots_per_job:
                assigned.append(self._source_queue.popleft())
            if not assigned:
                break
            self._queue_bundle(account=account, assigned=assigned)

    def _queue_bundles_for_account(self, *, account: AccountConfigLike, slots: Sequence[SlotTaskRef]) -> int:
        queued_jobs = 0
        recovery_queue = deque(slots)
        while recovery_queue:
            assigned: list[SlotTaskRef] = []
            while recovery_queue and len(assigned) < self._slots_per_job:
                assigned.append(recovery_queue.popleft())
            if not assigned:
                break
            self._queue_bundle(account=account, assigned=assigned)
            queued_jobs += 1
        return queued_jobs


def run_slot_workers(
    *,
    slot_queue: list[SlotTaskRef],
    accounts: list[AccountConfigLike],
    slots_per_job: int,
    pending_buffer_per_account: int,
    worker: Callable[[BundleSpec], T],
    capacity_lookup: Callable[..., AccountCapacitySnapshot] = query_account_capacity,
    initial_completed_slots: dict[str, int] | None = None,
    job_index_start: int = 1,
    max_workers: int | None = None,
    idle_sleep_seconds: float = 1.0,
    on_capacity_snapshot: Callable[[AccountCapacitySnapshot], None] | None = None,
    on_bundle_submitted: Callable[[BundleSpec], None] | None = None,
    on_capacity_error: Callable[[AccountConfigLike, Exception], None] | None = None,
    recovery_slots_lookup: Callable[[BundleSpec, T], Sequence[SlotTaskRef]] | None = None,
    terminal_bundle_lookup: Callable[[BundleSpec, T], bool] | None = None,
) -> BalancedBatchResult[T]:
    if not slot_queue:
        return BalancedBatchResult(results=[], max_inflight_jobs=0, submitted_jobs=0)
    controller = SlotWorkerController(
        accounts=accounts,
        slots_per_job=slots_per_job,
        pending_buffer_per_account=pending_buffer_per_account,
        worker=worker,
        capacity_lookup=capacity_lookup,
        initial_completed_slots=initial_completed_slots,
        job_index_start=job_index_start,
        max_workers=max_workers,
        idle_sleep_seconds=idle_sleep_seconds,
        on_capacity_snapshot=on_capacity_snapshot,
        on_bundle_submitted=on_bundle_submitted,
        on_capacity_error=on_capacity_error,
        recovery_slots_lookup=recovery_slots_lookup,
        terminal_bundle_lookup=terminal_bundle_lookup,
    )
    controller.enqueue_slots(slot_queue)
    return controller.finalize()


def calculate_effective_slots(*, max_jobs_per_account: int) -> int:
    if max_jobs_per_account <= 0:
        raise ValueError("max_jobs_per_account must be > 0")
    return max_jobs_per_account


def run_jobs_with_slots(
    *,
    job_specs: list[JobSpec],
    max_slots: int,
    worker: Callable[[JobSpec], T],
) -> ScheduledBatchResult[T]:
    if max_slots <= 0:
        raise ValueError("max_slots must be > 0")

    lock = Lock()
    inflight = {"current": 0, "max": 0}

    def _tracked_worker(job: JobSpec) -> T:
        with lock:
            inflight["current"] += 1
            inflight["max"] = max(inflight["max"], inflight["current"])
        try:
            return worker(job)
        finally:
            with lock:
                inflight["current"] -= 1

    results: list[T] = []
    with ThreadPoolExecutor(max_workers=max_slots) as executor:
        futures: dict[Future[T], JobSpec] = {
            executor.submit(_tracked_worker, job_spec): job_spec for job_spec in job_specs
        }
        for future in as_completed(futures):
            results.append(future.result())

    return ScheduledBatchResult(results=results, max_inflight=inflight["max"])
