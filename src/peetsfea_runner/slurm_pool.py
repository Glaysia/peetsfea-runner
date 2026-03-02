from __future__ import annotations

import base64
import logging
import re
import shlex
import subprocess
from typing import Protocol

from peetsfea_runner.config import RunnerConfig, SlurmPolicy, WorkerAccount

LOG = logging.getLogger(__name__)


class SlurmClientError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


E_BOOTSTRAP_GIT = "E_BOOTSTRAP_GIT"
E_BOOTSTRAP_PYTHON = "E_BOOTSTRAP_PYTHON"
E_BOOTSTRAP_VENV = "E_BOOTSTRAP_VENV"
E_BOOTSTRAP_DEPS = "E_BOOTSTRAP_DEPS"
E_WIN_WORKER_PYTHON = "E_WIN_WORKER_PYTHON"
E_WIN_WORKER_IMPORT = "E_WIN_WORKER_IMPORT"
E_WIN_WORKER_RUNTIME = "E_WIN_WORKER_RUNTIME"
E_WIN_WORKER_NATIVE_CRASH = "E_WIN_WORKER_NATIVE_CRASH"
E_WIN_NO_INTERACTIVE_SESSION = "E_WIN_NO_INTERACTIVE_SESSION"
E_WIN_TASK_REGISTER = "E_WIN_TASK_REGISTER"
E_WIN_TASK_START = "E_WIN_TASK_START"
E_WIN_TASK_QUERY = "E_WIN_TASK_QUERY"


class SlurmClient(Protocol):
    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        raise NotImplementedError

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        raise NotImplementedError

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        raise NotImplementedError


class SubprocessSlurmClient:
    _REMOTE_REPO_PATH = "/home1/harry261/peetsfea-runner"
    _REMOTE_VENV_PATH = "/home1/harry261/.peetsfea-venv"
    _REMOTE_REPO_PATH_WIN = "C:/peetsfea-runner"
    _REMOTE_VENV_PATH_WIN = "C:/.peetsfea-venv"
    _REMOTE_PID_PATH_WIN = "C:/peetsfea-runner/var/remote_worker.pid"
    _REMOTE_AEDT_PATH_WIN = r"C:\Program Files\ANSYS Inc\v252\AnsysEM\ansysedt.exe"
    _WORKER_POLL_SEC = 2.0
    _DEFAULT_WIN_TASK_NAME = "peetsfea-worker-win5600x2"

    def __init__(self) -> None:
        self._bootstrapped_accounts: set[str] = set()
        self._windows_task_name_by_account: dict[str, str] = {}

    def _run_or_raise(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(args, capture_output=True, text=True, check=False, errors="replace")
        except TypeError:
            result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise SlurmClientError(f"command={' '.join(args)}; detail={detail}")

    def _run_bootstrap_step_or_raise(self, *, code: str, args: list[str]) -> None:
        try:
            self._run_or_raise(args)
        except SlurmClientError as exc:
            raise SlurmClientError(f"error_code={code}; {exc.message}") from exc

    @staticmethod
    def _is_windows_account(account: WorkerAccount) -> bool:
        if account.spool_paths is None:
            return False
        return len(account.spool_paths.inbox) >= 2 and account.spool_paths.inbox[1] == ":"

    @staticmethod
    def _ps_quote(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _windows_path_match_pattern(path: str) -> str:
        parts: list[str] = []
        for ch in path:
            if ch in ("/", "\\"):
                parts.append(r"[\\/]")
                continue
            parts.append(re.escape(ch))
        return "".join(parts)

    @staticmethod
    def _extract_digit_lines(text: str) -> list[str]:
        return [line.strip() for line in text.splitlines() if re.fullmatch(r"\d+", line.strip() or "")]

    @staticmethod
    def _read_first_nonempty_line(text: str) -> str | None:
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                return stripped
        return None

    def _task_name_for_account(self, *, account_id: str, policy: SlurmPolicy | None = None) -> str:
        if policy is not None:
            task_name = policy.windows_task_name.strip() if policy.windows_task_name.strip() else self._DEFAULT_WIN_TASK_NAME
            self._windows_task_name_by_account[account_id] = task_name
            return task_name
        return self._windows_task_name_by_account.get(account_id, self._DEFAULT_WIN_TASK_NAME)

    def _run_windows_powershell_or_raise(self, *, account: WorkerAccount, script: str) -> subprocess.CompletedProcess[str]:
        encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
        command = f"powershell -NoProfile -NonInteractive -EncodedCommand {encoded}"
        return self._run_or_raise(["ssh", account.ssh_alias, command])

    def _ensure_remote_bootstrap(self, *, account: WorkerAccount, policy: SlurmPolicy) -> None:
        if account.account_id in self._bootstrapped_accounts:
            return
        if account.spool_paths is None:
            raise SlurmClientError(
                f"account={account.account_id}; detail=missing spool_paths for remote worker bootstrap"
            )
        if self._is_windows_account(account):
            self._ensure_remote_bootstrap_windows(account=account, policy=policy)
            self._bootstrapped_accounts.add(account.account_id)
            return

        self._run_bootstrap_step_or_raise(
            code=E_BOOTSTRAP_GIT,
            args=[
                "ssh",
                account.ssh_alias,
                (
                    "mkdir -p "
                    f"{shlex.quote(self._REMOTE_REPO_PATH)} "
                    f"{shlex.quote(account.spool_paths.inbox)} "
                    f"{shlex.quote(account.spool_paths.claimed)} "
                    f"{shlex.quote(account.spool_paths.results)} "
                    f"{shlex.quote(account.spool_paths.failed)}"
                ),
            ],
        )

        repo_cmd = (
            "set -euo pipefail; "
            f"REPO_PATH={shlex.quote(self._REMOTE_REPO_PATH)}; "
            f"REPO_URL={shlex.quote(policy.repo_url)}; "
            f"RELEASE_TAG={shlex.quote(policy.release_tag)}; "
            'if [ ! -d "$REPO_PATH/.git" ]; then '
            'rm -rf "$REPO_PATH"; '
            'git clone "$REPO_URL" "$REPO_PATH"; '
            "fi; "
            'cd "$REPO_PATH"; '
            "git fetch --tags --force --prune; "
            'git checkout --force "tags/$RELEASE_TAG"; '
            "git clean -fdx"
        )
        self._run_bootstrap_step_or_raise(
            code=E_BOOTSTRAP_GIT,
            args=["ssh", account.ssh_alias, f"bash -lc {shlex.quote(repo_cmd)}"],
        )

        python_cmd = (
            "set -euo pipefail; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            'if [ ! -x "$VENV_PATH/bin/python" ]; then '
            "if command -v python3.12 >/dev/null 2>&1; then "
            'python3.12 -m venv "$VENV_PATH"; '
            "elif command -v conda >/dev/null 2>&1; then "
            'conda create -y -n peetsfea-py312 python=3.12 && '
            'conda run -n peetsfea-py312 python -m venv "$VENV_PATH"; '
            "else "
            'if [ ! -x "$HOME/miniconda3/bin/conda" ]; then '
            'curl -fsSL -o "$HOME/miniconda.sh" '
            '"https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"; '
            'bash "$HOME/miniconda.sh" -b -p "$HOME/miniconda3"; '
            'rm -f "$HOME/miniconda.sh"; '
            "fi; "
            '"$HOME/miniconda3/bin/conda" create -y -n peetsfea-py312 python=3.12 && '
            '"$HOME/miniconda3/bin/conda" run -n peetsfea-py312 python -m venv "$VENV_PATH"; '
            "fi; "
            "fi"
        )
        self._run_bootstrap_step_or_raise(
            code=E_BOOTSTRAP_PYTHON,
            args=["ssh", account.ssh_alias, f"bash -lc {shlex.quote(python_cmd)}"],
        )

        deps_cmd = (
            "set -euo pipefail; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            f"REPO_PATH={shlex.quote(self._REMOTE_REPO_PATH)}; "
            '"$VENV_PATH/bin/python" -m ensurepip >/dev/null 2>&1 || true; '
            '"$VENV_PATH/bin/python" -m pip install -q --disable-pip-version-check uv; '
            'cd "$REPO_PATH"; '
            '"$VENV_PATH/bin/python" -m uv pip install -q -e . pyaedt==0.24.1'
        )
        self._run_bootstrap_step_or_raise(
            code=E_BOOTSTRAP_DEPS,
            args=["ssh", account.ssh_alias, f"bash -lc {shlex.quote(deps_cmd)}"],
        )

        check_cmd = (
            "set -euo pipefail; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            'test -x "$VENV_PATH/bin/python"'
        )
        self._run_bootstrap_step_or_raise(
            code=E_BOOTSTRAP_VENV,
            args=["ssh", account.ssh_alias, f"bash -lc {shlex.quote(check_cmd)}"],
        )

        self._bootstrapped_accounts.add(account.account_id)

    def _ensure_remote_bootstrap_windows(self, *, account: WorkerAccount, policy: SlurmPolicy) -> None:
        assert account.spool_paths is not None
        bootstrap_script = (
            "$ErrorActionPreference='Stop'; "
            f"$repo={self._ps_quote(self._REMOTE_REPO_PATH_WIN)}; "
            f"$venv={self._ps_quote(self._REMOTE_VENV_PATH_WIN)}; "
            f"$repoUrl={self._ps_quote(policy.repo_url)}; "
            f"$releaseTag={self._ps_quote(policy.release_tag)}; "
            "$spool=@("
            f"{self._ps_quote(account.spool_paths.inbox)},"
            f"{self._ps_quote(account.spool_paths.claimed)},"
            f"{self._ps_quote(account.spool_paths.results)},"
            f"{self._ps_quote(account.spool_paths.failed)}"
            "); "
            "foreach($d in $spool){ New-Item -ItemType Directory -Force -Path $d | Out-Null }; "
            "if (!(Test-Path -LiteralPath ($repo + '/.git'))) { "
            "if (Test-Path -LiteralPath $repo) { Remove-Item -Recurse -Force -LiteralPath $repo }; "
            "git clone $repoUrl $repo | Out-Null }; "
            "Set-Location $repo; "
            "git fetch --tags --force --prune | Out-Null; "
            "git checkout --force ('tags/' + $releaseTag) | Out-Null; "
            "git clean -fdx | Out-Null; "
            "if (!(Test-Path -LiteralPath ($venv + '/Scripts/python.exe'))) { "
            "if (Get-Command py -ErrorAction SilentlyContinue) { py -3.12 -m venv $venv } "
            "elseif (Get-Command python -ErrorAction SilentlyContinue) { python -m venv $venv } "
            "else { throw 'python3.12 launcher not found on Windows host' } }; "
            "& ($venv + '/Scripts/python.exe') -m ensurepip | Out-Null; "
            "& ($venv + '/Scripts/python.exe') -m pip install --disable-pip-version-check uv | Out-Null; "
            "& ($venv + '/Scripts/python.exe') -m uv pip install -e . pyaedt==0.24.1 | Out-Null"
        )
        try:
            self._run_windows_powershell_or_raise(account=account, script=bootstrap_script)
        except SlurmClientError as exc:
            raise SlurmClientError(f"error_code={E_BOOTSTRAP_GIT}; {exc.message}") from exc

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        if self._is_windows_account(account):
            return self._query_windows_workers(account=account, policy=policy)
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        query_cmd = f"squeue -h -n {shlex.quote(job_name)} -o %A"
        result = self._run_or_raise(["ssh", account.ssh_alias, query_cmd])
        lines = [line.strip() for line in result.stdout.splitlines()]
        return [line for line in lines if line]

    def _query_windows_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        assert account.spool_paths is not None
        task_name = self._task_name_for_account(account_id=account.account_id, policy=policy)
        spool_inbox = account.spool_paths.inbox
        escaped_task_name = self._ps_quote(task_name)
        escaped_spool_inbox = self._ps_quote(spool_inbox)
        escaped_spool_pattern = self._ps_quote(self._windows_path_match_pattern(spool_inbox))
        query_script = (
            "$ErrorActionPreference='Stop'; "
            "$ProgressPreference='SilentlyContinue'; "
            f"$pidPath={self._ps_quote(self._REMOTE_PID_PATH_WIN)}; "
            f"$taskName={escaped_task_name}; "
            f"$spoolInbox={escaped_spool_inbox}; "
            f"$spoolPattern={escaped_spool_pattern}; "
            "$resolved=@(); "
            "if (Test-Path -LiteralPath $pidPath) { "
            "$workerPid=(Get-Content -LiteralPath $pidPath -Raw).Trim(); "
            "if ($workerPid) { "
            "$p=Get-Process -Id $workerPid -ErrorAction SilentlyContinue; "
            "if ($null -ne $p) { $resolved += [string]$workerPid } "
            "else { Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue } "
            "} else { Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue } "
            "}; "
            "if ($resolved.Count -eq 0) { "
            "$candidates=Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
            "Where-Object { $_.CommandLine -and $_.CommandLine -match 'peetsfea_runner.remote_worker' -and $_.CommandLine -match $spoolPattern }; "
            "foreach($candidate in $candidates){ "
            "$cp=Get-Process -Id $candidate.ProcessId -ErrorAction SilentlyContinue; "
            "if($null -ne $cp){ $resolved += [string]$candidate.ProcessId } "
            "}; "
            "$resolved=$resolved | Select-Object -Unique; "
            "if($resolved.Count -gt 0){ $resolved[0] | Set-Content -LiteralPath $pidPath -NoNewline } "
            "}; "
            "if ($resolved.Count -eq 0) { "
            "$task=Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "
            "if ($null -ne $task) { "
            "$taskInfo=Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction SilentlyContinue; "
            "if ($null -ne $taskInfo) { "
            "Write-Output ('TASK_STATE=' + $taskInfo.State + ';TASK_LAST=' + $taskInfo.LastTaskResult) "
            "} "
            "} "
            "}; "
            "foreach($resolvedPid in $resolved){ Write-Output $resolvedPid }; "
            "exit 0"
        )
        try:
            result = self._run_windows_powershell_or_raise(account=account, script=query_script)
        except SlurmClientError as exc:
            raise SlurmClientError(f"error_code={E_WIN_TASK_QUERY}; {exc.message}") from exc

        lines = self._extract_digit_lines(result.stdout)
        if lines:
            return lines

        first_line = self._read_first_nonempty_line(result.stdout)
        if first_line and first_line.startswith("TASK_STATE="):
            LOG.info("windows_task_idle account=%s detail=%s", account.account_id, first_line)
        return []

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        if self._is_windows_account(account):
            return self._submit_windows_worker(account=account, policy=policy)
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        self._ensure_remote_bootstrap(account=account, policy=policy)
        assert account.spool_paths is not None
        aedt_flag = ""
        if policy.aedt_executable_path:
            aedt_flag = f" --aedt-executable-path {shlex.quote(policy.aedt_executable_path)}"
        worker_cmd = (
            "set -euo pipefail; "
            f"REPO_PATH={shlex.quote(self._REMOTE_REPO_PATH)}; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            'if [ ! -d "$REPO_PATH" ]; then echo "missing repo: $REPO_PATH" >&2; exit 2; fi; '
            'if [ ! -x "$VENV_PATH/bin/python" ]; then echo "missing venv python: $VENV_PATH/bin/python" >&2; exit 3; fi; '
            "source /etc/profile.d/modules.sh >/dev/null 2>&1 || true; "
            "module load ansys-electronics/v252; "
            'cd "$REPO_PATH"; '
            '"$VENV_PATH/bin/python" -m peetsfea_runner.remote_worker '
            f"--spool-inbox {shlex.quote(account.spool_paths.inbox)} "
            f"--spool-claimed {shlex.quote(account.spool_paths.claimed)} "
            f"--spool-results {shlex.quote(account.spool_paths.results)} "
            f"--spool-failed {shlex.quote(account.spool_paths.failed)} "
            f"--poll-sec {self._WORKER_POLL_SEC} "
            f"--internal-procs {policy.job_internal_procs}"
            f"{aedt_flag}"
        )
        wrap = "bash -lc " + shlex.quote(worker_cmd)
        submit_cmd = (
            "sbatch --parsable "
            f"--job-name {shlex.quote(job_name)} "
            f"--partition {shlex.quote(policy.partition)} "
            f"--cpus-per-task {policy.cores} "
            f"--mem {policy.memory_gb}G "
            f"--export ALL,PEETSFEA_INTERNAL_PROCS={policy.job_internal_procs} "
            f"--wrap={shlex.quote(wrap)}"
        )
        result = self._run_or_raise(["ssh", account.ssh_alias, submit_cmd])
        job_id_field = result.stdout.strip().split(";", 1)[0]
        if not job_id_field:
            raise SlurmClientError(
                f"command=ssh {account.ssh_alias} {submit_cmd}; detail=empty job id"
            )
        return job_id_field

    def _submit_windows_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        if policy.windows_launch_mode == "service":
            return self._submit_windows_worker_service(account=account, policy=policy)
        return self._submit_windows_worker_interactive_task(account=account, policy=policy)

    def _submit_windows_worker_service(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        self._ensure_remote_bootstrap(account=account, policy=policy)
        assert account.spool_paths is not None
        aedt_executable_path = policy.aedt_executable_path or self._REMOTE_AEDT_PATH_WIN
        # Windows debug host policy: never exceed 6 cores for a single AEDT task.
        windows_internal_procs = min(policy.job_internal_procs, 6)

        active = self._query_windows_workers(account=account, policy=policy)
        if active:
            return active[0]

        start_script = (
            "$ErrorActionPreference='Stop'; "
            "$ProgressPreference='SilentlyContinue'; "
            f"$repo={self._ps_quote(self._REMOTE_REPO_PATH_WIN)}; "
            f"$venv={self._ps_quote(self._REMOTE_VENV_PATH_WIN)}; "
            f"$pidPath={self._ps_quote(self._REMOTE_PID_PATH_WIN)}; "
            "$pidDir=[System.IO.Path]::GetDirectoryName($pidPath); "
            "if ($pidDir) { New-Item -ItemType Directory -Force -Path $pidDir | Out-Null }; "
            "$stdoutPath=($pidDir + '/remote_worker.stdout.log'); "
            "$stderrPath=($pidDir + '/remote_worker.stderr.log'); "
            "$python=($venv + '/Scripts/python.exe'); "
            "if (!(Test-Path -LiteralPath $python)) { "
            f"throw {self._ps_quote(f'error_code={E_WIN_WORKER_PYTHON}; detail=missing python: ')} + $python "
            "}; "
            "$args=@("
            "'-X','faulthandler',"
            "'-m','peetsfea_runner.remote_worker',"
            f"'--spool-inbox',{self._ps_quote(account.spool_paths.inbox)},"
            f"'--spool-claimed',{self._ps_quote(account.spool_paths.claimed)},"
            f"'--spool-results',{self._ps_quote(account.spool_paths.results)},"
            f"'--spool-failed',{self._ps_quote(account.spool_paths.failed)},"
            f"'--poll-sec',{self._ps_quote(str(self._WORKER_POLL_SEC))},"
            f"'--internal-procs',{self._ps_quote(str(windows_internal_procs))},"
            f"'--aedt-executable-path',{self._ps_quote(aedt_executable_path)},"
            "'--gui'"
            "); "
            "$quotedArgs=($args | ForEach-Object { '\"' + ($_ -replace '\"','\\\"') + '\"' }) -join ' '; "
            "$commandLine='\"' + $python + '\" ' + $quotedArgs; "
            "$result=Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
            "-Arguments @{CommandLine=$commandLine; CurrentDirectory=$repo}; "
            "if ($result.ReturnValue -ne 0 -or -not $result.ProcessId) { "
            f"throw {self._ps_quote(f'error_code={E_WIN_WORKER_RUNTIME}; detail=Win32_Process.Create failed; return_value=')} + $result.ReturnValue "
            "}; "
            "$workerPid=[int]$result.ProcessId; "
            "Start-Sleep -Seconds 3; "
            "$alive=Get-Process -Id $workerPid -ErrorAction SilentlyContinue; "
            "if ($null -eq $alive) { "
            "$stderrTail=''; "
            "if (Test-Path -LiteralPath $stderrPath) { $stderrTail=((Get-Content -LiteralPath $stderrPath -Tail 80) -join \"`n\") }; "
            "$stdoutTail=''; "
            "if (Test-Path -LiteralPath $stdoutPath) { $stdoutTail=((Get-Content -LiteralPath $stdoutPath -Tail 80) -join \"`n\") }; "
            f"$code={self._ps_quote(E_WIN_WORKER_RUNTIME)}; "
            "if ($stderrTail -match 'ModuleNotFoundError|ImportError|No module named|Failed to import') { "
            f"$code={self._ps_quote(E_WIN_WORKER_IMPORT)} "
            "} "
            "elseif ([string]::IsNullOrWhiteSpace($stderrTail)) { "
            f"$code={self._ps_quote(E_WIN_WORKER_NATIVE_CRASH)} "
            "}; "
            "throw ("
            "'error_code=' + $code + "
            "'; detail=worker_died_after_launch; pid=' + $workerPid + "
            "'; stdout_tail=' + $stdoutTail + "
            "'; stderr_tail=' + $stderrTail"
            "); "
            "}; "
            "$workerPid | Set-Content -LiteralPath $pidPath -NoNewline; "
            "Write-Output $workerPid"
        )
        result = self._run_windows_powershell_or_raise(account=account, script=start_script)
        numeric_lines = self._extract_digit_lines(result.stdout)
        job_id_field = numeric_lines[-1] if numeric_lines else ""
        if not job_id_field:
            raise SlurmClientError(
                f"account={account.account_id}; detail=empty windows worker pid; stdout={result.stdout.strip()}"
            )
        return job_id_field

    def _submit_windows_worker_interactive_task(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        self._ensure_remote_bootstrap(account=account, policy=policy)
        assert account.spool_paths is not None
        aedt_executable_path = policy.aedt_executable_path or self._REMOTE_AEDT_PATH_WIN
        windows_internal_procs = min(policy.job_internal_procs, 6)
        task_name = self._task_name_for_account(account_id=account.account_id, policy=policy)
        spool_pattern = self._windows_path_match_pattern(account.spool_paths.inbox)

        active = self._query_windows_workers(account=account, policy=policy)
        if active:
            return active[0]

        start_script = (
            "$ErrorActionPreference='Stop'; "
            "$ProgressPreference='SilentlyContinue'; "
            f"$t={self._ps_quote(task_name)}; "
            f"$u={self._ps_quote(policy.windows_interactive_user)}; "
            f"$r={self._ps_quote(self._REMOTE_REPO_PATH_WIN)}; "
            f"$v={self._ps_quote(self._REMOTE_VENV_PATH_WIN)}; "
            f"$p={self._ps_quote(self._REMOTE_PID_PATH_WIN)}; "
            f"$i={self._ps_quote(account.spool_paths.inbox)}; "
            f"$m={self._ps_quote(spool_pattern)}; "
            f"$c={self._ps_quote(account.spool_paths.claimed)}; "
            f"$s={self._ps_quote(account.spool_paths.results)}; "
            f"$f={self._ps_quote(account.spool_paths.failed)}; "
            f"$a={self._ps_quote(aedt_executable_path)}; "
            f"$o={self._ps_quote(str(self._WORKER_POLL_SEC))}; "
            f"$n={self._ps_quote(str(windows_internal_procs))}; "
            "$py=($v + '/Scripts/python.exe'); "
            "if(!(Test-Path -LiteralPath $py)){ "
            f"throw {self._ps_quote(f'error_code={E_WIN_WORKER_PYTHON}; detail=missing python')} "
            "}; "
            "$arg=('-X faulthandler -m peetsfea_runner.remote_worker "
            "--spool-inbox \"' + $i + '\" "
            "--spool-claimed \"' + $c + '\" "
            "--spool-results \"' + $s + '\" "
            "--spool-failed \"' + $f + '\" "
            "--poll-sec ' + $o + ' "
            "--internal-procs ' + $n + ' "
            "--aedt-executable-path \"' + $a + '\" "
            "--gui'); "
            "$pr=$null; "
            "try{$pr=New-ScheduledTaskPrincipal -UserId $u -LogonType InteractiveToken -RunLevel Highest}"
            "catch{$pr=New-ScheduledTaskPrincipal -UserId $u -LogonType Interactive -RunLevel Highest}; "
            "$ac=New-ScheduledTaskAction -Execute $py -Argument $arg -WorkingDirectory $r; "
            "$st=New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew; "
            "try{"
            "Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue | Out-Null; "
            "Register-ScheduledTask -TaskName $t -Action $ac -Principal $pr -Settings $st -Force | Out-Null"
            "}catch{"
            f"throw {self._ps_quote(f'error_code={E_WIN_TASK_REGISTER}; detail=')} + $_.Exception.Message"
            "}; "
            "try{Start-ScheduledTask -TaskName $t}"
            "catch{"
            f"throw {self._ps_quote(f'error_code={E_WIN_TASK_START}; detail=')} + $_.Exception.Message"
            "}; "
            "Start-Sleep -Seconds 4; "
            "$w=Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
            "Where-Object { $_.CommandLine -and $_.CommandLine -match 'peetsfea_runner.remote_worker' -and $_.CommandLine -match $m } | "
            "Sort-Object CreationDate -Descending | Select-Object -First 1 -ExpandProperty ProcessId; "
            "if(-not $w){"
            f"throw {self._ps_quote(f'error_code={E_WIN_WORKER_RUNTIME}; detail=worker_died_after_launch')}"
            "}; "
            "$d=[System.IO.Path]::GetDirectoryName($p); "
            "if($d){New-Item -ItemType Directory -Force -Path $d | Out-Null}; "
            "[string]$w | Set-Content -LiteralPath $p -NoNewline; "
            "Write-Output $w"
        )
        result = self._run_windows_powershell_or_raise(account=account, script=start_script)
        numeric_lines = self._extract_digit_lines(result.stdout)
        job_id_field = numeric_lines[-1] if numeric_lines else ""
        if not job_id_field:
            raise SlurmClientError(
                f"account={account.account_id}; detail=empty windows worker pid; stdout={result.stdout.strip()}"
            )
        return job_id_field

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        if self._is_windows_account(account):
            task_name = self._task_name_for_account(account_id=account.account_id)
            spool_inbox = account.spool_paths.inbox if account.spool_paths is not None else ""
            spool_pattern = self._windows_path_match_pattern(spool_inbox) if spool_inbox else ""
            stop_script = (
                "$ErrorActionPreference='Stop'; "
                "$ProgressPreference='SilentlyContinue'; "
                f"$pidPath={self._ps_quote(self._REMOTE_PID_PATH_WIN)}; "
                f"$workerPid={self._ps_quote(slurm_job_id)}; "
                f"$taskName={self._ps_quote(task_name)}; "
                f"$spoolInbox={self._ps_quote(spool_inbox)}; "
                f"$spoolPattern={self._ps_quote(spool_pattern)}; "
                "Stop-Process -Id $workerPid -Force -ErrorAction SilentlyContinue; "
                "if ($spoolInbox) { "
                "$orphans=Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
                "Where-Object { $_.CommandLine -and $_.CommandLine -match 'peetsfea_runner.remote_worker' -and $_.CommandLine -match $spoolPattern }; "
                "foreach($orphan in $orphans){ Stop-Process -Id $orphan.ProcessId -Force -ErrorAction SilentlyContinue } "
                "}; "
                "Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; "
                "Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null; "
                "if (Test-Path -LiteralPath $pidPath) { Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue }"
            )
            self._run_windows_powershell_or_raise(account=account, script=stop_script)
            return
        self._run_or_raise(["ssh", account.ssh_alias, f"scancel {shlex.quote(slurm_job_id)}"])


class WorkerPoolManager:
    def __init__(self, config: RunnerConfig, client: SlurmClient | None = None) -> None:
        self._config = config
        self._client = client if client is not None else SubprocessSlurmClient()
        self._degraded_accounts: set[str] = set()

    @staticmethod
    def _is_windows_account(account: WorkerAccount) -> bool:
        if account.spool_paths is None:
            return False
        return len(account.spool_paths.inbox) >= 2 and account.spool_paths.inbox[1] == ":"

    def is_degraded(self, account_id: str) -> bool:
        return account_id in self._degraded_accounts

    def healthy_accounts(self) -> tuple[WorkerAccount, ...]:
        return tuple(
            account
            for account in self._config.worker_accounts
            if account.account_id not in self._degraded_accounts
        )

    def _mark_degraded(self, *, account_id: str, message: str) -> None:
        self._degraded_accounts.add(account_id)
        LOG.warning("worker_pool_degraded account=%s reason=%s", account_id, message)

    def _mark_recovered(self, *, account_id: str) -> None:
        if account_id in self._degraded_accounts:
            self._degraded_accounts.remove(account_id)
            LOG.info("worker_pool_recovered account=%s", account_id)

    def _log_snapshot(self, *, account_id: str, pool_actual: int) -> None:
        LOG.info(
            "worker_pool_snapshot account=%s pool_target=%d pool_actual=%d degraded=%s",
            account_id,
            self._config.slurm_policy.pool_target_per_account,
            pool_actual,
            account_id in self._degraded_accounts,
        )

    def _trim_excess_workers(
        self,
        *,
        account: WorkerAccount,
        active_job_ids: list[str],
    ) -> tuple[int, bool]:
        processed = 0
        target = self._config.slurm_policy.pool_target_per_account
        if len(active_job_ids) <= target:
            return 0, True

        extras = active_job_ids[target:]
        for job_id in extras:
            try:
                self._client.cancel_worker(account=account, slurm_job_id=job_id)
            except SlurmClientError as exc:
                self._mark_degraded(account_id=account.account_id, message=exc.message)
                return processed + 1, False

            processed += 1
            LOG.info(
                "worker_cancelled account=%s slurm_job_id=%s pool_target=%d pool_actual=%d",
                account.account_id,
                job_id,
                target,
                max(target, len(active_job_ids) - processed),
            )

        return processed, True

    def _fill_missing_workers(
        self,
        *,
        account: WorkerAccount,
        active_count: int,
    ) -> tuple[int, int]:
        processed = 0
        current = active_count
        target = self._config.slurm_policy.pool_target_per_account
        while current < target:
            try:
                slurm_job_id = self._client.submit_worker(account=account, policy=self._config.slurm_policy)
            except SlurmClientError as exc:
                self._mark_degraded(account_id=account.account_id, message=exc.message)
                return processed + 1, current

            processed += 1
            current += 1
            LOG.info(
                "worker_submitted account=%s slurm_job_id=%s pool_target=%d pool_actual=%d",
                account.account_id,
                slurm_job_id,
                target,
                current,
            )

        return processed, current

    def process_once(self) -> int:
        processed = 0
        for account in self._config.worker_accounts:
            try:
                active_job_ids = self._client.query_workers(account=account, policy=self._config.slurm_policy)
            except SlurmClientError as exc:
                self._mark_degraded(account_id=account.account_id, message=exc.message)
                processed += 1
                continue

            self._mark_recovered(account_id=account.account_id)

            # Guard against duplicate job ids from inconsistent query output.
            deduped_job_ids = list(dict.fromkeys(active_job_ids))

            trimmed_count, trim_ok = self._trim_excess_workers(
                account=account,
                active_job_ids=deduped_job_ids,
            )
            processed += trimmed_count

            current_count = min(
                len(deduped_job_ids),
                self._config.slurm_policy.pool_target_per_account,
            )
            if not trim_ok:
                self._log_snapshot(account_id=account.account_id, pool_actual=current_count)
                continue

            filled_count, current_count = self._fill_missing_workers(
                account=account,
                active_count=current_count,
            )
            processed += filled_count
            self._log_snapshot(account_id=account.account_id, pool_actual=current_count)

        return processed

    def cancel_all_workers(self) -> int:
        processed = 0
        for account in self._config.worker_accounts:
            try:
                active_job_ids = self._client.query_workers(account=account, policy=self._config.slurm_policy)
            except SlurmClientError as exc:
                self._mark_degraded(account_id=account.account_id, message=exc.message)
                processed += 1
                continue

            self._mark_recovered(account_id=account.account_id)
            deduped_job_ids = list(dict.fromkeys(active_job_ids))

            # Windows interactive-task mode may leave a scheduled task behind even
            # when the tracked pid is already gone. Force one cleanup pass.
            if not deduped_job_ids and self._is_windows_account(account):
                try:
                    self._client.cancel_worker(account=account, slurm_job_id="0")
                except SlurmClientError as exc:
                    self._mark_degraded(account_id=account.account_id, message=exc.message)
                    processed += 1
                    continue

                processed += 1
                LOG.info("worker_cancelled_on_shutdown account=%s slurm_job_id=0", account.account_id)
                continue

            for job_id in deduped_job_ids:
                try:
                    self._client.cancel_worker(account=account, slurm_job_id=job_id)
                except SlurmClientError as exc:
                    self._mark_degraded(account_id=account.account_id, message=exc.message)
                    processed += 1
                    break

                processed += 1
                LOG.info("worker_cancelled_on_shutdown account=%s slurm_job_id=%s", account.account_id, job_id)

        return processed
