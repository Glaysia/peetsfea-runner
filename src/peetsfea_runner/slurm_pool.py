from __future__ import annotations

import logging
import shlex
import subprocess
from pathlib import Path
from typing import Protocol

from peetsfea_runner.config import RunnerConfig, SlurmPolicy, WorkerAccount

LOG = logging.getLogger(__name__)


class SlurmClientError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class SlurmClient(Protocol):
    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        raise NotImplementedError

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        raise NotImplementedError

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        raise NotImplementedError


class SubprocessSlurmClient:
    _LOCAL_REPO_PATH = Path(__file__).resolve().parents[2]
    _REMOTE_REPO_PATH = "/home1/harry261/peetsfea-runner"
    _REMOTE_VENV_PATH = "/home1/harry261/.peetsfea-venv"
    _WORKER_POLL_SEC = 2.0

    def __init__(self) -> None:
        self._bootstrapped_accounts: set[str] = set()

    def _run_or_raise(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise SlurmClientError(f"command={' '.join(args)}; detail={detail}")

    def _ensure_remote_bootstrap(self, *, account: WorkerAccount) -> None:
        if account.account_id in self._bootstrapped_accounts:
            return
        if account.spool_paths is None:
            raise SlurmClientError(
                f"account={account.account_id}; detail=missing spool_paths for remote worker bootstrap"
            )

        self._run_or_raise(
            [
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
            ]
        )

        self._run_or_raise(
            [
                "rsync",
                "-az",
                "--delete",
                "--exclude",
                ".git",
                "--exclude",
                ".venv",
                "--exclude",
                "__pycache__",
                "--exclude",
                ".pytest_cache",
                f"{self._LOCAL_REPO_PATH}/",
                f"{account.ssh_alias}:{self._REMOTE_REPO_PATH}/",
            ]
        )

        bootstrap_cmd = (
            "set -euo pipefail; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            f"REPO_PATH={shlex.quote(self._REMOTE_REPO_PATH)}; "
            'if [ ! -x "$VENV_PATH/bin/python" ]; then python3.12 -m venv "$VENV_PATH"; fi; '
            '"$VENV_PATH/bin/python" -m ensurepip >/dev/null 2>&1 || true; '
            '"$VENV_PATH/bin/python" -m pip install -q --disable-pip-version-check uv; '
            '"$VENV_PATH/bin/python" -m uv pip install -q -e "$REPO_PATH" pyaedt==0.24.1'
        )
        self._run_or_raise(["ssh", account.ssh_alias, f"bash -lc {shlex.quote(bootstrap_cmd)}"])
        self._bootstrapped_accounts.add(account.account_id)

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        query_cmd = f"squeue -h -n {shlex.quote(job_name)} -o %A"
        result = self._run_or_raise(["ssh", account.ssh_alias, query_cmd])
        lines = [line.strip() for line in result.stdout.splitlines()]
        return [line for line in lines if line]

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        self._ensure_remote_bootstrap(account=account)
        assert account.spool_paths is not None
        worker_cmd = (
            "set -euo pipefail; "
            f"REPO_PATH={shlex.quote(self._REMOTE_REPO_PATH)}; "
            f"VENV_PATH={shlex.quote(self._REMOTE_VENV_PATH)}; "
            'if [ ! -d "$REPO_PATH" ]; then echo "missing repo: $REPO_PATH" >&2; exit 2; fi; '
            'if [ ! -x "$VENV_PATH/bin/python" ]; then echo "missing venv python: $VENV_PATH/bin/python" >&2; exit 3; fi; '
            'cd "$REPO_PATH"; '
            '"$VENV_PATH/bin/python" -m peetsfea_runner.remote_worker '
            f"--spool-inbox {shlex.quote(account.spool_paths.inbox)} "
            f"--spool-claimed {shlex.quote(account.spool_paths.claimed)} "
            f"--spool-results {shlex.quote(account.spool_paths.results)} "
            f"--spool-failed {shlex.quote(account.spool_paths.failed)} "
            f"--poll-sec {self._WORKER_POLL_SEC} "
            f"--internal-procs {policy.job_internal_procs}"
        )
        wrap = (
            "bash -lc "
            + shlex.quote(worker_cmd)
        )
        submit_cmd = (
            "sbatch --parsable "
            f"--job-name {shlex.quote(job_name)} "
            f"--partition {shlex.quote(policy.partition)} "
            f"--cpus-per-task {policy.cores} "
            f"--mem {policy.memory_gb}G "
            f"--export ALL,PEETSFEA_INTERNAL_PROCS={policy.job_internal_procs} "
            f"--wrap {wrap}"
        )
        result = self._run_or_raise(["ssh", account.ssh_alias, submit_cmd])
        job_id_field = result.stdout.strip().split(";", 1)[0]
        if not job_id_field:
            raise SlurmClientError(
                f"command=ssh {account.ssh_alias} {submit_cmd}; detail=empty job id"
            )
        return job_id_field

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        self._run_or_raise(["ssh", account.ssh_alias, f"scancel {shlex.quote(slurm_job_id)}"])


class WorkerPoolManager:
    def __init__(self, config: RunnerConfig, client: SlurmClient | None = None) -> None:
        self._config = config
        self._client = client if client is not None else SubprocessSlurmClient()
        self._degraded_accounts: set[str] = set()

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
