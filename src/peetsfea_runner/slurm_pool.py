from __future__ import annotations

import logging
import shlex
import subprocess
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
    def _run_or_raise(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return result

        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit_code={result.returncode}"
        raise SlurmClientError(f"command={' '.join(args)}; detail={detail}")

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        query_cmd = f"squeue -h -n {shlex.quote(job_name)} -o %A"
        result = self._run_or_raise(["ssh", account.ssh_alias, query_cmd])
        lines = [line.strip() for line in result.stdout.splitlines()]
        return [line for line in lines if line]

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        job_name = f"{policy.job_name_prefix}-{account.account_id}"
        wrap = (
            "bash -lc "
            + shlex.quote(
                "echo peetsfea worker started; "
                "sleep infinity"
            )
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
