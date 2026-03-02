from __future__ import annotations

import subprocess
from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.slurm_pool import SlurmClientError, SubprocessSlurmClient, WorkerPoolManager


class FakeSlurmClient:
    def __init__(self) -> None:
        self.jobs: dict[str, list[str]] = {}
        self.query_fail_accounts: set[str] = set()
        self.submit_fail_accounts: set[str] = set()
        self.cancel_fail_accounts: set[str] = set()
        self.submit_records: list[tuple[str, SlurmPolicy]] = []
        self.cancel_records: list[tuple[str, str]] = []
        self._next_job_id = 2000

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        _ = policy
        if account.account_id in self.query_fail_accounts:
            raise SlurmClientError(f"query failed for {account.account_id}")
        return list(self.jobs.get(account.account_id, []))

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        if account.account_id in self.submit_fail_accounts:
            raise SlurmClientError(f"submit failed for {account.account_id}")
        self._next_job_id += 1
        job_id = str(self._next_job_id)
        self.jobs.setdefault(account.account_id, []).append(job_id)
        self.submit_records.append((account.account_id, policy))
        return job_id

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        if account.account_id in self.cancel_fail_accounts:
            raise SlurmClientError(f"cancel failed for {account.account_id}")
        current = self.jobs.get(account.account_id, [])
        self.jobs[account.account_id] = [job_id for job_id in current if job_id != slurm_job_id]
        self.cancel_records.append((account.account_id, slurm_job_id))


def _build_config(
    tmp_path: Path,
    *,
    target: int,
    account_ids: tuple[str, ...],
) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    gate_account = GateAccount(
        account_id=account_ids[0],
        ssh_alias=f"ssh-{account_ids[0]}",
        spool_paths=RemoteSpoolPaths(
            inbox="/remote/spool/inbox",
            claimed="/remote/spool/claimed",
            results="/remote/spool/results",
            failed="/remote/spool/failed",
        ),
    )
    worker_accounts = tuple(WorkerAccount(account_id=account_id, ssh_alias=f"ssh-{account_id}") for account_id in account_ids)
    slurm_policy = SlurmPolicy(
        partition="cpu2",
        cores=32,
        memory_gb=320,
        job_internal_procs=8,
        pool_target_per_account=target,
    )
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=queue_dirs.state / "runner.duckdb",
        queue_dirs=queue_dirs,
        gate_account=gate_account,
        gate_accounts=(gate_account,),
        worker_accounts=worker_accounts,
        slurm_policy=slurm_policy,
    )


def test_worker_pool_fills_missing_workers_to_target(tmp_path: Path) -> None:
    config = _build_config(tmp_path, target=3, account_ids=("acct-a",))
    client = FakeSlurmClient()
    client.jobs["acct-a"] = ["1001"]

    manager = WorkerPoolManager(config, client=client)
    processed = manager.process_once()

    assert processed == 2
    assert len(client.jobs["acct-a"]) == 3


def test_worker_pool_cancels_excess_workers_above_target(tmp_path: Path) -> None:
    config = _build_config(tmp_path, target=3, account_ids=("acct-a",))
    client = FakeSlurmClient()
    client.jobs["acct-a"] = ["1", "2", "3", "4", "5"]

    manager = WorkerPoolManager(config, client=client)
    processed = manager.process_once()

    assert processed == 2
    assert len(client.jobs["acct-a"]) == 3
    assert len(client.cancel_records) == 2


def test_worker_pool_recovers_after_worker_loss(tmp_path: Path) -> None:
    config = _build_config(tmp_path, target=3, account_ids=("acct-a",))
    client = FakeSlurmClient()
    client.jobs["acct-a"] = ["1001", "1002", "1003"]

    manager = WorkerPoolManager(config, client=client)
    first = manager.process_once()
    assert first == 0

    client.jobs["acct-a"].pop()

    second = manager.process_once()
    assert second == 1
    assert len(client.jobs["acct-a"]) == 3


def test_worker_pool_bypasses_degraded_account_and_keeps_healthy_pool(tmp_path: Path) -> None:
    config = _build_config(tmp_path, target=3, account_ids=("acct-a", "acct-b"))
    client = FakeSlurmClient()
    client.jobs["acct-b"] = ["2001"]
    client.query_fail_accounts.add("acct-a")

    manager = WorkerPoolManager(config, client=client)
    processed = manager.process_once()

    assert processed == 3
    assert manager.is_degraded("acct-a") is True
    assert [account.account_id for account in manager.healthy_accounts()] == ["acct-b"]
    assert len(client.jobs["acct-b"]) == 3


def test_worker_pool_submit_uses_fixed_slurm_policy(tmp_path: Path) -> None:
    config = _build_config(tmp_path, target=1, account_ids=("acct-a",))
    client = FakeSlurmClient()

    manager = WorkerPoolManager(config, client=client)
    processed = manager.process_once()

    assert processed == 1
    assert len(client.submit_records) == 1

    _, used_policy = client.submit_records[0]
    assert used_policy.partition == "cpu2"
    assert used_policy.cores == 32
    assert used_policy.memory_gb == 320
    assert used_policy.job_internal_procs == 8


def test_subprocess_slurm_client_submit_uses_remote_worker_entrypoint(monkeypatch: object) -> None:
    calls: list[list[str]] = []

    def _fake_run(args: list[str], capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        _ = capture_output, text, check
        calls.append(args)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="12345;cluster\n", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)

    client = SubprocessSlurmClient()
    account = WorkerAccount(
        account_id="acct-a",
        ssh_alias="gate1-harry",
        spool_paths=RemoteSpoolPaths(
            inbox="/home1/harry261/peetsfea-spool/inbox",
            claimed="/home1/harry261/peetsfea-spool/claimed",
            results="/home1/harry261/peetsfea-spool/results",
            failed="/home1/harry261/peetsfea-spool/failed",
        ),
    )
    policy = SlurmPolicy(
        partition="cpu2",
        cores=32,
        memory_gb=320,
        job_internal_procs=8,
        pool_target_per_account=10,
    )

    job_id = client.submit_worker(account=account, policy=policy)

    assert job_id == "12345"
    assert len(calls) == 4
    assert calls[0][0] == "ssh"
    assert "mkdir -p /home1/harry261/peetsfea-runner" in calls[0][2]
    assert calls[1][0] == "rsync"
    assert calls[2][0] == "ssh"
    assert "python3.12 -m venv" in calls[2][2]

    submit_args = calls[3]
    assert submit_args[0] == "ssh"
    assert submit_args[1] == "gate1-harry"
    command = submit_args[2]
    assert "sbatch --parsable" in command
    assert "--partition cpu2" in command
    assert "--cpus-per-task 32" in command
    assert "--mem 320G" in command
    assert "python\" -m peetsfea_runner.remote_worker" in command
    assert "--spool-inbox /home1/harry261/peetsfea-spool/inbox" in command


def test_subprocess_slurm_client_submit_requires_worker_spool_paths() -> None:
    client = SubprocessSlurmClient()
    account = WorkerAccount(account_id="acct-a", ssh_alias="gate1-harry")
    policy = SlurmPolicy(
        partition="cpu2",
        cores=32,
        memory_gb=320,
        job_internal_procs=8,
        pool_target_per_account=10,
    )
    try:
        client.submit_worker(account=account, policy=policy)
    except SlurmClientError as exc:
        assert "missing spool_paths" in exc.message
    else:
        raise AssertionError("expected SlurmClientError")


def test_subprocess_slurm_client_bootstraps_once_per_account(monkeypatch: object) -> None:
    calls: list[list[str]] = []

    def _fake_run(args: list[str], capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        _ = capture_output, text, check
        calls.append(args)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="1001;cluster\n", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)

    client = SubprocessSlurmClient()
    account = WorkerAccount(
        account_id="acct-a",
        ssh_alias="gate1-harry",
        spool_paths=RemoteSpoolPaths(
            inbox="/home1/harry261/peetsfea-spool/inbox",
            claimed="/home1/harry261/peetsfea-spool/claimed",
            results="/home1/harry261/peetsfea-spool/results",
            failed="/home1/harry261/peetsfea-spool/failed",
        ),
    )
    policy = SlurmPolicy(
        partition="cpu2",
        cores=32,
        memory_gb=320,
        job_internal_procs=8,
        pool_target_per_account=10,
    )

    client.submit_worker(account=account, policy=policy)
    client.submit_worker(account=account, policy=policy)

    # first submit: ssh mkdir + rsync + ssh bootstrap + ssh sbatch
    # second submit: ssh sbatch only
    assert len(calls) == 5
    assert calls[0][0] == "ssh"
    assert calls[1][0] == "rsync"
    assert calls[2][0] == "ssh"
    assert calls[3][0] == "ssh"
    assert calls[4][0] == "ssh"
