from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.service import run_daemon

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_ROOT / "var"
QUEUE_DIRS = build_queue_dirs(BASE_DIR)

ACCOUNTS: tuple[GateAccount, ...] = (
    GateAccount(
        account_id="account-01",
        ssh_alias="gate-account-01",
        spool_paths=RemoteSpoolPaths(
            inbox="/srv/peetsfea/account-01/spool/inbox",
            claimed="/srv/peetsfea/account-01/spool/claimed",
            results="/srv/peetsfea/account-01/spool/results",
            failed="/srv/peetsfea/account-01/spool/failed",
        ),
    ),
    GateAccount(
        account_id="account-02",
        ssh_alias="gate-account-02",
        spool_paths=RemoteSpoolPaths(
            inbox="/srv/peetsfea/account-02/spool/inbox",
            claimed="/srv/peetsfea/account-02/spool/claimed",
            results="/srv/peetsfea/account-02/spool/results",
            failed="/srv/peetsfea/account-02/spool/failed",
        ),
    ),
    GateAccount(
        account_id="account-03",
        ssh_alias="gate-account-03",
        spool_paths=RemoteSpoolPaths(
            inbox="/srv/peetsfea/account-03/spool/inbox",
            claimed="/srv/peetsfea/account-03/spool/claimed",
            results="/srv/peetsfea/account-03/spool/results",
            failed="/srv/peetsfea/account-03/spool/failed",
        ),
    ),
    GateAccount(
        account_id="account-04",
        ssh_alias="gate-account-04",
        spool_paths=RemoteSpoolPaths(
            inbox="/srv/peetsfea/account-04/spool/inbox",
            claimed="/srv/peetsfea/account-04/spool/claimed",
            results="/srv/peetsfea/account-04/spool/results",
            failed="/srv/peetsfea/account-04/spool/failed",
        ),
    ),
    GateAccount(
        account_id="account-05",
        ssh_alias="gate-account-05",
        spool_paths=RemoteSpoolPaths(
            inbox="/srv/peetsfea/account-05/spool/inbox",
            claimed="/srv/peetsfea/account-05/spool/claimed",
            results="/srv/peetsfea/account-05/spool/results",
            failed="/srv/peetsfea/account-05/spool/failed",
        ),
    ),
)

WORKER_ACCOUNTS: tuple[WorkerAccount, ...] = tuple(
    WorkerAccount(account_id=account.account_id, ssh_alias=account.ssh_alias)
    for account in ACCOUNTS
)

SLURM_POLICY = SlurmPolicy(
    partition="cpu2",
    cores=32,
    memory_gb=320,
    job_internal_procs=8,
    pool_target_per_account=10,
)

CONFIG = RunnerConfig(
    base_dir=BASE_DIR,
    poll_interval_sec=1.0,
    idle_sleep_sec=5.0,
    duckdb_path=QUEUE_DIRS.state / "runner.duckdb",
    queue_dirs=QUEUE_DIRS,
    gate_account=ACCOUNTS[0],
    worker_accounts=WORKER_ACCOUNTS,
    slurm_policy=SLURM_POLICY,
)


def main() -> None:
    run_daemon(CONFIG)


if __name__ == "__main__":
    main()
