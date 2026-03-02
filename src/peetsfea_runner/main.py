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
        account_id="harry261",
        ssh_alias="gate1-harry",
        spool_paths=RemoteSpoolPaths(
            inbox="/home1/harry261/peetsfea-spool/inbox",
            claimed="/home1/harry261/peetsfea-spool/claimed",
            results="/home1/harry261/peetsfea-spool/results",
            failed="/home1/harry261/peetsfea-spool/failed",
        ),
    ),
)

WORKER_ACCOUNTS: tuple[WorkerAccount, ...] = tuple(
    WorkerAccount(
        account_id=account.account_id,
        ssh_alias=account.ssh_alias,
        spool_paths=account.spool_paths,
    )
    for account in ACCOUNTS
)

SLURM_POLICY = SlurmPolicy(
    partition="cpu2",
    cores=32,
    memory_gb=320,
    job_internal_procs=8,
    pool_target_per_account=10,
    repo_url="https://github.com/Glaysia/peetsfea-runner",
    release_tag="v2026.03.02-gate1-r1",
)

CONFIG = RunnerConfig(
    base_dir=BASE_DIR,
    poll_interval_sec=1.0,
    idle_sleep_sec=5.0,
    duckdb_path=QUEUE_DIRS.state / "runner.duckdb",
    queue_dirs=QUEUE_DIRS,
    gate_account=ACCOUNTS[0],
    gate_accounts=ACCOUNTS,
    worker_accounts=WORKER_ACCOUNTS,
    slurm_policy=SLURM_POLICY,
)


def main() -> None:
    run_daemon(CONFIG)


if __name__ == "__main__":
    main()
