from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class SlurmPolicy:
    partition: str
    cores: int
    memory_gb: int
    job_internal_procs: int
    pool_target_per_account: int
    repo_url: str = "https://github.com/Glaysia/peetsfea-runner"
    release_tag: str = "v2026.03.02-gate1-r1"
    job_name_prefix: str = "peetsfea-worker"
    aedt_executable_path: str | None = None
    windows_launch_mode: Literal["interactive_task", "service"] = "interactive_task"
    windows_task_name: str = "peetsfea-worker-win5600x2"
    windows_interactive_user: str = r"DESKTOP-L5CB36B\5600x2"


@dataclass(frozen=True)
class RemoteSpoolPaths:
    inbox: str
    claimed: str
    results: str
    failed: str


@dataclass(frozen=True)
class GateAccount:
    account_id: str
    ssh_alias: str
    spool_paths: RemoteSpoolPaths


@dataclass(frozen=True)
class WorkerAccount:
    account_id: str
    ssh_alias: str
    spool_paths: RemoteSpoolPaths | None = None


@dataclass(frozen=True)
class QueueDirs:
    incoming: Path
    pending: Path
    uploaded: Path
    done: Path
    failed: Path
    state: Path


@dataclass(frozen=True)
class RunnerConfig:
    base_dir: Path
    poll_interval_sec: float
    idle_sleep_sec: float
    duckdb_path: Path
    queue_dirs: QueueDirs
    gate_account: GateAccount
    gate_accounts: tuple[GateAccount, ...]
    worker_accounts: tuple[WorkerAccount, ...]
    slurm_policy: SlurmPolicy


def build_queue_dirs(base_dir: Path) -> QueueDirs:
    return QueueDirs(
        incoming=base_dir / "incoming",
        pending=base_dir / "pending",
        uploaded=base_dir / "uploaded",
        done=base_dir / "done",
        failed=base_dir / "failed",
        state=base_dir / "state",
    )


def build_5600x2_runner_config(project_root: Path) -> RunnerConfig:
    base_dir = project_root / "var"
    queue_dirs = build_queue_dirs(base_dir)
    account = GateAccount(
        account_id="win5600x2",
        ssh_alias="5600X2",
        spool_paths=RemoteSpoolPaths(
            inbox="C:/peetsfea-spool/inbox",
            claimed="C:/peetsfea-spool/claimed",
            results="C:/peetsfea-spool/results",
            failed="C:/peetsfea-spool/failed",
        ),
    )
    worker_account = WorkerAccount(
        account_id=account.account_id,
        ssh_alias=account.ssh_alias,
        spool_paths=account.spool_paths,
    )
    slurm_policy = SlurmPolicy(
        partition="debug-windows",
        cores=6,
        memory_gb=16,
        job_internal_procs=6,
        pool_target_per_account=1,
        repo_url="https://github.com/Glaysia/peetsfea-runner",
        release_tag="v2026.03.02-5600x2-r4",
        aedt_executable_path=r"C:\Program Files\ANSYS Inc\v252\AnsysEM\ansysedt.exe",
        windows_launch_mode="interactive_task",
        windows_task_name="peetsfea-worker-win5600x2",
        windows_interactive_user=r"DESKTOP-L5CB36B\5600x2",
    )
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=1.0,
        idle_sleep_sec=5.0,
        duckdb_path=queue_dirs.state / "runner.duckdb",
        queue_dirs=queue_dirs,
        gate_account=account,
        gate_accounts=(account,),
        worker_accounts=(worker_account,),
        slurm_policy=slurm_policy,
    )
