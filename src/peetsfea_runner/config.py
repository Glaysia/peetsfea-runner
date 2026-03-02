from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SlurmPolicy:
    partition: str
    cores: int
    memory_gb: int
    job_internal_procs: int
    pool_target_per_account: int
    job_name_prefix: str = "peetsfea-worker"


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
