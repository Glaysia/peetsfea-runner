from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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


def build_queue_dirs(base_dir: Path) -> QueueDirs:
    return QueueDirs(
        incoming=base_dir / "incoming",
        pending=base_dir / "pending",
        uploaded=base_dir / "uploaded",
        done=base_dir / "done",
        failed=base_dir / "failed",
        state=base_dir / "state",
    )
