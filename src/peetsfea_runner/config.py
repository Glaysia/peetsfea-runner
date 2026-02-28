from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QueueDirs:
    inbox: Path
    staging: Path
    done: Path
    failed: Path


@dataclass(frozen=True)
class RunnerConfig:
    base_dir: Path
    poll_interval_sec: float
    idle_sleep_sec: float
    duckdb_path: Path
    queue_dirs: QueueDirs


def build_queue_dirs(base_dir: Path) -> QueueDirs:
    return QueueDirs(
        inbox=base_dir / "inbox",
        staging=base_dir / "staging",
        done=base_dir / "done",
        failed=base_dir / "failed",
    )
