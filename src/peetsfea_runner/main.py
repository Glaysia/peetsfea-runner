from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import RunnerConfig, build_queue_dirs
from peetsfea_runner.service import run_daemon

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_ROOT / "var"
QUEUE_DIRS = build_queue_dirs(BASE_DIR)

CONFIG = RunnerConfig(
    base_dir=BASE_DIR,
    poll_interval_sec=1.0,
    idle_sleep_sec=5.0,
    duckdb_path=BASE_DIR / "runner.duckdb",
    queue_dirs=QUEUE_DIRS,
)


def main() -> None:
    run_daemon(CONFIG)


if __name__ == "__main__":
    main()
