from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import GateAccount, RemoteSpoolPaths, RunnerConfig, build_queue_dirs
from peetsfea_runner.service import run_daemon

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_ROOT / "var"
QUEUE_DIRS = build_queue_dirs(BASE_DIR)

GATE_ACCOUNT = GateAccount(
    account_id="account-01",
    ssh_alias="gate-account-01",
    spool_paths=RemoteSpoolPaths(
        inbox="/srv/peetsfea/spool/inbox",
        claimed="/srv/peetsfea/spool/claimed",
        results="/srv/peetsfea/spool/results",
        failed="/srv/peetsfea/spool/failed",
    ),
)

CONFIG = RunnerConfig(
    base_dir=BASE_DIR,
    poll_interval_sec=1.0,
    idle_sleep_sec=5.0,
    duckdb_path=QUEUE_DIRS.state / "runner.duckdb",
    queue_dirs=QUEUE_DIRS,
    gate_account=GATE_ACCOUNT,
)


def main() -> None:
    run_daemon(CONFIG)


if __name__ == "__main__":
    main()
