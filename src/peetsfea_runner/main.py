from __future__ import annotations

from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RunnerConfig,
    build_gate1_multi_runner_config,
)
from peetsfea_runner.service import run_daemon

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG: RunnerConfig = build_gate1_multi_runner_config(PROJECT_ROOT)
ACCOUNTS: tuple[GateAccount, ...] = CONFIG.gate_accounts


def main() -> None:
    run_daemon(CONFIG)


if __name__ == "__main__":
    main()
