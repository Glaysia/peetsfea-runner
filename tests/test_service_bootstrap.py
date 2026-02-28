from __future__ import annotations

import signal
from pathlib import Path

from peetsfea_runner.config import RunnerConfig, build_queue_dirs
from peetsfea_runner.service import RunnerService


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=base_dir / "runner.duckdb",
        queue_dirs=build_queue_dirs(base_dir),
    )


def test_service_bootstrap_creates_queue_directories(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config)

    service.ensure_runtime_directories()

    assert config.queue_dirs.inbox.exists()
    assert config.queue_dirs.staging.exists()
    assert config.queue_dirs.done.exists()
    assert config.queue_dirs.failed.exists()

    service.close()


def test_service_stops_on_signal_handler(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config)

    service._handle_signal(signal.SIGTERM, None)

    assert service.stop_event.is_set() is True
    service.close()
