from __future__ import annotations

import signal
import threading
from pathlib import Path

from peetsfea_runner.config import RunnerConfig
from peetsfea_runner.store import JobStore
from peetsfea_runner.watcher import QueueWatcher


class RunnerService:
    def __init__(self, config: RunnerConfig) -> None:
        self._config = config
        self._stop_event = threading.Event()
        self._store = JobStore(config.duckdb_path)
        self._watcher = QueueWatcher(config, self._store)

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    def _handle_signal(self, signum: int, _frame: object | None) -> None:
        _ = signum
        self._stop_event.set()

    def _register_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def ensure_runtime_directories(self) -> None:
        self._config.base_dir.mkdir(parents=True, exist_ok=True)
        queue_dirs: list[Path] = [
            self._config.queue_dirs.incoming,
            self._config.queue_dirs.pending,
            self._config.queue_dirs.uploaded,
            self._config.queue_dirs.done,
            self._config.queue_dirs.failed,
            self._config.queue_dirs.state,
        ]
        for directory in queue_dirs:
            directory.mkdir(parents=True, exist_ok=True)

    def run(self, *, register_signals: bool = True, max_loops: int | None = None) -> None:
        self.ensure_runtime_directories()
        self._store.initialize_schema()
        if register_signals:
            self._register_signal_handlers()

        loops = 0
        while not self._stop_event.is_set():
            processed = self._watcher.process_once()
            wait_sec = self._config.poll_interval_sec if processed else self._config.idle_sleep_sec
            self._stop_event.wait(wait_sec)

            loops += 1
            if max_loops is not None and loops >= max_loops:
                break

    def close(self) -> None:
        self._store.close()


def run_daemon(config: RunnerConfig) -> None:
    service = RunnerService(config)
    try:
        service.run(register_signals=True)
    finally:
        service.close()
