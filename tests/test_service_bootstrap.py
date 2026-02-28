from __future__ import annotations

import shutil
import signal
from pathlib import Path

from peetsfea_runner.config import GateAccount, RemoteSpoolPaths, RunnerConfig, build_queue_dirs
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore
from peetsfea_runner.service import RunnerService


class _FakeUploadClient:
    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root
        self.upload_calls = 0

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def remote_file_exists(self, *, remote_host: str, remote_path: str) -> bool:
        _ = remote_host
        return self._local_remote_path(remote_path).exists()

    def upload_to_spool_inbox(self, *, local_path: Path, remote_host: str, remote_path: str) -> None:
        _ = remote_host
        self.upload_calls += 1
        target = self._local_remote_path(remote_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, target)


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    gate_account = GateAccount(
        account_id="acct-test",
        ssh_alias="gate-test",
        spool_paths=RemoteSpoolPaths(
            inbox="/remote/spool/inbox",
            claimed="/remote/spool/claimed",
            results="/remote/spool/results",
            failed="/remote/spool/failed",
        ),
    )
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=queue_dirs.state / "runner.duckdb",
        queue_dirs=queue_dirs,
        gate_account=gate_account,
    )


def test_service_bootstrap_creates_queue_directories(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config)

    service.ensure_runtime_directories()

    assert config.queue_dirs.incoming.exists()
    assert config.queue_dirs.pending.exists()
    assert config.queue_dirs.uploaded.exists()
    assert config.queue_dirs.done.exists()
    assert config.queue_dirs.failed.exists()
    assert config.queue_dirs.state.exists()

    service.close()


def test_service_stops_on_signal_handler(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config)

    service._handle_signal(signal.SIGTERM, None)

    assert service.stop_event.is_set() is True
    service.close()


def test_service_processes_pending_upload_in_same_loop(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    remote_root = tmp_path / "remote"
    upload_client = _FakeUploadClient(remote_root)
    service = RunnerService(config, upload_client=upload_client)

    service.ensure_runtime_directories()
    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    store.close()

    incoming_file = config.queue_dirs.incoming / "loop.aedt"
    incoming_file.write_text("payload")

    service.run(register_signals=False, max_loops=1)

    store = JobStore(config.duckdb_path)
    state = store.get_job_state("loop")
    store.close()

    assert state == JobState.UPLOADED.value
    assert upload_client.upload_calls == 1
    assert (config.queue_dirs.uploaded / "loop.aedt").exists()
    assert (remote_root / "remote" / "spool" / "inbox" / "loop" / "loop.aedt").exists()

    service.close()
