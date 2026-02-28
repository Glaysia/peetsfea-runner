from __future__ import annotations

import shutil
import signal
from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.service import RunnerService
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


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


class _FakeSlurmClient:
    def __init__(self) -> None:
        self._jobs: dict[str, list[str]] = {}
        self._next_id = 1000
        self.submit_calls = 0

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        _ = policy
        return list(self._jobs.get(account.account_id, []))

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        _ = policy
        self.submit_calls += 1
        self._next_id += 1
        job_id = str(self._next_id)
        self._jobs.setdefault(account.account_id, []).append(job_id)
        return job_id

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        current = self._jobs.get(account.account_id, [])
        self._jobs[account.account_id] = [job for job in current if job != slurm_job_id]


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
    slurm_policy = SlurmPolicy(
        partition="cpu2",
        cores=32,
        memory_gb=320,
        job_internal_procs=8,
        pool_target_per_account=10,
    )
    return RunnerConfig(
        base_dir=base_dir,
        poll_interval_sec=0.01,
        idle_sleep_sec=0.01,
        duckdb_path=queue_dirs.state / "runner.duckdb",
        queue_dirs=queue_dirs,
        gate_account=gate_account,
        worker_accounts=(WorkerAccount(account_id=gate_account.account_id, ssh_alias=gate_account.ssh_alias),),
        slurm_policy=slurm_policy,
    )


def test_service_bootstrap_creates_queue_directories(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config, slurm_client=_FakeSlurmClient())

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
    service = RunnerService(config, slurm_client=_FakeSlurmClient())

    service._handle_signal(signal.SIGTERM, None)

    assert service.stop_event.is_set() is True
    service.close()


def test_service_processes_pending_upload_in_same_loop(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    remote_root = tmp_path / "remote"
    upload_client = _FakeUploadClient(remote_root)
    slurm_client = _FakeSlurmClient()
    service = RunnerService(config, upload_client=upload_client, slurm_client=slurm_client)

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
    assert slurm_client.submit_calls == 10

    service.close()
