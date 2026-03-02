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
from peetsfea_runner.reconciler import E_RECONCILE_DONE_ZIP_MISSING
from peetsfea_runner.slurm_pool import SlurmClientError
from peetsfea_runner.service import RunnerService
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


class _FakeUploadClient:
    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root
        self.upload_calls = 0
        self.remote_hosts: list[str] = []

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def remote_file_exists(self, *, remote_host: str, remote_path: str) -> bool:
        _ = remote_host
        return self._local_remote_path(remote_path).exists()

    def upload_to_spool_inbox(self, *, local_path: Path, remote_host: str, remote_path: str) -> None:
        self.remote_hosts.append(remote_host)
        self.upload_calls += 1
        target = self._local_remote_path(remote_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, target)


class _FakeSlurmClient:
    def __init__(self) -> None:
        self._jobs: dict[str, list[str]] = {}
        self._next_id = 1000
        self.submit_calls = 0
        self.cancel_calls = 0
        self.cancel_records: list[tuple[str, str]] = []
        self.query_fail_accounts: set[str] = set()

    def query_workers(self, *, account: WorkerAccount, policy: SlurmPolicy) -> list[str]:
        _ = policy
        if account.account_id in self.query_fail_accounts:
            raise SlurmClientError(f"query failed for {account.account_id}")
        return list(self._jobs.get(account.account_id, []))

    def submit_worker(self, *, account: WorkerAccount, policy: SlurmPolicy) -> str:
        _ = policy
        self.submit_calls += 1
        self._next_id += 1
        job_id = str(self._next_id)
        self._jobs.setdefault(account.account_id, []).append(job_id)
        return job_id

    def cancel_worker(self, *, account: WorkerAccount, slurm_job_id: str) -> None:
        self.cancel_calls += 1
        self.cancel_records.append((account.account_id, slurm_job_id))
        current = self._jobs.get(account.account_id, [])
        self._jobs[account.account_id] = [job for job in current if job != slurm_job_id]


class _FakeResultsClient:
    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root
        self.download_calls = 0

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def list_result_zips(self, *, remote_host: str, remote_results_path: str) -> list[str]:
        _ = remote_host
        base = self._local_remote_path(remote_results_path)
        if not base.exists():
            return []
        paths: list[str] = []
        for file_path in sorted(base.rglob("*.reports.zip")):
            relative = file_path.relative_to(base).as_posix()
            paths.append(f"{remote_results_path.rstrip('/')}/{relative}")
        return paths

    def download_result_zip(self, *, remote_host: str, remote_path: str, local_path: Path) -> None:
        _ = remote_host
        self.download_calls += 1
        source = self._local_remote_path(remote_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, local_path)


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    gate_accounts = (
        GateAccount(
            account_id="acct-a",
            ssh_alias="gate-a",
            spool_paths=RemoteSpoolPaths(
                inbox="/remote/acct-a/spool/inbox",
                claimed="/remote/acct-a/spool/claimed",
                results="/remote/acct-a/spool/results",
                failed="/remote/acct-a/spool/failed",
            ),
        ),
        GateAccount(
            account_id="acct-b",
            ssh_alias="gate-b",
            spool_paths=RemoteSpoolPaths(
                inbox="/remote/acct-b/spool/inbox",
                claimed="/remote/acct-b/spool/claimed",
                results="/remote/acct-b/spool/results",
                failed="/remote/acct-b/spool/failed",
            ),
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
        gate_account=gate_accounts[0],
        gate_accounts=gate_accounts,
        worker_accounts=tuple(
            WorkerAccount(account_id=account.account_id, ssh_alias=account.ssh_alias)
            for account in gate_accounts
        ),
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


def test_service_stop_cancels_all_active_workers(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    slurm_client = _FakeSlurmClient()
    slurm_client._jobs["acct-a"] = ["2001", "2002"]
    slurm_client._jobs["acct-b"] = ["3001"]
    service = RunnerService(config, slurm_client=slurm_client)

    service.ensure_runtime_directories()
    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    store.close()

    service._handle_signal(signal.SIGTERM, None)
    service.run(register_signals=False, max_loops=1)

    assert slurm_client.cancel_calls == 3
    assert slurm_client.cancel_records == [
        ("acct-a", "2001"),
        ("acct-a", "2002"),
        ("acct-b", "3001"),
    ]
    assert slurm_client._jobs["acct-a"] == []
    assert slurm_client._jobs["acct-b"] == []

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
    assert (
        (remote_root / "remote" / "acct-a" / "spool" / "inbox" / "loop" / "loop.aedt").exists()
        or (remote_root / "remote" / "acct-b" / "spool" / "inbox" / "loop" / "loop.aedt").exists()
    )
    assert slurm_client.submit_calls == 20

    service.close()


def test_service_routes_upload_to_healthy_account_when_one_is_degraded(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    remote_root = tmp_path / "remote"
    upload_client = _FakeUploadClient(remote_root)
    slurm_client = _FakeSlurmClient()
    slurm_client.submit_calls = 0
    slurm_client._jobs["acct-b"] = ["3001"] * 10
    slurm_client._jobs["acct-a"] = ["2001"] * 10
    slurm_client.query_fail_accounts.add("acct-a")
    service = RunnerService(config, upload_client=upload_client, slurm_client=slurm_client)

    service.ensure_runtime_directories()
    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    store.close()

    incoming_file = config.queue_dirs.incoming / "route.aedt"
    incoming_file.write_text("payload")

    service.run(register_signals=False, max_loops=1)

    assert upload_client.upload_calls == 1
    assert upload_client.remote_hosts == ["gate-b"]
    assert (remote_root / "remote" / "acct-b" / "spool" / "inbox" / "route" / "route.aedt").exists()

    service.close()


def test_service_reconciles_done_job_with_missing_zip_on_startup(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config, slurm_client=_FakeSlurmClient())
    service.ensure_runtime_directories()

    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    inserted = store.insert_job(
        task_id="done-missing",
        filename="done-missing.aedt",
        source_path=str(config.queue_dirs.incoming / "done-missing.aedt"),
        pending_path=str(config.queue_dirs.pending / "done-missing.aedt"),
        state=JobState.DONE,
    )
    assert inserted is True
    store.close()

    service.run(register_signals=False, max_loops=1)

    store = JobStore(config.duckdb_path)
    assert store.get_job_state("done-missing") == JobState.FAILED.value
    error = store.get_job_error("done-missing")
    assert error is not None
    assert error[0] == E_RECONCILE_DONE_ZIP_MISSING
    store.close()
    service.close()


def test_service_registers_orphan_done_zip_on_startup(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    service = RunnerService(config, slurm_client=_FakeSlurmClient())
    service.ensure_runtime_directories()

    orphan_zip = config.queue_dirs.done / "orphan.reports.zip"
    orphan_zip.write_text("zip")

    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    store.close()

    service.run(register_signals=False, max_loops=1)

    store = JobStore(config.duckdb_path)
    assert store.get_job_state("orphan") == JobState.DONE.value
    assert store.get_report_zip_local_path("orphan") == str(orphan_zip)
    store.close()
    service.close()


def test_service_collects_remote_result_zip_in_same_loop(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    remote_root = tmp_path / "remote"
    results_client = _FakeResultsClient(remote_root)
    slurm_client = _FakeSlurmClient()
    slurm_client._jobs["acct-a"] = ["2001"] * 10
    slurm_client._jobs["acct-b"] = ["3001"] * 10
    service = RunnerService(config, results_client=results_client, slurm_client=slurm_client)
    service.ensure_runtime_directories()

    store = JobStore(config.duckdb_path)
    store.initialize_schema()
    inserted = store.insert_job(
        task_id="collect-loop",
        filename="collect-loop.aedt",
        source_path=str(config.queue_dirs.incoming / "collect-loop.aedt"),
        pending_path=str(config.queue_dirs.pending / "collect-loop.aedt"),
        uploaded_path=str(config.queue_dirs.uploaded / "collect-loop.aedt"),
        state=JobState.UPLOADED,
    )
    assert inserted is True
    store.close()

    remote_zip = remote_root / "remote" / "acct-a" / "spool" / "results" / "collect-loop.reports.zip"
    remote_zip.parent.mkdir(parents=True, exist_ok=True)
    remote_zip.write_text("zip")

    service.run(register_signals=False, max_loops=1)

    local_zip = config.queue_dirs.done / "collect-loop.reports.zip"
    store = JobStore(config.duckdb_path)
    assert local_zip.exists() is True
    assert store.get_report_zip_local_path("collect-loop") == str(local_zip)
    assert store.get_report_zip_remote_path("collect-loop") == "/remote/acct-a/spool/results/collect-loop.reports.zip"
    store.close()
    service.close()
