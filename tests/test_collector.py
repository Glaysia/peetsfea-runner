from __future__ import annotations

import shutil
from pathlib import Path

from peetsfea_runner.collector import ResultsClientError, ResultsCollector
from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.event_types import COLLECT_DONE, COLLECT_DUPLICATE_SKIPPED, COLLECT_STARTED
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


class FakeResultsClient:
    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root
        self.list_error: str | None = None
        self.download_error: str | None = None
        self.list_calls = 0
        self.download_calls = 0

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def list_result_zips(self, *, remote_host: str, remote_results_path: str) -> list[str]:
        _ = remote_host
        self.list_calls += 1
        if self.list_error is not None:
            raise ResultsClientError(self.list_error)

        base = self._local_remote_path(remote_results_path)
        if not base.exists():
            return []

        remote_paths: list[str] = []
        for file_path in sorted(base.rglob("*.reports.zip")):
            if not file_path.is_file():
                continue
            relative = file_path.relative_to(base).as_posix()
            remote_paths.append(f"{remote_results_path.rstrip('/')}/{relative}")
        return remote_paths

    def download_result_zip(self, *, remote_host: str, remote_path: str, local_path: Path) -> None:
        _ = remote_host
        self.download_calls += 1
        if self.download_error is not None:
            raise ResultsClientError(self.download_error)

        source = self._local_remote_path(remote_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, local_path)


def _build_config(tmp_path: Path) -> RunnerConfig:
    base_dir = tmp_path / "var"
    queue_dirs = build_queue_dirs(base_dir)
    gate_account = GateAccount(
        account_id="acct-test",
        ssh_alias="gate-test",
        spool_paths=RemoteSpoolPaths(
            inbox="/spool/inbox",
            claimed="/spool/claimed",
            results="/spool/results",
            failed="/spool/failed",
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
        gate_accounts=(gate_account,),
        worker_accounts=(WorkerAccount(account_id=gate_account.account_id, ssh_alias=gate_account.ssh_alias),),
        slurm_policy=slurm_policy,
    )


def _mkdir_queue_dirs(config: RunnerConfig) -> None:
    config.base_dir.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.incoming.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.pending.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.uploaded.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.done.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.failed.mkdir(parents=True, exist_ok=True)
    config.queue_dirs.state.mkdir(parents=True, exist_ok=True)


def test_collector_downloads_remote_result_and_updates_existing_job(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    inserted = store.insert_job(
        task_id="collect-1",
        filename="collect-1.aedt",
        source_path=str(config.queue_dirs.incoming / "collect-1.aedt"),
        pending_path=str(config.queue_dirs.pending / "collect-1.aedt"),
        uploaded_path=str(config.queue_dirs.uploaded / "collect-1.aedt"),
        state=JobState.UPLOADED,
    )
    assert inserted is True

    remote_root = tmp_path / "remote"
    remote_zip = remote_root / "spool" / "results" / "collect-1.reports.zip"
    remote_zip.parent.mkdir(parents=True, exist_ok=True)
    remote_zip.write_text("zip")

    client = FakeResultsClient(remote_root)
    collector = ResultsCollector(config, store, client=client)
    processed = collector.process_once()

    local_zip = config.queue_dirs.done / "collect-1.reports.zip"
    assert processed == 1
    assert local_zip.exists()
    assert store.get_job_state("collect-1") == JobState.UPLOADED.value
    assert store.get_report_zip_local_path("collect-1") == str(local_zip)
    assert store.get_report_zip_remote_path("collect-1") == "/spool/results/collect-1.reports.zip"

    events = [event[0] for event in store.get_task_events("collect-1")]
    assert COLLECT_STARTED in events
    assert COLLECT_DONE in events
    store.close()


def test_collector_skips_download_when_local_done_zip_exists(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    inserted = store.insert_job(
        task_id="collect-dup",
        filename="collect-dup.aedt",
        source_path=str(config.queue_dirs.incoming / "collect-dup.aedt"),
        pending_path=str(config.queue_dirs.pending / "collect-dup.aedt"),
        uploaded_path=str(config.queue_dirs.uploaded / "collect-dup.aedt"),
        state=JobState.UPLOADED,
    )
    assert inserted is True

    local_zip = config.queue_dirs.done / "collect-dup.reports.zip"
    local_zip.write_text("existing")

    remote_root = tmp_path / "remote"
    remote_zip = remote_root / "spool" / "results" / "collect-dup.reports.zip"
    remote_zip.parent.mkdir(parents=True, exist_ok=True)
    remote_zip.write_text("remote")

    client = FakeResultsClient(remote_root)
    collector = ResultsCollector(config, store, client=client)
    processed = collector.process_once()

    assert processed == 1
    assert client.download_calls == 0
    assert store.get_report_zip_local_path("collect-dup") == str(local_zip)
    assert store.get_report_zip_remote_path("collect-dup") == "/spool/results/collect-dup.reports.zip"

    events = [event[0] for event in store.get_task_events("collect-dup")]
    assert COLLECT_DUPLICATE_SKIPPED in events
    store.close()


def test_collector_registers_done_job_when_result_is_orphaned(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    remote_root = tmp_path / "remote"
    remote_zip = remote_root / "spool" / "results" / "orphan-collect.reports.zip"
    remote_zip.parent.mkdir(parents=True, exist_ok=True)
    remote_zip.write_text("zip")

    client = FakeResultsClient(remote_root)
    collector = ResultsCollector(config, store, client=client)
    processed = collector.process_once()

    local_zip = config.queue_dirs.done / "orphan-collect.reports.zip"
    assert processed == 1
    assert local_zip.exists()
    assert store.get_job_state("orphan-collect") == JobState.DONE.value
    assert store.get_report_zip_local_path("orphan-collect") == str(local_zip)
    assert store.get_report_zip_remote_path("orphan-collect") == "/spool/results/orphan-collect.reports.zip"
    store.close()
