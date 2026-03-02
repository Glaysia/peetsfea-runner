from __future__ import annotations

import os
from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.event_types import (
    AEDT_DELETE_LOCAL_DONE,
    AEDT_RETENTION_VIOLATION_DETECTED,
    RECONCILE_DONE_ZIP_MISSING,
    RECONCILE_ORPHAN_DONE_ZIP_REGISTERED,
    RECONCILE_DONE_ZIP_RECOVERED,
    RECONCILE_PENDING_TO_UPLOADED,
    RECONCILE_PENDING_TTL_REQUEUE,
)
from peetsfea_runner.reconciler import (
    E_RECONCILE_AEDT_DELETE_FAILED,
    E_RECONCILE_DONE_ZIP_MISSING,
    OperationsReconciler,
    ReconcilePolicy,
)
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore


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


def test_reconciler_requeues_pending_when_ttl_exceeded(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "stale.aedt"
    pending_file.write_text("payload")
    os.utime(pending_file, (1, 1))
    inserted = store.insert_job(
        task_id="stale",
        filename="stale.aedt",
        source_path=str(config.queue_dirs.incoming / "stale.aedt"),
        pending_path=str(pending_file),
        state=JobState.PENDING,
    )
    assert inserted is True

    reconciler = OperationsReconciler(config, store, policy=ReconcilePolicy(pending_ttl_sec=1))
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("stale") == JobState.NEW.value
    assert (config.queue_dirs.incoming / "stale.aedt").exists()
    assert pending_file.exists() is False

    events = [event[0] for event in store.get_task_events("stale")]
    assert RECONCILE_PENDING_TTL_REQUEUE in events
    store.close()


def test_reconciler_registers_orphan_done_zip_into_db(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    orphan_zip = config.queue_dirs.done / "orphan.reports.zip"
    orphan_zip.write_text("zip")

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("orphan") == JobState.DONE.value
    assert store.get_report_zip_local_path("orphan") == str(orphan_zip)

    events = [event[0] for event in store.get_task_events("orphan")]
    assert RECONCILE_ORPHAN_DONE_ZIP_REGISTERED in events
    store.close()


def test_reconciler_promotes_pending_to_uploaded_when_uploaded_artifact_exists(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    uploaded_file = config.queue_dirs.uploaded / "sync.aedt"
    uploaded_file.write_text("payload")
    inserted = store.insert_job(
        task_id="sync",
        filename="sync.aedt",
        source_path=str(config.queue_dirs.incoming / "sync.aedt"),
        pending_path=str(config.queue_dirs.pending / "sync.aedt"),
        uploaded_path=str(uploaded_file),
        state=JobState.PENDING,
    )
    assert inserted is True

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("sync") == JobState.UPLOADED.value
    events = [event[0] for event in store.get_task_events("sync")]
    assert RECONCILE_PENDING_TO_UPLOADED in events
    store.close()


def test_reconciler_recovers_done_zip_path_from_done_directory(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    recovered_zip = config.queue_dirs.done / "done-1.reports.zip"
    recovered_zip.write_text("zip")
    inserted = store.insert_job(
        task_id="done-1",
        filename="done-1.aedt",
        source_path=str(config.queue_dirs.incoming / "done-1.aedt"),
        pending_path=str(config.queue_dirs.pending / "done-1.aedt"),
        state=JobState.DONE,
    )
    assert inserted is True

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("done-1") == JobState.DONE.value
    assert store.get_report_zip_local_path("done-1") == str(recovered_zip)
    events = [event[0] for event in store.get_task_events("done-1")]
    assert RECONCILE_DONE_ZIP_RECOVERED in events
    store.close()


def test_reconciler_marks_done_job_failed_when_zip_is_missing(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
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

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("done-missing") == JobState.FAILED.value
    error = store.get_job_error("done-missing")
    assert error is not None
    assert error[0] == E_RECONCILE_DONE_ZIP_MISSING
    events = [event[0] for event in store.get_task_events("done-missing")]
    assert RECONCILE_DONE_ZIP_MISSING in events
    store.close()


def test_reconciler_detects_and_deletes_lingering_aedt_for_done_job(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    report_zip = config.queue_dirs.done / "done-clean.reports.zip"
    report_zip.write_text("zip")
    lingering_aedt = config.queue_dirs.uploaded / "done-clean.aedt"
    lingering_aedt.write_text("payload")
    inserted = store.insert_job(
        task_id="done-clean",
        filename="done-clean.aedt",
        source_path=str(config.queue_dirs.incoming / "done-clean.aedt"),
        pending_path=str(config.queue_dirs.pending / "done-clean.aedt"),
        uploaded_path=str(lingering_aedt),
        report_zip_local_path=str(report_zip),
        state=JobState.DONE,
    )
    assert inserted is True

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("done-clean") == JobState.DONE.value
    assert lingering_aedt.exists() is False
    events = [event[0] for event in store.get_task_events("done-clean")]
    assert AEDT_RETENTION_VIOLATION_DETECTED in events
    assert AEDT_DELETE_LOCAL_DONE in events
    row = store.connection.execute(
        "SELECT local_aedt_deleted_ts FROM jobs WHERE task_id = ?",
        ["done-clean"],
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    store.close()


def test_reconciler_marks_failed_when_aedt_delete_fails(tmp_path: Path, monkeypatch: object) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    report_zip = config.queue_dirs.done / "done-delete-fail.reports.zip"
    report_zip.write_text("zip")
    lingering_aedt = config.queue_dirs.uploaded / "done-delete-fail.aedt"
    lingering_aedt.write_text("payload")
    inserted = store.insert_job(
        task_id="done-delete-fail",
        filename="done-delete-fail.aedt",
        source_path=str(config.queue_dirs.incoming / "done-delete-fail.aedt"),
        pending_path=str(config.queue_dirs.pending / "done-delete-fail.aedt"),
        uploaded_path=str(lingering_aedt),
        report_zip_local_path=str(report_zip),
        state=JobState.DONE,
    )
    assert inserted is True

    original_unlink = Path.unlink

    def _patched_unlink(self: Path, missing_ok: bool = False) -> None:
        if self == lingering_aedt:
            raise OSError("forced delete failure")
        original_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", _patched_unlink)

    reconciler = OperationsReconciler(config, store)
    processed = reconciler.process_once()

    assert processed == 1
    assert store.get_job_state("done-delete-fail") == JobState.FAILED.value
    error = store.get_job_error("done-delete-fail")
    assert error is not None
    assert error[0] == E_RECONCILE_AEDT_DELETE_FAILED
    store.close()
