from __future__ import annotations

import shutil
from pathlib import Path

from peetsfea_runner.config import (
    GateAccount,
    RemoteSpoolPaths,
    RunnerConfig,
    SlurmPolicy,
    WorkerAccount,
    build_queue_dirs,
)
from peetsfea_runner.state import JobState
from peetsfea_runner.store import JobStore
from peetsfea_runner.uploader import (
    E_UPLOAD_LOCAL_MOVE,
    E_UPLOAD_NETWORK,
    E_UPLOAD_PERMISSION,
    E_UPLOAD_REMOTE_PATH,
    UploadClientError,
    UploadDispatcher,
)


class FakeUploadClient:
    def __init__(self, remote_root: Path) -> None:
        self._remote_root = remote_root
        self.exists_error: str | None = None
        self.upload_error: str | None = None
        self.exists_calls = 0
        self.upload_calls = 0
        self.remote_hosts: list[str] = []

    def _local_remote_path(self, remote_path: str) -> Path:
        return self._remote_root / remote_path.lstrip("/")

    def remote_file_exists(self, *, remote_host: str, remote_path: str) -> bool:
        _ = remote_host
        self.exists_calls += 1
        if self.exists_error is not None:
            raise UploadClientError(self.exists_error)
        return self._local_remote_path(remote_path).exists()

    def upload_to_spool_inbox(self, *, local_path: Path, remote_host: str, remote_path: str) -> None:
        self.remote_hosts.append(remote_host)
        self.upload_calls += 1
        if self.upload_error is not None:
            raise UploadClientError(self.upload_error)

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


def _insert_pending_job(store: JobStore, *, task_id: str, pending_file: Path) -> None:
    inserted = store.insert_job(
        task_id=task_id,
        filename=pending_file.name,
        source_path=str(pending_file),
        pending_path=str(pending_file),
        state=JobState.PENDING,
    )
    assert inserted is True


def test_upload_dispatcher_moves_pending_to_uploaded_and_marks_uploaded(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "ok.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="ok", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    dispatcher = UploadDispatcher(config, store, client=client)

    processed = dispatcher.process_once()

    uploaded_file = config.queue_dirs.uploaded / "ok.aedt"
    remote_file = remote_root / "spool" / "inbox" / "ok" / "ok.aedt"

    assert processed == 1
    assert not pending_file.exists()
    assert uploaded_file.exists()
    assert remote_file.exists()
    assert store.get_job_state("ok") == JobState.UPLOADED.value

    events = [event[0] for event in store.get_task_events("ok")]
    assert "UPLOAD_STARTED" in events
    assert "UPLOAD_DONE" in events
    store.close()


def test_upload_dispatcher_sets_failed_upload_on_network_error(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "network.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="network", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    client.upload_error = "ssh: connect to host gate-test port 22: Connection timed out"
    dispatcher = UploadDispatcher(config, store, client=client)

    processed = dispatcher.process_once()

    assert processed == 1
    assert pending_file.exists()
    assert store.get_job_state("network") == JobState.FAILED_UPLOAD.value

    error = store.get_job_error("network")
    assert error is not None
    assert error[0] == E_UPLOAD_NETWORK
    store.close()


def test_upload_dispatcher_classifies_permission_error(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "perm.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="perm", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    client.upload_error = "scp: /spool/inbox/perm/perm.aedt: Permission denied"
    dispatcher = UploadDispatcher(config, store, client=client)

    dispatcher.process_once()

    assert store.get_job_state("perm") == JobState.FAILED_UPLOAD.value
    error = store.get_job_error("perm")
    assert error is not None
    assert error[0] == E_UPLOAD_PERMISSION
    store.close()


def test_upload_dispatcher_classifies_remote_path_error(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "path.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="path", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    client.upload_error = "scp: /spool/inbox/path/path.aedt: No such file or directory"
    dispatcher = UploadDispatcher(config, store, client=client)

    dispatcher.process_once()

    assert store.get_job_state("path") == JobState.FAILED_UPLOAD.value
    error = store.get_job_error("path")
    assert error is not None
    assert error[0] == E_UPLOAD_REMOTE_PATH
    store.close()


def test_upload_dispatcher_recovers_when_remote_already_exists_without_reupload(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "already.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="already", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    remote_file = remote_root / "spool" / "inbox" / "already" / "already.aedt"
    remote_file.parent.mkdir(parents=True, exist_ok=True)
    remote_file.write_text("uploaded")

    client = FakeUploadClient(remote_root)
    dispatcher = UploadDispatcher(config, store, client=client)

    processed = dispatcher.process_once()

    assert processed == 1
    assert client.upload_calls == 0
    assert store.get_job_state("already") == JobState.UPLOADED.value
    assert (config.queue_dirs.uploaded / "already.aedt").exists()

    processed_again = dispatcher.process_once()
    assert processed_again == 0
    assert client.upload_calls == 0
    store.close()


def test_upload_dispatcher_resumes_upload_from_uploaded_when_pending_missing(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    uploaded_file = config.queue_dirs.uploaded / "resume_up.aedt"
    uploaded_file.write_text("payload")
    pending_file = config.queue_dirs.pending / "resume_up.aedt"

    inserted = store.insert_job(
        task_id="resume_up",
        filename="resume_up.aedt",
        source_path=str(config.queue_dirs.incoming / "resume_up.aedt"),
        pending_path=str(pending_file),
        uploaded_path=str(uploaded_file),
        state=JobState.PENDING,
    )
    assert inserted is True

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    dispatcher = UploadDispatcher(config, store, client=client)

    processed = dispatcher.process_once()

    assert processed == 1
    assert client.upload_calls == 1
    assert store.get_job_state("resume_up") == JobState.UPLOADED.value
    assert (remote_root / "spool" / "inbox" / "resume_up" / "resume_up.aedt").exists()

    events = [event[0] for event in store.get_task_events("resume_up")]
    assert "UPLOAD_RESUME_FROM_UPLOADED" in events
    assert "UPLOAD_DONE" in events
    store.close()


def test_upload_dispatcher_marks_failed_upload_when_local_move_fails(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    config = _build_config(tmp_path)
    _mkdir_queue_dirs(config)

    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "move_fail.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="move_fail", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    dispatcher = UploadDispatcher(config, store, client=client)

    uploaded_file = config.queue_dirs.uploaded / "move_fail.aedt"
    original_rename = Path.rename

    def _patched_rename(self: Path, target: Path) -> Path:
        if self == pending_file and target == uploaded_file:
            raise OSError("forced local move failure")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", _patched_rename)

    processed = dispatcher.process_once()

    assert processed == 1
    assert store.get_job_state("move_fail") == JobState.FAILED_UPLOAD.value
    error = store.get_job_error("move_fail")
    assert error is not None
    assert error[0] == E_UPLOAD_LOCAL_MOVE
    assert pending_file.exists()
    assert (remote_root / "spool" / "inbox" / "move_fail" / "move_fail.aedt").exists()
    store.close()


def test_upload_dispatcher_uses_preferred_healthy_account_for_routing(tmp_path: Path) -> None:
    base_config = _build_config(tmp_path)
    account_a = base_config.gate_account
    account_b = GateAccount(
        account_id="acct-b",
        ssh_alias="gate-b",
        spool_paths=RemoteSpoolPaths(
            inbox="/spool-b/inbox",
            claimed="/spool-b/claimed",
            results="/spool-b/results",
            failed="/spool-b/failed",
        ),
    )
    config = RunnerConfig(
        base_dir=base_config.base_dir,
        poll_interval_sec=base_config.poll_interval_sec,
        idle_sleep_sec=base_config.idle_sleep_sec,
        duckdb_path=base_config.duckdb_path,
        queue_dirs=base_config.queue_dirs,
        gate_account=account_a,
        gate_accounts=(account_a, account_b),
        worker_accounts=base_config.worker_accounts,
        slurm_policy=base_config.slurm_policy,
    )

    _mkdir_queue_dirs(config)
    store = JobStore(config.duckdb_path)
    store.initialize_schema()

    pending_file = config.queue_dirs.pending / "route_pref.aedt"
    pending_file.write_text("payload")
    _insert_pending_job(store, task_id="route_pref", pending_file=pending_file)

    remote_root = tmp_path / "remote"
    client = FakeUploadClient(remote_root)
    dispatcher = UploadDispatcher(config, store, client=client)

    processed = dispatcher.process_once(preferred_accounts=(account_b,))

    assert processed == 1
    assert client.remote_hosts == ["gate-b"]
    assert store.get_job_state("route_pref") == JobState.UPLOADED.value
    assert (remote_root / "spool-b" / "inbox" / "route_pref" / "route_pref.aedt").exists()
    store.close()
