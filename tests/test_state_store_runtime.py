from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from peetsfea_runner.state_store import StateStore


def test_in_memory_slot_lease_lifecycle() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run_01")
        store.create_slot_task(
            run_id="run_01",
            slot_id="slot_01",
            input_path="/tmp/input.aedt",
            output_path="/tmp/output.aedt.out",
            account_id=None,
        )

        leased = store.acquire_slot_lease(
            run_id="run_01",
            worker_id="worker_01",
            job_id="worker_01",
            account_id="account_01",
            slurm_job_id="12345",
            lease_token="lease-token",
            lease_ttl_seconds=120,
        )
        assert leased is not None
        assert leased["state"] == "LEASED"

        updated = store.update_slot_lease_state(
            run_id="run_01",
            lease_token="lease-token",
            state="RUNNING",
            extend_ttl_seconds=120,
        )
        assert updated is not None
        assert updated["state"] == "RUNNING"

        completed = store.clear_slot_lease(
            run_id="run_01",
            lease_token="lease-token",
            final_state="SUCCEEDED",
        )
        assert completed is not None
        assert completed["state"] == "SUCCEEDED"
        assert completed["lease_token"] is None


def test_ingest_candidate_rearm_rules() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()

        inserted = store.register_ingest_candidate(
            input_path="/tmp/example.aedt",
            ready_path="/tmp/example.aedt.ready",
            ready_present=True,
            ready_mode="SIDECAR",
            ready_error=None,
            ready_mtime_ns=10,
            file_size=100,
            file_mtime_ns=20,
        )
        assert inserted is True

        inserted_again = store.register_ingest_candidate(
            input_path="/tmp/example.aedt",
            ready_path="/tmp/example.aedt.ready",
            ready_present=True,
            ready_mode="SIDECAR",
            ready_error=None,
            ready_mtime_ns=10,
            file_size=100,
            file_mtime_ns=20,
        )
        assert inserted_again is False

        store.mark_ingest_state(input_path="/tmp/example.aedt", state="FAILED")
        rearmed = store.register_ingest_candidate(
            input_path="/tmp/example.aedt",
            ready_path="/tmp/example.aedt.ready",
            ready_present=True,
            ready_mode="SIDECAR",
            ready_error=None,
            ready_mtime_ns=30,
            file_size=100,
            file_mtime_ns=20,
        )
        assert rearmed is True


def test_clear_slot_lease_rejects_stale_token() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run_01")
        store.create_slot_task(
            run_id="run_01",
            slot_id="slot_01",
            input_path="/tmp/input.aedt",
            output_path="/tmp/output.aedt.out",
            account_id=None,
        )

        store.acquire_slot_lease(
            run_id="run_01",
            worker_id="worker_01",
            job_id="worker_01",
            account_id="account_01",
            slurm_job_id="12345",
            lease_token="live-token",
            lease_ttl_seconds=120,
        )
        assert (
            store.clear_slot_lease(
                run_id="run_01",
                lease_token="stale-token",
                final_state="SUCCEEDED",
            )
            is None
        )


def test_list_active_slurm_workers_includes_multiple_runs() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()

        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_a",
            job_id="job_01",
            attempt_no=1,
            account_id="acct_a",
            host_alias="host-a",
            slurm_job_id="1001",
            worker_state="RUNNING",
            slots_configured=4,
            backend="sshfs",
        )
        store.upsert_slurm_worker(
            run_id="run_02",
            worker_id="worker_b",
            job_id="job_02",
            attempt_no=1,
            account_id="acct_b",
            host_alias="host-b",
            slurm_job_id="1002",
            worker_state="PENDING",
            slots_configured=4,
            backend="sshfs",
        )

        active = store.list_active_slurm_workers()
        assert len(active) == 2
        assert {str(row["run_id"]) for row in active} == {"run_01", "run_02"}
        assert active[0]["run_id"] == "run_01"
        assert active[1]["run_id"] == "run_02"
        assert active[0]["host_alias"] == "host-a"
        assert active[1]["host_alias"] == "host-b"
        assert active[0]["slurm_job_id"] == "1001"
        assert active[1]["slurm_job_id"] == "1002"
        assert active[0]["account_id"] == "acct_a"
        assert active[1]["account_id"] == "acct_b"
        assert active[0]["worker_id"] == "worker_a"


def test_list_active_slurm_workers_filters_inactive_states() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()

        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_active",
            job_id="job_01",
            attempt_no=1,
            account_id="acct_a",
            host_alias="host-active",
            slurm_job_id="2001",
            worker_state="RUNNING",
            slots_configured=4,
            backend="sshfs",
        )
        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_inactive",
            job_id="job_02",
            attempt_no=1,
            account_id="acct_a",
            host_alias="host-inactive",
            slurm_job_id="2002",
            worker_state="COMPLETED",
            slots_configured=4,
            backend="sshfs",
        )

        active = store.list_active_slurm_workers()
        assert len(active) == 1
        assert active[0]["worker_id"] == "worker_active"
        assert active[0]["worker_state"] == "RUNNING"


def test_list_active_slurm_workers_does_not_merge_duplicates() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()

        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_a",
            job_id="job_01",
            attempt_no=1,
            account_id="acct_a",
            host_alias="host-a",
            slurm_job_id="3001",
            worker_state="RUNNING",
            slots_configured=4,
            backend="sshfs",
        )
        store.upsert_slurm_worker(
            run_id="run_02",
            worker_id="worker_b",
            job_id="job_02",
            attempt_no=1,
            account_id="acct_b",
            host_alias="host-b",
            slurm_job_id="3001",
            worker_state="RUNNING",
            slots_configured=4,
            backend="sshfs",
        )

        active = store.list_active_slurm_workers()
        assert len(active) == 2
        slurm_job_ids = [str(row["slurm_job_id"]) for row in active]
        assert slurm_job_ids == ["3001", "3001"]
        assert len({row["worker_id"] for row in active}) == 2


def test_list_active_slurm_workers_returns_copies() -> None:
    with TemporaryDirectory() as tmpdir:
        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()

        store.upsert_slurm_worker(
            run_id="run_01",
            worker_id="worker_a",
            job_id="job_01",
            attempt_no=1,
            account_id="acct_a",
            host_alias="host-a",
            slurm_job_id="4001",
            worker_state="RUNNING",
            slots_configured=4,
            backend="sshfs",
        )

        active = store.list_active_slurm_workers()
        assert len(active) == 1
        active[0]["worker_state"] = "CORRUPTED"

        stored = store.list_slurm_workers(run_id="run_01")
        assert stored[0]["worker_state"] == "RUNNING"
