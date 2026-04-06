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
