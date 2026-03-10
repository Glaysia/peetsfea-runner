from __future__ import annotations

from dataclasses import dataclass
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from peetsfea_runner.scheduler import (
    AccountCapacitySnapshot,
    AccountReadinessSnapshot,
    bootstrap_account_runtime,
    JobSpec,
    SlotTaskRef,
    calculate_effective_slots,
    parse_squeue_state_counts,
    pick_balanced_account,
    query_account_preflight,
    query_account_readiness,
    query_account_capacity,
    run_jobs_with_slots,
    run_slot_bundles,
    run_slot_workers,
)


@dataclass(slots=True)
class _Account:
    account_id: str
    host_alias: str
    max_jobs: int


@dataclass(slots=True)
class _WorkerOutcome:
    name: str
    requeue_slots: tuple[SlotTaskRef, ...] = ()

    @property
    def terminal_worker(self) -> bool:
        return bool(self.requeue_slots)


class TestScheduler(unittest.TestCase):
    def test_effective_slots_uses_max_jobs_per_account(self) -> None:
        slots = calculate_effective_slots(max_jobs_per_account=10)
        self.assertEqual(slots, 10)

    def test_run_jobs_with_slots_caps_max_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            jobs = [
                JobSpec(
                    job_id=f"job_{idx:04d}",
                    job_index=idx,
                    input_path=base / f"file_{idx}.aedt",
                    relative_path=Path(f"file_{idx}.aedt"),
                    output_dir=base / f"out_{idx}.aedt_all",
                    account_id="account_01",
                    host_alias="gate1-harry",
                )
                for idx in range(1, 81)
            ]

            def _worker(job: JobSpec) -> str:
                time.sleep(0.02)
                return job.job_id

            batch = run_jobs_with_slots(job_specs=jobs, max_slots=10, worker=_worker)
            self.assertEqual(len(batch.results), 80)
            self.assertLessEqual(batch.max_inflight, 10)

    def test_run_jobs_with_slots_rejects_invalid_slots(self) -> None:
        with self.assertRaises(ValueError):
            run_jobs_with_slots(job_specs=[], max_slots=0, worker=lambda _job: None)

    def test_parse_squeue_state_counts(self) -> None:
        running, pending, counts = parse_squeue_state_counts(["R", "PD", "CG", "R", "X"])
        self.assertEqual(running, 3)
        self.assertEqual(pending, 1)
        self.assertEqual(counts["R"], 2)
        self.assertEqual(counts["PD"], 1)

    def test_query_account_capacity_uses_running_and_pending_limits(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)

        snapshot = query_account_capacity(
            account=account,
            pending_buffer_per_account=3,
            run_command=lambda _cmd: (0, "R\nPD\nCG\nPD\n", ""),
        )

        self.assertEqual(snapshot.running_count, 2)
        self.assertEqual(snapshot.pending_count, 2)
        self.assertEqual(snapshot.allowed_submit, (10 - 2) + (3 - 2))

    def test_query_account_capacity_raises_on_command_timeout(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        with patch("peetsfea_runner.scheduler.subprocess.run") as run_mock:
            run_mock.side_effect = subprocess.TimeoutExpired(cmd="ssh host-1", timeout=8)
            with self.assertRaises(RuntimeError):
                query_account_capacity(account=account, pending_buffer_per_account=3)

    def test_query_account_readiness_reports_ready_snapshot(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 venv=1 python=1 module=1 binaries=1 ansys=1\n",
                "",
            ),
        )
        self.assertEqual(
            snapshot,
            AccountReadinessSnapshot(
                account_id="a1",
                host_alias="host-1",
                ready=True,
                status="READY",
                reason="ok",
                home_ok=True,
                runtime_path_ok=True,
                venv_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
            ),
        )

    def test_query_account_readiness_reports_blocked_checks(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 venv=0 python=0 module=1 binaries=1 ansys=0\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "DISABLED_FOR_DISPATCH")
        self.assertEqual(snapshot.reason, "venv,python,ansys")

    def test_query_account_readiness_marks_bootstrap_required_for_missing_runtime(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 venv=0 python=0 module=1 binaries=1 ansys=1\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BOOTSTRAP_REQUIRED")
        self.assertEqual(snapshot.reason, "venv,python")

    def test_query_account_preflight_reports_missing_uv_and_pyaedt(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 venv=1 python=1 module=1 binaries=1 ansys=1 uv=0 pyaedt=0\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "PREFLIGHT_FAILED")
        self.assertEqual(snapshot.reason, "uv,pyaedt")
        self.assertFalse(snapshot.uv_ok)
        self.assertFalse(snapshot.pyaedt_ok)

    def test_bootstrap_account_runtime_requires_success_marker(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        output = bootstrap_account_runtime(
            account=account,
            run_command=lambda _cmd: (0, "__PEETSFEA_BOOTSTRAP__:ok\n", ""),
        )
        self.assertIn("__PEETSFEA_BOOTSTRAP__:ok", output)

    def test_pick_balanced_account_uses_score_then_running_then_account_id(self) -> None:
        capacities = [
            AccountCapacitySnapshot("a1", "h1", running_count=3, pending_count=0, allowed_submit=1),
            AccountCapacitySnapshot("a2", "h2", running_count=2, pending_count=0, allowed_submit=1),
            AccountCapacitySnapshot("a3", "h3", running_count=1, pending_count=0, allowed_submit=0),
        ]
        selected = pick_balanced_account(
            capacities=capacities,
            completed_slots_by_account={"a1": 2, "a2": 2, "a3": 0},
            inflight_slots_by_account={"a1": 0, "a2": 0, "a3": 0},
        )
        assert selected is not None
        self.assertEqual(selected.account_id, "a2")

    def test_run_slot_bundles_chunks_by_slots_per_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 18)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            batch = run_slot_bundles(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.slot_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 99),
                max_workers=2,
            )

            self.assertEqual(sorted(batch.results), [1, 8, 8])
            self.assertEqual(batch.submitted_jobs, 3)

    def test_run_slot_bundles_skips_blocked_account(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 5)
            ]
            accounts = [
                _Account(account_id="a1", host_alias="h1", max_jobs=10),
                _Account(account_id="a2", host_alias="h2", max_jobs=10),
            ]

            def _capacity_lookup(*, account, pending_buffer_per_account):
                if account.account_id == "a1":
                    return AccountCapacitySnapshot("a1", "h1", 10, 3, 0)
                return AccountCapacitySnapshot("a2", "h2", 0, 0, 10 + pending_buffer_per_account)

            batch = run_slot_bundles(
                slot_queue=slots,
                accounts=accounts,
                slots_per_job=2,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.account_id,
                capacity_lookup=_capacity_lookup,
                max_workers=2,
            )

            self.assertEqual(batch.results, ["a2", "a2"])

    def test_run_slot_workers_caps_submitted_jobs_at_account_max(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 86)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            batch = run_slot_workers(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.slot_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 10),
            )

            self.assertEqual(batch.submitted_jobs, 11)
            self.assertEqual(sorted(batch.results), [5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8])
            self.assertLessEqual(batch.max_inflight_jobs, 10)

    def test_run_slot_workers_respects_existing_running_and_pending_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 13)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            batch = run_slot_workers(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.slot_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 7, 1, 2),
            )

            self.assertEqual(batch.submitted_jobs, 10)
            self.assertEqual(sum(batch.results), 12)
            self.assertLessEqual(batch.max_inflight_jobs, 2)

    def test_run_slot_workers_submits_replacement_bundle_from_backlog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 18)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=2)

            batch = run_slot_workers(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.slot_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 2),
                max_workers=2,
            )

            self.assertEqual(batch.submitted_jobs, 3)
            self.assertEqual(sorted(batch.results), [1, 8, 8])
            self.assertLessEqual(batch.max_inflight_jobs, 2)

    def test_run_slot_workers_waits_until_worker_slot_is_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id="w_0001",
                    input_path=root / "in_1.aedt",
                    relative_path=Path("in_1.aedt"),
                    output_dir=root / "out_1.aedt_all",
                )
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=1)
            snapshot_calls = {"count": 0}

            def _capacity_lookup(**_kwargs):
                snapshot_calls["count"] += 1
                if snapshot_calls["count"] == 1:
                    return AccountCapacitySnapshot("a1", "h1", 1, 0, 0)
                return AccountCapacitySnapshot("a1", "h1", 0, 0, 1)

            with patch("peetsfea_runner.scheduler.time.sleep") as mocked_sleep:
                batch = run_slot_workers(
                    slot_queue=slots,
                    accounts=[account],
                    slots_per_job=8,
                    pending_buffer_per_account=3,
                    worker=lambda bundle: bundle.slot_count,
                    capacity_lookup=_capacity_lookup,
                    idle_sleep_seconds=1.0,
                )

            self.assertGreaterEqual(mocked_sleep.call_count, 1)
            self.assertEqual(batch.submitted_jobs, 1)
            self.assertEqual(batch.results, [1])

    def test_run_slot_workers_recovers_terminal_worker_with_replacement_before_run_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            initial_slots = [
                SlotTaskRef(
                    run_id="run_01",
                    slot_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 3)
            ]
            recovery_slot = SlotTaskRef(
                run_id="run_01",
                slot_id="w_9999",
                input_path=root / "recovery.aedt",
                relative_path=Path("recovery.aedt"),
                output_dir=root / "recovery.aedt_all",
                attempt_no=2,
            )
            account = _Account(account_id="a1", host_alias="h1", max_jobs=2)
            submitted_job_ids: list[str] = []
            recovery_submitted = threading.Event()

            def _on_bundle_submitted(bundle):
                submitted_job_ids.append(bundle.job_id)
                if bundle.job_id == "job_0003":
                    recovery_submitted.set()

            def _worker(bundle):
                if bundle.job_id == "job_0001":
                    return _WorkerOutcome(name=bundle.job_id, requeue_slots=(recovery_slot,))
                if bundle.job_id == "job_0002":
                    self.assertTrue(recovery_submitted.wait(timeout=2))
                    return _WorkerOutcome(name=bundle.job_id)
                return _WorkerOutcome(name=bundle.job_id)

            batch = run_slot_workers(
                slot_queue=initial_slots,
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 2),
                max_workers=2,
                on_bundle_submitted=_on_bundle_submitted,
                recovery_slots_lookup=lambda _bundle, outcome: outcome.requeue_slots,
                terminal_bundle_lookup=lambda _bundle, outcome: outcome.terminal_worker,
            )

            self.assertEqual(submitted_job_ids, ["job_0001", "job_0002", "job_0003"])
            self.assertEqual(batch.submitted_jobs, 3)
            self.assertEqual(batch.terminal_jobs, 1)
            self.assertEqual(batch.replacement_jobs, 1)
            self.assertLessEqual(batch.max_inflight_jobs, 2)


if __name__ == "__main__":
    unittest.main()
