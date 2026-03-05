from __future__ import annotations

from dataclasses import dataclass
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from peetsfea_runner.scheduler import (
    AccountCapacitySnapshot,
    JobSpec,
    WindowTaskRef,
    calculate_effective_slots,
    parse_squeue_state_counts,
    pick_balanced_account,
    query_account_capacity,
    run_jobs_with_slots,
    run_window_bundles,
)


@dataclass(slots=True)
class _Account:
    account_id: str
    host_alias: str
    max_jobs: int


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

    def test_pick_balanced_account_uses_score_then_running_then_account_id(self) -> None:
        capacities = [
            AccountCapacitySnapshot("a1", "h1", running_count=3, pending_count=0, allowed_submit=1),
            AccountCapacitySnapshot("a2", "h2", running_count=2, pending_count=0, allowed_submit=1),
            AccountCapacitySnapshot("a3", "h3", running_count=1, pending_count=0, allowed_submit=0),
        ]
        selected = pick_balanced_account(
            capacities=capacities,
            completed_windows_by_account={"a1": 2, "a2": 2, "a3": 0},
            inflight_windows_by_account={"a1": 0, "a2": 0, "a3": 0},
        )
        assert selected is not None
        self.assertEqual(selected.account_id, "a2")

    def test_run_window_bundles_chunks_by_windows_per_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            windows = [
                WindowTaskRef(
                    run_id="run_01",
                    window_id=f"w_{idx:04d}",
                    input_path=root / f"in_{idx}.aedt",
                    relative_path=Path(f"in_{idx}.aedt"),
                    output_dir=root / f"out_{idx}.aedt_all",
                )
                for idx in range(1, 18)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            batch = run_window_bundles(
                window_queue=windows,
                accounts=[account],
                windows_per_job=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.window_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 99),
                max_workers=2,
            )

            self.assertEqual(sorted(batch.results), [1, 8, 8])
            self.assertEqual(batch.submitted_jobs, 3)

    def test_run_window_bundles_skips_blocked_account(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            windows = [
                WindowTaskRef(
                    run_id="run_01",
                    window_id=f"w_{idx:04d}",
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

            batch = run_window_bundles(
                window_queue=windows,
                accounts=accounts,
                windows_per_job=2,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.account_id,
                capacity_lookup=_capacity_lookup,
                max_workers=2,
            )

            self.assertEqual(batch.results, ["a2", "a2"])


if __name__ == "__main__":
    unittest.main()
