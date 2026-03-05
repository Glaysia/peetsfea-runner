from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from peetsfea_runner.scheduler import JobSpec, calculate_effective_slots, run_jobs_with_slots


class TestScheduler(unittest.TestCase):
    def test_effective_slots_uses_min_formula(self) -> None:
        slots = calculate_effective_slots(
            max_jobs_per_account=10,
            license_cap_per_account=80,
            windows_per_job=8,
        )
        self.assertEqual(slots, 10)

    def test_run_jobs_with_slots_caps_max_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            jobs = [
                JobSpec(job_id=f"job_{idx:04d}", job_index=idx, aedt_path=base / f"file_{idx}.aedt")
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


if __name__ == "__main__":
    unittest.main()
