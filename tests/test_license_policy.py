from __future__ import annotations

import unittest

from peetsfea_runner.license_policy import (
    LicenseAccountState,
    compute_license_target_plan,
    next_desired_total_active_slots,
    parse_license_usage,
)


class TestLicensePolicy(unittest.TestCase):
    def test_parse_license_usage_uses_root_entries_for_effective_usage(self) -> None:
        snapshot = parse_license_usage(
            """
            Users of elec_solve_level1:  (Total of 550 licenses issued;  Total of 67 licenses in use)
              root nib110.hpc n110.hpc 1896957 (v2025.0506) (license-server/1055 6785), start Mon 4/6 19:59, PID: 1896801
              root nib110.hpc n110.hpc 1903224 (v2025.0506) (license-server/1055 18420), start Mon 4/6 20:00, PID: 1902908
              harry harrypc harrypc 372 (v2025.0506) (license-server/1055 20301), start Mon 4/6 19:42, PID: 224
            Users of elec_solve_level2:  (Total of 550 licenses issued;  Total of 69 licenses in use)
              root nib116.hpc n116.hpc 1707934 (v2025.0506) (license-server/1055 87029), start Mon 4/6 20:00, PID: 1707668
              root nib107.hpc n107.hpc 1316733 (v2025.0506) (license-server/1055 134663), start Mon 4/6 20:00, PID: 1316601
              root nib112.hpc n112.hpc 1781571 (v2025.0506) (license-server/1055 82769), start Mon 4/6 20:01, PID: 1781088
            """
        )

        self.assertEqual(snapshot.status, "OK")
        self.assertEqual(snapshot.level1_in_use, 2)
        self.assertEqual(snapshot.level2_in_use, 3)
        self.assertEqual(snapshot.effective_in_use, 3)
        self.assertEqual(snapshot.reported_level1_in_use, 67)
        self.assertEqual(snapshot.reported_level2_in_use, 69)
        self.assertEqual(snapshot.reported_effective_in_use, 69)

    def test_parse_license_usage_fails_when_required_lines_missing(self) -> None:
        snapshot = parse_license_usage("Users of elec_solve_level1:  (Total of 550 licenses issued;  Total of 67 licenses in use)")

        self.assertEqual(snapshot.status, "FAILED")
        self.assertIn("elec_solve_level2", snapshot.error or "")

    def test_next_desired_total_active_slots_increases_by_dispatchable_accounts(self) -> None:
        desired_total = next_desired_total_active_slots(
            current_desired_total_active_slots=8,
            effective_in_use=120,
            dispatchable_account_count=3,
            total_active_slots=6,
            total_queued_slots=20,
            ceiling=520,
        )

        self.assertEqual(desired_total, 11)

    def test_compute_license_target_plan_aggregates_account_rows_and_keeps_budget_on_poll_failure(self) -> None:
        account_states = [
            LicenseAccountState(
                run_id="preserve_01",
                account_id="account_01",
                host="gate1-harry261",
                ready=True,
                queued_slots=12,
                active_slots=2,
                max_active_slots=40,
                ts="2026-03-25T00:00:00+00:00",
            ),
            LicenseAccountState(
                run_id="prune_01",
                account_id="account_01",
                host="gate1-harry261",
                ready=True,
                queued_slots=0,
                active_slots=1,
                max_active_slots=40,
                ts="2026-03-25T00:00:00+00:00",
            ),
            LicenseAccountState(
                run_id="prune_01",
                account_id="account_02",
                host="gate1-dhj02",
                ready=True,
                queued_slots=8,
                active_slots=2,
                max_active_slots=40,
                ts="2026-03-25T00:00:00+00:00",
            ),
        ]

        plan = compute_license_target_plan(
            account_states=account_states,
            desired_total_active_slots=10,
            effective_in_use=None,
            ceiling=520,
        )

        self.assertFalse(plan.frozen)
        self.assertEqual(plan.total_active_slots, 5)
        self.assertEqual(plan.total_queued_slots, 20)
        self.assertEqual(plan.target_slots_by_account["account_01"], 5)
        self.assertEqual(plan.target_slots_by_account["account_02"], 5)


if __name__ == "__main__":
    unittest.main()
