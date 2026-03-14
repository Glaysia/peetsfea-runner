from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from peetsfea_runner.built_in_service import (
    EXPECTED_LANE_NAMES,
    build_service_profile,
    validate_service_layout,
    _lane_pipeline_config,
)


class TestBuiltInService(unittest.TestCase):
    def test_systemd_unit_uses_built_in_service_without_environment_overrides(self) -> None:
        service_path = Path(__file__).resolve().parent.parent / "systemd" / "peetsfea-runner.service"
        content = service_path.read_text(encoding="utf-8")

        self.assertNotIn("Environment=", content)
        self.assertIn("WorkingDirectory=%h/mnt/8tb/peetsfea-runner", content)
        self.assertIn(
            'ExecStart=%h/mnt/8tb/peetsfea-runner/.venv/bin/python -c "from peetsfea_runner.built_in_service import run_built_in_service; run_built_in_service()"',
            content,
        )

    def test_build_service_profile_hardcodes_preserve_and_prune_result_lanes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            profile = build_service_profile(repo_root=Path(tmpdir))

        lane_by_id = {lane.lane_id: lane for lane in profile.lanes}
        self.assertEqual(tuple(lane_by_id), EXPECTED_LANE_NAMES)
        self.assertEqual(lane_by_id["preserve_results"].cpus_per_job, 32)
        self.assertEqual(lane_by_id["preserve_results"].slots_per_job, 1)
        self.assertEqual(lane_by_id["preserve_results"].cores_per_slot, 32)
        self.assertEqual(lane_by_id["preserve_results"].tasks_per_slot, 4)
        self.assertTrue(lane_by_id["preserve_results"].retain_aedtresults)
        self.assertEqual([account.host_alias for account in lane_by_id["preserve_results"].accounts], ["gate1-harry"])
        self.assertEqual(lane_by_id["prune_results"].cpus_per_job, 20)
        self.assertEqual(lane_by_id["prune_results"].slots_per_job, 5)
        self.assertEqual(lane_by_id["prune_results"].cores_per_slot, 4)
        self.assertEqual(lane_by_id["prune_results"].tasks_per_slot, 1)
        self.assertFalse(lane_by_id["prune_results"].retain_aedtresults)
        self.assertEqual(
            [account.host_alias for account in lane_by_id["prune_results"].accounts],
            ["gate1-dhj02", "gate1-jji0930", "gate1-dw16"],
        )

    def test_validate_service_layout_creates_required_output_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "prune_results").mkdir(parents=True, exist_ok=True)
            profile = build_service_profile(repo_root=root)

            validate_service_layout(profile=profile)

            self.assertTrue((root / "output" / "preserve_results").is_dir())
            self.assertTrue((root / "output" / "prune_results").is_dir())
            self.assertTrue((root / "output" / "_delete_failed").is_dir())

    def test_validate_service_layout_rejects_loose_root_level_aedt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "prune_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "foo.aedt").write_text("x", encoding="utf-8")
            profile = build_service_profile(repo_root=root)

            with self.assertRaisesRegex(ValueError, "loose \\.aedt file"):
                validate_service_layout(profile=profile)

    def test_validate_service_layout_rejects_unexpected_top_level_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "prune_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "extra").mkdir(parents=True, exist_ok=True)
            profile = build_service_profile(repo_root=root)

            with self.assertRaisesRegex(ValueError, "unexpected top-level input_queue entry"):
                validate_service_layout(profile=profile)

    def test_validate_service_layout_requires_both_lane_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            profile = build_service_profile(repo_root=root)

            with self.assertRaisesRegex(FileNotFoundError, "missing required lane directory"):
                validate_service_layout(profile=profile)

    def test_lane_pipeline_config_carries_lane_specific_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "prune_results").mkdir(parents=True, exist_ok=True)
            profile = build_service_profile(repo_root=root)
            lane_by_id = {lane.lane_id: lane for lane in profile.lanes}

            preserve_cfg = _lane_pipeline_config(profile, lane_by_id["preserve_results"])
            prune_cfg = _lane_pipeline_config(profile, lane_by_id["prune_results"])

            self.assertEqual(preserve_cfg.run_namespace, "preserve_results")
            self.assertEqual(preserve_cfg.tasks_per_slot, 4)
            self.assertTrue(preserve_cfg.retain_aedtresults)
            self.assertEqual(preserve_cfg.input_source_policy, "allow_original")
            self.assertEqual(len(preserve_cfg.accounts_registry), 1)
            self.assertEqual(prune_cfg.run_namespace, "prune_results")
            self.assertEqual(prune_cfg.tasks_per_slot, 1)
            self.assertFalse(prune_cfg.retain_aedtresults)
            self.assertEqual(len(prune_cfg.accounts_registry), 3)


if __name__ == "__main__":
    unittest.main()
