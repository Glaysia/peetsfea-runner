from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path

from peetsfea_runner import web_status
from peetsfea_runner.built_in_service import (
    EXPECTED_LANE_NAMES,
    build_service_profile,
    validate_service_layout,
    _lane_pipeline_config,
)
from peetsfea_runner.pipeline import build_lease_server_context


class TestBuiltInService(unittest.TestCase):
    def test_systemd_unit_uses_built_in_service_without_environment_overrides(self) -> None:
        service_path = Path(__file__).resolve().parent.parent / "systemd" / "peetsfea-runner.service"
        content = service_path.read_text(encoding="utf-8")

        self.assertNotIn("Environment=", content)
        self.assertNotIn("ExecStopPost=", content)
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
        self.assertEqual(lane_by_id["preserve_results"].slots_per_job, 2)
        self.assertEqual(lane_by_id["preserve_results"].cores_per_slot, 16)
        self.assertEqual(lane_by_id["preserve_results"].tasks_per_slot, 4)
        self.assertTrue(lane_by_id["preserve_results"].retain_aedtresults)
        self.assertTrue(lane_by_id["preserve_results"].rename_input_to_done_on_success)
        self.assertEqual(tuple(lane_by_id["preserve_results"].accounts), ())
        self.assertEqual(lane_by_id["prune_results"].cpus_per_job, 60)
        self.assertEqual(lane_by_id["prune_results"].slots_per_job, 15)
        self.assertEqual(lane_by_id["prune_results"].cores_per_slot, 4)
        self.assertEqual(lane_by_id["prune_results"].tasks_per_slot, 1)
        self.assertFalse(lane_by_id["prune_results"].retain_aedtresults)
        self.assertTrue(lane_by_id["prune_results"].rename_input_to_done_on_success)
        self.assertEqual(
            [account.host_alias for account in lane_by_id["prune_results"].accounts],
            [
                "gate1-harry261",
                "gate1-jji0930",
            ],
        )
        configured_worker_slots = sum(
            sum(account.max_jobs for account in lane.accounts) * lane.slots_per_job
            for lane in profile.lanes
        )
        self.assertEqual(configured_worker_slots, 255)

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
            (root / ".ssh").mkdir(parents=True, exist_ok=True)
            (root / ".ssh" / "config").write_text("Host gate1-harry261\n", encoding="utf-8")
            profile = build_service_profile(repo_root=root)
            lane_by_id = {lane.lane_id: lane for lane in profile.lanes}

            prune_cfg = _lane_pipeline_config(profile, lane_by_id["prune_results"])

            with self.assertRaisesRegex(ValueError, "lane has no assigned accounts: preserve_results"):
                _lane_pipeline_config(profile, lane_by_id["preserve_results"])
        self.assertEqual(prune_cfg.run_namespace, "prune_results")
        self.assertEqual(prune_cfg.tasks_per_slot, 1)
        self.assertFalse(prune_cfg.retain_aedtresults)
        self.assertTrue(prune_cfg.rename_input_to_done_on_success)
        self.assertEqual(len(prune_cfg.accounts_registry), 2)
        self.assertEqual(prune_cfg.ssh_config_path, str(root / ".ssh" / "config"))
        self.assertEqual(prune_cfg.remote_root, "~/aedt_runs")
        self.assertEqual(prune_cfg.pull_workspace_user, profile.control_plane_return_user)
        self.assertEqual(prune_cfg.pull_workspace_host, profile.control_plane_return_host)
        self.assertEqual(prune_cfg.pull_workspace_path, str(root))
        self.assertEqual(prune_cfg.pull_workspace_mount_root, "/workspace")
        self.assertEqual(prune_cfg.slots_per_job, 15)
        self.assertEqual(prune_cfg.mem, "960G")
        self.assertEqual(prune_cfg.slot_min_concurrency, 1)
        self.assertEqual(prune_cfg.slot_max_concurrency, 15)
        self.assertEqual(prune_cfg.worker_payload_slot_limit, 15)
        self.assertEqual(prune_cfg.worker_bundle_multiplier, 4)
        self.assertEqual(prune_cfg.worker_pool_size, 50)
        self.assertEqual(prune_cfg.lease_ttl_seconds, 600)
        self.assertEqual(prune_cfg.lease_heartbeat_seconds, 15)
        self.assertTrue(prune_cfg.license_gate_enabled)
        self.assertEqual(prune_cfg.license_ceiling, 350)

    def test_prune_lane_keeps_slot_based_worker_contract_outside_submit_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input_queue" / "preserve_results").mkdir(parents=True, exist_ok=True)
            (root / "input_queue" / "prune_results").mkdir(parents=True, exist_ok=True)
            profile = build_service_profile(repo_root=root)
            lane_by_id = {lane.lane_id: lane for lane in profile.lanes}

            prune_cfg = _lane_pipeline_config(profile, lane_by_id["prune_results"])
            lease_context = build_lease_server_context(config=prune_cfg)

        self.assertEqual(prune_cfg.slots_per_job, 15)
        self.assertEqual(prune_cfg.slot_max_concurrency, 15)
        self.assertEqual(prune_cfg.worker_payload_slot_limit, 15)
        self.assertEqual(prune_cfg.cpus_per_job, 60)
        self.assertEqual(prune_cfg.cores_per_slot, 4)
        self.assertEqual(prune_cfg.tasks_per_slot, 1)
        self.assertEqual(prune_cfg.mem, "960G")
        self.assertEqual(prune_cfg.worker_pool_size, 50)
        self.assertEqual(prune_cfg.capacity_scope, "all_user_jobs")
        self.assertEqual(prune_cfg.pending_buffer_per_account, 3)
        self.assertEqual(lease_context.lease_ttl_seconds, 600)
        self.assertTrue(lease_context.license_gate_enabled)
        self.assertEqual(lease_context.license_ceiling, 350)
        self.assertFalse(hasattr(lease_context, "allowed_submit"))
        self.assertFalse(hasattr(lease_context, "pending_buffer_per_account"))
        self.assertFalse(hasattr(lease_context, "capacity_scope"))

    def test_lease_request_handler_uses_electronics_desktop_lease_gate_names(self) -> None:
        source = inspect.getsource(web_status)
        request_start = source.index('if parsed.path == "/internal/leases/request":')
        request_end = source.index('if parsed.path == "/internal/leases/heartbeat":')
        request_handler = source[request_start:request_end]

        self.assertIn("license_closed", request_handler)
        self.assertIn("license_feature", request_handler)
        self.assertIn("license_in_use", request_handler)
        self.assertIn("license_ceiling", request_handler)
        self.assertNotIn("query_account_capacity", request_handler)
        self.assertNotIn("allowed_submit", request_handler)
        self.assertNotIn("capacity_by_account", request_handler)
        self.assertNotIn("pending_buffer_per_account", request_handler)


if __name__ == "__main__":
    unittest.main()
