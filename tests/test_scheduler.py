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
    _build_enroot_preflight_script,
    _build_windows_preflight_script,
    _build_windows_readiness_script,
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
    SlotWorkerController,
)


@dataclass(slots=True)
class _Account:
    account_id: str
    host_alias: str
    max_jobs: int
    platform: str = "linux"
    scheduler: str = "slurm"


@dataclass(slots=True)
class _WorkerOutcome:
    name: str
    requeue_slots: tuple[SlotTaskRef, ...] = ()

    @property
    def terminal_worker(self) -> bool:
        return bool(self.requeue_slots)


class TestScheduler(unittest.TestCase):
    def test_build_enroot_preflight_script_extracts_numeric_inner_marker_values(self) -> None:
        script = _build_enroot_preflight_script(
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            remote_root="~/aedt_runs",
        )

        self.assertIn("sed -n 's/.*tmpfs=\\([01]\\).*/\\1/p'", script)
        self.assertIn("sed -n 's/.*python=\\([01]\\).*/\\1/p'", script)
        self.assertNotIn("\\\\1/p", script)

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
                    host_alias="gate1-harry261",
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
        self.assertEqual(snapshot.allowed_submit, 6)

    def test_query_account_capacity_clamps_when_visible_occupied_exceeds_max_jobs(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)

        snapshot = query_account_capacity(
            account=account,
            pending_buffer_per_account=3,
            run_command=lambda _cmd: (0, "R\nR\nR\nR\nR\nR\nPD\nPD\nPD\nPD\nPD\n", ""),
        )

        self.assertEqual(snapshot.running_count, 6)
        self.assertEqual(snapshot.pending_count, 5)
        self.assertEqual(snapshot.allowed_submit, 0)

    def test_query_account_capacity_raises_on_command_timeout(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        with patch("peetsfea_runner.scheduler.subprocess.run") as run_mock:
            run_mock.side_effect = subprocess.TimeoutExpired(cmd="ssh host-1", timeout=8)
            with self.assertRaises(RuntimeError):
                query_account_capacity(account=account, pending_buffer_per_account=3)

    def test_query_account_capacity_uses_repo_local_ssh_config_when_provided(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        seen_commands: list[list[str]] = []

        def _run_command(command: list[str]) -> tuple[int, str, str]:
            seen_commands.append(command)
            return 0, "R\n", ""

        query_account_capacity(
            account=account,
            pending_buffer_per_account=3,
            run_command=_run_command,
            ssh_config_path="/repo/.ssh/config",
        )

        self.assertEqual(
            seen_commands[0][:8],
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-F", "/repo/.ssh/config", "host-1"],
        )

    def test_query_account_readiness_reports_ready_snapshot(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1\n",
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
                env_ok=True,
                python_ok=True,
                module_ok=True,
                binaries_ok=True,
                ansys_ok=True,
                scratch_root="$HOME/aedt_runs",
            ),
        )

    def test_query_account_readiness_reports_blocked_checks(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=0 python=0 module=1 binaries=1 ansys=0\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BLOCKED")
        self.assertEqual(snapshot.reason, "env,python,ansys")

    def test_query_account_readiness_blocks_storage_when_inode_full(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 storage=1 inode_pct=100 free_mb=12345\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BLOCKED")
        self.assertEqual(snapshot.reason, "home_inode_exhausted inode_pct=100 threshold_pct=98")
        self.assertFalse(snapshot.storage_ready)
        self.assertEqual(snapshot.inode_use_percent, 100)
        self.assertEqual(snapshot.free_mb, 12345)

    def test_query_account_readiness_blocks_gpfs_inode_pressure(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 storage=1 inode_pct=100 free_mb=12345 fs_type=gpfs\n",
                "",
            ),
        )
        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(snapshot.reason, "ok")
        self.assertTrue(snapshot.storage_ready)
        self.assertEqual(snapshot.inode_use_percent, 100)
        self.assertEqual(snapshot.free_mb, 12345)

    def test_query_account_readiness_marks_bootstrap_required_for_missing_runtime(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=0 python=0 module=1 binaries=1 ansys=1\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BOOTSTRAP_REQUIRED")
        self.assertEqual(snapshot.reason, "env,python")

    def test_query_account_readiness_in_enroot_mode_reports_missing_image_without_bootstrap(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=0 env=1 python=1 module=1 binaries=1 ansys=1 storage=1 inode_pct=10 free_mb=8192 fs_type=ext4 container=1 missing=image_missing image_path=$HOME/runtime/enroot/aedt.sqsh\n",
                "",
            ),
        )

        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BOOTSTRAP_REQUIRED")
        self.assertEqual(snapshot.reason, "image_missing path=$HOME/runtime/enroot/aedt.sqsh")

    def test_query_account_readiness_in_enroot_mode_marks_stale_image_for_bootstrap(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_readiness(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_READY__:home=1 runtime=0 env=1 python=1 module=1 binaries=1 ansys=1 storage=1 inode_pct=10 free_mb=8192 fs_type=ext4 container=1 missing=image_stale image_path=$HOME/runtime/enroot/aedt.sqsh\n",
                "",
            ),
        )

        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BOOTSTRAP_REQUIRED")
        self.assertEqual(snapshot.reason, "image_stale path=$HOME/runtime/enroot/aedt.sqsh")

    def test_query_account_readiness_in_enroot_mode_uses_direct_probe(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        seen_commands: list[list[str]] = []

        def _run_command(command: list[str]) -> tuple[int, str, str]:
            seen_commands.append(command)
            return (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 storage=1 inode_pct=10 free_mb=8192 fs_type=ext4 container=1 missing= image_path=$HOME/runtime/enroot/aedt.sqsh\n",
                "",
            )

        snapshot = query_account_readiness(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            run_command=_run_command,
        )

        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(snapshot.reason, "ok")
        self.assertNotIn("srun -p", seen_commands[0][-1])

    def test_query_account_preflight_reports_missing_uv_and_pyaedt(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 uv=0 pyaedt=0 pandas=0 pyvista=0\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "PREFLIGHT_FAILED")
        self.assertEqual(snapshot.reason, "uv,pyaedt,pandas,pyvista")
        self.assertFalse(snapshot.uv_ok)
        self.assertFalse(snapshot.pyaedt_ok)

    def test_query_account_preflight_in_enroot_mode_accepts_scheduler_queue_delay(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        seen_commands: list[list[str]] = []

        def _run_command(command: list[str]) -> tuple[int, str, str]:
            seen_commands.append(command)
            return 1, "", "srun: Unable to allocate resources: Requested nodes are busy\n"

        snapshot = query_account_preflight(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            run_command=_run_command,
        )

        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(snapshot.reason, "scheduler_queue_delay")
        self.assertTrue(snapshot.uv_ok)
        self.assertTrue(snapshot.pyaedt_ok)
        self.assertIn("--immediate=15", seen_commands[0][-1])

    def test_query_account_preflight_in_enroot_mode_accepts_image_backed_runtime(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 uv=1 pyaedt=1 pandas=1 pyvista=1 storage=1 inode_pct=10 free_mb=8192 fs_type=ext4 container=1 missing=\n",
                "",
            ),
        )

        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(snapshot.reason, "ok")
        self.assertTrue(snapshot.uv_ok)
        self.assertTrue(snapshot.pyaedt_ok)

    def test_query_account_preflight_blocks_storage_when_free_mb_below_threshold(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            remote_storage_min_free_mb=2048,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 uv=1 pyaedt=1 storage=1 inode_pct=10 free_mb=1024\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BLOCKED")
        self.assertEqual(snapshot.reason, "home_storage_insufficient free_mb=1024 threshold_mb=2048")
        self.assertFalse(snapshot.storage_ready)

    def test_query_account_preflight_blocks_gpfs_inode_pressure(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 uv=1 pyaedt=1 pandas=1 pyvista=1 storage=1 inode_pct=100 free_mb=20480 fs_type=gpfs\n",
                "",
            ),
        )
        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(snapshot.reason, "ok")
        self.assertTrue(snapshot.storage_ready)

    def test_query_account_preflight_gpfs_still_blocks_when_free_mb_below_threshold(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        snapshot = query_account_preflight(
            account=account,
            remote_storage_min_free_mb=2048,
            run_command=lambda _cmd: (
                0,
                "__PEETSFEA_PREFLIGHT__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1 uv=1 pyaedt=1 pandas=1 pyvista=1 storage=1 inode_pct=100 free_mb=1024 fs_type=gpfs\n",
                "",
            ),
        )
        self.assertFalse(snapshot.ready)
        self.assertEqual(snapshot.status, "BLOCKED")
        self.assertEqual(snapshot.reason, "home_storage_insufficient free_mb=1024 threshold_mb=2048")
        self.assertFalse(snapshot.storage_ready)

    def test_bootstrap_account_runtime_requires_success_marker(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        output = bootstrap_account_runtime(
            account=account,
            run_command=lambda _cmd: (0, "__PEETSFEA_BOOTSTRAP__:ok\n", ""),
        )
        self.assertIn("__PEETSFEA_BOOTSTRAP__:ok", output)

    def test_linux_bootstrap_recreates_stale_miniconda_dir(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        seen: list[list[str]] = []

        def _runner(cmd: list[str]) -> tuple[int, str, str]:
            seen.append(cmd)
            return 0, "__PEETSFEA_BOOTSTRAP__:ok\n", ""

        bootstrap_account_runtime(account=account, run_command=_runner)
        self.assertTrue(seen)
        self.assertIn('if [ -e "$MINICONDA_DIR" ]; then', seen[0][-1])
        self.assertIn('rm -rf "$MINICONDA_DIR"', seen[0][-1])

    def test_bootstrap_account_runtime_supports_enroot_mode(self) -> None:
        account = _Account(account_id="a1", host_alias="host-1", max_jobs=10)
        seen: list[list[str]] = []

        def _runner(cmd: list[str]) -> tuple[int, str, str]:
            seen.append(cmd)
            return 0, "__PEETSFEA_BOOTSTRAP__:ok\n", ""

        output = bootstrap_account_runtime(
            account=account,
            remote_container_runtime="enroot",
            remote_container_image="~/runtime/enroot/aedt.sqsh",
            run_command=_runner,
        )
        self.assertIn("__PEETSFEA_BOOTSTRAP__:ok", output)
        self.assertTrue(seen)
        self.assertIn("srun -N1 -n1", seen[0][-1])
        self.assertNotIn("srun -p cpu2 -N1 -n1", seen[0][-1])
        self.assertIn("--job-name=peetsfea-bootstrap", seen[0][-1])
        self.assertIn("docker://ubuntu:24.04", seen[0][-1])
        self.assertIn("aedt.sqsh.meta.json", seen[0][-1])
        self.assertIn("/ansys_inc/v252", seen[0][-1])
        self.assertIn("2026-03-18-aedt-sqsh-v2", seen[0][-1])

    def test_query_windows_account_capacity_uses_local_max_jobs(self) -> None:
        account = _Account(account_id="w1", host_alias="win-1", max_jobs=2, platform="windows", scheduler="none")
        snapshot = query_account_capacity(account=account, pending_buffer_per_account=3)
        self.assertEqual(snapshot.running_count, 0)
        self.assertEqual(snapshot.pending_count, 0)
        self.assertEqual(snapshot.allowed_submit, 2)

    def test_query_windows_account_readiness_uses_powershell_marker(self) -> None:
        account = _Account(account_id="w1", host_alias="win-1", max_jobs=2, platform="windows", scheduler="none")
        seen: list[list[str]] = []

        def _runner(cmd: list[str]) -> tuple[int, str, str]:
            seen.append(cmd)
            return (
                0,
                "__PEETSFEA_READY__:home=1 runtime=1 env=1 python=1 module=1 binaries=1 ansys=1\n",
                "",
            )

        snapshot = query_account_readiness(
            account=account,
            run_command=_runner,
        )
        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertTrue(seen)
        self.assertIn("powershell -NoProfile -NonInteractive -Command", seen[0][-1])

    def test_windows_bootstrap_dispatches_via_powershell(self) -> None:
        account = _Account(account_id="w1", host_alias="win-1", max_jobs=2, platform="windows", scheduler="none")
        seen: list[list[str]] = []

        def _runner(cmd: list[str]) -> tuple[int, str, str]:
            seen.append(cmd)
            return 0, "__PEETSFEA_BOOTSTRAP__:ok\n", ""

        output = bootstrap_account_runtime(account=account, run_command=_runner)
        self.assertIn("__PEETSFEA_BOOTSTRAP__:ok", output)
        self.assertTrue(seen)
        self.assertIn("powershell -NoProfile -NonInteractive -Command", seen[0][-1])

    def test_windows_scripts_do_not_reference_slurm_or_modules(self) -> None:
        readiness = _build_windows_readiness_script()
        preflight = _build_windows_preflight_script()
        for content in (readiness, preflight):
            self.assertNotIn("squeue", content)
            self.assertNotIn("srun", content)
            self.assertNotIn("module load", content)
            self.assertNotIn("bash -lc", content)

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

    def test_run_slot_bundles_can_prefetch_more_slots_than_pool_size(self) -> None:
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
                for idx in range(1, 7)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            batch = run_slot_bundles(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=4,
                bundle_slot_limit=8,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.slot_count,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 99),
                max_workers=1,
            )

            self.assertEqual(batch.results, [6])
            self.assertEqual(batch.submitted_jobs, 1)

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
                return AccountCapacitySnapshot("a2", "h2", 0, 0, 10)

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

            self.assertEqual(batch.submitted_jobs, 2)
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

    def test_slot_worker_controller_waits_for_full_slot_pool_before_new_bundle(self) -> None:
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
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)
            submitted: list[int] = []
            release_event = threading.Event()

            def _worker(bundle):
                submitted.append(bundle.slot_count)
                self.assertTrue(release_event.wait(timeout=2))
                return bundle.slot_count

            controller = SlotWorkerController(
                accounts=[account],
                slots_per_job=4,
                bundle_slot_limit=4,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 0, 0, 10),
                max_workers=10,
            )
            controller.enqueue_slots(slots[:3])
            progressed = controller.step(wait_for_progress=False)
            self.assertFalse(progressed)
            self.assertEqual(controller.snapshot().inflight_jobs, 0)

            controller.enqueue_slots([slots[3]])
            controller.step(wait_for_progress=False)
            self.assertEqual(controller.snapshot().inflight_jobs, 1)
            release_event.set()
            while controller.has_work():
                controller.step(wait_for_progress=False, flush_partial_bundles=True)

            self.assertEqual(submitted, [4])

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

    def test_run_slot_workers_counts_local_submissions_against_capacity_until_observed(self) -> None:
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
                for idx in range(1, 11)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)
            submitted_job_ids: list[str] = []
            release_event = threading.Event()

            def _worker(bundle):
                submitted_job_ids.append(bundle.job_id)
                if len(submitted_job_ids) < 10:
                    self.assertTrue(release_event.wait(timeout=2))
                return bundle.job_id

            controller = SlotWorkerController(
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 7, 0, 3),
                max_workers=10,
            )
            controller.enqueue_slots(slots)
            controller.step(wait_for_progress=False)
            self.assertEqual(controller.snapshot().inflight_jobs, 3)
            release_event.set()
            while controller.has_work():
                controller.step(wait_for_progress=False)

            self.assertEqual(len(submitted_job_ids), 10)

    def test_slot_worker_controller_uses_remaining_remote_visible_capacity_across_steps(self) -> None:
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
                for idx in range(1, 9)
            ]
            account = _Account(account_id="dw16", host_alias="h1", max_jobs=10)
            submitted_job_ids: list[str] = []
            release_event = threading.Event()

            def _worker(bundle):
                submitted_job_ids.append(bundle.job_id)
                self.assertTrue(release_event.wait(timeout=2))
                return bundle.job_id

            controller = SlotWorkerController(
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("dw16", "h1", 7, 0, 3),
                max_workers=10,
            )
            controller.enqueue_slots(slots)
            controller.step(wait_for_progress=False)
            self.assertEqual(controller.snapshot().inflight_jobs, 3)
            progressed = controller.step(wait_for_progress=False)
            self.assertTrue(progressed)
            self.assertEqual(controller.snapshot().inflight_jobs, 6)
            release_event.set()
            while controller.has_work():
                controller.step(wait_for_progress=False)

            self.assertEqual(len(submitted_job_ids), 8)

    def test_slot_worker_controller_allows_remote_visible_capacity_without_double_counting_local_workers(self) -> None:
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
                for idx in range(1, 11)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)
            submitted_job_ids: list[str] = []
            release_event = threading.Event()

            def _worker(bundle):
                submitted_job_ids.append(bundle.job_id)
                self.assertTrue(release_event.wait(timeout=2))
                return bundle.job_id

            controller = SlotWorkerController(
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 5, 0, 5),
                max_workers=10,
            )
            controller.enqueue_slots(slots)
            controller.step(wait_for_progress=False)
            self.assertEqual(controller.snapshot().inflight_jobs, 5)

            progressed = controller.step(wait_for_progress=False)
            self.assertTrue(progressed)
            self.assertEqual(controller.snapshot().inflight_jobs, 10)

            release_event.set()
            while controller.has_work():
                controller.step(wait_for_progress=False)

            self.assertEqual(len(submitted_job_ids), 10)

    def test_run_slot_bundles_counts_local_submissions_against_capacity(self) -> None:
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
                for idx in range(1, 7)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)
            release_event = threading.Event()
            submitted_job_ids: list[str] = []

            def _worker(bundle):
                submitted_job_ids.append(bundle.job_id)
                if len(submitted_job_ids) == 3:
                    release_event.set()
                elif len(submitted_job_ids) < 3:
                    self.assertTrue(release_event.wait(timeout=2))
                return bundle.job_id

            batch = run_slot_bundles(
                slot_queue=slots,
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=_worker,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 7, 0, 3),
                max_workers=10,
            )

            self.assertEqual(batch.max_inflight_jobs, 3)

    def test_slot_worker_controller_rebalances_backlog_to_account_with_capacity(self) -> None:
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
            submitted_accounts: list[str] = []

            def _capacity_lookup(*, account, pending_buffer_per_account):
                if account.account_id == "a1":
                    return AccountCapacitySnapshot("a1", "h1", 10, 0, 0)
                return AccountCapacitySnapshot("a2", "h2", 0, 0, 10)

            controller = SlotWorkerController(
                accounts=accounts,
                slots_per_job=2,
                pending_buffer_per_account=3,
                worker=lambda bundle: submitted_accounts.append(bundle.account_id) or bundle.account_id,
                capacity_lookup=_capacity_lookup,
                max_workers=2,
            )
            controller.enqueue_slots(slots)
            result = controller.finalize()

            self.assertEqual(result.results, ["a2", "a2"])

    def test_slot_worker_controller_rebalances_underfilled_capacity_across_multiple_accounts(self) -> None:
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
            accounts = [
                _Account(account_id="blocked", host_alias="h1", max_jobs=10),
                _Account(account_id="dhj02", host_alias="h2", max_jobs=10),
                _Account(account_id="jji0930", host_alias="h3", max_jobs=10),
            ]
            submitted_accounts: list[str] = []

            def _capacity_lookup(*, account, pending_buffer_per_account):
                if account.account_id == "blocked":
                    return AccountCapacitySnapshot("blocked", "h1", 10, 0, 0)
                if account.account_id == "dhj02":
                    return AccountCapacitySnapshot("dhj02", "h2", 3, 0, 3)
                return AccountCapacitySnapshot("jji0930", "h3", 2, 0, 4)

            controller = SlotWorkerController(
                accounts=accounts,
                slots_per_job=2,
                pending_buffer_per_account=3,
                worker=lambda bundle: submitted_accounts.append(bundle.account_id) or bundle.account_id,
                capacity_lookup=_capacity_lookup,
                max_workers=4,
            )
            controller.enqueue_slots(slots)
            result = controller.finalize()

            self.assertEqual(len(result.results), 6)
            self.assertNotIn("blocked", submitted_accounts)
            self.assertIn("dhj02", submitted_accounts)
            self.assertIn("jji0930", submitted_accounts)

    def test_local_capacity_lookup_respects_account_max_jobs_only(self) -> None:
        from peetsfea_runner.pipeline import _local_capacity_lookup

        account = _Account(account_id="a1", host_alias="h1", max_jobs=10)
        snapshot = _local_capacity_lookup(account=account, pending_buffer_per_account=3)
        self.assertEqual(snapshot.allowed_submit, 10)

    def test_slot_worker_controller_does_not_submit_when_capacity_is_full(self) -> None:
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
                for idx in range(1, 4)
            ]
            account = _Account(account_id="a1", host_alias="h1", max_jobs=10)

            controller = SlotWorkerController(
                accounts=[account],
                slots_per_job=1,
                pending_buffer_per_account=3,
                worker=lambda bundle: bundle.job_id,
                capacity_lookup=lambda **_kwargs: AccountCapacitySnapshot("a1", "h1", 10, 0, 0),
                max_workers=10,
            )
            controller.enqueue_slots(slots)
            progressed = controller.step(wait_for_progress=False)

            self.assertFalse(progressed)
            self.assertEqual(controller.snapshot().inflight_jobs, 0)


if __name__ == "__main__":
    unittest.main()
