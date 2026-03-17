from __future__ import annotations

import tempfile
import unittest
from os import environ
from pathlib import Path
from unittest.mock import patch

import duckdb

from peetsfea_runner.pipeline import EXIT_CODE_SUCCESS, PipelineConfig, PipelineResult
from peetsfea_runner.state_store import StateStore
from peetsfea_runner.systemd_worker import (
    _AutorecoveryControlState,
    _WorkerRuntimeState,
    _build_config,
    _heartbeat_once,
    _normalize_control_plane_ssh_target,
    _run_worker_iteration,
)


class TestSystemdWorker(unittest.TestCase):
    def test_build_config_reads_slot_parallelism_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_CPUS_PER_JOB": "24",
                    "PEETSFEA_MEM": "512G",
                    "PEETSFEA_TIME_LIMIT": "02:30:00",
                    "PEETSFEA_SLOTS_PER_JOB": "6",
                    "PEETSFEA_CORES_PER_SLOT": "5",
                },
                clear=False,
            ):
                config = _build_config()

            self.assertEqual(config.slots_per_job, 6)
            self.assertEqual(config.cores_per_slot, 5)
            self.assertEqual(config.cpus_per_job, 24)
            self.assertEqual(config.mem, "512G")
            self.assertEqual(config.time_limit, "02:30:00")

    def test_build_config_parses_windows_account_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_ACCOUNTS": "account_01@gate1-harry261:10,account_04@gate1-dw16:2:windows:none",
                },
                clear=False,
            ):
                config = _build_config()

        self.assertEqual(len(config.accounts_registry), 2)
        self.assertEqual(config.accounts_registry[1].platform, "windows")
        self.assertEqual(config.accounts_registry[1].scheduler, "none")

    def test_build_config_reads_repo_local_ssh_config_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ssh_config_path = Path(tmpdir) / ".ssh-config"
            ssh_config_path.write_text("Host gate1-harry261\n", encoding="utf-8")
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_SSH_CONFIG_PATH": str(ssh_config_path),
                },
                clear=False,
            ):
                config = _build_config()

        self.assertEqual(config.ssh_config_path, str(ssh_config_path))

    def test_build_config_reads_enroot_runtime_fields_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_REMOTE_EXECUTION_BACKEND": "slurm_batch",
                    "PEETSFEA_REMOTE_CONTAINER_RUNTIME": "enroot",
                    "PEETSFEA_REMOTE_CONTAINER_IMAGE": "~/runtime/enroot/aedt.sqsh",
                    "PEETSFEA_REMOTE_CONTAINER_ANSYS_ROOT": "/opt/ohpc/pub/Electronics/v252",
                    "PEETSFEA_REMOTE_ANSYS_EXECUTABLE": "/mnt/AnsysEM/ansysedt",
                },
                clear=False,
            ):
                config = _build_config()

        self.assertEqual(config.remote_container_runtime, "enroot")
        self.assertEqual(config.remote_container_image, "~/runtime/enroot/aedt.sqsh")
        self.assertEqual(config.remote_container_ansys_root, "/opt/ohpc/pub/Electronics/v252")
        self.assertEqual(config.remote_ansys_executable, "/mnt/AnsysEM/ansysedt")

    def test_normalize_control_plane_ssh_target_prefixes_local_user(self) -> None:
        with patch.dict(environ, {"USER": "peetsmain"}, clear=False):
            self.assertEqual(_normalize_control_plane_ssh_target("harrypc"), "peetsmain@harrypc")

    def test_normalize_control_plane_ssh_target_preserves_explicit_user(self) -> None:
        with patch.dict(environ, {"USER": "peetsmain"}, clear=False):
            self.assertEqual(_normalize_control_plane_ssh_target("admin@harrypc"), "admin@harrypc")

    def test_build_config_normalizes_control_plane_ssh_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_CONTROL_PLANE_SSH_TARGET": "harrypc",
                    "USER": "peetsmain",
                },
                clear=False,
            ):
                config = _build_config()

        self.assertEqual(config.control_plane_ssh_target, "peetsmain@harrypc")
        self.assertEqual(config.control_plane_return_user, "peetsmain")
        self.assertEqual(config.control_plane_return_host, "harrypc")
        self.assertEqual(config.control_plane_return_port, 5722)

    def test_build_config_prefers_explicit_return_host_and_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_CONTROL_PLANE_RETURN_HOST": "192.168.0.10",
                    "PEETSFEA_CONTROL_PLANE_RETURN_PORT": "5722",
                    "PEETSFEA_CONTROL_PLANE_RETURN_USER": "peetsmain",
                    "USER": "peetsmain",
                },
                clear=False,
            ):
                config = _build_config()

        self.assertEqual(config.control_plane_return_host, "192.168.0.10")
        self.assertEqual(config.control_plane_return_port, 5722)
        self.assertEqual(config.control_plane_return_user, "peetsmain")
        self.assertEqual(config.control_plane_ssh_target, "peetsmain@192.168.0.10")

    def test_run_worker_iteration_marks_idle_when_pipeline_is_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState()

            with patch(
                "peetsfea_runner.systemd_worker.run_pipeline",
                return_value=PipelineResult(
                    success=True,
                    exit_code=EXIT_CODE_SUCCESS,
                    run_id="run_01",
                    remote_run_dir="/tmp/remote",
                    local_artifacts_dir="/tmp/out",
                    summary="total_slots=0 active_slots=0",
                    total_jobs=0,
                    success_jobs=0,
                    failed_jobs=0,
                    quarantined_jobs=0,
                    total_slots=0,
                    active_slots=0,
                    success_slots=0,
                    failed_slots=0,
                    quarantined_slots=0,
                ),
            ):
                result = _run_worker_iteration(config=config, store=store, runtime_state=runtime_state)

            self.assertEqual(result.run_id, "run_01")
            self.assertEqual(runtime_state.snapshot(), ("run_01", "IDLE"))

            conn = duckdb.connect(str(db_path))
            try:
                stages = [
                    row[0]
                    for row in conn.execute(
                        "SELECT stage FROM events ORDER BY ts ASC"
                    ).fetchall()
                ]
            finally:
                conn.close()
            self.assertEqual(stages, ["WORKER_LOOP_ACTIVE", "WORKER_LOOP_IDLE"])

    def test_run_worker_iteration_records_work_completion_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState()

            with patch(
                "peetsfea_runner.systemd_worker.run_pipeline",
                return_value=PipelineResult(
                    success=True,
                    exit_code=EXIT_CODE_SUCCESS,
                    run_id="run_02",
                    remote_run_dir="/tmp/remote",
                    local_artifacts_dir="/tmp/out",
                    summary="total_slots=3 active_slots=0",
                    total_jobs=1,
                    success_jobs=1,
                    failed_jobs=0,
                    quarantined_jobs=0,
                    total_slots=3,
                    active_slots=0,
                    success_slots=3,
                    failed_slots=0,
                    quarantined_slots=0,
                ),
            ):
                result = _run_worker_iteration(config=config, store=store, runtime_state=runtime_state)

            self.assertEqual(result.run_id, "run_02")
            self.assertEqual(runtime_state.snapshot(), ("run_02", "IDLE"))

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute(
                    "SELECT run_id, stage FROM events ORDER BY ts ASC"
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(rows[0], ("__worker__", "WORKER_LOOP_ACTIVE"))
            self.assertEqual(rows[1], ("run_02", "WORKER_LOOP_OK"))

    def test_run_worker_iteration_marks_blocked_when_readiness_blocks_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState()

            with patch(
                "peetsfea_runner.systemd_worker.run_pipeline",
                return_value=PipelineResult(
                    success=False,
                    exit_code=13,
                    run_id="run_blocked",
                    remote_run_dir="/tmp/remote",
                    local_artifacts_dir="/tmp/out",
                    summary="blocked",
                    total_jobs=0,
                    success_jobs=0,
                    failed_jobs=0,
                    quarantined_jobs=0,
                    total_slots=4,
                    active_slots=0,
                    success_slots=0,
                    failed_slots=0,
                    quarantined_slots=0,
                    blocked_accounts=("account_01",),
                    readiness_blocked_slots=4,
                ),
            ):
                result = _run_worker_iteration(config=config, store=store, runtime_state=runtime_state)

            self.assertEqual(result.run_id, "run_blocked")
            self.assertEqual(runtime_state.snapshot(), ("run_blocked", "BLOCKED"))

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT run_id, stage FROM events ORDER BY ts ASC").fetchall()
            finally:
                conn.close()
            self.assertEqual(rows[0], ("__worker__", "WORKER_LOOP_ACTIVE"))
            self.assertEqual(rows[1], ("run_blocked", "WORKER_LOOP_BLOCKED"))

    def test_run_worker_iteration_marks_recovering_for_capacity_shortfall(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState()

            with patch(
                "peetsfea_runner.systemd_worker.run_pipeline",
                return_value=PipelineResult(
                    success=False,
                    exit_code=13,
                    run_id="run_recover",
                    remote_run_dir="/tmp/remote",
                    local_artifacts_dir="/tmp/out",
                    summary="recover",
                    total_jobs=2,
                    success_jobs=0,
                    failed_jobs=1,
                    quarantined_jobs=1,
                    total_slots=8,
                    active_slots=0,
                    success_slots=0,
                    failed_slots=4,
                    quarantined_slots=4,
                    terminal_jobs=2,
                    replacement_jobs=1,
                ),
            ):
                result = _run_worker_iteration(config=config, store=store, runtime_state=runtime_state)

            self.assertEqual(result.run_id, "run_recover")
            self.assertEqual(runtime_state.snapshot(), ("run_recover", "RECOVERING"))

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT run_id, stage FROM events ORDER BY ts ASC").fetchall()
            finally:
                conn.close()
            self.assertEqual(rows[0], ("__worker__", "WORKER_LOOP_ACTIVE"))
            self.assertEqual(rows[1], ("run_recover", "WORKER_LOOP_RECOVERING"))

    def test_run_worker_iteration_rate_limits_repeated_recovery_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState()
            control_state = _AutorecoveryControlState()
            result_payload = PipelineResult(
                success=False,
                exit_code=13,
                run_id="run_recover",
                remote_run_dir="/tmp/remote",
                local_artifacts_dir="/tmp/out",
                summary="recover",
                total_jobs=1,
                success_jobs=0,
                failed_jobs=1,
                quarantined_jobs=0,
                total_slots=4,
                active_slots=0,
                success_slots=0,
                failed_slots=4,
                quarantined_slots=0,
                terminal_jobs=1,
                replacement_jobs=0,
            )

            with patch("peetsfea_runner.systemd_worker.run_pipeline", return_value=result_payload):
                _run_worker_iteration(
                    config=config,
                    store=store,
                    runtime_state=runtime_state,
                    control_state=control_state,
                    autorecovery_min_interval_seconds=60,
                )
                _run_worker_iteration(
                    config=config,
                    store=store,
                    runtime_state=runtime_state,
                    control_state=control_state,
                    autorecovery_min_interval_seconds=60,
                )

            conn = duckdb.connect(str(db_path))
            try:
                recovery_events = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE stage = 'WORKER_LOOP_RECOVERING'"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(recovery_events, 1)

    def test_heartbeat_once_records_resource_snapshots_for_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                mem="512G",
                slots_per_job=4,
            )
            store = StateStore(db_path)
            store.initialize()
            runtime_state = _WorkerRuntimeState(run_id="run_01", status="ACTIVE")
            store.start_run("run_01")

            with (
                patch("peetsfea_runner.systemd_worker._read_meminfo_mb", return_value=(1024, 256, 768)),
                patch("peetsfea_runner.systemd_worker._load_average", return_value=(1.0, 0.8, 0.6)),
                patch("peetsfea_runner.systemd_worker._tmp_usage_mb", return_value=(100, 25, 75)),
                patch("peetsfea_runner.systemd_worker._read_self_rss_mb", return_value=128),
                patch("peetsfea_runner.systemd_worker._system_process_count", return_value=321),
            ):
                _heartbeat_once(
                    store=store,
                    config=config,
                    service_name="peetsfea-runner",
                    host="mainpc",
                    pid=1234,
                    runtime_state=runtime_state,
                )

            conn = duckdb.connect(str(db_path))
            try:
                node_row = conn.execute(
                    """
                    SELECT allocated_mem_mb, total_mem_mb, used_mem_mb, free_mem_mb, load_1, process_count, running_worker_count, active_slot_count
                    FROM node_resource_snapshots
                    """
                ).fetchone()
                worker_row = conn.execute(
                    """
                    SELECT configured_slots, active_slots, idle_slots, rss_mb, process_count
                    FROM worker_resource_snapshots
                    """
                ).fetchone()
                summary_row = conn.execute(
                    """
                    SELECT allocated_mem_mb, used_mem_mb, free_mem_mb, load_1, stale
                    FROM resource_summary_snapshots
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(node_row, (512 * 1024, 1024, 256, 768, 1.0, 321, 0, 0))
            self.assertEqual(worker_row, (4, 0, 4, 128, 321))
            self.assertEqual(summary_row, (512 * 1024, 256, 768, 1.0, False))


if __name__ == "__main__":
    unittest.main()
