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
    _run_worker_iteration,
)


class TestSystemdWorker(unittest.TestCase):
    def test_build_config_reads_window_parallelism_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(Path(tmpdir) / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(Path(tmpdir) / "out"),
                    "PEETSFEA_DB_PATH": str(Path(tmpdir) / "state.duckdb"),
                    "PEETSFEA_WINDOWS_PER_JOB": "6",
                    "PEETSFEA_CORES_PER_WINDOW": "5",
                },
                clear=False,
            ):
                config = _build_config()

            self.assertEqual(config.windows_per_job, 6)
            self.assertEqual(config.cores_per_window, 5)

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
                    summary="total_windows=0 active_windows=0",
                    total_jobs=0,
                    success_jobs=0,
                    failed_jobs=0,
                    quarantined_jobs=0,
                    total_windows=0,
                    active_windows=0,
                    success_windows=0,
                    failed_windows=0,
                    quarantined_windows=0,
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
                    summary="total_windows=3 active_windows=0",
                    total_jobs=1,
                    success_jobs=1,
                    failed_jobs=0,
                    quarantined_jobs=0,
                    total_windows=3,
                    active_windows=0,
                    success_windows=3,
                    failed_windows=0,
                    quarantined_windows=0,
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
                    total_windows=4,
                    active_windows=0,
                    success_windows=0,
                    failed_windows=0,
                    quarantined_windows=0,
                    blocked_accounts=("account_01",),
                    readiness_blocked_windows=4,
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
                    total_windows=8,
                    active_windows=0,
                    success_windows=0,
                    failed_windows=4,
                    quarantined_windows=4,
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
                total_windows=4,
                active_windows=0,
                success_windows=0,
                failed_windows=4,
                quarantined_windows=0,
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


if __name__ == "__main__":
    unittest.main()
