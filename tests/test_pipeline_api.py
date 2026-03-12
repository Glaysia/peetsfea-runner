from __future__ import annotations

from collections import deque
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline
from peetsfea_runner import __version__
from peetsfea_runner.pipeline import (
    EXIT_CODE_SUCCESS,
    PipelineResult,
    _BundleRuntimeOutcome,
    _is_stale_worker_heartbeat,
    _record_launch_transient_failure,
    _reconcile_slurm_truth,
    _materialize_slot_outputs,
)
from peetsfea_runner.remote_job import CaseExecutionSummary, RemoteJobAttemptResult
from peetsfea_runner.scheduler import AccountCapacitySnapshot, AccountReadinessSnapshot
from peetsfea_runner.state_store import StateStore


class TestPipelineApi(unittest.TestCase):
    def test_reconcile_slurm_truth_escalates_transient_lag_to_capacity_mismatch(self) -> None:
        snapshot = AccountCapacitySnapshot(
            account_id="account_01",
            host_alias="gate1-harry",
            running_count=1,
            pending_count=0,
            allowed_submit=0,
        )

        state1, events1 = _reconcile_slurm_truth(
            snapshot=snapshot,
            local_active_jobs=2,
            previous_state=None,
        )
        self.assertEqual([event[1] for event in events1], ["SLURM_TRUTH_REFRESHED", "SLURM_TRUTH_LAG"])

        state2, events2 = _reconcile_slurm_truth(
            snapshot=snapshot,
            local_active_jobs=2,
            previous_state=state1,
        )
        self.assertEqual(state2.mismatch_streak, 2)
        self.assertEqual([event[1] for event in events2], ["CAPACITY_MISMATCH"])

        state3, events3 = _reconcile_slurm_truth(
            snapshot=AccountCapacitySnapshot(
                account_id="account_01",
                host_alias="gate1-harry",
                running_count=2,
                pending_count=0,
                allowed_submit=0,
            ),
            local_active_jobs=2,
            previous_state=state2,
        )
        self.assertEqual(state3.mismatch_streak, 0)
        self.assertEqual([event[1] for event in events3], ["SLURM_TRUTH_REFRESHED"])

    def test_defaults_match_roadmap09(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "sample.aedt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir))
            config.validate()
            self.assertEqual(config.output_root_dir, "./output")
            self.assertTrue(config.delete_input_after_upload)
            self.assertEqual(config.delete_failed_quarantine_dir, "./output/_delete_failed")
            self.assertTrue(config.license_observe_only)
            self.assertTrue(config.emit_output_variables_csv)
            self.assertEqual(len(config.accounts_registry), 1)
            self.assertTrue(config.continuous_mode)
            self.assertEqual(config.ready_sidecar_suffix, ".ready")
            self.assertEqual(config.run_rotation_hours, 24)
            self.assertFalse((input_dir / "sample.aedt.ready").exists())

    def test_record_launch_transient_failure_triggers_cooldown_after_threshold(self) -> None:
        history: dict[str, deque[float]] = {}
        cooldowns: dict[str, tuple[float, str]] = {}

        triggered, cooldown_until = _record_launch_transient_failure(
            account_id="account_04",
            history_by_account=history,
            cooldowns_by_account=cooldowns,
            now_monotonic=10.0,
            threshold=3,
            window_seconds=300,
            cooldown_seconds=300,
            reason="Connection closed by 172.16.10.36 port 22",
        )
        self.assertFalse(triggered)
        self.assertIsNone(cooldown_until)

        _record_launch_transient_failure(
            account_id="account_04",
            history_by_account=history,
            cooldowns_by_account=cooldowns,
            now_monotonic=20.0,
            threshold=3,
            window_seconds=300,
            cooldown_seconds=300,
            reason="Connection closed by 172.16.10.36 port 22",
        )
        triggered, cooldown_until = _record_launch_transient_failure(
            account_id="account_04",
            history_by_account=history,
            cooldowns_by_account=cooldowns,
            now_monotonic=30.0,
            threshold=3,
            window_seconds=300,
            cooldown_seconds=300,
            reason="Connection closed by 172.16.10.36 port 22",
        )
        self.assertTrue(triggered)
        self.assertEqual(cooldown_until, 330.0)
        self.assertEqual(cooldowns["account_04"][0], 330.0)

    def test_validate_rejects_unknown_remote_execution_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "sample.aedt").write_text("x", encoding="utf-8")
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                continuous_mode=False,
                remote_execution_backend="ssh_screen",
            )
            with self.assertRaisesRegex(ValueError, "remote_execution_backend"):
                config.validate()

    def test_slurm_batch_backend_records_worker_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "fixture_01.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(
                *,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                **kwargs,
            ):
                if on_upload_success is not None:
                    on_upload_success()
                if on_worker_submitted is not None:
                    on_worker_submitted("552740", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("PENDING", None)
                    on_worker_state_change("RUNNING", "n115")
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552740",
                    observed_node="n115",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT slurm_job_id, worker_state, observed_node, backend, slots_configured,
                           submitted_at, started_at, ended_at
                    FROM slurm_workers
                    ORDER BY last_seen_ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], "552740")
            self.assertEqual(row[1], "COMPLETED")
            self.assertEqual(row[2], "n115")
            self.assertEqual(row[3], "slurm_batch")
            self.assertEqual(row[4], 4)
            self.assertIsNotNone(row[5])
            self.assertIsNotNone(row[6])
            self.assertIsNotNone(row[7])

    def test_slurm_batch_does_not_mark_slots_running_before_worker_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "fixture_01.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(
                *,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                **kwargs,
            ):
                if on_upload_success is not None:
                    on_upload_success()
                if on_worker_submitted is not None:
                    on_worker_submitted("552740", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("PENDING", None)
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552740",
                    observed_node=None,
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                stages = [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT stage
                        FROM slot_events
                        WHERE slot_id IN (SELECT slot_id FROM slot_tasks LIMIT 1)
                        ORDER BY ts
                        """
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertIn("LEASED", stages)
            self.assertIn("DOWNLOADING", stages)
            self.assertIn("UPLOADING", stages)
            self.assertIn("SUCCEEDED", stages)
            self.assertNotIn("RUNNING", stages)

    def test_slurm_batch_restart_rediscovery_refreshes_active_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            run_id = store.ensure_continuous_run(rotation_hours=24)
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="SUBMITTED",
                observed_node=None,
                slots_configured=4,
                backend="slurm_batch",
            )
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=True,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch("peetsfea_runner.pipeline.query_slurm_job_state", return_value=("RUNNING", "n115")),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                worker_row = conn.execute(
                    """
                    SELECT worker_state, observed_node
                    FROM slurm_workers
                    WHERE run_id = ? AND worker_id = 'attempt_0001'
                    """,
                    [run_id],
                ).fetchone()
                event_rows = conn.execute(
                    """
                    SELECT stage
                    FROM events
                    WHERE run_id = ?
                    ORDER BY ts
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(worker_row, ("RUNNING", "n115"))
            self.assertIn("SLURM_WORKERS_REDISCOVERED", [str(row[0]) for row in event_rows])

    def test_slurm_batch_restart_rediscovery_marks_stale_tunnel_as_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            run_id = store.ensure_continuous_run(rotation_hours=24)
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="RUNNING",
                observed_node="n115",
                slots_configured=4,
                backend="slurm_batch",
                tunnel_session_id="session-1",
                tunnel_state="CONNECTED",
                heartbeat_ts="2000-01-01T00:00:00+00:00",
            )
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=True,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                tunnel_heartbeat_timeout_seconds=30,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch("peetsfea_runner.pipeline.query_slurm_job_state", return_value=("RUNNING", "n115")),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                worker_row = conn.execute(
                    """
                    SELECT worker_state, tunnel_state, degraded_reason
                    FROM slurm_workers
                    WHERE run_id = ? AND worker_id = 'attempt_0001'
                    """,
                    [run_id],
                ).fetchone()
                event_rows = conn.execute(
                    """
                    SELECT stage
                    FROM events
                    WHERE run_id = ?
                    ORDER BY ts
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(worker_row, ("RUNNING", "DEGRADED", "tunnel heartbeat stale after main restart"))
            stages = [str(row[0]) for row in event_rows]
            self.assertIn("SLURM_WORKERS_REDISCOVERED", stages)
            self.assertIn("CONTROL_TUNNEL_LOST", stages)

    def test_slurm_batch_restart_rediscovery_does_not_degrade_pending_worker_without_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            run_id = store.ensure_continuous_run(rotation_hours=24)
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="attempt_0001",
                job_id="job_0001",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-harry",
                slurm_job_id="552740",
                worker_state="PENDING",
                observed_node=None,
                slots_configured=4,
                backend="slurm_batch",
                tunnel_state="PENDING",
                heartbeat_ts=None,
            )
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=True,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                tunnel_heartbeat_timeout_seconds=30,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch("peetsfea_runner.pipeline.query_slurm_job_state", return_value=("PENDING", None)),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                worker_row = conn.execute(
                    """
                    SELECT worker_state, tunnel_state, degraded_reason
                    FROM slurm_workers
                    WHERE run_id = ? AND worker_id = 'attempt_0001'
                    """,
                    [run_id],
                ).fetchone()
                event_rows = conn.execute(
                    """
                    SELECT stage
                    FROM events
                    WHERE run_id = ?
                    ORDER BY ts
                    """,
                    [run_id],
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(worker_row, ("PENDING", "PENDING", None))
            stages = [str(row[0]) for row in event_rows]
            self.assertIn("SLURM_WORKERS_REDISCOVERED", stages)
            self.assertNotIn("CONTROL_TUNNEL_LOST", stages)

    def test_stale_worker_heartbeat_helper(self) -> None:
        self.assertTrue(_is_stale_worker_heartbeat(heartbeat_ts=None, timeout_seconds=30))
        self.assertFalse(
            _is_stale_worker_heartbeat(
                heartbeat_ts="2099-01-01T00:00:00+00:00",
                timeout_seconds=30,
            )
        )

    def test_slurm_batch_worker_death_requeues_replacement_worker_and_preserves_lost_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                job_retry_count=0,
                worker_requeue_limit=1,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )
            attempt_counter = {"count": 0}

            def _mock_attempt(
                *,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                **kwargs,
            ):
                attempt_counter["count"] += 1
                if on_upload_success is not None:
                    on_upload_success()
                if attempt_counter["count"] == 1:
                    if on_worker_submitted is not None:
                        on_worker_submitted("552818", None)
                    if on_worker_state_change is not None:
                        on_worker_state_change("PENDING", None)
                        on_worker_state_change("RUNNING", "n108")
                        on_worker_state_change("LOST", "n108")
                    return RemoteJobAttemptResult(
                        success=False,
                        exit_code=13,
                        session_name="s1",
                        case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:13"]),
                        message="worker lost",
                        failed_case_lines=["case_01:13"],
                        failure_category="launch",
                        slurm_job_id="552818",
                        observed_node="n108",
                        worker_terminal_state="LOST",
                    )
                assert local_job_dir is not None
                if on_worker_submitted is not None:
                    on_worker_submitted("552819", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("PENDING", None)
                    on_worker_state_change("RUNNING", "n109")
                    on_worker_state_change("IDLE_DRAINING", "n109")
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s2",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552819",
                    observed_node="n109",
                    worker_terminal_state="COMPLETED",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_jobs, 2)
            self.assertEqual(attempt_counter["count"], 2)
            self.assertEqual(result.terminal_jobs, 1)
            self.assertEqual(result.replacement_jobs, 1)

            conn = duckdb.connect(str(db_path))
            try:
                worker_rows = conn.execute(
                    """
                    SELECT slurm_job_id, worker_state, observed_node
                    FROM slurm_workers
                    ORDER BY submitted_at
                    """
                ).fetchall()
                event_stages = [row[0] for row in conn.execute("SELECT stage FROM events ORDER BY ts").fetchall()]
            finally:
                conn.close()

            self.assertEqual(worker_rows, [("552818", "LOST", "n108"), ("552819", "COMPLETED", "n109")])
            self.assertIn("WORKER_DEATH_DETECTED", event_stages)
            self.assertTrue((output_root / "fixture_01.aedt.out" / "fixture_01.aedt").is_file())

    def test_storage_failure_requeues_slot_to_other_account_without_same_account_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "fixture_01.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                job_retry_count=1,
                worker_requeue_limit=1,
                accounts_registry=(
                    AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),
                    AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=1),
                ),
            )

            seen_hosts: list[str] = []

            def _mock_attempt(*, config=None, local_job_dir=None, on_upload_success=None, **kwargs):
                assert config is not None
                seen_hosts.append(config.host)
                if on_upload_success is not None:
                    on_upload_success()
                if config.host == "gate1-harry":
                    return RemoteJobAttemptResult(
                        success=False,
                        exit_code=13,
                        session_name="s1",
                        case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:13"]),
                        message='scp upload failed: scp: write remote "/home1/harry261/aedt_runs/job_0001/project_01.aedt": Failure',
                        failed_case_lines=["case_01:13"],
                        failure_category="storage",
                    )
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s2",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            readiness_ready = AccountReadinessSnapshot(
                account_id="account_01",
                host_alias="gate1-harry",
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
                uv_ok=True,
                pyaedt_ok=True,
            )

            def _capacity(*, account=None, **kwargs):
                assert account is not None
                return AccountCapacitySnapshot(
                    account_id=account.account_id,
                    host_alias=account.host_alias,
                    running_count=0,
                    pending_count=0,
                    allowed_submit=1,
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch("peetsfea_runner.pipeline.query_account_readiness", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_preflight", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_capacity", side_effect=_capacity),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(seen_hosts, ["gate1-harry", "gate1-dhj02"])

            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute("SELECT state FROM slot_tasks").fetchone()[0]
                attempts = [row[0] for row in conn.execute("SELECT stage FROM slot_events ORDER BY ts").fetchall()]
            finally:
                conn.close()

            self.assertEqual(state, "SUCCEEDED")
            self.assertIn("LEASE_EXPIRED", attempts)
            self.assertNotIn("QUARANTINED", attempts)

    def test_launch_transient_retries_same_account_once_then_requeues_to_other_account(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "fixture_01.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                job_retry_count=0,
                worker_requeue_limit=1,
                accounts_registry=(
                    AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),
                    AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=1),
                ),
            )

            seen_hosts: list[str] = []
            transient_attempts = {"count": 0}

            def _mock_attempt(*, config=None, local_job_dir=None, on_upload_success=None, **kwargs):
                assert config is not None
                seen_hosts.append(config.host)
                if on_upload_success is not None:
                    on_upload_success()
                if config.host == "gate1-harry" and transient_attempts["count"] < 2:
                    transient_attempts["count"] += 1
                    return RemoteJobAttemptResult(
                        success=False,
                        exit_code=13,
                        session_name=f"s{transient_attempts['count']}",
                        case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:13"]),
                        message="sbatch submit failed: Connection closed by 172.16.10.36 port 22",
                        failed_case_lines=[],
                        failure_category="launch_transient",
                    )
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s3",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            readiness_ready = AccountReadinessSnapshot(
                account_id="account_01",
                host_alias="gate1-harry",
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
                uv_ok=True,
                pyaedt_ok=True,
            )

            def _capacity(*, account=None, **kwargs):
                assert account is not None
                return AccountCapacitySnapshot(
                    account_id=account.account_id,
                    host_alias=account.host_alias,
                    running_count=0,
                    pending_count=0,
                    allowed_submit=1,
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch("peetsfea_runner.pipeline.query_account_readiness", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_preflight", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_capacity", side_effect=_capacity),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(seen_hosts, ["gate1-harry", "gate1-harry", "gate1-dhj02"])

            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute("SELECT state FROM slot_tasks").fetchone()[0]
                attempts = [row[0] for row in conn.execute("SELECT stage FROM slot_events ORDER BY ts").fetchall()]
                events = [row[0] for row in conn.execute("SELECT stage FROM events ORDER BY ts").fetchall()]
            finally:
                conn.close()

            self.assertEqual(state, "SUCCEEDED")
            self.assertIn("RETRY_QUEUED", attempts)
            self.assertIn("LEASE_EXPIRED", attempts)
            self.assertIn("RETRY_BACKOFF", events)

    def test_slurm_batch_backend_records_failed_worker_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "fixture_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "fixture_01.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(
                *,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                **kwargs,
            ):
                if on_upload_success is not None:
                    on_upload_success()
                if on_worker_submitted is not None:
                    on_worker_submitted("552818", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("PENDING", None)
                    on_worker_state_change("RUNNING", "n108")
                assert local_job_dir is not None
                (Path(local_job_dir) / "bundle.exit.code").write_text("13", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=False,
                    exit_code=13,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=[]),
                    message="failed",
                    failed_case_lines=["case_01:13"],
                    failure_category="solve",
                    slurm_job_id="552818",
                    observed_node="n108",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT slurm_job_id, worker_state, observed_node, backend
                    FROM slurm_workers
                    ORDER BY last_seen_ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row, ("552818", "FAILED", "n108", "slurm_batch"))

    def test_sample_canary_success_records_cutover_ready_and_canary_passed_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(
                *,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                run_id=None,
                worker_id=None,
                **kwargs,
            ):
                if on_upload_success is not None:
                    on_upload_success()
                if on_worker_submitted is not None:
                    on_worker_submitted("552740", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("RUNNING", "n115")
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                (case_dir / "output_variables.csv").write_text(
                    "source_aedt_path,source_case_dir,source_aedt_name\n/path,/case,sample.aedt\n",
                    encoding="utf-8",
                )
                StateStore(db_path).update_slurm_worker_control_plane(
                    run_id=str(run_id),
                    worker_id=str(worker_id),
                    tunnel_state="CONNECTED",
                    tunnel_session_id="t_01",
                    heartbeat_ts="2026-03-12T04:00:00+00:00",
                    observed_node="n115",
                )
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552740",
                    observed_node="n115",
                    worker_terminal_state="COMPLETED",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                run_pipeline(config)

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT stage FROM events ORDER BY ts").fetchall()
                message_rows = conn.execute("SELECT message FROM events ORDER BY ts").fetchall()
            finally:
                conn.close()
            stages = [str(row[0]) for row in rows]
            messages = [str(row[0]) for row in message_rows]
            self.assertIn("CUTOVER_READY", stages)
            self.assertIn("SLURM_TRUTH_REFRESHED", stages)
            self.assertIn("CANARY_PASSED", stages)
            self.assertIn("FULL_ROLLOUT_READY", stages)
            self.assertIn("SERVICE_BOUNDARY", stages)
            self.assertTrue(any("input_source_policy=sample_only" in message for message in messages))

    def test_sample_canary_allows_full_rollout_when_output_materializes_without_live_return_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                remote_execution_backend="slurm_batch",
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, on_worker_submitted=None, on_worker_state_change=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                if on_worker_submitted is not None:
                    on_worker_submitted("552741", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("RUNNING", "n115")
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                (case_dir / "output_variables.csv").write_text(
                    "source_aedt_path,source_case_dir,source_aedt_name\n/path,/case,sample.aedt\n",
                    encoding="utf-8",
                )
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552741",
                    observed_node="n115",
                    worker_terminal_state="COMPLETED",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT stage, message FROM events ORDER BY ts").fetchall()
            finally:
                conn.close()
            stages = [str(row[0]) for row in rows]
            messages = [str(row[1]) for row in rows]
            self.assertIn("CONTROL_TUNNEL_DEGRADED", stages)
            self.assertIn("CANARY_PASSED", stages)
            self.assertIn("FULL_ROLLOUT_READY", stages)
            self.assertFalse(any("reason=tunnel_not_connected" in message for message in messages if "CANARY_FAILED" in message))

    def test_sample_canary_failure_records_canary_failed_and_rollback_triggered(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                (Path(local_job_dir) / "bundle.exit.code").write_text("13", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=False,
                    exit_code=13,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=[]),
                    message="failed",
                    failed_case_lines=["case_01:13"],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                run_pipeline(config)

            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT stage FROM events ORDER BY ts").fetchall()
            finally:
                conn.close()
            stages = [str(row[0]) for row in rows]
            self.assertIn("CANARY_FAILED", stages)
            self.assertIn("ROLLBACK_TRIGGERED", stages)

    def test_sample_canary_blocked_records_cutover_blocked_without_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                continuous_mode=False,
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        ready=False,
                        status="DISABLED_FOR_DISPATCH",
                        reason="ansys",
                        home_ok=True,
                        runtime_path_ok=True,
                        venv_ok=True,
                        python_ok=True,
                        module_ok=True,
                        binaries_ok=True,
                        ansys_ok=False,
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            conn = duckdb.connect(str(db_path))
            try:
                rows = conn.execute("SELECT stage FROM events ORDER BY ts").fetchall()
            finally:
                conn.close()
            stages = [str(row[0]) for row in rows]
            self.assertIn("CUTOVER_BLOCKED", stages)
            self.assertIn("CANARY_FAILED", stages)
            self.assertIn("ROLLBACK_TRIGGERED", stages)

    def test_validate_rejects_missing_directory(self) -> None:
        missing = Path(tempfile.gettempdir()) / "missing_input_queue"
        config = PipelineConfig(input_queue_dir=str(missing))
        with self.assertRaises(FileNotFoundError):
            config.validate()

    def test_validate_rejects_no_aedt_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "x.txt").write_text("x", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            with self.assertRaises(ValueError):
                config.validate()

    def test_validate_allows_empty_queue_in_continuous_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=True)
            _input_root, _output_root, files, _accounts = config.validate()
            self.assertEqual(files, [])

    def test_validate_auto_creates_ready_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("x", encoding="utf-8")
            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            _input_root, _output_root, files, _accounts = config.validate()
            self.assertEqual(files, [input_file.resolve()])
            self.assertTrue((input_dir / "foo.aedt.ready").is_file())

    def test_validate_does_not_create_ready_sidecar_when_lock_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("x", encoding="utf-8")
            Path(f"{input_file}.lock").write_text("", encoding="utf-8")

            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            _input_root, _output_root, files, _accounts = config.validate()

            self.assertEqual(files, [input_file.resolve()])
            self.assertFalse((input_dir / "foo.aedt.ready").exists())

    def test_validate_follows_symlinked_input_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_dir = root / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            source_dir = root / "source"
            source_dir.mkdir(parents=True, exist_ok=True)
            source_file = source_dir / "foo.aedt"
            source_file.write_text("x", encoding="utf-8")
            linked_dir = input_dir / "linked"
            linked_dir.symlink_to(source_dir, target_is_directory=True)

            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            _input_root, _output_root, files, _accounts = config.validate()

            logical_file = linked_dir / "foo.aedt"
            self.assertEqual(files, [logical_file])
            self.assertTrue((linked_dir / "foo.aedt.ready").is_file())

    def test_validate_recursive_symlink_cycle_does_not_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_dir = root / "in"
            nested = input_dir / "nested"
            nested.mkdir(parents=True, exist_ok=True)
            input_file = nested / "foo.aedt"
            input_file.write_text("x", encoding="utf-8")
            (nested / "back").symlink_to(input_dir, target_is_directory=True)

            config = PipelineConfig(input_queue_dir=str(input_dir), continuous_mode=False)
            _input_root, _output_root, files, _accounts = config.validate()

            self.assertEqual(files, [input_file])
            self.assertTrue((nested / "foo.aedt.ready").is_file())

    def test_dry_run_creates_mirrored_out_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            nested = input_dir / "a" / "b"
            nested.mkdir(parents=True, exist_ok=True)
            (nested / "foo.aedt").write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
            )

            result = run_pipeline(config)

            expected = output_root / "a" / "b" / "foo.aedt.out"
            self.assertTrue(expected.is_dir())
            self.assertTrue((expected / "foo.aedt").is_file())
            self.assertIsInstance(result, PipelineResult)
            self.assertTrue(result.success)
            self.assertEqual(result.exit_code, EXIT_CODE_SUCCESS)
            self.assertEqual(result.version, __version__)
            self.assertEqual(result.total_jobs, 1)
            self.assertIn("failed_job_ids=[]", result.summary)
            self.assertTrue((nested / "foo.aedt.ready").is_file())

    def test_continuous_mode_schedules_existing_queued_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            store = StateStore(db_path)
            store.initialize()
            store.start_run("run_01")
            store.create_slot_task(
                run_id="run_01",
                slot_id="w_backlog_0001",
                input_path=str(input_dir / "backlog.aedt"),
                output_path=str(output_root / "backlog.aedt.out"),
                account_id=None,
                state="QUEUED",
            )
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
            )

            result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_slots, 1)
            self.assertEqual(result.total_jobs, 1)
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute(
                    "SELECT state FROM slot_tasks WHERE run_id = ? AND slot_id = ?",
                    ["run_01", "w_backlog_0001"],
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "SUCCEEDED")

    def test_pipeline_dispatches_backlog_without_batch_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            for index in range(1, 3):
                input_file = input_dir / f"sample_{index:02d}.aedt"
                input_file.write_text("placeholder", encoding="utf-8")

            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=False,
            )
            dispatched_sizes: list[int] = []

            def _mock_run_slot_workers(*, slot_queue, job_index_start, **kwargs):
                dispatched_sizes.append(len(slot_queue))
                conn = duckdb.connect(str(db_path))
                try:
                    conn.execute(
                        """
                        UPDATE slot_tasks
                        SET state = 'SUCCEEDED',
                            updated_at = now()
                        """
                    )
                finally:
                    conn.close()
                return SimpleNamespace(
                    results=[],
                    max_inflight_jobs=0,
                    submitted_jobs=0,
                    terminal_jobs=0,
                    replacement_jobs=0,
                )

            with patch("peetsfea_runner.pipeline.run_slot_workers", side_effect=_mock_run_slot_workers):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_slots, 2)
            self.assertEqual(dispatched_sizes, [2])
            for index in range(1, 3):
                self.assertTrue((input_dir / f"sample_{index:02d}.aedt.ready").is_file())

    def test_continuous_mode_dispatches_before_full_ingest_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            for index in range(1, 31):
                input_file = input_dir / f"sample_{index:02d}.aedt"
                input_file.write_text("placeholder", encoding="utf-8")

            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
            )
            discovered_counts_at_dispatch: list[int] = []
            dispatched_sizes: list[int] = []

            class _FakeController:
                def __init__(self, **kwargs):
                    self._pending: list = []
                    self._submitted = 0

                def enqueue_slots(self, slots, flush_partial=False):
                    del flush_partial
                    self._pending.extend(slots)

                def snapshot(self):
                    return SimpleNamespace(
                        queued_slots=0,
                        pending_slots=len(self._pending),
                        inflight_slots=0,
                        inflight_jobs=0,
                        submitted_jobs=self._submitted,
                    )

                def has_work(self):
                    return bool(self._pending)

                def step(self, *, wait_for_progress=False, flush_partial_bundles=False):
                    del wait_for_progress, flush_partial_bundles
                    if not self._pending:
                        return False
                    conn = duckdb.connect(str(db_path))
                    try:
                        discovered_counts_at_dispatch.append(
                            conn.execute("SELECT COUNT(*) FROM slot_tasks").fetchone()[0]
                        )
                        dispatched_sizes.append(len(self._pending))
                        conn.executemany(
                            """
                            UPDATE slot_tasks
                            SET state = 'SUCCEEDED',
                                updated_at = now()
                            WHERE slot_id = ?
                            """,
                            [[slot.slot_id] for slot in self._pending],
                        )
                    finally:
                        conn.close()
                    self._submitted += max(1, (len(self._pending) + 3) // 4)
                    self._pending = []
                    return True

                def finalize(self):
                    return SimpleNamespace(
                        results=[],
                        max_inflight_jobs=0,
                        submitted_jobs=self._submitted,
                        terminal_jobs=0,
                        replacement_jobs=0,
                    )

            with (
                patch("peetsfea_runner.pipeline._continuous_backlog_limits", return_value=(12, 24)),
                patch("peetsfea_runner.pipeline.SlotWorkerController", _FakeController),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_slots, 30)
            self.assertGreaterEqual(len(dispatched_sizes), 1)
            self.assertEqual(dispatched_sizes[0], 4)
            self.assertEqual(discovered_counts_at_dispatch[0], 4)
            self.assertLess(discovered_counts_at_dispatch[0], 30)
            for index in range(1, 31):
                self.assertTrue((input_dir / f"sample_{index:02d}.aedt.ready").is_file())

    def test_continuous_mode_rescans_and_ingests_late_arriving_file_during_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            first_file = input_dir / "sample_01.aedt"
            late_file = input_dir / "sample_02.aedt"
            first_file.write_text("placeholder", encoding="utf-8")
            late_file.write_text("placeholder", encoding="utf-8")

            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
                ingest_poll_seconds=1,
                slots_per_job=1,
            )
            submitted_sizes: list[int] = []
            scan_calls = 0

            def _mock_scan(*, input_root, recursive):
                nonlocal scan_calls
                scan_calls += 1
                if scan_calls == 1:
                    return [first_file]
                return [first_file, late_file]

            class _FakeController:
                def __init__(self, **kwargs):
                    self._pending: list = []
                    self._step_calls = 0
                    self._submitted = 0

                def enqueue_slots(self, slots, flush_partial=False):
                    del flush_partial
                    self._pending.extend(slots)

                def snapshot(self):
                    return SimpleNamespace(
                        queued_slots=0,
                        pending_slots=len(self._pending),
                        inflight_slots=0,
                        inflight_jobs=0,
                        submitted_jobs=self._submitted,
                    )

                def has_work(self):
                    return bool(self._pending)

                def step(self, *, wait_for_progress=False, flush_partial_bundles=False):
                    del wait_for_progress, flush_partial_bundles
                    self._step_calls += 1
                    if not self._pending:
                        return False
                    if len(self._pending) < 2:
                        return False
                    submitted_sizes.append(len(self._pending))
                    conn = duckdb.connect(str(db_path))
                    try:
                        conn.executemany(
                            """
                            UPDATE slot_tasks
                            SET state = 'SUCCEEDED',
                                updated_at = now()
                            WHERE slot_id = ?
                            """,
                            [[slot.slot_id] for slot in self._pending],
                        )
                    finally:
                        conn.close()
                    self._submitted += 1
                    self._pending = []
                    return True

                def finalize(self):
                    return SimpleNamespace(
                        results=[],
                        max_inflight_jobs=0,
                        submitted_jobs=self._submitted,
                        terminal_jobs=0,
                        replacement_jobs=0,
                    )

            monotonic_tick = {"value": -1.0}

            def _mock_monotonic():
                monotonic_tick["value"] += 1.0
                return min(monotonic_tick["value"], 3.0)

            with (
                patch("peetsfea_runner.pipeline._continuous_backlog_limits", return_value=(1, 1)),
                patch("peetsfea_runner.pipeline._scan_input_aedt_files", side_effect=_mock_scan),
                patch("peetsfea_runner.pipeline.SlotWorkerController", _FakeController),
                patch("peetsfea_runner.pipeline.time.monotonic", side_effect=_mock_monotonic),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_slots, 2)
            self.assertGreaterEqual(scan_calls, 2)
            self.assertEqual(submitted_sizes, [2])
            self.assertTrue((input_dir / "sample_01.aedt.ready").is_file())
            self.assertTrue((input_dir / "sample_02.aedt.ready").is_file())

    def test_continuous_mode_ingests_without_ready_sidecar_when_internal_ready_fallback_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")

            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
            )

            with patch(
                "peetsfea_runner.pipeline._ensure_ready_artifact",
                return_value=SimpleNamespace(
                    ready_path=Path(f"{input_file}.ready"),
                    ready_present=False,
                    ready_mode="INTERNAL_ONLY",
                    ready_error="read-only filesystem",
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertFalse((input_dir / "sample_01.aedt.ready").exists())
            self.assertTrue((output_root / "sample_01.aedt.out" / "sample_01.aedt").is_file())

            conn = duckdb.connect(str(db_path))
            try:
                ingest_row = conn.execute(
                    """
                    SELECT ready_present, ready_mode, ready_error, state
                    FROM ingest_index
                    WHERE input_path = ?
                    """,
                    [str(input_file.resolve())],
                ).fetchone()
                stages = [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT stage
                        FROM slot_events
                        ORDER BY ts
                        """
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertEqual(
                (bool(ingest_row[0]), ingest_row[1], ingest_row[2], ingest_row[3]),
                (False, "INTERNAL_ONLY", "read-only filesystem", "SUCCEEDED"),
            )
            self.assertIn("READY_INTERNAL", stages)

    def test_continuous_mode_skips_locked_input_without_creating_ready_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "sample_01.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            Path(f"{input_file}.lock").write_text("", encoding="utf-8")

            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=False,
                continuous_mode=True,
            )

            result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_slots, 0)
            self.assertFalse((input_dir / "sample_01.aedt.ready").exists())
            self.assertFalse((output_root / "sample_01.aedt.out").exists())

            conn = duckdb.connect(str(db_path))
            try:
                slot_count = conn.execute("SELECT COUNT(*) FROM slot_tasks").fetchone()[0]
                ingest_count = conn.execute("SELECT COUNT(*) FROM ingest_index").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(slot_count, 0)
            self.assertEqual(ingest_count, 0)

    def test_materialized_terminal_output_deletes_input_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            ready_file = input_dir / "foo.aedt.ready"
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            self.assertFalse(input_file.exists())
            self.assertFalse(ready_file.exists())
            self.assertTrue((output_root / "foo.aedt.out" / "foo.aedt").is_file())
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute(
                    "SELECT delete_final_state FROM file_lifecycle ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()[0]
                ingest_state = conn.execute(
                    "SELECT state FROM ingest_index ORDER BY discovered_at DESC LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "DELETED")
            self.assertEqual(ingest_state, "DELETED")

    def test_terminal_output_is_retained_when_delete_policy_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            ready_file = input_dir / "foo.aedt.ready"
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                delete_input_after_upload=False,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            self.assertTrue(input_file.exists())
            self.assertTrue(ready_file.exists())
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute(
                    "SELECT delete_final_state FROM file_lifecycle ORDER BY updated_at DESC LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "RETAINED")

    def test_remote_attempt_records_slot_lifecycle_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                local_job_dir = kwargs["local_job_dir"]
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "project.aedt.q.complete").write_text("done", encoding="utf-8")
                (case_dir / "project.aedtresults" / "trace.txt").write_text("trace", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                (Path(local_job_dir) / "case_summary.txt").write_text("case_01:0\n", encoding="utf-8")
                (Path(local_job_dir) / "failed.count").write_text("0\n", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            conn = duckdb.connect(str(db_path))
            try:
                stages = [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT stage
                        FROM slot_events
                        WHERE slot_id IN (SELECT slot_id FROM slot_tasks LIMIT 1)
                        """
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertIn("LEASED", stages)
            self.assertIn("DOWNLOADING", stages)
            self.assertIn("UPLOADING", stages)
            self.assertIn("SUCCEEDED", stages)
            self.assertNotIn("RUNNING", stages)

    def test_materialize_slot_outputs_mirrors_case_outputs_into_out_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_file = root / "nested" / "sample.aedt"
            input_file.parent.mkdir(parents=True, exist_ok=True)
            input_file.write_text("input", encoding="utf-8")
            output_dir = root / "out" / "nested" / "sample.aedt.out"
            local_bundle_dir = root / "bundle"
            case_dir = local_bundle_dir / "case_01"
            (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
            (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
            (case_dir / "project.aedt.q.complete").write_text("done", encoding="utf-8")
            (case_dir / "output_variables.csv").write_text(
                "source_aedt_path,source_case_dir,source_aedt_name,Loss\n"
                "/tmp/project.aedt,/tmp/case_01,project.aedt,1.23\n",
                encoding="utf-8",
            )
            (case_dir / "output_variables.error.log").write_text("warn", encoding="utf-8")
            (case_dir / "project.aedtresults" / "trace.txt").write_text("trace", encoding="utf-8")
            (case_dir / "run.log").write_text("log", encoding="utf-8")
            (case_dir / "exit.code").write_text("0", encoding="utf-8")
            (local_bundle_dir / "case_summary.txt").write_text("case_01:0\n", encoding="utf-8")
            (local_bundle_dir / "failed.count").write_text("0\n", encoding="utf-8")

            _materialize_slot_outputs(
                local_bundle_dir=local_bundle_dir,
                slots=[
                    SimpleNamespace(
                        slot_id="w_001",
                        input_path=input_file,
                        output_dir=output_dir,
                    )
                ],
            )

            self.assertTrue((output_dir / "sample.aedt").is_file())
            self.assertTrue((output_dir / "sample.aedt.q.complete").is_file())
            self.assertTrue((output_dir / "sample.aedtresults").is_dir())
            self.assertTrue((output_dir / "sample.aedtresults" / "trace.txt").is_file())
            self.assertTrue((output_dir / "output_variables.csv").is_file())
            self.assertTrue((output_dir / "output_variables.error.log").is_file())
            self.assertTrue((output_dir / "run.log").is_file())
            self.assertTrue((output_dir / "exit.code").is_file())
            self.assertEqual((output_dir / "case_summary.txt").read_text(encoding="utf-8"), "case_01:0\n")
            self.assertEqual((output_dir / "failed.count").read_text(encoding="utf-8"), "0\n")
            self.assertEqual(
                (output_dir / "output_variables.csv").read_text(encoding="utf-8"),
                "source_aedt_path,source_case_dir,source_aedt_name,Loss\n"
                "/tmp/project.aedt,/tmp/case_01,project.aedt,1.23\n",
            )

    def test_failed_remote_attempt_still_materializes_case_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                (Path(local_job_dir) / "bundle.exit.code").write_text("1", encoding="utf-8")
                (Path(local_job_dir) / "remote_submission.log").write_text("launch stderr", encoding="utf-8")
                (Path(local_job_dir) / "failure_category.txt").write_text("solve", encoding="utf-8")
                (Path(local_job_dir) / "failure_reason.txt").write_text("failed", encoding="utf-8")
                (Path(local_job_dir) / "failed_case_lines.txt").write_text("case_01:1\n", encoding="utf-8")
                (Path(local_job_dir) / "case_summary.txt").write_text("case_01:1\n", encoding="utf-8")
                (Path(local_job_dir) / "failed.count").write_text("1\n", encoding="utf-8")
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "project.aedt.q.complete").write_text("done", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("1", encoding="utf-8")
                ((case_dir / "project.aedtresults") / "trace.txt").write_text("trace", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=False,
                    exit_code=1,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:1"]),
                    message="failed",
                    failed_case_lines=["case_01:1"],
                    failure_category="solve",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            out_dir = output_root / "foo.aedt.out"
            self.assertTrue((out_dir / "foo.aedt").is_file())
            self.assertTrue((out_dir / "foo.aedt.q.complete").is_file())
            self.assertTrue((out_dir / "foo.aedtresults").is_dir())
            self.assertTrue((out_dir / "run.log").is_file())
            self.assertTrue((out_dir / "exit.code").is_file())
            self.assertEqual((out_dir / "failure_category.txt").read_text(encoding="utf-8"), "solve")
            self.assertEqual((out_dir / "failed_case_lines.txt").read_text(encoding="utf-8"), "case_01:1\n")
            self.assertEqual((out_dir / "case_summary.txt").read_text(encoding="utf-8"), "case_01:1\n")
            self.assertEqual((out_dir / "failed.count").read_text(encoding="utf-8"), "1\n")

    def test_successful_remote_attempt_without_case_output_is_not_marked_succeeded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertFalse((output_root / "foo.aedt.out").exists())

    def test_launch_failure_materializes_bundle_failure_artifacts_into_out_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                worker_requeue_limit=0,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                bundle_dir = Path(local_job_dir)
                (bundle_dir / "bundle.exit.code").write_text("13", encoding="utf-8")
                (bundle_dir / "remote_stdout.log").write_text("stdout log", encoding="utf-8")
                (bundle_dir / "remote_stderr.log").write_text("stderr log", encoding="utf-8")
                (bundle_dir / "remote_submission.log").write_text("submission log\nstderr log\n", encoding="utf-8")
                (bundle_dir / "failure_category.txt").write_text("launch", encoding="utf-8")
                (bundle_dir / "failure_reason.txt").write_text("ssh failed", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=False,
                    exit_code=13,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                    message="ssh failed",
                    failed_case_lines=[],
                    failure_category="launch",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            out_dir = output_root / "foo.aedt.out"
            self.assertTrue((out_dir / "foo.aedt").is_file())
            self.assertEqual((out_dir / "failure_category.txt").read_text(encoding="utf-8"), "launch")
            self.assertEqual((out_dir / "failure_reason.txt").read_text(encoding="utf-8"), "ssh failed")
            self.assertEqual((out_dir / "exit.code").read_text(encoding="utf-8"), "13")
            self.assertEqual((out_dir / "run.log").read_text(encoding="utf-8"), "submission log\nstderr log\n")
            self.assertTrue((out_dir / "remote_stderr.log").is_file())

            conn = duckdb.connect(str(db_path))
            try:
                state, failure_reason = conn.execute(
                    "SELECT state, failure_reason FROM slot_tasks LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(state, "QUARANTINED")
            self.assertEqual(failure_reason, "LAUNCH: ssh failed")

    def test_worker_failure_without_materialized_output_requeues_and_succeeds_within_same_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                worker_requeue_limit=1,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )
            attempt_counter = {"count": 0}
            readiness_ready = AccountReadinessSnapshot(
                account_id="account_01",
                host_alias="gate1-harry",
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
                uv_ok=True,
                pyaedt_ok=True,
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                attempt_counter["count"] += 1
                if on_upload_success is not None:
                    on_upload_success()
                if attempt_counter["count"] == 1:
                    return RemoteJobAttemptResult(
                        success=False,
                        exit_code=1,
                        session_name="s1",
                        case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:1"]),
                        message="launch failed",
                        failed_case_lines=["case_01:1"],
                    )
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s2",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch("peetsfea_runner.pipeline.query_account_readiness", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_preflight", return_value=readiness_ready),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_jobs, 2)
            self.assertEqual(attempt_counter["count"], 2)
            self.assertIn("terminal_jobs=1", result.summary)
            self.assertIn("replacement_jobs=1", result.summary)
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute("SELECT state FROM slot_tasks LIMIT 1").fetchone()[0]
                stages = [row[0] for row in conn.execute("SELECT stage FROM slot_events ORDER BY ts").fetchall()]
            finally:
                conn.close()
            self.assertEqual(state, "SUCCEEDED")
            self.assertIn("LEASE_EXPIRED", stages)
            self.assertTrue((output_root / "foo.aedt.out" / "foo.aedt").is_file())

    def test_worker_failure_without_materialized_output_quarantines_after_requeue_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                worker_requeue_limit=1,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )
            readiness_ready = AccountReadinessSnapshot(
                account_id="account_01",
                host_alias="gate1-harry",
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
                uv_ok=True,
                pyaedt_ok=True,
            )

            def _mock_attempt(*, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                return RemoteJobAttemptResult(
                    success=False,
                    exit_code=1,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=0, failed_cases=1, case_lines=["case_01:1"]),
                    message="launch failed",
                    failed_case_lines=["case_01:1"],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("peetsfea_runner.scheduler.time.sleep"),
                patch("peetsfea_runner.pipeline.query_account_readiness", return_value=readiness_ready),
                patch("peetsfea_runner.pipeline.query_account_preflight", return_value=readiness_ready),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertEqual(result.total_jobs, 2)
            self.assertIn("terminal_jobs=2", result.summary)
            self.assertIn("replacement_jobs=1", result.summary)
            conn = duckdb.connect(str(db_path))
            try:
                state = conn.execute("SELECT state FROM slot_tasks LIMIT 1").fetchone()[0]
                retry_count = conn.execute(
                    "SELECT COUNT(*) FROM slot_events WHERE stage = 'RETRY_QUEUED'"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "QUARANTINED")
            self.assertEqual(retry_count, 0)
            conn = duckdb.connect(str(db_path))
            try:
                lease_expired_count = conn.execute(
                    "SELECT COUNT(*) FROM slot_events WHERE stage = 'LEASE_EXPIRED'"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(lease_expired_count, 1)

    def test_slurm_batch_prefetches_backlog_into_single_worker_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            for index in range(1, 6):
                input_file = input_dir / f"sample_{index:02d}.aedt"
                input_file.write_text("placeholder", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                remote_execution_backend="slurm_batch",
                slots_per_job=4,
                worker_bundle_multiplier=2,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            submitted_slot_counts: list[int] = []
            worker_states: list[tuple[str, str | None]] = []

            def _mock_attempt(
                *,
                slot_inputs=None,
                local_job_dir=None,
                on_upload_success=None,
                on_worker_submitted=None,
                on_worker_state_change=None,
                **kwargs,
            ):
                if on_upload_success is not None:
                    on_upload_success()
                assert slot_inputs is not None
                assert local_job_dir is not None
                if on_worker_submitted is not None:
                    on_worker_submitted("552940", None)
                if on_worker_state_change is not None:
                    on_worker_state_change("PENDING", None)
                    on_worker_state_change("RUNNING", "n108")
                    on_worker_state_change("IDLE_DRAINING", "n108")
                worker_states.extend(
                    [
                        ("PENDING", None),
                        ("RUNNING", "n108"),
                        ("IDLE_DRAINING", "n108"),
                    ]
                )
                submitted_slot_counts.append(len(slot_inputs))
                for index, _slot in enumerate(slot_inputs, start=1):
                    case_dir = Path(local_job_dir) / f"case_{index:02d}"
                    (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                    (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                    (case_dir / "run.log").write_text("log", encoding="utf-8")
                    (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=len(slot_inputs), failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                    slurm_job_id="552940",
                    observed_node="n108",
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=1,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_jobs, 1)
            self.assertEqual(submitted_slot_counts, [5])
            self.assertEqual(
                worker_states,
                [("PENDING", None), ("RUNNING", "n108"), ("IDLE_DRAINING", "n108")],
            )

            conn = duckdb.connect(str(db_path))
            try:
                worker_rows = conn.execute(
                    """
                    SELECT slurm_job_id, worker_state, observed_node, slots_configured
                    FROM slurm_workers
                    ORDER BY last_seen_ts
                    """
                ).fetchall()
                slot_rows = conn.execute(
                    """
                    SELECT job_id, COUNT(*)
                    FROM slot_tasks
                    GROUP BY job_id
                    ORDER BY job_id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(worker_rows, [("552940", "COMPLETED", "n108", 4)])
            self.assertEqual(slot_rows, [("job_0001", 5)])

    def test_delete_failure_moves_to_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            (input_dir / "foo.aedt.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            quarantine_root = Path(tmpdir) / "delete_failed"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                delete_failed_quarantine_dir=str(quarantine_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            def _mock_attempt(*, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                case_dir = Path(local_job_dir) / "case_01"
                (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                (case_dir / "run.log").write_text("log", encoding="utf-8")
                (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=1, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch("pathlib.Path.unlink", side_effect=OSError("locked")),
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                run_pipeline(config)

            quarantined_path = quarantine_root / "foo.aedt"
            self.assertTrue(quarantined_path.exists())

    def test_remote_pipeline_blocks_dispatch_for_unready_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        ready=False,
                        status="DISABLED_FOR_DISPATCH",
                        reason="venv,python",
                        home_ok=True,
                        runtime_path_ok=True,
                        venv_ok=False,
                        python_ok=False,
                        module_ok=True,
                        binaries_ok=True,
                        ansys_ok=True,
                    ),
                ),
                patch("peetsfea_runner.pipeline.run_slot_workers") as run_workers_mock,
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertEqual(result.total_jobs, 0)
            self.assertEqual(result.exit_code, 13)
            self.assertIn("blocked_accounts=['account_01']", result.summary)
            self.assertIn("readiness_blocked_slots=1", result.summary)
            run_workers_mock.assert_not_called()

            conn = duckdb.connect(str(db_path))
            try:
                slot_state = conn.execute("SELECT state FROM slot_tasks LIMIT 1").fetchone()[0]
                readiness_row = conn.execute(
                    """
                    SELECT ready, status, reason
                    FROM account_readiness_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(slot_state, "QUEUED")
            self.assertEqual((bool(readiness_row[0]), readiness_row[1], readiness_row[2]), (False, "DISABLED_FOR_DISPATCH", "venv,python"))

    def test_remote_pipeline_bootstraps_account_before_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        ready=False,
                        status="BOOTSTRAP_REQUIRED",
                        reason="venv,python",
                        home_ok=True,
                        runtime_path_ok=True,
                        venv_ok=False,
                        python_ok=False,
                        module_ok=True,
                        binaries_ok=True,
                        ansys_ok=True,
                    ),
                ),
                patch("peetsfea_runner.pipeline.bootstrap_account_runtime", return_value="__PEETSFEA_BOOTSTRAP__:ok"),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                        uv_ok=True,
                        pyaedt_ok=True,
                    ),
                ),
            ):
                def _mock_run_slot_workers(*args, **kwargs):
                    conn = duckdb.connect(str(db_path))
                    try:
                        conn.execute(
                            """
                            UPDATE slot_tasks
                            SET state = 'SUCCEEDED',
                                job_id = 'job_0001',
                                account_id = 'account_01',
                                attempt_no = 1,
                                updated_at = now()
                            """
                        )
                    finally:
                        conn.close()
                    return SimpleNamespace(
                        results=[
                            _BundleRuntimeOutcome(
                                job_id="job_0001",
                                success=True,
                                quarantined=False,
                                exit_code=0,
                                message="ok",
                                success_slots=1,
                                failed_slots=0,
                                quarantined_slots=0,
                            )
                        ],
                        max_inflight_jobs=1,
                        submitted_jobs=1,
                        terminal_jobs=0,
                        replacement_jobs=0,
                    )

                with patch("peetsfea_runner.pipeline.run_slot_workers", side_effect=_mock_run_slot_workers) as run_workers_mock:
                    result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertIn("bootstrapping_accounts=['account_01']", result.summary)
            self.assertIn("ready_accounts=['account_01']", result.summary)
            run_workers_mock.assert_called_once()

            conn = duckdb.connect(str(db_path))
            try:
                statuses = [
                    row[0]
                    for row in conn.execute(
                        "SELECT status FROM account_readiness_snapshots ORDER BY ts"
                    ).fetchall()
                ]
            finally:
                conn.close()
            self.assertIn("BOOTSTRAP_REQUIRED", statuses)
            self.assertIn("BOOTSTRAPPING", statuses)
            self.assertIn("READY", statuses)

    def test_remote_pipeline_records_preflight_failure_and_skips_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            input_file = input_dir / "foo.aedt"
            input_file.write_text("placeholder", encoding="utf-8")
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=1),),
            )

            with (
                patch(
                    "peetsfea_runner.pipeline.query_account_readiness",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
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
                ),
                patch(
                    "peetsfea_runner.pipeline.query_account_preflight",
                    return_value=AccountReadinessSnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        ready=False,
                        status="PREFLIGHT_FAILED",
                        reason="uv,pyaedt",
                        home_ok=True,
                        runtime_path_ok=True,
                        venv_ok=True,
                        python_ok=True,
                        module_ok=True,
                        binaries_ok=True,
                        ansys_ok=True,
                        uv_ok=False,
                        pyaedt_ok=False,
                    ),
                ),
                patch("peetsfea_runner.pipeline.run_slot_workers") as run_workers_mock,
            ):
                result = run_pipeline(config)

            self.assertFalse(result.success)
            self.assertIn("blocked_accounts=['account_01']", result.summary)
            self.assertIn("readiness_blocked_slots=1", result.summary)
            run_workers_mock.assert_not_called()

            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute(
                    """
                    SELECT status, reason, uv_ok, pyaedt_ok
                    FROM account_readiness_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual((row[0], row[1], bool(row[2]), bool(row[3])), ("PREFLIGHT_FAILED", "uv,pyaedt", False, False))

    def test_remote_pipeline_caps_worker_jobs_at_account_max(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "in"
            input_dir.mkdir(parents=True, exist_ok=True)
            for index in range(1, 86):
                input_file = input_dir / f"sample_{index:03d}.aedt"
                input_file.write_text("placeholder", encoding="utf-8")
                Path(f"{input_file}.ready").write_text("", encoding="utf-8")
            output_root = Path(tmpdir) / "out"
            db_path = Path(tmpdir) / "state.duckdb"
            config = PipelineConfig(
                input_queue_dir=str(input_dir),
                output_root_dir=str(output_root),
                metadata_db_path=str(db_path),
                execute_remote=True,
                continuous_mode=False,
                job_retry_count=0,
                accounts_registry=(AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),),
            )

            def _mock_attempt(*, slot_inputs=None, local_job_dir=None, on_upload_success=None, **kwargs):
                if on_upload_success is not None:
                    on_upload_success()
                assert local_job_dir is not None
                case_count = len(slot_inputs or [])
                for index in range(1, case_count + 1):
                    case_dir = Path(local_job_dir) / f"case_{index:02d}"
                    (case_dir / "project.aedtresults").mkdir(parents=True, exist_ok=True)
                    (case_dir / "project.aedt").write_text("simulated", encoding="utf-8")
                    (case_dir / "run.log").write_text("log", encoding="utf-8")
                    (case_dir / "exit.code").write_text("0", encoding="utf-8")
                return RemoteJobAttemptResult(
                    success=True,
                    exit_code=0,
                    session_name="s",
                    case_summary=CaseExecutionSummary(success_cases=case_count, failed_cases=0, case_lines=[]),
                    message="ok",
                    failed_case_lines=[],
                )

            with (
                patch("peetsfea_runner.pipeline.run_remote_job_attempt", side_effect=_mock_attempt),
                patch("peetsfea_runner.pipeline.cleanup_orphan_session"),
                patch("peetsfea_runner.pipeline.cleanup_orphan_sessions_for_run"),
                patch(
                    "peetsfea_runner.pipeline.query_account_capacity",
                    return_value=AccountCapacitySnapshot(
                        account_id="account_01",
                        host_alias="gate1-harry",
                        running_count=0,
                        pending_count=0,
                        allowed_submit=10,
                    ),
                ),
            ):
                result = run_pipeline(config)

            self.assertTrue(result.success)
            self.assertEqual(result.total_jobs, 22)
            self.assertEqual(result.total_slots, 85)


if __name__ == "__main__":
    unittest.main()
