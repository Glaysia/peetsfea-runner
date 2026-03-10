from __future__ import annotations

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
    _materialize_slot_outputs,
)
from peetsfea_runner.remote_job import CaseExecutionSummary, RemoteJobAttemptResult
from peetsfea_runner.scheduler import AccountCapacitySnapshot, AccountReadinessSnapshot
from peetsfea_runner.state_store import StateStore


class TestPipelineApi(unittest.TestCase):
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
            self.assertEqual(len(config.accounts_registry), 1)
            self.assertTrue(config.continuous_mode)
            self.assertEqual(config.ready_sidecar_suffix, ".ready")
            self.assertEqual(config.run_rotation_hours, 24)
            self.assertFalse((input_dir / "sample.aedt.ready").exists())

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

                def enqueue_slots(self, slots):
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

                def step(self, *, wait_for_progress=False):
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

            self.assertIn("ASSIGNED", stages)
            self.assertIn("UPLOADING", stages)
            self.assertIn("RUNNING", stages)
            self.assertIn("SUCCEEDED", stages)

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
            self.assertTrue((output_dir / "run.log").is_file())
            self.assertTrue((output_dir / "exit.code").is_file())
            self.assertEqual((output_dir / "case_summary.txt").read_text(encoding="utf-8"), "case_01:0\n")
            self.assertEqual((output_dir / "failed.count").read_text(encoding="utf-8"), "0\n")

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
            self.assertIn("RETRY_QUEUED", stages)
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
            self.assertEqual(retry_count, 1)

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
