from __future__ import annotations

from dataclasses import replace
import signal
import sys
import types

from pathlib import Path

import pytest

from peetsfea_runner.built_in_service import AccountConfig, LaneSpec, ServiceProfile
from peetsfea_runner import built_in_service


def _make_test_profile(*, root: Path) -> ServiceProfile:
    return ServiceProfile(
        repo_root=root,
        ssh_config_path=None,
        input_queue_root=root / "input_queue",
        output_root=root / "output",
        delete_failed_root=root / "output" / "_delete_failed",
        state_path=root / "peetsfea_runner.state",
        control_plane_host="127.0.0.1",
        control_plane_port=8765,
        control_plane_ssh_target="user@127.0.0.1",
        control_plane_return_host="127.0.0.1",
        control_plane_return_port=22,
        control_plane_return_user="peets",
        web_host="127.0.0.1",
        web_port=8765,
        poll_seconds=30,
        heartbeat_seconds=15,
        recovery_poll_seconds=5,
        blocked_poll_seconds=30,
        autorecovery_min_interval_seconds=60,
        ingest_poll_seconds=120,
        lanes=(
            LaneSpec(
                lane_id="prune_results",
                input_root=root / "input_queue" / "prune_results",
                output_root=root / "output" / "prune_results",
                accounts=(
                    AccountConfig(account_id="acct-a", host_alias="gate1-harry261", max_jobs=1),
                    AccountConfig(account_id="acct-b", host_alias="gate1-jji0930", max_jobs=1),
                ),
                cpus_per_job=60,
                slots_per_job=15,
                cores_per_slot=4,
                tasks_per_slot=1,
                retain_aedtresults=False,
                delete_input_after_upload=False,
                rename_input_to_done_on_success=True,
            ),
        ),
    )


class _FakeStateStore:
    def __init__(self, state_path: Path) -> None:
        self.state_path = state_path

    def list_active_slurm_workers(self) -> list[dict[str, object]]:
        return [
            {"host_alias": "gate1-harry261", "slurm_job_id": "101"},
            {"host_alias": "row-extra", "account_id": "from-row", "slurm_job_id": "202"},
            {"host_alias": "", "slurm_job_id": "303"},
        ]


def test_scancel_service_slurm_jobs_uses_active_worker_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    profile = _make_test_profile(root=tmp_path)
    cleanup_calls: dict[str, object] = {}

    fake_cleanup_module = types.ModuleType("peetsfea_runner.slurm_cleanup")

    def fake_cleanup_slurm_workers(
        *, accounts: list[AccountConfig], active_worker_rows: list[dict[str, object]], ssh_config_path: str, run_command
    ) -> None:
        cleanup_calls["accounts"] = accounts
        cleanup_calls["rows"] = active_worker_rows
        cleanup_calls["ssh"] = ssh_config_path
        cleanup_calls["runner"] = run_command

    fake_cleanup_module.cleanup_slurm_workers = fake_cleanup_slurm_workers
    monkeypatch.setitem(sys.modules, "peetsfea_runner.slurm_cleanup", fake_cleanup_module)
    monkeypatch.setattr(built_in_service, "build_service_profile", lambda: profile)
    monkeypatch.setattr(built_in_service, "StateStore", _FakeStateStore)

    built_in_service.scancel_service_slurm_jobs()

    accounts = cleanup_calls["accounts"]
    assert isinstance(accounts, list)
    assert [acc.host_alias for acc in accounts] == [
        "gate1-harry261",
        "gate1-jji0930",
        "row-extra",
    ]
    assert cleanup_calls["rows"] == _FakeStateStore(profile.state_path).list_active_slurm_workers()
    assert cleanup_calls["ssh"] == ""
    assert cleanup_calls["runner"] is not None


def test_scancel_service_slurm_jobs_swallow_cleanup_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    profile = _make_test_profile(root=tmp_path)

    class _NoRowsStateStore:
        def __init__(self, state_path: Path) -> None:
            self.state_path = state_path

        def list_active_slurm_workers(self) -> list[dict[str, object]]:
            return []

    fake_cleanup_module = types.ModuleType("peetsfea_runner.slurm_cleanup")

    def failing_cleanup_slurm_workers(
        *, accounts: list[AccountConfig], active_worker_rows: list[dict[str, object]], ssh_config_path: str, run_command
    ) -> None:
        raise RuntimeError("cleanup failed")

    fake_cleanup_module.cleanup_slurm_workers = failing_cleanup_slurm_workers
    monkeypatch.setitem(sys.modules, "peetsfea_runner.slurm_cleanup", fake_cleanup_module)
    monkeypatch.setattr(built_in_service, "build_service_profile", lambda: profile)
    monkeypatch.setattr(built_in_service, "StateStore", _NoRowsStateStore)

    built_in_service.scancel_service_slurm_jobs()


class _FakeServer:
    def serve_forever(self) -> None:
        pass

    def shutdown(self) -> None:
        self.shutdown_called = True

    def server_close(self) -> None:
        self.server_closed = True


class _NoopThread:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.target = kwargs.get("target")

    def start(self) -> None:
        if self.target is not None:
            self.target()

    def join(self, timeout: object | None = None) -> None:
        return None


def test_run_built_in_service_registers_and_uses_shutdown_signals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    profile = _make_test_profile(root=tmp_path)
    signal_registrations: list[tuple[int, object]] = []

    monkeypatch.setattr(
        built_in_service,
        "build_service_profile",
        lambda: replace(
            profile,
            lanes=(replace(profile.lanes[0], accounts=()),),
        ),
    )
    monkeypatch.setattr(built_in_service, "validate_service_layout", lambda profile: None)
    monkeypatch.setattr(built_in_service, "start_status_server", lambda **kwargs: _FakeServer())
    monkeypatch.setattr(built_in_service.threading, "Thread", _NoopThread)

    scancel_calls: list[str] = []

    def fake_scancel() -> None:
        scancel_calls.append("called")

    def fake_signal(signum: int, handler: object) -> object:
        signal_registrations.append((signum, handler))
        return lambda s, f=None: None

    monkeypatch.setattr(built_in_service.signal, "signal", fake_signal)
    monkeypatch.setattr(built_in_service, "scancel_service_slurm_jobs", fake_scancel)

    built_in_service.run_built_in_service()

    assert len(signal_registrations) >= 2
    handlers_by_signal: dict[int, object] = {
        signum: handler for signum, handler in signal_registrations[:2]
    }
    assert signal.SIGINT in handlers_by_signal
    assert signal.SIGTERM in handlers_by_signal
    handler_sigint = handlers_by_signal[signal.SIGINT]
    handler_sigterm = handlers_by_signal[signal.SIGTERM]
    assert callable(handler_sigint)
    assert callable(handler_sigterm)

    handler_sigint(signal.SIGINT, None)
    handler_sigterm(signal.SIGTERM, None)
    assert scancel_calls == ["called"]
