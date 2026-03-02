from __future__ import annotations

import json
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

from peetsfea_runner import remote_worker as remote_worker_module
from peetsfea_runner.remote_worker import PyAedtHfssAdapter, RemoteWorker, RemoteWorkerConfig


class _SuccessAdapter:
    def __init__(self) -> None:
        self._reports = ["S11"]

    def open_project(self, *, aedt_path: Path, aedt_executable_path: str) -> None:
        _ = aedt_path, aedt_executable_path

    def disable_auto_save(self) -> None:
        return None

    def analyze(self) -> None:
        return None

    def list_report_names(self) -> list[str]:
        return list(self._reports)

    def export_report(self, *, report_name: str, export_format: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(f"{report_name}:{export_format}")

    def close(self) -> None:
        return None


class _FailAnalyzeAdapter(_SuccessAdapter):
    def analyze(self) -> None:
        raise RuntimeError("analyze failed")


class _PostExportToFile:
    def __init__(self, *, create_file: bool) -> None:
        self.create_file = create_file
        self.calls: list[tuple[str, str, str]] = []

    def export_report_to_file(self, output_dir: str, plot_name: str, extension: str) -> str:
        self.calls.append((output_dir, plot_name, extension))
        path = Path(output_dir) / f"{plot_name}{extension}"
        if self.create_file:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("report")
        return str(path)


def _build_config(tmp_path: Path) -> RemoteWorkerConfig:
    spool_root = tmp_path / "spool"
    return RemoteWorkerConfig(
        spool_inbox=spool_root / "inbox",
        spool_claimed=spool_root / "claimed",
        spool_results=spool_root / "results",
        spool_failed=spool_root / "failed",
        poll_sec=0.01,
        max_tasks=1,
    )


def test_remote_worker_claims_from_nested_inbox_and_writes_reports_zip(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    source = config.spool_inbox / "task-1" / "task-1.aedt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("aedt")

    worker = RemoteWorker(config, adapter_factory=lambda: _SuccessAdapter())
    processed = worker.process_once()

    assert processed is True
    assert source.exists() is False
    output_zip = config.spool_results / "task-1.reports.zip"
    assert output_zip.exists()
    with ZipFile(output_zip) as archive:
        assert archive.namelist() == ["S11/S11.csv"]


def test_remote_worker_writes_failure_metadata_when_execution_fails(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    source = config.spool_inbox / "task-2" / "task-2.aedt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("aedt")

    worker = RemoteWorker(config, adapter_factory=lambda: _FailAnalyzeAdapter())
    processed = worker.process_once()

    assert processed is True
    metadata = config.spool_failed / "task-2.failed.json"
    assert metadata.exists()
    payload = json.loads(metadata.read_text(encoding="utf-8"))
    assert payload["task_id"] == "task-2"
    assert "analyze failed" in payload["message"]


def test_remote_worker_resumes_preclaimed_task_without_inbox_file(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    claimed = config.spool_claimed / "task-3.aedt"
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text("aedt")

    worker = RemoteWorker(config, adapter_factory=lambda: _SuccessAdapter())
    processed = worker.process_once()

    assert processed is True
    assert claimed.exists() is False
    output_zip = config.spool_results / "task-3.reports.zip"
    assert output_zip.exists()


def test_pyaedt_adapter_export_report_uses_output_dir_signature(tmp_path: Path) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=True)
    post = _PostExportToFile(create_file=True)
    adapter._app = type("App", (), {"post": post})()

    output_path = tmp_path / "safe_report.csv"
    adapter.export_report(report_name="Report S(1,1)", export_format="csv", output_path=output_path)

    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == "report"
    assert post.calls == [(str(tmp_path), "Report S(1,1)", ".csv")]


def test_pyaedt_adapter_export_report_raises_when_file_missing(tmp_path: Path) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=True)
    post = _PostExportToFile(create_file=False)
    adapter._app = type("App", (), {"post": post})()

    output_path = tmp_path / "missing.csv"
    with pytest.raises(RuntimeError, match="no file found"):
        adapter.export_report(report_name="Report S(1,1)", export_format="csv", output_path=output_path)


def test_remote_worker_main_writes_startup_banner(tmp_path: Path, monkeypatch: object) -> None:
    spool_root = tmp_path / "spool"
    for name in ("inbox", "claimed", "results", "failed"):
        (spool_root / name).mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "remote_worker.py",
            "--spool-inbox",
            str(spool_root / "inbox"),
            "--spool-claimed",
            str(spool_root / "claimed"),
            "--spool-results",
            str(spool_root / "results"),
            "--spool-failed",
            str(spool_root / "failed"),
            "--max-tasks",
            "0",
        ],
    )
    monkeypatch.setattr(remote_worker_module.RemoteWorker, "run_forever", lambda _self: 0)

    remote_worker_module.main()

    startup = tmp_path / "var" / "remote_worker.startup.json"
    assert startup.exists()
    payload = json.loads(startup.read_text(encoding="utf-8"))
    assert payload["spool_inbox"] == str(spool_root / "inbox")
    assert payload["pid"] > 0


def test_remote_worker_main_writes_fatal_log(tmp_path: Path, monkeypatch: object) -> None:
    spool_root = tmp_path / "spool"
    for name in ("inbox", "claimed", "results", "failed"):
        (spool_root / name).mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "remote_worker.py",
            "--spool-inbox",
            str(spool_root / "inbox"),
            "--spool-claimed",
            str(spool_root / "claimed"),
            "--spool-results",
            str(spool_root / "results"),
            "--spool-failed",
            str(spool_root / "failed"),
        ],
    )

    def _raise_runtime(_self: object) -> int:
        raise RuntimeError("boom")

    monkeypatch.setattr(remote_worker_module.RemoteWorker, "run_forever", _raise_runtime)

    try:
        remote_worker_module.main()
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")

    fatal = tmp_path / "var" / "remote_worker.fatal.json"
    assert fatal.exists()
    payload = json.loads(fatal.read_text(encoding="utf-8"))
    assert payload["error_type"] == "RuntimeError"
    assert "boom" in payload["error_message"]
