from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

from peetsfea_runner.remote_worker import RemoteWorker, RemoteWorkerConfig


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
