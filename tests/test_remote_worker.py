from __future__ import annotations

import json
import socket
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

from peetsfea_runner import remote_worker as remote_worker_module
from peetsfea_runner.remote_worker import PyAedtHfssAdapter, RemoteWorker, RemoteWorkerConfig


class _FakeProcess:
    def __init__(self, polls: list[int | None] | None = None) -> None:
        self._polls = list(polls or [None])
        self.terminate_called = False
        self.kill_called = False

    def poll(self) -> int | None:
        if self._polls:
            return self._polls.pop(0)
        return None

    def terminate(self) -> None:
        self.terminate_called = True

    def wait(self, timeout: float | None = None) -> int:
        _ = timeout
        return 0

    def kill(self) -> None:
        self.kill_called = True


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


class _AppAnalyzeFalse:
    def analyze(self, **kwargs: object) -> bool:
        _ = kwargs
        return False


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
    app = type("App", (), {"post": post})()
    adapter._app = app
    adapter._hfss_class = type(app)
    adapter._load_post_processor_class = staticmethod(lambda: _PostExportToFile)  # type: ignore[method-assign]

    output_path = tmp_path / "safe_report.csv"
    adapter.export_report(report_name="Report S(1,1)", export_format="csv", output_path=output_path)

    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == "report"
    assert post.calls == [(str(tmp_path), "Report S(1,1)", ".csv")]


def test_pyaedt_adapter_export_report_raises_when_file_missing(tmp_path: Path) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=True)
    post = _PostExportToFile(create_file=False)
    app = type("App", (), {"post": post})()
    adapter._app = app
    adapter._hfss_class = type(app)
    adapter._load_post_processor_class = staticmethod(lambda: _PostExportToFile)  # type: ignore[method-assign]

    output_path = tmp_path / "missing.csv"
    with pytest.raises(RuntimeError, match="no file found"):
        adapter.export_report(report_name="Report S(1,1)", export_format="csv", output_path=output_path)


def test_pyaedt_adapter_analyze_raises_when_pyaedt_returns_false() -> None:
    adapter = PyAedtHfssAdapter(gui_mode=True)
    adapter._app = _AppAnalyzeFalse()
    adapter._hfss_class = _AppAnalyzeFalse

    with pytest.raises(RuntimeError, match="returned False"):
        adapter.analyze()


def test_pyaedt_adapter_linux_launch_includes_locale_and_license_env(monkeypatch: object) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=False)
    captured: dict[str, list[str]] = {}
    process = _FakeProcess()

    def _fake_popen(args: list[str], stdout: object, stderr: object) -> _FakeProcess:
        _ = stdout, stderr
        captured["args"] = args
        return process

    monkeypatch.setattr("subprocess.Popen", _fake_popen)
    monkeypatch.setattr(
        PyAedtHfssAdapter,
        "_wait_for_grpc_ready",
        lambda self, **kwargs: None,
    )

    launched = adapter._launch_ansys_grpc(aedt_executable_path="/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt", port=50051)

    assert launched is process
    launch_cmd = captured["args"][2]
    assert "source /etc/profile.d/modules.sh" in launch_cmd
    assert "module load ansys-electronics/v252" in launch_cmd
    assert "export LANG=en_US.UTF-8;" in launch_cmd
    assert "export LC_ALL=en_US.UTF-8;" in launch_cmd
    assert "unset LANGUAGE;" in launch_cmd
    assert "export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81;" in launch_cmd


def test_pyaedt_adapter_wait_for_grpc_ready_raises_with_stdout_stderr_tails(tmp_path: Path) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=False)
    stdout_path = tmp_path / "launch.stdout.log"
    stderr_path = tmp_path / "launch.stderr.log"
    stdout_path.write_text("line1\nline2\n")
    stderr_path.write_text("err1\nerr2\n")
    process = _FakeProcess(polls=[9])

    with pytest.raises(RuntimeError, match="process_exited_early") as exc_info:
        adapter._wait_for_grpc_ready(
            process=process,  # type: ignore[arg-type]
            port=50051,
            timeout_sec=1.0,
            poll_sec=0.01,
            command_summary="bash -lc test",
            stdout_log_path=stdout_path,
            stderr_log_path=stderr_path,
        )

    message = str(exc_info.value)
    assert "returncode=9" in message
    assert "stdout_tail='line1\\nline2'" in message
    assert "stderr_tail='err1\\nerr2'" in message
    assert "command=bash -lc test" in message


def test_pyaedt_adapter_wait_for_grpc_ready_timeout_stops_process(tmp_path: Path, monkeypatch: object) -> None:
    adapter = PyAedtHfssAdapter(gui_mode=False)
    stdout_path = tmp_path / "launch.stdout.log"
    stderr_path = tmp_path / "launch.stderr.log"
    stdout_path.write_text("")
    stderr_path.write_text("")
    process = _FakeProcess(polls=[None, None, None])

    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda address, timeout: (_ for _ in ()).throw(OSError("not ready")),
    )

    with pytest.raises(RuntimeError, match="startup_timeout_port_not_open"):
        adapter._wait_for_grpc_ready(
            process=process,  # type: ignore[arg-type]
            port=50051,
            timeout_sec=0.01,
            poll_sec=0.0,
            command_summary="bash -lc test",
            stdout_log_path=stdout_path,
            stderr_log_path=stderr_path,
        )

    assert process.terminate_called is True


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
