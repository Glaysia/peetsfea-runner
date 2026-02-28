from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

from peetsfea_runner.hfss_worker import (
    AEDT_EXECUTABLE_PATH,
    E_HFSS_ANALYZE,
    E_HFSS_CLEANUP,
    E_HFSS_EXPORT,
    E_HFSS_NO_REPORTS,
    E_HFSS_PACKAGE,
    HfssWorkerRunner,
    HfssWorkerTask,
    ReportExportSpec,
)


class FakeHfssAdapter:
    def __init__(
        self,
        *,
        reports: list[str] | None = None,
        fail_open: bool = False,
        fail_analyze: bool = False,
        fail_export: bool = False,
        fail_close: bool = False,
    ) -> None:
        self.reports = reports if reports is not None else ["S11"]
        self.fail_open = fail_open
        self.fail_analyze = fail_analyze
        self.fail_export = fail_export
        self.fail_close = fail_close
        self.opened = False
        self.closed = False
        self.used_executable: str | None = None
        self.export_calls: list[tuple[str, str, Path]] = []

    def open_project(self, *, aedt_path: Path, aedt_executable_path: str) -> None:
        _ = aedt_path
        if self.fail_open:
            raise RuntimeError("open failed")
        self.opened = True
        self.used_executable = aedt_executable_path

    def disable_auto_save(self) -> None:
        return None

    def analyze(self) -> None:
        if self.fail_analyze:
            raise RuntimeError("analyze failed")

    def list_report_names(self) -> list[str]:
        return list(self.reports)

    def export_report(self, *, report_name: str, export_format: str, output_path: Path) -> None:
        if self.fail_export:
            raise RuntimeError("export failed")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(f"{report_name}:{export_format}")
        self.export_calls.append((report_name, export_format, output_path))

    def close(self) -> None:
        if self.fail_close:
            raise RuntimeError("close failed")
        self.closed = True


def _build_task(tmp_path: Path, *, task_id: str = "task-1") -> HfssWorkerTask:
    claimed_dir = tmp_path / "claimed"
    claimed_dir.mkdir(parents=True, exist_ok=True)
    aedt_path = claimed_dir / f"{task_id}.aedt"
    aedt_path.write_text("aedt")

    return HfssWorkerTask(
        task_id=task_id,
        aedt_path=aedt_path,
        work_dir=tmp_path / "work" / task_id,
        results_dir=tmp_path / "results",
    )


def test_hfss_worker_exports_all_reports_and_packages_report_only_zip(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="ok")
    adapter = FakeHfssAdapter(reports=["S(1,1)", "Gain Plot"])
    runner = HfssWorkerRunner(export_spec=ReportExportSpec(formats=("csv", "png")))

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is True
    assert result.error_code is None
    assert result.report_zip_path is not None
    assert result.report_zip_path.exists()
    assert result.aedt_deleted is True
    assert task.aedt_path.exists() is False
    assert task.work_dir.exists() is False
    assert len(result.exported_files) == 4
    assert len(adapter.export_calls) == 4
    assert adapter.used_executable == AEDT_EXECUTABLE_PATH

    with ZipFile(result.report_zip_path) as archive:
        names = sorted(archive.namelist())
    assert names == [
        "Gain_Plot/Gain_Plot.csv",
        "Gain_Plot/Gain_Plot.png",
        "S_1_1/S_1_1.csv",
        "S_1_1/S_1_1.png",
    ]
    assert all(not name.endswith(".aedt") for name in names)


def test_hfss_worker_marks_analyze_failure_and_cleans_up(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="analyze_fail")
    adapter = FakeHfssAdapter(fail_analyze=True)
    runner = HfssWorkerRunner()

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is False
    assert result.error_code == E_HFSS_ANALYZE
    assert result.report_zip_path is None
    assert result.aedt_deleted is True
    assert task.aedt_path.exists() is False
    assert task.work_dir.exists() is False


def test_hfss_worker_marks_no_reports_failure(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="no_reports")
    adapter = FakeHfssAdapter(reports=[])
    runner = HfssWorkerRunner()

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is False
    assert result.error_code == E_HFSS_NO_REPORTS
    assert result.report_zip_path is None
    assert result.aedt_deleted is True


def test_hfss_worker_marks_export_failure(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="export_fail")
    adapter = FakeHfssAdapter(reports=["S11"], fail_export=True)
    runner = HfssWorkerRunner(export_spec=ReportExportSpec(formats=("csv", "png")))

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is False
    assert result.error_code == E_HFSS_EXPORT
    assert result.report_zip_path is None
    assert result.aedt_deleted is True


def test_hfss_worker_marks_packaging_failure(tmp_path: Path, monkeypatch: object) -> None:
    task = _build_task(tmp_path, task_id="package_fail")
    adapter = FakeHfssAdapter(reports=["S11"])
    runner = HfssWorkerRunner()

    def _raise_package_failure(*, reports_dir: Path, zip_path: Path) -> None:
        _ = reports_dir, zip_path
        raise RuntimeError("packaging failed")

    monkeypatch.setattr("peetsfea_runner.hfss_worker.package_reports_only", _raise_package_failure)

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is False
    assert result.error_code == E_HFSS_PACKAGE
    assert result.report_zip_path is None
    assert result.aedt_deleted is True


def test_hfss_worker_avoids_filename_collisions_for_sanitized_report_names(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="collision")
    adapter = FakeHfssAdapter(reports=["Gain Plot", "Gain/Plot", "Gain-Plot"])
    runner = HfssWorkerRunner(export_spec=ReportExportSpec(formats=("csv",)))

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is True
    assert result.report_zip_path is not None
    with ZipFile(result.report_zip_path) as archive:
        names = sorted(archive.namelist())

    assert names == [
        "Gain-Plot/Gain-Plot.csv",
        "Gain_Plot/Gain_Plot.csv",
        "Gain_Plot__2/Gain_Plot__2.csv",
    ]


def test_hfss_worker_marks_cleanup_failure_when_close_fails(tmp_path: Path) -> None:
    task = _build_task(tmp_path, task_id="close_fail")
    adapter = FakeHfssAdapter(reports=["S11"], fail_close=True)
    runner = HfssWorkerRunner()

    result = runner.run_task(task=task, adapter=adapter)

    assert result.success is False
    assert result.error_code == E_HFSS_CLEANUP
    assert result.report_zip_path is None
    assert result.aedt_deleted is True
    assert task.aedt_path.exists() is False
