from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from zipfile import ZIP_DEFLATED, ZipFile

AEDT_EXECUTABLE_PATH = "/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt"

E_HFSS_AEDT_MISSING = "E_HFSS_AEDT_MISSING"
E_HFSS_OPEN = "E_HFSS_OPEN"
E_HFSS_ANALYZE = "E_HFSS_ANALYZE"
E_HFSS_NO_REPORTS = "E_HFSS_NO_REPORTS"
E_HFSS_EXPORT = "E_HFSS_EXPORT"
E_HFSS_PACKAGE = "E_HFSS_PACKAGE"
E_HFSS_CLEANUP = "E_HFSS_CLEANUP"


class NoReportsError(RuntimeError):
    pass


class HfssAdapter(Protocol):
    def open_project(self, *, aedt_path: Path, aedt_executable_path: str) -> None:
        raise NotImplementedError

    def disable_auto_save(self) -> None:
        raise NotImplementedError

    def analyze(self) -> None:
        raise NotImplementedError

    def list_report_names(self) -> list[str]:
        raise NotImplementedError

    def export_report(self, *, report_name: str, export_format: str, output_path: Path) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class ReportExportSpec:
    formats: tuple[str, ...] = ("csv",)
    reports_dirname: str = "reports"


@dataclass(frozen=True)
class HfssWorkerTask:
    task_id: str
    aedt_path: Path
    work_dir: Path
    results_dir: Path


@dataclass(frozen=True)
class HfssWorkerResult:
    task_id: str
    success: bool
    report_zip_path: Path | None
    exported_files: tuple[Path, ...]
    error_code: str | None
    error_message: str | None
    aedt_deleted: bool


def sanitize_report_name(report_name: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "_", report_name.strip())
    candidate = candidate.strip("._")
    return candidate or "report"


def package_reports_only(*, reports_dir: Path, zip_path: Path) -> None:
    files = sorted(path for path in reports_dir.rglob("*") if path.is_file())
    if not files:
        raise RuntimeError("No exported report files to package")

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as archive:
        for file_path in files:
            relative_path = file_path.relative_to(reports_dir)
            archive.write(file_path, arcname=str(relative_path))


def _remove_file_if_exists(path: Path) -> bool:
    if not path.exists():
        return True
    try:
        path.unlink()
    except OSError:
        return False
    return True


class HfssWorkerRunner:
    def __init__(
        self,
        *,
        export_spec: ReportExportSpec | None = None,
        aedt_executable_path: str = AEDT_EXECUTABLE_PATH,
    ) -> None:
        self._export_spec = export_spec if export_spec is not None else ReportExportSpec()
        self._aedt_executable_path = aedt_executable_path

    def run_task(self, *, task: HfssWorkerTask, adapter: HfssAdapter) -> HfssWorkerResult:
        exported_files: list[Path] = []
        report_zip_path: Path | None = None
        error_code: str | None = None
        error_message: str | None = None
        success = False

        task.work_dir.mkdir(parents=True, exist_ok=True)
        task.results_dir.mkdir(parents=True, exist_ok=True)

        reports_dir = task.work_dir / self._export_spec.reports_dirname
        zip_tmp_path = task.work_dir / f"{task.task_id}.reports.zip"
        zip_final_path = task.results_dir / f"{task.task_id}.reports.zip"

        if not task.aedt_path.exists():
            return HfssWorkerResult(
                task_id=task.task_id,
                success=False,
                report_zip_path=None,
                exported_files=(),
                error_code=E_HFSS_AEDT_MISSING,
                error_message=f"AEDT file not found: {task.aedt_path}",
                aedt_deleted=True,
            )

        opened = False
        try:
            adapter.open_project(
                aedt_path=task.aedt_path,
                aedt_executable_path=self._aedt_executable_path,
            )
            opened = True
            adapter.disable_auto_save()
        except Exception as exc:  # noqa: BLE001
            error_code = E_HFSS_OPEN
            error_message = str(exc)

        if error_code is None:
            try:
                adapter.analyze()
            except Exception as exc:  # noqa: BLE001
                error_code = E_HFSS_ANALYZE
                error_message = str(exc)

        if error_code is None:
            try:
                report_names = sorted(dict.fromkeys(adapter.list_report_names()))
                if not report_names:
                    raise NoReportsError("No reports available for export")

                for report_name in report_names:
                    safe_name = sanitize_report_name(report_name)
                    report_dir = reports_dir / safe_name
                    report_dir.mkdir(parents=True, exist_ok=True)
                    for export_format in self._export_spec.formats:
                        output_path = report_dir / f"{safe_name}.{export_format}"
                        adapter.export_report(
                            report_name=report_name,
                            export_format=export_format,
                            output_path=output_path,
                        )
                        exported_files.append(output_path)
            except NoReportsError as exc:
                error_code = E_HFSS_NO_REPORTS
                error_message = str(exc)
            except Exception as exc:  # noqa: BLE001
                error_code = E_HFSS_EXPORT
                error_message = str(exc)

        if error_code is None:
            try:
                package_reports_only(reports_dir=reports_dir, zip_path=zip_tmp_path)
                if zip_final_path.exists():
                    zip_final_path.unlink()
                zip_tmp_path.replace(zip_final_path)
                report_zip_path = zip_final_path
                success = True
            except Exception as exc:  # noqa: BLE001
                error_code = E_HFSS_PACKAGE
                error_message = str(exc)

        close_error: str | None = None
        if opened:
            try:
                adapter.close()
            except Exception as exc:  # noqa: BLE001
                close_error = str(exc)

        aedt_deleted = _remove_file_if_exists(task.aedt_path)
        workdir_deleted = True
        try:
            shutil.rmtree(task.work_dir)
        except OSError:
            workdir_deleted = False

        if close_error is not None or not aedt_deleted or not workdir_deleted:
            success = False
            report_zip_path = None
            error_code = E_HFSS_CLEANUP
            if close_error is not None:
                error_message = close_error
            elif not aedt_deleted:
                error_message = f"Failed to delete AEDT file: {task.aedt_path}"
            else:
                error_message = f"Failed to cleanup work directory: {task.work_dir}"

        return HfssWorkerResult(
            task_id=task.task_id,
            success=success,
            report_zip_path=report_zip_path,
            exported_files=tuple(exported_files),
            error_code=error_code,
            error_message=error_message,
            aedt_deleted=aedt_deleted,
        )
