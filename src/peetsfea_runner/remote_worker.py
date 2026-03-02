from __future__ import annotations

import argparse
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from time import sleep, time
from typing import Callable

from peetsfea_runner.hfss_worker import HfssAdapter, HfssWorkerRunner, HfssWorkerTask

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class RemoteWorkerConfig:
    spool_inbox: Path
    spool_claimed: Path
    spool_results: Path
    spool_failed: Path
    poll_sec: float = 2.0
    internal_procs: int = 8
    max_tasks: int | None = None


class PyAedtHfssAdapter(HfssAdapter):
    def __init__(self, *, internal_procs: int = 8) -> None:
        self._internal_procs = internal_procs
        self._app: object | None = None

    def open_project(self, *, aedt_path: Path, aedt_executable_path: str) -> None:
        hfs_cls = None
        import_error: Exception | None = None

        try:
            from pyaedt import Hfss as _Hfss

            hfs_cls = _Hfss
        except Exception as exc:  # noqa: BLE001
            import_error = exc

        if hfs_cls is None:
            try:
                from ansys.aedt.core import Hfss as _Hfss  # type: ignore[import-not-found]

                hfs_cls = _Hfss
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"Failed to import PyAEDT Hfss: {import_error or exc}") from exc

        self._app = hfs_cls(
            project=str(aedt_path),
            non_graphical=True,
            new_desktop_session=True,
            close_on_exit=False,
            aedt_process_id=None,
            student_version=False,
            specified_version=None,
            aedt_exe_path=aedt_executable_path,
            num_cores=self._internal_procs,
        )

    def disable_auto_save(self) -> None:
        app = self._require_app()
        for method_name in ("autosave_disable", "disable_autosave"):
            method = getattr(app, method_name, None)
            if callable(method):
                method()
                return

    def analyze(self) -> None:
        app = self._require_app()
        for method_name in ("analyze_all", "analyze"):
            method = getattr(app, method_name, None)
            if callable(method):
                method()
                return
        raise RuntimeError("PyAEDT app has no analyze method")

    def list_report_names(self) -> list[str]:
        app = self._require_app()
        post = getattr(app, "post", None)
        if post is None:
            return []
        names = getattr(post, "all_report_names", None)
        if callable(names):
            values = names()
        else:
            values = names
        if values is None:
            return []
        return [str(item) for item in values]

    def export_report(self, *, report_name: str, export_format: str, output_path: Path) -> None:
        app = self._require_app()
        post = getattr(app, "post", None)
        if post is None:
            raise RuntimeError("PyAEDT app has no post processor")

        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True, exist_ok=True)

        export_method = getattr(post, "export_report_to_file", None)
        if callable(export_method):
            try:
                export_method(report_name, str(output_path))
                return
            except TypeError:
                export_method(report_name, str(output_path), export_format)
                return

        export_method = getattr(post, "export_report_from_name", None)
        if callable(export_method):
            export_method(report_name, str(output_path))
            return

        raise RuntimeError("PyAEDT post has no report export method")

    def close(self) -> None:
        app = self._app
        if app is None:
            return

        release = getattr(app, "release_desktop", None)
        if callable(release):
            try:
                release(close_projects=True, close_desktop=True)
            except TypeError:
                release()
            finally:
                self._app = None
            return

        close_desktop = getattr(app, "close_desktop", None)
        if callable(close_desktop):
            close_desktop()

        self._app = None

    def _require_app(self) -> object:
        if self._app is None:
            raise RuntimeError("PyAEDT app is not initialized")
        return self._app


class RemoteWorker:
    def __init__(
        self,
        config: RemoteWorkerConfig,
        *,
        runner: HfssWorkerRunner | None = None,
        adapter_factory: Callable[[], HfssAdapter] | None = None,
    ) -> None:
        self._config = config
        self._runner = runner if runner is not None else HfssWorkerRunner()
        self._adapter_factory = adapter_factory if adapter_factory is not None else (
            lambda: PyAedtHfssAdapter(internal_procs=config.internal_procs)
        )

    def _ensure_dirs(self) -> None:
        for directory in (
            self._config.spool_inbox,
            self._config.spool_claimed,
            self._config.spool_results,
            self._config.spool_failed,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def _iter_inbox_aedt(self) -> list[Path]:
        return sorted(path for path in self._config.spool_inbox.rglob("*.aedt") if path.is_file())

    def _claim_one(self) -> tuple[str, Path] | None:
        for source in self._iter_inbox_aedt():
            task_id = source.stem
            target = self._config.spool_claimed / f"{task_id}.aedt"
            if target.exists():
                continue

            try:
                source.parent.mkdir(parents=True, exist_ok=True)
                source.replace(target)
            except OSError:
                continue

            self._cleanup_empty_dirs(source.parent)
            return task_id, target
        return None

    def _cleanup_empty_dirs(self, directory: Path) -> None:
        try:
            if directory == self._config.spool_inbox:
                return
            directory.rmdir()
        except OSError:
            return

    def _write_failure_metadata(self, *, task_id: str, message: str) -> None:
        failure_path = self._config.spool_failed / f"{task_id}.failed.json"
        payload = {
            "task_id": task_id,
            "message": message,
            "timestamp_ms": int(time() * 1000),
        }
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _process_claimed_task(self, *, task_id: str, claimed_path: Path) -> bool:
        work_dir = self._config.spool_claimed / ".work" / task_id
        task = HfssWorkerTask(
            task_id=task_id,
            aedt_path=claimed_path,
            work_dir=work_dir,
            results_dir=self._config.spool_results,
        )

        adapter = self._adapter_factory()
        result = self._runner.run_task(task=task, adapter=adapter)
        if result.success:
            return True

        if claimed_path.exists():
            failed_target = self._config.spool_failed / claimed_path.name
            failed_target.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(claimed_path), str(failed_target))
            except OSError:
                claimed_path.unlink(missing_ok=True)

        self._write_failure_metadata(task_id=task_id, message=result.error_message or result.error_code or "unknown")
        return True

    def process_once(self) -> bool:
        self._ensure_dirs()
        claimed = self._claim_one()
        if claimed is None:
            return False

        task_id, claimed_path = claimed
        return self._process_claimed_task(task_id=task_id, claimed_path=claimed_path)

    def run_forever(self) -> int:
        handled = 0
        while True:
            processed = self.process_once()
            if processed:
                handled += 1
                if self._config.max_tasks is not None and handled >= self._config.max_tasks:
                    return handled
                continue
            sleep(self._config.poll_sec)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run remote HFSS worker loop")
    parser.add_argument("--spool-inbox", required=True)
    parser.add_argument("--spool-claimed", required=True)
    parser.add_argument("--spool-results", required=True)
    parser.add_argument("--spool-failed", required=True)
    parser.add_argument("--poll-sec", type=float, default=2.0)
    parser.add_argument("--internal-procs", type=int, default=8)
    parser.add_argument("--max-tasks", type=int, default=None)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args()
    config = RemoteWorkerConfig(
        spool_inbox=Path(args.spool_inbox),
        spool_claimed=Path(args.spool_claimed),
        spool_results=Path(args.spool_results),
        spool_failed=Path(args.spool_failed),
        poll_sec=args.poll_sec,
        internal_procs=args.internal_procs,
        max_tasks=args.max_tasks,
    )
    worker = RemoteWorker(config)
    processed = worker.run_forever()
    LOG.info("remote_worker_done processed=%d", processed)


if __name__ == "__main__":
    main()
