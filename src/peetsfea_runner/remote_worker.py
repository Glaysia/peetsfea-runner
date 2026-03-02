from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import socket
import subprocess
import shutil
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from time import sleep, time
from typing import TYPE_CHECKING, Any, Callable

from peetsfea_runner.hfss_worker import AEDT_EXECUTABLE_PATH, HfssAdapter, HfssWorkerRunner, HfssWorkerTask

LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ansys.aedt.core.hfss import Hfss as HfssApp
    from ansys.aedt.core.visualization.post.common import PostProcessorCommon


@dataclass(frozen=True)
class RemoteWorkerConfig:
    spool_inbox: Path
    spool_claimed: Path
    spool_results: Path
    spool_failed: Path
    poll_sec: float = 2.0
    internal_procs: int = 8
    analysis_cores: int = 8
    max_tasks: int | None = None
    aedt_executable_path: str = AEDT_EXECUTABLE_PATH
    gui_mode: bool = False


class PyAedtHfssAdapter(HfssAdapter):
    _GRPC_START_TIMEOUT_SEC = 30.0
    _GRPC_START_POLL_SEC = 0.5

    def __init__(self, *, internal_procs: int = 8, analysis_cores: int = 8, gui_mode: bool = False) -> None:
        self._internal_procs = internal_procs
        self._analysis_cores = max(1, analysis_cores)
        self._gui_mode = gui_mode
        self._app: HfssApp | None = None
        self._hfss_class: type[Any] | None = None
        self._ansys_process: subprocess.Popen[bytes] | None = None
        self._attached_existing_desktop = False

    @staticmethod
    def _is_windows_exec(aedt_executable_path: str) -> bool:
        return aedt_executable_path.lower().endswith(".exe") or ":" in aedt_executable_path[:3]

    @staticmethod
    def _load_hfss_class() -> type[Any]:
        try:
            from pyaedt import Hfss as hfs_cls
        except Exception:
            from ansys.aedt.core import Hfss as hfs_cls  # type: ignore[import-not-found]
        assert isinstance(hfs_cls, type), "PyAEDT Hfss class import failed"
        return hfs_cls

    @staticmethod
    def _load_post_processor_class() -> type[Any]:
        from ansys.aedt.core.visualization.post.common import PostProcessorCommon as post_cls

        assert isinstance(post_cls, type), "PyAEDT PostProcessor class import failed"
        return post_cls

    def _pick_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _tail_log(path: Path, *, max_lines: int = 80) -> str:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return ""
        if not lines:
            return ""
        return "\n".join(lines[-max_lines:])

    def _build_launch_failure_message(
        self,
        *,
        reason: str,
        command_summary: str,
        returncode: int | None,
        stdout_log_path: Path,
        stderr_log_path: Path,
    ) -> str:
        stdout_tail = self._tail_log(stdout_log_path)
        stderr_tail = self._tail_log(stderr_log_path)
        return (
            f"Failed to start ansysedt gRPC server; reason={reason}; returncode={returncode}; "
            f"command={command_summary}; stdout_tail={stdout_tail!r}; stderr_tail={stderr_tail!r}"
        )

    def _wait_for_grpc_ready(
        self,
        *,
        process: subprocess.Popen[bytes],
        port: int,
        timeout_sec: float,
        poll_sec: float,
        command_summary: str,
        stdout_log_path: Path,
        stderr_log_path: Path,
    ) -> None:
        deadline = time() + timeout_sec
        while time() < deadline:
            returncode = process.poll()
            if returncode is not None:
                raise RuntimeError(
                    self._build_launch_failure_message(
                        reason="process_exited_early",
                        command_summary=command_summary,
                        returncode=returncode,
                        stdout_log_path=stdout_log_path,
                        stderr_log_path=stderr_log_path,
                    )
                )
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    return
            except OSError:
                sleep(poll_sec)

        returncode = process.poll()
        if returncode is not None:
            reason = "process_exited_during_startup_wait"
        else:
            reason = "startup_timeout_port_not_open"
            self._stop_ansys_process(process)
        raise RuntimeError(
            self._build_launch_failure_message(
                reason=reason,
                command_summary=command_summary,
                returncode=returncode,
                stdout_log_path=stdout_log_path,
                stderr_log_path=stderr_log_path,
            )
        )

    def _launch_ansys_grpc(self, *, aedt_executable_path: str, port: int) -> subprocess.Popen[bytes]:
        is_windows_exec = self._is_windows_exec(aedt_executable_path)
        stdout_fd, stdout_tmp = tempfile.mkstemp(prefix="peetsfea-aedt-grpc-", suffix=".stdout.log")
        stderr_fd, stderr_tmp = tempfile.mkstemp(prefix="peetsfea-aedt-grpc-", suffix=".stderr.log")
        os.close(stdout_fd)
        os.close(stderr_fd)
        stdout_log_path = Path(stdout_tmp)
        stderr_log_path = Path(stderr_tmp)
        if is_windows_exec:
            cmd = [aedt_executable_path]
            if not self._gui_mode:
                cmd.append("-ng")
            cmd.extend(["-grpcsrv", str(port)])
            command_summary = " ".join(shlex.quote(item) for item in cmd)
            with stdout_log_path.open("wb") as stdout_handle, stderr_log_path.open("wb") as stderr_handle:
                process = subprocess.Popen(  # noqa: S603
                    cmd,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                )
        else:
            headless_arg = "" if self._gui_mode else "-ng "
            launch_cmd = (
                "set -euo pipefail; "
                "source /etc/profile.d/modules.sh >/dev/null 2>&1 || true; "
                "module load ansys-electronics/v252; "
                "export LANG=en_US.UTF-8; "
                "export LC_ALL=en_US.UTF-8; "
                "unset LANGUAGE; "
                "export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81; "
                f"exec {aedt_executable_path} {headless_arg}-grpcsrv {port}"
            )
            command_summary = f"bash -lc {shlex.quote(launch_cmd)}"
            with stdout_log_path.open("wb") as stdout_handle, stderr_log_path.open("wb") as stderr_handle:
                process = subprocess.Popen(  # noqa: S603
                    ["bash", "-lc", launch_cmd],
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                )
        self._wait_for_grpc_ready(
            process=process,
            port=port,
            timeout_sec=self._GRPC_START_TIMEOUT_SEC,
            poll_sec=self._GRPC_START_POLL_SEC,
            command_summary=command_summary,
            stdout_log_path=stdout_log_path,
            stderr_log_path=stderr_log_path,
        )
        return process

    def open_project(self, *, aedt_path: Path, aedt_executable_path: str) -> None:
        hfs_cls = self._load_hfss_class()

        if self._gui_mode:
            try:
                app = hfs_cls(
                    project=str(aedt_path),
                    non_graphical=False,
                    new_desktop=False,
                    close_on_exit=False,
                    student_version=False,
                    remove_lock=True,
                )
                assert isinstance(app, hfs_cls), f"Unexpected HFSS app type: {type(app)!r}"
                self._app = app
                self._hfss_class = hfs_cls
                self._attached_existing_desktop = True
                return
            except Exception as exc:  # noqa: BLE001
                self._app = None
                self._hfss_class = None
                self._attached_existing_desktop = False
                if self._is_windows_exec(aedt_executable_path):
                    raise RuntimeError(
                        "Failed to attach to existing ansysedt desktop "
                        "(gui_mode=True, new_desktop=False). "
                        "Launch ansysedt.exe in the interactive Windows session first."
                    ) from exc

        grpc_port = self._pick_free_port()
        self._ansys_process = self._launch_ansys_grpc(
            aedt_executable_path=aedt_executable_path,
            port=grpc_port,
        )
        try:
            app = hfs_cls(
                project=str(aedt_path),
                non_graphical=not self._gui_mode,
                new_desktop=False,
                close_on_exit=False,
                aedt_process_id=None,
                student_version=False,
                remove_lock=True,
                machine="localhost",
                port=grpc_port,
            )
            assert isinstance(app, hfs_cls), f"Unexpected HFSS app type: {type(app)!r}"
            self._app = app
            self._hfss_class = hfs_cls
            self._attached_existing_desktop = False
        except Exception:  # noqa: BLE001
            self._stop_ansys_process(self._ansys_process)
            self._ansys_process = None
            raise

    def disable_auto_save(self) -> None:
        app = self._app
        hfss_class = self._hfss_class
        assert app is not None and hfss_class is not None, "PyAEDT app is not initialized"
        assert isinstance(app, hfss_class), f"Unexpected HFSS app type: {type(app)!r}"
        try:
            app.autosave_disable()
        except AttributeError:
            app.disable_autosave() # type: ignore

    def analyze(self) -> None:
        app = self._app
        hfss_class = self._hfss_class
        assert app is not None and hfss_class is not None, "PyAEDT app is not initialized"
        assert isinstance(app, hfss_class), f"Unexpected HFSS app type: {type(app)!r}"
        setup_names: list[str]
        try:
            setup_names = [str(name) for name in app.setup_names]
        except Exception:  # noqa: BLE001
            setup_names = []
        LOG.info(
            "hfss_analyze_start setup_count=%d setup_names=%s cores=%d",
            len(setup_names),
            setup_names,
            self._analysis_cores,
        )
        result = app.analyze(cores=self._analysis_cores)
        LOG.info("hfss_analyze_done result=%r", result)
        if result is False:
            raise RuntimeError("PyAEDT analyze returned False")

    def list_report_names(self) -> list[str]:
        app = self._app
        hfss_class = self._hfss_class
        assert app is not None and hfss_class is not None, "PyAEDT app is not initialized"
        assert isinstance(app, hfss_class), f"Unexpected HFSS app type: {type(app)!r}"
        post = app.post
        post_class = self._load_post_processor_class()
        assert post
        assert isinstance(post, post_class), f"Unexpected PostProcessor type: {type(post)!r}"

        values = post.all_report_names
        if values is None:
            return []
        names = [str(item) for item in values]
        LOG.info("hfss_report_names count=%d names=%s", len(names), names)
        return names

    def export_report(self, *, report_name: str, export_format: str, output_path: Path) -> None:
        app = self._app
        hfss_class = self._hfss_class
        assert app is not None and hfss_class is not None, "PyAEDT app is not initialized"
        assert isinstance(app, hfss_class), f"Unexpected HFSS app type: {type(app)!r}"
        post = app.post
        assert post
        post_class = self._load_post_processor_class()
        assert isinstance(post, post_class), f"Unexpected PostProcessor type: {type(post)!r}"

        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True, exist_ok=True)

        extension = export_format if export_format.startswith(".") else f".{export_format}"
        try:
            exported_path = post.export_report_to_file(
                output_dir=str(output_path.parent),
                plot_name=report_name,
                extension=extension,
            )
        except TypeError:
            exported_path = post.export_report_to_file(str(output_path.parent), report_name, extension)

        candidate_paths: list[Path] = []
        if isinstance(exported_path, str) and exported_path:
            candidate_paths.append(Path(exported_path))
        candidate_paths.append(output_path.parent / f"{report_name}{extension}")
        candidate_paths.append(output_path)

        for candidate in candidate_paths:
            if not candidate.exists():
                continue
            if candidate != output_path:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists():
                    output_path.unlink()
                candidate.replace(output_path)
            return

        raise RuntimeError(
            f"Report export completed but no file found for report={report_name}, format={extension}"
        )

    def close(self) -> None:
        app = self._app
        ansys_process = self._ansys_process
        try:
            if app is None:
                return

            try:
                app.release_desktop(close_projects=True, close_desktop=not self._attached_existing_desktop)
            except TypeError:
                app.release_desktop()
            except AttributeError:
                if not self._attached_existing_desktop:
                    app.close_desktop()

            self._app = None
            self._hfss_class = None
            self._attached_existing_desktop = False
        finally:
            self._stop_ansys_process(ansys_process)
            self._ansys_process = None

    def _stop_ansys_process(self, process: subprocess.Popen[bytes] | None) -> None:
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

class RemoteWorker:
    def __init__(
        self,
        config: RemoteWorkerConfig,
        *,
        runner: HfssWorkerRunner | None = None,
        adapter_factory: Callable[[], HfssAdapter] | None = None,
    ) -> None:
        self._config = config
        self._runner = runner if runner is not None else HfssWorkerRunner(
            aedt_executable_path=config.aedt_executable_path
        )
        self._adapter_factory = adapter_factory if adapter_factory is not None else (
            lambda: PyAedtHfssAdapter(
                internal_procs=config.internal_procs,
                analysis_cores=config.analysis_cores,
                gui_mode=config.gui_mode,
            )
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

    def _iter_claimed_aedt(self) -> list[Path]:
        claimed_files: list[Path] = []
        for path in self._config.spool_claimed.rglob("*.aedt"):
            if not path.is_file():
                continue
            # Ignore temporary workdir artifacts and only resume real claimed tasks.
            if ".work" in path.parts:
                continue
            claimed_files.append(path)
        return sorted(claimed_files)

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
        LOG.info("worker_task_start task_id=%s claimed_path=%s", task_id, claimed_path)
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
            LOG.info(
                "worker_task_done task_id=%s success=true zip=%s exported_count=%d",
                task_id,
                result.report_zip_path,
                len(result.exported_files),
            )
            return True

        if claimed_path.exists():
            failed_target = self._config.spool_failed / claimed_path.name
            failed_target.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(claimed_path), str(failed_target))
            except OSError:
                claimed_path.unlink(missing_ok=True)

        self._write_failure_metadata(task_id=task_id, message=result.error_message or result.error_code or "unknown")
        LOG.error(
            "worker_task_done task_id=%s success=false error_code=%s error_message=%s",
            task_id,
            result.error_code,
            result.error_message,
        )
        return True

    def process_once(self) -> bool:
        self._ensure_dirs()
        preclaimed = self._iter_claimed_aedt()
        if preclaimed:
            claimed_path = preclaimed[0]
            return self._process_claimed_task(task_id=claimed_path.stem, claimed_path=claimed_path)

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
    parser.add_argument("--analysis-cores", type=int, default=8)
    parser.add_argument("--max-tasks", type=int, default=None)
    parser.add_argument("--aedt-executable-path", default=AEDT_EXECUTABLE_PATH)
    parser.add_argument("--gui", action="store_true")
    return parser


def _write_json_log(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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
        analysis_cores=args.analysis_cores,
        max_tasks=args.max_tasks,
        aedt_executable_path=args.aedt_executable_path,
        gui_mode=bool(args.gui),
    )
    runtime_var_dir = Path.cwd() / "var"
    startup_payload: dict[str, Any] = {
        "pid": os.getpid(),
        "timestamp_ms": int(time() * 1000),
        "python_executable": str(Path(os.sys.executable)),
        "spool_inbox": str(config.spool_inbox),
        "spool_claimed": str(config.spool_claimed),
        "spool_results": str(config.spool_results),
        "spool_failed": str(config.spool_failed),
        "poll_sec": config.poll_sec,
        "internal_procs": config.internal_procs,
        "analysis_cores": config.analysis_cores,
        "max_tasks": config.max_tasks,
        "aedt_executable_path": config.aedt_executable_path,
        "gui_mode": config.gui_mode,
    }
    _write_json_log(runtime_var_dir / "remote_worker.startup.json", startup_payload)

    worker = RemoteWorker(config)
    try:
        processed = worker.run_forever()
        LOG.info("remote_worker_done processed=%d", processed)
    except Exception as exc:  # noqa: BLE001
        fatal_payload: dict[str, Any] = {
            "pid": os.getpid(),
            "timestamp_ms": int(time() * 1000),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        }
        _write_json_log(runtime_var_dir / "remote_worker.fatal.json", fatal_payload)
        _write_json_log(config.spool_failed / "remote_worker.fatal.json", fatal_payload)
        LOG.exception("remote_worker_fatal")
        raise


if __name__ == "__main__":
    main()
