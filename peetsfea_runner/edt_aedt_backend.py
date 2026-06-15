"""RealEdtBackend — 실제 ansysedt를 띄우고 관리 pyaedt 세션으로 warm 유지.

`EdtBackend` 프로토콜의 실 구현. ansysedt를 `-ng -grpcsrv <port>`로 자식 프로세스 그룹에
띄우고(확실한 SIGKILL용), grpc가 열리면 관리용 pyaedt `Desktop`을 `close_on_exit=False`로
부착해 warm·라이선스 점유를 유지한다. 시뮬 대여 시 관리 세션을 잠깐 release하고, 끝나면 재부착한다.

pyaedt/ansysedt는 실 환경(로컬·클러스터)에서만 동작하므로 import는 메서드 안에서 지연시킨다.
상태기계/타이밍 검증은 `edtmgr`의 fake backend 단위테스트로, 실 동작은 `tests/smoke_edt_backend.py`
스크립트(실 AEDT)로 확인한다.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .edtmgr import AedtSession, EdtBackend


def default_ansysedt_executable() -> Path:
    """로컬은 `ANSYSEM_ROOT252`, 없으면 클러스터 기본 설치 경로를 쓴다(부록 A)."""

    root = os.environ.get("ANSYSEM_ROOT252")
    if root:
        return Path(root) / "ansysedt"
    return Path("/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt")


def _pick_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _grpc_port_open(port: int, *, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class EdtBackendError(RuntimeError):
    pass


@dataclass
class RealEdtBackend(EdtBackend):
    """슬롯 1개의 실제 ansysedt 수명을 소유."""

    slot_id: str = "slot"
    executable: Path = field(default_factory=default_ansysedt_executable)
    non_graphical: bool = True
    grpc_startup_timeout: float = 180.0
    work_dir: Path | None = None
    _proc: subprocess.Popen[bytes] | None = field(default=None, init=False, repr=False)
    _port: int | None = field(default=None, init=False, repr=False)
    _mgmt: Any = field(default=None, init=False, repr=False)

    # --- EdtBackend 구현 -----------------------------------------------------
    def start(self) -> AedtSession:
        self._kill_process()  # 잔재 정리(멱등)
        port = _pick_free_tcp_port()
        proc = self._launch_ansysedt(port)
        self._proc = proc
        self._port = port
        self._wait_grpc(port)
        self._attach_management(port)
        return AedtSession(pid=proc.pid, grpc_port=port)

    def lend(self) -> AedtSession:
        if self._proc is None or self._port is None:
            raise EdtBackendError(f"{self.slot_id}: lend before start")
        # 관리 세션 점유만 놓는다(ansysedt는 살림 → 라이선스 유지). 시뮬 pyaedt가 같은 포트에 붙는다.
        self._release_management()
        return AedtSession(pid=self._proc.pid, grpc_port=self._port)

    def reclaim(self) -> None:
        if self._port is None:
            raise EdtBackendError(f"{self.slot_id}: reclaim before start")
        self._attach_management(self._port)

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def kill(self) -> None:
        self._release_management()
        self._kill_process()

    # --- 내부 --------------------------------------------------------------
    def _launch_ansysedt(self, port: int) -> subprocess.Popen[bytes]:
        if not Path(self.executable).exists():
            raise EdtBackendError(f"ansysedt not found: {self.executable}")
        cmd = [str(self.executable), "-ng", "-grpcsrv", str(port)]
        cwd = str(self.work_dir) if self.work_dir is not None else None
        # start_new_session=True → 새 프로세스 그룹. ansysedt가 자식들을 띄워도 killpg로 한 번에 정리.
        return subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=cwd,
        )

    def _wait_grpc(self, port: int) -> None:
        deadline = time.monotonic() + self.grpc_startup_timeout
        while time.monotonic() < deadline:
            if self._proc is not None and self._proc.poll() is not None:
                raise EdtBackendError(f"{self.slot_id}: ansysedt exited during startup (rc={self._proc.returncode})")
            if _grpc_port_open(port):
                return
            time.sleep(1.0)
        raise EdtBackendError(f"{self.slot_id}: grpc not up on port {port} within {self.grpc_startup_timeout:.0f}s")

    def _attach_management(self, port: int) -> None:
        from ansys.aedt.core import Desktop  # 지연 import

        self._mgmt = Desktop(
            new_desktop=False,
            port=port,
            non_graphical=self.non_graphical,
            close_on_exit=False,
        )

    def _release_management(self) -> None:
        mgmt = self._mgmt
        self._mgmt = None
        if mgmt is None:
            return
        try:
            mgmt.release_desktop(close_projects=False, close_on_exit=False)
        except Exception:
            pass

    def _kill_process(self) -> None:
        proc = self._proc
        self._proc = None
        self._port = None
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


__all__ = ["EdtBackendError", "RealEdtBackend", "default_ansysedt_executable"]
