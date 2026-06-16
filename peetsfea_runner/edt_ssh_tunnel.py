"""SSH 터널 매니저 — 결과 ingest(:7876) 백채널을 상시 유지.

gate를 경유 TCP 허브로 쓰는 두 방향의 터널을 각각 자동 재기동하며 관리한다(끊기면 재연결):

- **로컬 데몬(역터널, `reverse_tunnel`)**: `ssh -N -R <port>:127.0.0.1:<port> <gate>` →
  gate loopback:<port>가 로컬 ingest 서버로 연결.
- **컨테이너(정터널, `forward_tunnel`)**: `ssh -N -L <port>:127.0.0.1:<port> <gate>` →
  컨테이너 loopback:<port>가 gate loopback:<port>(=로컬 ingest)로 연결.

loopback만 쓰므로 GatewayPorts 불필요. `ExitOnForwardFailure=yes`로 포워딩 실패 시 즉시 죽고
재기동 루프가 다시 띄운다. `ServerAliveInterval`로 끊긴 터널을 빠르게 감지.
"""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .edt_result_ingest import DEFAULT_INGEST_PORT

# 터널 ssh 공통 옵션: 끊김 감지 + 포워딩 실패 시 즉시 종료(→ 재기동) + 비대화형.
_SSH_KEEPALIVE = (
    "-o", "ExitOnForwardFailure=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=accept-new",
)

ProcessRunner = Callable[[Sequence[str]], "subprocess.Popen[bytes]"]


def _spawn(argv: Sequence[str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(list(argv), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def reverse_tunnel_argv(gate: str, *, port: int = DEFAULT_INGEST_PORT) -> list[str]:
    """로컬 → gate 역터널: gate loopback:port → 로컬 ingest:port."""
    return ["ssh", "-N", *_SSH_KEEPALIVE, "-R", f"{port}:127.0.0.1:{port}", gate]


def forward_tunnel_argv(gate: str, *, port: int = DEFAULT_INGEST_PORT) -> list[str]:
    """컨테이너 → gate 정터널: 컨테이너 loopback:port → gate loopback:port(=로컬 ingest)."""
    return ["ssh", "-N", *_SSH_KEEPALIVE, "-L", f"{port}:127.0.0.1:{port}", gate]


@dataclass
class SshTunnel:
    """ssh 터널 프로세스를 별도 스레드에서 상시 유지(죽으면 backoff 후 재기동)."""

    argv: Sequence[str]
    spawn: ProcessRunner = _spawn
    sleep: Callable[[float], None] = time.sleep
    restart_delay_seconds: float = 5.0
    name: str = "edt-ssh-tunnel"
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _proc: "subprocess.Popen[bytes] | None" = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name=self.name, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._proc = self.spawn(self.argv)
            self._proc.wait()  # 끊기면 반환 → 재기동.
            if self._stop.is_set():
                return
            self.sleep(self.restart_delay_seconds)

    def stop(self) -> None:
        self._stop.set()
        proc = self._proc
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def is_running(self) -> bool:
        proc = self._proc
        return proc is not None and proc.poll() is None


__all__ = [
    "SshTunnel",
    "forward_tunnel_argv",
    "reverse_tunnel_argv",
]
