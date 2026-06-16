"""7877 대용량 .aedt 전송 — 완료 project_dir를 로컬 아카이브 버퍼로 push (Phase 6 전송).

토폴로지: 7876(결과 JSON)과 같은 gate 허브 경유 push, 단 전송은 **rsync over ssh**(재개·증분).
컨테이너가 시뮬 성공 직후 project_dir를 로컬 전용 sshd(:7877, rrsync 감금)로 rsync push하고,
완료를 `.done` 마커로 알린다. 로컬 컨트롤 플레인의 버퍼 스캐너가 `.done`을 보면 `ArchiveStore.add()`로
넘겨 20GB 묶음 압축 / 2TB(기본 1TB) FIFO를 굴린다.

    [컨테이너] rsync -e 'ssh -p7877 -i key' project_dir → 127.0.0.1:7877(터널)
        → [gate] → [로컬 sshd:7877, rrsync -wo incoming] → incoming/<request_id>/
        → 버퍼 스캐너(.done) → ArchiveStore.add → batch_*.tar.gz

- **컨테이너측 `BulkPushSink`**: `.push(project_dir, request_id)`. rsync 실패해도 raise 안 함(디스패처
  보호) — 원본은 gpfs에 남으므로 다음에 재시도 가능. rsync runner 주입 가능(테스트).
- **로컬측 `BufferScanner`**: `incoming/<rid>/.done` 도착분을 ArchiveStore로 흘려보내는 스레드.
"""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from .edt_archive import ArchiveStore

DONE_MARKER = ".done"

# (argv) -> 반환코드. 0=성공.
RsyncRunner = Callable[[Sequence[str]], int]


def _subprocess_rsync(argv: Sequence[str]) -> int:
    return subprocess.run(list(argv), stdin=subprocess.DEVNULL).returncode


def local_sshd_argv(config_path: Path, *, sshd: str = "/usr/sbin/sshd") -> list[str]:
    """로컬 전용 sshd(7877 수신구) foreground 실행 argv. `SshTunnel`이 감독(죽으면 재기동)."""
    return [sshd, "-D", "-e", "-f", str(config_path)]


def _ssh_transport(port: int, identity: Path | None) -> str:
    parts = ["ssh", "-p", str(port), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
    if identity is not None:
        parts += ["-i", str(identity)]
    return " ".join(parts)


@dataclass
class BulkPushSink:
    """컨테이너측: 완료 project_dir를 로컬 아카이브 버퍼로 rsync push.

    rrsync write-only 감금 하의 원격 루트 기준 상대경로(`<request_id>/`)로 보낸다. 전송 끝나면
    빈 `.done` 마커를 2차 rsync(원격 rename이 막혀 있어 완료를 마커로 신호).
    """

    host: str = "127.0.0.1"
    user: str = "peets"
    port: int = 7877
    identity: Path | None = None
    retries: int = 3
    retry_sleep_seconds: float = 5.0
    runner: RsyncRunner = _subprocess_rsync
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)

    def _dest(self, rel: str) -> str:
        return f"{self.user}@{self.host}:{rel}"

    def push(self, project_dir: Path, request_id: str) -> bool:
        """project_dir → incoming/<request_id>/. 성공 시 .done 마커까지 보내고 True."""
        project_dir = Path(project_dir)
        transport = _ssh_transport(self.port, self.identity)
        data_argv = ["rsync", "-a", "--mkpath", "-e", transport, f"{project_dir}/", self._dest(f"{request_id}/")]
        if not self._run_with_retries(data_argv):
            return False
        # 완료 마커(빈 파일). 데이터가 다 올라간 뒤에만 보내므로 스캐너가 부분전송을 아카이브하지 않는다.
        marker = project_dir.parent / f"{request_id}{DONE_MARKER}"
        try:
            marker.write_text("", encoding="utf-8")
            marker_argv = ["rsync", "-a", "-e", transport, str(marker), self._dest(f"{request_id}/{DONE_MARKER}")]
            ok = self._run_with_retries(marker_argv)
        finally:
            marker.unlink(missing_ok=True)
        return ok

    def _run_with_retries(self, argv: Sequence[str]) -> bool:
        for attempt in range(1, self.retries + 1):
            try:
                if self.runner(argv) == 0:
                    return True
            except Exception:  # noqa: BLE001 — ssh/rsync 실행 자체 실패도 디스패처를 죽이지 않는다.
                pass
            if attempt < self.retries:
                self.sleep(self.retry_sleep_seconds)
        return False


def scan_buffer_once(incoming_root: Path, archive: ArchiveStore) -> int:
    """`incoming/<rid>/.done` 가 있는 완료분을 ArchiveStore로 이관. 반환: 이관 건수."""
    if not incoming_root.exists():
        return 0
    moved = 0
    for child in sorted(incoming_root.iterdir()):
        if not child.is_dir():
            continue
        if not (child / DONE_MARKER).exists():
            continue  # 아직 전송중(마커 없음) → 다음 스캔에.
        archive.add(child)  # 내부에서 pending로 move → 20GB 묶음 / FIFO.
        moved += 1
    return moved


@dataclass
class BufferScanner:
    """로컬측: incoming 버퍼를 주기 스캔해 완료 project_dir를 ArchiveStore로 흘려보내는 스레드."""

    incoming_root: Path
    archive: ArchiveStore
    interval_seconds: float = 30.0
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._thread is not None:
            return
        self.incoming_root.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._loop, name="edt-bulk-scanner", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                scan_buffer_once(self.incoming_root, self.archive)
            except Exception:  # noqa: BLE001 — 스캔 실패가 스레드를 죽이면 안 된다.
                pass
            self._stop.wait(self.interval_seconds)

    def stop(self) -> None:
        self._stop.set()
        self.archive.flush()  # 종료 시 대기 묶음 강제 압축.


__all__ = [
    "DONE_MARKER",
    "BufferScanner",
    "BulkPushSink",
    "RsyncRunner",
    "scan_buffer_once",
]
