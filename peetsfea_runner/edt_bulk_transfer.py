"""대용량 산출물 전송 :7877 — project_dir(aedt)를 로컬로 보내 아카이브 + gpfs 절약 (GOAL §6.1).

토폴로지는 7876(결과 ingest)과 동일: gate 경유 ssh 터널 + loopback 바인딩(외부 도달 불가, 인증 불필요).
**sshd 변경 불필요 — HTTP tar 스트림.**

    [컨테이너] tar czf - project_dir → POST 127.0.0.1:7877/bulk/<rid>
        --ssh -L--> [gate] 127.0.0.1:7877 <--ssh -R-- [로컬] :7877 (수신 → 스트리밍 추출 → ArchiveStore)

- **로컬(`start_bulk_transfer_server`)**: `POST /bulk/<request_id>`로 tar.gz 스트림을 받아 **상수 메모리로
  스트리밍 추출**(받는 즉시 raw로 풀어 둔다 — 개별 압축 보관 금지). 추출된 project_dir를 `ArchiveStore`에
  add → 여러 project_dir가 모여 20GB 묶음 solid 압축, 2TB FIFO. (개별 압축 보관 시 묶음이 "이미 압축된
  덩어리들의 tar"가 되어 추가 압축이 안 먹으므로 raw로 푼다.)
- **컨테이너(`BulkPushSink`)**: 시뮬 성공 시 project_dir를 `tar.gz`로 만들어 POST. **성공 시 gpfs 원본 삭제**
  (무조건 절약), 실패 시 재시도→보존(디스패처 안 죽임). 전송 구간만 압축, 도착하면 raw로 풀린다.

7877도 7875 사용자가 모르는 슈퍼컴 전용 통로.
"""

from __future__ import annotations

import http.client
import json
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import IO, Any, cast
from urllib.parse import urlparse

from .edt_archive import ArchiveStore

DEFAULT_BULK_PORT = 7877


class _LimitedReader:
    """rfile에서 정확히 `limit` 바이트까지만 읽는 래퍼(tar 스트림이 본문 경계를 넘지 않게)."""

    def __init__(self, stream: IO[bytes], limit: int) -> None:
        self._stream = stream
        self._remaining = limit

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        want = self._remaining if size < 0 else min(size, self._remaining)
        chunk = self._stream.read(want)
        self._remaining -= len(chunk)
        return chunk


def start_bulk_transfer_server(
    *,
    archive_store: ArchiveStore,
    host: str = "127.0.0.1",
    port: int = DEFAULT_BULK_PORT,
) -> ThreadingHTTPServer:
    """대용량 산출물 수신 서버. `POST /bulk/<rid>`(tar.gz 스트림) → 추출 → ArchiveStore.add. `GET /health`."""

    incoming = archive_store.archive_root / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()

    def _ingest(request_id: str, body: IO[bytes]) -> None:
        # 받는 즉시 raw로 스트리밍 추출(상수 메모리). 같은 fs(archive_root)에 풀어 add()의 move가 빠르게.
        staging = Path(tempfile.mkdtemp(dir=str(incoming), prefix=f"{request_id}."))
        project_dir = staging / request_id
        project_dir.mkdir(parents=True, exist_ok=True)
        try:
            with tarfile.open(fileobj=body, mode="r|gz") as tar:
                tar.extractall(project_dir, filter="data")  # filter=data: 경로 탈출/특수파일 차단
            with lock:  # incoming 정리까지 직렬화(ArchiveStore 자체는 내부 락 보유).
                archive_store.add(project_dir)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-bulk"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            if urlparse(self.path).path == "/health":
                _write_json(self, 200, {"status": "ok", "batches": len(archive_store.archive_files()), "bytes": archive_store.total_bytes()})
                return
            _write_json(self, 404, {"error": "not_found"})

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if not path.startswith("/bulk/"):
                _write_json(self, 404, {"error": "not_found"})
                return
            request_id = path[len("/bulk/"):]
            if not request_id:
                _write_json(self, 400, {"error": "missing request_id"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                _write_json(self, 400, {"error": "bad Content-Length"})
                return
            try:
                _ingest(request_id, cast("IO[bytes]", _LimitedReader(cast("IO[bytes]", self.rfile), length)))
            except Exception as exc:  # noqa: BLE001
                _write_json(self, 500, {"error": type(exc).__name__, "message": str(exc)})
                return
            _write_json(self, 200, {"status": "ok", "request_id": request_id})

    return ThreadingHTTPServer((host, port), Handler)


@dataclass
class BulkPushSink:
    """컨테이너측: 성공한 project_dir를 tar.gz로 :7877에 push. 성공 시 gpfs 원본 삭제(무조건 절약)."""

    host: str = "127.0.0.1"
    port: int = DEFAULT_BULK_PORT
    timeout_seconds: float = 600.0
    retries: int = 3
    retry_sleep_seconds: float = 5.0
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    tar_runner: Callable[[Path, Path], None] = field(default=lambda src, dst: _tar_gz(src, dst), repr=False)

    def push(self, request_id: str, project_dir: Path) -> bool:
        """project_dir push. 성공 시 원본 삭제하고 True, 끝내 실패면 원본 보존하고 False(절대 raise 안 함)."""
        project_dir = Path(project_dir)
        if not project_dir.exists():
            return False
        tmp = project_dir.parent / f".{project_dir.name}.bulk.tar.gz"
        try:
            self.tar_runner(project_dir, tmp)
        except Exception:  # noqa: BLE001 — tar 실패는 원본 보존.
            tmp.unlink(missing_ok=True)
            return False
        try:
            for attempt in range(1, self.retries + 1):
                if self._post(request_id, tmp):
                    shutil.rmtree(project_dir, ignore_errors=True)  # gpfs 절약: 전송 성공 시 원본 삭제.
                    return True
                if attempt < self.retries:
                    self.sleep(self.retry_sleep_seconds)
            return False  # 끝내 실패 → 원본 보존(다음 기회/수동 복구).
        finally:
            tmp.unlink(missing_ok=True)

    def _post(self, request_id: str, tar_path: Path) -> bool:
        size = tar_path.stat().st_size
        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_seconds)
            try:
                conn.putrequest("POST", f"/bulk/{request_id}")
                conn.putheader("Content-Type", "application/gzip")
                conn.putheader("Content-Length", str(size))
                conn.endheaders()
                with tar_path.open("rb") as f:
                    while True:
                        chunk = f.read(1 << 20)
                        if not chunk:
                            break
                        conn.send(chunk)
                resp = conn.getresponse()
                resp.read()
                return resp.status == 200
            finally:
                conn.close()
        except Exception:  # noqa: BLE001 — 터널 단절 등.
            return False


def _tar_gz(src_dir: Path, dst: Path) -> None:
    """project_dir를 tar.gz로(전송 구간 압축). 외부 `tar`가 있으면 그걸, 없으면 파이썬 tarfile."""
    if shutil.which("tar") is not None:
        subprocess.run(
            ["tar", "czf", str(dst), "-C", str(src_dir.parent), src_dir.name],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return
    with tarfile.open(dst, "w:gz") as tar:
        tar.add(str(src_dir), arcname=src_dir.name)


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


__all__ = [
    "DEFAULT_BULK_PORT",
    "BulkPushSink",
    "start_bulk_transfer_server",
]
