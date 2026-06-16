from __future__ import annotations

import tarfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path

from peetsfea_runner.edt_archive import ArchiveStore
from peetsfea_runner.edt_bulk_transfer import BulkPushSink, start_bulk_transfer_server


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def _serving(store: ArchiveStore, port: int) -> Iterator[ThreadingHTTPServer]:
    server = start_bulk_transfer_server(archive_store=store, port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _make_project(root: Path, name: str) -> Path:
    pd = root / name
    (pd / "results").mkdir(parents=True)
    (pd / "model.aedt").write_text("AEDT" * 1000, encoding="utf-8")
    (pd / "results" / "data.csv").write_text("a,b,c\n1,2,3\n", encoding="utf-8")
    return pd


def test_push_extracts_into_archive_and_deletes_origin(tmp_path: Path) -> None:
    store = ArchiveStore(archive_root=tmp_path / "arch", batch_threshold_bytes=1)  # 즉시 flush
    port = _free_port()
    pd = _make_project(tmp_path / "gpfs", "sim-1")
    with _serving(store, port):
        sink = BulkPushSink(port=port)
        ok = sink.push("sim-1", pd)

    assert ok is True
    assert not pd.exists()  # gpfs 원본 삭제됨
    batches = store.archive_files()
    assert len(batches) == 1
    # 묶음 안에 raw로 풀린 project_dir가 들어있다(개별압축 아님).
    with tarfile.open(batches[0], "r:gz") as tar:
        names = tar.getnames()
    assert any(n.endswith("sim-1/model.aedt") for n in names)
    assert any(n.endswith("sim-1/results/data.csv") for n in names)


def test_push_keeps_origin_when_server_unreachable(tmp_path: Path) -> None:
    pd = _make_project(tmp_path / "gpfs", "sim-x")
    sink = BulkPushSink(port=_free_port(), retries=1, sleep=lambda _s: None)  # 아무도 안 듣는 포트
    ok = sink.push("sim-x", pd)
    assert ok is False
    assert pd.exists()  # 실패 시 원본 보존(다음 기회)
    assert not (pd.parent / ".sim-x.bulk.tar.gz").exists()  # 임시 tar 정리됨


def test_pending_accumulates_until_threshold(tmp_path: Path) -> None:
    store = ArchiveStore(archive_root=tmp_path / "arch", batch_threshold_bytes=10**12)  # 큰 임계 → flush 안 함
    port = _free_port()
    with _serving(store, port):
        sink = BulkPushSink(port=port)
        for i in range(3):
            pd = _make_project(tmp_path / "gpfs", f"sim-{i}")
            assert sink.push(f"sim-{i}", pd) is True
    assert store.archive_files() == []  # 아직 묶음 안 만들어짐
    pending = list((store.archive_root / "pending").iterdir())
    assert sorted(p.name for p in pending) == ["sim-0", "sim-1", "sim-2"]  # raw로 대기
    flushed = store.flush()
    assert flushed is not None and flushed.exists()


def test_health_reports_archive_stats(tmp_path: Path) -> None:
    import json
    import urllib.request

    store = ArchiveStore(archive_root=tmp_path / "arch", batch_threshold_bytes=1)
    port = _free_port()
    with _serving(store, port):
        body = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3))
    assert body["status"] == "ok" and "batches" in body and "bytes" in body
