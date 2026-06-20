from __future__ import annotations

import json
import threading
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from peetsfea_runner.edt_result_ingest import (
    HttpResultSink,
    replay_spilled,
    start_result_ingest_server,
)
from peetsfea_runner.edt_ssh_tunnel import (
    forward_tunnel_argv,
    reverse_tunnel_argv,
)
from peetsfea_runner.single_simulation_store import SingleSimulationResultStore


def _envelope(request_id: str, *, partition: str = "cpu2", node: str = "n1") -> dict[str, object]:
    return {
        "request_id": request_id,
        "terminal_state": "success",
        "started_at": "2026-06-16T00:00:00+00:00",
        "finished_at": "2026-06-16T00:14:00+00:00",
        "account_id": "account_01",
        "host_alias": "gate1-harry261",
        "partition": partition,
        "node": node,
        "peetsfea_version": "0.3.4",
        "mode": "full",
        "seed": 7,
        "slot_id": "slot_00",
        "result": {"design_id": "d1", "point_values": {"w": 1.0}, "setup_pass_counts": {"Setup1": 6}},
    }


@contextmanager
def _serving(store: SingleSimulationResultStore, port: int) -> Iterator[ThreadingHTTPServer]:
    server = start_result_ingest_server(store=store, port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_sink_pushes_envelope_into_store(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    port = _free_port()
    with _serving(store, port):
        sink = HttpResultSink(url=f"http://127.0.0.1:{port}/ingest")
        sink.record(_envelope("sim-1"))
        sink.record(_envelope("sim-2", partition="gpu3", node="n7"))

    rows = store.fetch_rows()
    by_id = {r["request_id"]: r for r in rows}
    assert set(by_id) == {"sim-1", "sim-2"}
    assert by_id["sim-2"]["partition"] == "gpu3"
    assert by_id["sim-2"]["node"] == "n7"


def test_health_reports_count(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    port = _free_port()
    with _serving(store, port):
        sink = HttpResultSink(url=f"http://127.0.0.1:{port}/ingest")
        sink.record(_envelope("sim-1"))
        body = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3))
    assert body == {"status": "ok", "count": 1}


def test_sink_spills_to_fallback_when_unreachable(tmp_path: Path) -> None:
    spill = tmp_path / "spool"
    # 터널이 죽은 상태(아무도 안 듣는 포트): 재시도 없이 즉시 spill하도록 retries=1, sleep no-op.
    sink = HttpResultSink(
        url=f"http://127.0.0.1:{_free_port()}/ingest",
        fallback_dir=spill,
        retries=1,
        sleep=lambda _s: None,
    )
    sink.record(_envelope("sim-x"))  # raise 하지 않아야 함(디스패처 보호).
    spilled = list(spill.glob("*.json"))
    assert len(spilled) == 1
    saved = json.loads(spilled[0].read_text())
    assert saved["request_id"] == "sim-x"


def test_replay_spilled_resends_after_recovery(tmp_path: Path) -> None:
    spill = tmp_path / "spool"
    dead = HttpResultSink(url=f"http://127.0.0.1:{_free_port()}/ingest", fallback_dir=spill, retries=1, sleep=lambda _s: None)
    dead.record(_envelope("sim-a"))
    dead.record(_envelope("sim-b"))
    assert len(list(spill.glob("*.json"))) == 2

    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    port = _free_port()
    with _serving(store, port):
        sent = replay_spilled(f"http://127.0.0.1:{port}/ingest", spill)
    assert sent == 2
    assert {r["request_id"] for r in store.fetch_rows()} == {"sim-a", "sim-b"}
    assert list(spill.glob("*.json")) == []  # 재전송 성공분은 삭제


def test_ingest_rejects_bad_json(tmp_path: Path) -> None:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    store.initialize()
    port = _free_port()
    with _serving(store, port):
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/ingest", data=b"not-json", headers={"Content-Type": "application/json"}, method="POST"
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(req, timeout=3)
    assert exc.value.code == 400


class _FakeProc:
    def __init__(self, gate: list[int]) -> None:
        self._gate = gate
        self._done = threading.Event()

    def wait(self, timeout: float | None = None) -> int:
        self._done.wait(timeout if timeout is not None else 5)
        return 0

    def poll(self) -> int | None:
        return 0 if self._done.is_set() else None

    def finish(self) -> None:
        self._done.set()

    def terminate(self) -> None:
        self._done.set()

    def kill(self) -> None:
        self._done.set()


def test_ssh_tunnel_restarts_when_process_dies() -> None:
    from peetsfea_runner.edt_ssh_tunnel import SshTunnel

    spawned: list[_FakeProc] = []
    lock = threading.Lock()

    def spawn(argv: object) -> _FakeProc:
        with lock:
            proc = _FakeProc([])
            spawned.append(proc)
        return proc

    tunnel = SshTunnel(argv=["ssh", "-N"], spawn=spawn, sleep=lambda _s: None)  # type: ignore[arg-type]
    tunnel.start()
    # 첫 프로세스 뜰 때까지
    for _ in range(100):
        if spawned:
            break
        threading.Event().wait(0.01)
    assert len(spawned) >= 1
    spawned[0].finish()  # 죽이면 재기동 루프가 다음 프로세스 생성
    for _ in range(100):
        if len(spawned) >= 2:
            break
        threading.Event().wait(0.01)
    assert len(spawned) >= 2
    tunnel.stop()


def test_tunnel_argv_shapes() -> None:
    rev = reverse_tunnel_argv("gate1", port=7876)
    fwd = forward_tunnel_argv("gate1", port=7876)
    assert rev[:2] == ["ssh", "-N"] and "-R" in rev and "7876:127.0.0.1:7876" in rev and rev[-1] == "gate1"
    assert fwd[:2] == ["ssh", "-N"] and "-L" in fwd and "7876:127.0.0.1:7876" in fwd and fwd[-1] == "gate1"
    assert "ExitOnForwardFailure=yes" in rev  # 포워딩 실패 시 즉시 죽고 재기동.


def test_reverse_tunnel_asymmetric_for_multi_account() -> None:
    # 다계정: 두 번째 계정은 게이트 측 포트만 다르게(7886) 쓰고 로컬 ingest는 단일 포트(7876)로 모은다.
    rev = reverse_tunnel_argv("gate1-hmlee31", port=7886, local_port=7876)
    assert "-R" in rev and "7886:127.0.0.1:7876" in rev and rev[-1] == "gate1-hmlee31"
    # local_port 생략 시 대칭(기존 동작) 유지.
    assert "7876:127.0.0.1:7876" in reverse_tunnel_argv("gate1", port=7876)
