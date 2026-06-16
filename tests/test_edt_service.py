from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from peetsfea_runner.edt_service import EdtServiceConfig, build_dispatcher
from peetsfea_runner.edtmgr import AedtSession, EdtBackend, EdtManager
from peetsfea_runner.edt_queue import QueueItem, TomlQueue


class FakeBackend(EdtBackend):
    def __init__(self) -> None:
        self.alive = False

    def start(self) -> AedtSession:
        self.alive = True
        return AedtSession(pid=4242, grpc_port=51999)

    def lend(self) -> AedtSession:
        return AedtSession(pid=4242, grpc_port=51999)

    def reclaim(self) -> None:
        return None

    def is_alive(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.alive = False


def test_dispatcher_persists_envelope_to_duckdb_store(tmp_path: Path) -> None:
    def primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int, **_: Any) -> dict[str, Any]:
        return {"k_ratio": 0.42, "Lrx_uH": 12.5, "setup_pass_counts": {"Setup1": 7}}

    config = EdtServiceConfig(
        output_root=tmp_path / "out",
        db_path=tmp_path / "results.duckdb",
        slot_count=1,
    )
    queue = TomlQueue()
    queue.put(QueueItem("req_db", "x = 1\n", seed=3))
    slots = [EdtManager(backend=FakeBackend(), clock=time.monotonic, slot_id="slot_00")]

    dispatcher, _q, store = build_dispatcher(config, primitive=primitive, slots=slots, queue=queue)
    processed = dispatcher.run()

    assert processed == 1
    row = store.fetch_result("req_db")
    assert row is not None
    assert row["terminal_state"] == "success"
    assert row["seed"] == 3
    import peetsfea

    assert row["peetsfea_version"] == peetsfea.__version__  # 런타임 실측(설치 버전)
    assert "0.42" in row["result_json"]
    assert "Setup1" in row["setup_pass_counts_json"]
