from __future__ import annotations

import time

from peetsfea_runner.edtmgr import AedtSession, EdtBackend, EdtManager
from peetsfea_runner.edt_watchdog import SlotWatchdog


class FakeBackend(EdtBackend):
    def __init__(self) -> None:
        self.alive = False
        self.starts = 0
        self.kills = 0

    def start(self) -> AedtSession:
        self.starts += 1
        self.alive = True
        return AedtSession(pid=1, grpc_port=2)

    def lend(self) -> AedtSession:
        return AedtSession(pid=1, grpc_port=2)

    def reclaim(self) -> None:
        return None

    def is_alive(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.kills += 1
        self.alive = False


def _mgr() -> tuple[EdtManager, FakeBackend]:
    backend = FakeBackend()
    return EdtManager(backend=backend, clock=time.monotonic, slot_id="s"), backend


def test_watchdog_restarts_dead_warm_slot() -> None:
    mgr, backend = _mgr()
    mgr.ensure_warm()
    restarts: list[str] = []
    wd = SlotWatchdog(slots=[mgr], on_restart=lambda s: restarts.append(s.slot_id))

    assert wd.tick() == 0  # 살아있음 → 아무 일 없음
    backend.alive = False
    assert wd.tick() == 1  # 죽음 → 재기동
    assert backend.starts == 2
    assert restarts == ["s"]


def test_watchdog_ignores_lent_slot() -> None:
    mgr, backend = _mgr()
    mgr.acquire()  # LENT
    backend.alive = False  # 대여 중 사망
    wd = SlotWatchdog(slots=[mgr])
    assert wd.tick() == 0  # 워치독은 LENT를 건드리지 않음(디스패처 소유)
    assert backend.kills == 0
