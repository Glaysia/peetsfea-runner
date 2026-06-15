"""슬롯 liveness 워치독 — 대여 중이 아닌 슬롯의 죽은 ansysedt를 주기적으로 재기동.

대여(LENT) 슬롯의 65분 백스톱은 디스패처가 future 타임아웃으로 소유한다. 워치독은 그 사이
(WARM/IDLE)에 ansysedt가 죽은 경우만 감지해 다시 warm으로 띄운다(경합 회피).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from .edtmgr import EdtManager


@dataclass
class SlotWatchdog:
    slots: list[EdtManager]
    interval_seconds: float = 30.0
    on_restart: Callable[[EdtManager], None] | None = None
    _stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def tick(self) -> int:
        """전 슬롯 1회 점검. 재기동한 슬롯 수 반환(테스트용 결정론적 진입점)."""
        restarted = 0
        for slot in self.slots:
            if slot.check_liveness():
                restarted += 1
                if self.on_restart is not None:
                    self.on_restart(slot)
        return restarted

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(self.interval_seconds)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        thread = threading.Thread(target=self._loop, name="edt-watchdog", daemon=True)
        self._thread = thread
        thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0)
        self._thread = None


__all__ = ["SlotWatchdog"]
