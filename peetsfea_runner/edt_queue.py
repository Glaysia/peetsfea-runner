"""대기 큐 — fixed candidate toml을 슬롯에 순차 공급 (Phase 1).

Phase 1은 큐를 **수동 시드**한다(7875 인테이크는 Phase 4). 디렉토리의 `*.toml`을 읽어
`QueueItem`으로 적재하거나, 직접 `put`한다. 스레드 안전한 단순 FIFO다.
"""

from __future__ import annotations

import hashlib
import threading
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class QueueItem:
    """큐 1건 = fixed candidate toml 하나."""

    request_id: str
    candidate_toml_text: str
    seed: int = 0
    mode: str = "full"

    def input_toml_hash(self) -> str:
        return hashlib.sha256(self.candidate_toml_text.encode("utf-8")).hexdigest()


@dataclass
class TomlQueue:
    """스레드 안전 FIFO. `get()`은 비어 있으면 `None`(논블로킹)."""

    _items: deque[QueueItem] = field(default_factory=deque, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def put(self, item: QueueItem) -> None:
        with self._lock:
            self._items.append(item)

    def extend(self, items: Iterable[QueueItem]) -> None:
        with self._lock:
            self._items.extend(items)

    def get(self) -> QueueItem | None:
        with self._lock:
            if not self._items:
                return None
            return self._items.popleft()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


def load_queue_items_from_dir(directory: Path, *, seed: int = 0, mode: str = "full") -> list[QueueItem]:
    """디렉토리의 `*.toml`을 정렬해 `QueueItem`으로 읽는다(파일명 = request_id)."""

    directory = Path(directory).expanduser()
    items: list[QueueItem] = []
    for path in sorted(directory.glob("*.toml")):
        if not path.is_file():
            continue
        items.append(
            QueueItem(
                request_id=path.stem,
                candidate_toml_text=path.read_text(encoding="utf-8"),
                seed=seed,
                mode=mode,
            )
        )
    return items


__all__ = ["QueueItem", "TomlQueue", "load_queue_items_from_dir"]
