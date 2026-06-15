"""대기 큐 — fixed candidate toml을 슬롯에 순차 공급 (Phase 1).

Phase 1은 큐를 **수동 시드**한다(7875 인테이크는 Phase 4). 디렉토리의 `*.toml`을 읽어
`QueueItem`으로 적재하거나, 직접 `put`한다. 스레드 안전한 단순 FIFO다.
"""

from __future__ import annotations

import hashlib
import threading
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


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


class QueueLike(Protocol):
    """디스패처가 소비하는 큐 인터페이스. 비어 있으면 `get()`은 `None`(논블로킹)."""

    def get(self) -> QueueItem | None: ...


# baseline 레인 리필용 샘플러: 호출 시 전체공간 풀샘플 배치를 반환(배치마다 seed 롤링은 구현체 책임).
BaselineSampler = Callable[[], list[QueueItem]]


@dataclass
class TwoLaneQueue:
    """우선순위 레인 + baseline 레인의 가중(기본 85:15) 스케줄러 (Phase 4, MASTER_PLAN §2.6.1).

    - **priority 레인:** Intake(:7875)가 넣는 subset 샘플. FIFO로 먼저 소비. 영속은 상위 계층 책임.
    - **baseline 레인:** 전체공간 풀샘플. 깊이가 low watermark 아래로 떨어지면 `baseline_sampler`로
      리필(휘발 — seed로 언제든 재생성). 슬롯이 놀지 않게 하는 상시 공급원.
    - **`get()`:** `baseline_floor`%(기본 15)는 baseline에 하드 할당(결정론적 인터리브). 나머지는
      priority 우선. priority가 비면 100% baseline, baseline이 비면(리필 불가) priority만.
    """

    baseline_sampler: BaselineSampler | None = None
    baseline_low_watermark: int = 200
    baseline_floor_percent: int = 15
    _priority: deque[QueueItem] = field(default_factory=deque, init=False, repr=False)
    _baseline: deque[QueueItem] = field(default_factory=deque, init=False, repr=False)
    _tick: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def put_priority(self, item: QueueItem) -> None:
        with self._lock:
            self._priority.append(item)

    def extend_priority(self, items: Iterable[QueueItem]) -> None:
        with self._lock:
            self._priority.extend(items)

    def seed_baseline(self, items: Iterable[QueueItem]) -> None:
        """baseline 레인을 직접 채운다(테스트/초기화용; 평소엔 sampler가 리필)."""
        with self._lock:
            self._baseline.extend(items)

    def depths(self) -> tuple[int, int]:
        """(priority, baseline) 깊이."""
        with self._lock:
            return len(self._priority), len(self._baseline)

    def _refill_baseline_if_low(self) -> None:
        if self.baseline_sampler is None:
            return
        if len(self._baseline) >= self.baseline_low_watermark:
            return
        self._baseline.extend(self.baseline_sampler())

    def get(self) -> QueueItem | None:
        with self._lock:
            self._refill_baseline_if_low()
            serve_baseline = (self._tick % 100) < self.baseline_floor_percent
            self._tick += 1
            # baseline 플로어 슬롯이고 baseline에 재고가 있으면 baseline.
            if serve_baseline and self._baseline:
                return self._baseline.popleft()
            # 평소: priority 우선.
            if self._priority:
                return self._priority.popleft()
            # priority가 비면 baseline 100%.
            if self._baseline:
                return self._baseline.popleft()
            return None


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


__all__ = [
    "BaselineSampler",
    "QueueItem",
    "QueueLike",
    "TomlQueue",
    "TwoLaneQueue",
    "load_queue_items_from_dir",
]
