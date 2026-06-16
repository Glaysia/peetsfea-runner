"""아카이브 저장소 (Phase 6, MASTER_PLAN §2.8).

완료된 슬롯 `project_directory`(`*.aedt`/`*.aedtresults` 등)를 누적하다 묶음 크기가 ~20GB에
도달하면 **여러 폴더를 하나의 압축파일(solid)** 로 만든다(폴더 간 중복이 커 묶음 압축 효율↑).
전체 버퍼가 2TB를 넘으면 **가장 오래된 묶음 파일 1개씩 FIFO 삭제**.

압축은 `Compressor`로 분리(테스트는 빠른 tar, 실서비스는 zstd 등 교체 가능).
"""

from __future__ import annotations

import shutil
import tarfile
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .constants import ARCHIVE_BATCH_BYTES, ARCHIVE_BUFFER_BYTES

# (dirs, output_file) -> None : 여러 디렉토리를 하나의 압축파일로.
Compressor = Callable[[list[Path], Path], None]


def tar_gzip_compressor(dirs: list[Path], output: Path) -> None:
    """tar.gz solid 묶음(이식성). 실서비스는 zstd long-range로 교체 권장(압축효율↑)."""
    with tarfile.open(output, "w:gz") as tar:
        for d in dirs:
            tar.add(str(d), arcname=d.name)


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


@dataclass
class ArchiveStore:
    archive_root: Path
    batch_threshold_bytes: int = ARCHIVE_BATCH_BYTES
    buffer_limit_bytes: int = ARCHIVE_BUFFER_BYTES
    compressor: Compressor = tar_gzip_compressor
    archive_suffix: str = ".tar.gz"
    _pending: list[Path] = field(default_factory=list, init=False, repr=False)
    _pending_bytes: int = field(default=0, init=False, repr=False)
    _seq: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.archive_root = Path(self.archive_root).expanduser().resolve()
        self.archive_root.mkdir(parents=True, exist_ok=True)
        (self.archive_root / "pending").mkdir(exist_ok=True)
        self._seq = self._max_existing_seq()

    def _max_existing_seq(self) -> int:
        mx = 0
        for f in self.archive_root.glob(f"batch_*{self.archive_suffix}"):
            try:
                mx = max(mx, int(f.name.split("_")[1].split(".")[0]))
            except (IndexError, ValueError):
                continue
        return mx

    def _stage(self, project_dir: Path) -> Path:
        dest = self.archive_root / "pending" / project_dir.name
        if dest.exists():
            dest = self.archive_root / "pending" / f"{project_dir.name}.{self._seq}.{len(self._pending)}"
        shutil.move(str(project_dir), str(dest))
        return dest

    def add(self, project_dir: Path) -> Path | None:
        """완료 project_dir를 아카이브에 스테이징. 묶음 임계 도달 시 압축파일 경로 반환(아니면 None)."""
        with self._lock:
            staged = self._stage(Path(project_dir))
            self._pending.append(staged)
            self._pending_bytes += _dir_size(staged)
            if self._pending_bytes >= self.batch_threshold_bytes:
                return self._flush_locked()
            return None

    def flush(self) -> Path | None:
        """대기 중인 묶음을 강제로 압축(서비스 종료 등). 없으면 None."""
        with self._lock:
            if not self._pending:
                return None
            return self._flush_locked()

    def _flush_locked(self) -> Path:
        self._seq += 1
        output = self.archive_root / f"batch_{self._seq:06d}{self.archive_suffix}"
        self.compressor(list(self._pending), output)
        for d in self._pending:
            shutil.rmtree(d, ignore_errors=True)
        self._pending.clear()
        self._pending_bytes = 0
        self._evict_locked()
        return output

    def _evict_locked(self) -> None:
        files = self.archive_files()  # batch_NNNNNN 이름순 = 생성(FIFO)순
        total = sum(f.stat().st_size for f in files)
        for f in files:  # 가장 오래된(작은 seq) 것부터
            if total <= self.buffer_limit_bytes:
                break
            size = f.stat().st_size
            f.unlink(missing_ok=True)
            total -= size

    def archive_files(self) -> list[Path]:
        # batch_000001.tar.gz 식 zero-padded seq 이름순 = 생성 순서(FIFO).
        return sorted(self.archive_root.glob(f"batch_*{self.archive_suffix}"))

    def total_bytes(self) -> int:
        return sum(f.stat().st_size for f in self.archive_files())


__all__ = ["ArchiveStore", "Compressor", "tar_gzip_compressor"]
