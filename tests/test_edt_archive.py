from __future__ import annotations

import tarfile
from pathlib import Path

from peetsfea_runner.edt_archive import ArchiveStore


def _make_project(root: Path, name: str, size: int) -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "proj.aedt").write_bytes(b"x" * size)
    (d / "proj.aedtresults").mkdir()
    (d / "proj.aedtresults" / "data.bin").write_bytes(b"y" * size)
    return d


def test_batches_when_threshold_reached(tmp_path: Path) -> None:
    store = ArchiveStore(archive_root=tmp_path / "arch", batch_threshold_bytes=300, buffer_limit_bytes=10**9)
    src = tmp_path / "src"
    # 각 project_dir ≈ 200바이트(2×100). 2개 누적하면 ≥300 → flush.
    assert store.add(_make_project(src, "p0", 100)) is None  # ~200 < 300
    out = store.add(_make_project(src, "p1", 100))  # ~400 ≥ 300 → 묶음
    assert out is not None and out.exists()
    assert len(store.archive_files()) == 1
    # 원본 project_dir들은 압축 후 삭제됨
    assert not (src / "p0").exists() and not (src / "p1").exists()
    # 묶음 안에 두 폴더가 다 들어 있음(solid)
    with tarfile.open(out, "r:gz") as tar:
        names = tar.getnames()
    assert any("p0" in n for n in names) and any("p1" in n for n in names)


def test_flush_forces_partial_batch(tmp_path: Path) -> None:
    store = ArchiveStore(archive_root=tmp_path / "arch", batch_threshold_bytes=10**9, buffer_limit_bytes=10**9)
    src = tmp_path / "src"
    store.add(_make_project(src, "p0", 50))
    assert len(store.archive_files()) == 0  # 임계 미달
    out = store.flush()
    assert out is not None and len(store.archive_files()) == 1


def test_fifo_eviction_over_buffer_limit(tmp_path: Path) -> None:
    # 고정 100바이트 묶음 + buffer_limit 250 → 2개 유지(가장 오래된 것부터 FIFO 삭제).
    def fixed_compressor(dirs: list[Path], output: Path) -> None:
        output.write_bytes(b"z" * 100)

    store = ArchiveStore(
        archive_root=tmp_path / "arch",
        batch_threshold_bytes=1,  # 매 add마다 즉시 flush
        buffer_limit_bytes=250,
        compressor=fixed_compressor,
    )
    src = tmp_path / "src"
    outs = [store.add(_make_project(src, f"p{i}", 10)) for i in range(4)]
    files = store.archive_files()
    assert len(files) == 2  # 100×4=400 > 250 → 오래된 2개 삭제, 최신 2개 생존
    assert files == outs[-2:]  # 이름(seq)순 = 최신 2묶음


def test_seq_resumes_from_existing(tmp_path: Path) -> None:
    arch = tmp_path / "arch"
    arch.mkdir()
    (arch / "batch_000005.tar.gz").write_bytes(b"old")
    store = ArchiveStore(archive_root=arch, batch_threshold_bytes=1, buffer_limit_bytes=10**9)
    out = store.add(_make_project(tmp_path / "src", "p", 10))
    assert out is not None and out.name == "batch_000006.tar.gz"  # 기존 seq 이어감
