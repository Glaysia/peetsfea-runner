from __future__ import annotations

from pathlib import Path

from peetsfea_runner.edt_queue import QueueItem, TomlQueue, load_queue_items_from_dir


def test_fifo_order_and_len() -> None:
    q = TomlQueue()
    assert q.get() is None
    q.put(QueueItem("a", "x"))
    q.extend([QueueItem("b", "y"), QueueItem("c", "z")])
    assert len(q) == 3
    assert [q.get().request_id for _ in range(3)] == ["a", "b", "c"]  # type: ignore[union-attr]
    assert q.get() is None


def test_input_toml_hash_is_stable() -> None:
    item = QueueItem("r1", "spec_version = \"0.3.1\"\n")
    assert item.input_toml_hash() == item.input_toml_hash()
    assert len(item.input_toml_hash()) == 64


def test_load_queue_items_from_dir(tmp_path: Path) -> None:
    (tmp_path / "0002.toml").write_text("b = 2\n", encoding="utf-8")
    (tmp_path / "0001.toml").write_text("a = 1\n", encoding="utf-8")
    (tmp_path / "skip.txt").write_text("nope\n", encoding="utf-8")
    items = load_queue_items_from_dir(tmp_path, seed=7, mode="full")
    assert [i.request_id for i in items] == ["0001", "0002"]  # 정렬됨
    assert items[0].candidate_toml_text == "a = 1\n"
    assert items[0].seed == 7
