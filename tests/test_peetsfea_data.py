from __future__ import annotations

from pathlib import Path

from peetsfea_runner.peetsfea_data import find_peetsfea_data_toml, load_peetsfea_data_toml_text


def _write(path: Path, text: str = "spec_version = 'x'\n") -> None:
    path.write_text(text, encoding="utf-8")


def test_wildcard_current_family_beats_old_concrete(tmp_path: Path) -> None:
    _write(tmp_path / "0.3.7.1_sweep.toml")
    _write(tmp_path / "0.3.x_sweep.toml")

    selected = find_peetsfea_data_toml("sweep", data_dir=tmp_path, installed_version="0.3.8.0")

    assert selected.name == "0.3.x_sweep.toml"


def test_more_specific_compatible_file_beats_wildcard(tmp_path: Path) -> None:
    _write(tmp_path / "0.3.x_sweep.toml")
    _write(tmp_path / "0.3.8_sweep.toml")
    _write(tmp_path / "0.3.8.0_sweep.toml")

    selected = find_peetsfea_data_toml("sweep", data_dir=tmp_path, installed_version="0.3.8.0")

    assert selected.name == "0.3.8.0_sweep.toml"


def test_numeric_version_sorting_does_not_pick_0_3_9_over_0_3_10(tmp_path: Path) -> None:
    _write(tmp_path / "0.3.9_fixed.toml")
    _write(tmp_path / "0.3.10_fixed.toml", "spec_version = 'new'\n")

    selected = find_peetsfea_data_toml("fixed", data_dir=tmp_path, installed_version="0.3.10")

    assert selected.name == "0.3.10_fixed.toml"
    assert load_peetsfea_data_toml_text("fixed", data_dir=tmp_path) in {
        "spec_version = 'x'\n",
        "spec_version = 'new'\n",
    }
