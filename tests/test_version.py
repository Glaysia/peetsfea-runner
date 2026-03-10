from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import tomllib

from peetsfea_runner import __version__
from peetsfea_runner.version import load_version, validate_pyproject_version


class TestVersion(unittest.TestCase):
    def test_load_version_matches_pyproject(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        self.assertEqual(load_version(repo_root / "pyproject.toml"), __version__)

    def test_validate_pyproject_version_accepts_date_build(self) -> None:
        self.assertEqual(
            validate_pyproject_version({"project": {"version": "2026.03.10.7"}}),
            "2026.03.10.7",
        )

    def test_validate_pyproject_version_rejects_invalid_format(self) -> None:
        with self.assertRaises(ValueError):
            validate_pyproject_version({"project": {"version": "0.1.0"}})

    def test_load_version_raises_when_version_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pyproject_path = Path(tmpdir) / "pyproject.toml"
            pyproject_path.write_text("[project]\nname='peetsfea-runner'\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_version(pyproject_path)

    def test_repo_does_not_define_extra_version_file(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        self.assertFalse((repo_root / "VERSION").exists())
        self.assertEqual(
            tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"],
            __version__,
        )


if __name__ == "__main__":
    unittest.main()
