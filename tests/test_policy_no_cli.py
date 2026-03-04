from __future__ import annotations

import unittest
from pathlib import Path


class TestPolicyNoCli(unittest.TestCase):
    def test_no_cli_imports_in_package(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        package_root = repo_root / "peetsfea_runner"
        forbidden = ("import argparse", "from argparse", "import click", "from click", "import typer", "from typer")

        for path in package_root.rglob("*.py"):
            content = path.read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, content, f"Forbidden CLI import found in {path}: {token}")

    def test_no_console_scripts_in_pyproject(self) -> None:
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        content = pyproject.read_text(encoding="utf-8")
        self.assertNotIn("console_scripts", content)
        self.assertNotIn("[project.scripts]", content)


if __name__ == "__main__":
    unittest.main()

