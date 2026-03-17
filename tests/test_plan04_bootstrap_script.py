from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "remote_bootstrap_install.sh"


class TestPlan04BootstrapScript(unittest.TestCase):
    def test_script_exists_and_is_executable(self) -> None:
        self.assertTrue(SCRIPT_PATH.exists(), f"Missing script: {SCRIPT_PATH}")
        self.assertTrue(SCRIPT_PATH.stat().st_mode & 0o111, "Script must be executable")

    def test_contains_miniconda_install_and_conda_python312(self) -> None:
        content = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("MINICONDA_DIR=\"$HOME/miniconda3\"", content)
        self.assertIn("Miniconda3-latest-Linux-x86_64.sh", content)
        self.assertIn("bash \"${installer_path}\" -b -p \"${MINICONDA_DIR}\"", content)
        self.assertIn("\"${conda_bin}\" install -y python=3.12 pip", content)

    def test_contains_base_env_policy(self) -> None:
        content = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("CONDA_PYTHON_PATH", content)
        self.assertNotIn("VENV_DIR=", content)
        self.assertNotIn("CONDA_ENV_NAME=", content)
        self.assertNotIn("-m venv", content)

    def test_contains_tag_install_and_verification(self) -> None:
        content = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("REPO_URL=\"https://github.com/Glaysia/peetsfea-runner.git\"", content)
        self.assertIn("git+${REPO_URL}@${TAG}", content)
        self.assertIn("-m pip install --upgrade pip", content)
        self.assertIn("태그 설치 실패. 태그 존재/접근 권한/프록시를 점검하세요.", content)
        self.assertIn("import peetsfea_runner", content)


if __name__ == "__main__":
    unittest.main()
