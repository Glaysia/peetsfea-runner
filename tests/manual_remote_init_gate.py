from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass


@dataclass(slots=True)
class RemoteInitConfig:
    host: str = os.environ.get("PEETS_GATE_HOST", "gate1-harry261")
    miniconda_dir: str = os.environ.get("PEETS_MINICONDA_DIR", "$HOME/miniconda3")


def _remote_script(cfg: RemoteInitConfig) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail

MINICONDA_DIR="{cfg.miniconda_dir}"
CONDA_PYTHON_PATH="$MINICONDA_DIR/bin/python"

download_miniconda_installer() {{
  local installer_path="$1"
  local url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$installer_path"
    return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "$installer_path" "$url"
    return 0
  fi
  echo "[ERROR] curl/wget not found for miniconda download" >&2
  exit 1
}}

ensure_miniconda() {{
  if [[ -x "$MINICONDA_DIR/bin/conda" ]]; then
    echo "[INFO] Miniconda exists: $MINICONDA_DIR"
    return 0
  fi
  local installer_path
  installer_path="$(mktemp /tmp/miniconda_installer.XXXXXX.sh)"
  echo "[INFO] Installing Miniconda to $MINICONDA_DIR"
  download_miniconda_installer "$installer_path"
  bash "$installer_path" -b -p "$MINICONDA_DIR"
  rm -f "$installer_path"
}}

ensure_conda_base_python() {{
  local conda_bin="$MINICONDA_DIR/bin/conda"
  echo "[INFO] Updating base env to Python 3.12"
  "$conda_bin" install -y python=3.12 pip
}}

verify() {{
  echo "[INFO] Verify versions"
  "$MINICONDA_DIR/bin/conda" --version
  "$CONDA_PYTHON_PATH" -V
}}

ensure_miniconda
ensure_conda_base_python
verify
echo "[INFO] Remote init test done"
"""


def run_remote_init_test(cfg: RemoteInitConfig) -> int:
    command = ["ssh", cfg.host, "bash -s"]
    script = _remote_script(cfg)
    completed = subprocess.run(command, input=script, text=True, capture_output=True, check=False)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.returncode != 0:
        if completed.stderr:
            print(completed.stderr, file=sys.stderr, end="")
    return completed.returncode


def main() -> None:
    cfg = RemoteInitConfig()
    rc = run_remote_init_test(cfg)
    if rc != 0:
        raise SystemExit(rc)


if __name__ == "__main__":
    main()
