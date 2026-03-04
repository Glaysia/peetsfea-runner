from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass


@dataclass(slots=True)
class RemoteInitConfig:
    host: str = os.environ.get("PEETS_GATE_HOST", "gate1-harry")
    miniconda_dir: str = os.environ.get("PEETS_MINICONDA_DIR", "$HOME/miniconda3")
    conda_env_name: str = os.environ.get("PEETS_CONDA_ENV_NAME", "peetsfea-runner-py312")
    venv_dir: str = os.environ.get("PEETS_REMOTE_VENV_DIR", "$HOME/.peetsfea-runner-venv")


def _remote_script(cfg: RemoteInitConfig) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail

MINICONDA_DIR="{cfg.miniconda_dir}"
CONDA_ENV_NAME="{cfg.conda_env_name}"
VENV_DIR="{cfg.venv_dir}"
CONDA_PYTHON_PATH="$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python"

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

ensure_conda_py312() {{
  local conda_bin="$MINICONDA_DIR/bin/conda"
  if [[ -x "$CONDA_PYTHON_PATH" ]]; then
    local mm
    mm="$("$CONDA_PYTHON_PATH" -c 'import sys; print(f"{{sys.version_info.major}}.{{sys.version_info.minor}}")')"
    if [[ "$mm" == "3.12" ]]; then
      echo "[INFO] conda py312 exists: $CONDA_PYTHON_PATH"
      return 0
    fi
  fi
  echo "[INFO] Creating conda env $CONDA_ENV_NAME (python=3.12)"
  "$conda_bin" create -y -n "$CONDA_ENV_NAME" python=3.12
}}

ensure_venv_py312() {{
  local recreate=0
  if [[ -d "$VENV_DIR" ]]; then
    if [[ ! -x "$VENV_DIR/bin/python" ]]; then
      recreate=1
    else
      local mm
      mm="$("$VENV_DIR/bin/python" -c 'import sys; print(f"{{sys.version_info.major}}.{{sys.version_info.minor}}")')"
      if [[ "$mm" != "3.12" ]]; then
        recreate=1
      fi
    fi
  else
    recreate=1
  fi
  if [[ "$recreate" -eq 1 ]]; then
    echo "[INFO] Recreating venv with conda python: $VENV_DIR"
    rm -rf "$VENV_DIR"
    "$CONDA_PYTHON_PATH" -m venv "$VENV_DIR"
  fi
}}

verify() {{
  echo "[INFO] Verify versions"
  "$MINICONDA_DIR/bin/conda" --version
  "$CONDA_PYTHON_PATH" -V
  "$VENV_DIR/bin/python" -V
}}

ensure_miniconda
ensure_conda_py312
ensure_venv_py312
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
