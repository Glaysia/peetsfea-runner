#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-v2026.03.04.4}"
VENV_DIR="$HOME/.peetsfea-runner-venv"
REPO_URL="https://github.com/Glaysia/peetsfea-runner.git"
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="peetsfea-runner-py312"
CONDA_PYTHON_PATH="$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python"

fail() {
  echo "[ERROR] $1" >&2
  exit 1
}

download_miniconda_installer() {
  local installer_path="$1"
  local url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "${url}" -o "${installer_path}"
    return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "${installer_path}" "${url}"
    return 0
  fi

  fail "Miniconda installer 다운로드 실패. curl 또는 wget이 필요합니다."
}

ensure_miniconda() {
  if [[ -x "$MINICONDA_DIR/bin/conda" ]]; then
    return 0
  fi

  echo "[INFO] Installing Miniconda3 to ${MINICONDA_DIR}"
  local installer_path
  installer_path="$(mktemp /tmp/miniconda_installer.XXXXXX.sh)"
  download_miniconda_installer "${installer_path}"

  if ! bash "${installer_path}" -b -p "${MINICONDA_DIR}"; then
    fail "Miniconda3 설치 실패. 권한/디스크/네트워크를 점검하세요."
  fi
  rm -f "${installer_path}"
}

ensure_conda_python312() {
  local conda_bin="$MINICONDA_DIR/bin/conda"
  if [[ ! -x "${conda_bin}" ]]; then
    fail "Miniconda3 conda 실행 파일을 찾을 수 없습니다."
  fi

  if [[ -x "${CONDA_PYTHON_PATH}" ]]; then
    local major_minor
    major_minor="$("${CONDA_PYTHON_PATH}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    if [[ "${major_minor}" == "3.12" ]]; then
      return 0
    fi
  fi

  echo "[INFO] Creating conda env ${CONDA_ENV_NAME} with Python 3.12"
  if ! "${conda_bin}" create -y -n "${CONDA_ENV_NAME}" python=3.12; then
    fail "conda python3.12 환경 생성 실패. 네트워크/권한을 점검하세요."
  fi

  if [[ ! -x "${CONDA_PYTHON_PATH}" ]]; then
    fail "conda env python 경로 확인 실패: ${CONDA_PYTHON_PATH}"
  fi
}

ensure_venv_python312() {
  local recreate=0

  if [[ -d "${VENV_DIR}" ]]; then
    if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
      recreate=1
    else
      local major_minor
      major_minor="$("${VENV_DIR}/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
      if [[ "${major_minor}" != "3.12" ]]; then
        recreate=1
      fi
    fi
  else
    recreate=1
  fi

  if [[ "${recreate}" -eq 1 ]]; then
    echo "[INFO] (Re)creating venv at ${VENV_DIR} using conda Python 3.12"
    rm -rf "${VENV_DIR}"
    "${CONDA_PYTHON_PATH}" -m venv "${VENV_DIR}"
  fi
}

install_tagged_package() {
  if ! "${VENV_DIR}/bin/python" -m pip install --upgrade pip; then
    fail "pip 업그레이드 실패. 네트워크/권한을 점검하세요."
  fi

  if ! "${VENV_DIR}/bin/python" -m pip install "git+${REPO_URL}@${TAG}"; then
    fail "태그 설치 실패. 태그 존재/접근 권한/프록시를 점검하세요."
  fi
}

verify_install() {
  "${VENV_DIR}/bin/python" -c "import peetsfea_runner; print('ok')"
}

main() {
  ensure_miniconda
  ensure_conda_python312
  ensure_venv_python312
  install_tagged_package
  verify_install
  echo "[INFO] Remote bootstrap completed with tag ${TAG}"
}

main "$@"
