#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-v2026.03.04.4}"
REPO_URL="https://github.com/Glaysia/peetsfea-runner.git"
MINICONDA_DIR="$HOME/miniconda3"
CONDA_PYTHON_PATH="$MINICONDA_DIR/bin/python"

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

  if [[ -e "$MINICONDA_DIR" ]]; then
    rm -rf "${MINICONDA_DIR}"
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

ensure_conda_base_python() {
  local conda_bin="$MINICONDA_DIR/bin/conda"
  if [[ ! -x "${conda_bin}" ]]; then
    fail "Miniconda3 conda 실행 파일을 찾을 수 없습니다."
  fi

  if "${conda_bin}" tos --help >/dev/null 2>&1; then
    "${conda_bin}" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true
    "${conda_bin}" tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r >/dev/null 2>&1 || true
  fi

  echo "[INFO] Updating Miniconda base env to Python 3.12"
  if ! "${conda_bin}" install -y python=3.12 pip; then
    fail "conda base python3.12 환경 구성 실패. 네트워크/권한을 점검하세요."
  fi

  if [[ ! -x "${CONDA_PYTHON_PATH}" ]]; then
    fail "conda base python 경로 확인 실패: ${CONDA_PYTHON_PATH}"
  fi
}

install_tagged_package() {
  if ! "${CONDA_PYTHON_PATH}" -m pip install --upgrade pip; then
    fail "pip 업그레이드 실패. 네트워크/권한을 점검하세요."
  fi

  if ! "${CONDA_PYTHON_PATH}" -m pip install "git+${REPO_URL}@${TAG}"; then
    fail "태그 설치 실패. 태그 존재/접근 권한/프록시를 점검하세요."
  fi
}

verify_install() {
  "${CONDA_PYTHON_PATH}" -c "import peetsfea_runner; print('ok')"
}

main() {
  ensure_miniconda
  ensure_conda_base_python
  install_tagged_package
  verify_install
  echo "[INFO] Remote bootstrap completed with tag ${TAG}"
}

main "$@"
