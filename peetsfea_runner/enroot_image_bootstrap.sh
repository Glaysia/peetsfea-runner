#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_MARKER="__PEETSFEA_BOOTSTRAP__:ok"

TARGET_IMAGE="${TARGET_IMAGE:-${HOME}/runtime/enroot/aedt.sqsh}"
IMAGE_DIR="${IMAGE_DIR:-$(dirname "${TARGET_IMAGE}")}"
METADATA_PATH="${METADATA_PATH:-${TARGET_IMAGE}.meta.json}"
CONTRACT_VERSION="${CONTRACT_VERSION:-2026-03-18-aedt-sqsh-v2}"
BASE_IMAGE="${BASE_IMAGE:-docker://ubuntu:24.04}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
PYAEDT_SPEC="${PYAEDT_SPEC:-pyaedt==0.25.1}"
PANDAS_SPEC="${PANDAS_SPEC:-pandas<2.4}"
PYVISTA_SPEC="${PYVISTA_SPEC:-pyvista}"
HOST_ANSYS_ROOT="${HOST_ANSYS_ROOT:-/opt/ohpc/pub/Electronics/v252/AnsysEM}"
HOST_ANSYS_BASE="${HOST_ANSYS_BASE:-/opt/ohpc/pub/Electronics/v252}"
LOCK_DIR="${LOCK_DIR:-${TARGET_IMAGE}.lockdir}"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/peetsfea-enroot-build.XXXXXX")"
BASE_SQSH="${TMP_ROOT}/base.sqsh"
TMP_IMAGE="${TARGET_IMAGE}.tmp"
TMP_METADATA="${METADATA_PATH}.tmp"
CONTAINER_NAME="peetsfea-aedt-build-$$"
VALIDATE_CONTAINER_NAME="${CONTAINER_NAME}-validate"
LOCK_HELD=0

image_is_current() {
  [ -f "${TARGET_IMAGE}" ] || return 1
  [ -f "${METADATA_PATH}" ] || return 1
  grep -Fq "\"contract_version\":\"${CONTRACT_VERSION}\"" "${METADATA_PATH}"
}

write_metadata() {
  cat > "${TMP_METADATA}" <<EOF
{"contract_version":"${CONTRACT_VERSION}","base_image":"${BASE_IMAGE}","python_version":"${PYTHON_VERSION}","pyaedt":"${PYAEDT_SPEC}","pandas":"${PANDAS_SPEC}","pyvista":"${PYVISTA_SPEC}","host_ansys_root":"${HOST_ANSYS_ROOT}","host_ansys_base":"${HOST_ANSYS_BASE}"}
EOF
}

cleanup() {
  rc=$?
  enroot remove -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  enroot remove -f "${VALIDATE_CONTAINER_NAME}" >/dev/null 2>&1 || true
  rm -f "${BASE_SQSH}" "${TMP_IMAGE}" "${TMP_METADATA}" >/dev/null 2>&1 || true
  rm -rf "${TMP_ROOT}" >/dev/null 2>&1 || true
  if [ "${LOCK_HELD}" -eq 1 ]; then
    rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true
  fi
  exit "${rc}"
}
trap cleanup EXIT

mkdir -p "${IMAGE_DIR}"

if image_is_current; then
  printf '%s\n' "${BOOTSTRAP_MARKER}"
  exit 0
fi

waited_seconds=0
until mkdir "${LOCK_DIR}" 2>/dev/null; do
  if image_is_current; then
    printf '%s\n' "${BOOTSTRAP_MARKER}"
    exit 0
  fi
  sleep 2
  waited_seconds=$((waited_seconds + 2))
  if [ "${waited_seconds}" -ge 1800 ]; then
    echo "timed out waiting for enroot image build lock: ${LOCK_DIR}" >&2
    exit 1
  fi
done
LOCK_HELD=1

if image_is_current; then
  printf '%s\n' "${BOOTSTRAP_MARKER}"
  exit 0
fi

if [ ! -x "${HOST_ANSYS_ROOT}/ansysedt" ]; then
  echo "missing ansys executable at ${HOST_ANSYS_ROOT}/ansysedt" >&2
  exit 1
fi
if [ ! -x "${HOST_ANSYS_BASE}/licensingclient/linx64/ansyscl" ]; then
  echo "missing ansys licensing client at ${HOST_ANSYS_BASE}/licensingclient/linx64/ansyscl" >&2
  exit 1
fi
if ! command -v enroot >/dev/null 2>&1; then
  echo "enroot is not available on remote host" >&2
  exit 1
fi

export ENROOT_RUNTIME_PATH="${TMP_ROOT}/runtime"
export ENROOT_CACHE_PATH="${TMP_ROOT}/cache"
export ENROOT_DATA_PATH="${TMP_ROOT}/data"
export ENROOT_TEMP_PATH="${TMP_ROOT}/tmp"
mkdir -p "${ENROOT_RUNTIME_PATH}" "${ENROOT_CACHE_PATH}" "${ENROOT_DATA_PATH}" "${ENROOT_TEMP_PATH}"
chmod 700 "${ENROOT_RUNTIME_PATH}" "${ENROOT_CACHE_PATH}" "${ENROOT_DATA_PATH}" "${ENROOT_TEMP_PATH}"

rm -f "${BASE_SQSH}" "${TMP_IMAGE}" "${TMP_METADATA}"
enroot import -o "${BASE_SQSH}" "${BASE_IMAGE}"
enroot create -f -n "${CONTAINER_NAME}" "${BASE_SQSH}" >/dev/null
enroot start --root --rw "${CONTAINER_NAME}" /bin/bash -s -- \
  "${PYTHON_VERSION}" "${PYAEDT_SPEC}" "${PANDAS_SPEC}" "${PYVISTA_SPEC}" <<'EOF'
set -euo pipefail
PYTHON_VERSION="$1"
PYAEDT_SPEC="$2"
PANDAS_SPEC="$3"
PYVISTA_SPEC="$4"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  bzip2 ca-certificates curl file perl lsb-release xsltproc fontconfig \
  libfreetype6 libgif7 libglib2.0-0t64 libdrm2 libjpeg-turbo8 libpng16-16t64 \
  libselinux1 libx11-6 libxau6 libxcb1 libx11-xcb1 libxdamage1 libxext6 \
  libxfixes3 libxft2 libxi6 libxmu6 libxrandr2 libxrender1 libxt6t64 \
  libxtst6 libxxf86vm1 libgl1-mesa-dri libglx-mesa0 libglu1-mesa \
  libsm6 libice6 libopengl0 libnsl2 mesa-utils zlib1g
rm -rf /var/lib/apt/lists/*
curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /opt/miniconda3
rm -f /tmp/miniconda.sh
if /opt/miniconda3/bin/conda tos --help >/dev/null 2>&1; then
  /opt/miniconda3/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true
  /opt/miniconda3/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r >/dev/null 2>&1 || true
fi
/opt/miniconda3/bin/conda install -y "python=${PYTHON_VERSION}" pip
/opt/miniconda3/bin/python -m pip install --upgrade pip setuptools wheel uv
/opt/miniconda3/bin/python -m uv pip install "${PYAEDT_SPEC}" "${PANDAS_SPEC}" "${PYVISTA_SPEC}"
/opt/miniconda3/bin/python -c "import ansys.aedt.core, pandas, pyvista"
EOF

write_metadata

enroot export -f -o "${TMP_IMAGE}" "${CONTAINER_NAME}"

enroot create -f -n "${VALIDATE_CONTAINER_NAME}" "${TMP_IMAGE}" >/dev/null
enroot start --root --rw \
  --mount "${HOST_ANSYS_ROOT}:/mnt/AnsysEM" \
  --mount "${HOST_ANSYS_BASE}:/ansys_inc/v252" \
  "${VALIDATE_CONTAINER_NAME}" /bin/bash -s <<'EOF'
set -euo pipefail
mkdir -p /tmp/peetsfea-home /tmp/peetsfea-tmp
export HOME=/tmp/peetsfea-home
export TMPDIR=/tmp/peetsfea-tmp
export XDG_CONFIG_HOME=/tmp/peetsfea-home/.config
test -x /mnt/AnsysEM/ansysedt
test -x /ansys_inc/v252/licensingclient/linx64/ansyscl
/opt/miniconda3/bin/python -m uv --version >/dev/null
/opt/miniconda3/bin/python -c "import ansys.aedt.core, pandas, pyvista"
EOF

mv "${TMP_IMAGE}" "${TARGET_IMAGE}"
mv "${TMP_METADATA}" "${METADATA_PATH}"

printf '%s\n' "${BOOTSTRAP_MARKER}"
