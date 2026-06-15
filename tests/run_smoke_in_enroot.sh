#!/usr/bin/env bash
# 로컬 enroot 컨테이너 안에서 RealEdtBackend 실 스모크를 돌린다.
# 호스트(Ubuntu 25.10)는 ansysedt 미지원 → 컨테이너(24.04 userspace)에서 실행.
# 경로/마운트 스킴은 PLANS/local_enroot_aedt.md, single_simulation_remote.py 와 일치.
#
#   tests/run_smoke_in_enroot.sh [CONTAINER] [PY_TARGET]
#
# 환경변수(로컬 dev 보정):
#   PEETS_SMOKE_PYTHON       컨테이너 안 python 경로 (기본 /opt/miniconda3/bin/python)
#   PEETS_SMOKE_CONDA_MOUNT  "HOST:CONT" 형식 추가 마운트(없으면 미사용).
#                            예) /home/peets/miniconda3:/opt/miniconda3
# 프로덕션 컨테이너는 python이 이미 들어 있으므로 두 변수 모두 불필요.
set -euo pipefail

CONTAINER="${1:-local-aedt-smoke}"
PY_TARGET="${2:-/workspace/tests/smoke_edt_backend.py}"
PY_BIN="${PEETS_SMOKE_PYTHON:-/opt/miniconda3/bin/python}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANSYS_BASE="/opt/ohpc/pub/Electronics/v252"

extra_mounts=()
if [[ -n "${PEETS_SMOKE_CONDA_MOUNT:-}" ]]; then
  extra_mounts+=(--mount "${PEETS_SMOKE_CONDA_MOUNT}")
fi

exec enroot start --root --rw \
  --mount "${ANSYS_BASE}/AnsysEM:/mnt/AnsysEM" \
  --mount "${ANSYS_BASE}:/ansys_inc/v252" \
  --mount "${ANSYS_BASE}/licensingclient:/mnt/licensingclient" \
  --mount "${REPO_ROOT}:/workspace" \
  "${extra_mounts[@]}" \
  --env ANSYSEM_ROOT252=/mnt/AnsysEM \
  --env ANS_IGNOREOS=1 \
  --env ANSYSLMD_LICENSE_FILE=1055@172.16.10.81 \
  "${CONTAINER}" \
  /bin/bash -lc "export PATH=/mnt/AnsysEM:\$PATH; exec ${PY_BIN} ${PY_TARGET}"
