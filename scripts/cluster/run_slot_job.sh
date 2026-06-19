#!/bin/bash
set -e
echo "[job] NODE=$(hostname) JOB=$SLURM_JOB_ID LIC=$ANSYSLMD_LICENSE_FILE"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
# 검증용: 고정 후보(우선순위 레인, ~14분 확정 solve). baseline은 끈다(random 무거운 후보 회피).
resolve_peetsfea_toml() {
  RESOLVED_TOML=$("$VENVPY" -m peetsfea_runner.peetsfea_data "$1")
  rc=$?
  if [ "$rc" -ne 0 ] || [ -z "$RESOLVED_TOML" ]; then
    echo "[job] failed to resolve peetsfea $1 TOML"
    exit 1
  fi
}
resolve_peetsfea_toml fixed
FIXED=$RESOLVED_TOML
OUT=$DEPLOY/run_out/$SLURM_JOB_ID
mkdir -p "$OUT/work"
C=edt-job-$SLURM_JOB_ID
enroot create --name "$C" "$HOME/runtime/enroot/aedt.sqsh" >/dev/null 2>&1
enroot start --root --rw \
  --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
  --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$HOME:$HOME" \
  --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 \
  --env "ANSYSLMD_LICENSE_FILE=$ANSYSLMD_LICENSE_FILE" \
  --env "EDT_OUTPUT_ROOT=$OUT" --env "EDT_DB_PATH=$OUT/results.duckdb" --env "EDT_WORK_DIR=$OUT/work" \
  --env EDT_SLOT_COUNT=1 --env EDT_MAX_SIMS=1 \
  --env "EDT_PRIORITY_TOML=$FIXED" --env "VENVPY=$VENVPY" \
  "$C" /bin/bash -lc '
    ldconfig -p | grep -q libGL.so.1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq libgl1 libglu1-mesa libxrender1 libxext6 libsm6 >/dev/null 2>&1; }
    export PATH=/mnt/AnsysEM:$PATH
    "$VENVPY" -m peetsfea_runner.edt_entrypoint
  '
rc=$?
enroot remove -f "$C" >/dev/null 2>&1 || true
echo "[job] DONE rc=$rc"
exit $rc
