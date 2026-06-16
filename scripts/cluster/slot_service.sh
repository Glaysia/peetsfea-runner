#!/bin/bash
# Production 슬롯 서비스 — 잡 컨테이너가 entrypoint로 build_steady_state_service를 무한 가동.
# baseline 전역 샘플링으로 슬롯 자기공급(요청 없어도 계속 시뮬). SIGTERM(=scancel)까지.
# GPU 노출(NVIDIA_VISIBLE_DEVICES=all): peetsfea 0.3.5가 GPU 접근 가능하면 알아서 켠다.
set -e
echo "[slot] NODE=$(hostname) JOB=$SLURM_JOB_ID PART=$EDT_PARTITION LIC=$ANSYSLMD_LICENSE_FILE"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
REF=$DEPLOY/venv/lib/python3.12/site-packages/peetsfea/data/0.3.4_sweep.toml
OUT=$DEPLOY/run_out/$SLURM_JOB_ID
DB=$DEPLOY/results.duckdb   # 모든 잡 공통 공유 결과 DB(대시보드가 읽음)
mkdir -p "$OUT/work"
C=edt-job-$SLURM_JOB_ID
enroot create --name "$C" "$HOME/runtime/enroot/aedt.sqsh" >/dev/null 2>&1
enroot start --root --rw \
  --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
  --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$HOME:$HOME" \
  --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env NVIDIA_VISIBLE_DEVICES=all \
  --env "ANSYSLMD_LICENSE_FILE=$ANSYSLMD_LICENSE_FILE" \
  --env "EDT_OUTPUT_ROOT=$OUT" --env "EDT_DB_PATH=$DB" --env "EDT_WORK_DIR=$OUT/work" \
  --env "EDT_SLOT_COUNT=${EDT_SLOT_COUNT:-11}" --env "EDT_REFERENCE_SWEEP=$REF" \
  --env "EDT_PARTITION=$EDT_PARTITION" --env "VENVPY=$VENVPY" \
  "$C" /bin/bash -lc '
    ldconfig -p | grep -q libGL.so.1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq libgl1 libglu1-mesa libxrender1 libxext6 libsm6 >/dev/null 2>&1; }
    export PATH=/mnt/AnsysEM:$PATH
    exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
  '
enroot remove -f "$C" >/dev/null 2>&1 || true
echo "[slot] DONE"
