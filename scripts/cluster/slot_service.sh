#!/bin/bash
# Production 슬롯 서비스 — 잡 컨테이너가 entrypoint로 build_steady_state_service를 무한 가동.
# baseline 전역 샘플링으로 슬롯 자기공급(요청 없어도 계속 시뮬). SIGTERM(=scancel)까지.
# GPU 노출(NVIDIA_VISIBLE_DEVICES=all): peetsfea 0.3.5가 GPU 접근 가능하면 알아서 켠다.
#
# 결과 전송: 슈퍼컴엔 DB 없음. 결과 envelope를 로컬 데몬(:7876)으로 push한다.
#   compute node --ssh -L 7876:127.0.0.1:7876--> gate 127.0.0.1:7876 <--ssh -R-- 로컬 ingest
#   (로컬 데몬이 gate로 역터널을 유지하고, 여기선 compute node→gate 정터널을 띄운다.)
#   enroot는 host netns를 공유하므로 compute node의 127.0.0.1:7876을 컨테이너가 그대로 본다.
#   주의: 같은 compute node에 잡 2개가 뜨면 127.0.0.1:7876 바인딩이 충돌한다(현재는 노드당 1잡 가정).
set -e
echo "[slot] NODE=$(hostname) JOB=$SLURM_JOB_ID PART=$EDT_PARTITION LIC=$ANSYSLMD_LICENSE_FILE"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
REF=$DEPLOY/venv/lib/python3.12/site-packages/peetsfea/data/0.3.5_sweep.toml
OUT=$DEPLOY/run_out/$SLURM_JOB_ID
# compute node가 gate를 부르는 클러스터 내부명(로컬 ssh 별칭 gate1-harry261 아님!). 실측: n043→gate1 OK.
GATE=${EDT_GATE_HOST:-gate1}
PORT=${EDT_INGEST_PORT:-7876}
INGEST_URL="http://127.0.0.1:$PORT/ingest"
C=edt-job-$SLURM_JOB_ID
mkdir -p "$OUT/work"

# compute node → gate 정터널(결과 push 백채널). 끊기면 즉시 죽고(ExitOnForwardFailure) 재기동 루프가 살린다.
TUNNEL_PID=""
start_tunnel() {
  ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
      -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -L "$PORT:127.0.0.1:$PORT" "$GATE" &
  TUNNEL_PID=$!
}
keep_tunnel() { while true; do start_tunnel; wait "$TUNNEL_PID"; sleep 5; done; }
keep_tunnel &
KEEPER_PID=$!
cleanup() { kill "$KEEPER_PID" "$TUNNEL_PID" 2>/dev/null || true; enroot remove -f "$C" >/dev/null 2>&1 || true; }
trap cleanup EXIT

enroot create --name "$C" "$HOME/runtime/enroot/aedt.sqsh" >/dev/null 2>&1
enroot start --root --rw \
  --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
  --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$HOME:$HOME" \
  --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env NVIDIA_VISIBLE_DEVICES=all \
  --env "ANSYSLMD_LICENSE_FILE=$ANSYSLMD_LICENSE_FILE" \
  --env "EDT_OUTPUT_ROOT=$OUT" --env "EDT_RESULT_INGEST_URL=$INGEST_URL" --env "EDT_WORK_DIR=$OUT/work" \
  --env "EDT_SLOT_COUNT=${EDT_SLOT_COUNT:-11}" --env "EDT_REFERENCE_SWEEP=$REF" \
  --env "EDT_PARTITION=$EDT_PARTITION" --env "VENVPY=$VENVPY" \
  "$C" /bin/bash -lc '
    ldconfig -p | grep -q libGL.so.1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq libgl1 libglu1-mesa libxrender1 libxext6 libsm6 >/dev/null 2>&1; }
    export PATH=/mnt/AnsysEM:$PATH
    exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
  '
echo "[slot] DONE"
