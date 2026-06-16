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
# 무거운 AEDT 산출물은 노드 로컬 스크래치 /enroot/{USER}_{JOB}에 쓴다(gpfs $HOME 쿼터 회피, job_workspace 설계).
# gpfs $HOME에 쓰면 88 워커 aedt가 per-user 쿼터를 터뜨린다. /enroot는 노드 로컬·대용량·잡 종료 시 삭제.
JOBDIR=/enroot/${USER}_${SLURM_JOB_ID}
OUT=$JOBDIR/run_out
CHOME=$JOBDIR/home   # 컨테이너 HOME(노드 로컬): AEDT가 ~/.ansys 등을 gpfs $HOME 대신 여기에 쓰게 한다.
# 시작 시 이 노드의 내 죽은 잡(squeue에 없는) /enroot 잔재 청소 — SIGKILL/노드크래시로 trap을 못 탄 경우 대비.
# squeue가 비어있으면(명령 실패) 스킵 = 안전(돌고 있는 잡 dir 오삭제 방지). 다른 유저 dir은 prefix로 제외.
RUNNING_JIDS=$(squeue -h -u "$USER" -o "%i" 2>/dev/null)
if [ -n "$RUNNING_JIDS" ]; then
  # (a) /enroot/{USER}_{JOB} 스크래치 디렉토리 잔재
  for d in /enroot/${USER}_*; do
    [ -d "$d" ] || continue
    jid=$(basename "$d"); jid=${jid#${USER}_}
    echo "$RUNNING_JIDS" | grep -qx "$jid" || { echo "[slot] cleaning stale /enroot scratch: $d"; rm -rf "$d" 2>/dev/null || true; }
  done
  # (b) 추출된 enroot 컨테이너(edt-job-{JOB}) 잔재 — enroot remove가 하드킬로 안 돈 경우(2.7G/개).
  for c in $(enroot list 2>/dev/null | grep "^edt-job-"); do
    cj=${c#edt-job-}
    echo "$RUNNING_JIDS" | grep -qx "$cj" || { echo "[slot] removing stale container: $c"; enroot remove -f "$c" >/dev/null 2>&1 || true; }
  done
fi
# compute node가 gate를 부르는 클러스터 내부명(로컬 ssh 별칭 gate1-harry261 아님!). 실측: n043→gate1 OK.
GATE=${EDT_GATE_HOST:-gate1}
PORT=${EDT_INGEST_PORT:-7876}        # 결과 JSON ingest 백채널
BULK=${EDT_BULK_PORT:-7877}          # 대용량 산출물(aedt) 백채널
INGEST_URL="http://127.0.0.1:$PORT/ingest"
C=edt-job-$SLURM_JOB_ID
mkdir -p "$OUT/work" "$CHOME/tmp"
# AEDT 라이선스 유저 config(작음)만 노드 로컬 HOME에 시드 — 라이선스는 env로 받지만 config 누락 회피.
cp -rn "$HOME/Ansoft" "$CHOME/" 2>/dev/null || true
# GPU 노출은 gpu* 파티션에서만. cpu2 노드엔 NVIDIA 드라이버가 없어 NVIDIA_VISIBLE_DEVICES=all이면
# enroot의 98-nvidia.sh가 'nvml driver not loaded'로 실패해 컨테이너가 3초 만에 죽는다(void=훅 비활성).
case "$EDT_PARTITION" in
  gpu*) NVD=all ;;
  *)    NVD=void ;;
esac

# compute node → gate 정터널(결과 push 백채널 7876 + 산출물 7877). 둘 다 한 ssh로 포워딩.
# 끊기면 즉시 죽고(ExitOnForwardFailure) 재기동 루프가 살린다.
TUNNEL_PID=""
start_tunnel() {
  ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
      -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -L "$PORT:127.0.0.1:$PORT" -L "$BULK:127.0.0.1:$BULK" "$GATE" &
  TUNNEL_PID=$!
}
keep_tunnel() { while true; do start_tunnel; wait "$TUNNEL_PID"; sleep 5; done; }
keep_tunnel &
KEEPER_PID=$!
cleanup() { kill "$KEEPER_PID" "$TUNNEL_PID" 2>/dev/null || true; enroot remove -f "$C" >/dev/null 2>&1 || true; rm -rf "$JOBDIR" 2>/dev/null || true; }
# EXIT뿐 아니라 scancel(SIGTERM)/SIGINT도 잡아 /enroot 잔재를 남기지 않는다(공용 공간 청결). rm -rf는 멱등.
trap cleanup EXIT INT TERM

enroot create --name "$C" "$HOME/runtime/enroot/aedt.sqsh" >/dev/null 2>&1
enroot start --root --rw \
  --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
  --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$DEPLOY:$DEPLOY" --mount "$JOBDIR:$JOBDIR" \
  --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env "NVIDIA_VISIBLE_DEVICES=$NVD" \
  --env "ANSYSLMD_LICENSE_FILE=$ANSYSLMD_LICENSE_FILE" \
  --env "HOME=$CHOME" --env "TMPDIR=$CHOME/tmp" \
  --env "EDT_OUTPUT_ROOT=$OUT" --env "EDT_RESULT_INGEST_URL=$INGEST_URL" --env "EDT_WORK_DIR=$OUT/work" \
  --env "EDT_BULK_PORT=$BULK" --env "EDT_BULK_HOST=127.0.0.1" \
  --env "EDT_SLOT_COUNT=${EDT_SLOT_COUNT:-11}" --env "EDT_REFERENCE_SWEEP=$REF" \
  --env "EDT_WORKER_STAGGER_SECONDS=${EDT_WORKER_STAGGER_SECONDS:-180}" \
  --env "EDT_PARTITION=$EDT_PARTITION" --env "VENVPY=$VENVPY" \
  "$C" /bin/bash -lc '
    ldconfig -p | grep -q libGL.so.1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq libgl1 libglu1-mesa libxrender1 libxext6 libsm6 >/dev/null 2>&1; }
    export PATH=/mnt/AnsysEM:$PATH
    exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
  '
echo "[slot] DONE"
