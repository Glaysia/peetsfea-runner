#!/bin/bash
# 잡당 서브 오케스트레이터 — 단명(1솔브) enroot 컨테이너 풀을 유지한다 (per_solve_container_architecture).
#
# 기존 slot_service.sh(1잡=1컨테이너×50슬롯, 장수 워커 → 메모리/스레드 누수)를 대체:
#   1컨테이너 = 시뮬 1건 후 완전 종료(enroot remove) → AEDT·pyaedt가 컨테이너와 함께 소멸 → 누수 OS회수.
#   오케스트레이터는 TARGET개를 동시 유지(끝나면 새로 spawn) = 오버슈팅 버퍼. solve 밴드는 컨테이너의
#   permit(중앙 제어기 7879)이 그대로 건다(nominal=컨테이너수 > effective=permit허용 solve수).
# 터널/sshd는 잡당 1개(host netns 공유라 모든 컨테이너가 127.0.0.1:포트를 그대로 봄).
set -u
echo "[orch] NODE=$(hostname) JOB=$SLURM_JOB_ID PART=${EDT_PARTITION:-} TARGET=${EDT_CONTAINER_TARGET:-14}"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
IMG=$HOME/runtime/enroot/aedt.sqsh
REF=$DEPLOY/venv/lib/python3.12/site-packages/peetsfea/data/0.3.x_sweep.toml
JOBDIR=/enroot/${USER}_${SLURM_JOB_ID}
OUT=$JOBDIR/run_out
mkdir -p "$OUT/work"

# --- 시작 시 죽은 잡 /enroot 잔재 청소(slot_service.sh와 동일) ---
RUNNING_JIDS=$(squeue -h -u "$USER" -o "%i" 2>/dev/null)
if [ -n "$RUNNING_JIDS" ]; then
  for d in /enroot/${USER}_*; do
    [ -d "$d" ] || continue
    jid=$(basename "$d"); jid=${jid#${USER}_}
    echo "$RUNNING_JIDS" | grep -qx "$jid" || { echo "[orch] clean stale scratch $d"; rm -rf "$d" 2>/dev/null || true; }
  done
  for c in $(enroot list 2>/dev/null | grep -E "^edt-(job-)?[0-9]"); do
    cj=$(echo "$c" | grep -oE "[0-9]+" | head -1)
    echo "$RUNNING_JIDS" | grep -qx "$cj" || { echo "[orch] remove stale ctnr $c"; enroot remove -f "$c" >/dev/null 2>&1 || true; }
  done
fi

GATE=${EDT_GATE_HOST:-gate1}
PORT=${EDT_INGEST_PORT:-7876}; BULK=${EDT_BULK_PORT:-7877}
LEASE=${EDT_PRIORITY_LEASE_PORT:-7878}; LIC=${EDT_LICENSE_CTRL_PORT:-7879}
SSHD=${EDT_ORCH_SSHD_PORT:-0}   # >0이면 잡 오케스트레이터 sshd를 gate:SSHD로 -R 노출(라이브 디버깅)
INGEST_URL="http://127.0.0.1:$PORT/ingest"; LEASE_URL="http://127.0.0.1:$LEASE/lease"; LIC_URL="http://127.0.0.1:$LIC"
[ -n "${EDT_NO_PERMIT:-}" ] && LIC_URL=""   # 파일럿/제어기 없을 때: permit 비활성(컨테이너가 자유 솔브)
CLOG=$DEPLOY/clogs; mkdir -p "$CLOG"        # 컨테이너별 로그(디버깅; gpfs라 게이트에서 읽힘)
case "${EDT_PARTITION:-}" in gpu*) NVD=all ;; *) NVD=void ;; esac

# --- compute node → gate 정터널 (결과/산출물/lease/permit) + 선택적 sshd 역노출 ---
TUNNEL_PID=""
start_tunnel() {
  local RFLAG=""
  [ "$SSHD" -gt 0 ] 2>/dev/null && RFLAG="-R ${SSHD}:127.0.0.1:22"
  ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
      -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -L "$PORT:127.0.0.1:$PORT" -L "$BULK:127.0.0.1:$BULK" -L "$LEASE:127.0.0.1:$LEASE" -L "$LIC:127.0.0.1:$LIC" \
      $RFLAG "$GATE" &
  TUNNEL_PID=$!
}
keep_tunnel() { while true; do start_tunnel; wait "$TUNNEL_PID"; sleep 5; done; }
keep_tunnel & KEEP_PID=$!

STOP=0
declare -A CPID CNAME
remove_all() { for c in $(enroot list 2>/dev/null | grep "^edt-$SLURM_JOB_ID-"); do enroot remove -f "$c" >/dev/null 2>&1 || true; done; }
cleanup() {
  STOP=1
  kill "$KEEP_PID" "$TUNNEL_PID" 2>/dev/null || true
  for slot in "${!CPID[@]}"; do kill -TERM "${CPID[$slot]}" 2>/dev/null || true; done
  sleep 2; remove_all; rm -rf "$JOBDIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- 1솔브 컨테이너 1개 실행(백그라운드): create→start(1솔브)→remove. 컨테이너별 격리(HOME/Ansoft/seed). ---
run_one() {
  local slot=$1 seed=$2
  local C="edt-$SLURM_JOB_ID-$slot-$$-$RANDOM"
  CNAME[$slot]=$C
  (
    CHOME=$JOBDIR/h-$slot          # 컨테이너별 HOME(노드 로컬): ~/.ansys·temp·Ansoft 격리 → 공유상태 손상 차단
    rm -rf "$CHOME"; mkdir -p "$CHOME/tmp"
    cp -a "$HOME/Ansoft" "$CHOME/Ansoft" 2>/dev/null || mkdir -p "$CHOME/Ansoft"   # 초기화된 프로필 격리 복사(6M)
    enroot create --name "$C" "$IMG" >/dev/null 2>&1
    enroot start --root --rw \
      --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
      --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$HOME:$HOME" --mount "$JOBDIR:$JOBDIR" \
      --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env "NVIDIA_VISIBLE_DEVICES=$NVD" \
      --env "ANSYSLMD_LICENSE_FILE=${ANSYSLMD_LICENSE_FILE:-}" \
      --env "HOME=$CHOME" --env "TMPDIR=$CHOME/tmp" \
      --env "EDT_OUTPUT_ROOT=$OUT" --env "EDT_RESULT_INGEST_URL=$INGEST_URL" --env "EDT_WORK_DIR=$OUT/work-$slot" \
      --env "EDT_BULK_PORT=$BULK" --env "EDT_BULK_HOST=127.0.0.1" \
      --env "EDT_PRIORITY_LEASE_URL=$LEASE_URL" --env "EDT_LICENSE_CTRL_URL=$LIC_URL" \
      --env "EDT_REFERENCE_SWEEP=$REF" --env "EDT_PARTITION=${EDT_PARTITION:-}" --env "VENVPY=$VENVPY" \
      --env "EDT_JOB_INDEX=${EDT_JOB_INDEX:-0}" --env "EDT_GPU_COUNT=${EDT_GPU_COUNT:-0}" \
      --env "EDT_SLOT_COUNT=1" --env "EDT_MAX_SIMS=1" \
      --env "EDT_BASELINE_BATCH=1" --env "EDT_BASELINE_WATERMARK=1" \
      --env "EDT_BASELINE_SEED_START=$seed" \
      "$C" /bin/bash -lc '
        ldconfig -p | grep -q libGL.so.1 || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq libgl1 libglu1-mesa libxrender1 libxext6 libsm6 >/dev/null 2>&1; }
        export PATH=/mnt/AnsysEM:$PATH
        exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
      ' > "$CLOG/${SLURM_JOB_ID}-${slot}.log" 2>&1
    enroot remove -f "$C" >/dev/null 2>&1
    rm -rf "$CHOME" 2>/dev/null || true
  ) &
  CPID[$slot]=$!
}

# --- 풀 유지 루프: TARGET개를 동시 유지. 끝난 슬롯은 새 seed로 재spawn. ---
JOB_BASE=$(( ${EDT_BASELINE_SEED_EPOCH:-0} + ${EDT_JOB_INDEX:-0} * 10000000 ))  # 잡별 disjoint seed 대역
COUNTER=0
while [ $STOP -eq 0 ]; do
  TARGET=${EDT_CONTAINER_TARGET:-14}
  for slot in $(seq 0 $((TARGET-1))); do
    [ $STOP -eq 0 ] || break
    pid=${CPID[$slot]:-}
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
      [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
      run_one "$slot" "$(( JOB_BASE + COUNTER ))"
      COUNTER=$((COUNTER+1))
      sleep 0.5   # spawn 스태거(동시 콜드스타트 thundering 완화)
    fi
  done
  sleep 3
done
echo "[orch] DONE job=$SLURM_JOB_ID processed_spawns=$COUNTER"
