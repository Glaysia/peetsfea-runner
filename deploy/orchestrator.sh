#!/bin/bash
# 잡당 서브 오케스트레이터 — 단명(1솔브) enroot 컨테이너 풀을 **제어기 지령대로** 유지 (per_solve_container).
#
#   1컨테이너 = 시뮬 1건 후 완전 종료(enroot remove) → AEDT·pyaedt 소멸 → 누수 OS회수. 재사용/재시작 없음.
#   동시 컨테이너 수 = 제어기(:7879 /container_plan?job=N)가 지정. solve<100이면 제어기가 target↑, >150이면
#   target↓(→ 여기서 가장 최근 컨테이너를 SIGTERM 안전종료, 강종 금지). 잡당 ≤20(제어기 cap).
#   permit 게이팅 없음(제어기가 컨테이너 수로 직접 제어). 터널 1개/잡(host netns 공유).
# 주의: set -u 금지 — 컴퓨트노드 구버전 bash에서 빈 연관배열 접근(${#C_PID[@]}, ${!C_PID[@]})이
# "unbound variable"로 잡을 즉사시킨다. 모든 변수는 ${VAR:-default}로 방어.
echo "[orch] NODE=$(hostname) JOB=$SLURM_JOB_ID JIDX=${EDT_JOB_INDEX:-0} PART=${EDT_PARTITION:-}"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
IMG=$HOME/runtime/enroot/aedt.sqsh
REF=$DEPLOY/venv/lib/python3.12/site-packages/peetsfea/data/0.3.x_sweep.toml
JOBDIR=/enroot/${USER}_${SLURM_JOB_ID}
OUT=$JOBDIR/run_out
CLOG=$DEPLOY/clogs; mkdir -p "$OUT/work" "$CLOG"

# 시작 시 죽은 잡 /enroot 잔재 청소
RUNNING_JIDS=$(squeue -h -u "$USER" -o "%i" 2>/dev/null)
if [ -n "$RUNNING_JIDS" ]; then
  for d in /enroot/${USER}_*; do
    [ -d "$d" ] || continue; jid=$(basename "$d"); jid=${jid#${USER}_}
    echo "$RUNNING_JIDS" | grep -qx "$jid" || { echo "[orch] clean stale $d"; rm -rf "$d" 2>/dev/null||true; }
  done
  for c in $(enroot list 2>/dev/null | grep -E "^edt-[0-9]"); do
    cj=$(echo "$c" | grep -oE "[0-9]+" | head -1)
    echo "$RUNNING_JIDS" | grep -qx "$cj" || enroot remove -f "$c" >/dev/null 2>&1 || true
  done
fi

# /enroot 여유 가드 — 타 유저가 채운 포화 노드(n001/n113류)에 떨어지면 우리 솔브가 No-space로 폭사한다.
# 여유가 임계 미만이면 이 노드는 스킵(클린 종료) → keeper가 잡 수 유지하려 재제출 → 다른 노드 배치.
ENROOT_FREE_GB=$(df -BG /enroot 2>/dev/null | tail -1 | awk '{gsub(/G/,"",$4); print $4+0}')
ENROOT_MIN_GB=${EDT_ENROOT_MIN_GB:-80}
if [ -n "$ENROOT_FREE_GB" ] && [ "$ENROOT_FREE_GB" -lt "$ENROOT_MIN_GB" ] 2>/dev/null; then
  echo "[orch] /enroot free=${ENROOT_FREE_GB}G < ${ENROOT_MIN_GB}G — 포화 노드, 스킵 종료"
  exit 0
fi

GATE=${EDT_GATE_HOST:-gate1}
PORT=${EDT_INGEST_PORT:-7876}; BULK=${EDT_BULK_PORT:-7877}
LEASE=${EDT_PRIORITY_LEASE_PORT:-7878}; LIC=${EDT_LICENSE_CTRL_PORT:-7879}
SSHD=${EDT_ORCH_SSHD_PORT:-0}
INGEST_URL="http://127.0.0.1:$PORT/ingest"; LEASE_URL="http://127.0.0.1:$LEASE/lease"
PLAN_URL="http://127.0.0.1:$LIC/container_plan?job=${EDT_JOB_INDEX:-0}"
case "${EDT_PARTITION:-}" in gpu*) NVD=all ;; *) NVD=void ;; esac

# compute node → gate 정터널 (결과/산출물/lease/제어기) + 선택 sshd 역노출
TUNNEL_PID=""
start_tunnel() {
  local R=""; [ "${SSHD:-0}" -gt 0 ] 2>/dev/null && R="-R ${SSHD}:127.0.0.1:22"
  ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
      -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      -L "$PORT:127.0.0.1:$PORT" -L "$BULK:127.0.0.1:$BULK" -L "$LEASE:127.0.0.1:$LEASE" -L "$LIC:127.0.0.1:$LIC" \
      $R "$GATE" & TUNNEL_PID=$!
}
keep_tunnel() { while true; do start_tunnel; wait "$TUNNEL_PID"; sleep 5; done; }
keep_tunnel & KEEP_PID=$!

STOP=0
declare -A C_PID C_NAME C_CHOME C_OUT   # id -> bg pid / 컨테이너명 / CHOME / 컨테이너별 output (id 단조증가=spawn 순서)
NEXTID=0
JOB_BASE=$(( ${EDT_BASELINE_SEED_EPOCH:-0} + ${EDT_JOB_INDEX:-0} * 10000000 ))
COUNTER=0
remove_all() { for c in $(enroot list 2>/dev/null | grep "^edt-$SLURM_JOB_ID-"); do enroot remove -f "$c" >/dev/null 2>&1||true; done; }
cleanup() {
  STOP=1; kill "$KEEP_PID" "$TUNNEL_PID" 2>/dev/null||true
  for id in "${!C_PID[@]}"; do kill -TERM "${C_PID[$id]}" 2>/dev/null||true; done
  sleep 3; remove_all; rm -rf "$JOBDIR" 2>/dev/null||true
}
trap cleanup EXIT INT TERM

# 1솔브 컨테이너 1개 spawn(백그라운드). bg pid = enroot start = 컨테이너 init → SIGTERM이 entrypoint에 전달(안전종료).
spawn_one() {
  local id=$NEXTID; NEXTID=$((NEXTID+1))
  local seed=$(( JOB_BASE + COUNTER )); COUNTER=$((COUNTER+1))
  local C="edt-$SLURM_JOB_ID-$id-$$" CHOME=$JOBDIR/h-$id COUT=$OUT/c-$id
  rm -rf "$CHOME" "$COUT"; mkdir -p "$CHOME/tmp" "$COUT/work"; cp -a "$HOME/Ansoft" "$CHOME/Ansoft" 2>/dev/null || mkdir -p "$CHOME/Ansoft"
  enroot create --name "$C" "$IMG" >/dev/null 2>&1
  enroot start --root --rw \
    --mount "$ANSB/AnsysEM:/mnt/AnsysEM" --mount "$ANSB:/ansys_inc/v252" \
    --mount "$ANSB/licensingclient:/mnt/licensingclient" --mount "$HOME:$HOME" --mount "$JOBDIR:$JOBDIR" \
    --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env "NVIDIA_VISIBLE_DEVICES=$NVD" \
    --env "ANSYSLMD_LICENSE_FILE=${ANSYSLMD_LICENSE_FILE:-}" \
    --env "HOME=$CHOME" --env "TMPDIR=$CHOME/tmp" \
    --env "EDT_OUTPUT_ROOT=$COUT" --env "EDT_RESULT_INGEST_URL=$INGEST_URL" --env "EDT_WORK_DIR=$COUT/work" \
    --env "EDT_BULK_PORT=$BULK" --env "EDT_BULK_HOST=127.0.0.1" \
    --env "EDT_PRIORITY_LEASE_URL=$LEASE_URL" --env "EDT_LICENSE_CTRL_URL=" \
    --env "EDT_REFERENCE_SWEEP=$REF" --env "EDT_PARTITION=${EDT_PARTITION:-}" --env "VENVPY=$VENVPY" \
    --env "EDT_JOB_INDEX=${EDT_JOB_INDEX:-0}" --env "EDT_GPU_COUNT=${EDT_GPU_COUNT:-0}" \
    --env "EDT_SLOT_COUNT=1" --env "EDT_MAX_SIMS=1" \
    --env "EDT_BASELINE_BATCH=1" --env "EDT_BASELINE_WATERMARK=1" --env "EDT_BASELINE_SEED_START=$seed" \
    "$C" /bin/bash -lc '
      export PATH=/mnt/AnsysEM:$PATH
      exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
    ' > "$CLOG/${SLURM_JOB_ID}-${id}.log" 2>&1 &
  C_PID[$id]=$!; C_NAME[$id]=$C; C_CHOME[$id]=$CHOME; C_OUT[$id]=$COUT
}
reap() {  # 끝난(1솔브 후 exit) 컨테이너를 추적에서 제거 + enroot/스크래치 청소
  for id in "${!C_PID[@]}"; do
    if ! kill -0 "${C_PID[$id]}" 2>/dev/null; then
      wait "${C_PID[$id]}" 2>/dev/null||true
      enroot remove -f "${C_NAME[$id]}" >/dev/null 2>&1||true
      # 컨테이너 완전 종료 후 → CHOME + 컨테이너별 output 통째 삭제(누수 0). 솔브 도중이 아니라 레이스 없음.
      rm -rf "${C_CHOME[$id]}" "${C_OUT[$id]}" 2>/dev/null||true
      unset "C_PID[$id]" "C_NAME[$id]" "C_CHOME[$id]" "C_OUT[$id]"
    fi
  done
}
youngest_id() { local y=-1; for id in "${!C_PID[@]}"; do [ "$id" -gt "$y" ] && y=$id; done; echo "$y"; }
fetch_target() {  # 제어기 지령. 실패 시 기본 4.
  local t; t=$(curl -s -m4 "$PLAN_URL" 2>/dev/null | grep -oE '"target"[ :]+[0-9]+' | grep -oE '[0-9]+$')
  [ -n "$t" ] && echo "$t" || echo "${EDT_CONTAINER_TARGET:-4}"
}

while [ $STOP -eq 0 ]; do
  reap
  TGT=$(fetch_target)
  [ "$TGT" -gt 20 ] && TGT=20
  cur=${#C_PID[@]}
  if [ "$cur" -lt "$TGT" ]; then
    spawn_one          # 한 번에 1개(스태거) — 콜드스타트 thundering 회피
  elif [ "$cur" -gt "$TGT" ]; then
    yid=$(youngest_id); [ "$yid" -ge 0 ] && kill -TERM "${C_PID[$yid]}" 2>/dev/null||true   # 안전종료
  fi
  sleep 2
done
echo "[orch] DONE job=$SLURM_JOB_ID spawns=$COUNTER"
