#!/bin/bash
# 잡당 서브 오케스트레이터 (롤링 라이프사이클) — 출생시 정해진 N개의 단명(1솔브) enroot 컨테이너를
# **stagger(노드당 동시 콜드스타트 제한)** 로 가동하고, respawn 없이 드레인되며, 20분 TTL에 종료.
#
#   1컨테이너 = 시뮬 1건 후 완전 종료(enroot remove) → AEDT·pyaedt 소멸 → 누수 OS회수.
#   N = 제어기가 잡 출생 시 결정(EDT_JOB_CONTAINERS). respawn 없음 → 살아있는 컨테이너는 시간이 지나며 감소.
#   동시 콜드스타트를 노드당 EDT_COLD_CAP개로 제한 → AEDT 동시 기동 gRPC herd 차단.
#   제어기(:7879)에 살아있는 컨테이너 수 주기 보고(관측/대시보드).
#   TTL(EDT_JOB_TTL_SEC, 기본 1200s) 경과 또는 전부 드레인 시 안전종료(강종 금지) → 잡 exit.
# 주의: set -u 금지 — 구버전 bash 빈 연관배열 접근이 "unbound variable"로 잡을 즉사시킨다.
echo "[orch] NODE=$(hostname) JOB=$SLURM_JOB_ID JIDX=${EDT_JOB_INDEX:-0} PART=${EDT_PARTITION:-}"
ANSB=/opt/ohpc/pub/Electronics/v252
DEPLOY=$HOME/edt-deploy
VENVPY=$DEPLOY/venv/bin/python
IMG=$HOME/runtime/enroot/aedt.sqsh
resolve_peetsfea_toml() {
  RESOLVED_TOML=$("$VENVPY" -m peetsfea_runner.peetsfea_data "$1")
  rc=$?
  if [ "$rc" -ne 0 ] || [ -z "$RESOLVED_TOML" ]; then
    echo "[orch] failed to resolve peetsfea $1 TOML"
    exit 1
  fi
}
resolve_peetsfea_toml sweep
REF=$RESOLVED_TOML
JOBDIR=/enroot/${USER}_${SLURM_JOB_ID}
OUT=$JOBDIR/run_out
CLOG=$DEPLOY/clogs; mkdir -p "$OUT" "$CLOG"

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

# /enroot 여유 가드 — 타 유저가 채운 포화 노드에 떨어지면 솔브가 No-space로 폭사 → 스킵 종료(keeper 재배치).
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
PLAN_URL="http://127.0.0.1:$LIC/job_plan?job=${EDT_JOB_INDEX:-0}"
REPORT_URL="http://127.0.0.1:$LIC/orch_report"
case "${EDT_PARTITION:-}" in gpu*) NVD=all ;; *) NVD=void ;; esac

# 라이프사이클 파라미터
TTL=${EDT_JOB_TTL_SEC:-1200}              # 잡 수명(초, 20분) — 경과 시 안전종료
STAGGER=${EDT_SPAWN_STAGGER_SEC:-15}      # 컨테이너 출생 최소 간격(초)
COLD_EST=${EDT_COLD_EST_SEC:-200}         # 콜드스타트로 간주하는 나이(초)
COLD_CAP=${EDT_COLD_CAP:-10}              # 노드당 동시 콜드스타트 상한(herd 차단). 10 동시 기동은 안전 확인됨.
START=$(date +%s)

# N = 제어기가 출생 시 결정한 컨테이너 수. env 없으면 /job_plan 1회 조회, 그것도 없으면 기본 20.
fetch_N() { curl -s -m4 "$PLAN_URL" 2>/dev/null | grep -oE '"n"[ :]+[0-9]+' | grep -oE '[0-9]+$'; }
N=${EDT_JOB_CONTAINERS:-}
[ -z "$N" ] && N=$(fetch_N)
[ -z "$N" ] && N=20
[ "$N" -gt 20 ] 2>/dev/null && N=20
[ "$N" -lt 1 ] 2>/dev/null && N=1
echo "[orch] N=$N TTL=${TTL}s stagger=${STAGGER}s cold_cap=${COLD_CAP}"

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
declare -A C_PID C_NAME C_CHOME C_OUT C_BORN   # id -> bg pid / 컨테이너명 / CHOME / 컨테이너별 output / 출생시각
NEXTID=0
JOB_BASE=$(( ${EDT_BASELINE_SEED_EPOCH:-0} + ${EDT_JOB_INDEX:-0} * 10000000 ))
COUNTER=0
SPAWNED=0
remove_all() { for c in $(enroot list 2>/dev/null | grep "^edt-$SLURM_JOB_ID-"); do enroot remove -f "$c" >/dev/null 2>&1||true; done; }
cleanup() {
  STOP=1; kill "$KEEP_PID" "$TUNNEL_PID" 2>/dev/null||true
  for id in "${!C_PID[@]}"; do kill -TERM "${C_PID[$id]}" 2>/dev/null||true; done   # 안전종료(SIGTERM → entrypoint)
  sleep 3; remove_all; rm -rf "$JOBDIR" 2>/dev/null||true
}
trap cleanup EXIT INT TERM

# 1솔브 컨테이너 1개 spawn(백그라운드). bg pid = enroot start = 컨테이너 init → SIGTERM이 entrypoint에 전달.
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
  C_PID[$id]=$!; C_NAME[$id]=$C; C_CHOME[$id]=$CHOME; C_OUT[$id]=$COUT; C_BORN[$id]=$(date +%s)
  SPAWNED=$((SPAWNED+1))
}
reap() {  # 끝난(1솔브 후 exit) 컨테이너를 추적에서 제거 + enroot/스크래치 청소. **respawn 없음** — 드레인.
  for id in "${!C_PID[@]}"; do
    if ! kill -0 "${C_PID[$id]}" 2>/dev/null; then
      wait "${C_PID[$id]}" 2>/dev/null||true
      enroot remove -f "${C_NAME[$id]}" >/dev/null 2>&1||true
      rm -rf "${C_CHOME[$id]}" "${C_OUT[$id]}" 2>/dev/null||true
      unset "C_PID[$id]" "C_NAME[$id]" "C_CHOME[$id]" "C_OUT[$id]" "C_BORN[$id]"
    fi
  done
}
young_count() {  # 콜드스타트중(나이 < COLD_EST)인 살아있는 컨테이너 수 — herd cap 판정
  local now=$1 y=0
  for id in "${!C_BORN[@]}"; do [ $(( now - ${C_BORN[$id]} )) -lt "$COLD_EST" ] && y=$((y+1)); done
  echo "$y"
}
report() {  # 제어기에 살아있는 컨테이너 수 보고(가장 적게 남은 잡 선택용). 실패 무시.
  local live=${#C_PID[@]} now=$1
  curl -s -m3 -X POST "$REPORT_URL" \
    -d "job=${EDT_JOB_INDEX:-0}&slurm=$SLURM_JOB_ID&live=$live&spawned=$SPAWNED&target=$N&age=$(( now - START ))" >/dev/null 2>&1 || true
}

LAST_SPAWN=0
while [ $STOP -eq 0 ]; do
  reap
  now=$(date +%s)
  # 종료 조건: TTL 경과 또는 (N 다 띄웠는데 전부 드레인) → 안전종료
  if [ $(( now - START )) -ge "$TTL" ]; then echo "[orch] TTL ${TTL}s 도달 — 안전종료"; break; fi
  if [ "$SPAWNED" -ge "$N" ] && [ "${#C_PID[@]}" -eq 0 ]; then echo "[orch] N개 전부 드레인 — 종료"; break; fi
  # staggered 출생: 아직 N 미달 + 간격 충족 + 콜드스타트 cap 미만일 때만 1개
  if [ "$SPAWNED" -lt "$N" ] && [ $(( now - LAST_SPAWN )) -ge "$STAGGER" ] && [ "$(young_count "$now")" -lt "$COLD_CAP" ]; then
    spawn_one; LAST_SPAWN=$now
  fi
  report "$now"
  sleep 2
done
echo "[orch] DONE job=$SLURM_JOB_ID spawned=$SPAWNED"
