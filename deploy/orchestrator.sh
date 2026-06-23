#!/bin/bash
# 잡당 서브 오케스트레이터 (고정 잡 + respawn-to-N) — 단명(1솔브) enroot 컨테이너를 **stagger(노드당
# 동시 콜드스타트 제한)** 로 가동하고, 죽으면 보충해 **목표 N을 유지**하며, TTL에 안전종료(키퍼가 재제출).
#
#   1컨테이너 = 시뮬 1건 후 완전 종료(unshare PID-ns → enroot remove) → AEDT 고아까지 전량 OS회수.
#   N = 잡별 컨테이너 목표. /job_plan?job=i를 주기 재조회(적분제어가 동적 조절) 또는 EDT_JOB_CONTAINERS 고정.
#     respawn-to-N: alive<N이면 보충, N 감소 시엔 자연 드레인(kill 안 함). 잡은 제어 목적으로 안 죽임.
#   동시 콜드스타트를 노드당 EDT_COLD_CAP개로 제한 → AEDT 동시 기동 gRPC herd 차단.
#   제어기(:7879)에 살아있는 컨테이너 수 주기 보고(관측/대시보드 + /orch_report 피드백).
#   TTL(EDT_JOB_TTL_SEC) 경과 시 안전종료(강종 금지) → 잡 exit → 키퍼가 새 잡으로 교체(고정 인프라 유지).
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
# 다계정: hmlee31 게이트는 .bashrc에 ANSYSLMD_LICENSE_FILE 미설정이라 컨테이너 라이선스가 비어
# ansysedt가 시작 시 rc=134(SIGABRT)로 죽는다(0 솔브 + churn). 전역 FlexLM 기본값 보장(env 있으면 우선).
: "${ANSYSLMD_LICENSE_FILE:=1055@license-server}"; export ANSYSLMD_LICENSE_FILE
PORT=${EDT_INGEST_PORT:-7876}; BULK=${EDT_BULK_PORT:-7877}
LEASE=${EDT_PRIORITY_LEASE_PORT:-7878}; LIC=${EDT_LICENSE_CTRL_PORT:-7879}
SSHD=${EDT_ORCH_SSHD_PORT:-0}
INGEST_URL="http://127.0.0.1:$PORT/ingest"; LEASE_URL="http://127.0.0.1:$LEASE/lease"
PLAN_URL="http://127.0.0.1:$LIC/job_plan?job=${EDT_JOB_INDEX:-0}"
REPORT_URL="http://127.0.0.1:$LIC/orch_report"
case "${EDT_PARTITION:-}" in gpu*) NVD=all ;; *) NVD=void ;; esac

# 라이프사이클 파라미터
TTL=${EDT_JOB_TTL_SEC:-1800}              # 잡 수명(초, 20분) — 경과 시 안전종료
STAGGER=${EDT_SPAWN_STAGGER_SEC:-15}      # 컨테이너 출생 최소 간격(초)
COLD_EST=${EDT_COLD_EST_SEC:-200}         # 콜드스타트로 간주하는 나이(초)
COLD_CAP=${EDT_COLD_CAP:-10}              # 노드당 동시 콜드스타트 상한(herd 차단). 10 동시 기동은 안전 확인됨.
START=$(date +%s)

# N = 잡별 컨테이너 목표. EDT_JOB_CONTAINERS로 고정(테스트)하거나, 미설정 시 /job_plan?job=i에서 받아
# 주기 재조회한다(적분제어가 동적으로 바꾼다 → respawn-to-N). 잡은 죽지 않고 컨테이너 수만 N에 맞춘다.
fetch_N() { curl -s -m4 "$PLAN_URL" 2>/dev/null | grep -oE '"n"[ :]+[0-9]+' | grep -oE '[0-9]+$'; }
clamp_N() { [ -z "$N" ] && N=12; [ "$N" -gt 20 ] 2>/dev/null && N=20; [ "$N" -lt 0 ] 2>/dev/null && N=0; }
if [ -n "${EDT_JOB_CONTAINERS:-}" ]; then N=$EDT_JOB_CONTAINERS; N_FIXED=1; else N=$(fetch_N); N_FIXED=""; fi
clamp_N
REFETCH_SEC=${EDT_PLAN_REFETCH_SEC:-30}   # /job_plan 재조회 주기(고정 N이면 무시)
echo "[orch] N=$N TTL=${TTL}s stagger=${STAGGER}s cold_cap=${COLD_CAP} fixed=${N_FIXED:-0}"

# 디버그 sshd: 클러스터 22(중앙인증·손못댐) 대신 **우리 소유 sshd**를 노드 로컬 포트에 non-root로 띄운다.
# 키/authorized_keys 자동생성·등록(매번 idempotent). StrictModes no·UsePAM no라 평유저로 우리 키만 받는다.
# 노드 로컬 포트(DEBUG_LOCAL)와 게이트 포트(SSHD) 둘 다 launcher가 (계정×잡)별 유일값으로 주입 → 충돌 0.
DEBUG_LOCAL=${EDT_DEBUG_LOCAL_SSHD:-0}
DBG_SSHD_PID=""
setup_debug_sshd() {
  [ "${SSHD:-0}" -gt 0 ] 2>/dev/null && [ "${DEBUG_LOCAL:-0}" -gt 0 ] 2>/dev/null || return 0
  local D=$HOME/edt-deploy/debug; mkdir -p "$D"; chmod 700 "$D" 2>/dev/null || true
  [ -f "$D/edt_debug" ] || ssh-keygen -t ed25519 -f "$D/edt_debug" -N "" -q -C edt-debug 2>/dev/null  # 클라이언트 키 자동생성
  [ -f "$D/hostkey" ]   || ssh-keygen -t ed25519 -f "$D/hostkey"   -N "" -q 2>/dev/null                # 호스트 키 자동생성
  cp "$D/edt_debug.pub" "$D/authorized_keys" 2>/dev/null; chmod 600 "$D/authorized_keys" 2>/dev/null || true  # 등록 자동
  local CFG=$D/sshd_config.$SLURM_JOB_ID
  printf 'Port %s\nListenAddress 127.0.0.1\nHostKey %s\nAuthorizedKeysFile %s\nStrictModes no\nUsePAM no\nPidFile %s\n' \
    "$DEBUG_LOCAL" "$D/hostkey" "$D/authorized_keys" "$D/pid.$SLURM_JOB_ID" > "$CFG"
  local S; S=$(command -v sshd || echo /usr/sbin/sshd)
  "$S" -f "$CFG" -E "$D/sshd.$SLURM_JOB_ID.log" 2>/dev/null \
    && { DBG_SSHD_PID=$(cat "$D/pid.$SLURM_JOB_ID" 2>/dev/null); \
         echo "[orch] debug sshd ↑ node:127.0.0.1:$DEBUG_LOCAL ←gate:$SSHD  접속: ssh -J <gate> -p $SSHD -i edt_debug $USER@127.0.0.1"; } \
    || echo "[orch] debug sshd 기동 실패(무시)"
}
setup_debug_sshd

# compute node → gate 정터널 (결과/산출물/lease/제어기) + 디버그 sshd 역노출(우리 sshd 포트로)
TUNNEL_PID=""
start_tunnel() {
  local R=""; [ "${SSHD:-0}" -gt 0 ] 2>/dev/null && [ "${DEBUG_LOCAL:-0}" -gt 0 ] 2>/dev/null && R="-R ${SSHD}:127.0.0.1:${DEBUG_LOCAL}"
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
  [ -n "$DBG_SSHD_PID" ] && kill "$DBG_SSHD_PID" 2>/dev/null||true   # 디버그 sshd 종료(게이트 포트 회수)
  rm -f "$HOME/edt-deploy/debug/sshd_config.$SLURM_JOB_ID" "$HOME/edt-deploy/debug/pid.$SLURM_JOB_ID" 2>/dev/null||true
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
    --mount /gpfs:/gpfs \

    --env ANSYSEM_ROOT252=/mnt/AnsysEM --env ANS_IGNOREOS=1 --env "NVIDIA_VISIBLE_DEVICES=$NVD" \
    --env "ANSYSLMD_LICENSE_FILE=${ANSYSLMD_LICENSE_FILE:-}" \
    --env "HOME=$CHOME" --env "TMPDIR=$CHOME/tmp" \
    --env "EDT_OUTPUT_ROOT=$COUT" --env "EDT_RESULT_INGEST_URL=$INGEST_URL" --env "EDT_WORK_DIR=$COUT/work" \
    --env "EDT_BULK_PORT=$BULK" --env "EDT_BULK_HOST=127.0.0.1" \
    --env "EDT_PRIORITY_LEASE_URL=$LEASE_URL" --env "EDT_LICENSE_CTRL_URL=" \
    --env "EDT_REFERENCE_SWEEP=$REF" --env "EDT_PARTITION=${EDT_PARTITION:-}" --env "VENVPY=$VENVPY" \
    --env "EDT_JOB_INDEX=${EDT_JOB_INDEX:-0}" --env "EDT_GPU_COUNT=${EDT_GPU_COUNT:-0}" \
    --env "EDT_ACCOUNT_ID=${EDT_ACCOUNT_ID:-account_01}" --env "EDT_HOST_ALIAS=${EDT_HOST_ALIAS:-gate1-harry261}" \
    --env "EDT_SLOT_COUNT=1" --env "EDT_MAX_SIMS=1" \
    --env "EDT_BASELINE_BATCH=1" --env "EDT_BASELINE_WATERMARK=1" --env "EDT_BASELINE_SEED_START=$seed" \
    --env "EDT_PID_NS=${EDT_PID_NS:-1}" \
    "$C" /bin/bash -lc '
      export PATH=/mnt/AnsysEM:$PATH
      # PID-ns 격리(EDT_PID_NS=1, 기본): python=새 PID 네임스페이스의 PID 1 → 1솔브 후 exit 시
      # 커널이 그 ns의 잔여 프로세스(고아 ansysedt) 전량 SIGKILL → 노드 누수 회수. enroot는 host PID ns를
      # 공유해 enroot remove만으론 AEDT 고아가 살아남아 누적(4h 검증: RSS 14→433GB·FD 339→32273).
      # A/B 검증(687790 vs 687794): 격리=평탄, 무격리=우상향. enroot --root의 user-ns CAP_SYS_ADMIN이 unshare 허용.
      if [ "${EDT_PID_NS:-1}" = "1" ] && unshare --pid --fork --mount-proc true 2>/dev/null; then
        exec unshare --pid --fork --mount-proc "$VENVPY" -m peetsfea_runner.edt_entrypoint
      else
        [ "${EDT_PID_NS:-1}" = "1" ] && echo "[orch] WARN: unshare --pid 불가 — 격리 없이 진행(누수 누적 위험)" >&2
        exec "$VENVPY" -m peetsfea_runner.edt_entrypoint
      fi
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

LAST_SPAWN=0; LAST_REFETCH=0
while [ $STOP -eq 0 ]; do
  reap                                   # 죽은(1솔브 끝) 컨테이너 제거 → 슬롯 비움
  now=$(date +%s)
  if [ $(( now - START )) -ge "$TTL" ]; then echo "[orch] TTL ${TTL}s 도달 — 안전종료"; break; fi
  # 잡별 목표 N 주기 재조회(고정 N이 아니면). 적분제어가 동적으로 바꾼 값을 반영.
  if [ -z "$N_FIXED" ] && [ $(( now - LAST_REFETCH )) -ge "$REFETCH_SEC" ]; then
    newn=$(fetch_N); LAST_REFETCH=$now
    if [ -n "$newn" ]; then N=$newn; clamp_N; fi
  fi
  alive=${#C_PID[@]}
  # respawn-to-N: 살아있는 수가 N 미만이면 stagger+cold_cap로 1개 보충(초과=N 감소는 자연 드레인, kill 안 함).
  if [ "$alive" -lt "$N" ] && [ $(( now - LAST_SPAWN )) -ge "$STAGGER" ] && [ "$(young_count "$now")" -lt "$COLD_CAP" ]; then
    spawn_one; LAST_SPAWN=$now
  fi
  report "$now"
  sleep 2
done
echo "[orch] DONE job=$SLURM_JOB_ID 누적솔브사이클=$SPAWNED"
