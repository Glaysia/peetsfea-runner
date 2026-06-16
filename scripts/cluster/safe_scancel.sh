#!/bin/bash
# safe-scancel — /enroot 잔재 없이 peetsfea 잡을 안전하게 취소한다. **raw `scancel` 대신 항상 이걸 사용.**
#
# 왜: raw `scancel`은 SIGTERM 후 partition KillWait(짧을 수 있음) 뒤 SIGKILL을 보낸다. SIGKILL이
# slot_service의 EXIT/TERM trap보다 먼저 오면 노드 로컬 /enroot/{USER}_{JOB} 스크래치가 안 지워져
# 공용 공간에 잔재가 남는다. 그래서 여기선 **graceful SIGTERM만 먼저 보내고**(자동 KILL 없음) trap이
# 컨테이너를 내리고 `rm -rf /enroot/{job}`까지 끝낼 충분한 grace를 준 뒤, 끝내 안 죽은 것만 강제 종료한다.
# 강제 종료된 잔재도 그 노드에 이 유저의 다음 잡이 뜰 때 slot_service 시작 스윕이 정리한다.
#
#   safe_scancel.sh [JOBID ...]        인자 없으면 이 유저의 peetsfea-edt 잡 전부
#   SAFE_SCANCEL_GRACE_SECONDS=120     graceful 대기 상한(기본 120s)
set -u
U=${USER:-$(whoami)}
GRACE=${SAFE_SCANCEL_GRACE_SECONDS:-120}

if [ "$#" -gt 0 ]; then
  JIDS="$*"
else
  JIDS=$(squeue -h -u "$U" -o "%i %j" 2>/dev/null | awk '$2 ~ /^peetsfea-edt/{print $1}' | tr '\n' ' ')
fi
JIDS=$(echo $JIDS)
if [ -z "$JIDS" ]; then echo "[safe-scancel] 대상 잡 없음"; exit 0; fi
CSV=$(echo "$JIDS" | tr ' ' ,)

echo "[safe-scancel] graceful SIGTERM → $JIDS (grace ${GRACE}s; slot_service trap이 /enroot 청소)"
# --full: 배치 스크립트(slot_service bash) + 모든 자식(enroot/supervisor/ansysedt)에 SIGTERM.
# supervisor가 워커를 내리고 enroot가 종료 → bash 포그라운드 복귀 → trap이 rm -rf /enroot/{job}.
# PENDING 잡은 /enroot 스크래치가 없으므로(아직 안 떴음) 즉시 일반 scancel로 큐에서 뺀다.
# (scancel --signal=TERM은 프로세스가 없는 PENDING 잡을 큐에서 제거하지 못해 orphan으로 남는다.)
PEND=$(squeue -h -j "$CSV" -t PD,CF -o "%i" 2>/dev/null | tr '\n' ' ')
[ -n "${PEND// }" ] && { echo "[safe-scancel] PENDING 즉시 정리: $PEND"; scancel $PEND 2>/dev/null; }
# RUNNING/COMPLETING 잡만 graceful SIGTERM → slot_service trap이 /enroot 청소.
RUN=$(squeue -h -j "$CSV" -t R,CG -o "%i" 2>/dev/null | tr '\n' ' ')
[ -n "${RUN// }" ] && scancel --full --signal=TERM $RUN 2>/dev/null

deadline=$(( $(date +%s) + GRACE ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  left=$(squeue -h -j "$CSV" -o "%i" 2>/dev/null | grep -c .)
  if [ "${left:-0}" -eq 0 ]; then
    echo "[safe-scancel] 전부 정상 종료 — trap이 /enroot를 청소했습니다."
    exit 0
  fi
  sleep 5
done

echo "[safe-scancel] grace 만료, 아직 ${left} 잔존 → 강제 종료(SIGKILL). /enroot 잔재는 다음 잡 startup sweep이 정리."
scancel $JIDS 2>/dev/null
exit 0
