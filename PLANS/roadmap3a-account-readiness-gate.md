# Roadmap 3A: Account Readiness Gate

## Summary
- 새 계정이 추가됐을 때 가장 먼저 필요한 건 worker 제출 전에 readiness를 확인하는 것이다.
- readiness가 안 된 계정에 job을 보내면 worker slot만 낭비하고 처리량이 깨진다.
- readiness는 계정별로 명시적 상태를 가져야 한다.

## Implementation Decisions
- 계정별 readiness check를 worker 제출 전 단계로 둔다.
- readiness 항목은 최소한 Python, venv, Ansys module, 필수 바이너리, 실행 가능한 홈/runtime 경로를 포함한다.
- readiness 미충족 계정은 `disabled for dispatch` 상태로 분리한다.
- readiness 회복 후에만 다시 worker dispatch 후보에 넣는다.

## Acceptance Signals
- readiness가 없는 계정은 조용히 worker가 죽지 않고 dispatch 대상에서 빠진다.
- readiness 상태는 계정별로 노출된다.
- readiness가 만족되면 같은 service 실행 중 다시 dispatch 대상에 복귀한다.

## Assumptions
- 계정별 홈과 runtime 상태는 상호 독립적이다.
- readiness 실패는 solve 실패와 별개 범주다.
