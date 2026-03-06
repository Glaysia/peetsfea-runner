# Roadmap 2C: Terminal State Recovery Within Run

## Summary
- 같은 run 안에서 terminal worker를 복구하지 못하면 24/7 서비스 목표가 성립하지 않는다.
- `QUARANTINED`와 `FAILED`는 상태 기록일 뿐, 곧바로 capacity 감소를 용인하는 신호가 되어서는 안 된다.
- run 중 자동 회복이 서비스 재시작보다 우선이다.

## Implementation Decisions
- terminal worker 발생 시 즉시 replacement scheduling을 트리거한다.
- replacement scheduling은 다음 poll을 기다리지 않고 가능한 한 빨리 일어난다.
- terminal worker에 묶여 있던 입력 상태와 worker slot 상태를 분리해서 처리한다.
- 같은 run 안에서 복구 가능한 오류는 service-level terminal로 승격하지 않는다.

## Acceptance Signals
- `QUARANTINED` worker가 생겨도 전체 active worker 수가 회복된다.
- service 재시작 없이 같은 run 안에서 capacity가 복구된다.
- `FAILED`/`QUARANTINED` worker 수와 replacement worker 수가 추적된다.

## Assumptions
- run 중 자동 복구는 scheduler 책임으로 둔다.
- terminal state 기록과 capacity refill은 동시에 일어나야 한다.
