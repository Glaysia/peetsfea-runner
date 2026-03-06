# Roadmap 2A: Account Worker Pool Maintenance

## Summary
- 계정당 worker job 수를 항상 `10`으로 유지하는 것이 throughput 유지의 출발점이다.
- 지금처럼 worker가 terminal state로 빠지고 보충되지 않으면 목표 용량이 즉시 깨진다.
- worker pool 유지 로직은 scheduler의 최우선 책임이 되어야 한다.

## Implementation Decisions
- 계정별 target worker count를 상태로 유지한다.
- `RUNNING`, `PENDING`, `BOOTING`을 합친 live worker 수가 `10` 미만이면 즉시 replacement를 계획한다.
- `FAILED`, `QUARANTINED`, `STOPPED` worker는 target count 계산에서 제외한다.
- service 실행 중에는 입력이 남아 있는 한 target worker count를 계속 만족시키려 한다.

## Acceptance Signals
- 특정 계정에서 worker 여러 개가 죽어도 다시 `10`까지 복구된다.
- `squeue` 기준 계정별 running/pending worker 수가 입력 충분 시 `10`에 근접한다.
- worker drop 후 긴 공백 없이 replacement worker가 제출된다.

## Assumptions
- worker job은 계정당 최대 `10`을 넘기지 않는다.
- 부족한 worker를 보충하는 것이 새 입력 분배보다 우선이다.
