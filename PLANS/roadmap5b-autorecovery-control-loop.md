# Roadmap 5B: Autorecovery Control Loop

## Summary
- service 전체 재시작 없이 capacity를 복구하려면 독립된 autorecovery loop가 필요하다.
- 이 loop는 계정별 worker 수, readiness 상태, terminal worker 수를 감시하고 자동 보충을 수행해야 한다.
- autorecovery가 없으면 운영자는 계속 수동 intervention을 하게 된다.

## Implementation Decisions
- 계정별 live worker 수가 target 아래로 떨어지면 recovery action을 트리거한다.
- readiness 실패 계정은 recovery 대상에서 제외하고 명시적 원인 상태를 남긴다.
- terminal worker의 미완료 입력을 재큐잉하고 replacement worker를 즉시 제출한다.
- recovery action 빈도와 실패 반복을 rate-limit 하되 장시간 방치하지 않는다.

## Acceptance Signals
- 일부 계정 worker가 죽어도 service 재시작 없이 worker 수가 복구된다.
- readiness 미충족 계정은 무한 실패 루프 대신 blocked 상태로 보인다.
- autorecovery 동작이 journal과 웹 상태에 남는다.

## Assumptions
- recovery는 scheduler 내부 루프의 책임으로 둔다.
- manual restart는 예외 복구 수단이다.
