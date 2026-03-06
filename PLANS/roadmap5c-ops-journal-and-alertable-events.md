# Roadmap 5C: Ops Journal And Alertable Events

## Summary
- 24/7 운영에서는 "무슨 일이 있었는지"가 장기 로그와 이벤트로 남아야 한다.
- journal과 최근 이벤트 피드는 capacity drop, quarantine 폭증, readiness failure를 사람이 바로 찾을 수 있게 해야 한다.
- 장기적으로는 경고 가능한 이벤트 집합이 필요하다.

## Implementation Decisions
- worker drop, replacement submit, readiness failure, repeated quarantine, collect failure를 주요 운영 이벤트로 정의한다.
- 최근 이벤트 API와 journal이 같은 핵심 사건을 공통 키로 보여주게 한다.
- 계정별 failure burst를 운영 이벤트로 남긴다.
- 목표 throughput 미달이 일정 시간 지속될 때 alertable 상태로 본다.

## Acceptance Signals
- 운영자는 journal과 recent events만으로 capacity drop 원인을 좁힐 수 있다.
- 동일 계정에서 quarantine가 연속 발생하면 이벤트로 드러난다.
- 목표 capacity 미달 상태가 장시간 지속될 때 운영자가 즉시 식별할 수 있다.

## Assumptions
- 알림 시스템 자체를 이번 문서에서 구현하지 않아도, alertable event 정의는 먼저 필요하다.
- 운영 이벤트는 계정 단위와 run 단위 둘 다 추적 가능해야 한다.
