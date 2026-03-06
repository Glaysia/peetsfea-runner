# Roadmap 3B: Runtime Bootstrap And Preflight

## Summary
- readiness를 자동화하려면 bootstrap과 preflight가 필요하다.
- bootstrap은 런타임이 없는 계정을 usable 상태로 만드는 단계이고, preflight는 제출 직전 실제 실행 가능성을 확인하는 단계다.
- 둘 중 하나라도 없으면 새 계정이 추가될 때마다 운영자가 수동 개입하게 된다.

## Implementation Decisions
- bootstrap은 계정 홈에 필요한 venv/runtime을 준비하는 장기 작업으로 정의한다.
- preflight는 worker 제출 전에 짧게 실행되는 검증 단계로 정의한다.
- bootstrap 중인 계정은 running capacity 계산과 분리한다.
- preflight 실패 이유는 계정 상태에 저장하고 재시도 정책을 둔다.

## Acceptance Signals
- 새 계정 추가 시 service가 bootstrap 필요 여부를 식별한다.
- preflight 실패가 있으면 원인 메시지가 남는다.
- bootstrap 완료 후 별도 수동 수정 없이 worker 제출이 가능해진다.

## Assumptions
- bootstrap은 login node에서 끝내는 것이 compute node bootstrap보다 안정적이다.
- preflight는 빠르고 반복 가능해야 한다.
