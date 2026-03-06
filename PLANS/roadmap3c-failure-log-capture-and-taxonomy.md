# Roadmap 3C: Failure Log Capture And Taxonomy

## Summary
- 실패 원인을 남기지 않으면 capacity drop을 복구해도 같은 장애가 반복된다.
- `exit code 1`만으로는 운영과 디버깅이 불가능하다.
- failure artifact 수집과 taxonomy는 24/7 운영의 필수 기능이다.

## Implementation Decisions
- case failure마다 최소 `run.log`, `exit.code`, launch stderr를 회수한다.
- worker failure와 case failure를 별도 상태와 메시지로 기록한다.
- 실패 taxonomy는 readiness, launch, solve, collect, cleanup으로 나눈다.
- 계산 노드 local `/tmp`에서 job 종료 전 반드시 failure artifacts를 묶어 전송한다.

## Acceptance Signals
- 실패한 case는 output 또는 로컬 artifact에서 원인 로그를 확인할 수 있다.
- 운영 로그에서 failure category가 드러난다.
- 동일 failure pattern을 계정별로 비교할 수 있다.

## Assumptions
- output 성공 여부와 무관하게 failure artifacts는 수집 대상이다.
- 실패 taxonomy는 운영 UI와 scheduler recovery 둘 다에서 사용된다.
