# Roadmap 1B: Service Loop And Run Model

## Summary
- 서비스는 배치 스크립트가 아니라 상시 loop로 동작해야 한다.
- 입력이 없으면 idle, 입력이 생기면 즉시 active, 다시 입력이 없으면 idle로 돌아가는 모델이 필요하다.
- run 개념은 한 번의 종료성 실행보다 장기 service state를 보조하는 식별자로 내려가야 한다.

## Implementation Decisions
- `run_user_worker_loop()`는 서비스 생존 기간 전체를 책임지는 메인 루프로 유지한다.
- 입력이 있는 동안만 active state를 유지하고, 입력이 없으면 idle state로 heartbeat를 갱신한다.
- run rotation은 유지하되, 운영자는 service continuity와 active/idle 상태를 더 중요하게 본다.
- service 재시작 없이 입력 유입에 반응하는 것이 기본 모델이다.

## Acceptance Signals
- 입력이 없을 때 service는 종료하지 않고 idle heartbeat를 유지한다.
- 입력이 들어오면 다음 loop에서 자동 active 상태로 전환된다.
- 서비스가 하루 이상 떠 있어도 반복 실행으로 상태가 깨지지 않는다.

## Assumptions
- `run_pipeline(config)` 단일 진입점 계약은 유지한다.
- `run_id`는 운영 식별자이지 수동 실행 단위 의미를 줄여도 된다.
