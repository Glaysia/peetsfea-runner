# Roadmap 1C: Throughput KPI And Scaling Target

## Summary
- 서비스의 핵심 KPI는 입력이 남아 있는 동안 활성 slot 수를 최대한 유지하는 것이다.
- 현재 기준은 `240`, 확장 기준은 `400`이며, 문서와 지표 모두 이 숫자를 바로 보여줘야 한다.
- 처리량 목표가 불명확하면 worker refill, autorecovery, 대시보드 우선순위가 흐려진다.

## Implementation Decisions
- 현재 목표 capacity를 `3 * 10 * 8 = 240`으로 명시한다.
- 확장 목표 capacity를 `5 * 10 * 8 = 400`으로 명시한다.
- 운영 상태는 `목표 capacity`, `현재 active slots`, `부족분`, `복구 대기량`으로 표현한다.
- 입력 부족으로 줄어드는 경우와 장애로 줄어드는 경우를 구분한다.

## Acceptance Signals
- 운영 화면에서 현재 목표 대비 실제 활성 slot 수를 바로 볼 수 있다.
- 입력이 충분한 동안 목표 대비 활성 slot 부족분이 즉시 드러난다.
- 계정 추가 시 구조 변경 없이 목표 capacity만 증가한다.

## Assumptions
- `windows_per_job=8`, `max_jobs=10` 구조는 계속 유지한다.
- KPI는 총 처리 건수보다 현재 활성 slot 유지율을 더 우선한다.
