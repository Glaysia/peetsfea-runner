# Roadmap 5A: Live Capacity Dashboard

## Summary
- 운영자가 가장 먼저 봐야 할 것은 현재 활성 worker와 활성 slot이 목표 대비 얼마나 부족한지다.
- 총 처리량 누적값보다 live capacity 상태가 더 중요하다.
- 대시보드는 "지금 240이 왜 안 나오나"에 즉시 답해야 한다.

## Implementation Decisions
- 계정별 active worker, pending worker, active slots, completed slots를 노출한다.
- 목표 `240/400` 대비 현재 활성 slot 수와 부족분을 노출한다.
- `running_count`, `pending_count`, `allowed_submit`, `quarantined_count`를 같이 보여준다.
- 입력 충분 여부와 capacity 부족 여부를 분리해서 표시한다.

## Acceptance Signals
- 대시보드에서 계정별로 몇 개의 worker가 실제 running인지 즉시 보인다.
- 목표 대비 부족한 slot 수가 바로 보인다.
- 특정 계정만 capacity가 빈 상태를 시각적으로 구분할 수 있다.

## Assumptions
- 웹 상태 페이지는 장기 운영에서 1차 관찰 수단으로 유지한다.
- capacity 지표는 service loop 한 번마다 갱신 가능해야 한다.
