# Roadmap 4C: Input Deletion And Postrun Cleanup

## Summary
- 24/7 운영에서는 처리 완료 입력이 queue에 계속 남아 있으면 혼란과 중복 실행 위험이 커진다.
- 입력 삭제는 선택 기능이 아니라 운영 lifecycle의 마지막 단계다.
- 삭제 실패와 삭제 지연도 추적 가능한 상태여야 한다.

## Implementation Decisions
- 입력 삭제 기준을 정의한다.
  - 최소 기준은 업로드 또는 실행 시작 후 재실행 방지
  - 최종 기준은 성공 또는 정책 만족 후 삭제/이동
- `.aedt`와 `.ready`는 함께 정리한다.
- 삭제 실패 시 retry와 quarantine 경로를 둔다.
- 삭제 상태는 per-window lifecycle에 남긴다.

## Acceptance Signals
- 처리 완료 입력이 queue에 무기한 남지 않는다.
- 삭제 실패 시 입력이 사라지지 않고 quarantine 또는 retry 상태로 남는다.
- 운영자는 어떤 입력이 실행 완료, 삭제 대기, 삭제 실패 상태인지 구분할 수 있다.

## Assumptions
- 입력 삭제는 성공 즉시만이 아니라 정책 조건 충족 후에 일어날 수 있다.
- 삭제는 output materialize 이후에만 수행한다.
