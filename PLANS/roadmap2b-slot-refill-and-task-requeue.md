# Roadmap 2B: Slot Refill And Task Requeue

## Summary
- worker가 살아 있어도 내부 slot refill이 늦으면 전체 throughput이 무너진다.
- worker나 case가 실패할 때 미완료 `.aedt`가 사라지지 않고 다시 queue로 돌아가야 한다.
- refill과 requeue는 같은 문제의 양면이다.

## Implementation Decisions
- worker 내부 빈 slot은 batch 종료를 기다리지 않고 즉시 다음 입력을 받는다.
- 미완료 입력은 worker failure, timeout, launch failure 시 재큐잉한다.
- 재큐잉 입력은 일반 신규 입력과 같은 scheduler 경로로 다시 공급한다.
- 이미 terminal output이 충분히 materialize된 입력만 최종 실패 또는 성공으로 닫는다.

## Acceptance Signals
- slot 하나가 끝난 직후 idle 시간이 짧게 유지된다.
- worker failure 뒤 해당 입력이 queue로 복귀한다.
- 같은 input이 중복으로 동시에 두 slot에 배정되지 않는다.

## Assumptions
- 실패한 worker 자체는 버려질 수 있어도, 입력은 버려지지 않는 방향을 기본으로 둔다.
- 재큐잉은 같은 계정에 고정하지 않고 전체 capacity 기준으로 재배치할 수 있다.
