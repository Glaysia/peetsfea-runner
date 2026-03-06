# Roadmap 2: Capacity Maintenance And Refill

## Summary
- 처리량 문제의 핵심은 "입력이 남아 있는데도 계정별 worker 수가 줄어드는 상태"를 허용하면 안 된다는 점이다.
- 서비스는 계정당 Slurm worker job `10개`를 유지하고, 각 worker 내부 `8개` slot이 비면 즉시 다음 `.aedt`를 공급해야 한다.
- worker 종료, 실패, timeout, quarantine가 발생해도 미완료 입력은 사라지지 않고 다시 queue로 돌아가야 한다.

## Must-Have Improvements
- 계정당 worker job 목표 수를 `10`으로 고정하고, 입력이 충분한 동안에는 실제 running job 수가 이 목표 아래로 떨어지면 즉시 대체 worker를 띄운다.
- worker job이 종료되더라도 그 worker가 맡고 있던 미완료 입력은 재큐잉한다.
- worker 내부 `8`개 slot 중 하나가 비면 전체 batch 완료를 기다리지 않고 다음 입력을 즉시 할당한다.
- worker 생명주기와 task 생명주기를 분리한다. task 실패가 곧 worker 폐기를 뜻하지 않도록 한다.
- 특정 계정에서 worker가 여러 개 죽더라도 같은 run 안에서 다시 `10`개까지 회복되도록 한다.
- 입력이 남아 있는 동안 계정별 capacity drop을 자동 복구하는 refill loop를 운영 계약으로 둔다.

## Acceptance Signals
- 입력이 충분할 때 계정 3개 기준 활성 worker 수가 총 `30`에서 장시간 내려가지 않는다.
- 특정 계정의 worker가 죽어도 같은 run 안에서 다시 `10`개까지 회복된다.
- worker 실패 후 미완료 입력이 `FAILED`로 고정되지 않고 다시 queue로 돌아간다.
- slot 하나가 끝난 뒤 다음 입력이 즉시 투입되어 idle slot이 오래 남지 않는다.

## Assumptions
- `max_jobs`는 계정당 유지해야 하는 worker 목표 수로 해석한다.
- `windows_per_job`는 worker 내부 병렬 slot 수로 해석한다.
- 입력이 부족한 경우에만 worker 수와 slot 수가 자연스럽게 줄어드는 것을 허용한다.
