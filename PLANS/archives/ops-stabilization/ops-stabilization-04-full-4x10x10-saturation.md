# OPS Stabilization 04: 10 Jobs per Account, 5 Slots per Job

## 배경

현재 `5/account`에 머무는 직접 원인은 입력 부족이 아니라, capacity 계산에서 `remote visible jobs`와 `local inflight workers`를 이중 차감하는 구조다.
그 결과 `allowed_submit`이 남아 있어도 `effective_allowed=0`처럼 보이면서 `10/account`까지 worker가 확장되지 못한다.

## 목표

입력 backlog가 충분할 때 각 계정이 `최대 10 jobs`까지 자연스럽게 확장되도록 만든다.
각 worker는 `5 slots`를 가지며, 입력이 부족할 때는 partial fill을 허용하되 `allowed_submit`이 남는데 scheduler가 조기 정지하는 현상은 없애는 것을 목표로 한다.

## 핵심 문제

현재 문제는 `effective_allowed = allowed_submit - inflight_workers` 식으로 계산하면서, 이미 remote에서 `RUNNING/PENDING`으로 보이는 worker까지 다시 shadow 차감한다는 점이다.
이 때문에 실제로는 계정별 `allowed_submit=5`가 남아 있는데도 dispatch는 `5/account`에서 멈춘다.

## 구현 방향

- `submitted_not_visible_workers`와 `remote_visible_workers`를 분리한다.
- `remote visible` worker는 더 이상 shadow 차감하지 않는다.
- `effective_allowed_submit`은 `submitted_not_visible_workers` 기준으로만 계산한다.
- `10/account` hard cap은 그대로 유지한다.
- `remote visible`로 전이된 worker는 shadow에서 즉시 해제한다.
- 상태/API에는 다음 관측값을 추가한다.
  - `submitted_not_visible_workers`
  - `remote_visible_workers`
  - `effective_allowed_submit`
  - `capacity_shadow_delta`

## 기대 동작

- backlog가 충분하면 계정별 `10 workers`까지 확장된다.
- backlog가 부족하면 partial fill은 허용된다.
- `allowed_submit`이 남는데 `effective_allowed=0`으로 막히는 현상은 제거된다.
- 총 슬롯 수는 계정 수와 설정값에서 파생되는 값으로만 다루고, 별도 목표값으로 고정하지 않는다.

## 테스트 계획

- scheduler unit:
  - `remote running=5`, `allowed_submit=5`, `local inflight workers=5`인데 그 worker가 이미 remote visible이면 추가 제출 가능해야 한다.
  - `submitted_not_visible_workers=5`, `remote visible=0`이면 shadow 차감이 유지돼야 한다.
- controller integration:
  - `4 accounts`, `max_jobs=10`, `slots_per_job=5`, backlog가 충분할 때 계정별 `10 workers`까지 확장되는지 확인한다.
  - 실제 패턴인 `443 inputs`, `5/account`, `allowed_submit=5` 회귀를 재현해 더 확장되는지 검증한다.
- live acceptance:
  - account별 `running_count=10`까지 상승
  - `job당 5 slots` 유지
  - `capacity mismatch` 없이 확장 지속

## 운영 적용 순서

1. 코드 수정
2. `.venv/bin/python -m pytest`
3. service stop
4. `peetsfea_runner.duckdb` 삭제
5. service start
6. live saturation 확인

## 성공 기준

- account별 `running_count=10`까지 올라간다.
- `job당 5 slots`가 유지된다.
- `allowed_submit`이 남는데도 scheduler가 더 이상 확장하지 못하는 현상이 사라진다.
