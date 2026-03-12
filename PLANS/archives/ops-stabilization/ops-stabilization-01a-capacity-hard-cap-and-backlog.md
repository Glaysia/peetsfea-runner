# OPS Stabilization 01A: Capacity Hard Cap and Backlog

## 목적

이 문서는 현재 `foreground ssh` 경로를 유지한 상태에서 capacity correctness를 먼저 바로잡는 실행 계획서다.
핵심은 계정별 visible worker 수가 hard cap을 넘지 않게 만들고, backlog가 빈 계정을 우선 채우도록 재배치하는 것이다.

## 해결할 문제

- `max_jobs=10`이 hard cap으로 작동하지 않는다.
- `pending_buffer`가 cap을 우회하는 추가 제출 허용치처럼 동작할 수 있다.
- 한 계정에 backlog가 과배정되면 다른 계정이 비어도 못 가져간다.
- `local inflight`와 실제 Slurm 상태 차이가 설명 없이 누적될 수 있다.

## 목표 동작

- 계정별 `remote running + remote pending + submitted_unobserved <= max_jobs`를 보장한다.
- 빈 account가 있으면 backlog는 fairness보다 capacity fill을 우선한다.
- oversubmission risk와 underfill을 동일 화면과 동일 로그 축에서 설명할 수 있어야 한다.

## 설계 규칙

### Hard Cap 규칙

- `pending_buffer`는 hard cap 확장 수단이 아니다.
- `pending_buffer`는 `submitted_unobserved` 또는 관측 지연 완충치로만 쓴다.
- submit 허용치는 `max_jobs - visible_occupied`를 기준으로 계산한다.
- `visible_occupied`는 최소 `remote running + remote pending + submitted_unobserved`로 정의한다.

### Backlog 규칙

- backlog는 계정별 큐에 일찍 고정하지 않는다.
- 대기 bundle은 글로벌 후보군으로 유지하고, 매 dispatch 시점에 빈 계정이 가져간다.
- tie-breaker가 필요하면 그때만 fairness 또는 throughput score를 적용한다.

### 경보 규칙

- `visible_occupied > max_jobs`는 즉시 `OVERSUBMISSION_RISK`다.
- `available_capacity > 0`인데 dispatch가 반복적으로 일어나지 않으면 `UNDERFILL`이다.
- 로컬 계산과 원격 queue 관측이 오래 어긋나면 `CAPACITY_MISMATCH`다.

## 상태 모델

- `submitted_unobserved`
- `visible_occupied`
- `effective_capacity_gap`
- `underfilled_account`
- `oversubmitted_account`
- `capacity_mismatch`

## 구현 체크리스트

1. account capacity 계산식에서 hard cap 우회 경로를 제거한다.
2. `pending_buffer` 의미를 관측 지연 보정으로 재정의한다.
3. backlog 배정 시점을 늦추고 계정 간 재분배 가능 구조로 바꾼다.
4. account summary에 `target`, `running`, `pending`, `submitted_unobserved`, `visible_occupied`, `gap`를 함께 노출한다.
5. event stream과 GUI 요약에 oversubmission / underfill 경보를 추가한다.

## 테스트 계획

- `max_jobs=10`에서 visible worker 수가 10을 넘지 않는 단위 테스트
- 특정 account가 가득 찬 상태에서 다른 빈 account가 backlog를 가져가는 테스트
- 로컬 inflight 증가 후 queue 반영이 지연되는 상황에서 `submitted_unobserved`가 cap 계산에 반영되는 테스트
- `dw16` 과제출과 `dhj02` underfill에 해당하는 회귀 테스트

## Canary

- `examples/sample.aedt`만 사용한다.
- 처음에는 1개 account로 hard cap 동작을 검증한다.
- 그 다음 2개 이상 account로 backlog 재분배를 검증한다.

## 수용 기준

- `10 R + 10 PD`처럼 같은 account에 과제출이 재현되지 않는다.
- 빈 account가 있는데도 backlog가 다른 account에 묶이는 현상이 장시간 지속되지 않는다.
- 첫 화면 또는 account summary에서 왜 underfill/oversubmission이 발생했는지 바로 설명 가능하다.
