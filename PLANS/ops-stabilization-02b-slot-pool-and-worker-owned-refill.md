# OPS Stabilization 02B: Slot Pool and Worker-Owned Refill

## 목적

이 문서는 worker job 내부에서 여러 `pyaedt` 실행을 처리하는 slot pool 모델과 worker-owned refill 규칙을 정의한다.
핵심은 `slot 4개`를 예시 기본값으로 두고, slot이 끝날 때마다 main PC가 다시 주입하지 않아도 worker가 스스로 pool을 유지하게 만드는 것이다.

## 핵심 원칙

- slot은 최소 실행 상태 단위다.
- slot pool 기본 예시는 `4`다.
- slot 하나가 끝나면 worker가 새 lease를 받아 즉시 refill한다.
- steady-state에서 main PC는 실행 중 worker 내부에 다시 붙지 않는다.
- backlog가 비면 worker는 refill을 멈추고 `IDLE_DRAINING`으로 간다.

## slot lifecycle

- `QUEUED`
- `LEASED`
- `DOWNLOADING`
- `RUNNING`
- `UPLOADING`
- `SUCCEEDED`
- `FAILED`
- `LEASE_EXPIRED`
- `QUARANTINED`

## refill 규칙

- worker는 활성 slot 수가 `slot_pool_size`보다 작아지면 즉시 다음 lease를 요청한다.
- refill은 `backlog available`과 `worker not draining` 조건에서만 수행한다.
- slot 실패는 기본적으로 다음 lease를 받아 pool을 메우는 방향으로 처리한다.
- 반복 실패 slot은 `QUARANTINED`로 보내고 바로 재시도하지 않는다.

## drain 규칙

- global backlog가 비면 신규 lease를 중단한다.
- 현재 실행 중 slot이 끝나면 worker는 정상 종료를 준비한다.
- 장시간 idle이면 worker를 남겨둘지 종료할지 정책을 둔다.

## 상태와 인터페이스

- `slot_id`
- `lease_token`
- `slot_pool_size`
- `refill_generation`
- `slot_state`
- `last_heartbeat_ts`

## 비채택 모델

- main PC가 실행 중 worker job 안에 다시 붙어 slot을 주입하는 steady-state 모델은 채택하지 않는다.
- slot 종료마다 per-call SSH로 다시 들어가는 방식도 채택하지 않는다.

## 구현 체크리스트

1. worker 내부 slot pool 관리자 개념을 도입한다.
2. slot 종료 후 refill 트리거를 worker 내부 루프로 둔다.
3. `IDLE_DRAINING`과 `LEASE_EXPIRED` 전이를 정의한다.
4. slot 실패와 worker 실패를 분리 추적한다.
5. slot quarantine 기준을 정한다.

## 테스트 계획

- worker 시작 시 slot 4개가 모두 채워지는 테스트
- slot 1개 종료 후 같은 worker 안에서 refill되어 다시 4개가 되는 테스트
- backlog 고갈 시 refill이 멈추고 drain으로 넘어가는 테스트
- heartbeat 상실 시 `LEASE_EXPIRED`가 되는 테스트
- slot 실패가 worker 전체 실패로 번지지 않는 테스트

## 수용 기준

- worker job 1개가 slot pool을 유지한다.
- slot 종료 후 worker가 main PC 개입 없이 같은 job 안에서 refill한다.
- drain과 retry 경계가 분명하다.
