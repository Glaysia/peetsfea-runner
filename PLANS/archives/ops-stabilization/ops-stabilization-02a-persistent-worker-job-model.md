# OPS Stabilization 02A: Persistent Worker Job Model

## 목적

이 문서는 `foreground ssh` 경로를 대체할 `sbatch` 기반 장수 worker job 모델을 정의한다.
핵심은 service와 원격 실행 생명주기를 분리하고, main PC가 Slurm job id 기준으로 worker를 추적할 수 있게 만드는 것이다.

## 목표 구조

- main PC는 control plane이다.
- Slurm에 제출되는 실체는 `worker job`이다.
- worker job 하나는 allocation 하나를 가진다.
- worker job은 `worker_id`와 `slurm_job_id`를 가진다.
- 결과 수집은 제출 세션과 분리한다.

## worker 책임

- 기동 시 자신을 등록한다.
- slot pool 실행 환경을 준비한다.
- 정상 종료 전 drain 절차를 수행한다.
- worker 전체 실패가 아니면 main PC 개입 없이 slot pool을 유지할 기반을 제공한다.

## worker lifecycle

- `CREATED`
- `SUBMITTED`
- `PENDING`
- `RUNNING`
- `IDLE_DRAINING`
- `COMPLETED`
- `FAILED`
- `LOST`

## 데이터 모델

- `worker_id`
- `slurm_job_id`
- `account_id`
- `host_alias`
- `observed_node`
- `submitted_at`
- `started_at`
- `ended_at`
- `slots_configured`
- `worker_state`

## 결과 수집 분리 원칙

- submit 성공과 execution 성공은 다른 단계다.
- execution 성공과 artifact 등록 성공도 다른 단계다.
- long-running SSH 세션을 결과 수집 통로로 사용하지 않는다.

## 구현 체크리스트

1. `sbatch` 제출 backend를 추가한다.
2. `slurm_job_id`를 DB 상태에 저장한다.
3. worker/job 상태를 service restart 이후 재발견할 수 있게 한다.
4. foreground 경로와 공존 가능한 backend 선택 구조를 유지한다.
5. worker 완료와 artifact 수집을 분리한다.

## 테스트 계획

- `sbatch` 제출 후 `slurm_job_id`가 기록되는 테스트
- service 재시작 후 실행 중 worker를 다시 발견하는 테스트
- worker 완료와 artifact 수집을 분리해도 상태가 일관된 테스트
- canary account 하나에서 detached worker job이 끝까지 유지되는 검증

## 수용 기준

- service를 내려도 이미 제출된 worker job은 계속 실행된다.
- service restart 후 `slurm_job_id` 기준 상태 복구가 가능하다.
- main PC가 foreground SSH 세션을 오래 붙들지 않아도 worker 추적이 가능하다.
