# SHORT TERM PLAN 03 - Slurm Worker Pool and Capacity Control

## Goal
- 계정별 상시 worker 풀(10개)을 유지하면서 Slurm 자원 정책을 강제한다.

## Scope
- Slurm worker 제출/조회/복구 래퍼 도입
- 계정당 활성 worker 잡 상한 `10` 강제
- worker 잡 자원 고정:
  - partition `cpu2`
  - `32코어`, `320GB`
  - 잡 내부 PyAEDT `8`
- 계정 장애 시 degraded 마킹 및 우회 정책 정의

## Deliverables
- Slurm 어댑터(`submit_worker`, `query_workers`, `cancel_worker`)
- 풀 유지 제어 로직(목표 개수 vs 실제 개수)
- 장애/복구 시퀀스 문서
- 운영 로그 키 확장(`account`, `slurm_job_id`, `pool_target`, `pool_actual`)

## Done Criteria
- 계정별 활성 worker가 10개를 넘지 않고 목표치를 유지한다.
- worker 잡은 고정 자원/내부 병렬도(8)로 실행된다.
- worker 비정상 종료가 자동 감지되고 재기동된다.
- 계정 degraded 시 다른 계정으로 작업이 우회된다.
