# 07-02: 10잡 슬롯 스케줄러 + 잡 단위 원격 실행기

## 목적

1. 1계정에서 최대 10잡 동시 운영을 제어한다.
2. 잡별 독립 실행/재시도/격리 흐름을 구현한다.
3. 한 잡 실패가 전체 진행을 막지 않는 구조를 고정한다.

## 스케줄러 정책

1. 슬롯 계산식:
   - `effective_slots = min(max_jobs_per_account, license_cap_per_account // windows_per_job)`
2. 기본값 기준:
   - `min(10, 80 // 8) = 10`
3. 슬롯 초과 제출은 금지한다.
4. 큐가 빌 때까지 제출/수거를 반복한다.

## 잡 단위 실행기 정책

1. 세션명 규칙:
   - `aedt_<run_id>_<job_index:02d>_a<attempt>`
2. 원격 경로:
   - `<remote_root>/<run_id>/<job_id>/`
3. 로컬 경로:
   - `<local_artifacts_dir>/<run_id>/<job_id>/`
4. 잡 상태 전이:
   - `PENDING -> SUBMITTED -> RUNNING -> COLLECTING -> SUCCEEDED|FAILED`

## 실패/재시도/격리

1. 실패 시 `job_retry_count=1`까지 재시도한다.
2. 재시도 소진 시 `QUARANTINED`로 전이한다.
3. 격리 시 원인(`failure_reason`)과 마지막 `exit_code`를 저장한다.
4. 다른 잡의 제출/실행은 계속 진행한다.

## 구현 산출물

1. 신규 모듈:
   - `peetsfea_runner/scheduler.py`
   - `peetsfea_runner/remote_job.py`
2. 파이프라인은 공개 진입점 `run_pipeline(config)`만 유지한다.
3. 내부적으로만 잡 단위 함수와 슬롯 관리자를 호출한다.

## 수용 기준

1. 80개 입력에서도 동시 실행 수가 10을 넘지 않는다.
2. 재시도 성공/재시도 실패(격리) 케이스 테스트가 통과한다.
3. 일부 잡 실패 시 나머지 잡이 정상 완료된다.
4. 슬롯 초과 제출 0건을 테스트로 검증한다.
