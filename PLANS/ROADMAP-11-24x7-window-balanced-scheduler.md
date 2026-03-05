# ROADMAP 11: 24/7 실시간 큐 + `.aedt=window` + 계정별 균등 분배

## 1. 요약

1. 실행 모델을 초기 일괄 스캔에서 24시간 상시 ingest/schedule 모델로 전환한다.
2. 처리 단위를 `1 .aedt = 1 window task`로 고정한다.
3. Slurm 제출 단위는 `1 job = 최대 8 window` bundle로 유지한다.
4. 계정별 상한은 사용자 전체 잡 기준으로 `RUNNING<=10`, `PENDING<=3`을 강제한다.
5. 계정 분배는 window 처리량 균등 기준으로 적용하며, 제약 시 가능한 계정끼리 재균등한다.
6. 입력 파일은 `.aedt`와 `.aedt.ready`가 모두 있어야 처리한다.
7. 웹 기본 KPI는 window 기준으로 전환한다.

## 2. 범위 및 비범위

### 범위

1. `run_pipeline(config)` 단일 진입점 유지 하에서 상시 처리 모델 구현.
2. window 단위 큐/상태/이벤트 데이터 모델 추가.
3. 계정 용량 제어 + 균등 분배 스케줄러 적용.
4. 입력 ready 사이드카 규칙 적용.
5. 웹 API/대시보드 KPI를 window 중심으로 개편.

### 비범위

1. CLI/서브커맨드/console script 신규 추가.
2. 라이선스 앱 레벨 스로틀링 복귀.
3. 입력 생산 시스템(외부) 변경.

## 3. 공개 API/타입 변경 명세

### PipelineConfig 확장

기존 필드에 더해 다음 필드를 추가한다.

1. `continuous_mode: bool = True`
2. `ingest_poll_seconds: int = 30`
3. `ready_sidecar_suffix: str = ".ready"`
4. `run_rotation_hours: int = 24`
5. `pending_buffer_per_account: int = 3`
6. `capacity_scope: Literal["all_user_jobs"] = "all_user_jobs"`
7. `balance_metric: Literal["window_throughput"] = "window_throughput"`

### 내부 모델 변경

1. `JobSpec`
   - 기존: 단건 `input_path`
   - 변경: 다건 `window_inputs` (최대 8)
2. `PipelineResult.summary`
   - 필수 포함: `total_windows`, `active_windows`, `success_windows`, `failed_windows`, `quarantined_windows`
   - 보조 포함: `active_jobs`

## 4. 스케줄링 규칙 (결정 완료)

### 4.1 용량 조회

제출 시점마다 계정별로 원격 조회:

```bash
ssh <host> 'squeue -u $USER -h -o "%T"'
```

### 4.2 eligibility

계정별 신규 제출 가능 조건:

1. `RUNNING < 10`
2. `PENDING < 3`

위 조건을 동시에 만족하는 계정만 제출 후보로 사용한다.

### 4.3 균등 분배 기준

1. 지표: `window_throughput_score = completed_windows + inflight_windows` (현재 run 기준)
2. 후보 계정 중 score가 가장 낮은 계정을 우선 선택한다.
3. tie-break:
   1. `running_jobs`가 적은 계정
   2. `account_id` 오름차순
4. 선택 계정에 전역 window queue에서 최대 8개를 묶어 1 bundle job 제출.
5. 특정 계정이 제약/장애로 막히면 제외하고, 나머지 후보에서 동일 규칙을 계속 적용한다.

## 5. 24/7 실행 플로우

1. worker 루프는 서비스 생존 동안 계속 실행.
2. `ingest_poll_seconds=30` 주기로 입력 폴더를 스캔.
3. `.aedt`와 대응 `.aedt.ready`가 모두 존재할 때만 ingest.
4. 신규 window task를 DB에 등록하고 전역 queue에 적재.
5. scheduler loop는 account capacity와 균등 규칙으로 bundle 제출.
6. 원격 bundle 실행 시 `case_01..case_08`에 서로 다른 `.aedt`를 매핑.
7. 업로드 성공한 window는 입력 `.aedt`와 `.ready` 삭제 시도.
8. 실패 window만 재시도 queue로 이동, 소진 시 quarantine.
9. run은 `run_rotation_hours=24` 기준으로 회전하며 처리 지속.

## 6. DuckDB 스키마 변경

### 유지

1. `jobs` (bundle 단위)
2. `attempts`
3. `events`/`job_events`
4. `artifacts`

### 신규

1. `window_tasks`
   - `window_id`, `run_id`, `job_id`, `account_id`
   - `input_path`, `output_path`
   - `state`, `attempt_no`, `failure_reason`
   - `created_at`, `updated_at`
2. `window_events`
   - `window_id`, `run_id`, `level`, `stage`, `message`, `ts`
3. `ingest_index`
   - `input_path`, `ready_path`
   - `file_size`, `file_mtime_ns`
   - `discovered_at`, `state`
4. `account_capacity_snapshots`
   - `account_id`, `host`, `running_count`, `pending_count`, `allowed_submit`, `ts`

### 확장

1. `file_lifecycle`를 window 식별 기준으로 확장한다.

## 7. 실패 처리/복구

1. 업로드 실패
   - 해당 window만 `UPLOAD_FAILED` 후 재큐잉
2. 삭제 실패
   - 재시도 후 `_delete_failed` 격리
   - 상태 `DELETE_QUARANTINED`
3. bundle 부분 실패
   - 성공 window는 확정
   - 실패 window만 재시도
4. worker 재시작
   - DB 상태(`window_tasks`, `ingest_index`)로 복원
5. 계정 장애
   - 해당 계정 제외 후 나머지 계정 재균등 분배

## 8. 웹/API 변경

### KPI 우선순위

1. 기본 KPI를 window 기준으로 고정:
   - `queued_windows`
   - `active_windows`
   - `succeeded_windows`
   - `failed_windows`
   - `quarantined_windows`
2. 보조 KPI:
   - `active_jobs`
   - `queued_jobs`

### API

1. 유지/확장: `/api/metrics/throughput`
   - window 지표 + 계정별 completed/inflight 지표 포함
2. 신규: `/api/windows`
3. 신규: `/api/windows/{id}/timeline`
4. 신규: `/api/accounts/capacity`

## 9. 테스트 시나리오

1. 실행 중 신규 `.aedt + .ready` 추가 시 30초 내 queue 편입.
2. `.ready` 없는 `.aedt`는 편입 금지.
3. 계정별 상한(`RUNNING<=10`, `PENDING<=3`) 위반 0건.
4. window 처리량 기준 균등 분배 유지 검증.
5. 한 계정 제약 시 나머지 계정 재균등 동작 검증.
6. bundle이 서로 다른 8개 `.aedt`를 매핑하는지 검증.
7. 실패 window만 재시도되는지 검증.
8. 삭제 실패 격리 동작 검증.
9. 웹 KPI와 DuckDB 집계 일치 검증.
10. 정책 회귀
    - `run_pipeline(config)` 단일 진입 유지
    - CLI 미추가 유지

## 10. 수용 기준

1. 24/7 서비스 상태에서 신규 `.aedt`가 지속적으로 자동 처리된다.
2. Slurm 사용자 전체 잡 기준 계정별 상한(`R10/PD3`)을 넘지 않는다.
3. window 처리량 분배 편차가 지속적으로 축소/유지된다.
4. 웹에서 window 기준 상태 판단이 즉시 가능하다.
5. 재시작/장애 후 중복 처리 없이 복원된다.
6. 테스트 통과:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

## 11. 롤아웃 단계

1. Phase A: DB 스키마(window/ingest/capacity) 추가 및 마이그레이션.
2. Phase B: ingest 루프 + ready 규칙 + queue 등록 구현.
3. Phase C: bundle scheduler를 window 단위 + 용량 제어 + 균등 분배로 전환.
4. Phase D: 원격 실행 결과의 window 단위 상태 반영 및 재시도/격리 적용.
5. Phase E: 웹/API window KPI 전환.
6. Phase F: 24시간 soak test 및 수용 기준 검증.

## 12. 가정 및 기본값

1. 입력 생산자는 `.aedt` 완료 후 `.aedt.ready`를 생성한다.
2. 기본 폴링은 30초.
3. run 회전은 24시간.
4. 계정은 `gate1-harry`, `gate1-dhj02`, `gate1-jji0930` 3개 사용.
5. `windows_per_job=8`, `cpus_per_job=32`, `cores_per_window=4` 유지.
6. 입력/출력 미러링 및 `.aedt -> .aedt_all` 규칙 유지.
