# 11-03: 웹/관측/테스트/롤아웃 세부계획

## 1. 목적

1. 웹 상태판을 job 중심에서 window 중심으로 전환한다.
2. 계정 용량/균등 분배 상태를 실시간으로 관측 가능하게 만든다.
3. 24/7 운영 기준 테스트, 수용 게이트, 롤아웃 절차를 고정한다.

## 2. 웹/API 변경 상세

## 2.1 기존 API 확장

### `/api/metrics/throughput`

기존 필드 유지 + 아래 필드 추가:

1. `total_windows`
2. `queued_windows`
3. `active_windows`
4. `succeeded_windows`
5. `failed_windows`
6. `quarantined_windows`
7. `delete_quarantined_windows`
8. `account_window_scores` (account별 completed/inflight/score)

## 2.2 신규 API

### `/api/windows`

쿼리:

1. `run_id` (optional, default latest)
2. `status` (optional)
3. `limit` (default 500)

응답 항목:

1. `window_id`
2. `run_id`
3. `job_id`
4. `account_id`
5. `input_path`
6. `output_path`
7. `state`
8. `attempt_no`
9. `updated_at`

### `/api/windows/{id}/timeline`

응답:

1. `window_task` 본문
2. `events[]`
3. `file_lifecycle`
4. `attempt_summary`

### `/api/accounts/capacity`

응답 항목:

1. `account_id`
2. `host`
3. `running_count`
4. `pending_count`
5. `allowed_submit`
6. `score`
7. `ts`

## 3. 대시보드 UI 전환

## 3.1 상단 KPI 카드

1. `Queued Windows`
2. `Active Windows`
3. `Succeeded Windows`
4. `Failed Windows`
5. `Quarantined Windows`
6. `Delete Quarantined`
7. 보조: `Active Jobs`, `Queued Jobs`

## 3.2 본문 테이블

1. 기본 테이블: window 목록(`/api/windows`)
2. 보조 테이블: 계정 용량/점수(`/api/accounts/capacity`)
3. 이벤트 테이블: 최근 `window_events` + worker events

## 3.3 사용자 혼동 방지

1. 화면에 단위 명시:
   - `Window(.aedt) 기준`
2. job 관련 수치는 보조 영역으로 분리.

## 4. 운영 관측/쿼리

DuckDB 운영 확인 쿼리:

```sql
-- 계정별 용량 최신값
SELECT account_id, host, running_count, pending_count, allowed_submit, ts
FROM account_capacity_snapshots
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ts DESC) = 1;

-- window 상태 집계
SELECT state, COUNT(*) FROM window_tasks
WHERE run_id = ?
GROUP BY state ORDER BY state;

-- 균등 분배 편차
SELECT account_id, COUNT(*) AS completed_windows
FROM window_tasks
WHERE run_id = ? AND state IN ('SUCCEEDED','FAILED','QUARANTINED')
GROUP BY account_id
ORDER BY account_id;
```

## 5. 테스트 전략

## 5.1 단위 테스트

1. `StateStore`:
   - window/account_capacity CRUD
2. scheduler:
   - capacity gating
   - fairness selection
3. ingest:
   - ready 사이드카 규칙
   - 중복 등록 방지
4. web:
   - 신규 API 응답 스키마
   - 빈 DB/부분데이터 안전응답

## 5.2 통합 테스트

1. 24/7 루프 중 신규 파일 추가 시 ingest 확인.
2. 계정 제약 시 재균등 제출 확인.
3. bundle 부분 실패 후 window 단위 재시도 확인.
4. 재시작 복구(중복/누락 없음) 확인.

## 5.3 정책 회귀 테스트

1. CLI/console script 미추가 확인.
2. `run_pipeline(config)` 단일 진입점 유지 확인.

실행 명령:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

## 6. 롤아웃 절차

### Phase A: 스키마 배포

1. 신규 테이블 생성.
2. 기존 데이터 호환 확인.

### Phase B: ingest 전환

1. ready 기반 ingest 활성화.
2. 초기 dry soak(입력 소량) 검증.

### Phase C: 스케줄러 전환

1. capacity gating + fairness 적용.
2. `R10/PD3` 준수 실측.

### Phase D: 웹 전환

1. window KPI/UI/API 반영.
2. 운영자 검증.

### Phase E: 장시간 soak

1. 24시간 운영.
2. 재시작 복구 및 편차 지표 확인.

## 7. 수용 기준

1. 24/7 중 신규 `.aedt + .ready`가 지속 처리된다.
2. 계정별 상한(`RUNNING<=10`, `PENDING<=3`) 위반이 없다.
3. window 처리량 분배 편차가 장시간 관측에서 수렴/안정한다.
4. 대시보드가 window 중심으로 즉시 운영 판단 가능하다.
5. 장애/재시작 이후 중복/누락 처리 없이 복구된다.

## 8. 가정 및 기본값

1. 생산자는 `.aedt.ready`를 파일 완료 신호로 반드시 생성한다.
2. 기본 폴링 30초.
3. 기본 run 롤링 24시간.
4. 기본 계정 3개(`gate1-harry`, `gate1-dhj02`, `gate1-jji0930`).
5. 실행 자원 기본값: `windows_per_job=8`, `cpus_per_job=32`, `cores_per_window=4`.
