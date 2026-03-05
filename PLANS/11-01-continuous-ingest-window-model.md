# 11-01: 24/7 Ingest + Window 데이터모델 구현 계획

## 1. 목적

`ROADMAP-11-24x7-window-balanced-scheduler.md`의 1단계 구현 문서다.

1. 실행 모델을 단발성 스캔 기반에서 상시 ingest 기반으로 전환한다.
2. 처리 단위를 `1 .aedt = 1 window_task`로 고정한다.
3. `.aedt.ready` 사이드카가 있는 입력만 큐에 등록한다.
4. run 경계는 24시간 롤링으로 유지한다.

## 2. 구현 범위

### 포함

1. `PipelineConfig` 연속운영 관련 필드 확장 및 검증.
2. ingest 루프 도입(`ingest_poll_seconds=30` 기본값).
3. DuckDB 스키마 추가:
   - `window_tasks`
   - `window_events`
   - `ingest_index`
4. 기존 `file_lifecycle`의 window 식별 확장.
5. run 24시간 롤링 생성/종료 로직.

### 제외

1. account capacity 기반 제출 제어(11-02).
2. 웹 API/KPI 전환(11-03).

## 3. 공개 타입/API 변경

`run_pipeline(config)` 단일 진입점은 유지한다.

### 3.1 PipelineConfig 신규 필드

1. `continuous_mode: bool = True`
2. `ingest_poll_seconds: int = 30`
3. `ready_sidecar_suffix: str = ".ready"`
4. `run_rotation_hours: int = 24`
5. `pending_buffer_per_account: int = 3`
6. `capacity_scope: Literal["all_user_jobs"] = "all_user_jobs"`
7. `balance_metric: Literal["window_throughput"] = "window_throughput"`

### 3.2 validate() 규칙

1. `ingest_poll_seconds > 0`
2. `run_rotation_hours > 0`
3. `ready_sidecar_suffix`는 비어있지 않아야 함.
4. `continuous_mode=True`일 때 입력이 비어도 에러로 종료하지 않음.
5. `continuous_mode=False`는 기존 일회성 동작을 유지.

## 4. 데이터 모델 상세

## 4.1 신규 테이블 DDL 초안

```sql
CREATE TABLE IF NOT EXISTS window_tasks (
    run_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    job_id TEXT,
    account_id TEXT,
    input_path TEXT NOT NULL,
    output_path TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_no INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, window_id)
);

CREATE TABLE IF NOT EXISTS window_events (
    run_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    level TEXT NOT NULL,
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_index (
    input_path TEXT PRIMARY KEY,
    ready_path TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    file_mtime_ns BIGINT NOT NULL,
    discovered_at TEXT NOT NULL,
    state TEXT NOT NULL
);
```

### 4.2 상태 정의

`window_tasks.state` 고정 집합:

1. `QUEUED`
2. `ASSIGNED`
3. `UPLOADING`
4. `RUNNING`
5. `COLLECTING`
6. `SUCCEEDED`
7. `FAILED`
8. `RETRY_QUEUED`
9. `QUARANTINED`
10. `UPLOAD_FAILED`
11. `DELETE_RETRYING`
12. `DELETE_QUARANTINED`

## 5. ingest 설계

### 5.1 파일 탐지 규칙

1. 입력 스캔은 디렉토리 재귀(`scan_recursive`) 설정을 그대로 존중.
2. `.aedt` 발견 시 `<same_path>.ready` 존재 여부 확인.
3. ready 없으면 skip(로그 없음 또는 debug 레벨 로그).
4. ready 있으면 `stat` 값(size/mtime) 추출 후 `ingest_index`와 비교.

### 5.2 중복 방지 규칙

1. `ingest_index.input_path`가 없으면 신규 등록.
2. `state in ("QUEUED","ASSIGNED","RUNNING","RETRY_QUEUED")`인 window가 있으면 재등록 금지.
3. 입력이 새 파일로 재생성된 경우(size/mtime 변경)만 재등록 허용.

### 5.3 ID 생성 규칙

1. `window_id` 포맷: `w_{sha1(rel_path)}_{mtime_ns}`
2. `output_path` 매핑: 기존 규칙 유지(`.aedt -> .aedt_all`).
3. `run_id`는 UTC 기준 `%Y%m%d_%H%M%S`, 24시간마다 회전.

## 6. run 롤링

1. worker 시작 시 active run 1개를 생성.
2. `run_rotation_hours` 경과 시 현재 run을 `ROLLED`로 종료하고 새 run 시작.
3. 미완료 window는 새 run으로 이관하지 않고 기존 run 소속 유지.
4. 신규 ingest만 새 run에 할당.

## 7. 모듈/파일 단위 구현 항목

1. `peetsfea_runner/pipeline.py`
   - `PipelineConfig` 확장
   - continuous 모드 분기
   - ingest 루프 호출 훅 추가
2. `peetsfea_runner/state_store.py`
   - 신규 테이블/CRUD 메서드 추가
   - window 이벤트 기록 API 추가
3. `peetsfea_runner/systemd_worker.py`
   - 24/7 loop에서 ingest + scheduler step 분리 호출
4. (선택) `peetsfea_runner/ingest.py` 신설
   - 입력 스캔/등록 책임 분리

## 8. 테스트 계획

1. ready 없는 `.aedt`는 등록되지 않는지.
2. ready 있는 `.aedt`는 30초 내 등록되는지.
3. 같은 파일 재스캔 시 중복 등록이 없는지.
4. 파일 변경(size/mtime) 시 재등록되는지.
5. continuous 모드에서 입력 0건이어도 worker가 유지되는지.
6. run 롤링(24h) 경계에서 신규 ingest의 run_id가 바뀌는지.

## 9. 완료 기준

1. `window_tasks/window_events/ingest_index` 스키마가 생성된다.
2. `.aedt + .ready` 조건에서만 window가 QUEUED 된다.
3. 중복 등록 없이 상시 루프가 유지된다.
4. 단위테스트 및 회귀테스트가 통과한다.
