# 07-01: API 재정의 + 큐 스캔 + DuckDB 상태 골격

## 목적

1. ROADMAP 07의 브레이킹 API를 확정한다.
2. 입력을 `input_aedt_dir` 기반 디렉토리 스캔으로 전환한다.
3. 잡 상태 저장소를 프로젝트 루트 단일 DuckDB로 고정한다.

## 공개 API 변경 (브레이킹)

1. `PipelineConfig` 필수: `input_aedt_dir: str`
2. `PipelineConfig` 기본값:
   - `host="gate1-harry"`
   - `partition="cpu2"`
   - `nodes=1`
   - `ntasks=1`
   - `cpus_per_job=32`
   - `mem="320G"`
   - `time_limit="05:00:00"`
   - `remote_root="~/aedt_runs"`
   - `local_artifacts_dir="./artifacts"`
   - `execute_remote=False`
   - `max_jobs_per_account=10`
   - `windows_per_job=8`
   - `cores_per_window=4`
   - `license_cap_per_account=80`
   - `job_retry_count=1`
   - `scan_recursive=False`
   - `metadata_db_path="./peetsfea_runner.duckdb"`
3. 제거 필드:
   - `input_aedt_path`
   - `parallel_windows`
   - `retry_count`
   - `cpus`

## 큐 스캔 정책

1. 입력 경로는 디렉토리여야 한다.
2. 기본 정책은 비재귀 스캔(`scan_recursive=False`)이다.
3. `.aedt` 확장자만 큐에 포함한다.
4. 파일명 오름차순 정렬 후 잡 큐를 생성한다.
5. 기존 성공 이력과 무관하게 항상 재실행한다.

## DuckDB 저장소 설계

1. 저장 위치는 프로젝트 루트 단일 파일(`metadata_db_path`)이다.
2. 스키마 테이블:
   - `runs`
   - `jobs`
   - `job_events`
   - `quarantine_jobs`
3. `jobs` 필수 컬럼:
   - `run_id`
   - `job_id`
   - `attempt`
   - `aedt_path`
   - `session_name`
   - `state`
   - `started_at`
   - `finished_at`
   - `exit_code`
   - `failure_reason`

## 수용 기준

1. `PipelineConfig.validate()`가 신규 필드/제약을 검증한다.
2. 디렉토리 비재귀 스캔 규칙이 테스트로 고정된다.
3. `.aedt` 파일이 0개이면 명시적 실패를 반환한다.
4. DuckDB 초기화/스키마 생성 테스트가 통과한다.
5. 문서/테스트 예시는 `run_pipeline(config)` 함수 호출만 사용한다.
