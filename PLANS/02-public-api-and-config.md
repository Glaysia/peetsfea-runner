# Plan 02: Public API and Config (ROADMAP 07)

## Public API (Only Two)

1. `PipelineConfig` dataclass
2. `run_pipeline(config: PipelineConfig) -> PipelineResult`

추가 공개 엔트리포인트는 허용하지 않는다.

## PipelineConfig

필수:

1. `input_aedt_dir: str`

기본값:

1. `host="gate1-harry"`
2. `partition="cpu2"`
3. `nodes=1`
4. `ntasks=1`
5. `cpus_per_job=32`
6. `mem="320G"`
7. `time_limit="05:00:00"`
8. `remote_root="~/aedt_runs"`
9. `local_artifacts_dir="./artifacts"`
10. `execute_remote=False`
11. `max_jobs_per_account=10`
12. `windows_per_job=8`
13. `cores_per_window=4`
14. `license_cap_per_account=80`
15. `job_retry_count=1`
16. `scan_recursive=False`
17. `metadata_db_path="./peetsfea_runner.duckdb"`

## PipelineResult

필수 필드:

1. `success: bool`
2. `exit_code: int`
3. `run_id: str`
4. `remote_run_dir: str`
5. `local_artifacts_dir: str`
6. `summary: str`
7. `total_jobs: int`
8. `success_jobs: int`
9. `failed_jobs: int`
10. `quarantined_jobs: int`

## Exit Code Mapping

1. `0`: 전체 성공
2. `10`: SSH/접속 실패
3. `11`: Slurm 할당 실패
4. `12`: Screen 생성/검증 실패
5. `13`: 원격 실행 실패
6. `14`: 결과 다운로드 실패
7. `15`: 원격 정리/orphan 정리 실패

## API Constraints

1. 설정 주입은 함수 인자로만 수행한다.
2. 외부 실행은 모듈 import 후 함수 호출 방식만 사용한다.
3. 문서/테스트 예시도 함수 호출만 사용한다.
4. CLI/서브커맨드/추가 엔트리포인트를 추가하지 않는다.
