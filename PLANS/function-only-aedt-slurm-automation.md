# Function-Only AEDT Slurm Automation Plan

## Summary

이 스펙은 기존 계획을 대체한다.

1. 계획서는 이 파일 단일 경로로 관리한다.
2. 파이프라인 실행 경로는 CLI 없이 Python 함수 호출 1개(`run_pipeline`)만 허용한다.

## Scope

1. 함수 호출 기반 파이프라인 구현을 위한 기술 명세를 본 문서에 고정한다.
2. 저장소 규칙은 `AGENTS.md`에서 CLI 전면 금지 + 단일 진입점 강제로 관리한다.

## Public API / Interfaces

공개 API:

1. `PipelineConfig` dataclass
2. `run_pipeline(config: PipelineConfig) -> PipelineResult`

금지:

1. `argparse`, `click`, `typer` 등 모든 CLI 엔트리포인트
2. `python -m ...` 실행 경로
3. 우회 진입점(두 번째 경로)

`PipelineConfig` 기본값:

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
18. `input_aedt_dir`는 필수

`PipelineResult` 필드:

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

## Implementation Spec

함수 내부 단계(고정):

1. 로컬 검증
2. 원격 작업 디렉토리 생성
3. `sample.aedt` + 실행 스크립트 업로드
4. `ssh gate1-harry` 접속 (`pexpect`)
5. 1계정 작업 큐 생성(디렉토리 스캔 `.aedt`)
6. 슬롯 계산(`min(max_jobs_per_account, license_cap_per_account // windows_per_job)`)
7. 잡 단위 원격 실행(최대 10잡 동시)
8. 잡 내부 `screen` 1세션 + 8윈도우 실행 및 집계
9. 잡 실패 시 1회 재시도, 소진 시 격리(`QUARANTINED`)
10. 결과 다운로드/원격 정리
11. run_id 접두 orphan session 정리
12. DuckDB 상태 저장 + `PipelineResult` 반환

`screen` 내부 최초 1회 설정 명령(고정):

```bash
export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/AnsysEM
export SCREENDIR="$HOME/.screen"
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
unset LANGUAGE
export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81
module load ansys-electronics/v252
```

운영 규칙:

1. 위 설정은 `screen` 세션 안에서 실행 시작 시 1회만 수행한다.
2. 동일 세션 내 재실행 시에는 중복 적용하지 않도록 상태 파일로 가드한다.

재시도 정책:

1. `ssh`, `scp 업로드`, `srun`, `scp 다운로드` 단계만 1회 재시도
2. 재시도 간 대기 10초
3. 2회 실패 시 즉시 종료 + 로그 보존

## Test Cases and Scenarios

1. `PipelineConfig` 유효성 검사 실패/성공
2. 정상 E2E 모의 흐름에서 `PipelineResult.success=True`
3. 80개 입력에서 동시 실행 최대 10 검증
4. 잡 실패 후 재시도 성공/실패(격리) 검증
5. 일부 잡 실패 시 나머지 잡 계속 진행 검증
6. 원격 실행 실패와 다운로드 실패 코드 구분 검증
7. DuckDB 상태 레코드(`runs/jobs/job_events/quarantine_jobs`) 검증
8. CLI 관련 파일/코드가 없는지 정적 검사

## Assumptions and Defaults

1. `gate1-harry`는 SSH 키 기반 접속 가능
2. 계산 노드에서 `screen`, `python3`, `pip` 사용 가능
3. 로그인/계산 노드가 원격 작업 디렉토리를 공유
4. Ansys 실행 경로는 기존 `run.py` 기준 유지
5. 외부 네트워크 제한으로 pip 설치 실패 가능성은 로그로 명확히 남긴다
