# ROADMAP 09 통합 최종: 계정 셰어딩 + systemd --user + DuckDB + 웹 상태판

## 목표/범위

목표:

1. 기존 08(다계정 확장)과 09~10(운영 자동화/가시화)을 단일 최종 단계로 통합한다.
2. 입력 큐 폴더 구조를 출력 폴더에 1:1 미러링한다.
3. 입력 `foo.aedt`를 출력 `foo.aedt_all/`로 치환 생성한다.
4. 원격 업로드 성공 직후 입력 `.aedt`를 삭제한다.
5. DuckDB 상태를 웹 API로 조회한다.

범위 외:

1. CLI 엔트리포인트 추가
2. 앱 레벨 라이선스 스로틀링 구현

## 핵심 정책

1. 파이프라인 진입점은 `run_pipeline(config)` 단일 경로를 유지한다.
2. 라이선스 제어는 Ansys/Slurm 내장 동작을 신뢰하며, 앱은 관측/경보만 수행한다.
3. 단일 계정으로 지속 검증하되 계정 레지스트리 설계는 유지한다.
4. 삭제 실패는 재시도 후 `_delete_failed` 격리로 전이한다.

## 공개 API/인터페이스

`PipelineConfig` 운영 필드:

1. `input_queue_dir`
2. `output_root_dir`
3. `delete_input_after_upload=True`
4. `delete_failed_quarantine_dir`
5. `metadata_db_path`
6. `accounts_registry` (기본 1계정)
7. `license_observe_only=True`

`PipelineResult` 유지:

1. `success`, `exit_code`, `run_id`, `summary`
2. `total_jobs`, `success_jobs`, `failed_jobs`, `quarantined_jobs`

## 데이터 모델(DuckDB)

1. `runs(run_id, started_at, finished_at, state, summary)`
2. `jobs(run_id, job_id, input_path, output_path, account_id, status, created_at, updated_at, last_attempt_no, failure_reason)`
3. `attempts(run_id, attempt_id, job_id, attempt_no, node, started_at, ended_at, exit_code, error)`
4. `artifacts(run_id, job_id, artifact_root, size_bytes, checksum, created_at)`
5. `events(run_id, job_id, level, message, stage, ts)`
6. `file_lifecycle(run_id, job_id, input_path, input_deleted_at, delete_retry_count, delete_final_state, quarantine_path, updated_at)`

## 입출력 매핑 규칙

1. 입력 루트 기준 상대경로를 그대로 유지한다.
2. 입력 `relative/path/foo.aedt`는 출력 `relative/path/foo.aedt_all/`로 생성한다.
3. 원격 실행 산출물은 해당 `foo.aedt_all/`에 저장한다.

## 실행 플로우

1. watcher 또는 단발 실행이 `input_queue_dir`에서 `.aedt`를 스캔한다.
2. 글로벌 스케줄러가 계정 레지스트리 기반으로 작업을 배정한다.
3. 업로드 성공 시 즉시 입력 파일 삭제 시도.
4. 삭제 실패 시 재시도 후 `_delete_failed/<relative_path>.aedt` 격리.
5. 실행 완료 후 `*.aedt_all/`에 아티팩트 기록.
6. 상태/이벤트/수명주기를 DuckDB에 반영.
7. 웹 API(`/api/jobs`, `/api/jobs/{id}`, `/api/metrics/throughput`)로 조회.
8. `systemd --user` 워커에서 상시 반복 운영.

## 실패 처리 및 복구

1. 업로드 실패: `UPLOAD_FAILED` 기록, 입력 파일 유지, 재큐잉.
2. 삭제 실패: `DELETE_RETRYING -> DELETE_QUARANTINED`.
3. 실행 실패: 잡 단위 재시도 후 `FAILED/QUARANTINED`.
4. 워커 크래시: `systemd --user` 자동 재시작 + DuckDB 상태 기반 복원.
5. 계정 장애: 계정 비활성화 후 작업 재큐잉.

## 테스트/수용 기준

1. 입력/출력 상대경로 미러링 및 `.aedt_all` 치환 검증.
2. 업로드 성공 직후 입력 삭제 검증.
3. 삭제 실패 재시도 및 격리 전이 검증.
4. 단일 계정 80개 큐 소진/반복 동작 검증.
5. 계정 비활성화 재큐잉 검증.
6. DuckDB 스키마/이벤트/수명주기 무결성 검증.
7. 웹 API 3종 응답 정확성 검증.
8. `systemd --user` 재시작 후 상태 복원 검증.

## 운영 체크리스트

1. `systemctl --user` 워커 상태 정상.
2. DuckDB 파일/백업/용량 정상.
3. 웹 API health 및 핵심 endpoint 정상.
4. `_delete_failed` 누적량 모니터링 정상.

## 롤백 기준

1. 통합 워커 불안정 시 수동 `run_pipeline(config)` 실행 모드로 복귀.
2. DuckDB 이상 시 마지막 정상 백업으로 복구.

## 다음 단계

1. 본 문서를 최종 통합 단계로 간주한다.
2. 추가 확장은 운영 요구 기반의 별도 부속 계획으로 정의한다.
