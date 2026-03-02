# SHORT TERM PLAN 06 - Results Collection and DONE Promotion Gate

## Goal
- gate `results/`의 report-only zip을 mainPC로 회수하고, `DONE` 승격을 정책 게이트로 강제한다.
- 실행 단위를 3개로 분리해 순차 완수한다.

## Gap Summary (from LONG_TERM_PLAN review)
- `Collect` 책임(원격 `results/` 회수 + 중복 방지)은 장기 계획에 정의되어 있으나 아직 단기 계획/구현 분해가 부족하다.
- `DONE` 승격 전제(원격/로컬 `.aedt` 삭제 + zip 회수)도 명시적 승격 게이트로 코드 계약화되어 있지 않다.
- DuckDB 핵심 필드(`report_zip_remote_path`, `remote_aedt_deleted_ts`, `report_export_spec_id`)가 아직 미완성 상태다.

## Split Plan
1. Plan 06-1: Results Collector 기본 루프
- 계정별 원격 `results/` 스캔
- `task_id` zip 원자 회수(`.part -> rename`)
- 회수 멱등성(이미 로컬 `done/` 존재 시 skip)
- DuckDB 경로 필드 `report_zip_remote_path` 저장
- 이벤트 `COLLECT_STARTED|DONE|DUPLICATE_SKIPPED|FAILED` 표준화

2. Plan 06-2: DONE 승격 게이트
- `zip 회수 + AEDT_DELETE_REMOTE_DONE + AEDT_DELETE_LOCAL_DONE` 충족 시만 `DONE`
- 조건 미충족 시 `DONE` 승격 금지/보류
- `remote_aedt_deleted_ts`, `report_export_spec_id` 추적 확장

3. Plan 06-3: 원격 정리/운영 보강
- 회수 성공 후 원격 `results/` 정리(또는 TTL 보존) 정책 고정
- Collector 장애 복구/부분 다운로드 재시도/backoff
- runbook의 수동 회수 절차 및 장애 triage 보강

## Out of Scope
- `.aedt` 결정론적 재생성 입력 포맷 정의(별도 계획에서 다룸)
- 실패 zip 내 로그/메타의 표준 스키마 확정(별도 계획에서 다룸)

## Plan 06-1 Completed (2026-03-01)
- `ResultsCollector` 모듈 추가 및 service 루프 통합
- 로컬 원자 회수(`.part`)와 중복 skip 동작 구현
- DuckDB `report_zip_remote_path` 컬럼/접근자 추가
- 테스트 추가:
  - `tests/test_collector.py`
  - `tests/test_service_bootstrap.py` collector 통합 시나리오
  - `tests/test_store.py` report zip remote path 검증

## Done Criteria
- Plan 06-1:
  - 원격 `results/` zip이 mainPC `done/`으로 중복 없이 회수된다.
  - 비정상 종료 후 재시작해도 이미 회수된 결과를 안전하게 건너뛴다.
  - `report_zip_remote_path`와 로컬 zip 경로가 DuckDB에 추적된다.
- Plan 06-2:
  - `DONE` 상태는 승격 게이트 조건 충족 시에만 기록된다.
  - 원격 `.aedt` 삭제 이벤트 누락 작업은 `DONE`으로 승격되지 않는다.
- Plan 06-3:
  - 회수 성공 결과의 원격 정리 정책이 자동 집행되고, runbook으로 수동 복구가 가능하다.
