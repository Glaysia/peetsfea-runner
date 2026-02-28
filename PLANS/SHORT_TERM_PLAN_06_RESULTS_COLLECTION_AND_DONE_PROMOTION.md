# SHORT TERM PLAN 06 - Results Collection and DONE Promotion Gate

## Goal
- gate `results/`의 report-only zip을 mainPC로 회수하고, `DONE` 승격을 정책 게이트로 강제한다.

## Gap Summary (from LONG_TERM_PLAN review)
- `Collect` 책임(원격 `results/` 회수 + 중복 방지)은 장기 계획에 정의되어 있으나 아직 단기 계획/구현 분해가 부족하다.
- `DONE` 승격 전제(원격/로컬 `.aedt` 삭제 + zip 회수)도 명시적 승격 게이트로 코드 계약화되어 있지 않다.
- DuckDB 핵심 필드(`report_zip_remote_path`, `remote_aedt_deleted_ts`, `report_export_spec_id`)가 아직 미완성 상태다.

## Scope
- Result Collector 루프 추가:
  - 계정별 원격 `results/` 스캔
  - `task_id` 단위 zip 발견 시 로컬 `done/`으로 원자 회수
  - 재시작 시 멱등성 유지(이미 회수된 zip 재다운로드 방지)
- DONE 승격 게이트 추가:
  - `zip 회수 완료 + AEDT_DELETE_REMOTE_DONE + AEDT_DELETE_LOCAL_DONE` 충족 시에만 `DONE`
  - 조건 미충족 작업은 `UPLOADED/CLAIMED/RUNNING` 또는 보류 상태 유지
- DuckDB/이벤트 확장:
  - `report_zip_remote_path`, `remote_aedt_deleted_ts`, `report_export_spec_id` 저장
  - 이벤트: `COLLECT_STARTED`, `COLLECT_DONE`, `COLLECT_DUPLICATE_SKIPPED`, `COLLECT_FAILED`
- 원격 정리 정책:
  - 회수 성공 후 원격 `results/` zip 정리(또는 보존 TTL) 정책을 코드 상수로 고정

## Out of Scope
- `.aedt` 결정론적 재생성 입력 포맷 정의(별도 계획에서 다룸)
- 실패 zip 내 로그/메타의 표준 스키마 확정(별도 계획에서 다룸)

## Deliverables
- Collector 인터페이스와 기본 어댑터(`list_results`, `download_result`, `delete_remote_result`)
- 로컬 `done/` 원자 회수 유틸(임시 파일 -> 최종 rename)
- DONE 승격 정책 모듈(`can_promote_done` 확장 또는 전용 Policy)
- DuckDB 스키마 마이그레이션 및 이벤트 키 문서
- 재시작/중복/부분 회수 장애 회귀 테스트
- 운영 runbook 보강(수동 회수/재조정 절차)

## Done Criteria
- 원격 `results/` zip이 mainPC `done/`으로 중복 없이 회수된다.
- 비정상 종료 후 재시작해도 이미 회수된 결과를 안전하게 건너뛴다.
- `DONE` 상태는 승격 게이트 조건 충족 시에만 기록된다.
- `DONE` 작업마다 `report_zip_remote_path`와 로컬 zip 경로가 추적된다.
- 원격 `.aedt` 삭제 이벤트 누락 작업은 `DONE`으로 승격되지 않는다.
