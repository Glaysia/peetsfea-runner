# SHORT TERM PLAN 05 - Operations, Reconciliation, and Observability

## Goal
- 재시작 복구, 데이터 정합성, `.aedt` 삭제 정책 준수 여부를 운영에서 자동 검증한다.

## Scope
- reconciliation 규칙 확정:
  - claimed TTL 초과 작업 requeue/정리
  - `results/` zip과 DuckDB 상태 불일치 자동 보정
- 삭제 정책 감사:
  - 원격/로컬 `.aedt` 잔존 탐지 및 정리
- 구조화 로그/이벤트 키 표준화
- systemd user runbook 정리

## Deliverables
- reconcile/audit 규칙 문서
- 이벤트 표준:
  - `EXPORT_REPORTS_*`, `PACKAGE_ZIP_*`, `AEDT_DELETE_*`
- 운영 체크리스트(일일/장애시)
- 수동 복구 runbook

## Implemented Rules (2026-03-01)
- Reconciler 모듈:
  - `PENDING` TTL 초과 작업을 `incoming/`으로 requeue한다.
  - `PENDING/UPLOADED/FAILED_UPLOAD`와 로컬 파일(`pending/`, `uploaded/`) 불일치를 자동 보정한다.
  - `DONE` 작업의 `report_zip_local_path`가 유실되면 `var/done/<task_id>.reports.zip`에서 복구 시도 후, 실패 시 `FAILED`로 격하한다.
  - `DONE + aedt_retention=delete_after_done`에서 `.aedt` 잔존을 탐지하고 삭제한다.
- 서비스 루프 통합:
  - `RunnerService` 루프에 reconcile step이 상시 포함된다.
- 이벤트 키 표준화:
  - `src/peetsfea_runner/event_types.py`에서 Plan 05 핵심 이벤트 키를 상수로 관리한다.

## Runbook
- 운영/장애/수동복구 절차:
  - [PLANS/OPERATIONS_RUNBOOK.md](PLANS/OPERATIONS_RUNBOOK.md)

## Done Criteria
- 비정상 종료 후 재시작 시 상태 불일치가 자동 정리된다.
- DB와 파일 스토어 간 불일치가 주기적으로 보정된다.
- DONE 상태 작업에 `.aedt` 잔존이 있으면 정책 위반으로 탐지된다.
- runbook만으로 수동 복구가 가능함이 검증된다.
