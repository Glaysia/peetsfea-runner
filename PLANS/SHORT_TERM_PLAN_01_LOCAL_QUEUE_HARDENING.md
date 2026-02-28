# SHORT TERM PLAN 01 - Local Queue Hardening

## Goal
- mainPC 로컬 큐를 `incoming/pending/uploaded/done/failed` 계약으로 안정화한다.

## Scope
- 로컬 디렉터리 계약 고정:
  - `incoming`, `pending`, `uploaded`, `done`, `failed`, `state`
- `incoming -> pending` 원자 이동 정책 고도화
- 입력 중복/재등록 규칙 확정(작업 ID 기반)
- DONE 승격 전제에 `.aedt` 삭제 이벤트 검증 훅 추가

## Deliverables
- 로컬 큐 전이 규칙 문서
- DuckDB 로컬 상태 컬럼 확장안(`aedt_retention`, 삭제 타임스탬프)
- ingest 오류 코드 표준(`E_INGEST_*`, `E_DUPLICATE_*`)
- 재시작/중복/원자 이동 회귀 테스트 목록 업데이트

## Done Criteria
- 재시작 후 ingest 루프가 중복 처리 없이 복구된다.
- 중복 입력이 동일 작업으로 합류되고 이력은 이벤트로 남는다.
- 원자 이동 실패가 `FAILED_LOCAL`로 일관 기록된다.
- DONE 승격 전 `.aedt` 삭제 전제 검증 경로가 준비된다.
