# SHORT TERM PLAN 02 - Gate Upload and Spool Dispatch

## Goal
- mainPC에서 gate spool `inbox/`로 작업 입력을 안정적으로 업로드한다.

## Scope
- 전송 수단 표준화: `scp` 또는 `rsync`
- 계정별 spool 경로 계약(`inbox/claimed/results/failed`) 고정
- 업로드 성공/실패 상태 전이(`UPLOADED`, `FAILED_UPLOAD`) 확정
- 업로드 중단/재시작 시 멱등성(중복 업로드 억제) 확보

## Deliverables
- 업로드 인터페이스 초안(`upload_to_spool_inbox`)
- 원격 경로/파일명 규약 문서(작업 ID 기반)
- 상태 전이 확장안 및 오류 분류
- 네트워크 장애 주입 테스트 설계

## Done Criteria
- 네트워크 실패 시 상태가 일관되게 실패 전이된다.
- 성공 업로드된 입력만 worker claim 대상이 된다.
- 재시작 후 중복 업로드 없이 이어서 처리된다.
- 연결 끊김/권한 오류/경로 없음 장애 시나리오가 검증된다.
