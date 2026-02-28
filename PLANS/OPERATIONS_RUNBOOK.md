# OPERATIONS RUNBOOK - Reconciliation and Recovery

## Purpose
- Plan 05 운영 목표인 "재시작 복구/정합성 보정/삭제 정책 감사"를 수동 절차만으로도 복구 가능하게 만든다.

## Daily Checklist
1. 서비스 상태 확인:
   - `systemctl --user status peetsfea-runner`
2. 최근 오류 이벤트 확인:
   - `duckdb var/state/runner.duckdb "SELECT task_id,event_type,error_code,created_at FROM job_events WHERE error_code IS NOT NULL ORDER BY created_at DESC LIMIT 50;"`
3. DONE + `.aedt` 잔존 위반 확인:
   - `find var/incoming var/pending var/uploaded -maxdepth 1 -name '*.aedt' -type f | sort`
4. 결과 zip 경로 무결성 확인:
   - `duckdb var/state/runner.duckdb "SELECT task_id,report_zip_local_path FROM jobs WHERE state='DONE' ORDER BY updated_at DESC LIMIT 50;"`

## Incident Checklist
1. 서비스 중단 후 재시작:
   - `systemctl --user restart peetsfea-runner`
2. 1회 루프 이후 보정 이벤트 확인:
   - `duckdb var/state/runner.duckdb "SELECT task_id,event_type,error_code FROM job_events WHERE event_type LIKE 'RECONCILE_%' ORDER BY created_at DESC LIMIT 100;"`
3. 삭제 정책 위반 탐지 확인:
   - `duckdb var/state/runner.duckdb "SELECT task_id,event_type,message FROM job_events WHERE event_type IN ('AEDT_RETENTION_VIOLATION_DETECTED','AEDT_DELETE_LOCAL_FAILED','AEDT_DELETE_LOCAL_DONE') ORDER BY created_at DESC LIMIT 100;"`

## Manual Recovery Procedures
### 1) PENDING stuck (TTL 초과) 수동 복구
1. 파일 이동:
   - `mv var/pending/<name>.aedt var/incoming/<name>.aedt`
2. 상태 되돌림:
   - `duckdb var/state/runner.duckdb "UPDATE jobs SET state='NEW', error_code=NULL, error_message=NULL WHERE task_id='<task_id>';"`
3. 감사 이벤트 추가:
   - `duckdb var/state/runner.duckdb "INSERT INTO job_events(event_id,task_id,event_type,message) VALUES (uuid(),'<task_id>','RECONCILE_PENDING_TTL_REQUEUE','manual recovery');"`

### 2) DONE인데 zip 경로 유실된 경우
1. 기대 위치 확인:
   - `ls -l var/done/<task_id>.reports.zip`
2. 존재하면 경로 복구:
   - `duckdb var/state/runner.duckdb "UPDATE jobs SET report_zip_local_path='var/done/<task_id>.reports.zip' WHERE task_id='<task_id>';"`
3. 존재하지 않으면 FAILED 격하:
   - `duckdb var/state/runner.duckdb "UPDATE jobs SET state='FAILED', error_code='E_RECONCILE_DONE_ZIP_MISSING', error_message='manual downgrade: zip missing' WHERE task_id='<task_id>';"`

### 3) DONE 작업의 `.aedt` 잔존 처리
1. 잔존 파일 삭제:
   - `rm -f var/incoming/<task_id>.aedt var/pending/<task_id>.aedt var/uploaded/<task_id>.aedt`
2. 삭제 시각 마킹:
   - `duckdb var/state/runner.duckdb "UPDATE jobs SET local_aedt_deleted_ts=CURRENT_TIMESTAMP WHERE task_id='<task_id>';"`
3. 이벤트 기록:
   - `duckdb var/state/runner.duckdb "INSERT INTO job_events(event_id,task_id,event_type,message) VALUES (uuid(),'<task_id>','AEDT_DELETE_LOCAL_DONE','manual cleanup');"`

## Event Key Standard (Plan 05)
- HFSS contract:
  - `EXPORT_REPORTS_STARTED`, `EXPORT_REPORTS_DONE`
  - `PACKAGE_ZIP_STARTED`, `PACKAGE_ZIP_DONE`
  - `AEDT_DELETE_REMOTE_DONE`, `AEDT_DELETE_LOCAL_DONE`
- Reconcile contract:
  - `RECONCILE_PENDING_TTL_REQUEUE`
  - `RECONCILE_PENDING_TO_UPLOADED`, `RECONCILE_UPLOADED_TO_PENDING`
  - `RECONCILE_DONE_ZIP_RECOVERED`, `RECONCILE_DONE_ZIP_MISSING`

## Validation Record (Plan 05)
- 단위 테스트로 아래 시나리오를 검증한다:
  - pending TTL requeue
  - PENDING/UPLOADED mismatch correction
  - DONE zip recovery / missing downgrade
  - DONE `.aedt` retention audit (delete success/failure)
