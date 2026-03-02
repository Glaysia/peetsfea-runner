# LONG TERM PLAN - peetsfea-runner v2 (worker 풀 + report-only zip)

## 0) 요구사항 업데이트 (최우선)
- 최종 장기 보존 산출물은 `mainPC`의 결과 zip 1개다.
- 결과 zip에는 "리포트 출력 파일"만 포함한다.
- 원본 `.aedt`는 처리 완료 후 로컬/원격에서 삭제한다.
- 저장소는 `mainPC`에서만 운영하며 계정별 gate/home에 git clone 하지 않는다.
- 계정은 5개, 계정당 활성 Slurm worker 잡은 10개를 유지한다.
- Slurm 고정값: `cpu2`, 잡당 `32코어/320GB`, 잡 내부 PyAEDT `8` 프로세스.
- 상태 저장은 mainPC 로컬 DuckDB 단일 파일을 사용하며, 런타임 외부 설정 파일(JSON/TOML/YAML)은 사용하지 않는다.

## 1) 최상위 설계
- `.aedt`마다 `sbatch`를 제출하지 않고 계정당 worker 잡 10개를 상시 유지한다.
- 작업은 gate spool `inbox/`로 push, worker는 `claimed/`로 원자 이동 후 pull 방식으로 처리한다.
- worker는 "리포트 export -> report-only zip 생성 -> 원본 `.aedt` 삭제"를 책임진다.
- mainPC orchestrator는 결과 zip만 회수해 `done/`에 보존한다.

## 2) 아키텍처 개요
- MainPC Orchestrator (`systemd --user`)
  - 로컬 입력 감시/등록
  - gate 업로드
  - 계정별 worker 풀 유지
  - 결과 zip 회수
  - 로컬/원격 `.aedt` 삭제 정책 강제
  - DuckDB 상태/이벤트 기록
- Gate Spool (계정별)
  - `inbox/`, `claimed/`, `results/` (+ 옵션 `done/`, `failed/`)
- Slurm Worker (계정당 10잡)
  - `inbox -> claimed` 원자 claim
  - 잡 내부 PyAEDT 8개 병렬 실행
  - 모든 리포트 export
  - 리포트 파일만 zip 패키징
  - `.aedt` 및 불필요 중간 산출물 삭제

## 3) 산출물/보존 정책
- 장기 보존 대상: mainPC `report-only zip` 1개.
- 비보존 대상: 원본 `.aedt`, 프로젝트/캐시/중간 산출물.
- DONE 승격 전제:
  - 결과 zip 회수 성공
  - 원격 `.aedt` 삭제 완료
  - 로컬 `.aedt` 삭제 완료

## 4) Worker 실행 계약 (필수)
1. Simulate
- PyAEDT/HFSS로 해석 완료.

2. Export All Reports
- 해석 후 결과가 채워진 리포트를 포함해 모든 리포트를 파일로 export.
- export 형식/파일명/디렉터리 규칙은 `ReportExportSpec`으로 고정.

3. Package Reports Only
- 리포트 전용 결과 디렉터리만 zip에 포함.
- zip 내부에 `.aedt`, 프로젝트 파일, 캐시, 중간 산출물 금지.

4. Cleanup
- 결과 zip 이동 완료 후 `.aedt`와 임시파일 삭제.
- `claimed/` 입력도 정리(필요 시 최소 메타만 보존).

## 5) 큐/상태 머신
- mainPC 로컬
  - `incoming/` -> `pending/` -> `uploaded/` -> `done/|failed/`
- gate spool
  - `inbox/` -> `claimed/` -> `results/|failed/`
- 상태 전이
  - `NEW -> PENDING -> UPLOADED -> CLAIMED -> RUNNING -> DONE|FAILED`
- DONE 조건
  - report-only zip 회수 + 원격/로컬 `.aedt` 삭제 이벤트 충족.

## 6) DuckDB 계약 변경
- `tasks` 필드(핵심)
  - `aedt_retention` (`delete_after_done`)
  - `report_zip_local_path`
  - `report_zip_remote_path`
  - `report_export_spec_id`
  - `local_aedt_deleted_ts`
  - `remote_aedt_deleted_ts`
- `events` 필드(핵심)
  - `EXPORT_REPORTS_STARTED`, `EXPORT_REPORTS_DONE`
  - `PACKAGE_ZIP_STARTED`, `PACKAGE_ZIP_DONE`
  - `AEDT_DELETE_REMOTE_DONE`, `AEDT_DELETE_LOCAL_DONE`

## 7) Orchestrator 책임
- Ingest: 입력 등록 + DB 기록 + `pending` 이동.
- Upload: gate `inbox` 업로드 + 성공 기록.
- Pool Manager: 계정별 worker 10개 유지.
- Collect: `results/`의 report-only zip 회수 및 중복 방지.
- Cleanup Enforcement: DONE 시 로컬 `.aedt` 삭제 강제.
- Auditor: claimed TTL 초과 복구, DB/파일 reconcile, `.aedt` 잔존 탐지/정리.

## 8) Worker Bundle 배포
- 번들은 mainPC에서 단일 생성 후 gate로 배포.
- gate/home에는 repo clone 금지.
- 번들 버전을 결과 zip 메타에 포함해 재현성을 확보.

## 9) 실패/재시도 정책
- SSH/네트워크/업로드 실패: retryable (backoff).
- worker 부족/비정상 종료: Pool Manager 자동 복구.
- PyAEDT/HFSS 실패: 기본 non-retryable 또는 제한 재시도(정책 고정).
- 리포트 export 실패: 실패 zip(가능 산출물 + 로그/메타) 또는 `failed` 마킹.
- `.aedt` 삭제 실패: DONE 승격 금지, 감사/재시도 대상으로 유지.

## 10) 설정 원칙 (생성자 주입)
- `main.py`에서 Python 객체를 직접 생성/주입한다.
- 고정 객체
  - `Accounts` (5계정, pool_size=10)
  - `SlurmPolicy` (`cpu2`, `32코어`, `320GB`, 내부 `8`)
  - `ReportExportSpec`
  - `RetentionPolicy(aedt_delete_after_done=True)`
  - `TTL/RetryPolicy`
  - `LocalPaths` (queue/done/duckdb)

## 11) 마일스톤
- M0: 최소 E2E
  - 업로드 -> worker 실행 -> 모든 리포트 export -> report-only zip 회수 -> 원본 삭제.
- M1: worker 풀/중복 회수
  - 계정별 10 worker 유지, zip 중복 회수 방지.
- M2: 5계정 확장/격리
  - degraded 계정 우회, backoff/circuit breaker.
- M3: 감사/복구
  - TTL requeue, DB/파일 reconcile, 삭제 정책 위반 자동 정리.

## 12) 코드 작성 전 확정 항목
1. 리포트 출력 결과 정의
- 포함 대상 리포트, 형식(CSV/PNG/PDF), 파일명 규약.

2. 실패 zip 최소 포함 범위
- 로그/메타를 리포트 범주에 포함할지.

3. `.aedt` 삭제 타이밍
- worker 즉시 삭제(권장) vs 회수 확인 후 삭제.

4. 입력 포맷 전환
- 초기 `.aedt` 업로드 허용, 장기적으로 결정론적 재생성 정의 입력으로 전환.
