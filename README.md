# peetsfea-runner

Minimal 5% bootstrap of a daemon service for `.aedt` queue intake.

## 운영 목표(v2 계약)
- 최종 보존 산출물은 mainPC의 `report-only zip` 1개다.
- `.aedt` 원본은 처리 완료 후 원격/로컬에서 삭제한다.
- 리포트 export는 단일 리포트가 아니라 해석 후 `모든 리포트`를 대상으로 한다.
- 실행 구조는 `mainPC orchestrator + gate spool + 계정별 worker 풀(10)`을 기준으로 확장한다.
- 저장소는 mainPC에서만 운영하고 gate/home에는 git clone 하지 않는다.

## 문서 체계
- 작업 규칙: [AGENTS.md](AGENTS.md)
- 장기 계획: [PLANS/LONG_TERM_PLAN.md](PLANS/LONG_TERM_PLAN.md)
- 단기 계획 01: [PLANS/SHORT_TERM_PLAN_01_LOCAL_QUEUE_HARDENING.md](PLANS/SHORT_TERM_PLAN_01_LOCAL_QUEUE_HARDENING.md)
- 단기 계획 02: [PLANS/SHORT_TERM_PLAN_02_SSH_UPLOAD_AND_DISPATCH.md](PLANS/SHORT_TERM_PLAN_02_SSH_UPLOAD_AND_DISPATCH.md)
- 단기 계획 03: [PLANS/SHORT_TERM_PLAN_03_SLURM_BATCH_AND_CAPACITY_CONTROL.md](PLANS/SHORT_TERM_PLAN_03_SLURM_BATCH_AND_CAPACITY_CONTROL.md)
- 단기 계획 04: [PLANS/SHORT_TERM_PLAN_04_HFSS_EXECUTION_REPORT_EXPORT_AND_PACKAGING.md](PLANS/SHORT_TERM_PLAN_04_HFSS_EXECUTION_REPORT_EXPORT_AND_PACKAGING.md)
- 단기 계획 05: [PLANS/SHORT_TERM_PLAN_05_OPERATIONS_RECONCILIATION_AND_OBSERVABILITY.md](PLANS/SHORT_TERM_PLAN_05_OPERATIONS_RECONCILIATION_AND_OBSERVABILITY.md)
- 운영 runbook: [PLANS/OPERATIONS_RUNBOOK.md](PLANS/OPERATIONS_RUNBOOK.md)

## Scope in this bootstrap
- Folder queue watcher (`incoming/pending/uploaded/done/failed`)
- Gate spool upload dispatcher (`PENDING -> UPLOADED/FAILED_UPLOAD`)
- Slurm worker pool manager (계정별 목표 worker 수 유지)
- HFSS worker execution contract (`analyze -> full report export -> report-only zip -> cleanup`)
- Reconciler/Auditor (`pending TTL requeue`, `DB-file mismatch correction`, `DONE .aedt retention audit`)
- Persistent job state in DuckDB
- Daemon main loop with graceful shutdown (SIGTERM/SIGINT)
- systemd `--user` unit template

Out of scope for now:
- 원격 Slurm worker 잡 내부와 HFSS worker 모듈의 실제 연결

## Run
```bash
python -m peetsfea_runner.main
```

The runtime directories are created automatically under `var/`:
- `var/incoming`
- `var/pending`
- `var/uploaded`
- `var/done`
- `var/failed`
- `var/state`
- `var/state/runner.duckdb`

## Gate Upload Path Contract
- Remote spool path contract:
  - `inbox/`, `claimed/`, `results/`, `failed/`
- Upload target path template:
  - `<spool_inbox>/<task_id>/<filename>`
- Upload idempotency:
  - remote file exists 시 재업로드하지 않고 `UPLOADED`로 복구
  - upload 실패 시 `FAILED_UPLOAD`로 전이하고 오류코드를 기록

## Slurm Worker Pool Contract
- 어댑터 인터페이스:
  - `query_workers`, `submit_worker`, `cancel_worker`
- 계정별 풀 유지:
  - 목표 수(`pool_target_per_account`)보다 적으면 자동 제출
  - 목표 수보다 많으면 초과분 자동 취소
- 고정 자원 정책:
  - partition=`cpu2`, cores=`32`, mem=`320GB`, internal_procs=`8`
- 장애 격리:
  - 계정별 degraded 상태를 추적하고, 정상 계정의 풀 관리는 계속 수행
  - 업로드 단계는 degraded 계정을 제외한 건강한 계정으로만 라우팅

## HFSS Worker Contract
- 고정 AEDT 실행 경로:
  - `/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt`
- 실행 순서:
  - `.aedt` 열기 -> 자동저장 비활성화 -> analyze -> 모든 리포트 export -> report-only zip 생성
- 패키징 규약:
  - zip에는 리포트 출력 파일만 포함하고 `.aedt`/중간파일은 포함하지 않음
- 정리 규약:
  - 성공/실패 모두 `.aedt`와 작업 디렉터리를 정리

## Operations Reconciliation Contract
- 주기적 reconcile 대상:
  - `PENDING` 파일 TTL 초과 시 `incoming/`으로 requeue
  - `PENDING/UPLOADED/FAILED_UPLOAD` 상태와 `pending/`, `uploaded/` 파일 실제 상태 불일치 보정
  - `DONE` 상태의 `report_zip_local_path` 유실 복구 또는 실패 격하
- `.aedt` 삭제 감사:
  - `DONE + aedt_retention=delete_after_done`에서 로컬 `.aedt` 잔존 탐지
  - 잔존 발견 시 `AEDT_RETENTION_VIOLATION_DETECTED` 기록 후 삭제 시도
  - 삭제 성공 시 `AEDT_DELETE_LOCAL_DONE`, 실패 시 작업을 `FAILED`로 전이
- 표준 이벤트 키:
  - `EXPORT_REPORTS_*`, `PACKAGE_ZIP_*`, `AEDT_DELETE_*`, `RECONCILE_*`

## systemd user service
Template file:
- `deploy/systemd/user/peetsfea-runner.service`

Typical flow:
```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/user/peetsfea-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now peetsfea-runner
journalctl --user -u peetsfea-runner -f
```
