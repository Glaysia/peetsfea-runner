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

## Scope in this bootstrap
- Folder queue watcher (`incoming/pending/uploaded/done/failed`)
- Gate spool upload dispatcher (`PENDING -> UPLOADED/FAILED_UPLOAD`)
- Slurm worker pool manager (계정별 목표 worker 수 유지)
- Persistent job state in DuckDB
- Daemon main loop with graceful shutdown (SIGTERM/SIGINT)
- systemd `--user` unit template

Out of scope for now:
- HFSS worker execution pipeline (analyze/export/report packaging)

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
