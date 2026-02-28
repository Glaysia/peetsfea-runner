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
- Persistent job state in DuckDB
- Daemon main loop with graceful shutdown (SIGTERM/SIGINT)
- systemd `--user` unit template

Out of scope for now:
- Remote SSH/Slurm dispatch
- HFSS analyze/export execution

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
