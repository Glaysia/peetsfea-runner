# peetsfea-runner

Minimal 5% bootstrap of a daemon service for `.aedt` queue intake.

## 운영 목표(v2 계약)
- 최종 보존 산출물은 mainPC의 `report-only zip` 1개다.
- `.aedt` 원본은 처리 완료 후 원격/로컬에서 삭제한다.
- 리포트 export는 단일 리포트가 아니라 해석 후 `모든 리포트`를 대상으로 한다.
- 실행 구조는 `mainPC orchestrator + gate spool + 단일 계정 worker 풀(10)`으로 운영한다.
- 저장소는 mainPC에서만 운영하고 gate/home에는 git clone 하지 않는다.

## 문서 체계
- 작업 규칙: [AGENTS.md](AGENTS.md)
- 마스터 계획(SSOT): [PLANS/MASTER_PLAN.md](PLANS/MASTER_PLAN.md)
- 계획 아카이브 인덱스: [PLANS/archive/2026-03-01/INDEX.md](PLANS/archive/2026-03-01/INDEX.md)

## Scope in this bootstrap
- Folder queue watcher (`incoming/pending/uploaded/done/failed`)
- Gate spool upload dispatcher (`PENDING -> UPLOADED/FAILED_UPLOAD`)
- Gate results collector (`results -> done`, atomic download, idempotent skip)
- Slurm worker pool manager (계정별 목표 worker 수 유지)
- HFSS worker execution contract (`analyze -> full report export -> report-only zip -> cleanup`)
- Reconciler/Auditor (`pending TTL requeue`, `DB-file mismatch correction`, `DONE .aedt retention audit`)
- Persistent job state in DuckDB
- Daemon main loop with graceful shutdown (SIGTERM/SIGINT)
- systemd `--user` unit template

Current rollout scope:
- 단일 계정 `gate1-harry` (`harry261`)를 사용한다.
- 원격 spool 경로는 `/home1/harry261/peetsfea-spool/{inbox,claimed,results,failed}`를 사용한다.
- Slurm worker는 `peetsfea_runner.remote_worker`를 실행해 claimed 작업을 처리한다.

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
- 단일 계정 운영:
  - ssh alias=`gate1-harry`
  - remote repo=`/home1/harry261/peetsfea-runner`
  - remote venv=`/home1/harry261/.peetsfea-venv`
  - submit 시 remote bootstrap 자동화:
    - `~/peetsfea-runner` 미존재 시 생성 + mainPC에서 rsync 동기화
    - `~/.peetsfea-venv` 미존재 시 `python3.12 -m venv`로 생성
    - `uv`, `pyaedt==0.24.1`, editable 패키지 설치 보장
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
  - 실패 메타데이터는 `failed/<task_id>.failed.json`으로 기록

## Operations Reconciliation Contract
- 주기적 reconcile 대상:
  - `PENDING` 파일 TTL 초과 시 `incoming/`으로 requeue
  - `PENDING/UPLOADED/FAILED_UPLOAD` 상태와 `pending/`, `uploaded/` 파일 실제 상태 불일치 보정
  - `DONE` 상태의 `report_zip_local_path` 유실 복구 또는 실패 격하
  - `done/`의 고아 `*.reports.zip`(DB 미등록)을 자동 등록해 DB-파일 정합성 복구
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
