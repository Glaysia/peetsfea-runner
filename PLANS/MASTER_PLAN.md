# MASTER PLAN - peetsfea-runner

## 문서 목적
- 이 문서는 `peetsfea-runner`의 계획/운영 SSOT(Single Source of Truth)다.
- 신규 계획은 이 문서에 먼저 반영하고, 세부 파생 문서는 필요할 때만 추가한다.
- 과거 계획 문서는 `PLANS/archive/2026-03-01/`에 보관한다.

## 목표
- `.aedt` 큐를 안정적으로 수집/추적하는 daemon을 제공한다.
- 로컬 감시 -> 원격 실행 -> 결과 회수까지 단일 운영 파이프라인을 완성한다.
- 최종 보존 산출물을 mainPC의 `report-only zip` 1개로 고정한다.
- 처리 완료 후 원본 `.aedt`를 원격/로컬에서 삭제한다.

## 비목표
- GUI 운영 도구 제공.
- 무제한 오토스케일링.
- 모든 AEDT 애플리케이션 동시 지원.

## 운영 고정값
- 현재 실행 프로필: `5600X2` Windows GUI 디버그
- Windows 런처 모드: `Task Scheduler + InteractiveToken`
- 계정 수: `1` (`win5600x2`)
- 계정당 활성 worker 상한: `1`
- worker 내부 동시 PyAEDT 프로세스: `6` (Windows 상한)
- AEDT 실행 경로: `C:\Program Files\ANSYS Inc\v252\AnsysEM\ansysedt.exe`
- 배포 태그: `v2026.03.02-5600x2-r2`
- 리포트 export 정책: 해석 후 `모든 리포트` 출력
- 결과 보존 정책: mainPC에 `report-only zip` 1개만 보존
- 원본 `.aedt` 정책: 완료 후 원격/로컬 삭제
- 상태 저장소: mainPC 로컬 DuckDB 단일 파일

## 현재 구현 상태
### 완료
- 로컬 큐 감시 및 전이: `incoming/pending/uploaded/done/failed`
- gate 업로드 디스패처(멱등 복구/오류 분류 포함)
- gate 결과 zip 회수기(원자 다운로드/중복 skip)
- Slurm worker pool 유지(계정별 target 유지, degraded 우회)
- HFSS worker 실행 계약 모듈(어댑터 인터페이스, report-only zip 패키징, cleanup)
- Slurm `--wrap` -> 원격 `peetsfea_runner.remote_worker` 실행 연결
- submit 시 원격 bootstrap 자동화(`repo/venv` 미존재 시 생성 + 태그 checkout)
- Windows submit/query/cancel 계약:
  - submit: Scheduled Task `InteractiveToken` 등록/시작
  - query: pidfile 우선 + Win32_Process 커맨드라인 fallback
  - cancel: PID 종료 + orphan python 종료 + Scheduled Task cleanup
- Reconciler/Auditor(상태 불일치 보정, DONE zip 고아 등록, `.aedt` 잔존 감사)
- DuckDB 스키마/이벤트 기록 및 daemon 루프 통합

### 미완료
- PyAEDT 실제 어댑터의 실환경 통합 테스트
- DONE 승격 게이트의 원격 `.aedt` 삭제 이벤트 강제 검증 강화

## 최신 합의 반영
- mainPC 서비스 실행 경로는 `harry` 계정 기준으로 고정:
  - `WorkingDirectory=/home/harry/Projects/PythonProjects/peetsfea-runner`
  - `ExecStart=/home/harry/Projects/PythonProjects/peetsfea-runner/.venv/bin/python -m peetsfea_runner.main`
- 원격 worker 환경 전제(5600X2):
  - `C:/.peetsfea-venv` 존재(없으면 bootstrap에서 생성)
  - `C:/peetsfea-runner` clone 존재(없으면 bootstrap에서 clone)
  - spool 경로: `C:/peetsfea-spool/{inbox,claimed,results,failed}`
- 원격 worker bootstrap 정책:
  - 원격 repo bootstrap은 `git clone/fetch --tags` 후 지정 태그 checkout(`v2026.03.02-5600x2-r2`)
  - python/venv/uv/pyaedt 설치 보장 후 worker 시작
- Windows GUI 가시성 정책:
  - worker python과 `ansysedt.exe`는 로그인 사용자 세션(Session 2)에서 실행되어야 한다.
  - Session 0 실행은 장애로 간주하고 degraded 처리한다.
- PyAEDT 버전 정책:
  - `pyaedt==0.24.1` 고정

## 단계별 실행 로드맵
### 즉시(Phase A)
- systemd user service를 `peetsmain` 절대경로로 전환
- 문서 진입점을 본 `MASTER_PLAN` 단일화
- 원격 bootstrap 계약(태그 기준 동기화 + `python -m uv`)을 코드/테스트 계약으로 반영

### 단기(Phase B)
- Windows submit 경로 안정화:
  - `Register-ScheduledTask`/`Start-ScheduledTask` 성공/실패 코드 표준화
  - no-interactive-session(`E_WIN_NO_INTERACTIVE_SESSION`) 명확화
  - launch health check 실패 시 stdout/stderr tail + Task last result 포함
- bootstrap 이벤트/오류 코드 표준화 및 운영 로그 확장
- `pyproject.toml`에 `pyaedt==0.24.1` 반영 및 설치 검증 경로 추가

### 중기(Phase C)
- PyAEDT 어댑터를 실제 원격 worker 실행 경로에 연결
- end-to-end 통합 테스트(원격 claim -> analyze -> export -> report-only zip -> collect) 구축
- DONE 승격 게이트를 `zip 회수 + 원격/로컬 .aedt 삭제`로 엄격화

## 완료 기준(DoD)
- 계획/운영 문서는 `MASTER_PLAN` 기준으로 일관 참조된다.
- worker bootstrap 실패가 누락 없이 실패 전이된다.
- 버전 동기화 기준(릴리스 태그)과 설치 방식(`python -m uv`)이 코드/문서에 동일하게 반영된다.
- 결과 보존/삭제 정책(report-only zip 1개, `.aedt` 삭제)이 테스트/운영 절차와 충돌하지 않는다.

## 검증 방법
- 문서 구조 검증:
  - `PLANS/` 루트에 `MASTER_PLAN.md`만 존재
  - 기존 계획 문서는 `PLANS/archive/2026-03-01/`에 존재
- 링크 검증:
  - `README.md`, `AGENTS.md`에서 `MASTER_PLAN` 우선 참조
  - 아카이브 경로 안내가 명시됨
- 회귀 검증:
  - `src/` 코드 비변경 확인(문서 개편 작업 기준)
  - 테스트 실행 정책과 문서 내용 간 충돌 없음
