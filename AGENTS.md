# AGENTS

이 문서는 `peetsfea-runner` 저장소에서 작업하는 코딩 에이전트의 작업 규칙을 정의한다.

## 프로젝트 목표
- `.aedt` 파일 큐를 안정적으로 수집/추적하는 daemon을 제공한다.
- 로컬 감시 -> 원격 실행 -> 결과 회수까지의 운영 파이프라인을 단계적으로 완성한다.
- 운영 중 재현성과 추적 가능성을 우선한다.
- 최종 산출물은 mainPC의 report-only zip 1개로 고정하고 원본 `.aedt`는 삭제 정책을 강제한다.

## 비목표 (현재 단계)
- GUI 기반 운영 도구 제공.
- 무제한 확장/오토스케일링.
- 모든 AEDT 애플리케이션 동시 지원.

## 코딩 원칙
- 타입 힌트를 우선한다.
- 실패는 빠르게 드러내고, 실패 원인을 명시적으로 기록한다.
- 기본값은 코드에 명시하고 숨은 동작을 만들지 않는다.
- 외부 경계(DB/SSH/HFSS)는 계약(입력/출력)을 고정하고 단위 테스트 가능하게 분리한다.

## 실행 환경 규칙
- 가상환경 생성은 아래 명령을 사용한다.
  - `~/miniconda3/envs/py312/bin/python -m uv venv`
- 프로젝트 명령은 `.venv` 바이너리를 우선 사용한다.
  - `.venv/bin/python`
  - `.venv/bin/pytest`

## 설정 원칙
- 런타임 외부 설정 파일(`toml/json/yaml`)을 사용하지 않는다.
- `main.py`의 Python 객체 설정을 SSOT로 유지한다.
- 설정 변경 시 `README.md`, `PLANS/*`, `AGENTS.md`를 함께 갱신한다.

## 테스트 원칙
- 단위 테스트를 우선한다.
- 원격 SSH/Slurm/HFSS 연동은 통합 테스트로 분리한다.
- 기본 테스트 실행은 빠르고 결정론적이어야 한다.

## 문서 동기화 원칙
- 동작 계약(API/상태/산출물)이 바뀌면 다음 파일을 동시에 갱신한다.
  - `README.md`
  - `PLANS/*`
  - `AGENTS.md`
  - 운영 절차 변경 시 `PLANS/MASTER_PLAN.md`의 운영 절차 섹션도 함께 갱신한다.

## 계획 문서 운용 원칙
- 계획 문서 SSOT는 `PLANS/MASTER_PLAN.md`로 유지한다.
- 신규 계획/합의는 먼저 `PLANS/MASTER_PLAN.md`에 반영한다.
- 기존 계획 문서는 날짜 폴더(`PLANS/archive/YYYY-MM-DD/`)로 아카이브하고, 아카이브 문서는 원본 보존을 원칙으로 한다.

## 운영 핵심 고정값 (합의 사항)
- 현재 기본 프로필: `gate1` Linux 다중 계정
- 계정 수: `3` (`gate1-harry`, `gate1-hmlee31`, `gate1-dhj02`)
- 계정당 worker 상한: `10`
- worker 내부 동시 PyAEDT 프로세스: `8`
- Slurm 파티션/자원: `cpu2`, `32 cores`, `320GB`
- 배포 태그: `v2026.03.03-proc8-core4-r1`
- 리포트 export: 해석 후 `모든 리포트`를 출력
- 결과 보존: mainPC에 `report-only zip` 1개만 보존
- 원본 `.aedt`: 완료 후 원격/로컬에서 삭제
- 저장소 운영 위치: repo는 `mainPC`를 기준으로 운영하되, worker bootstrap 용 원격 태그 clone은 허용
- 다중 계정 운영 시 원격 spool: `/gpfs/home1/<user>/peetsfea-spool/{inbox,claimed,results,failed}`
- 결과 회수: Collector가 gate `results/`를 스캔해 `done/`으로 원자 회수하며 중복 다운로드를 건너뛴다.
- 운영 감사: Reconciler가 상태 불일치(고아 done zip 포함) 보정과 `.aedt` 잔존 감사(`delete_after_done`)를 수행
- 원격 bootstrap 경로: 계정별 `/gpfs/home1/<user>/{peetsfea-runner,.peetsfea-venv}`
- 원격 AEDT 런치 환경 강제: `LANG`, `LC_ALL`, `LANGUAGE`, `ANSYSLMD_LICENSE_FILE`를 코드에서 export/unset으로 고정

## Discord 알림 규칙 (MCP)
- 작업 시작/종료 시각은 셸 초 단위로 계산한다.
  - 시작: `start_ts=$(date +%s)`
  - 종료: `end_ts=$(date +%s); elapsed=$((end_ts-start_ts))`
- `elapsed >= 300`일 때만 알림을 보낸다.
- 성공 메시지 첫 줄은 `Codex 완료`로 시작한다.
- 실패/중단 메시지 첫 줄은 `Codex 실패`로 시작한다.
- 본문에는 요약 1~2줄과 다음 행동(있는 경우)만 포함한다.

## 작업 완료 점검
- 문서 링크가 유효한지 확인한다.
- 계획 문서의 Done Criteria가 구현/테스트 전략과 충돌하지 않는지 확인한다.
- 변경사항을 `git status`로 점검하고 누락 파일이 없는지 확인한다.
