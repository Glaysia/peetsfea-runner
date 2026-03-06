# Roadmap 3: Runtime Readiness And Failure Diagnostics

## Summary
- 새 계정이 추가되어도 조용히 죽지 않게 하려면, worker 제출 전에 계정별 runtime 준비 상태가 보장되어야 한다.
- 실패한 case는 `case_x:1` 같은 축약 상태만 남기면 안 되고, 실제 `run.log`, stderr, launch failure reason이 반드시 로컬에서 보이도록 수집해야 한다.
- readiness 부족과 실제 시뮬레이션 실패를 구분할 수 있어야 운영 중 자동복구와 원인 분석이 가능하다.

## Must-Have Improvements
- 계정별 Python, venv, Ansys module, 실행 바이너리, 필수 의존성 readiness를 worker 제출 전에 확인한다.
- readiness가 부족한 계정은 자동 bootstrap 또는 명시적 차단 대상으로 처리하고, 조용히 worker를 날리지 않는다.
- 계산 노드에서 생성된 `run.log`, stderr, exit code, launch failure reason을 항상 로컬로 회수한다.
- worker 실패와 case 실패의 원인 메시지를 구분해 저장한다.
- 원격 임시 경로가 계산 노드 local `/tmp`라는 사실을 전제로, 종료 전 로그와 failure artifacts를 반드시 수집한다.
- readiness 실패, launch 실패, solve 실패, collect 실패를 서로 다른 진단 범주로 남긴다.

## Acceptance Signals
- 새 계정 추가 후 runtime 부족 때문에 worker가 조용히 사라지지 않는다.
- 실패한 case는 최소한 `run.log` 또는 동등한 원인 로그가 로컬에 남는다.
- 운영자가 로그만 보고 readiness 문제인지 시뮬레이션 자체 실패인지 구분할 수 있다.
- 계산 노드 local `/tmp`를 써도 종료 전 failure artifacts가 보존된다.

## Assumptions
- 계정별 홈 디렉터리와 runtime 상태는 서로 다를 수 있다는 전제를 유지한다.
- readiness 보장은 worker 제출 전에 끝내는 것이 운영 안정성에 더 유리하다.
- 문서의 목적은 디버깅 경로와 실패 원인 보존을 운영 요구사항으로 고정하는 것이다.
