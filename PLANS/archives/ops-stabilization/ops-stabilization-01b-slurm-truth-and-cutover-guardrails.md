# OPS Stabilization 01B: Slurm Truth and Cutover Guardrails

## 목적

이 문서는 현재 경로 안정화와 이후 전체 전환 사이를 잇는 안전장치 계획서다.
핵심은 `지금 보이는 상태`를 Slurm truth 기준으로 덜 틀리게 만들고, 야간 maintenance cutover의 stop/reset/restart 규칙을 고정하는 것이다.

## 해결할 문제

- service 내부 상태만으로는 실제 Slurm queue를 정확히 설명하지 못한다.
- 재시작 또는 cutover 직전에 어떤 조건에서 service를 내려야 하는지 기준이 약하다.
- DB reset이 필요할 때와 아닌 때가 문서로 고정되어 있지 않다.
- 새 구조 canary 실패 시 어디까지 rollback할지 기준이 없다.

## 목표 동작

- account/worker 상태는 최소 `squeue` 기준으로 주기적으로 보정된다.
- maintenance cutover 전후 절차가 문서와 구현 양쪽에서 일관된다.
- canary 실패 시 fallback 범위가 미리 정해져 있다.

## Slurm Truth 최소 모델

- account별 `remote running`
- account별 `remote pending`
- worker별 `slurm job presence`
- queue 반영 지연 추정
- local inflight 대비 원격 관측 mismatch

이 단계의 목적은 full recovery 구현이 아니라, cutover 전까지 `지금 실제 queue에 뭐가 있나`를 신뢰 가능하게 만드는 것이다.

## Cutover Guardrails

### 사전 조건

- 구현 변경이 service 동작에 영향을 주는 major change인지 먼저 판정한다.
- major change면 service stop 이후 DB reset 필요 여부를 검토한다.
- queue와 local state 차이가 큰 상태에서 바로 cutover하지 않는다.

### maintenance 절차

1. service를 완전히 내린다.
2. 남아 있는 원격 worker/job 상태를 확인한다.
3. major change면 규칙대로 `peetsfea_runner.duckdb`를 삭제한다.
4. 새 구조 또는 fallback 구조로 service를 다시 올린다.
5. `sample.aedt` canary를 먼저 통과시킨다.

### rollback 규칙

- `sample.aedt` canary가 실패하면 heavy backlog로 넘어가지 않는다.
- 전체 새 구조가 실패하면 `01a/01b` 수준의 안정화만 남기고 현행 처리 지속 경로로 후퇴한다.
- rollback은 "새 구조 전면 폐기"가 아니라 "capacity correctness와 cutover safety만 유지"를 목표로 한다.

## 상태와 이벤트

- `SLURM_TRUTH_REFRESHED`
- `CUTOVER_BLOCKED`
- `CUTOVER_READY`
- `ROLLBACK_TRIGGERED`
- `CANARY_FAILED`
- `CANARY_PASSED`

## 구현 체크리스트

1. `squeue` 기반 account/worker truth refresh를 추가한다.
2. queue 반영 지연과 장기 mismatch를 구분해 기록한다.
3. service stop / DB reset / restart 체크리스트를 문서와 로그 메시지로 정리한다.
4. canary 통과 전 heavy input 금지 조건을 명시한다.
5. fallback 시 유지할 기능과 버릴 기능을 명시한다.

## 테스트 계획

- 원격 queue와 로컬 inflight가 잠시 어긋나는 상황에서 truth refresh가 정상 보정하는 테스트
- service stop 후 DB reset 경로가 문서 규칙과 일치하는 운영 체크 테스트
- canary 실패 시 heavy input 실행으로 넘어가지 않는 절차 테스트
- fallback 모드에서 hard cap과 underfill 경보만 유지되는지 확인하는 테스트

## 수용 기준

- cutover 직전과 직후에 운영자가 `지금 Slurm에 실제로 무엇이 살아 있는지` 설명할 수 있다.
- major 변경 시 DB reset 규칙이 문서와 실행 절차에서 일관된다.
- canary 실패 시 전체 전환을 중단하고 fallback으로 내려가는 기준이 명확하다.
