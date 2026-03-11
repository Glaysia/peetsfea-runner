# OPS Stabilization 01: Current Path Guardrails

## 역할

이 문서는 현재 `foreground ssh` 경로를 유지한 채 당장 운영 사고를 줄이는 workstream의 상위 index다.
실제 구현 단위는 아래 두 문서로 분리한다.

- [ops-stabilization-01a-capacity-hard-cap-and-backlog.md](./ops-stabilization-01a-capacity-hard-cap-and-backlog.md)
- [ops-stabilization-01b-slurm-truth-and-cutover-guardrails.md](./ops-stabilization-01b-slurm-truth-and-cutover-guardrails.md)

## 범위 요약

- 계정별 hard cap과 backlog 재분배
- oversubmission / underfill 수정
- 최소 Slurm truth reconcile
- cutover 전후 안전장치와 rollback 기준

## 현재 단계의 핵심 원칙

- AEDT는 계속 돌려야 한다.
- 새 구조 도입 전까지도 계정별 visible worker 수는 cap을 넘기면 안 된다.
- `pending_buffer`는 hard cap 확장 수단이 아니라 관측 지연 보정치다.
- 현재 경로 안정화는 후속 전환의 전제 조건이다.
- main PC가 실행 중 worker에 slot을 재주입하는 steady-state 모델은 채택하지 않는다.

## 문서 분담

### 01a: Capacity Hard Cap and Backlog

- 계정별 cap 계산 재정의
- local inflight / submitted_unobserved 처리
- backlog 재분배
- underfill / oversubmission 탐지

### 01b: Slurm Truth and Cutover Guardrails

- 최소 Slurm truth reconcile
- maintenance cutover 체크리스트
- service stop / DB reset / rollback 기준
- canary와 full cutover 경계

## 이 단계의 완료 기준

- `dw16` 과제출처럼 visible worker 수가 cap을 넘는 상태가 재현되지 않는다.
- `dhj02` underfill처럼 빈 계정이 장시간 놀면 즉시 경보가 드러난다.
- cutover 전에 어떤 조건에서 service를 내리고 DB를 리셋해야 하는지 문서 기준이 고정된다.
