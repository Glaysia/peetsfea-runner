# OPS Stabilization Master Plan

## 요약

이 문서는 `peetsfea-runner`를 밤새 전환 가능한 수준으로 정리하기 위한 umbrella 계획서다.
실제 구현 단위는 기존 3개 세부 계획서를 쪼갠 `총 8개 실행 계획서`로 관리한다.

핵심 방향은 다음과 같다.

- 오늘 밤 전체 일괄 전환을 전제로 준비한다.
- 변경 순서는 `문서 분해 -> 요청 매핑 -> 코드 구현 -> 단일 maintenance cutover -> sample canary -> 실운전`으로 고정한다.
- 구조 전환의 기준은 `persistent worker job + worker-owned slot refill + SSH-tunneled localhost control plane`이다.
- 새 구조가 canary를 통과하지 못하면 `01a/01b` 수준의 안정화만 적용한 fallback을 우선한다.

## 현재 문제 요약

- `foreground ssh` 경로가 service 생명주기와 원격 실행 생명주기를 강하게 묶고 있다.
- scheduler가 과제출과 underfill을 동시에 만들 수 있다.
- restart 이후 Slurm truth와 로컬 상태를 재연결하는 능력이 약하다.
- GUI가 느리고, `할당 메모리`, `실사용 메모리`, `부하` 같은 운영 지표가 부족하다.
- 앞으로 NAS/공개 저장소까지 고려하면 runner control plane과 storage boundary를 미리 분리해야 한다.

## 전체 목표

- 계정별 capacity correctness를 먼저 확보한다.
- cutover 전후 상태 복구 기준을 명확히 한다.
- `sbatch` 기반 장수 worker job으로 전환한다.
- worker job 내부에서 `slot pool size = 4`를 기본값으로 유지하고 worker가 스스로 refill한다.
- steady-state 통신은 `worker별 SSH 터널 + main PC localhost HTTP control API`로 제한한다.
- GUI와 telemetry를 운영 콘솔 수준으로 끌어올린다.
- 최종적으로 밤새 3000개 backlog를 새 구조로 처리 가능한 상태를 만든다.

## 설계 원칙

- 빅뱅 구현은 하되, cutover는 한 번만 한다.
- major 변경 전까지는 `examples/sample.aedt`만 검증 입력으로 쓴다.
- `original` 아래 heavy input은 모든 계획 이행 후 최종 실운전 단계에서만 사용한다.
- 문서와 구현은 함수 호출 기반 단일 경로를 유지한다.
- main PC는 steady-state slot refill 주체가 아니라 worker replacement 주체다.
- 외부 포트 개방은 하지 않는다.

## 실행 계획서 구성

### A. Current Path Guardrails

- [ops-stabilization-01a-capacity-hard-cap-and-backlog.md](./ops-stabilization-01a-capacity-hard-cap-and-backlog.md)
- [ops-stabilization-01b-slurm-truth-and-cutover-guardrails.md](./ops-stabilization-01b-slurm-truth-and-cutover-guardrails.md)

### B. sbatch / Slot Transition

- [ops-stabilization-02a-persistent-worker-job-model.md](./ops-stabilization-02a-persistent-worker-job-model.md)
- [ops-stabilization-02b-slot-pool-and-worker-owned-refill.md](./ops-stabilization-02b-slot-pool-and-worker-owned-refill.md)
- [ops-stabilization-02c-ssh-tunneled-control-plane-and-recovery.md](./ops-stabilization-02c-ssh-tunneled-control-plane-and-recovery.md)

### C. Operations / GUI / Service Readiness

- [ops-stabilization-03a-resource-telemetry-and-snapshots.md](./ops-stabilization-03a-resource-telemetry-and-snapshots.md)
- [ops-stabilization-03b-overview-gui-and-operator-flow.md](./ops-stabilization-03b-overview-gui-and-operator-flow.md)
- [ops-stabilization-03c-night-operations-rollout-and-service-boundary.md](./ops-stabilization-03c-night-operations-rollout-and-service-boundary.md)

기존 그룹 문서는 아래처럼 index 용도로만 남긴다.

- [ops-stabilization-01-current-path-guardrails.md](./ops-stabilization-01-current-path-guardrails.md)
- [ops-stabilization-02-sbatch-slot-transition.md](./ops-stabilization-02-sbatch-slot-transition.md)
- [ops-stabilization-03-operations-gui-and-service-readiness.md](./ops-stabilization-03-operations-gui-and-service-readiness.md)

## 야간 실행 순서

1. 21:00 전까지 8개 실행 계획서를 확정한다.
2. 이후 들어올 개선 요청은 반드시 8개 workstream 중 하나에 매핑한다.
3. 구현 순서는 `01a -> 01b -> 02a -> 02b -> 02c -> 03a -> 03b -> 03c`로 고정한다.
4. cutover 직전 service를 완전히 내리고, 규칙대로 DB reset 여부를 판단한다.
5. 새 구조로 service를 올린 뒤 `sample.aedt` 다건 canary를 통과시킨다.
6. canary 통과 후에만 실제 backlog를 전량 처리한다.

## 최종 완료 기준

- 계정별 visible worker 수가 `max_jobs`를 넘지 않는다.
- underfill과 oversubmission을 GUI와 로그에서 즉시 설명할 수 있다.
- service restart 이후 worker/job/slot 상태가 Slurm truth를 기준으로 복구된다.
- worker job 내부 slot pool 4개가 유지되고, slot 종료 후 worker가 같은 job 안에서 refill한다.
- main PC는 실행 중 worker job 안에 다시 붙지 않고 worker replacement만 담당한다.
- 외부 포트 개방 없이 SSH 터널 기반 control plane이 유지된다.
- GUI에서 `할당 메모리`, `실사용 메모리`, `부하`, `slot active/idle`, `tunnel status`를 볼 수 있다.
- `sample.aedt` canary가 통과하고, 이후 실제 backlog 실운전으로 넘어갈 수 있다.

## 후속 범위

NAS 형태의 공개 저장소, 외부 사용자 인증, quota, 권한 모델은 이번 야간 전환의 직접 구현 범위가 아니다.
다만 [ops-stabilization-03c-night-operations-rollout-and-service-boundary.md](./ops-stabilization-03c-night-operations-rollout-and-service-boundary.md) 에서 service boundary는 미리 정리한다.
