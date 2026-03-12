# OPS Stabilization 03: Operations, GUI, and Service Readiness

## 역할

이 문서는 GUI, telemetry, 야간 운영 절차, 향후 service boundary 정리를 위한 상위 index다.
실제 구현 단위는 아래 세 문서로 분리한다.

- [ops-stabilization-03a-resource-telemetry-and-snapshots.md](./ops-stabilization-03a-resource-telemetry-and-snapshots.md)
- [ops-stabilization-03b-overview-gui-and-operator-flow.md](./ops-stabilization-03b-overview-gui-and-operator-flow.md)
- [ops-stabilization-03c-night-operations-rollout-and-service-boundary.md](./ops-stabilization-03c-night-operations-rollout-and-service-boundary.md)

## 핵심 방향

- GUI는 overview-first로 가볍게 만든다.
- `할당 메모리`, `실사용 메모리`, `부하`는 느린 주기의 snapshot으로 신뢰성 있게 보여준다.
- slot pool occupancy, refill latency, tunnel health 같은 운영 지표를 기록한다.
- 야간 cutover와 full rollout 절차를 문서 수준에서 고정한다.
- NAS/공개 저장소는 runner 안정화 이후 단계로 분리하되 경계는 미리 정의한다.

## 세 문서의 분담

### 03a: Resource Telemetry and Snapshots

- memory / load / process snapshot
- node / worker / slot 자원 모델
- 저장 주기와 집계 규칙

### 03b: Overview GUI and Operator Flow

- 느린 GUI 원인 분석
- overview-first 화면 구성
- detail drill-down 구조
- 운영자 확인 순서

### 03c: Night Operations Rollout and Service Boundary

- canary와 full cutover 순서
- 야간 운영 체크리스트
- fallback 규칙
- NAS와 runner 경계

## 완료 기준

- GUI 첫 화면이 summary 중심으로 가볍게 동작한다.
- 운영자가 `할당 메모리`, `실사용 메모리`, `부하`, `slot active/idle`, `tunnel status`를 확인할 수 있다.
- 야간 전환 절차와 fallback 경계가 문서 기준으로 고정된다.
