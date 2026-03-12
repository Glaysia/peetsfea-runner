# Roadmap Tonight 02: Bundle Throughput and Prefetch

## 목적

이 문서는 오늘밤 처리량을 가장 빨리 끌어올리는 레버를 정의한다.
핵심은 `slots_per_job=5`를 유지한 채 `PEETSFEA_WORKER_BUNDLE_MULTIPLIER`를 활용해 bundle tail을 줄이고 idle slot을 줄이는 것이다.

## Leaf 문서

- [roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md](./roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md)
- [roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md](./roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md)
- [roadmap-tonight-02c-fallback-rollback-and-real-input-continuity.md](./roadmap-tonight-02c-fallback-rollback-and-real-input-continuity.md)

## 현재 상태

- 현재 `PEETSFEA_SLOTS_PER_JOB=5`다.
- 현재 `PEETSFEA_WORKER_BUNDLE_MULTIPLIER=1`이라 worker당 사실상 `5개 입력`만 소비한다.
- 실행 시간이 긴 케이스 하나가 남으면 빠른 슬롯이 먼저 비고, 그 뒤 새 입력을 받지 못한다.
- 결과적으로 `idle slot`이 생기고 throughput이 약 `50 pt/h` 수준에 머문다.
- 현재 CSV integrity 회귀가 있어, 입력 파라미터가 빠진 상태에서는 throughput 상승보다 extractor 복구가 먼저다.

## 핵심 변경

- 오늘밤의 핵심 throughput knob은 `PEETSFEA_WORKER_BUNDLE_MULTIPLIER`다.
- 기본 목표값은 `4`다.
- 불안정하면 fallback 값은 `2`다.
- multiplier는 슬롯 수를 늘리는 기능이 아니라, worker당 prefetch 입력 수를 늘리는 기능으로 정의한다.
- 오늘밤은 완전한 `worker-owned refill` 대신 이 경로를 사용한다.
- multiplier 적용 전제 조건은 CSV schema gate가 green인 것이다.

운영 계약은 다음과 같다.

- `slots_per_job`는 `5`로 유지한다.
- `worker_bundle_multiplier=4`면 worker 하나가 최대 `20개` 입력을 prefetch 대상으로 가진다.
- 병렬 실행 슬롯 수는 여전히 `5`다.
- 먼저 끝난 슬롯은 같은 worker 내부의 남은 입력을 이어받을 수 있어야 한다.
- `02:00` 창에서 multiplier를 올리고, `06:00` 창에서 최종 값을 고정한다.
- CSV integrity가 green이 아니면 `02:00` 창에서도 multiplier를 올리지 않는다.

## 운영 절차

throughput 변경 절차는 아래 순서로 문서화한다.

1. `04` 문서의 CSV schema gate green 확인
2. `02:00` 창에서 `worker_bundle_multiplier=4` 적용
3. 별도 validation lane에서 `10분 sample canary`
4. real `.aedt` 복귀
5. `rolling 30m/60m throughput`, `idle slots`, `success_slots/failed_slots` 확인
6. 불안정 시 `worker_bundle_multiplier=2`로 fallback
7. `06:00` 창에서 최종 값 재고정

관측 기준은 아래와 같이 둔다.

- throughput 상승이 없고 `idle slots`가 높으면 prefetch가 기대만큼 작동하지 않는 것으로 본다.
- throughput이 오르되 `failed_slots`가 급증하면 multiplier가 과한 것으로 본다.
- canary는 안정적이지만 real에서만 실패하면 입력 특성 또는 자원 압박을 같이 본다.
- CSV schema gate가 red면 throughput 해석을 보류하고 extractor 회귀를 먼저 본다.

## 테스트/수용 기준

- `worker_bundle_multiplier>1` 회귀 테스트
- CSV gate green 전 multiplier 적용 금지 검증
- `slots_per_job=5`를 유지한 채 worker당 prefetch 증가 확인
- `idle slot` 감소 확인
- `02:00` 이후 throughput 상승 확인
- fallback `4 -> 2`가 문서상 명확함

수용 기준은 다음과 같다.

- multiplier가 오늘밤의 1차 throughput 레버로 명확히 정의된다.
- CSV integrity 복구가 throughput 변경의 선행 조건으로 분명하다.
- `worker-owned refill`과 `prefetch`의 차이가 혼동 없이 설명된다.
- 운영자가 `02:00`과 `06:00`에서 무엇을 바꾸고 무엇을 보는지 바로 알 수 있다.

## 참고 문서

- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)
- [ops-stabilization-02b-slot-pool-and-worker-owned-refill.md](./archives/ops-stabilization/ops-stabilization-02b-slot-pool-and-worker-owned-refill.md)
