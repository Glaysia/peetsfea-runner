# Roadmap Tonight 02C: Fallback Rollback and Real Input Continuity

## 목적

이 문서는 multiplier 변경이 흔들릴 때의 fallback과 rollback 규칙을 고정한다.
목표는 throughput 실험이 real `.aedt` 연속성을 깨지 않도록 만드는 것이다.

## 현재 상태

- tonight throughput workstream은 `worker_bundle_multiplier=4`를 목표로 한다.
- 하지만 CSV gate, failed slot, resource 압박이 나빠지면 `4`를 유지하는 것이 더 손해일 수 있다.
- sample canary가 green이어도 real 입력에서만 문제가 드러날 수 있다.

## 핵심 변경

- fallback 순서는 `4 -> 2 -> 1`로 고정한다.
- `4 -> 2`는 tonight 기본 fallback이다.
- `2`에서도 문제면 throughput 변경을 철회하고 `1`로 rollback한다.
- rollback은 throughput 문제만으로 하지 않고 아래 조건 중 하나가 있을 때 수행한다.
  - `failed_slots` 급증
  - CSV gate red 재발
  - `/tmp` 또는 bad-node 경고 급증
  - throughput 개선이 없고 idle slot도 줄지 않음

real input continuity 계약은 아래와 같다.

- green canary 뒤에는 real `.aedt`를 재개한다.
- throughput fallback이 발생해도 real `.aedt` 자체를 sample로 되돌리지는 않는다.
- real lane 중단은 CSV gate 또는 restart gate red일 때만 허용한다.

## 운영 절차

1. multiplier `4` 적용 후 real `.aedt`를 관찰한다.
2. 불안정 징후가 보이면 `2`로 fallback한다.
3. `2`에서도 회복되지 않으면 `1`로 rollback한다.
4. fallback/rollback 중에도 CSV gate와 restart gate가 green이면 real 처리 자체는 계속한다.
5. real lane을 멈출지 여부는 `01c` runbook 기준으로 따로 판정한다.

## 테스트/수용 기준

- `4 -> 2 -> 1` fallback/rollback 순서가 명확하다.
- fallback 중에도 real input continuity 원칙이 유지된다.
- sample green / real red 상황의 처리 기준이 문서에 있다.
- rollback 조건이 throughput/CSV/resource 기준으로 분리되어 있다.

수용 기준은 다음과 같다.

- throughput 실험이 운영 연속성을 해치지 않는다.
- 운영자가 multiplier 변경을 되돌릴 때의 조건을 바로 이해할 수 있다.

## 참고 문서

- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](./roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md](./roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md)
- [roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md](./roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md)
