# Roadmap Tonight 02B: Idle Slot Throughput Acceptance and Measurement

## 목적

이 문서는 tonight throughput의 측정 기준과 acceptance 조건을 고정한다.
목표는 “빨라진 것 같은 느낌”이 아니라 `idle slot`과 시간당 유효 완료량으로 승패를 판단하는 것이다.

## 현재 상태

- 현재 baseline은 약 `50 pt/h`다.
- bundle tail 때문에 idle slot이 구조적으로 생긴다.
- CSV integrity 회귀가 해결되기 전에는 success 수치만으로 throughput을 해석하면 과대평가될 수 있다.

## 핵심 변경

- tonight throughput의 기본 numerator는 `유효 CSV를 가진 완료 슬롯`으로 둔다.
- 측정 창은 `rolling 30m`, `rolling 60m` 두 개를 사용한다.
- 운영 지표는 아래를 고정한다.
  - `rolling 30m throughput`
  - `rolling 60m throughput`
  - `idle slots`
  - `active slots`
  - `success_slots`
  - `failed_slots`

판정 기준은 아래와 같이 둔다.

| 상태 | 기준 |
| --- | --- |
| 상승 | `rolling 60m throughput`이 baseline 대비 명확히 증가하고 `idle slots`가 감소 |
| 정체 | throughput 변화가 작고 `idle slots`가 계속 높음 |
| 과부하 | throughput 상승보다 `failed_slots` 또는 resource 경고 증가가 큼 |

운영 목표는 아래로 고정한다.

- `02:00` 이후: baseline 대비 뚜렷한 상승 확인
- `06:00` 이후: sustained 상승 추세 확인
- `2026-03-13 19:00 KST` 전: `rolling 60m throughput ~= 200 pt/h` 목표

## 운영 절차

1. CSV gate green 상태에서 throughput 측정을 시작한다.
2. `02:00` 창 이후 `rolling 30m/60m throughput`을 다시 본다.
3. 같은 시점의 `idle slots`와 같이 해석한다.
4. `failed_slots`가 늘면 multiplier 또는 resource 문제를 먼저 의심한다.
5. `06:00` 창에서 목표 도달 가능성을 다시 판정한다.

## 테스트/수용 기준

- `idle slot` 감소가 throughput 해석에 포함된다.
- `rolling 30m/60m throughput` 두 창이 모두 문서상 고정된다.
- CSV gate green 이후의 완료량만 유효 throughput으로 본다.
- `200 pt/h` 목표가 시간 축과 함께 명시된다.

수용 기준은 다음과 같다.

- 운영자가 상승/정체/과부하를 숫자로 나눠 볼 수 있다.
- throughput acceptance가 다른 workstream과 충돌하지 않는다.

## 참고 문서

- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](./roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md](./roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md)
- [roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md](./roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md)
