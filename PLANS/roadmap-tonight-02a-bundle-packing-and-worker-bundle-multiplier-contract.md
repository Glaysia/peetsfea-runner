# Roadmap Tonight 02A: Bundle Packing and Worker Bundle Multiplier Contract

## 목적

이 문서는 tonight throughput knob의 계약을 고정한다.
목표는 `worker_bundle_multiplier`가 어떤 의미로 bundle 크기를 늘리고, 무엇을 바꾸지 않는지 명확히 하는 것이다.

## 현재 상태

- service 설정은 현재 `PEETSFEA_SLOTS_PER_JOB=5`, `PEETSFEA_WORKER_BUNDLE_MULTIPLIER=1`이다.
- 현재 `_worker_bundle_slot_limit(...)`는 slurm_batch에서 `slots_per_job * worker_bundle_multiplier`를 bundle 상한으로 계산한다.
- worker 내부 병렬 실행 슬롯 수는 여전히 `slots_per_job`이다.

## 핵심 변경

- tonight 기본값은 `worker_bundle_multiplier=4`다.
- fallback 값은 `2`다.
- `slots_per_job=5`는 유지한다.
- 따라서 multiplier `4`일 때 worker 1개는 최대 `20`개 입력을 prefetch 대상으로 가진다.
- 이 변경은 슬롯 수를 늘리지 않는다.
- 이 변경은 완전한 worker-owned refill을 도입하지 않는다.
- 이 변경은 CSV schema gate가 green일 때만 적용한다.

## 운영 절차

1. `04b`의 CSV gate green 확인
2. `02:00` 창에서 `worker_bundle_multiplier=4` 적용
3. sample canary 수행
4. canary와 real에서 idle slot 감소 여부 확인
5. 불안정하면 `2`로 fallback
6. `06:00` 창에서 최종 값 고정

변경하지 않는 항목은 아래와 같다.

- `slots_per_job`
- `cores_per_slot`
- `cpus_per_job`
- `mem`

## 테스트/수용 기준

- multiplier 적용 후 worker당 prefetch 입력 수가 늘어난다.
- slot 병렬도는 계속 `5`다.
- CSV gate red 상태에서는 multiplier가 적용되지 않는다.
- `4 -> 2` fallback 경로가 문서상 명확하다.

수용 기준은 다음과 같다.

- multiplier가 tonight prefetch 모델의 핵심 knob으로 오해 없이 정의된다.
- worker-owned refill과 tonight prefetch 모델의 차이가 분명하다.

## 참고 문서

- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](./roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md](./roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md)
- [roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md](./roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md)
