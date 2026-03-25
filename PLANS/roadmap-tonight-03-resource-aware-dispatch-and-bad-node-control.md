# Roadmap Tonight 03: Resource-Aware Dispatch and Bad Node Control

## 목적

이 문서는 이미 수집 중인 자원 telemetry를 tonight dispatch 판단과 node 회피에 연결하는 운영 계획을 정의한다.
핵심은 GPFS scratch 사용량, `No space left on device`, slot tmpfs probe, observed node, tunnel 상태를 triage와 exclude 계약에 반영하는 것이다.

## Leaf 문서

- [roadmap-tonight-03a-telemetry-ingestion-and-thresholds.md](./roadmap-tonight-03a-telemetry-ingestion-and-thresholds.md)
- [roadmap-tonight-03b-bad-node-quarantine-and-exclude-policy.md](./roadmap-tonight-03b-bad-node-quarantine-and-exclude-policy.md)
- [roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md](./roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md)

## 현재 상태

- node, worker, slot telemetry는 이미 수집된다.
- 하지만 dispatch 판단은 현재 이 telemetry를 거의 쓰지 않는다.
- 최근 운영 로그에는 `No space left on device`, `capacity query timed out`, `tunnel stale` 신호가 보였다.
- 특정 node가 나빠도 현재는 account 또는 worker 관점으로만 보기 쉬워 node 단위 회피가 약하다.
- 최근 `output_variables.csv` 회귀는 resource 문제와 별개로 extractor 계약 문제이므로 같은 bucket으로 다루면 안 된다.

## 핵심 변경

- `bad_nodes` 또는 동등한 node exclude 집합을 tonight 운영 개념으로 도입한다.
- 차단 기준은 다음 두 가지를 기본으로 둔다.
  - `No space left on device`
  - `scratch_usage_mb >= 90 GiB` 또는 `tmpfs probe failed`
- node 차단 기준은 observed node를 기준으로 적용한다.
- tonight 문서에서는 최초 차단 기간을 `8시간`으로 둔다.
- `tunnel stale`은 바로 bad-node로 보지 않고 control-plane 문제로 분리해서 본다.
- `CSV schema invalid`와 `23컬럼/3컬럼` CSV는 bad-node 신호가 아니라 extractor 회귀 신호로 분리한다.

운영 계약은 다음과 같다.

- `No space left on device`가 확인되면 해당 node는 즉시 exclude 후보가 된다.
- `scratch_usage_mb >= 90 GiB` 또는 `tmpfs probe failed`가 확인되면 해당 account/runtime은 dispatch 차단 후보가 된다.
- exclude된 node는 cooldown 동안 신규 dispatch 대상에서 제거한다.
- cooldown 만료 후에는 재평가 후보로 돌린다.

## 운영 절차

resource-aware triage는 아래 순서로 문서화한다.

1. node/worker/slot telemetry 확인
2. `No space left on device`, scratch hard-limit, tmpfs probe signal 확인
3. CSV schema invalid 여부를 별도 분류
4. observed node 기준으로 `bad_nodes` 반영
5. 별도 validation lane에서 `10분 sample canary`
6. real `.aedt` 복귀 후 `rolling throughput`, `failed_slots`, node 오류 추이 확인
7. cooldown 만료 전까지 신규 dispatch에서 해당 node 제외

운영 지표는 아래를 우선 본다.

- `scratch usage`
- `tmpfs probe`
- `No space left on device`
- `observed_node`
- `failed_slots`
- `rolling 30m throughput`
- `rolling 60m throughput`
- `tunnel stale`

## 테스트/수용 기준

- `No space left on device` 감지 시 node 차단 흐름 설명 가능
- scratch hard-limit / tmpfs probe 차단 규칙 명시
- `tunnel stale`를 bad-node와 분리 판단
- `CSV schema invalid`를 resource/bad-node와 분리 판단
- bad-node 적용 후 real 처리 지속성 확인
- cooldown `8시간`과 재평가 경계가 문서상 명확함

수용 기준은 다음과 같다.

- telemetry가 단순 관측이 아니라 dispatch 판단으로 연결된다.
- node 단위 triage와 account/worker 단위 triage가 구분된다.
- extractor 회귀와 bad-node triage가 혼동 없이 분리된다.
- 운영자가 `04:00` 창에서 볼 지표와 내릴 판단을 즉시 이해할 수 있다.

## 참고 문서

- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)
- [ops-stabilization-03a-resource-telemetry-and-snapshots.md](./archives/ops-stabilization/ops-stabilization-03a-resource-telemetry-and-snapshots.md)
