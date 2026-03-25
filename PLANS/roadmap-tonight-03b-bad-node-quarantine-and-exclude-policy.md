# Roadmap Tonight 03B: Bad Node Quarantine and Exclude Policy

## 목적

이 문서는 bad-node를 node 단위로 분류하고 dispatch 제외 정책을 고정한다.
목표는 account나 worker 단위로 뭉뚱그려 보던 장애를 observed node 기준으로 차단하는 것이다.

## 현재 상태

- 최근 운영 로그에는 `No space left on device`가 있었다.
- telemetry에는 scratch usage, tmpfs probe, observed node 정보가 있다.
- 아직 node 단위 exclude 정책은 구현되지 않았다.

## 핵심 변경

- runtime policy 파일 경로는 `tmp/runtime/bad_nodes.json`로 고정한다.
- bad-node 후보 기준은 아래 두 가지다.
  - `No space left on device`
  - `scratch_usage_mb >= 92160` 또는 `tmpfs probe failed`
- 차단 단위는 observed node다.
- 최초 cooldown은 `8시간`이다.
- cooldown 만료 후에는 재평가 후보로 돌린다.

정책 계약은 아래와 같다.

- bad-node 등록은 account 차단이 아니다.
- worker 단위 degraded와 node 단위 quarantine를 구분한다.
- CSV schema invalid만으로는 bad-node 등록을 하지 않는다.
- tunnel stale만으로는 bad-node 등록을 하지 않는다.

## 운영 절차

1. `No space left on device`, scratch hard-limit, tmpfs probe를 확인한다.
2. 관련 worker의 observed node를 찾는다.
3. 해당 node를 `tmp/runtime/bad_nodes.json`에 기록한다.
4. cooldown 동안 신규 dispatch 대상에서 제외한다.
5. `04:00` 창 이후에도 같은 node 경고가 계속되면 cooldown을 갱신한다.
6. cooldown 만료 후 재평가한다.

`bad_nodes.json`의 최소 필드는 아래로 고정한다.

- `node`
- `reason`
- `first_seen_at`
- `expires_at`

## 테스트/수용 기준

- `No space left on device`가 bad-node 분류로 이어진다.
- scratch hard-limit 또는 tmpfs probe failure가 dispatch 차단으로 이어진다.
- observed node 기준 차단이 문서상 명확하다.
- cooldown `8시간`과 재평가 경계가 있다.
- extractor 회귀가 bad-node로 잘못 분류되지 않는다.

수용 기준은 다음과 같다.

- node 단위 quarantine와 account/worker triage가 구분된다.
- 운영자가 차단 기준과 해제 기준을 바로 이해할 수 있다.

## 참고 문서

- [roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md](./roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md)
- [roadmap-tonight-03a-telemetry-ingestion-and-thresholds.md](./roadmap-tonight-03a-telemetry-ingestion-and-thresholds.md)
- [roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md](./roadmap-tonight-03c-overnight-monitoring-alerting-and-no-go.md)
