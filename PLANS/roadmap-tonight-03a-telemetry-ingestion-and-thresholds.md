# Roadmap Tonight 03A: Telemetry Ingestion and Thresholds

## 목적

이 문서는 이미 수집 중인 telemetry를 tonight 운영 지표로 정리한다.
목표는 node/worker/slot snapshot을 “보는 것”에서 끝내지 않고, threshold와 action으로 연결하는 것이다.

## 현재 상태

- runner는 이미 node, worker, slot resource snapshot을 수집한다.
- node snapshot에는 `allocated_mem_mb`, `total_mem_mb`, `used_mem_mb`, `free_mem_mb`, `load_1/5/15`, `tmp_total_mb`, `tmp_used_mb`, `tmp_free_mb`, `process_count`, `running_worker_count`, `active_slot_count`가 있다.
- worker snapshot에는 `configured_slots`, `active_slots`, `idle_slots`, `rss_mb`, `cpu_pct`, `tunnel_state`, `process_count`가 있다.
- slot snapshot에는 `allocated_mem_mb`, `used_mem_mb`, `load_1`, `rss_mb`, `cpu_pct`, `process_count`, `active_process_count`, `artifact_bytes`, `state`가 있다.
- account readiness snapshot에는 `scratch_root`, `scratch_usage_mb`, tmpfs probe 결과가 들어간다.

## 핵심 변경

- tonight 기준 핵심 metric은 아래로 고정한다.
  - node: `free_mem_mb`, `load_1`, `active_slot_count`
  - account/runtime: `scratch_usage_mb`, `tmpfs probe`, `storage_ready`
  - worker: `active_slots`, `idle_slots`, `tunnel_state`, `rss_mb`
  - slot: `used_mem_mb`, `artifact_bytes`, `state`
- threshold는 아래처럼 사용한다.
  - `scratch_usage_mb >= 80 GiB`: warning/janitor
  - `scratch_usage_mb >= 90 GiB`: 신규 dispatch 차단
  - `tmpfs probe failed`: account 차단
  - tunnel heartbeat timeout: `90s`
  - tunnel recovery grace: `30s`
  - `idle_slots` 높음: throughput 정체 후보

metric-to-action 매핑은 아래로 고정한다.

| metric | action |
| --- | --- |
| `scratch_usage_mb >= 81920` | `03b` scratch pressure warning |
| `scratch_usage_mb >= 92160` | dispatch/canary block |
| `tmpfs probe failed` | account runtime block |
| `tunnel_state=DEGRADED` 또는 stale heartbeat | control-plane triage |
| `idle_slots` 지속 증가 | `02` throughput triage |
| CSV schema invalid | `04` extractor triage |

## 운영 절차

1. latest account/runtime snapshot을 먼저 본다.
2. latest worker mix에서 `active_slots`와 `idle_slots`를 본다.
3. tunnel 상태와 heartbeat를 확인한다.
4. slot snapshot에서 진행 중 artifact와 메모리 사용을 본다.
5. metric을 위 action 표에 따라 분류한다.

## 테스트/수용 기준

- node/worker/slot별 핵심 metric이 문서상 구분된다.
- scratch usage/tmpfs, tunnel, idle slot, CSV gate가 서로 다른 action으로 연결된다.
- threshold 숫자가 문서에 명시된다.

수용 기준은 다음과 같다.

- 운영자가 snapshot을 보고 바로 어떤 workstream으로 넘길지 판단할 수 있다.
- resource 문제와 extractor 문제를 같은 bucket으로 다루지 않는다.

## 참고 문서

- [roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md](./roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md)
- [roadmap-tonight-03b-bad-node-quarantine-and-exclude-policy.md](./roadmap-tonight-03b-bad-node-quarantine-and-exclude-policy.md)
- [roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md](./roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md)
