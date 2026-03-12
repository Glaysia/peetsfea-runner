# Roadmap Tonight 03C: Overnight Monitoring Alerting and No-Go

## 목적

이 문서는 tonight 운영 중 어떤 신호를 어떤 우선순위로 보고 no-go를 판단할지 정리한다.
목표는 `00:00`부터 `2026-03-13 19:00 KST`까지 로그와 status만으로 운영 판단이 가능하도록 만드는 것이다.

## 현재 상태

- tonight에는 CSV gate, restart gate, throughput, bad-node가 동시에 움직인다.
- 이 네 신호를 같은 우선순위로 보면 대응 순서가 흔들린다.

## 핵심 변경

- alert triage 우선순위는 아래로 고정한다.
  1. CSV gate red
  2. restart/rediscovery red
  3. bad-node/resource red
  4. throughput 정체
- 창별 관측 항목을 아래처럼 고정한다.

| 창 | 필수 관측 |
| --- | --- |
| `00:00` | CSV schema, restart rediscovery, tunnel 상태 |
| `02:00` | CSV schema, multiplier 적용 후 idle slot, rolling throughput |
| `04:00` | bad-node, `/tmp` free, observed node, failed slots |
| `06:00` | CSV regression 재확인, throughput 추세, bad-node 누적 상태 |

- `06:00` 이후 `19:00`까지는 아래를 계속 본다.
  - `rolling 60m throughput`
  - `idle slots`
  - `failed_slots`
  - `tunnel stale`
  - `No space left on device`

- event/stage 해석은 아래처럼 고정한다.
  - CSV green: `CANARY_PASSED`
  - CSV red: `CANARY_FAILED` with `reason=output_variables_csv_schema_invalid|output_variables_csv_missing|materialized_output_missing`
  - restart green: `WORKER_LOOP_OK` 또는 `WORKER_LOOP_IDLE`
  - restart red: `WORKER_LOOP_ERROR`, `WORKER_LOOP_BLOCKED`
  - degraded but not immediate no-go: `CONTROL_TUNNEL_DEGRADED`, `WORKER_LOOP_RECOVERING`

## 운영 절차

1. 창이 열리면 우선 CSV gate를 본다.
2. 그다음 restart/rediscovery를 본다.
3. 둘 다 green이면 bad-node/resource를 본다.
4. 마지막에 throughput을 본다.
5. red가 나온 최초 상위 workstream이 그 창의 우선 작업이다.

no-go 판단은 아래로 고정한다.

- CSV gate red: 즉시 no-go
- restart/rediscovery red: 즉시 no-go
- bad-node red: conditional no-go, 차단/회피 후 재확인
- throughput 정체만 존재: no-go 아님, `02` workstream 조정 대상

운영자가 보는 최소 신호 묶음은 아래와 같다.

1. validation lane 요약의 `canary_gate=ok` 또는 failure reason
2. `CANARY_PASSED` / `CANARY_FAILED`
3. user service의 `WORKER_LOOP_OK` / `WORKER_LOOP_IDLE` / `WORKER_LOOP_ERROR` / `WORKER_LOOP_BLOCKED`
4. `SLURM_TRUTH_REFRESHED`
5. `CONTROL_TUNNEL_DEGRADED` 유무

## 테스트/수용 기준

- 운영자가 로그와 status만으로 no-go를 판단할 수 있다.
- 창별 필수 관측 항목이 빠지지 않는다.
- CSV/red, bad-node, throughput 정체의 우선순위가 분명하다.

수용 기준은 다음과 같다.

- tonight 운영 판단이 창마다 흔들리지 않는다.
- `06:00` 이후 장시간 관측 항목이 명확하다.

## 참고 문서

- [roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md](./roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md)
- [roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md](./roadmap-tonight-02b-idle-slot-throughput-acceptance-and-measurement.md)
- [roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md](./roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md)
