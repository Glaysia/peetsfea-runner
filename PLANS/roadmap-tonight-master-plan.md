# Roadmap Tonight Master Plan

## 목적

이 문서는 `2026-03-12 22:00 KST`부터 `2026-03-13 19:00 KST`까지의 야간 운영 루트 문서다.
오늘밤 active 문서 체계는 `루트 1 + 세부 4`로 두고, 이후 `11`개의 leaf 계획서와 `archive index 1개`를 더해 `1 + 4 + 11 + 1` 구조로 확장한다.

오늘밤의 직접 목표는 다음과 같다.

- `552fdbe` (`2026.03.12.4`) 이후 회귀한 `output_variables.csv` 입력 파라미터 누락을 먼저 복구한다.
- 처리량을 현재 약 `50 pt/h`에서 `2026-03-13 19:00 KST` 전까지 `200 pt/h` 수준으로 끌어올린다.
- `run_pipeline(config)` 단일 진입점을 유지한다.
- 완전한 `worker-owned refill` 전환 대신, 오늘밤은 `CSV integrity`, `drain`, `worker_bundle_multiplier`, `resource-aware bad node 회피`를 우선 적용한다.
- `00:00`, `02:00`, `04:00`, `06:00` 네 개 창을 기준으로 재시작과 운영 판단을 수행한다.

## Active 문서

- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](./roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md](./roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md)
- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)

각 문서의 역할은 다음과 같다.

- `01`: 재시작, `dispatch.mode`, DB 유지/삭제 기준, sample canary, Go/No-Go
- `02`: `worker_bundle_multiplier`, bundle tail 완화, throughput acceptance, fallback
- `03`: node/worker/slot telemetry 활용, `/tmp` 기준, `bad_nodes`, exclude/triage
- `04`: `output_variables.csv` 계약, variation merge, `23컬럼/3컬럼` 회귀 차단, canary schema gate

## 오늘밤 실행 개요

오늘밤의 운영 축은 네 가지로 고정한다.

1. `CSV integrity recovery`
2. `drain` 가능한 재시작
3. `worker_bundle_multiplier` 기반 prefetch 확대
4. `bad node` 및 `/tmp` 부족 회피

창별 기준은 아래와 같다.

| 창 | 핵심 목표 | 기준 문서 |
| --- | --- | --- |
| `00:00` | CSV integrity hotfix, `drain` bootstrap | `04`, `01` |
| `02:00` | `worker_bundle_multiplier=4` 적용 | `02` |
| `04:00` | telemetry 기반 exclude 반영 | `03` |
| `06:00` | final hardening, 재검증, 최종 설정 고정 | `01`, `02`, `03`, `04` |

공통 운영 규칙은 아래와 같다.

- 각 창 직후 `10분 sample canary`를 수행한다.
- canary 통과 시 즉시 real `.aedt` 처리로 복귀한다.
- `worker_bundle_multiplier=4` 적용은 CSV schema gate가 green일 때만 허용한다.
- DB 기본값은 `유지`다.
- DB 삭제는 `schema/state/ingest` 의미 변경이 있을 때만 예외적으로 허용한다.
- 현재 user service는 `PEETSFEA_CONTINUOUS_MODE=true`이므로 service 자체 재기동만으로는 canary가 발동하지 않는다.
- 따라서 tonight의 sample canary는 `run_pipeline(config)` 함수 호출 경로에서 `continuous_mode=False`인 별도 validation run으로 수행하고, green 확인 후 continuous service를 재개하는 절차로 본다.
- 현재 `dispatch.mode` soft drain은 아직 미구현이므로, tonight maintenance의 실제 제어는 user service hard stop fallback을 기준으로 본다.

## 창별 요약

현재 구현된 patch set 기준 창별 실행 요약은 아래와 같다.

| 창 | 코드 반영 범위 | DB 처리 | canary multiplier | Green 신호 | No-Go 조건 | start 후 확인 |
| --- | --- | --- | --- | --- | --- | --- |
| `00:00` | CSV integrity hotfix, canary schema gate, 문서/운영 경계 정리 | 유지 | `1` | `CANARY_PASSED`, `FULL_ROLLOUT_READY`, `reason=ok` | `CANARY_FAILED`, `reason=output_variables_csv_schema_invalid`, `reason=output_variables_csv_missing`, `reason=materialized_output_missing` | `WORKER_LOOP_START`, `WORKER_LOOP_OK` 또는 `WORKER_LOOP_IDLE`, `SLURM_TRUTH_REFRESHED` |
| `02:00` | `worker_bundle_multiplier` 상승 | 유지 | target `4`, fallback `2` | `00:00` green 유지, canary green, idle slot 감소 기대 | CSV gate red 재발, `failed_slots` 급증, throughput 개선 없음 | `WORKER_LOOP_OK`, `failed_slots` 급증 없음 |
| `04:00` | bad-node / scratch/tmpfs 정책 반영 | 유지 | 직전 green 값 유지 | CSV gate green 유지, resource 변경 후 canary green | `No space left on device` 지속, `REMOTE_SCRATCH_HARD_LIMIT`, `RUNTIME_TMPFS_PROBE_FAILED`, canary red, restart red | `WORKER_LOOP_OK`, node 오류 감소 추세 |
| `06:00` | final hardening, threshold/logging, 최종 값 고정 | 유지 | `4` 또는 안정 fallback 값 | CSV regression 없음, canary green, restart green | `WORKER_LOOP_ERROR`, CSV red 재발, restart red | `WORKER_LOOP_OK/IDLE`, `WORKER_LOOP_ERROR` 부재 |

주의사항은 아래와 같다.

- 현재 구현 범위에서는 `schema/state/ingest` 의미 변경이 없으므로 DB 기본값은 전 창 `유지`다.
- 이후 실제 코드가 state schema를 바꾸면 위 표보다 DB 삭제 규칙이 우선한다.
- canary는 항상 live service와 분리된 validation lane에서 수행한다.

## 문서 트리

현재 active 트리는 아래 `5개`다.

- `roadmap-tonight-master-plan.md`
- `roadmap-tonight-01-drain-and-restart.md`
- `roadmap-tonight-02-bundle-throughput-and-prefetch.md`
- `roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md`
- `roadmap-tonight-04-csv-integrity-and-output-contract.md`

leaf 문서 `11개`는 아래 구조로 정리한다.

- `01a`, `01b`, `01c`
- `02a`, `02b`, `02c`
- `03a`, `03b`, `03c`
- `04a`, `04b`

archive 진입점 문서는 [archives/README.md](./archives/README.md)로 고정한다.

## 수용 기준

오늘밤 루트 문서 기준 수용 조건은 다음과 같다.

- active 루트에서 세부 4장으로 바로 이동할 수 있다.
- archive 문서는 실행 기준이 아니라 참고 기록임이 분명하다.
- `1 + 4 + 11 + 1` 구조가 문서상 일관되게 표현된다.
- CSV integrity가 throughput보다 먼저 gate로 배치되어 있다.
- 각 세부 문서는 `목적`, `현재 상태`, `핵심 변경`, `운영 절차`, `테스트/수용 기준`, `참고 문서` 구조를 따른다.

## Archive

기존 `ops-stabilization-*` 문서는 active 실행 기준에서 제외하고 archive로 이동했다.
archive 진입점과 목록은 [archives/README.md](./archives/README.md)에서 관리한다.
