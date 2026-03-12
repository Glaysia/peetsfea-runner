# Roadmap Tonight 01: Drain and Restart

## 목적

이 문서는 오늘밤 재시작을 안전하게 수행하기 위한 운영 모델을 정의한다.
핵심은 `dispatch.mode=run|drain`, restart window contract, DB retention matrix, `10분 sample canary`, CSV schema gate, Go/No-Go를 고정하는 것이다.

## Leaf 문서

- [roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md](./roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md)
- [roadmap-tonight-01b-restart-rediscovery-and-db-retention-policy.md](./roadmap-tonight-01b-restart-rediscovery-and-db-retention-policy.md)
- [roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md](./roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md)

## 현재 상태

- 현재 live 운영은 real `.aedt` 처리 중이다.
- restart 이후 worker 상태 복구는 DB와 slurm rediscovery에 의존한다.
- 거친 `서비스 stop 시 항상 DB 삭제` 규칙은 restart-safe 운영과 충돌할 수 있다.
- 현재 canary 판단은 `output_variables.csv` 존재 여부만으로는 부족하고, 최근에는 결과-only `23컬럼` CSV 회귀를 놓칠 수 있다.
- 오늘밤은 `00:00`, `02:00`, `04:00`, `06:00` 네 개 창을 기준으로 움직인다.
- 현재 user service는 `continuous_mode=true`라서 service 재기동만으로는 `sample.aedt` canary candidate가 되지 않는다.
- 현재 `dispatch.mode` soft drain은 아직 미구현이라 tonight 실제 maintenance는 hard stop fallback으로 운영한다.

## 핵심 변경

- `dispatch.mode = run|drain`를 오늘밤 운영 제어점으로 도입한다.
- `drain`은 새 bundle 제출만 멈추고 inflight worker는 정상 종료까지 흘려보내는 의미로 고정한다.
- DB는 런타임 상태 저장소로 취급하고, 기본값을 `유지`로 둔다.
- DB 삭제는 `schema/state/ingest` 의미 변경이 있을 때만 허용한다.
- 각 창 직후 `10분 sample canary`를 수행하고, CSV schema gate 통과 시에만 real `.aedt`로 즉시 복귀한다.
- 이 canary는 continuous user service 안에서 자동으로 발생하지 않으므로, `run_pipeline(config)`를 `continuous_mode=False`로 호출하는 별도 validation lane으로 수행한다.
- canary는 `CSV exists`가 아니라 `CSV schema valid` 기준으로 판정한다.
- 새 canary failure bucket으로 `output_variables_csv_schema_invalid`를 둔다.

DB retention matrix는 아래와 같이 고정한다.

| 변경 유형 | DB 처리 |
| --- | --- |
| restart-safe 운영 변경 | 유지 |
| `dispatch.mode` 추가 | 유지 |
| telemetry/GUI/관측 추가 | 유지 |
| throughput knob 조정 | 유지 |
| schema 변경 | 삭제 |
| state 의미 변경 | 삭제 |
| ingest 의미 변경 | 삭제 |

## 운영 절차

모든 창은 같은 절차를 따른다.

1. 사전 확인
2. `dispatch.mode=drain` 전환 가능 여부 확인
3. service stop/restart
4. 별도 validation lane에서 `10분 sample canary`
5. CSV schema gate 통과 시 real `.aedt` 복귀
6. 실패 시 hold 또는 fallback

창별 강조점은 다음과 같다.

- `00:00`: `drain` bootstrap과 restart-safe 경계 정리
- `02:00`: throughput 상승 적용 전 restart 안전성 재확인
- `04:00`: bad-node 차단 정책을 걸기 전 control flow 유지 확인
- `06:00`: 최종 threshold/hardening 반영 후 안정화

sample canary contract는 아래와 같이 고정한다.

- 입력은 `examples/sample.aedt` 복제본만 사용한다.
- 시간은 `10분`으로 고정한다.
- canary는 validation lane이며 real backlog와 구분한다.
- canary는 current continuous user service가 아니라 함수 호출 기반 one-shot validation run으로 수행한다.
- canary에서 구조적 오류 또는 CSV schema invalid가 보이면 real 복귀를 금지한다.
- CSV gate는 아래 sentinel을 모두 본다.
  - source: `source_aedt_path`, `source_case_dir`, `source_aedt_name`
  - input: `coil_groups_0__count_mode`, `coil_shape_inner_margin_x`, `coil_spacing_tx_vertical_center_gap_mm`, `ferrite_present`, `tx_region_z_parts_dd_z_mm`
  - output: `k_ratio`, `Lrx_uH`
- 결과-only `23컬럼` CSV와 path-only `3컬럼` CSV는 모두 canary fail이다.

Go/No-Go 기준은 다음과 같다.

- `Go`: runner crash 없음, rediscovery 유지, sample materialize 성공, CSV schema valid, `failed_slots` 급증 없음
- `No-Go`: rediscovery 붕괴, sample 구조적 실패, `output_variables_csv_schema_invalid`, tunnel/worker 상태 붕괴, real 전환 후 즉시 재실패

## 테스트/수용 기준

- restart 후 slurm worker rediscovery 유지
- `drain` 상태에서 신규 bundle 정지
- `run` 복귀 후 dispatch 재개
- `23컬럼`/`3컬럼` CSV가 canary fail로 분류됨
- sample canary 실패 시 real 차단
- DB 유지 재시작과 DB 삭제 재시작의 판단 경계가 문서상 명확함

수용 기준은 다음과 같다.

- 재시작 절차가 네 개 창에 대해 동일한 문장으로 적용된다.
- DB 삭제가 예외 규칙으로 명확히 제한된다.
- canary와 real 전환 경계가 운영자가 바로 이해할 수 있다.
- CSV schema gate가 `00:00` 창의 first gate로 분명하다.

## 참고 문서

- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)
- [archives/README.md](./archives/README.md)
- [ops-stabilization-03c-night-operations-rollout-and-service-boundary.md](./archives/ops-stabilization/ops-stabilization-03c-night-operations-rollout-and-service-boundary.md)
