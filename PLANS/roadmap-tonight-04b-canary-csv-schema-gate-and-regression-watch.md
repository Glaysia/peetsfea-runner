# Roadmap Tonight 04B: Canary CSV Schema Gate and Regression Watch

## 목적

이 문서는 canary가 CSV 회귀를 놓치지 않도록 gate를 재정의한다.
목표는 `output_variables.csv`가 “존재하는가”가 아니라 “계약대로 생성됐는가”를 기준으로 tonight cutover를 통과시키는 것이다.

## 현재 상태

- 현재 canary gate는 [`peetsfea_runner/pipeline.py`](../peetsfea_runner/pipeline.py)에서 `output_variables.csv` 존재 여부만 본다.
- 이 기준으로는 결과-only `23컬럼` CSV도 pass처럼 보일 수 있다.
- path-only `3컬럼` CSV도 파일 자체는 있으므로 누락 회귀를 놓칠 수 있다.

## 핵심 변경

- canary gate는 `output_variables_csv_missing` 외에 `output_variables_csv_schema_invalid`를 가진다.
- `output_variables_csv_schema_invalid`는 아래 경우에 발생한다.
  - source 3개 컬럼이 빠짐
  - input sentinel 중 하나라도 빠짐
  - output sentinel 중 하나라도 빠짐
  - CSV가 결과-only `23컬럼`
  - CSV가 path-only `3컬럼`

sentinel 계약은 아래로 고정한다.

- source
  - `source_aedt_path`
  - `source_case_dir`
  - `source_aedt_name`
- input
  - `coil_groups_0__count_mode`
  - `coil_shape_inner_margin_x`
  - `coil_spacing_tx_vertical_center_gap_mm`
  - `ferrite_present`
  - `tx_region_z_parts_dd_z_mm`
- output
  - `k_ratio`
  - `Lrx_uH`

컬럼 개수는 참고 신호로 쓰되 최종 판정은 sentinel 기반으로 고정한다.

## 운영 절차

1. sample canary가 끝나면 validation lane output root 아래에서 `output_variables.csv`를 찾는다.
2. CSV가 없으면 `output_variables_csv_missing`으로 fail한다.
3. CSV가 있으면 header를 읽어 sentinel 존재 여부를 확인한다.
4. sentinel이 모두 있으면 canary pass다.
5. sentinel이 빠졌으면 `output_variables_csv_schema_invalid`로 fail한다.
6. canary가 fail이면 real `.aedt` 복귀를 금지하고 다음 창 전까지 원인을 고정한다.

validation lane 경로는 아래처럼 고정한다.

- output root
  - `/home/peetsmain/peetsfea-runner/tmp/tonight-canary/<window>/output`
- CSV 탐색 기준
  - `output_root/**/*.aedt.out/output_variables.csv`
- live continuous service output은 canary schema 판정에 섞지 않는다.

regression watch 규칙은 아래와 같다.

- `00:00` 창: hotfix 직후 첫 schema pass 확인
- `02:00` 창: multiplier 적용 전 schema green 재확인
- `04:00` 창: bad-node 작업 이후 schema red로 바뀌지 않았는지 확인
- `06:00` 창: final hardening 후 schema regression 재점검

## 테스트/수용 기준

- valid CSV는 canary pass
- 결과-only `23컬럼` CSV는 canary fail
- path-only `3컬럼` CSV는 canary fail
- sentinel 중 일부만 있는 CSV는 canary fail
- `output_variables_csv_missing`와 `output_variables_csv_schema_invalid`가 구분된다

수용 기준은 다음과 같다.

- tonight canary가 CSV 회귀를 다시 놓치지 않는다.
- `04` workstream이 `02` workstream의 선행 조건으로 분명하다.
- 운영자가 fail 원인을 `missing`과 `schema invalid`로 분리해 본다.

## 참고 문서

- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)
- [roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md](./roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md)
- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
