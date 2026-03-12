# Roadmap Tonight 04: CSV Integrity and Output Contract

## 목적

이 문서는 `output_variables.csv` 계약을 tonight 기준으로 다시 고정하는 계획서다.
핵심은 `552fdbe` 이후 생긴 입력 파라미터 누락 회귀를 막고, canary가 `23컬럼/3컬럼` CSV를 통과시키지 못하게 하는 것이다.

## Leaf 문서

- [roadmap-tonight-04a-output-variables-extraction-and-variation-merge.md](./roadmap-tonight-04a-output-variables-extraction-and-variation-merge.md)
- [roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md](./roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md)

## 현재 상태

- `552fdbe` (`2026.03.12.4`) 이후 `export_report_to_csv` 경로가 제거되고 `get_solution_data` 기반 추출로 바뀌었다.
- 그 과정에서 report export가 암묵적으로 제공하던 variation/input parameter 컬럼 merge가 빠졌다.
- 서비스 재기동 시각 `2026-03-12 19:19:58 KST` 이후 생성된 `output_variables.csv` `187개`는 전부 `23컬럼`이었다.
- 같은 구간의 `48컬럼` 완전행은 `0개`였다.
- 일부 산출물은 `source_*` 3개만 있는 path-only CSV로 떨어진다.

## 핵심 변경

- tonight의 첫 gate는 throughput이 아니라 CSV integrity다.
- `get_solution_data` 경로는 유지한다.
- 대신 `hfss.available_variations.nominal_values`를 CSV row에 명시적으로 merge한다.
- `Freq`는 variation 컬럼에서 제외한다.
- output variable이 없어도 source + variation은 남긴다.
- `23컬럼` 결과-only CSV와 `3컬럼` path-only CSV는 success가 아니라 schema failure로 본다.

운영 계약은 다음과 같다.

- source 컬럼은 항상 유지한다.
  - `source_aedt_path`
  - `source_case_dir`
  - `source_aedt_name`
- 입력 파라미터가 존재하는 프로젝트는 variation merge 후 CSV에 남아야 한다.
- output variable 추출은 현재처럼 `get_solution_data`에서 읽는다.
- canary pass는 `output_variables.csv` 존재만으로 충분하지 않다.
- canary pass는 source + input sentinel + output sentinel이 모두 있어야 한다.

sentinel 기준은 아래와 같이 고정한다.

- input sentinel
  - `coil_groups_0__count_mode`
  - `coil_shape_inner_margin_x`
  - `coil_spacing_tx_vertical_center_gap_mm`
  - `ferrite_present`
  - `tx_region_z_parts_dd_z_mm`
- output sentinel
  - `k_ratio`
  - `Lrx_uH`

## 운영 절차

CSV integrity 절차는 아래 순서로 문서화한다.

1. `00:00` 창에서 extractor hotfix를 first gate로 적용
2. 별도 validation lane에서 `10분 sample canary`
3. canary CSV에서 schema valid 확인
4. schema valid일 때만 real `.aedt` 복귀
5. `02:00` 창의 multiplier 적용 전 CSV gate green 재확인
6. `06:00` 창에서 regression 재점검

분류 규칙은 아래와 같이 둔다.

- `48컬럼` 수준의 source + input + output 포함 행: valid candidate
- `23컬럼` 결과-only 행: schema invalid
- `3컬럼` path-only 행: schema invalid

이번 문서의 범위 제외는 다음과 같다.

- 이미 생성된 CSV의 backfill
- 과거 dataset 복원
- 사용자 후처리 스크립트 작성

## 테스트/수용 기준

- 새 canary CSV가 input sentinel과 output sentinel을 모두 포함
- 결과-only `23컬럼` CSV는 canary fail
- path-only `3컬럼` CSV는 canary fail
- `get_solution_data` 경로는 유지
- variation merge가 `Freq` 없이 적용
- output variable이 비어도 source + variation이 남는 계약이 문서상 명확함

수용 기준은 다음과 같다.

- CSV integrity가 tonight의 first gate로 분명하다.
- `2026.03.12.4` 회귀 원인이 정확히 기록된다.
- extractor 회귀와 bad-node/resource 문제의 경계가 분명하다.
- `02:00` 이후 throughput workstream이 CSV gate green을 전제로 한다.

## 참고 문서

- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](./roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [ops-stabilization-04-full-4x10x10-saturation.md](./archives/ops-stabilization/ops-stabilization-04-full-4x10x10-saturation.md)
