# Roadmap Tonight 04A: Output Variables Extraction and Variation Merge

## 목적

이 문서는 `552fdbe` 회귀의 직접 원인을 고정하고, tonight extractor hotfix의 구현 계약을 정의한다.
목표는 `output_variables.csv`가 다시 `source + input parameter + output variable`을 함께 담도록 만드는 것이다.

## 현재 상태

- 현재 remote worker는 [`peetsfea_runner/remote_job.py`](../peetsfea_runner/remote_job.py) 안에서 `reopen_solved_project + get_solution_data` 경로로 CSV를 만든다.
- `2026.03.12.4` 전에는 `create_report(..., plot_type='Data Table') -> export_report_to_csv(...)` 경로를 사용했고, 이 경로가 variation/input parameter 컬럼을 같이 내보냈다.
- `2026.03.12.4`에서 report export가 제거되며 `hfss.available_variations.nominal_values`를 output query 인자로만 넘기고 CSV row에는 합치지 않게 되었다.
- 그 결과 solve는 성공해도 CSV는 결과-only `23컬럼`으로 떨어졌다.

## 핵심 변경

- `get_solution_data` 경로는 유지한다.
- `export_report_to_csv` 경로로 wholesale revert하지 않는다.
- nominal variation merge를 별도 helper로 다시 도입한다.
- helper는 `hfss.available_variations.nominal_values`에서 source row에 넣을 입력 파라미터만 뽑는다.
- `Freq`는 variation merge 대상에서 제외한다.
- scalar/list/numpy scalar는 CSV 한 칸에 안정적으로 직렬화한다.
- output variable이 비어도 source + variation은 남긴다.

구현 계약은 아래와 같이 고정한다.

- source 컬럼은 항상 먼저 쓴다.
  - `source_aedt_path`
  - `source_case_dir`
  - `source_aedt_name`
- variation 컬럼은 source 뒤에 붙인다.
- output variable 컬럼은 variation 뒤에 붙인다.
- key 충돌 시 source > variation > output 순서를 유지한다.
- output variable과 variation이 같은 이름을 쓰면 variation 키를 보존하는 별도 이름 변경은 하지 않는다.
  - tonight 범위에서는 입력 파라미터 키와 output variable 키가 겹치지 않는다는 현재 계약을 전제로 둔다.

## 운영 절차

1. remote script builder에서 variation flatten helper를 추가한다.
2. `extract_output_variables_csv(...)` 초반에 source row를 만든다.
3. `hfss.available_variations.nominal_values`를 flatten해 row에 merge한다.
4. 그 뒤 `get_solution_data(...)`로 output variable 값을 merge한다.
5. `hfss.output_variables`가 비어도 source + variation CSV는 쓰고 종료한다.
6. `output_variables.error.log`는 output variable 추출 실패일 때만 남긴다.

reopen 경로 계약은 아래로 고정한다.

- solve 단계는 현재처럼 `solve_in_batch(...)`를 사용한다.
- solve 직후 `save_project()`는 best effort로 둔다.
- CSV 추출은 solved project reopen 후 수행한다.
- reopen 경로 자체는 바꾸지 않는다.

## 테스트/수용 기준

- variation merge helper가 `Freq`를 제외한다.
- `hfss.available_variations.nominal_values`의 scalar/list 값을 안정적으로 row value로 만든다.
- source/input/output 컬럼이 한 CSV에 공존한다.
- `get_solution_data` 경로는 유지되고 `export_report_to_csv`는 복귀하지 않는다.
- output variable이 비어도 source + variation CSV는 남는다.

수용 기준은 다음과 같다.

- 새로 생성된 CSV가 더 이상 결과-only `23컬럼`으로 떨어지지 않는다.
- 입력 파라미터가 있는 프로젝트는 variation 컬럼이 다시 CSV에 보인다.
- extractor hotfix가 `2026.03.12.4`의 solve 안정화 의도를 깨지 않는다.

## 참고 문서

- [roadmap-tonight-04-csv-integrity-and-output-contract.md](./roadmap-tonight-04-csv-integrity-and-output-contract.md)
- [roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md](./roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md)
- [roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md](./roadmap-tonight-02a-bundle-packing-and-worker-bundle-multiplier-contract.md)
