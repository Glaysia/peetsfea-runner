# 06-05: Self Debug Execution Plan (직접 디버그 계획)

## 목적

1. 구현 후 내가 직접 재현 가능한 방식으로 디버그한다.
2. `.vscode/launch.json` 기준과 `pdb` 기준을 모두 충족한다.

## 디버그 실행 원칙

1. Python 실행은 저장소 루트 `.venv/bin/python`만 사용.
2. 기본 디버그 방식은 `.vscode/launch.json`의 `Runner: Debug runner.py`.
3. 보조 디버그 방식은 `pdb`:
   - `.venv/bin/python -m pdb runner.py`

## 시나리오

1. 시나리오 A: 다중 잡(10슬롯) 모두 성공.
2. 시나리오 B: 일부 잡 의도적 실패 + 재시도.
3. 시나리오 C: 재시도 소진 후 격리.
4. 시나리오 D: 다운로드 실패 분리 보고.

## 시나리오별 관찰 포인트

1. 공통:
   - 동시 실행 슬롯이 상한(기본 10)을 넘지 않는지.
   - 잡별 8 window 집계가 완료되는지.
   - 잡 상태전이(`PENDING -> SUBMITTED -> RUNNING -> COLLECTING -> SUCCEEDED|FAILED`)가 맞는지.
2. A:
   - 전체 성공 판정.
   - 회수 파일 완전성.
3. B:
   - 실패 잡만 재시도되는지.
   - 실패 잡 정보 summary 반영.
   - 다른 잡 진행이 지속되는지.
4. C:
   - 격리(`QUARANTINED`) 레코드 생성.
   - 재시도 상한 이후 추가 실행 없음.
5. D:
   - download 전용 오류 코드 매핑.
   - 실행 실패와 다운로드 실패 구분.

## 디버그 절차

1. 브레이크포인트 후보:
   - 슬롯 배정 직후
   - 잡 단위 재시도 분기
   - 격리 큐 전이 지점
   - orphan 정리 호출 전/후
2. 로그 확인:
   - `artifacts/<run_id>/<job_id>/case_XX/run.log`
   - `artifacts/<run_id>/<job_id>/case_XX/exit.code`
   - `peetsfea_runner.duckdb` (`runs/jobs/job_events/quarantine_jobs`)
3. 결과 판정:
   - 기대값과 실제값 비교표 작성.

## 완료 기준

1. 4개 시나리오에서 기대 결과와 실제 로그가 일치.
2. 회수 누락 0건.
3. 집계 문자열과 DuckDB 상태가 일관됨.
4. 재현 명령과 절차가 문서만으로 재수행 가능.
