# 07-03: Orphan 정리 + 멀티잡 요약 + 수용 게이트

## 목적

1. 종료 후 잔여 screen/session을 강제 정리한다.
2. `PipelineResult.summary`를 멀티잡 기준으로 표준화한다.
3. ROADMAP 07 게이트를 테스트로 고정한다.

## Orphan 정리 정책

1. 정리 시점:
   - 각 잡 종료 시
   - 전체 파이프라인 종료 시
2. 식별 범위:
   - `run_id` 접두를 가진 session만 대상
3. 정리 동작:
   - 탐지 -> 강제 종료 -> 정리 결과 이벤트 기록

## 결과 요약 정책

1. `PipelineResult`에 다음 집계 필드를 추가한다.
   - `total_jobs`
   - `success_jobs`
   - `failed_jobs`
   - `quarantined_jobs`
2. 요약 문자열 필수 항목:
   - `total_jobs`
   - `success_jobs`
   - `failed_jobs`
   - `quarantined_jobs`
   - `failed_job_ids`
3. 최종 성공 조건:
   - `failed_jobs == 0`
   - `quarantined_jobs == 0`

## 운영/디버그 정책

1. 디버그 기준:
   - `.vscode/launch.json`
   - `.venv/bin/python -m pdb runner.py`
2. 테스트 실행 기준:
   - `.venv/bin/python -m unittest`
3. CLI/서브커맨드/추가 엔트리포인트는 금지한다.

## 수용 기준

1. orphan 정리 누락 0건
2. 실패/다운로드 오류 구분 보고 테스트 통과
3. 정책 회귀 테스트(`run_pipeline` 단일 진입점, CLI 금지) 통과
4. 연속 3회 런 기준 문서화 및 검증 절차 포함
