# 06-02: Case Lifecycle 및 Exit Aggregation 세부 계획

## 목적

1. case별 상태 파일 규격을 고정한다.
2. `wait-all` 완료 기준과 최종 집계 규칙을 정의한다.

## 상태 파일 표준

1. `case_XX/run.log`: 실행 로그.
2. `case_XX/exit.code`: 정수 종료 코드.
3. `case_XX/started_at.txt`, `case_XX/ended_at.txt`는 선택 로그로 허용.

## wait-all 규칙

1. 8개 case의 `exit.code`가 모두 생성될 때까지 대기.
2. 중간 실패가 있어도 즉시 중단하지 않음.
3. 타임아웃은 `time_limit + buffer`를 사용.

## 집계 규칙

1. 8개 모두 `0`이면 전체 성공.
2. 하나라도 `!= 0`이면 전체 실패.
3. 요약은 case별 코드와 함께 기록.

## PipelineResult.summary 형식

1. 필수 항목:
   - `total_cases=8`
   - `success_cases=<n>`
   - `failed_cases=<m>`
   - `failed_case_ids=[...]`
2. 예시:
   - `8 cases completed (success=6, failed=2). failed=case_03:1,case_07:137`

## 실패 코드 매핑

1. remote 실행 자체 문제: `EXIT_CODE_REMOTE_RUN_FAILURE`.
2. download/압축 문제: `EXIT_CODE_DOWNLOAD_FAILURE`.
3. case 실패는 summary에 보존하고 전체 실패로 반영.
