# 07-04: 테스트 매트릭스 및 단계 게이트

## 테스트 매트릭스

1. 입력 스캔
   - 비재귀 스캔 시 하위폴더 `.aedt` 제외
   - `.aedt` 0개일 때 명시적 실패 반환
2. 자원 제약 검증
   - `windows_per_job=8`, `cores_per_window=4`, `cpus_per_job=32` 경계 검증
3. 스케줄링/동시성
   - 입력 80개에서도 동시 실행 최대 10 유지
   - 슬롯 초과 제출 0건
4. 실패 복구
   - 1회 재시도 후 성공
   - 재시도 소진 후 격리 전이
   - 일부 실패 시 전체 파이프라인 진행 지속
5. 오류 분류
   - 실행 실패와 다운로드 실패를 다른 코드/요약으로 보고
6. 정리/운영
   - orphan 정리 루틴 호출 및 누락 없음 검증
7. 상태 저장
   - DuckDB `runs/jobs/job_events/quarantine_jobs` 기록 검증
8. 정책 회귀
   - `run_pipeline` 단일 진입점 유지
   - CLI import/entrypoint 미추가 유지

## 단계 게이트

1. Gate 1 (07-01 완료)
   - API/검증/스캔/DB 초기화 테스트 통과
2. Gate 2 (07-02 완료)
   - 슬롯/재시도/격리/독립 실행 테스트 통과
3. Gate 3 (07-03 완료)
   - orphan 정리/요약/정책 회귀 테스트 통과
4. 최종 게이트
   - `.venv/bin/python -m unittest` 전체 통과
   - 연속 3회 실행 기준 문서/절차 검증

## 실행 명령 표준

```bash
.venv/bin/python -m unittest
.venv/bin/python -m pdb runner.py
```
