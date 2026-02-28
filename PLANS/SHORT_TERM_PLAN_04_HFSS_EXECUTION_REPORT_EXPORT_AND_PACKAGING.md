# SHORT TERM PLAN 04 - HFSS Execution, Full Report Export, and Report-Only Packaging

## Goal
- 원격 worker에서 HFSS 실행 후 모든 리포트를 export하고 report-only zip을 생성한다.

## Scope
- AEDT 실행 경로 고정:
  - `/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt`
- 실행 순서 고정:
  - `.aedt` 열기
  - 자동저장 비활성화
  - analyze
  - 모든 리포트 export
  - report-only zip 생성
  - `.aedt` 및 임시파일 삭제
- 성공/실패 모두 cleanup 표준화

## Deliverables
- HFSS 실행 어댑터(열기/분석/종료)
- 전체 리포트 export 유틸 + `ReportExportSpec`
- report-only ZIP 패키징 규약
- 삭제/정리 루틴(`claimed/workdir`의 `.aedt` 제거)

## Done Criteria
- 해석 성공 시 export 가능한 모든 리포트가 파일로 생성된다.
- ZIP에는 리포트 결과만 포함되고 `.aedt`는 포함되지 않는다.
- 결과 이동 후 원격 `.aedt`가 삭제된다.
- analyze/export/packaging 실패가 구분 기록되고 cleanup이 보장된다.
