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

1. 시나리오 A: 8개 모두 성공.
2. 시나리오 B: 일부 case 의도적 실패.
3. 시나리오 C: 다운로드 지연/실패.

## 시나리오별 관찰 포인트

1. 공통:
   - 8 window가 실제로 생성되었는지.
   - `wait-all` 루프가 조기 종료 없이 동작하는지.
   - case별 `exit.code`가 정확히 수집되는지.
2. A:
   - 전체 성공 판정.
   - 회수 파일 완전성.
3. B:
   - 전체 실패 판정.
   - 실패 case 정보 summary 반영.
   - 회수 동작 유지.
4. C:
   - download 전용 오류 코드 매핑.
   - 실행 실패와 다운로드 실패 구분.

## 디버그 절차

1. 브레이크포인트 후보:
   - screen window 생성 직후
   - wait-all 루프 진입/탈출 시점
   - 집계 및 summary 생성 시점
   - 다운로드 호출 직전/직후
2. 로그 확인:
   - `remote_run.log`
   - `case_XX/run.log`
   - `case_XX/exit.code`
3. 결과 판정:
   - 기대값과 실제값 비교표 작성.

## 완료 기준

1. 3개 시나리오에서 기대 결과와 실제 로그가 일치.
2. 회수 누락 0건.
3. 집계 문자열과 케이스 결과가 일관됨.
4. 재현 명령과 절차가 문서만으로 재수행 가능.
