# Plan 05: Testing and Acceptance

## Test Matrix

1. `PipelineConfig` 유효성 검사
2. 정상 흐름에서 `PipelineResult.success=True`
3. `ssh` 1회 실패 후 재시도 성공
4. `srun` 실패 시 종료 코드 매핑 검증
5. 원격 실행 실패 시에도 결과 회수 시도 검증
6. `screen` 윈도우 수가 1이 아닐 때 실패 처리 검증
7. 결과 다운로드 실패 후 재시도 검증
8. 원격 PC에 Miniconda3가 있을 때 conda py3.12 + `~/.peetsfea-runner-venv` 생성/설치 성공
9. 원격 PC에 Miniconda3가 없을 때 사용자 홈 설치 후 conda py3.12 + 설치 성공
10. Miniconda3 설치에 필요한 `curl`/`wget`이 모두 없을 때 즉시 실패 및 안내 메시지 검증
11. 기존 venv Python 버전이 3.12가 아니면 재생성되는지 검증
12. 잘못된 태그 설치 시 실패 메시지(태그/권한/프록시 점검 안내) 검증

## Static Policy Checks

1. `argparse`, `click`, `typer` import 금지 검사
2. `console_scripts` 및 CLI 엔트리포인트 미존재 검사
3. 함수 단일 진입점(`run_pipeline`) 유지 검사

## Operational Acceptance Criteria

1. 운영자가 태그를 지정해 `uv pip install git+...@<tag>` 설치 가능
2. 원격 PC에서 함수 import 후 실행 가능
3. `ssh -> srun --pty -> screen` 전체 단계 자동 수행
4. 시뮬레이션 후 결과가 로컬 아티팩트 디렉토리에 존재
5. 실패 시 로그와 실패 사유가 명시됨
6. 원격 설치는 항상 `~/.peetsfea-runner-venv`를 사용하고 Python 3.12 보장

## Assumptions

1. `gate1-harry`는 SSH 키 기반 접속 가능
2. 계산 노드에 `screen`, `python3`, `pip` 사용 가능
3. `module load ansys-electronics/v252` 가능
4. 라이선스 서버 `1055@172.16.10.81` 접근 가능
