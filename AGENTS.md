# Repository Agent Rules

이 저장소는 AEDT Slurm 자동화를 **함수 호출 기반 단일 경로**로 유지한다.

## Mandatory Rules

1. 본 저장소 파이프라인은 CLI를 사용하지 않는다.
2. 파이프라인 시작점은 `run_pipeline(config)` 단 하나만 허용한다.
3. 신규 코드/리팩터링 시 CLI 옵션/서브커맨드/엔트리포인트 추가를 금지한다.
4. 문서와 테스트도 함수 호출 경로만 예시로 사용한다.
5. 실행, 테스트, 디버깅은 항상 저장소 루트의 `.venv` Python(`.venv/bin/python`)을 사용한다.
6. `pytest`가 필요하지만 `.venv`에 없으면 `uv pip install --python .venv/bin/python pytest`로 설치한다.
7. `pytest` 실행은 항상 `.venv/bin/python -m pytest ...` 형태를 사용한다.

## Debug Policy

1. 실행과 디버깅은 항상 `.vscode/launch.json`에 정의된 방식으로 수행한다.
2. 디버깅은 `pdb`를 기본 경로로 사용한다.
3. 임의 실행 명령을 추가하지 않고 재현 가능한 방식으로 유지한다.

`pdb` 실행 기준 예시:

```bash
.venv/bin/python -m pdb runner.py
```

## Systemd Rule

1. `/home/peetsmain/.config/systemd/user/peetsfea-runner.service`는 심볼릭 링크를 풀지 말고 직접 수정하지 않는다.
2. systemd service 변경이 필요하면 `/home/peetsmain/peetsfea-runner/systemd/peetsfea-runner.service`만 수정한다.

## DB Reset Rule

1. 매 단계 전환에서 서비스를 끄는 경우, `/home/peetsmain/peetsfea-runner/peetsfea_runner.duckdb`를 삭제한다.
2. 서비스 동작에 영향을 주는 주요 코드 변경사항이 있는 경우, 다음 실행 전에 `/home/peetsmain/peetsfea-runner/peetsfea_runner.duckdb`를 삭제한다.
3. DB 삭제는 service가 완전히 내려간 뒤에만 수행한다.

## Input Source Rule

1. `/home/peetsmain/peetsfea-runner/original` 아래 파일은 실제로 최종 돌려야 하는 무거운 `.aedt` 원본으로 취급한다.
2. `PLANS/roadmap1`부터 `PLANS/roadmap5`의 모든 로드맵이 완수되기 전까지는 `/home/peetsmain/peetsfea-runner/examples/sample.aedt`를 여러 개 복사해서 테스트, 재현, 서비스 검증에 사용한다.
3. `original` 아래의 무거운 `.aedt` 파일은 모든 로드맵 완료 후 최종 검증 단계에서만 사용한다.
