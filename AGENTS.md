# Repository Agent Rules

이 저장소는 AEDT Slurm 자동화를 **함수 호출 기반 단일 경로**로 유지한다.

## Plan Authority

1. 현재 실행 기준 문서는 `/home/peetsmain/peetsfea-runner/PLANS/roadmap-tonight-master-plan.md`와 그 문서가 참조하는 active roadmap 문서들이다.
2. `/home/peetsmain/peetsfea-runner/PLANS/archives` 아래 문서는 참고용 기록이며, 직접 실행 기준으로 사용하지 않는다.
3. active `PLANS`와 이 문서가 충돌하면, active `PLANS`를 우선한다.

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

1. live DB 기본 정책은 `유지`다. restart-safe 운영 변경, canary gate 강화, throughput knob 조정, telemetry 추가, bad-node 정책 추가만으로는 `/home/peetsmain/peetsfea-runner/peetsfea_runner.duckdb`를 삭제하지 않는다.
2. DB 삭제는 `schema 변경`, `state 의미 변경`, `ingest 의미 변경`일 때만 허용한다.
3. DB 삭제가 필요하면 service가 완전히 내려간 뒤에만 수행한다.
4. sample canary나 validation lane은 live DB를 재사용하지 않고 별도 DB 경로를 사용한다.

## Input Source Rule

1. `/home/peetsmain/peetsfea-runner/original` 아래 파일은 실제로 최종 돌려야 하는 무거운 `.aedt` 원본으로 취급한다.
2. active `PLANS`가 명시적으로 real input 사용을 허용하기 전까지는 `/home/peetsmain/peetsfea-runner/examples/sample.aedt`를 여러 개 복사해서 테스트, 재현, sample canary, validation lane 검증에 사용한다.
3. `original` 아래의 무거운 `.aedt` 파일은 active `PLANS`가 final validation 또는 real cutover를 허용한 단계에서만 사용한다.

## Canary Rule

1. canary는 service 재기동만으로 발생한다고 가정하지 않는다.
2. canary는 항상 `run_pipeline(config)` 함수 호출 경로에서 `continuous_mode=False`인 별도 validation lane으로 수행한다.
3. canary 산출물은 live continuous service output과 섞지 않는다.
4. canary 판정은 `output_variables.csv` 존재 여부만이 아니라 active `PLANS`의 CSV schema gate 기준을 따라야 한다.

## Codex Resume Helper Rule

1. Codex resume automation 입력은 `/home/peetsmain/peetsfea-runner/build/codex_resume_requests.json`에 둔다.
2. 10분 간격 자동 resume runner는 `/home/peetsmain/peetsfea-runner/build/codex_resume_every_10m.py`를 사용한다.
3. resume automation용 요청도 active `PLANS`와 이 문서의 제약을 따라야 한다.
