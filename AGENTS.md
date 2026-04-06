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

## User Language Rule

1. 사용자가 말하는 `test`는 기본적으로 TDD/unit test를 뜻하지 않고, 일반적인 의미의 실행 검증/재현/확인을 뜻한다고 해석한다.
2. 사용자가 `test해봐`, `테스트해봐`, `돌려봐`, `확인해봐`라고 말할 때는 먼저 실제 실행 검증 경로를 우선한다.
3. 에이전트가 작성하거나 수정한 unit test/pytest는 사용자 요청이 명시적으로 있을 때만 우선 보고 대상으로 삼는다.
4. 사용자가 먼저 묻지 않았다면, 에이전트가 작성한 test 자체를 최종 보고의 중심으로 두지 않는다.
5. 사용자가 별도로 원하지 않았다면, `pytest` 추가/수정 사실은 핵심 실행 결과보다 앞세워 설명하지 않는다.

`pdb` 실행 기준 예시:

```bash
.venv/bin/python -m pdb runner.py
```

## Systemd Rule

1. `/home/peetsmain/.config/systemd/user/peetsfea-runner.service`는 심볼릭 링크를 풀지 말고 직접 수정하지 않는다.
2. systemd service 변경이 필요하면 `/home/peetsmain/peetsfea-runner/systemd/peetsfea-runner.service`만 수정한다.

## Runtime State Rule

1. durable truth는 DB가 아니라 `input_queue/**/*.aedt`, `*.ready`, `*.done`, `output/**/*.aedt.out`이다.
2. 런타임 상태는 프로세스 메모리와 `*.state` 네임스페이스를 사용한다.
3. service restart는 콜드 스타트로 취급하고, old worker/state를 복구하지 않는다.
4. sample canary나 validation lane은 live output과 섞지 않는 별도 input/output 경로를 사용한다.

## Input Source Rule

1. 입력 소스는 항상 `/home/peetsmain/peetsfea-runner/input_queue` 아래 경로만 사용한다.
2. 테스트나 검증이 필요하면 필요한 `.aedt`를 `input_queue` 아래 적절한 lane으로 복사해서 사용한다.
3. `original` 같은 별도 입력 루트는 특수 취급하지 않는다.

## Canary Rule

1. canary는 service 재기동만으로 발생한다고 가정하지 않는다.
2. canary는 항상 `run_pipeline(config)` 함수 호출 경로에서 `continuous_mode=False`인 별도 validation lane으로 수행한다.
3. canary 산출물은 live continuous service output과 섞지 않는다.
4. canary 판정은 `output_variables.csv` 존재 여부만이 아니라 active `PLANS`의 CSV schema gate 기준을 따라야 한다.

## Live Service Acceptance Rule

1. built-in service 복구나 ingest 복구를 주장하려면, 실제 `input_queue/<lane>/.../sample.aedt`가 서비스에 의해 소비되어야 한다.
2. acceptance 최소 기준은 다음 4가지를 모두 만족하는 것이다.
   - `input_queue` 아래 sample이 service ingest 대상이 된다.
   - `output/<lane>/.../sample.aedt.out` 아래 실제 산출물이 materialize 된다.
   - service 정책에 따라 입력이 active queue에서 빠진다. 현재 built-in service는 성공 시 `.done`으로 나가야 한다.
   - service journal 또는 실행 로그에서 `QUEUED -> submit/worker -> terminal` 진행이 확인된다.
3. unit test, direct `run_pipeline(config)` validation lane, 원격 수동 probe는 보조 검증일 뿐이며, 이 규칙을 대체하지 못한다.
4. 위 round-trip이 끝나지 않았으면 복구 완료로 간주하지 말고 계속 수정과 재검증을 반복한다.

## Codex Resume Helper Rule

1. Codex resume automation 입력은 `/home/peetsmain/peetsfea-runner/build/codex_resume_requests.json`에 둔다.
2. 10분 간격 자동 resume runner는 `/home/peetsmain/peetsfea-runner/build/codex_resume_every_10m.py`를 사용한다.
3. resume automation용 요청도 active `PLANS`와 이 문서의 제약을 따라야 한다.
