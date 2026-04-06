# Current Project Overview

이 문서는 `peetsfea-runner`를 다른 사람이 클론한 뒤 자기 계정과 자기 환경으로 붙일 때 먼저 봐야 하는 현재 상태 안내서다.
운영 판단 기준은 계속 active [`PLANS/`](../../PLANS/)가 우선이고, 이 문서는 코드가 지금 실제로 어떻게 연결되어 있는지만 간단히 설명한다.

## 처음 클론한 사람이 먼저 알아야 할 3가지

1. 공식 시작점은 CLI가 아니라 `run_pipeline(config)` 하나다.
2. 입력은 항상 `input_queue/<lane>/...` 아래에 두고, 처리 대상 `.aedt` 옆에 `.ready` 파일을 둔다.
3. 이 저장소를 자기 계정으로 쓰려면 `.ssh/config`, 계정 alias, remote runtime 경로, `systemd` 경로를 자기 환경에 맞게 바꿔야 한다.

## 빠른 시작 체크리스트

- Python은 저장소 루트 `.venv`의 Python 3.12를 사용한다.
- 입력 lane 디렉터리는 최소한 `input_queue/preserve_results`와 `input_queue/prune_results`를 준비한다.
- 출력 루트는 `output/`를 사용한다.
- durable truth는 `input_queue/**/*.aedt`, `*.ready`, `*.done`, `output/**/*.aedt.out`다.
- repo-local SSH 설정은 `.ssh/config`를 기준으로 본다.
- remote 실행을 쓸 경우 enroot 이미지 경로와 AEDT 경로가 원격 계정에서 실제로 존재해야 한다.

## 공식 시작점: `run_pipeline(config)`

이 프로젝트의 공식 실행 경로는 `run_pipeline(config)`다.
문서와 테스트도 이 경로를 기준으로 본다.

온보딩 시 가장 먼저 해볼 경로는 `continuous_mode=False`로 한 번만 돌리는 검증 run이다.
`runner.py`도 존재하지만, 현재 계정 목록이 하드코딩되어 있어서 clone/handover 기본 예시로는 적합하지 않다.

### 최소 입력 규칙

- 입력 파일은 `input_queue/<lane>/.../*.aedt`
- ready 파일은 같은 위치의 `*.aedt.ready`
- lane 이름은 built-in service 기준으로 `preserve_results`, `prune_results`
- canary/validation은 live output과 섞지 않는 별도 경로를 쓰는 것이 원칙

예를 들어 `sample.aedt`를 1회 검증으로 돌릴 때는 아래처럼 둔다.

```text
input_queue/
  prune_results/
    sample.aedt
    sample.aedt.ready
```

### 최소 Python 예시

아래 예시는 문서용 reference다.
핵심은 직접 `PipelineConfig(...)`를 만들고 `run_pipeline(config)`를 호출하는 것이다.

```python
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline

repo = Path("/path/to/peetsfea-runner").resolve()

config = PipelineConfig(
    input_queue_dir=str(repo / "input_queue" / "prune_results"),
    output_root_dir=str(repo / "output" / "prune_results"),
    accounts_registry=(
        AccountConfig(account_id="account_01", host_alias="my-gate-01", max_jobs=10),
    ),
    execute_remote=True,
    remote_execution_backend="slurm_batch",
    ssh_config_path=str(repo / ".ssh" / "config"),
    remote_container_image="~/runtime/enroot/aedt.sqsh",
    remote_container_ansys_root="/opt/ohpc/pub/Electronics/v252",
    remote_ansys_executable="/mnt/AnsysEM/ansysedt",
    continuous_mode=False,
)

result = run_pipeline(config)
print(result.summary)
print(result.success, result.exit_code)
```

### `PipelineConfig`에서 먼저 볼 필드

온보딩 시 아래 필드만 먼저 이해하면 된다.

- `input_queue_dir`
- `output_root_dir`
- `accounts_registry`
- `execute_remote`
- `remote_execution_backend`
- `ssh_config_path`
- `remote_container_image`
- `remote_container_ansys_root`
- `remote_ansys_executable`
- `continuous_mode`

## 다른 계정으로 쓰기 위한 최소 수정 포인트

### 1. repo-local SSH 설정

이 저장소에서 `host_alias`는 실제 host/IP가 아니라 repo-local `.ssh/config`의 `Host` alias를 뜻한다.
즉 `AccountConfig(..., host_alias="my-gate-01")`의 `my-gate-01`은 `.ssh/config` 안의 `Host my-gate-01`과 맞아야 한다.

최소 수정 항목은 아래 세 가지다.

- `Host`
- `User`
- `IdentityFile`

현재 `.ssh/config`는 저장소 내부 키 경로와 특정 사용자 계정을 직접 가리키고 있으므로, 클론한 사람은 자기 alias와 자기 키 경로로 바꿔야 한다.

예시:

```sshconfig
Host my-gate-01
  HostName <real-host-or-ip>
  User <my-remote-user>
  IdentityFile /path/to/peetsfea-runner/.ssh/id_ed25519
  IdentitiesOnly yes
```

### 2. 계정 alias와 코드 연결 규칙

새 계정을 붙일 때 규칙은 단순하다.

- `.ssh/config`에 `Host` alias를 만든다.
- `AccountConfig.host_alias` 또는 `PEETSFEA_ACCOUNTS`에 같은 alias를 넣는다.
- 실제 host/IP/user/key 정보는 `.ssh/config`가 해석한다.

즉 계정을 바꾼다고 해서 host/IP를 코드 여러 군데에 반복해서 넣는 구조는 아니다.
대신 alias 이름이 `.ssh/config`와 코드에서 정확히 일치해야 한다.

### 3. 입력 lane 준비

built-in service와 문서 예시는 둘 다 `input_queue` 아래 lane 구조를 전제로 한다.
최소 준비 경로는 아래 두 개다.

- `input_queue/preserve_results`
- `input_queue/prune_results`

live service를 쓰지 않더라도, 저장소 구조를 이 형태로 맞춰 두는 편이 가장 안전하다.

### 4. Python/.venv 준비

이 저장소의 실행, 테스트, 디버깅 기준 Python은 항상 저장소 루트의 `.venv/bin/python`이다.
다른 전역 Python을 기준으로 설명하지 않는다.

## 현재 코드에서 하드코딩되어 있는 값

다른 사람이 클론해서 자기 계정으로 붙일 때 가장 자주 놓치는 부분이다.
아래 값들은 환경변수로 일부 대체되더라도, 현재 기본 구현에는 하드코딩이 남아 있다.

### `runner.py`

`runner.py`는 환경변수 몇 개를 읽지만, 기본 `accounts_registry`는 특정 `gate1-*` 계정 alias로 하드코딩되어 있다.
그래서 clone 직후 자기 계정으로 바로 돌릴 기본 엔트리로 쓰기 어렵다.

### built-in service

`run_built_in_service()`는 lane별 계정과 리소스 구성을 코드 안에서 만든다.
현재 상태는 아래처럼 고정돼 있다.

- lane 이름: `preserve_results`, `prune_results`
- `preserve_results`는 단일 계정 중심
- `prune_results`는 여러 계정 중심
- lane별 `cpus_per_job`, `slots_per_job`, `cores_per_slot`, `tasks_per_slot` 기본값이 코드에 박혀 있음

즉 service까지 자기 환경으로 옮기려면 단순히 `.ssh/config`만 바꾸는 것으로 끝나지 않고, built-in service가 참조하는 lane/account 매핑도 함께 검토해야 한다.

### `systemd/peetsfea-runner.service`

현재 service 파일은 아래 두 값이 사용자 작업 디렉터리에 종속된다.

- `WorkingDirectory=%h/mnt/8tb/peetsfea-runner`
- `ExecStart=%h/mnt/8tb/peetsfea-runner/.venv/bin/python ...`

클론 경로가 다르면 이 값도 같이 바꿔야 한다.

### control-plane return 경로

built-in service 기본값에는 control-plane return host/user/port가 들어 있다.
특히 return host는 현재 환경의 특정 IP 기준으로 잡혀 있으므로, 다른 사용자가 그대로 쓰면 맞지 않을 가능성이 높다.

### remote runtime / AEDT 경로

현재 기본값에는 아래 성격의 경로가 포함되어 있다.

- enroot 이미지 경로
- remote root
- AEDT 설치 루트
- AEDT 실행 파일 경로

원격 계정마다 실제 경로가 다를 수 있으므로, clone 후 가장 먼저 확인해야 한다.

## `systemd` 서비스 경로

서비스 운영 경로는 함수 호출 경로 다음 순서로 이해하면 된다.

1. user service가 `run_built_in_service()`를 호출한다.
2. built-in service가 lane별 `PipelineConfig`를 만든다.
3. 각 lane worker loop가 `input_queue/<lane>`를 계속 감시한다.
4. 결과는 `output/<lane>`와 입력 파일의 `.done` 상태에 반영된다.

여기서 중요한 현재 상태는 아래와 같다.

- service는 `run_pipeline(config)`를 직접 감싸는 built-in loop다.
- lane 이름은 `preserve_results`, `prune_results`로 고정돼 있다.
- built-in service의 계정/리소스 기본값은 코드에 박혀 있다.
- service 복구나 운영 판단은 overview 문서가 아니라 active [`PLANS/`](../../PLANS/)를 따라야 한다.

## validation / canary를 어떻게 이해하면 되는가

온보딩 관점에서 필요한 해석만 남기면 아래와 같다.

- canary는 service restart 자체를 뜻하지 않는다.
- canary는 `run_pipeline(config)`를 `continuous_mode=False`로 호출하는 별도 validation run이다.
- validation run은 live output을 재사용하지 않는 편이 안전하다.
- `output_variables.csv` gate의 구체 규칙은 active [`PLANS/`](../../PLANS/)를 본다.

즉 처음 붙여볼 때는 service 재기동보다, 별도 output 경로를 둔 1회성 validation run부터 확인하는 편이 맞다.

## 이 문서만 읽고 바로 기억하면 되는 것

- 먼저 `run_pipeline(config)` 기준으로 1회 검증 run을 만든다.
- `input_queue/<lane>`와 `.ready` 규칙을 지킨다.
- `.ssh/config`의 alias와 `host_alias`를 맞춘다.
- `runner.py`, built-in service, `systemd` 파일에는 현재 사용자 고유값이 남아 있으니 그대로 쓰지 않는다.
- live service 운영 판단은 active [`PLANS/`](../../PLANS/)를 따른다.
