# Enroot Canary + Repo-Local SSH 인수인계서

## 목적

이 문서는 현재 확인된 enroot 사용 경험을 이 저장소의 AEDT Slurm 파이프라인에 안전하게 연결하기 위한 참고용 인수인계 문서다.
active 실행 기준은 계속 `PLANS/roadmap-tonight-master-plan.md`와 그 하위 active roadmap 문서들이며, 본 문서는 enroot 도입 설계와 구현 준비 기준을 정리하는 archive 참고 문서로 둔다.

핵심 목표는 아래 세 가지다.

- `run_pipeline(config)` 단일 진입점 제약을 유지한다.
- enroot 도입 범위를 먼저 `Linux + slurm_batch + continuous_mode=False canary`에 한정한다.
- 계정이 늘어나도 저장소 루트의 repo-local `.ssh/config`만 갱신하면 운영이 이어지도록 규약을 고정한다.

## 세부계획서

이 문서는 enroot archive 설계의 루트 문서다.
아래 세부계획서를 함께 본다.

- [enroot-canary-01-ssh-and-account-topology.md](./enroot-canary-01-ssh-and-account-topology.md)
- [enroot-canary-02-worker-runtime-and-container-contract.md](./enroot-canary-02-worker-runtime-and-container-contract.md)
- [enroot-canary-03-canary-rollout-and-270-slot-target.md](./enroot-canary-03-canary-rollout-and-270-slot-target.md)

## 도입 배경

현재 프로젝트는 함수 호출 기반 단일 경로를 유지한다.

- CLI를 추가하지 않는다.
- 신규 실행 예시도 `run_pipeline(config)` 기준으로만 설명한다.
- canary는 active `PLANS` 기준대로 `continuous_mode=False`인 별도 validation lane에서만 수행한다.

컨테이너 관점에서 현재까지 확인된 배경은 아래와 같다.

- podman은 cpu2 Slurm job 내부에서 바로 쓰기 어렵다.
- enroot는 이미지 import/create/start, 컨테이너 진입, host 경로 mount, Ansys headless 기동까지 확인됐다.
- 따라서 이 저장소에서 컨테이너 도입을 검토할 때 우선 대상은 podman이 아니라 enroot다.

podman이 우선 대상이 아닌 이유는 아래로 정리한다.

- `XDG_RUNTIME_DIR` 구성이 불안정했다.
- `runc`, `crun` 같은 OCI runtime 부재 문제가 있었다.
- 결과적으로 `podman run`까지 안정적으로 가지 못했다.

반면 enroot는 아래 범위가 동작했다.

- 이미지 준비 가능
- job 내부 컨테이너 실행 가능
- host 디렉터리 mount 가능
- Ansys Electronics Desktop 2025 R2의 headless `-ng -grpcsrv` 기동 가능

## 현재까지 검증된 사실

현재까지의 검증 사실은 아래로 고정한다.

- `cpu2` Slurm job 내부에서 enroot를 사용할 수 있다.
- mount 방식은 절대경로 직접 지정보다 `cd /opt/ohpc/pub/Electronics/v252` 후 `--mount .:/mnt` 방식이 실제로 동작했다.
- 검증 완료 범위는 GUI가 아니라 headless `-ng -grpcsrv`다.
- 최소 실증 의존성은 `file`, `perl`, `ANS_IGNOREOS=1`이었다.
- `VerifyOS.bash` 기준의 권장 의존성 목록은 더 길며, 실제 solver/프로젝트 로드/GUI 범위로 가면 추가 패키지가 더 필요할 수 있다.

현재까지 확인된 최소 동작 예시는 아래와 같다.

```bash
srun -p cpu2 -N1 -n1 -c1 --time=00:30:00 bash
cd /opt/ohpc/pub/Electronics/v252

export ENROOT_RUNTIME_PATH=/tmp/enroot/runtime/$USER
export ENROOT_CACHE_PATH=/tmp/enroot/cache/$USER
export ENROOT_DATA_PATH=/tmp/enroot/data/$USER
export ENROOT_TEMP_PATH=/tmp/enroot/tmp/$USER
mkdir -p "$ENROOT_RUNTIME_PATH" "$ENROOT_CACHE_PATH" "$ENROOT_DATA_PATH" "$ENROOT_TEMP_PATH"
chmod 700 "$ENROOT_RUNTIME_PATH" "$ENROOT_CACHE_PATH" "$ENROOT_DATA_PATH" "$ENROOT_TEMP_PATH"

enroot import -o /tmp/$USER-ubuntu2404.sqsh docker://ubuntu:24.04
enroot create -n ubuntu2404-test /tmp/$USER-ubuntu2404.sqsh
enroot start --root --rw --mount .:/mnt ubuntu2404-test /bin/bash

apt-get update
apt-get install -y file perl
export ANS_IGNOREOS=1
/mnt/AnsysEM/ansysedt -ng -grpcsrv 50056
```

추가 안정성 후보 패키지 목록은 아래를 기준으로 본다.

- `lsb-release`
- `xsltproc`
- `fontconfig`
- `libfreetype6`
- `libgif7`
- `libglib2.0-0` 또는 Ubuntu 24.04 계열의 `libglib2.0-0t64`
- `libdrm2`
- `libjpeg8` 또는 `libjpeg-turbo8`
- `libpng16-16` 또는 Ubuntu 24.04 계열의 `libpng16-16t64`
- `libselinux1`
- `libx11-6`
- `libxau6`
- `libx11-xcb1`
- `libxdamage1`
- `libxext6`
- `libxfixes3`
- `libxft2`
- `libxm4`
- `libxmu6`
- `libxrender1`
- `libxt6` 또는 Ubuntu 24.04 계열의 `libxt6t64`
- `libxxf86vm1`
- `libgl1-mesa-dri`
- `libglx-mesa0`
- `libglu1-mesa`
- `zlib1g`

중요한 해석은 아래와 같다.

- `file`, `perl`, `ANS_IGNOREOS=1`은 최소 실증 범위다.
- 위 패키지 목록은 안정성 후보이며, 아직 이 저장소의 actual canary 흐름에서 모두 검증된 것은 아니다.
- 따라서 운영 hot path에서 `apt-get`을 반복하는 대신, 필요한 패키지를 미리 넣은 pinned image를 준비하는 방향이 맞다.

## 프로젝트 적용 원칙

이 저장소에서 enroot를 도입할 때 우선 적용 원칙은 아래와 같다.

- 범위는 `Linux + slurm_batch + continuous_mode=False canary` 우선이다.
- live continuous service는 이번 단계에서 유지한다.
- canary는 active `PLANS` 기준대로 sample input, 별도 DB, 별도 output root, CSV schema gate를 유지한다.
- GUI는 범위 밖이다.
- 문서와 구현 예시는 모두 함수 호출 기반 `run_pipeline(config)` 경로를 따른다.

즉 이번 단계의 목표는 “컨테이너 기반 validation lane을 먼저 고정하고, live continuous lane은 다음 단계에서 전환 여부를 본다”로 둔다.

현재 계정 배치와 목표 용량은 아래처럼 고정한다.

| lane | 계정 | jobs/account | slots/job | total slots |
| --- | --- | --- | --- | --- |
| `prune` | `gate1-harry`, `gate1-dhj02`, `gate1-jji0930`, `gate1-hmlee31`, `gate1-dw16` | `10` | `5` | `250` |
| `preserve` | `gate1-r1jae262`, `gate1-wjddn5916` | `10` | `1` | `20` |
| total | `7 accounts` | - | - | `270` |

## Repo-Local `.ssh` 운영 규약

이 문서에서 제안하는 SSH 운영 표준은 “전역 `~/.ssh/config`를 보지 않고 저장소 루트의 repo-local `.ssh/config`를 기준으로 삼는 방식”이다.

기본 원칙은 아래와 같다.

- 저장소 루트 `.ssh/`를 전용 SSH 자산 경로로 사용한다.
- 전역 `~/.ssh/config`는 기준에서 제외한다.
- 실제 키 파일과 config는 비추적 상태로 관리한다.
- `host_alias`는 실제 호스트명이 아니라 `.ssh/config`의 `Host` alias를 뜻한다.

권장 디렉터리 구조는 아래처럼 둔다.

```text
<repo>/.ssh/
  config
  keys/
    gate1-harry
    gate1-dhj02
    gate1-jji0930
    gate1-hmlee31
    gate1-dw16
    gate1-r1jae262
    gate1-wjddn5916
```

예시 config 구조는 아래처럼 둔다.

```sshconfig
Host gate1-harry
  HostName <real-hostname-or-ip>
  User <remote-user>
  IdentityFile /home/peets/mnt/8tb/peetsfea-runner/.ssh/keys/gate1-harry
  IdentitiesOnly yes

Host gate1-dhj02
  HostName <real-hostname-or-ip>
  User <remote-user>
  IdentityFile /home/peets/mnt/8tb/peetsfea-runner/.ssh/keys/gate1-dhj02
  IdentitiesOnly yes
```

새 계정 추가 절차는 아래 순서로 고정한다.

1. `.ssh/config`에 새 `Host` block을 추가한다.
2. `.ssh/keys/`에 해당 alias용 키를 추가한다.
3. `PEETSFEA_ACCOUNTS`에 `account_id@host_alias:max_jobs[:platform:scheduler]` 형태로 계정을 추가한다.

예시는 아래처럼 둔다.

```text
PEETSFEA_ACCOUNTS=account_01@gate1-harry:10,account_02@gate1-dhj02:10,account_03@gate1-jji0930:10,account_04@gate1-hmlee31:10,account_05@gate1-dw16:10,account_06@gate1-r1jae262:10,account_07@gate1-wjddn5916:10
```

이 규약의 장점은 아래와 같다.

- 실제 호스트명, 사용자명, 키 경로를 `PEETSFEA_ACCOUNTS`에 반복해서 넣지 않아도 된다.
- 계정 추가가 환경 변수와 SSH alias 추가만으로 끝난다.
- host 교체나 IP 변경이 생겨도 `.ssh/config`만 수정하면 된다.

주의사항은 아래와 같다.

- control plane reverse tunnel은 기존 explicit `host/user/port` 방식 유지가 더 안전하다.
- 즉 compute node 내부에서 control plane으로 되돌아오는 경로는 repo-local `.ssh/config`에 억지로 묶지 않는다.
- repo-local `.ssh/config`는 launcher-side `ssh -F <repo>/.ssh/config`, `scp -F <repo>/.ssh/config`에만 적용하는 것으로 본다.

## Enroot 운영 표준

이 저장소에 맞는 enroot 운영 표준은 “job hot path에서 import/create/apt-get을 하지 않고, 미리 준비된 pinned image를 사용한다”로 고정한다.

핵심 원칙은 아래와 같다.

- `prebuilt pinned .sqsh` 이미지를 사용한다.
- job 실행 경로에서는 `enroot import`, `enroot create`, `apt-get`을 수행하지 않는다.
- ENROOT 관련 runtime/cache/data/tmp path는 Slurm job 단위로 `/tmp/enroot/.../$USER/$SLURM_JOB_ID` 아래에 분리한다.
- host Ansys root에서 먼저 `cd /opt/ohpc/pub/Electronics/v252`를 수행한 뒤 `--mount .:/mnt`를 사용한다.
- case workdir는 별도로 `/work`에 mount한다.
- container 내부 AEDT 실행은 `/mnt/AnsysEM/ansysedt -ng -grpcsrv <port>` 기준으로 본다.

권장 실행 계약 예시는 아래처럼 정리한다.

```bash
cd /opt/ohpc/pub/Electronics/v252

export ENROOT_RUNTIME_PATH=/tmp/enroot/runtime/$USER/$SLURM_JOB_ID
export ENROOT_CACHE_PATH=/tmp/enroot/cache/$USER/$SLURM_JOB_ID
export ENROOT_DATA_PATH=/tmp/enroot/data/$USER/$SLURM_JOB_ID
export ENROOT_TEMP_PATH=/tmp/enroot/tmp/$USER/$SLURM_JOB_ID
mkdir -p "$ENROOT_RUNTIME_PATH" "$ENROOT_CACHE_PATH" "$ENROOT_DATA_PATH" "$ENROOT_TEMP_PATH"
chmod 700 "$ENROOT_RUNTIME_PATH" "$ENROOT_CACHE_PATH" "$ENROOT_DATA_PATH" "$ENROOT_TEMP_PATH"

enroot start \
  --root \
  --rw \
  --mount .:/mnt \
  --mount "$CASE_DIR":/work \
  aedt-ubuntu2404-pinned \
  /bin/bash -lc 'cd /work && export ANS_IGNOREOS=1 && /mnt/AnsysEM/ansysedt -ng -grpcsrv 50056'
```

여기서 중요한 점은 아래 두 가지다.

- mount는 절대경로 직접 지정이 아니라 “host ansys root로 이동한 뒤 `.:/mnt`” 기준으로 고정한다.
- `/tmp` 사용량은 여전히 남기 때문에, enroot가 현재 bad-node `/tmp` 문제를 자동으로 해결해 주는 것은 아니다.

## Enroot가 해결하는 문제와 해결하지 못하는 문제

enroot가 이 저장소에서 주로 해결하는 문제는 아래에 가깝다.

- node별 Python/PyAEDT/runtime 편차 축소
- host-side Miniconda/venv/bootstrap 비용 감소
- canary에서 “환경 문제”와 “코드/CSV 회귀”를 더 잘 분리하는 것

반대로 enroot만으로 해결되지 않는 문제는 아래다.

- `/tmp free < 64 GiB` 자체
- `No space left on device`
- bad-node quarantine
- Slurm/control-plane rediscovery 자체

따라서 active `PLANS`의 아래 운영 축은 그대로 살아 있어야 한다.

- CSV integrity gate
- sample canary 분리 운영
- `/tmp` 기반 bad-node triage
- restart/rediscovery 확인

즉 enroot는 “환경 재현성 개선” 도구이지, 현재 문서가 다루는 resource/bad-node 문제를 대체하는 도구가 아니다.

## Rollout 절차

이번 단계의 rollout은 아래 3단계로 본다.

### 1단계: 문서화 및 SSH 규약 확정

- repo-local `.ssh` 운영 규약을 먼저 고정한다.
- 어떤 alias가 어떤 계정으로 연결되는지 정리한다.
- launcher-side `ssh -F <repo>/.ssh/config` 적용 필요성을 구현 항목으로 남긴다.

### 2단계: validation lane canary only

- `continuous_mode=False` validation lane에서만 enroot를 사용한다.
- 입력은 active `PLANS` 기준대로 `examples/sample.aedt` 복제본만 사용한다.
- DB와 output은 `tmp/tonight-canary/<window>/...` 아래 별도 경로를 사용한다.
- canary 통과 기준은 `output_variables.csv` 존재만이 아니라 active `PLANS`의 CSV schema gate다.

### 3단계: sample canary green 이후 live 검토

- sample canary가 green이고 CSV schema gate가 green일 때만 live continuous 전환을 검토한다.
- live cutover는 별도 문서와 검증으로 다루고, 이번 문서의 범위에는 포함하지 않는다.

## Validation Lane 예시

아래 예시는 “문서상 reference”이며, 여전히 `run_pipeline(config)` 단일 경로만 사용한다.
현재 7계정 배치는 prune lane `5계정`과 preserve lane `2계정`으로 나뉘므로 canary도 lane별로 분리해서 본다.
여기서는 prune canary reference를 먼저 적고, preserve canary는 계정과 `slots_per_job`만 바꿔 동일 구조를 사용한다.

```python
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline

repo = Path("/home/peetsmain/peetsfea-runner")
window = "00"
root = repo / "tmp" / "tonight-canary" / window
input_dir = root / "input"
output_dir = root / "output"
delete_failed_dir = root / "delete_failed"
db_path = root / "state.duckdb"

input_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)
delete_failed_dir.mkdir(parents=True, exist_ok=True)

sample_src = repo / "examples" / "sample.aedt"
sample_dst = input_dir / "sample.aedt"
sample_dst.write_bytes(sample_src.read_bytes())
(input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")

config = PipelineConfig(
    input_queue_dir=str(input_dir),
    output_root_dir=str(output_dir),
    delete_failed_quarantine_dir=str(delete_failed_dir),
    metadata_db_path=str(db_path),
    accounts_registry=(
        AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),
        AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
        AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
        AccountConfig(account_id="account_04", host_alias="gate1-hmlee31", max_jobs=10),
        AccountConfig(account_id="account_05", host_alias="gate1-dw16", max_jobs=10),
    ),
    execute_remote=True,
    remote_execution_backend="slurm_batch",
    partition="cpu2",
    cpus_per_job=20,
    mem="960G",
    time_limit="05:00:00",
    remote_root="~/aedt_runs",
    control_plane_host="127.0.0.1",
    control_plane_port=8765,
    control_plane_ssh_target="peetsmain@172.16.166.83",
    control_plane_return_host="172.16.166.83",
    control_plane_return_port=5722,
    control_plane_return_user="peetsmain",
    remote_ssh_port=22,
    delete_input_after_upload=False,
    continuous_mode=False,
    ingest_poll_seconds=120,
    ready_sidecar_suffix=".ready",
    slots_per_job=5,
    worker_bundle_multiplier=1,
    cores_per_slot=4,
    worker_requeue_limit=1,
    run_rotation_hours=24,
    pending_buffer_per_account=3,
    capacity_scope="all_user_jobs",
    balance_metric="slot_throughput",
    input_source_policy="sample_only",
    public_storage_mode="disabled",
)

result = run_pipeline(config)
print(result.summary)
print(result.success, result.exit_code)
```

preserve canary는 아래 값만 바꿔 mirror 구성으로 본다.

- `accounts_registry`
  - `gate1-r1jae262`
  - `gate1-wjddn5916`
- `slots_per_job=1`
- `cpus_per_job=32`
- `cores_per_slot=32`
- `tasks_per_slot=4`

위 예시에서 enroot 관련 세부 값은 구현 단계에서 config field로 추가해 연결한다.
이번 문서 단계에서는 “적용 범위와 운영 기준”만 고정한다.

## 실패 시 판단 기준

enroot 도입 canary에서 우선 보는 실패 범주는 아래다.

- ssh alias 미해결
- repo-local `.ssh/config` 미적용
- enroot binary 없음
- pinned image 누락
- host ansys root mount 실패
- `CANARY_FAILED`
- `output_variables.csv` schema red

그리고 아래는 계속 별도 문제로 본다.

- `/tmp` 부족
- `No space left on device`
- bad-node 등록/쿨다운
- control tunnel 문제

즉 `CANARY_FAILED`가 났다고 해서 모두 컨테이너 문제로 보지 않는다.
active `PLANS` 기준대로 CSV 회귀, resource 문제, tunnel 문제를 분리해서 본다.

## 구현 전 체크리스트

다음 구현자는 아래 체크리스트를 기준으로 작업을 시작하면 된다.

- `.ssh` 디렉터리와 키 파일은 git 비추적으로 관리할 것
- launcher-side 모든 `ssh`/`scp` 호출에 `-F <repo>/.ssh/config`를 강제할 방법을 넣을 것
- repo-local `.ssh/config`에서 `Host` alias와 `PEETSFEA_ACCOUNTS`의 `host_alias`가 1:1로 대응하게 할 것
- host-side Miniconda/venv/bootstrap을 container mode에서 어디까지 제거할지 경계를 정할 것
- canary 전용 enroot config/env 목록을 고정할 것
- validation lane DB/output가 live continuous 경로와 섞이지 않음을 유지할 것
- input source는 active `PLANS` 허용 전까지 `sample_only`를 유지할 것

## 결론

이 저장소에서 enroot는 바로 live continuous service 전체를 바꾸는 도구로 접근하지 않는다.
우선은 `Linux + slurm_batch + continuous_mode=False` validation lane canary에만 적용해, 환경 재현성과 SSH 운영 규약을 먼저 고정하는 것이 맞다.

repo-local `.ssh/config`와 `host_alias` alias 규약이 먼저 정리되면 계정 추가도 단순해진다.
그 다음 단계에서 sample canary green, CSV schema gate green, restart/rediscovery green이 쌓였을 때 live continuous lane 전환을 검토한다.
운영 용량 목표는 `prune 250 + preserve 20 = total 270 slots`로 고정한다.
