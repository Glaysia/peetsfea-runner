# Enroot Canary 02: Worker Runtime and Container Contract

## 목적

이 문서는 `slurm_batch` worker job 내부에서 enroot를 어떤 계약으로 실행할지 고정한다.
목표는 구현자가 host mode와 enroot mode의 경계를 더 이상 스스로 결정하지 않도록 만드는 것이다.

## 실행 모드 분리

worker 실행 모드는 아래 두 가지로 분리한다.

- host mode
  - 기존 Linux host 직접 실행 경로
  - Miniconda/shared venv/bootstrap을 계속 사용
- enroot mode
  - `remote_container_runtime="enroot"`일 때만 사용
  - pinned image 내부 Python/PyAEDT를 사용
  - host-side Miniconda/shared venv/bootstrap은 필수로 보지 않음

## public interface 초안

container mode를 위한 config/env 초안은 아래로 고정한다.

- `ssh_config_path`
- `remote_container_runtime`
- `remote_container_image`
- `remote_container_ansys_root`
- `remote_ansys_executable`

의도는 아래와 같다.

- `ssh_config_path`: launcher-side `ssh -F`/`scp -F`에 사용
- `remote_container_runtime`: `none|enroot`
- `remote_container_image`: pinned `.sqsh` 경로
- `remote_container_ansys_root`: host Ansys root, 기본값 `/opt/ohpc/pub/Electronics/v252`
- `remote_ansys_executable`: container 내부 최종 AEDT 실행 경로, 기본값 `/mnt/AnsysEM/ansysedt`

## validation rules

아래 규칙을 문서상 고정한다.

- `remote_container_runtime="enroot"`는 `platform=linux`, `scheduler=slurm`, `remote_execution_backend="slurm_batch"`에서만 허용
- Windows 경로에는 적용하지 않음
- `foreground_ssh`에는 적용하지 않음
- hot path에서는 `enroot import`, `enroot create`, `apt-get`을 금지

## readiness / preflight 전환

container mode에서 readiness/preflight 기준은 아래처럼 바뀐다.

기존 host bootstrap 기준:

- `module load ansys-electronics`
- host Miniconda
- host `.peetsfea-runner-venv`
- host `uv`
- host `pyaedt`

container mode 기준:

- `enroot` binary 존재
- pinned `.sqsh` image 존재
- `remote_container_ansys_root` 존재
- launcher-side SSH config/alias resolve 가능
- host storage guard와 `/tmp` 관련 metric 수집은 그대로 유지

즉 container mode에서는 Python/PyAEDT 문제를 host가 아니라 image가 책임진다.

## Worker payload 계약

enroot mode의 worker payload 계약은 아래로 고정한다.

- ENROOT path
  - `/tmp/enroot/runtime/$USER/$SLURM_JOB_ID`
  - `/tmp/enroot/cache/$USER/$SLURM_JOB_ID`
  - `/tmp/enroot/data/$USER/$SLURM_JOB_ID`
  - `/tmp/enroot/tmp/$USER/$SLURM_JOB_ID`
- host에서 `cd "$remote_container_ansys_root"` 먼저 수행
- `enroot start --root --rw --mount .:/mnt`
- case dir는 `--mount "$CASE_DIR":/work`
- container 내부 cwd는 `/work`
- `ANS_IGNOREOS=1`
- AEDT 실행은 `/mnt/AnsysEM/ansysedt -ng -grpcsrv <port>`
- Python/PyAEDT는 image 내부 경로를 사용

command skeleton은 아래로 고정한다.

```bash
cd "$REMOTE_CONTAINER_ANSYS_ROOT"

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
  "$REMOTE_CONTAINER_IMAGE" \
  /bin/bash -lc 'cd /work && export ANS_IGNOREOS=1 && /mnt/AnsysEM/ansysedt -ng -grpcsrv 50056'
```

## pinned image 계약

이미지는 하나의 shared path를 기준으로 본다.

예시:

```text
${remote_root}/runtime/enroot/aedt-ubuntu2404-pyaedt-0.25.1.sqsh
```

image는 최소한 아래를 포함하는 것으로 가정한다.

- Ubuntu 24.04 user-space
- Python
- PyAEDT
- `file`
- `perl`
- 추가 안정성 패키지

## pass / fail matrix

| check | PASS | FAIL |
| --- | --- | --- |
| launcher SSH | repo-local alias resolve | alias/config missing |
| runtime | `enroot` executable exists | `enroot` missing |
| image | pinned `.sqsh` readable | image missing |
| ansys root | host root exists and mount succeeds | host root missing or mount fail |
| worker start | `ansysedt -ng -grpcsrv` stays alive | process exits early |

## 금지사항

hot path에서 아래는 하지 않는다.

- `enroot import`
- `enroot create`
- `apt-get`
- host Miniconda 설치
- host PyAEDT 재설치

## 주의사항

- mount contract는 절대경로 직접 bind가 아니라 `cd` 후 `--mount .:/mnt`를 기준으로 한다.
- `/tmp` 사용량은 남기 때문에 bad-node 정책을 대체하지 않는다.
- 이 문서는 설계 문서다. active `PLANS`와 충돌하지 않게 canary-first 전환만 전제한다.
