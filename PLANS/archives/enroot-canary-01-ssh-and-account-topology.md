# Enroot Canary 01: SSH and Account Topology

## 목적

이 문서는 repo-local `.ssh/config` 운영 규약과 7계정 lane 배치를 archive 설계 기준으로 고정한다.
active `PLANS`의 실행 기준을 바꾸는 문서가 아니라, enroot canary 구현 전에 SSH/계정 토폴로지를 결정 완료 상태로 넘기기 위한 참고 문서다.

## 핵심 규약

- `host_alias`는 실제 host/IP가 아니라 저장소 루트 `.ssh/config`의 `Host` alias다.
- launcher-side 모든 `ssh`/`scp`는 `-F <repo>/.ssh/config`를 사용한다.
- 전역 `~/.ssh/config`는 기준에서 제외한다.
- control-plane reverse tunnel은 기존 explicit `control_plane_return_host/user/port`를 유지한다.
- 계정 추가는 코드 수정이 아니라 `.ssh/config`와 `PEETSFEA_ACCOUNTS` 갱신으로 끝나야 한다.

## Repo-Local `.ssh` 구조

권장 구조는 아래로 고정한다.

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

`config`는 alias 해석만 담당하고, 실제 host/user/key는 `Host` block에서 명시한다.

예시:

```sshconfig
Host gate1-harry
  HostName <real-host-or-ip>
  User <remote-user>
  IdentityFile /home/peets/mnt/8tb/peetsfea-runner/.ssh/keys/gate1-harry
  IdentitiesOnly yes
```

## 7계정 배치

7계정 lane 배치는 아래로 확정한다.

| lane | aliases |
| --- | --- |
| `preserve` | `gate1-r1jae262`, `gate1-wjddn5916` |
| `prune` | `gate1-harry`, `gate1-dhj02`, `gate1-jji0930`, `gate1-hmlee31`, `gate1-dw16` |

이 배치는 아래 운영 의미를 가진다.

- `preserve`는 `slots_per_job=1` lane으로 본다.
- `prune`는 `slots_per_job=5` lane으로 본다.
- 둘 다 계정당 `max_jobs=10`을 목표 채움 기준으로 본다.

## `PEETSFEA_ACCOUNTS` 예시

launcher-side 환경 변수 예시는 아래처럼 고정한다.

```text
PEETSFEA_ACCOUNTS=account_01@gate1-harry:10,account_02@gate1-dhj02:10,account_03@gate1-jji0930:10,account_04@gate1-hmlee31:10,account_05@gate1-dw16:10,account_06@gate1-r1jae262:10,account_07@gate1-wjddn5916:10
```

여기서 중요한 점은 아래다.

- `PEETSFEA_ACCOUNTS`는 host/IP/User/IdentityFile를 직접 담지 않는다.
- 그 해석은 전부 repo-local `.ssh/config`가 맡는다.
- host 교체나 사용자명 변경은 `.ssh/config` 수정만으로 처리한다.

## 새 계정 추가 절차

새 계정 추가 절차는 아래 순서로 고정한다.

1. `.ssh/config`에 새 `Host` block을 추가한다.
2. `.ssh/keys/`에 새 key를 추가한다.
3. `PEETSFEA_ACCOUNTS`에 `account_id@host_alias:max_jobs[:platform:scheduler]`를 추가한다.
4. lane 분류가 필요하면 preserve/prune 표에도 반영한다.

## 왜 `ssh -F`를 강제하는가

`ssh -F <repo>/.ssh/config`를 강제하는 이유는 아래와 같다.

- 이 저장소의 계정 확장은 repo-local alias 운영을 전제로 한다.
- 전역 `~/.ssh/config` 상태에 따라 동작이 달라지면 재현성이 깨진다.
- `PEETSFEA_ACCOUNTS`의 `host_alias`를 코드와 문서에서 안정적으로 유지하려면 launcher-side SSH config가 저장소 기준으로 고정돼야 한다.

## 주의사항

- compute node 내부에서 control plane으로 되돌아오는 SSH 경로는 repo-local `.ssh/config`에 묶지 않는다.
- repo-local `.ssh/config`는 launcher-side SSH 경로에만 적용한다.
- 이 문서는 설계 문서다. active `PLANS`를 대체하지 않는다.
