# 로컬 enroot에서 ansysedt 실행 (개발기 검증)

> 개발기(5950x, **Ubuntu 25.10**)에서 ansysedt를 **enroot 컨테이너** 안에서 돌려 슈퍼컴과 동일한
> 실행 환경을 재현한다. 전체 아키텍처는 `PLANS/MASTER_PLAN.md`, runner 연동은 `PLANS/peetsfea_runner.md`.

## 1. 왜 컨테이너인가
- ansysedt(2025R2)는 Ubuntu **22.04/24.04만 지원**, 25.10에서는 호스트 직접 실행이 불가.
- enroot 이미지(`~/runtime/enroot/ubuntu2404-local.sqsh`, base `ubuntu:24.04`, glibc 2.39,
  python3.12 + pyaedt 0.25.1)가 **24.04 userspace**를 제공하고, ansysedt 본체는 호스트에서
  bind-mount한다. → 25.10 위에서도 정상 동작.
- GUI 시절 쓰던 **podman은 enroot 경로에선 불필요/미사용**. enroot 단독으로 완결된다.

## 2. 경로 일관성 (로컬 = 원격)
코드가 기대하는 슈퍼컴 정규 경로(상수):
- `single_simulation_remote.py:22` `DEFAULT_REMOTE_CONTAINER_ANSYS_ROOT = "/opt/ohpc/pub/Electronics/v252"`
- `edt_aedt_backend.py:32` `…/AnsysEM/ansysedt`, `license_policy.py:19` `…/licensingclient/linx64/lmutil`

로컬 실제 설치는 `~/.local/share/ansys_inc/v252` (AnsysEM 37GB + licensingclient). 정규 경로로
맞추기 위해 **심링크 하나**만 건다(영구):

```
/opt/ansys/v252  ->  /home/peets/.local/share/ansys_inc/v252
```

기존 `/opt/ohpc/pub/Electronics -> /opt/ansys` 심링크와 합쳐져 다음이 완성된다:

| 정규 경로 | 해석 결과 |
|---|---|
| `/opt/ohpc/pub/Electronics/v252/AnsysEM/ansysedt` | raw `.runtimeexewrapper` ✅ |
| `/opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil` | ✅ |

- `/opt/ansys`는 peets 소유 → **sudo 불필요**.
- ⚠️ `~/.local/share/ansysedt-podman/v252/AnsysEM/ansysedt`는 **podman 래퍼**다. enroot 마운트엔
  쓰지 말고 위 raw 설치를 써야 한다.

## 3. 실행 (코드와 동일한 마운트)
`single_simulation_remote.py:643`과 동일한 스킴:

```
enroot start --root --rw \
  --mount "/opt/ohpc/pub/Electronics/v252/AnsysEM:/mnt/AnsysEM" \
  --mount "/opt/ohpc/pub/Electronics/v252:/ansys_inc/v252" \
  --mount "/opt/ohpc/pub/Electronics/v252/licensingclient:/mnt/licensingclient" \
  <container> /bin/bash …
```

컨테이너 내부 env: `ANSYSEM_ROOT252=/mnt/AnsysEM`, `ANS_IGNOREOS=1`,
`ANSYSLMD_LICENSE_FILE=1055@172.16.10.81`, `PATH=/opt/miniconda3/bin:/mnt/AnsysEM:…`.

## 4. 라이선스 주의
- 로컬에서 `lmutil lmstat` / `lmgrd`은 **응답이 안 뜬다**(슈퍼컴에서만 정상). ANSYSLI 2325도
  refused로 보인다. **그러나 실제 라이선스 체크아웃은 정상** — 이 진단 출력은 무시한다.

## 5. 검증 결과 (2026-06-15)
`~/mnt/8tb/peetsfea-runner/examples/sample_short.aedt`(HFSS Terminal)로 enroot 내 end-to-end 확인:

| 항목 | 결과 |
|---|---|
| 컨테이너 userspace | Ubuntu 24.04.4 / glibc 2.39 (호스트 25.10 회피) |
| ansysedt 헤드리스 기동 | gRPC Desktop **~8–12초** 기동, pyaedt 연결 OK |
| HFSS solve | `analyze=True`, **Setup1 is_solved=True**, **21초** |
| 결과 추출 | `S(Box3_T1,Box3_T1) = -0.99994563` 실제 산출 |
| podman | **미사용** (enroot 단독 완결) |

> 참고: 단일 도체·리턴경로 없는 부실 Maxwell 2D 자기정적 모델은 4초만에 실패했는데, 이는
> 라이선스가 아니라 모델 부실 탓. 온전한 HFSS 프로젝트는 정상 solve.

## 6. edtmgr / 디스패처 실 검증 (2026-06-15)
`peetsfea_runner.edt_aedt_backend.RealEdtBackend` + `edtmgr` + `SlotDispatcher`를 위 컨테이너 안에서
실 ansysedt로 검증. 실행: `tests/run_smoke_in_enroot.sh` (로컬은 host conda 마운트 보정 —
`PEETS_SMOKE_PYTHON=/host-conda/bin/python`, `PEETS_SMOKE_CONDA_MOUNT=/home/peets/miniconda3:/host-conda`).

| 스모크 | 결과 |
|---|---|
| `tests/smoke_edt_backend.py` | start→gRPC Desktop warm **7.4s** / lend(관리 release)·reclaim(같은 port 재접속) / kill. **lend↔reclaim 사이 ansysedt 동일 pid·port 유지 = warm·라이선스 점유 확인** |
| `tests/smoke_edt_dispatcher.py` | 1슬롯·2아이템 순차(12.7s). 두 시뮬 모두 **같은 ansysedt pid·port(예: 1093868/55727)에 접속** = 시뮬 사이 죽지 않고 warm 재사용. 디스패처 acquire→실 grpc→시뮬 접속→release→다음 루프 실증 |
| `tests/smoke_edt_solve.py` | **실 HFSS solve를 디스패처를 통해**(40.2s). Setup1 solved(22s), **리포트 산출 `St(Box3_T1,Box3_T1)` vs `Freq`**. `_solve_primitive`가 peetsfea 0.3.2 프리미티브의 drop-in 자리표시자 — §7 "결과/리포트 산출"을 실 AEDT로 닫음 |

- 검증 범위: edtmgr warm/lend/reclaim/kill + 디스패처 순차·재사용 + 시뮬 grpc 실접속 + **실 HFSS solve/리포트 산출**.
- **0.3.2 연동만 잔여:** 위 `_solve_primitive`(직접 pyaedt solve) 자리에 **peetsfea 0.3.2 프리미티브**가
  들어가면 동일 경로로 프로덕션 시뮬. 0.3.2는 아직 다른 PC 개발 중.
- **부트스트랩 멱등:** `tests/test_enroot_bootstrap_idempotent.py`(웜→스킵 / 와이프·stale→재빌드) 그린.
  실제 `rm -rf ~/*`+재시작 복구는 파괴적이라 ops 런북(게이트 `image_is_current`가 복구 보장).
- 컨테이너 외 단위검증은 host `.venv`: `.venv/bin/python -m pytest tests/test_edt*.py tests/test_job_workspace.py tests/test_enroot_bootstrap_idempotent.py`
  + `mypy --strict`(7개 모듈) 그린.
