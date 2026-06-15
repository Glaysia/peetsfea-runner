# Repository Agent Rules

이 저장소는 **edtmgr 기반 AEDT 오케스트레이터**다(단일 계정 동시 90 시뮬 → 결과 DB/대시보드/아카이브).
전체 설계는 `PLANS/MASTER_PLAN.md`를 따른다. 아래는 에이전트가 반드시 지킬 규칙이다.

## 0. 기준 문서 (Plan Authority)
1. **단일 기준(SSOT): `PLANS/MASTER_PLAN.md`.** 전체 목표·아키텍처·6단계.
2. **현재 구현 범위: `GOAL.md`** (= MASTER_PLAN의 Phase 1, 1/6).
3. **의존 계약:** `PLANS/peetsfea_main.md`(main에 추가할 것+이유), `PLANS/peetsfea_runner.md`(runner 쪽 연동).
4. **시각화:** `docs/architecture/new-architecture.html`. (옛 `SPECS/*`·roadmap·legacy docs는 제거됨.)
5. 문서 간 충돌 시 **MASTER_PLAN 우선**.

## 1. 실행 방법 (가장 중요)
1. 사용자(나)는 **오직 `systemctl --user start|restart peetsfea-runner` 로만** 이 프로그램을
   실행/재기동한다. 정식 진입점은 이 **systemd user 서비스**다.
2. 따라서 코드 변경 후 "정상 동작"의 기준은 systemctl 경로다. 에이전트가 함수/스크립트를 직접
   돌려 보는 것은 **디버깅·검증 보조로만** 허용된다(정식 실행 경로를 대체하지 않는다).
3. systemd 유닛 수정이 필요하면 repo의 **`systemd/peetsfea-runner.service` 만** 수정한다.
   `~/.config/systemd/user/` 아래 심볼릭 링크는 풀거나 직접 수정하지 않는다.
4. (이전의 "CLI 금지 / `run_pipeline(config)` 단일 경로" 규칙은 **폐기**. 필요하면 CLI·스크립트·
   엔트리포인트를 추가해도 된다.)

## 2. 타입 체킹 (필수)
1. 신규/수정 코드는 **완전한 타입 어노테이션**을 갖는다(공개 함수 시그니처, dataclass 필드 포함).
2. 정적 타입 체커를 **strict로 통과**해야 한다:
   `.venv/bin/python -m mypy peetsfea_runner` (또는 pyright strict). 체커는 dev 의존성으로 설치.
3. `# type: ignore`는 외부 라이브러리 스텁 부재 등 **불가피할 때만**, 사유 주석과 함께.
4. 타입 에러가 남아 있는 변경은 완료로 보지 않는다.

## 3. 의존성 (필수)
1. 기능에 필요한 의존성은 **주저 없이 추가**한다. 검증된 라이브러리를 피하려고 직접 재구현하지 않는다.
2. 추가하면 반드시 **`pyproject.toml`의 `dependencies`에 박제**하고 `.venv`에 설치한다
   (`uv pip install --python .venv/bin/python <pkg>` 또는 동등 명령).
3. peetsfea 핀은 **0.3.2**로 맞춘다 (`peetsfea @ git+...@0.3.2`).

## 4. 실행 환경
1. 모든 실행/테스트/디버깅은 저장소 루트의 `.venv` Python(**`.venv/bin/python`**)을 사용한다.
2. Python **3.12** 고정.
3. `pytest`가 필요하면 `.venv/bin/python -m pytest ...`, 없으면 dev 의존성으로 설치 후 사용.

## 5. "테스트/확인"의 의미 (사용자 언어)
1. 사용자가 "테스트해봐 / 돌려봐 / 확인해봐"라고 하면 기본적으로 unit test가 아니라
   **실제 실행 검증**(systemctl 경로 또는 재현 실행)을 뜻한다.
2. 에이전트가 작성한 unit test는 **사용자가 명시적으로 요청할 때만** 보고의 중심으로 둔다.

## 6. 검증 환경 / 접근 (실제 AEDT로 검증 가능)
1. **Ansys(AEDT)는 로컬 PC와 슈퍼컴 양쪽에 설치돼 있다.** 따라서 edtmgr·ansysedt·pyaedt 동작을
   **로컬에서 실제로 띄워 검증**할 수 있고, 같은 것을 잡에서도 확인한다(목/페이크에만 의존하지 말 것).
2. **에이전트는 클러스터에 직접 접근 가능하다:** `ssh gate1-harry261` 로 게이트노드에 붙고,
   그 안에서 `srun ...`(예: `srun --pty -p cpu2 -N1 -n1 -c4 --mem=5G --time=00:05:00 bash`)으로
   계산노드를 할당받아 자유롭게 실행/검증할 수 있다.
3. 로컬 PC와 계산노드(SLURM 잡) **둘 다 enroot 3.5.0**로 동일하다(`/usr/bin/enroot`).
   **로컬 enroot 컨테이너에서 되는 컨테이너 동작은 잡 안에서도 거의 그대로 된다** → 컨테이너 레벨 작업
   (sqsh/마운트/컨테이너 내부 실행/ansysedt 기동)은 로컬에서 먼저 빠르게 재현·검증하고 잡으로 올린다.
4. 단, 컨테이너 밖 요소는 로컬과 다를 수 있으니 그대로 옮기지 않는다:
   SLURM 할당/스케줄링, 노드별 마운트(`/opt/.../AnsysEM`, 공유 `$HOME`), 라이선스 서버·네트워크
   토폴로지(역터널/sshfs). 이런 건 잡(`ssh gate1-harry261` → `srun`)에서 별도 확인한다.

## 7. 런타임 상태 / 데이터
1. 결과·관측 데이터는 **결과 DB(DuckDB)**와 **아카이브 저장소**에 둔다(MASTER_PLAN §2.7/2.8).
   대시보드는 DB를 read-only로 시각화하며 시뮬에 영향 주지 않는다.
2. 서비스 재기동은 **콜드 스타트**로 취급한다(진행 중 잡/시뮬 복구 없음, Q8).
3. 원격 `$HOME` 부트스트랩은 **멱등**이다: `rm -rf ~/*`로 비워져도 서비스 재기동으로 복구되어야
   하고(느려도 됨), 웜 캐시 상태에선 재빌드 없이 빠르게 시작해야 한다(MASTER_PLAN §2.9).
