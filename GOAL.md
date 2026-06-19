# GOAL — HTML 정책 기준 코드 최신화

## 기준
- 최신 정책 SSOT: `PLANS/job_birth_controller.html`, `docs/architecture/new-architecture.html`.
- 현재 상태: HTML과 코드가 2분 홀짝 제어 정책을 같은 기준으로 따라야 한다.
- 목표: runner 코드와 테스트가 HTML 정책을 그대로 실행하도록 맞춘다.

## 구현 목표
- 상시 예열 풀/장수 AEDT 세션 재사용 전제를 제거한다.
- 런타임 단위는 `1 solve = 1 enroot container = 완료 후 완전 종료`로 둔다.
- 컨테이너는 `EDT_SLOT_COUNT=1`, `EDT_MAX_SIMS=1`로 1건 처리 후 종료한다.
- 잡 제어 루프는 현재 잡 수 구간을 보지 않는다.
- 매 2분마다 홀수 tick은 새 잡 4개 제출만 수행한다.
- 매 2분마다 짝수 tick은 가장 늙은 RUNNING 잡 1개 종료만 수행한다.
- 제출은 노드별 균등 분산이 필요하다.
- `squeue` 총량 상한은 15개다.
- 최근 운영 목표는 평균 RUNNING 9개 + 평균 AEDT/solve 120개다.
- RUNNING 9개 미만이면 짝수 tick의 가장 늙은 잡 종료를 건너뛴다.
- `squeue`가 15개로 꽉 찬 상태에서 stuck이면 가장 늙은 RUNNING 잡 1개 종료 + 새 잡 1개 요청으로 회복한다.
- 잡 TTL은 최대 20분으로 유지한다.
- 2분 루프 6회마다, 즉 12분마다 포화 제어를 평가한다.
- `elec_solve_hfss > 150`이면 가장 늙은 RUNNING 잡 1개를 추가 종료한다.

## LUT
- `solve <= 90`  → `N = 20`
- `solve <= 100` → `N = 15`
- `solve <= 110` → `N = 14`
- `solve <= 120` → `N = 13`
- `solve <= 130` → `N = 12`
- `solve <= 140` → `N = 11`
- `solve > 140`  → `N = 10`

## 주요 수정 대상
- `peetsfea_runner/edt_orchestrator.py`: 1~7 램프/10분 롤링/최소-live 제거, 2분 홀짝 루프 구현.
- `peetsfea_runner/edt_slurm_launcher.py`: squeue 15, node-even 제출, cpu2/gpu1/gpu2/gpu3 후보, gpu:1 백필 요청, stuck 회복에 필요한 상태/API 보강.
- `peetsfea_runner/edt_control_plane.py` 및 resource provider: solve 실측을 LUT/포화 제어에 공급.
- `tests/test_edt_orchestrator.py`, `tests/test_edt_slurm_launcher.py`: 새 정책 기준으로 갱신.

## 완료 조건
- `.venv/bin/python -m mypy peetsfea_runner` 통과.
- 관련 pytest 통과.
- 코드와 테스트에서 구정책 문구/동작(`1~7`, `8+`, `10분`, `최소-live`, `11개 보강`) 제거.
- HTML 문서와 코드 정책이 서로 모순되지 않음.
- 서비스 재시작/배포는 별도 요청이 있을 때만 수행.
