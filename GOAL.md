# GOAL — 0.3.9.1(솔브 가속) + 다계정 정석 안정화 (20잡 / 유효AEDT 250)

> 2026-06-23: 솔브 92분의 **근본원인 규명** + 사용자가 0.3.9.1 가속안 확정. 이 문서 = 현재 SSOT.

## ★ 핵심 발견 — 솔브 92분의 진짜 원인 (메시 오퍼레이션 + 지오메트리)
0.3.8 시절 솔브는 **median 11분**(p10 7.6 / p90 16.1, 구 DB 8039건)이었는데 0.3.9.0은 **median 87분 (~8배)**. 원인은 **솔버 설정이 아니라 두 변화의 곱**:
1. **메시 오퍼레이션 변경** (`peetsfea` 커밋 `0d67754`, 0.3.8.3): 단일 `Length1` 메시 → **`AssignSkinDepthOp` 신설**(구리 표면 25um×4층 조밀 표면메시). 0.3.8.0(11분)엔 이게 없었음.
2. **지오메트리 3배** (0.3.9.0 "y-span-freedom"): `width_max_mm` 360→**1080**, 코일 y-bounds 480→**1440**. 구리 표면적 3배 → skin-depth 표면메시 폭증.
- 즉 **큰 코일 위의 skin-depth 메시 op**이 시간 지배자. 패스/delta_S/코어는 0.3.8 이래 거의 불변(코어=4, delta_S=0.003, 패스 16/9/4).
- 실험 확증: round-1 단일노브(코어16/패스13/메시20k/skin2) **전부 65분 초과**(코어16은 레버 아님). round-2 조합 중 **skin=2 + 패스/delta_S 감속** 둘이 30분 내(R4 19분, R3 26분).
- 상세: `PLANS/solve_time_30min_tuning.html`.

## ★ 0.3.9.1 가속안 (사용자 확정 — 사용자가 peetsfea 패치)
> "스킨뎁스 적용, 시작 메시 10만 이하, 10% 리파인먼트로 30패스 — 훨씬 빠르고 정밀."
- skin-depth 메시 op은 **유지**하되(정확도), **시작 메시 ≤100k**, **percent_refinement 10%**, **30 passes**. 더 빠르고 정밀(사용자 단독 검증).
- 패치 주체 = **사용자**(peetsfea git에 v0.3.9.1 태그 생성 + 양 게이트 venv).

## 현재 상태 (이 세션에서 세팅 완료 — 서비스 down 대기)
- **서비스 전체 정지**(keeper/web/data), pg만 active. 양 게이트 잡 0(safe_scancel 정리됨). 실험잡도 취소.
- **DB 비움**: fresh `peetsfea`(0 테이블, 앱 기동 시 initialize() DDL). 구 0.3.9.0 데이터는 `peetsfea_0_3_9_0x`로 보존(리네임, 복사 아님). 0.3.8x는 `peetsfea_0_3_8x` 유지.
- **다계정 재활성**: keeper/web unit `EDT_ACCOUNTS=gate1-harry261:account_01:0,gate1-hmlee31:account_02:1`.
- **지령 = 계정당 125**(`edt_container_control.py` target_aedt), 2계정 → **시스템 총 유효AEDT 250**. 잡 **10/계정 → 총 20**(EDT_JOB_COUNT=10). 550 라이선스 풀 대비 여유(안정화).
- **버전핀 0.3.9.1**: `pyproject.toml` peetsfea@...@v0.3.9.1.
- **rc=134 근본픽스 프로덕션 반영**: `orchestrator.sh`에 `--mount /gpfs:/gpfs` 추가(실험에서만 있던 것). 양 게이트 home이 /gpfs/home1로 resolve → 양 계정 견고화(additive, harry261 회귀 없음). 라이선스 기본값 `1055@license-server`·EDT_ACCOUNT_ID는 이미 반영됨.

## 사용자 인계 (0.3.9.1 패치 후 기동 순서)
1. **사용자**: peetsfea git에 `v0.3.9.1` 태그(skin유지+시작메시≤100k+refine10%+30pass).
2. 로컬 runner venv 동기화: `cd ~/mnt/8tb/peetsfea-runner && uv sync`(또는 uv pip install) — 핀이 v0.3.9.1라 태그 생성 후 가능. **runner도 0.3.9.1이어야** sweep 지오메트리 일치.
3. 양 게이트 venv(harry261·hmlee31) 0.3.9.1 동기화 + 최신 orchestrator.sh 배포(/gpfs 마운트 포함).
4. **순차 기동**(동시 init race 회피): `systemctl --user start peetsfea-web` → 검증 → `peetsfea-keeper` → `peetsfea-data`.
5. 검증: 양 계정 잡 제출·account_id별 ingest·유효AEDT가 250(계정125)으로 수렴·키퍼 NR 안정·/enroot 평탄.

## ⚠ 인계 시 주의 (이전 다계정 라이브에서 관측된 잔여 이슈)
- **계정별 포트 주입**: 런처가 잡 sbatch에 ingest/lease/license 포트를 account.port_base에서 주입하는지 확인(미주입이면 hmlee31이 harry261 컨트롤러 7879를 따름 — 결과는 들어오나 제어 분리 안 됨). [[multi-account-ingest-port-collision]].
- **hmlee31 poller license=None**: lmstat 파싱이 hmlee31 ssh 환경에서 실패하면 solve=0 오독→과spawn. 기동 후 resource 스냅샷 license 값 확인.
- **노드 경합**: cpu2 ~10노드를 두 계정+타유저 공유. 250(125×2)은 이전 380보다 보수적이라 노드병목 여유 — 단 0.3.9.1로 솔브가 빨라지면(≈25분) 컨테이너 회전↑로 같은 유효AEDT에 더 많은 컨테이너 필요할 수 있음. n_max=220/계정 포화 시 관측 후 상향 검토. [[multi-account-live-node-contention]].
- **hmlee31 venv 실험 흔적**: ssw_ports.py(EXP_* env 읽기, 기본값=프로덕션 동일이라 무해)·edt_aedt_backend.py(stdout 노출) 패치 잔존. 0.3.9.1 재설치 시 깨끗이 덮임.

## 안전·원칙
- 코드 변경 후 `pytest tests/ -q`. 프로덕션 기동은 **순차**(advisory lock 폐기됨). 키퍼는 노드부족/예외에 안 죽음(`7b59f6b`), PID-ns 누수회수, 4h 잡 TTL.
- 되돌리기: 0.3.9.0 데이터=`peetsfea_0_3_9_0x`, 0.3.8x=`peetsfea_0_3_8x`. 코드=git(dev). 핀을 v0.3.9.0으로 되돌리면 구버전 복귀.

## 완료된 것 (히스토리)
- 다계정 control plane(accounts 리스트·계정별 스택·account_id 태깅, 162 passed). 데이터플레인(seq커서+Arrow :7884). 적분제어. PID-ns 누수픽스. 잡별 sshd. 4h TTL. 키퍼 견고화.
- hmlee31 rc=134 근본원인 = **/gpfs 미마운트**(sweep TOML이 /gpfs 경로 resolve, 컨테이너는 /home1만) → `--mount /gpfs:/gpfs`로 해결(실험 확증, 이제 프로덕션 반영).
- 솔브 92분 근본원인 규명(메시 op + 3배 지오메트리) → 0.3.9.1 가속안 확정.
- 참고: PLANS/solve_time_30min_tuning.html, PLANS/integral_container_control.html, [[local-enroot-aedt-setup]], [[edt-result-ingest-topology]].
