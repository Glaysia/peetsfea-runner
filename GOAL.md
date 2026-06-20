# GOAL — 안정 잡 + 컨테이너 적분제어 (리플 최소) · 다계정

## 지령 (setpoint)
- **유효 AEDT(동시 솔브, `elec_solve_hfss`) = 120**, **잡 = 10**, **리플 최소**.
- plant가 예측 가능해짐: GPU 노드 폐기 → cpu2 전용·균일(솔브 ~11.5분 일정). 적분제어가 깔끔히 수렴할 조건.

## 핵심 구조 전환 (홀짝·LUT 폐지)
근원: 잡-죽음이 **누수회수 + 제어**를 겸직 → 제어하려고 잡을 죽이면 통째(±14)로 출렁여 리플. 둘을 분리한다.

- **누수 회수 = 컨테이너 단위.** 컨테이너 `1솔브 = 죽음`(EDT_SLOT_COUNT=1, EDT_MAX_SIMS=1) → 솔브마다 OS가 메모리·gRPC 스레드·FD 전량 회수. 잡이 누수 때문에 죽을 이유 없음.
- **잡 = 고정 인프라 10개(안 죽임).** 64코어 cpu2 노드. 누수회수용 재활용이 필요하면 길게·stagger(웨이브 금지). 제어 목적의 잡-죽임 폐지.
- **제어 = 컨테이너 수 적분(I) 피드백.** 통째 잡이 아니라 컨테이너 ±1~3로 미세 actuate.
  - 매 tick(30~60s): `err = 120 - 유효AEDT`; `N += clamp(round(Ki·err), -3, +3)` (Ki≈0.3~0.5). 적분 → 정상상태 오차 0(평균 정확히 120).
  - 잡 10개에 균등 분배, 잡당 출생 큐로 **stagger**(노드당 콜드스타트 cap) → 코호트 제거.
- **수학(Little's Law):** 매끈한 출생률 λ=120/11.5≈10.4 컨테이너/min → 솔브중=120 일정(리플≈0). 리플은 λ 버스트에서만 생김.
- **폐지:** 2분 홀짝(submit4/kill1), LUT, 가장-늙은-잡 종료, squeue 15 stuck 회복, 12분 포화제어.

## 다계정 (harry261 + hmlee31)
- 게이트/계정을 파라미터화(ssh_host·역터널·게이트 home·SLURM 계정). harry261 하드코딩 제거.
- 로컬 컨트롤 = 다계정 brain: 두 게이트에 잡 제출, 둘 다 로컬 DB로 ingest. **license도 계정별 별도 체크아웃 = 총 용량 2배.**
- 부트스트랩은 **수동(Claude 실행 가능)·wipe-safe**: 게이트 데이터 다 지워도 재구축. (자동화 필수 아님.)

## 즉시 다음 단계 — 검증 먼저 (SSOT: `PLANS/leak_reclaim_test.html`)
1. **gate1-hmlee31 부트스트랩**(venv 복제+경로치환, enroot 이미지 재사용) → `venv OK` 확인.
2. **4h 테스트 잡**(per-solve, TTL=14400) — harry261 키퍼 켜둔 채 별도 잡으로:
   - 누수 회수 검증: RSS·스레드·FD·/enroot **평탄(추세≈0)** 이면 "잡 안 죽여도 됨" 확정.
   - 다계정 검증: hmlee31 잡이 ssh·sbatch·enroot·ingest 역터널·license 끝까지 동작.
3. 통과 시 → 런처/터널 계정 파라미터화 + 키퍼 통합 + 아래 제어 재작성.

## 완료된 것
- **데이터플레인 갈아엎기**(SSOT: `PLANS/data_plane_overhaul.html`) — 라이브 배포됨:
  - seq 증분 커서(트리거) + read API `:7884 /api/results?since=`(Arrow IPC 스트림). 학습은 변경분만.
  - `/results.parquet`·`:7877 bulk`·ArchiveStore 제거(FD死 부류 소멸). entrypoint 성공-삭제로 디스크 정리.
  - 프로세스: keeper / web / **data(read 평면)** / pg.
- **cpu2 전용 + 64코어** — gpu 노드 폐기(GPU 미사용 확인: nvidia-smi 0%, peetsfea는 gpus 넘기나 HFSS 미가속).

## 주요 수정 대상 (검증 통과 후)
- `peetsfea_runner/edt_orchestrator.py`: 홀짝·LUT·가장-늙은-잡 종료·포화제어 제거 → **잡10 고정 + 컨테이너 수 적분제어**.
- slot_service: 프로덕션 장수-AEDT형 → **per-solve(1솔브 컨테이너)형** 전환(누수 회수를 컨테이너로).
- `peetsfea_runner/edt_slurm_launcher.py` + 터널: **계정 파라미터화**(harry261/hmlee31).
- `peetsfea_runner/edt_control_plane.py`: 다계정 키퍼 통합, 적분제어 tick(30~60s) 배선.
- 테스트 갱신.

## 완료 조건
- `.venv/bin/python -m pytest tests/ -q` 통과.
- 유효 AEDT가 **120 평균·리플 한 자릿수**로 수렴(라이브 관측).
- 다계정(harry261+hmlee31) 둘 다 잡 제출·ingest 정상.
- 구정책(홀짝/LUT/가장-늙은-잡/12분 포화) 문구·동작 코드/테스트에서 제거.
- 서비스 재시작/배포는 별도 요청 시에만(harry261 주, hmlee31 검증).
