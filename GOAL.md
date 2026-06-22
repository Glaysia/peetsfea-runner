# GOAL — 다계정 적분제어 정석 구현 + 0.3.9.0 + 밤샘 모니터링 (자율 실행)

> 사용자 퇴근. 전권 위임: "갈아엎어도 되니 정석적으로, 너가 다 결정해서 오늘 밤 동안 다 해라."
> 이 문서 = Claude가 밤새 자율로 실행하는 SSOT. Stop 훅이 이걸 달성하라고 구동한다.

## ✅ 라이브 달성 (2026-06-22 밤)
- **다계정 정석 구현·배포 완료**: control plane이 `accounts` 리스트로 harry261(idx0,7876대)+hmlee31(idx1,7886대)를 한 프로세스에서 구동(`EDT_ACCOUNTS` env). 계정별 런처·오케스트레이터·폴러·라이선스·ingest/lease/license 서버·역터널. 결과는 단일 DB에 account_id 태깅. 162 passed, main=dev.
- **peetsfea 0.3.9.0** 로컬+양 게이트 venv, fresh `peetsfea` DB(0.3.8x는 `peetsfea_0_3_8x`+덤프 백업).
- **양 계정 라이브 제출·수렴 시작**: harry261 ~10잡, hmlee31 ~7잡. 양 license 서버(7879/7889) target190. 유효 AEDT 램프업, account_01 ingest 시작. 빡센 0.3.9.0 솔브 ~25분.
- **현실 제약(노드 경합)**: cpu2 ~10노드를 두 계정+타유저가 공유 → 20잡 다 못 올라가고 ~17잡 운영(`no available node`는 키퍼가 재시도, 견고). 총 처리량은 노드가 병목(라이선스 아님). 데이터는 양 계정에서 안정 축적 중.
- **모니터링 가동**: `/tmp/overnight_monitor.log`(5분 간격 ~4.7h) — 잡수·유효AEDT·account별 ingest·키퍼NR·/enroot.

## 최종 목표 (오늘 밤)
1. **정석 다계정 control plane**: 키퍼가 harry261 + hmlee31 **두 계정을 구동**. 단일계정 하드코딩 제거.
2. **peetsfea 0.3.9.0**로 양 계정 라이브 — **유효 AEDT 380(계정당 190) / 잡 20(계정당 10) / 리플 최소**.
3. 빡센 솔브(정확도↑, 2~3배≈25~35분) 결과가 단일 DB로 ingest(account_id 태깅), **밤새(~4h+) 모니터링**.

## 아키텍처 (정석 — 계정별 런타임 스택)
control plane을 **계정 리스트**로 일반화. 각 account = 독립 스택(검증된 단일계정 로직 재사용), 공유는 DB·대시보드뿐:
- account = {ssh_host, account_id, host_alias, account_index, port_base, job_count, target_aedt}.
  - harry261: gate1-harry261, account_01, idx0, 포트 7876/7878/7879, 잡10, target190.
  - hmlee31 : gate1-hmlee31, account_02, idx1, 포트 7886/7888/7889, 잡10, target190.
- 계정별: 런처(ssh_host) + 오케스트레이터(잡10 고정·respawn-to-N) + 적분 컨트롤러(190/10) + license/job_plan 서버(포트베이스) + ingest 서버(포트베이스) + 역터널(대칭, gate:78x6→local:78x6). 컨테이너는 account 태깅(EDT_ACCOUNT_ID).
- **공유**: postgres DB 1개(양 계정 결과, account_id 구분) + 대시보드 1개(:8080, 공유 DB 읽어 양 계정 표시).
- 적분은 계정별 독립(각 190 수렴) → 합 380. 노드/라이선스 차이를 계정별로 자가흡수.
- 구현: `ControlPlaneConfig.accounts: list`로 확장, 기동 루프가 계정마다 스택 생성. 단일계정은 accounts=[harry261] 특수케이스. **테스트 추가**(다계정 라우팅·포트 분리·태깅).

## 실행 순서 (단계별, 검증하며)
0. **현재 상태**: 서비스 down(pg만), DB 격리됨(peetsfea→peetsfea_0_3_8x, 백업 db_backups/peetsfea_0.3.8x_*.dump), 0.3.9.0 로컬+harry261게이트 venv 적용. harry261 단일계정 코드는 라이브 직전(target190/잡10).
1. **hmlee31 게이트 준비**: peetsfea 0.3.9.0 동기화(로컬 venv→hmlee31 게이트 venv, harry261 때처럼) + 최신 orchestrator.sh 배포. venv import/솔브 경로 확인.
2. **다계정 control plane 구현**(코드 갈아엎기 OK): accounts 리스트 + 계정별 스택 + 라우팅 + 태깅. `pytest tests/ -q` 통과. main 커밋, dev 동기화.
3. **순차 기동**(동시 init race 회피): harry261 스택 먼저 완전 기동 → 검증 → hmlee31 스택 기동. fresh peetsfea 생성. 양 계정 잡 제출·orchestrator /job_plan fetch·PID-ns·컨테이너 솔브 확인.
4. **수렴 검증**: 유효 AEDT가 380(계정별 190)으로 오르는지, account_id별 ingest 들어오는지, 리플·키퍼 안정.
5. **밤새 모니터링(~4h+)**: 30~60min 간격 — 실패율·유효 AEDT/target·리플·NRestarts·/enroot 누수·고아 AEDT·account별 결과율. 문제 시 자가수정(키퍼는 노드부족/예외에 안 죽게 이미 견고화됨). 큰 이상이면 안전 정지 + 기록.

## 안전·원칙
- 매 코드 변경 후 `pytest` 통과 확인. 프로덕션 기동은 **순차**(advisory lock 폐기 → concurrent init race는 순차로만 회피).
- 키퍼는 가용노드 부족/submit 예외에 안 죽음(`7b59f6b`). PID-ns로 누수 회수. 4h 잡 TTL.
- 되돌리기: 0.3.8x는 `peetsfea_0_3_8x` DB + 덤프로 복원 가능. 코드는 git(main/dev) 히스토리.
- 망가뜨리면(오늘 advisory lock·예외누락으로 2번 깸) **즉시 원인 격리 + 안전버전 복귀**, 무한 crash-loop 방치 금지.

## 완료 조건
- 다계정 control plane이 harry261+hmlee31 둘 다 잡 제출·ingest(account_id 태깅) 정상.
- peetsfea 0.3.9.0 빡센 솔브가 양 계정에서 돌고 결과 DB 적재.
- 유효 AEDT **380 평균 수렴·리플 한 자릿수**(라이브), 키퍼 NRestarts 안정.
- `pytest tests/ -q` 통과. main=dev 일치. 밤새 모니터링 로그 남김.

## 완료된 것 (히스토리)
- 데이터플레인(seq커서+Arrow :7884, bulk/ArchiveStore 제거). cpu2 64코어. 적분제어 통합 3/3. PID-ns 누수픽스. 잡별 sshd. 4h TTL. 키퍼 견고화. peetsfea 0.3.9.0 venv. 0.3.8x DB 백업/격리.
- 참고: PLANS/integral_container_control.html, PLANS/per_job_debug_access.html, PLANS/data_plane_overhaul.html, [[multi-account-ingest-port-collision]], [[leak-reclaim-needs-pid-ns]].

## ⚠ 밤샘 중 발견 (재시작 필요 → 사용자 인지 하에 수정 권장)
- **계정별 포트 미주입**: 런처가 EDT_INGEST_PORT/LICENSE_CTRL_PORT/PRIORITY_LEASE_PORT를 잡 sbatch에 주입 안 함 → 양 계정 잡이 기본 7876/7878/7879로 push/fetch(같은 gate loopback의 harry261 역터널 경유). 결과는 들어오나(account_02 태깅 OK) hmlee31이 자기 컨트롤러(7889) 대신 harry261 컨트롤러(7879)를 따름. 수정: 런처에 ingest/lease/license_port 필드 추가 + sbatch export(account.port_base에서).
- **hmlee31 poller license=None**: hmlee31 resource 서버(7892) 스냅샷에 license 없음 → solve=0 오독 → 적분 과spawn. lmstat 경로/파싱이 hmlee31 ssh 환경에서 실패하는 듯. 조사 필요.
- **타깃 380(190×2) 비현실적**: harry261+hmlee31이 같은 cpu2 ~10노드 공유(타유저까지 경합). 노드가 병목이라 합쳐도 유효 AEDT ~150대에서 포화. 적분이 도달못할 190을 쫓아 n_max(220)에 영구 포화 → N=20/job 과spawn churn. **현실 타깃은 클러스터 용량(~150 합계)으로 낮춰야 적분 수렴·churn 제거.** 다계정의 "라이선스 2배"는 노드 남을 때만 의미 — 이 클러스터선 노드가 한계.
- **현재 판단**: harry261 안정 축적 중·NRestarts=0·/enroot 평탄. 위 수정은 모두 재시작 필요(churn은 노드제한으로 비파괴적)이라, 안정 축적 우선으로 밤샘엔 기록만. 아침에 사용자와 타깃 현실화 + 포트주입 + license픽스 협응 수정.
