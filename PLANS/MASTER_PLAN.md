# peetsfea-runner MASTER PLAN

> 상태: 확정(결정 Q1~Q8 반영) / 실행 분할 6단계
> 적용 버전: peetsfea-runner(date-ver), peetsfea-main → **0.3.2**
> 이 문서가 전체 목표·아키텍처의 단일 기준(Single Source of Truth)이다.
> `GOAL.md`는 이 PLAN의 **Phase 1(1/6)** 만 발췌한, "지금 먼저 구현할 범위" 문서다.

---

## 0. 한 줄 요약
단일 계정에서 정상상태 동안 **동시 90개**(잡 9개 × 잡당 시뮬 10개) AEDT 시뮬레이션을
끊김 없이 유지하고, 입력 큐(랜덤 샘플링) → 시뮬 → 결과 DB/대시보드 → 아카이브까지
하나로 묶는 오케스트레이터. ansysedt를 껐다 켜는 비용이 크므로 `edtmgr`가 warm 상태와
EDT 라이선스 점유를 유지한다.

---

## 1. 목표 토폴로지 (Target Topology)
| 단위 | 수량 | 비고 |
|------|------|------|
| 계정(account) | **1개** | 다계정은 아주 나중(지금 안 함, Q7) |
| 잡(job, SLURM sbatch) | 계정당 **9개** | 잡당 컨테이너 1개 |
| 컨테이너(enroot) | 잡당 1개 | runner + peetsfea-main 모두 설치 |
| ansysedt | 컨테이너당 **10개 상시 기동** | warm 유지 |
| edtmgr | 컨테이너당 **10개 상시 기동** | ansysedt 1개당 1개, **runner에 위치(Q1)** |
| 동시 시뮬레이션 | 계정당 **90개** (9×10) | 정상상태에서 항상 90개 유지 |
| 잡 수명 | **5시간** | 만료 시 진행 중 90개 **그냥 폐기**(Q8) |

- 기존 50-worker lease/worker/bundle 파이프라인은 **레거시(교체 대상)**. 본 토폴로지가
  새 기준이며 `PLANS/roadmap-tonight-*`, `PLANS/archives/*`는 보류/참고용.

---

## 2. 핵심 컴포넌트 아키텍처

```
            [Intake :7875]  sweep toml + N개
                  │  랜덤 샘플 → fixed toml × N
                  ▼
            [대기 큐 (fixed toml)]
                  │  순차 디스패치 (slot 가용 시)
   ┌──────────────┼───────────────────────────── account_01 ─────────┐
   │  job_1 (enroot 컨테이너)          ...        job_9              │
   │  ┌───────────────────────────┐                                  │
   │  │ slot_1: edtmgr ── ansysedt(grpc) ── (warm/license hold)      │
   │  │ slot_2: edtmgr ── ansysedt(grpc)                            │
   │  │   ...                                                        │
   │  │ slot_10: edtmgr ── ansysedt(grpc)                           │
   │  └───────────────────────────┘                                  │
   │  실제 시뮬 pyaedt가 edtmgr에 acquire → (pid, grpc port) 빌림     │
   │  → 시뮬 실행 → 완료 시 release → 다음 fixed toml                  │
   └──────────────────────────────────────────────────────────────────┘
                  │ 결과(입출력 파라미터·타이밍·load telemetry)
                  ▼                              project_dir(.aedt/.aedtresults)
            [결과 DB (DuckDB)]  ◀─── [load balancer: CPU/mem 피드백 제어]
                  │                              │
                  ▼                              ▼
        [대시보드 :8080 (read-only)]      [아카이브 저장소 2TB, 압축, FIFO eviction]
```

### 2.1 edtmgr (AEDT 매니저) — runner 내부
- **목적:** ansysedt는 켜고 끄는 비용이 매우 크고, 끄면 EDT 라이선스를 놓친다.
  edtmgr는 **별도 시뮬이 아닌 관리용 pyaedt 세션**을 상시 물고 있어
  (1) ansysedt를 warm 유지하고 (2) 라이선스 점유를 유지한다.
- **구성:** 컨테이너당 10개 상시 기동(ansysedt 1개당 edtmgr 1개). 각 edtmgr는 자신의
  ansysedt를 `-ng -grpcsrv <port>` 로 띄우고 관리 세션을 `close_on_exit=False` 로 붙여 둔다.
- **대여 프로토콜(로컬 IPC, 컨테이너 내부):**
  - `acquire`: 관리 세션의 점유만 잠깐 놓아 주고(ansysedt는 죽이지 않음, 라이선스 유지)
    `{pid, grpc_port}` 반환. 시뮬 pyaedt가 같은 ansysedt에 grpc로 접속해 실행.
  - `release`/`done`: 시뮬이 성공적으로 반환하면 관리 세션을 다시 붙이고 다음 요청 대기.
- **타임아웃/장애 처리(Q2 반영):**
  - 시뮬 자체 워치독이 **60분**에 abort → **마지막 완료 패스 기준 리포트** 산출.
  - edtmgr 백스톱: 대여한 채 **65분**까지 반환 안 되면 해당 시뮬 pyaedt 세션 +
    ansysedt를 `SIGKILL(-9)` → 재기동 → 다음 요청 대기.
  - 상시 liveness 체크: 대여 중 ansysedt가 죽으면 즉시 감지·재기동(해당 대여는 실패 처리).

### 2.2 시뮬레이션 실행 정책
- 시뮬 1개 목표 ~**40분**(마지막 패스 후 리포트 저장), 하드 abort **60분**, edtmgr 강제종료 **65분**.
- 각 슬롯에서 fixed toml을 **순차 연속** 실행(끝나면 즉시 다음). 90슬롯 → 항상 90개 동시.
- 실행 진입점은 기존 단일 시뮬 API 계약 재사용:
  `primitive(candidate_toml_text, output_dir=, seed=, mode=)`
  (`peetsfea_runner/single_simulation_api.py:62,116`), 단 자체 ansysedt 기동 대신
  edtmgr가 준 grpc 세션에 접속(0.3.2 계약).

### 2.3 로드 밸런서 (Q3 반영)
- **문제:** 90개를 같은 시각에 시작하면 패스가 커지는(자원 최대) 구간이 겹쳐
  노드 부하 스파이크 → 활용률·스루풋 저하. 부하 곡선 근사 `t^1.5`.
- **신호:** 노드/슬롯의 **CPU·메모리** 사용률(주기 샘플링).
- **기법:** 이미 검증된 알고리즘/제어공학 기법을 적용한다 —
  - 시작 시각 **스태거(stagger)** 로 위상 분산.
  - 신규 시뮬 **admission control**: 측정 CPU/mem 사용률을 목표치로 수렴시키는
    **피드백 제어(PID 또는 EWMA 기반)** + 과부하 시 백오프(AIMD류)로 동시 시작 억제.
  - 매시간 단위 재최적화.

### 2.4 자원 / 파일시스템 규칙
- `/dev/shm`, `/tmp` **사용 금지** → 잡 전용 `job_tmpfs` / `job_disk`.
- 잡 시작 시 `/enroot/{USERNAME}_{SLURM_JOB_ID}` 생성, 잡 종료 시 삭제.

### 2.5 라이선스 (Q6)
- 라이선스는 충분하므로 상한은 고려하지 않는다. edtmgr 관리세션으로 상시 점유만 유지.

### 2.6 입력 인테이크 서비스 `:7875` (추가요건 B)
- `localhost:7875` 가 **peetsfea sweep toml + 개수 N** 을 받는다.
- **인입 toml 계약:** 7875로 들어오는 toml은 **온전한 sweep toml**이다(필드를 채워 주는 정규화 없음).
  단, 각 파라미터 **range(상하한)가 기준 sweep toml(`examples/0.3.0_sweep.toml`류)의 range 이내
  (= range subset)**여야 하고, **기준보다 넓으면 애초에 실패**시킨다.
  (기준 범위 정의·검증·샘플링은 peetsfea 0.3.2 책임 → `PLANS/peetsfea_main.md`)
- 흐름: 온전한 sweep toml 수신 → peetsfea가 **범위 검증(기준 이내인지)** → 통과 시
  **랜덤 샘플링으로 fixed toml × N** 생성 → **대기 큐** 적재 → 슬롯 가용 시 순차 디스패치 → 시뮬 후 DB 반영.

### 2.7 결과 DB + 대시보드 `:8080` (추가요건 A)
- 모든 누적 데이터(load, 시뮬 시간/패스, 입출력 파라미터, 슬롯/잡/계정, 아카이브 위치)를
  담는 **결과 DB**(DuckDB; 기존 `single_simulation_store.py` 확장).
- `localhost:8080` 대시보드는 이 DB를 **읽기 전용**으로 시각화/확인만. 시뮬에 영향 주는
  입력은 받지 않는다.
- 주의: 제어플레인 durable truth는 여전히 파일/큐. DB는 **관측·결과 저장소**로 한정.

### 2.8 아카이브 저장소 (추가요건 C)
- 시뮬 종료 시 슬롯의 `project_directory`(`project_name.aedt`,
  `project_name.aedtresults` 등 포함; 디렉토리명 규칙 미정)를 **로컬 별도 저장소**에 저장.
- **20GB 단위 묶음 압축(solid archive):** 완료된 project_directory들을 누적하다 묶음 크기가
  ~20GB에 도달하면 **여러 폴더를 하나의 압축파일로** 만든다. 여러 aedt 산출물은 폴더 간
  중복이 커서 묶어서 압축할 때 효율이 크게 좋아진다(복원이 다소 느려도 무방).
- **eviction:** 전체 버퍼 **2TB**. 2TB를 넘을 때마다 **압축파일 1개(가장 오래된 것)를 통째로
  삭제**한다 — 항목 단위가 아니라 20GB 묶음 파일 단위 FIFO. 복잡한 로직 없이 단순하게.

### 2.9 부트스트랩 / 원격 $HOME 프로비저닝
- **공유 $HOME 특성:** 슈퍼컴 게이트노드·계산노드 모두에서 접근 가능한 `$HOME`에는
  sqsh 이미지(`~/runtime/enroot/aedt.sqsh`)·스크립트·miniconda·repo 정도를 보관한다.
- **멱등 자가복구(필수):** `$HOME`이 `rm -rf ~/*`로 비워져도, **로컬 PC 서비스를 재시작하면
  부트스트랩이 처음부터 다시 수행되어 정상 상태로 복구**되어야 한다(느려도 됨).
- **`$HOME/Ansoft` 유저 설정 = 공유 마운트 필수 아티팩트(중요):** `$HOME/Ansoft`는
  **모든 컨테이너가 공유 마운트**해서 AEDT 유저 설정을 공유하는 디렉토리다. 내부에
  `ElectronicsDesktop<ver>/config/*.hpc_user.XML`(노드별 HPC 설정 `n001..n116` 등),
  `gate1.hpc.cfg`/`gate1.hpc_user.XML`, `PersonalLib` 가 들어 있다.
  - **비어 있으면 안 됨:** AEDT 최초 기동 시 **라이선스 서버가 신규유저 확인**을 이
    디렉토리(유저 설정) 기준으로 수행하므로, `Ansoft`가 비어 있으면 신규유저 처리로
    걸려 정상 기동이 막힌다. 즉 부트스트랩이 **반드시 `$HOME/Ansoft` 유저 설정을
    먼저 시드(seed)/복원**해 놓아야 한다(와이프 후 복구 경로에서도 동일).
  - **반영:** 부트스트랩 readiness 점검 항목에 `$HOME/Ansoft` 유저 설정 존재/완전성을
    추가하고, 없으면 시드 아티팩트(repo/이미지에 보관한 기준 `Ansoft` 설정)에서 복원.
    이 디렉토리는 컨테이너에 **공유 마운트(RW 공유 주의)** 로 노출.
  - **주의:** §9의 `Project*` 청소는 **이 설정 디렉토리/파일을 절대 건드리면 안 됨**
    (`config/`, `*.hpc_user.XML`, `PersonalLib`, `ElectronicsDesktop*/` 보존).
- **웜 캐시 우선(필수):** 평소에는 sqsh·캐시·스크립트가 이미 준비돼 있으므로 readiness
  점검만 하고 **재빌드 없이 빠르게** 시작해야 한다(정상 경로에서 느리면 안 됨).
- **구현 기반(기존 재사용):** readiness 프로브가 아티팩트 존재를 판정해 필요할 때만 부트스트랩
  (`bootstrap_needed = not runtime_path_ok or not env_ok or not python_ok or not binaries_ok`,
  `peetsfea_runner/scheduler.py:567`), `RUNTIME_PROBE_CACHE_TTL`(30분) 캐시,
  이미지 빌드 `enroot_image_bootstrap.sh`, 원격 설치 `scripts/remote_bootstrap_install.sh`.
  sqsh 계약 버전 `_ENROOT_IMAGE_CONTRACT_VERSION`(`scheduler.py:138`)은 0.3.2에 맞춰
  `...peetsfea031` → `...peetsfea032`로 bump.
- **핵심:** 부트스트랩은 **존재 검사 → 없으면만 재생성**(idempotent)이라, 와이프=느린 복구 /
  웜=빠른 시작 두 경로를 모두 만족한다.

---

## 3. 결정 사항 (Q1~Q8, 확정)
| # | 결정 |
|---|------|
| Q1 | edtmgr는 **runner**에 둔다(컨테이너에 runner+main 둘 다 설치). |
| Q2 | 시뮬 abort **60분**(리포트), edtmgr 강제종료 **65분**. |
| Q3 | 부하 신호 = **CPU·메모리**, 검증된 제어공학/알고리즘 LB 기법(피드백+스태거). |
| Q4 | 상세는 본 **MASTER_PLAN**에 전부, **GOAL.md = Phase 1(1/6)** 먼저 구현. |
| Q5 | GPU/CPU 테스트는 지금 안 함. 통계 쌓이면 정책화. |
| Q6 | 라이선스 충분 → 상한 미고려. |
| Q7 | 다계정은 아주 나중. 지금 단일 계정. |
| Q8 | 5h 만료 시 진행 중 90개 **폐기**(드레인 로직 없음). 리소스 낭비가 크면 추후 잡을 6~8h로 늘려 대응. |

---

## 4. 실행 분할 (6단계) — 각 1/6
> 의존: Phase 1이 코어 수직 슬라이스. 큐/DB/아카이브는 Phase 1에서 최소 형태로 stub 후 후속 단계에서 본격화.

### Phase 1 — 코어 단일 컨테이너 (= GOAL.md, **지금 구현**)
- **범위:** 단일 잡/단일 enroot 컨테이너 안에서 edtmgr 10 + ansysedt 10 warm 기동,
  대기 큐(수동 시드 가능)의 fixed toml을 슬롯에 순차 디스패치하여 단일 시뮬 실행.
- **포함:** edtmgr 대여 프로토콜(acquire/release), warm·라이선스 유지,
  60/65분 타이밍, `/enroot/{USER}_{SJOB}` lifecycle, `/dev/shm`·`/tmp` 비사용,
  peetsfea **0.3.2** warm-AEDT 접속 계약, 결과를 기존 `single_simulation_store`에 기록.
- **제외:** 9잡 오케스트레이션, 본격 LB, 7875/8080 서비스, 2TB 아카이브(경로만 확보).
- **수용 기준:** 컨테이너 1개에서 fixed toml N개가 10슬롯으로 순차 처리되어 각 결과/리포트가
  산출되고, ansysedt가 시뮬 사이에 죽지 않고 라이선스를 유지함을 확인.

### Phase 2 — 계정 내 9잡/90 동시 오케스트레이션
- 9잡 제출·생애주기 관리, 잡당 컨테이너 1개, 슬롯 90개 풀, 5h 만료 시 폐기(Q8).
- 잡 단위 `/enroot/{USER}_{SJOB}` 생성/정리, 잡 재기동(드레인 없음).
- **수용:** 정상상태에서 동시 90개 유지가 관측됨.

### Phase 3 — 로드 밸런서
- CPU/mem 샘플링 → 피드백 제어(PID/EWMA) + 시작 스태거 + AIMD 백오프, 매시간 재최적화.
- **수용:** 동시 시작 대비 노드 부하 스파이크 완화·활용률 상승이 데이터로 확인.

### Phase 4 — Intake 서비스 `:7875`
- sweep toml + N 수신 → 랜덤 샘플 fixed toml × N → 대기 큐 적재 → 순차 시뮬 → DB 반영.
- **수용:** sweep 1건 투입 시 N개 fixed toml이 큐를 통해 처리·기록됨.

### Phase 5 — 결과 DB + 대시보드 `:8080`
- DuckDB 스키마 확장(load/timing/IO param/archive ref), 8080 read-only 대시보드.
- **수용:** 누적 데이터가 대시보드에서 조회됨, 시뮬에 영향 없음.

### Phase 6 — 아카이브 저장소
- project_directory를 누적 → **20GB 단위 묶음 압축(solid)**, 2TB 버퍼 초과 시
  **가장 오래된 묶음 파일 1개 삭제**(파일 단위 FIFO), (느린) 복원 경로.
- **수용:** 묶음이 ~20GB마다 생성되고, 2TB 초과 시 오래된 묶음 파일부터 삭제되며 신규 저장이 지속됨.

---

## 5. peetsfea-runner 변경 계획
1. 토폴로지/구성 재정의: 단일 계정 / 9잡 / 잡당 컨테이너 1 / 컨테이너당 ansysedt·edtmgr 10 /
   동시 90 / 잡 수명 5h. 현 `DEFAULT_SLURM_JOB_TIME_LIMIT="00:45:00"`
   (`peetsfea_runner/constants.py:6`) → 5h. 현 `runner.py:46,60-61`(slots_per_job,
   account_01 max_jobs) 정비.
2. **edtmgr(신규 모듈):** 컨테이너 내 10개 관리 서버 + 대여 프로토콜 + 60/65분 타이밍 +
   liveness 재기동. ansysedt grpc 기동/접속 로직은 기존 remote_job의 grpc 런치 패턴 참고.
3. **슬롯 디스패처:** 대기 큐 → 슬롯 acquire → `single_simulation` primitive 실행 →
   release → 결과 기록. 기존 `single_simulation_*` 경로 재사용.
4. **로드밸런서(신규):** CPU/mem 텔레메트리 + 피드백 제어 + 스태거(Phase 3).
5. **Intake `:7875`(신규)**, **대시보드 `:8080`(신규)**, **DB 확장**(`single_simulation_store.py`),
   **아카이브 저장소(신규)**.
6. peetsfea 기대 버전 `0.3.1` → `0.3.2`
   (`peetsfea_runner/single_simulation_api.py:19`, `single_simulation_remote.py:194`).
7. 레거시 lease/worker/bundle 경로는 교체 대상으로 표기 후 점진 제거.
8. AGENTS.md 준수: 실행은 `systemctl --user start|restart peetsfea-runner`(systemd user 서비스가
   정식 진입점), 엄격 타입체킹, 필요한 의존성은 pyproject에 추가, `.venv/bin/python`.

## 6. peetsfea-main 변경 계획 → 0.3.2
> 상세 계약은 `PLANS/peetsfea_main.md`. 여기서는 요약.
시뮬 프리미티브(`ssw_random_sample_reports.run_ssw_random_sample_reports_from_toml_text`) 기준:
1. **기존 warm ansysedt 접속:** edtmgr가 준 `(pid, grpc_port)` 세션에 접속해 실행
   (자체 ansysedt 기동/종료 금지).
2. **완료 시 깨끗이 반환:** 프로젝트 정리 후 edtmgr에 release 가능 상태로 마무리.
3. **패스/시간 예산:** ~40분 목표, **60분 하드 abort 시 마지막 완료 패스 리포트 산출**.
4. **기준 sweep 범위 정의 + 범위 검증:** 0.3.2가 기준 sweep 스키마/범위(SSOT)를 정의하고,
   7875로 들어온 **온전한 sweep toml의 range가 기준 이내인지 검증**(넓으면 실패)하는 API 제공.
5. **랜덤 샘플링 제공:** (검증 통과한) sweep toml → fixed toml × N 샘플 API(Intake가 사용).
6. **리포트/CSV 스키마:** 입력 파라미터 + 출력 변수(k_ratio, Lrx_uH 등) 유지.
7. **라이선스/AEDT 수명 비소유:** edtmgr가 관리.
8. 버전 `0.3.1` → `0.3.2`.

---

## 7. 참고 앵커 (코드)
- `peetsfea_runner/single_simulation_api.py:19`, `single_simulation_remote.py:194` — 기대 버전(→0.3.2)
- `peetsfea_runner/single_simulation_api.py:62,116` — 시뮬 프리미티브 호출(계약 지점)
- `peetsfea_runner/single_simulation_store.py` — 결과 DB(DuckDB) 확장 기반
- `runner.py:46,60-61` — 현 토폴로지(slots_per_job, account_01 max_jobs)
- `peetsfea_runner/constants.py:6` — `DEFAULT_SLURM_JOB_TIME_LIMIT`(→5h)
- `peetsfea_runner/scheduler.py:567` — `bootstrap_needed` 판정(멱등 부트스트랩)
- `peetsfea_runner/scheduler.py:138` — `_ENROOT_IMAGE_CONTRACT_VERSION`(`peetsfea031`→`peetsfea032`)
- `peetsfea_runner/enroot_image_bootstrap.sh`, `scripts/remote_bootstrap_install.sh` — sqsh/원격 설치
- `peetsfea_runner/single_simulation_remote.py:21` — sqsh 경로 `~/runtime/enroot/aedt.sqsh`
- `edtmgr`, Intake(:7875), 대시보드(:8080), 아카이브 — **신규(현재 코드 없음)**

## 8. 잔여 미정(소소)
- project_directory(슬롯 디렉토리) 명명 규칙.
- 아카이브 압축 포맷(예: 20GB 묶음 `tar.zst` solid / 7z solid)과 묶음 인덱스(어느 묶음에 어느 project가 들어있는지) 방식.
- 대기 큐 구현 형태(파일 큐 vs 인메모리+영속).
- LB 제어 파라미터 튜닝(목표 사용률, 게인, 스태거 간격).

## 9. 장기 과제 / 알려진 이슈 (급하지 않음)
> 당장 막는 문제는 아니지만 누적되면 디스크/운영에 부담. 시간 날 때 정리.

- **`$HOME/Ansoft`에 프로젝트 파일이 쌓이는 고질적 버그.** ansysedt/pyaedt가
  슬롯의 지정된 `project_directory`가 아니라 `$HOME/Ansoft` 아래에
  `Project<N>.aedt` / `Project<N>.aedtresults` / `.aedt.lock` / `.aedt.temp` /
  `script<N>.aedt*` 들을 계속 떨어뜨린다. 방치하면 홈 디렉토리가 무한정 불어나
  현재는 `rm -rf ~/Ansoft/Project*` (Project 산출물만, 글로빙 한정) 로 수동 청소 중.
  - **⚠️ 절대 `$HOME/Ansoft` 통째로 지우지 말 것:** 이 디렉토리에는 라이선스 신규유저
    확인에 필요한 공유 유저 설정(`ElectronicsDesktop*/config/*.hpc_user.XML`,
    `PersonalLib` 등, §2.9)이 함께 있다. 청소는 `Project*`/`script*` 잔재 **파일/디렉토리
    글로빙으로만** 한정하고 설정 디렉토리는 보존해야 한다.
  - **추정 원인:** ansysedt 기본 프로젝트/스크립트 출력 경로가 `$HOME/Ansoft`로
    잡혀 있고(슬롯별 작업 디렉토리/임시 경로 미지정), 비정상 종료 시 `.lock`/`.temp`
    잔재까지 남음.
  - **해결 방향(나중):** edtmgr가 ansysedt를 띄울 때 프로젝트/스크립트/임시 출력
    경로를 슬롯 전용 `job_disk`(`/dev/shm`·`/tmp`·`$HOME` 금지, §2.4) 아래로
    강제 고정하고, 시뮬 종료/슬롯 release 시 잔재(`.lock`/`.temp` 포함) 정리를
    루틴화. 단 `$HOME/Ansoft`는 §2.9대로 **유저 설정 공유 마운트로 계속 존재해야 하므로
    "$HOME 격리로 자연 해소"는 설정 디렉토리에는 적용되지 않음** — 출력 경로만 분리한다.
  - **연관:** §2.4(파일시스템 규칙), §2.9(Ansoft 유저 설정 공유 마운트),
    §6의 "완료 시 깨끗이 반환", §8(project_dir 명명).
