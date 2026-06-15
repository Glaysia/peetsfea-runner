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
- peetsfea 랜덤 샘플링으로 sweep → **fixed toml × N** 생성 → **대기 큐**에 적재.
- 큐는 슬롯 가용 시 순차 디스패치되어 시뮬 후 결과 DB에 반영.

### 2.7 결과 DB + 대시보드 `:8080` (추가요건 A)
- 모든 누적 데이터(load, 시뮬 시간/패스, 입출력 파라미터, 슬롯/잡/계정, 아카이브 위치)를
  담는 **결과 DB**(DuckDB; 기존 `single_simulation_store.py` 확장).
- `localhost:8080` 대시보드는 이 DB를 **읽기 전용**으로 시각화/확인만. 시뮬에 영향 주는
  입력은 받지 않는다.
- 주의: 제어플레인 durable truth는 여전히 파일/큐. DB는 **관측·결과 저장소**로 한정.

### 2.8 아카이브 저장소 (추가요건 C)
- 시뮬 종료 시 슬롯의 `project_directory`(`project_name.aedt`,
  `project_name.aedtresults` 등 포함; 디렉토리명 규칙 미정)를 **로컬 별도 저장소**에 저장.
- **압축** 적용(여러 aedt 묶음은 압축 효율이 좋음; 복원이 다소 느려도 무방).
- 버퍼 용량 **2TB**, 초과 시 **그냥 삭제(FIFO eviction)** — 복잡한 로직 없이 단순하게.

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
- project_directory 압축 저장, 2TB 버퍼 FIFO eviction, (느린) 복원 경로.
- **수용:** 2TB 초과 시 오래된 항목부터 삭제되고 신규 저장이 지속됨.

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
8. AGENTS.md 관례 준수: CLI 신설 없이 `run_pipeline(config)`/서비스 진입점, `.venv/bin/python`,
   durable truth=파일/큐(DB는 관측/결과).

## 6. peetsfea-main 변경 계획 → 0.3.2
시뮬 프리미티브(`ssw_random_sample_reports.run_ssw_random_sample_reports_from_toml_text`) 기준:
1. **기존 warm ansysedt 접속:** edtmgr가 준 `(pid, grpc_port)` 세션에 접속해 실행
   (자체 ansysedt 기동/종료 금지).
2. **완료 시 깨끗이 반환:** 프로젝트 정리 후 edtmgr에 release 가능 상태로 마무리.
3. **패스/시간 예산:** ~40분 목표, **60분 하드 abort 시 마지막 완료 패스 리포트 산출**.
4. **랜덤 샘플링 제공:** sweep toml → fixed toml × N 샘플 API(Intake가 사용).
5. **리포트/CSV 스키마:** 입력 파라미터 + 출력 변수(k_ratio, Lrx_uH 등) 유지.
6. **라이선스/AEDT 수명 비소유:** edtmgr가 관리.
7. 버전 `0.3.1` → `0.3.2`.

---

## 7. 참고 앵커 (코드)
- `peetsfea_runner/single_simulation_api.py:19`, `single_simulation_remote.py:194` — 기대 버전(→0.3.2)
- `peetsfea_runner/single_simulation_api.py:62,116` — 시뮬 프리미티브 호출(계약 지점)
- `peetsfea_runner/single_simulation_store.py` — 결과 DB(DuckDB) 확장 기반
- `runner.py:46,60-61` — 현 토폴로지(slots_per_job, account_01 max_jobs)
- `peetsfea_runner/constants.py:6` — `DEFAULT_SLURM_JOB_TIME_LIMIT`(→5h)
- `edtmgr`, Intake(:7875), 대시보드(:8080), 아카이브 — **신규(현재 코드 없음)**

## 8. 잔여 미정(소소)
- project_directory(슬롯 디렉토리) 명명 규칙.
- 아카이브 압축 포맷(예: per-project `tar.zst`)과 인덱스 방식.
- 대기 큐 구현 형태(파일 큐 vs 인메모리+영속).
- LB 제어 파라미터 튜닝(목표 사용률, 게인, 스태거 간격).
