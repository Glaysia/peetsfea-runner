# GOAL — 전체 production 시스템 + 정상상태 동시 ~100 연속 가동

> [`PLANS/MASTER_PLAN.md`](PLANS/MASTER_PLAN.md) **전체(Phase 1~6)** 를 구현하고, 정상상태에서
> **동시 ~100개 시뮬을 끊김 없이 무한 연속** 가동해 **입출력 데이터셋을 축적**하는 것이 최종 목표.
> 요청(:7875)이 없을 땐 **전역 설계공간 baseline 랜덤 샘플링**으로 슬롯을 채워 계속 시뮬한다.
> 실행: `systemctl --user start|restart peetsfea-runner`. 의존: peetsfea **0.3.5**(자동 GPU + CPU 폴백,
> solver cores 고정=4, `solve_telemetry`에 `gpu_used`/`gpu_device_name`/`solver_cores`).

## 0. 한 줄 요약
단일 계정에서 **동시 ~100개 AEDT 시뮬을 정상상태로 무한 연속** 가동한다. Intake(:7875) 우선순위 sweep을
먼저 처리하되 비면 `0.3.5_sweep.toml` 전역 설계공간을 baseline 랜덤 샘플링해 슬롯을 끊김 없이 채운다.
**각 시뮬의 입력(설계점)→출력(리포트 데이터셋)** 을 로컬 단일 DB에 적재하고, 무거운 `.aedt` 산출물은 따로
아카이브한다. 결과는 대시보드(:8080)와 `results.csv`로 조회.

## 1. 아키텍처 (구현 완료된 골격)
| Phase | 내용 | 상태 |
|------|------|------|
| 1 | 코어 컨테이너: edtmgr warm 풀 + 슬롯 디스패처 + 실 HFSS solve | ✅ (로컬·클러스터 실 AEDT) |
| 2 | 9잡/~100 오케스트레이션(잡 lifecycle, 10h 폐기·재기동) | ✅ (실 SLURM) |
| 3 | 로드 밸런서(ramp-up, CPU/mem EWMA + admission) | ✅ |
| 4 | Intake :7875 + 2-레인 가중 큐(baseline + 우선순위, 85:15) | ✅ (큐/인테이크) |
| 5 | 결과 DB + 대시보드 :8080 + `results.csv` | ⚠️ DB 적재는 OK, **CSV 출력 데이터셋 누락(문제 2)** |
| 6 | 아카이브(:7877 tar 스트림 → 20GB 묶음/FIFO) | ✅ (`edt_bulk_transfer`) |
| 운영 | systemctl 상시 가동, 동시 ~100 무한 연속 | ⚠️ **기동은 됐으나 데이터 미생산(문제 1)** |

**백채널 토폴로지(전부 결선·기동됨):** 로컬 systemd `--user` 데몬이 단일 두뇌. 슈퍼컴엔 DB 없음.
- **:7875** 공개 sweep intake(우선순위 요청 수신).
- **:7876** 슈퍼컴 전용 결과 ingest — 컨테이너가 결과 envelope를 gate 경유 ssh 역터널로 push → **로컬 단일 DuckDB**.
- **:7877** 슈퍼컴 전용 대용량 산출물 — `.aedt` project_dir를 tar.gz 스트림으로 push → `ArchiveStore`(20GB 묶음, FIFO) + **gpfs 원본 삭제**.
- **:8080** 로컬 read-only 대시보드 + `results.csv`.
- 9 SLURM 잡(edt-0..8), cpu2 잡=64 cpus(QOS `cpu2_limit` cap), 그 외 32. 컨테이너당 warm 11 AEDT(gRPC 연결 확인).

## 2. 현재 상태 — 기동됐으나 **완전 작동 불가** (실측 진단)
9잡 fleet·역터널·포트는 전부 LIVE지만 **DB 적재 0건**. 실측 결과 두 가지 문제가 막고 있다.

### 문제 1 — baseline 샘플링이 슬롯을 굶겨 solve가 0회 (✅ 수정됨)
**증상:** 각 컨테이너에서 `build_ssw_body_boxes`가 수백 회 반복되는데 HFSS solve·`.aedt` 생성·결과 envelope는 0.
출력 디렉토리·spool·DB 전부 비어 있음.

**원인(소스 확인):** baseline 자기공급은 `make_baseline_sampler(..., batch_size=1000)` →
`sample_fixed_candidates_from_toml_text(count=1000)` → `sample_ssw_fixed_tomls`의 **rejection sampling 루프**
(`ssw_design_space.py` 575-600): 후보마다 `load_ssw_fixed_spec()` + `build_ssw_body_boxes()`로 **cadquery geometry를
빌드해 유효성 검사**(실패 시 재시도). 즉 후보 1000개를 뽑으려면 **geometry를 1000+회**(거부분 포함 더) 빌드해야 하고,
1회 ~5초 → **첫 배치 한 개 채우는 데만 ~80분+**. 게다가 이 refill이 **슬롯 디스패치 경로(`queue.get()`)에서 동기로**
실행돼 그 동안 모든 슬롯이 새 후보를 못 받아 **단 한 건도 HFSS solve에 도달하지 못한다.**

**수정 방향(러너 측):**
1. **baseline refill을 백그라운드 워커 스레드로 분리** — 디스패치 경로(`queue.get()`)에서 절대 샘플링이 일어나지
   않게 한다. 슬롯은 버퍼에 후보가 있으면 즉시 가져가 solve를 시작.
2. **batch_size 축소 + 점진 보충** — 1000 → 작은 값(예: 슬롯 수의 1~2배)으로 저수위 때마다 조금씩 채워, 시작 즉시
   슬롯이 돌고 버퍼는 백그라운드로 천천히 채워진다.
3. (선택) 샘플 후보를 미리 디스크에 풀로 비축해 두고 거기서 당겨오는 방식도 가능(geometry 재빌드 회피).

**수용:** 기동 후 **수 분 내 첫 HFSS solve 시작**, 슬롯이 baseline만으로 끊김 없이 채워지고 결과가 DB에 적재된다.

### 문제 2 — `results.csv`에 출력 데이터셋이 빠져 있음 (✅ 수정됨)
**요구사항(확정):** `:8080`의 `results.csv`는 **무거운 `.aedt` 파일만 빼고 모든 정보**를 담아야 하고,
**입력·출력 데이터셋을 반드시 포함**해야 한다(설계점 → 시뮬 리포트 결과).

**증상/원인:** 결과 DB(`single_simulation_store`)는 이미 입출력 전부 보관한다 —
`point_values_json`(입력), `solve_telemetry_json`·`setup_pass_counts_json`·**`csv_text_by_report_json`(출력 리포트
데이터셋)**·`csv_paths_json`·`result_json`·`envelope_json`. 그러나 대시보드의 `rows_to_csv`는 **입력(in_*)과
pass(pass_*)만** 평탄화해 내보내고 **출력 리포트 데이터셋·telemetry를 누락**한다.

**수정 방향(`edt_dashboard.rows_to_csv`):** 한 행 = 한 시뮬로, 다음을 **모두 포함**:
- **입력:** `in_<param>`(point_values), `design_id`, `point_hash`, `seed`, 차원 수.
- **출력:** `tel_<k>`(solve_telemetry: gpu_used/solver_cores/시간 등), `pass_<setup>`(setup_pass_counts),
  그리고 **출력 리포트 데이터셋**(`csv_text_by_report`). 리포트는 다행(주파수 스윕 곡선)이므로 표현 방식은 §3에서 확정.
- **메타:** request_id, terminal_state, started/finished_at, account/host/**partition/node**, peetsfea_version.
- **제외:** 무거운 `.aedt`/`.aedtresults` 바이너리(이건 :7877로 아카이브, CSV엔 아카이브 참조만).

**수용:** `curl localhost:8080/results.csv` 한 방에 **각 시뮬의 설계 입력과 시뮬 출력(리포트 값)** 이 모두 들어 있어
그대로 분석/서로게이트 학습용 데이터셋으로 쓸 수 있다. (실패 행은 입출력이 비어 컬럼이 안 뜨고, **성공 행이
들어오면** `in_*`/`tel_*`/`pass_*`/`reports_json` 컬럼이 동적으로 나타난다.)

### 문제 3 — pyaedt Desktop 프로세스 전역 충돌로 solve 100% 실패 (⚠️ 실제 블로커, 우회 적용 중)
문제 1·2 수정 후 실가동했더니 **모든 solve가 AEDT attach 단계에서 실패**(7/7 `failed`):
`AssertionError: Raw Hfss.project_name must be str (actual=NoneType)`, `'Desktop' object has no attribute 'grpc_plugin'`.

**원인(양쪽 소스 확인):**
- 러너 `edt_aedt_backend.py:122-130`: 슬롯마다 pyaedt `Desktop(new_desktop=False, port=port)`를 **한 파이썬
  프로세스 안에** 생성(슬롯 11개 = Desktop 11개 + 각 primitive의 attach Desktop).
- peetsfea `ssw_ports.py:528` `_release_keeping_desktop_alive`: 매 시뮬 끝에 **`release_desktop()`**(프로세스 전역).
- **pyaedt의 Desktop은 프로세스 전역 싱글톤** → 한 프로세스에 Desktop이 여럿 공존하거나 한 슬롯이
  `release_desktop()`을 부르면 다른 슬롯들의 전역 Desktop 상태가 깨진다(grpc_plugin 소실, project_name None).
- **0.3.4 검증이 성공한 건 `EDT_SLOT_COUNT=1`(Desktop 1개, 충돌 없음)** 이었음 — 1↔11 차이와 정확히 일치.
- peetsfea의 attach/release는 단일프로세스 기준으론 옳다. **러너가 "한 프로세스에 슬롯 N개(스레드)"로 pyaedt의
  one-Desktop-per-process 제약을 위반**한 것이 근본 문제.

**우회(즉시, 적용됨):** `slot_service.sh` `EDT_SLOT_COUNT=1` — 컨테이너당 슬롯 1개 = 프로세스당 Desktop 1개 →
충돌 없음. 9 컨테이너 × 1 슬롯 = 동시 9 solve로 **실제 데이터부터 쌓는다**.

**정식 수정(후속, ~100 밀도 복원):** pyaedt가 one-Desktop-per-process이므로 슬롯을 **별도 OS 프로세스로 격리**해야
한다. 후보: ① 컨테이너당 워커 서브프로세스 N개(각 1슬롯=1 Desktop)를 entrypoint가 spawn, 또는 ② `slot_service.sh`가
1-슬롯 컨테이너를 노드당 N개 띄움(컨테이너=프로세스=Desktop). 어느 쪽이든 디스패처/edtmgr/backend가 슬롯을
스레드가 아닌 프로세스 경계로 다루도록 바꾼다.

## 3. 결과 데이터 계약 (results.csv가 담아야 할 것)
> 리포트 출력 데이터셋(`csv_text_by_report`)의 CSV 내 표현 형식은 구현 시 확정(후보: ① 시뮬당 1행 + 리포트
> 원문을 JSON 컬럼으로 임베드, ② 시뮬×리포트행 long-format, ③ 핵심 출력 스칼라만 추출 컬럼화). 어느 쪽이든
> **데이터 손실 없이**(무거운 `.aedt` 제외) 입력→출력 전체가 복원 가능해야 한다.
- 한 시뮬의 **입력 설계점 + 출력 리포트**가 같은 레코드로 연결돼야 함(자동 벤치마크/서로게이트의 원천).
- partition/node 기록으로 **GPU vs CPU·파티션별 성능 자동 벤치마크**가 데이터로 쌓임(0.3.5 `gpu_used` 포함).

## 4. 최종 수용 기준
`systemctl --user start peetsfea-runner` 후:
1. **수 분 내 첫 solve 시작**, 동시 ~100 시뮬이 정상상태로 무한 유지(요청 없어도 baseline 자기공급, **슬롯이
   샘플링에 막히지 않음**).
2. 모든 시뮬의 **입력→출력 데이터셋이 로컬 단일 DB에 적재** → `:8080`/`results.csv`로 조회, CSV가 무거운 `.aedt`만
   빼고 입출력 전부 포함.
3. `.aedt` project_dir은 :7877로 아카이브(20GB 묶음, FIFO)되고 gpfs 원본 삭제.
4. 7875로 sweep 투입 시 N개 우선 처리, backlog 중에도 baseline ~15% 유지.
5. 잡 장애·10h 만료에 견고(폐기·재기동).

## 5. 남은 작업
- [x] **문제 1:** baseline refill 백그라운드 워커화(`BaselineRefiller`) + batch 1000→16·저수위 64. (테스트 통과)
- [x] **문제 2:** `edt_dashboard.rows_to_csv` 확장 — 입력+출력(tel_*/pass_*/`reports_json`) 전부, `.aedt`만 제외. (테스트 통과)
- [x] **문제 3 우회:** `EDT_SLOT_COUNT=1`로 pyaedt 충돌 회피 → 9 동시 solve로 실제 데이터 축적 시작.
- [ ] **문제 3 정식 수정:** 슬롯을 별도 프로세스로 격리(컨테이너당 워커 서브프로세스 N개) → 동시 ~100 밀도 복원.
- [ ] 1-슬롯 우회로 성공 데이터(`terminal_state=success` + 입출력 데이터셋)가 DB/CSV에 쌓이는지 실측 확인.

## 6. 운영/배포 메모 (실측)
- **데몬 코드 출처 = 배포 체크아웃 `~/mnt/8tb/peetsfea-runner`** (systemd `python -m`의 cwd가 sys.path 최상단).
  배포하려면 그 체크아웃을 브랜치 HEAD로 `git merge --ff-only` 해야 함(안 하면 구코드 실행).
- **클러스터 배포:** `~/.basenv/bin/uv pip install --python ./venv/bin/python --no-deps --reinstall <wheel>`
  (venv에 pip 없음). peetsfea 0.3.5 + peetsfea_runner wheel + `slot_service.sh`를 `/home1/harry261/edt-deploy/`.
- **gate 호스트명:** 로컬→gate 역터널은 ssh 별칭 `gate1-harry261`; **compute node→gate 정터널은 내부명 `gate1`**.
- **cpu2 QOS:** `cpu2_limit` `MaxTRESPerNode cpu=64` → cpu2 잡은 cpus-per-task=64(>64면 영구 PENDING).
- **아카이브 FIFO 상한:** 로컬 8TB 마운트 여유(~1.4TB)에 맞춰 `EDT_ARCHIVE_BUFFER_BYTES=1TB`(디스크 늘면 키울 것).

## 7. 향후 추가할 로직 (미구현 — 문서화만)
### 파티션 cooldown (pending-timeout 블랙리스트)
**동기:** 잡을 전 파티션에 랜덤 분배하는데, 포화/문제 파티션(예: gpu5·cpu1이 `Resources`/`Priority`로 10분+
PENDING)에 걸리면 시간만 버린다. 실측 사례: 잡이 제출 후 **10분 넘게 PENDING**으로 안 뜸.

**규칙:**
- 잡이 어떤 파티션에서 **N분(기본 10분) 넘게 PENDING**이면(못 뜨면) 그 잡을 **scancel**하고 즉시 다른 파티션으로
  재제출.
- 그 파티션을 **이후 K라운드(기본 10회 재제출) 동안 후보에서 제외(cooldown)**. cooldown 만료 후 다시 후보 포함.
- 모든 파티션이 cooldown이면 가장 빨리 풀리는 것부터 완화(데드락 방지).

**구현 위치(후속):** `SlurmJobLauncher`(제출 시각 기록 + `is_pending_too_long(handle)` + `partition_chooser`가
cooldown 파티션 skip) + `JobOrchestrator.poll()`(pending-age 초과 잡을 kill→재제출, 파티션 cooldown 카운터 갱신).
파티션별 PENDING/기동 통계는 어차피 자동 벤치마크로 쌓이므로 그걸 cooldown 판단에 활용 가능.

### 실시간 자원 사용량 텔레메트리 (✅ 구현됨 — 순간값)
`edt_resources.ResourcePoller`가 데몬에서 `ssh gate`로 `squeue`+`scontrol show node`+`lmstat`를 주기 폴링(20s)해
**컨테이너(=잡=노드)별 실시간 CPULoad·메모리 + 라이선스 + 잡 상태**를 캐시하고, 대시보드 `:8080/api/resources` +
"컨테이너 부하" 탭으로 노출(8s 자동 새로고침). 이미 기록 중인 `partition`/`node`/`gpu_used`로 파티션별 자동 벤치마크.
→ **남은 것은 "순간값"을 "시계열"로 확장**(아래).

### 시계열 메트릭 뷰 — x축 = 시간 (미구현, 문서화)
**동기:** 현재 대시보드는 **순간 스냅샷**(지금 부하·누적 카운트)만 보여준다. 운영 추세를 보려면 **x축을 시간**으로 두고
주요 지표의 변화를 봐야 한다.

**볼 지표(시간축):**
- **처리량** — 시간당 완료 시뮬 수(success/failed/aborted 적층), 성공률 추이.
- **동시 solve 수 · 라이선스 사용량**(electronics_desktop) 추이 — 가동 수준 한눈에.
- **평균 solve 시간** 추이 — 무거운/가벼운 후보 구간, 성능 변화.
- **노드/파티션별 부하**(CPULoad) 추이 + **GPU vs CPU 비율** 추이 — 자동 벤치마크의 시간적 분포.
- 잡 running/pending 추이 — 스케줄 압박·cooldown 효과.

**데이터 출처 / 구현:**
- **결과 시계열:** 이미 있는 `finished_at`을 시간 버킷팅. `store`에 집계 쿼리 추가
  → `GET /api/timeseries?metric=throughput|success_rate|avg_solve&bucket=5m&since=`.
  DuckDB `time_bucket`/`date_trunc`로 서버측 집계(전체 행 비전송).
- **리소스 시계열:** `ResourcePoller`는 지금 **순간값만** 캐시 → 시계열 보려면 스냅샷을 **시간순 링버퍼**(메모리, 예: 최근 24h)
  또는 경량 테이블에 누적. `GET /api/resources/history?metric=concurrent|license|cpuload&since=`.
- **프론트엔드:** "추세" 탭 신설 — 시간축 라인 차트(기존 SVG 렌더러 재사용, 다계열). 자동 새로고침.
- **활용:** 가동률·처리량·라이선스를 시간으로 추적해 **운영 이상 감지**(처리량 급락=장애, 라이선스 포화 등)와
  **장기 벤치마크**(파티션/ GPU 효과의 시간적 안정성)에 사용.
