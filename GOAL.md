# GOAL — Phase 2~4: 정상상태 파이프라인 (스케일 ~100 + 로드밸런서 + Intake)

> 이 문서는 [`PLANS/MASTER_PLAN.md`](PLANS/MASTER_PLAN.md) 의 **Phase 2·3·4** 를 구체화한 구현 범위다.
> Phase 1(코어 단일 컨테이너 + 실 solve)은 완료. 나머지 절반(Phase 5 DB/대시보드 + Phase 6 아카이브)은 다음 GOAL.
> 의존: peetsfea **0.3.4** ([`PLANS/peetsfea_main.md`](PLANS/peetsfea_main.md)).

## 0. 한 줄 요약
Phase 1의 단일 컨테이너를 **9 컨테이너(잡)** 로 확장해 정상상태 **동시 ~100 시뮬**을 유지하고,
**로드 밸런서**가 ramp-up으로 부하를 균등화하며, **Intake :7875** 가 sweep을 받아
**2-레인 큐(baseline + 우선순위, 85:15)** 로 슬롯을 끊김 없이 공급한다.

## 1. 범위 (In Scope)
| Phase | 내용 |
|------|------|
| 2 | 9잡 제출·생애주기 + 컨테이너당 warm ≥11·≤16 / 합 **~100 동시 실행**(유동) |
| 3 | 로드 밸런서: CPU·메모리 피드백(PID/EWMA) + admission control + ramp-up + 스태거 |
| 4 | Intake `:7875`(sweep+N→검증→샘플) + **2-레인 가중 큐**(baseline 1000 + 우선순위, 15% 플로어) |

> 제외(다음 GOAL): 결과 DB 확장 + 대시보드 `:8080` + CSV export(Phase 5), 아카이브 저장소(Phase 6).
> 단, 시뮬 결과는 이번에도 기존 `single_simulation_store`(DuckDB)에 계속 기록한다(확장은 Phase 5).

## 2. Phase 2 — 9잡 / ~100 동시 오케스트레이션
- **잡 토폴로지:** 단일 계정에서 **9개 SLURM 잡** 제출. 잡 1개 = enroot 컨테이너 1개 =
  컨테이너 안에서 Phase 1의 슬롯 서비스(edtmgr 풀 + `SlotDispatcher`) 실행.
- **컨테이너당 슬롯:** warm 보유 **≥11**, 최대 **16**. 9 컨테이너 합 **목표 ~100 동시 실행**,
  컨테이너별 동시 실행 ~7–16 **불균형 허용**(합 ~100 근처면 OK).
- **생애주기:** 제출 → readiness → 실행. **잡 수명 10h**, 만료 시 진행 중 ~100개 **그냥 폐기**
  (드레인 로직 없음, Q8) 후 재기동. 잡 1개가 죽어도 나머지 8개는 지속.
- **파일시스템:** 잡 단위 `/enroot/{USER}_{SJOB}` 생성/정리(`job_workspace.py` 재사용),
  `/dev/shm`·`/tmp` 비사용.
- **변경/재사용:** `edt_service.build_slots/build_dispatcher`(현 단일 컨테이너) →
  **9잡 오케스트레이터**(신규)로 확장. 잡 제출/모니터/재기동은 기존 `scheduler.py` sbatch 패턴,
  컨테이너·grpc 기동은 `remote_job.py` 패턴, `runner.py`의 `accounts_registry`(account_01) 재사용.
- **수용:** 정상상태에서 동시 ~100 유지가 관측됨; 잡 1개 강제종료해도 전체 지속; 10h 만료 시 폐기·재기동.

## 3. Phase 3 — 로드 밸런서 (ramp-up)
- **warm vs 실행 분리:** warm 보유 ≥11(항상) / 동시 실행 7–16(부하 유동). 7개만 돌더라도 11 warm 유지
  → 부하 풀리면 **슬로 스타트 없이 즉시 11까지**. 12–16 버스트만 추가 기동(지연 무방).
- **신호:** 노드/컨테이너 **CPU·메모리** 사용률(주기 샘플, `psutil` + `/proc`).
- **제어 루프:**
  - **admission control:** 슬롯이 비고 **그리고** 측정 부하 ≤ 목표(watermark)면 다음 fixed toml 디스패치.
    과부하면 **AIMD 백오프**로 신규 시작 보류.
  - 측정→목표 수렴 **피드백 제어(PID 또는 EWMA)**, **시작 스태거**(시작 간 최소 간격 + 부하 게이팅),
    매시간 재최적화.
  - **ramp-up:** 콜드 스타트는 적게 시작 → 부하 여유 따라 점진 증설. 새 시뮬은 초기(가벼움) 구간으로
    들어와 기존이 무거워지기 전에 흡수 → 시작 시점 자연 분산(무거운 패스 겹침 방지).
- **변경/재사용:** `edt_dispatcher.SlotDispatcher`의 슬롯 루프(`_slot_loop`/`_run_one`,
  `edt_dispatcher.py:95` 디스패치 지점)에 **admission 게이트** 추가. **load telemetry 모듈**(신규,
  CPU/mem 샘플), `edt_watchdog`와 통합. LB 파라미터는 `constants.py`에 추가.
- **수용:** 동시 시작(naïve) 대비 노드 부하 스파이크 완화 + 활용률·스루풋 상승이 데이터로 확인됨.

## 4. Phase 4 — Intake `:7875` + 2-레인 큐
- **Intake `:7875`:** 온전한 **sweep toml + 개수 N** 수신 → peetsfea
  `validate_sweep_toml_text`(기준 sweep 범위 이내인지, **넓으면 거절**) →
  `sample_fixed_candidates_from_toml_text(text, N, seed)`로 **fixed × N** → **우선순위 레인** 적재.
- **2-레인 가중 큐(§2.6.1):**
  - **baseline 레인(전역 탐색):** 전체 `0.3.x_sweep.toml`을 **풀스페이스 랜덤 샘플**해 **~1000칸 버퍼
    상시 리필**(저수위 시 다음 배치, **배치마다 seed 롤링**). 슬롯이 놀지 않게 하는 상시 공급원. **휘발**.
  - **우선순위 레인(어댑티브):** 7875로 들어온 subset sweep을 검증·샘플 후 앞쪽 적재, 먼저 소비. **파일 영속**.
  - **15% 탐색 플로어:** 디스패처가 두 레인을 **~85:15 결정론적 인터리브**(우선순위 backlog 중에도
    baseline 15% 하드 플로어). 우선순위 비면 **100% baseline**.
- **변경/재사용:** `edt_queue.TomlQueue`(단순 FIFO) → **2-레인 가중 스케줄러**로 확장
  (`get()`이 85:15로 baseline/우선순위 추출). **Intake HTTP 서버**(신규, `:7875`). baseline 샘플러는
  peetsfea `sample_fixed_candidates_from_toml_text` + seed 롤링. 결과는 `single_simulation_store`에 기록.
- **수용:** ① baseline만으로 슬롯이 끊김 없이 채워짐, ② sweep 1건 투입 시 N개가 **우선 처리**됨,
  ③ 우선순위 backlog 중에도 baseline **~15% 유지**가 데이터로 확인됨.

## 5. 변경 대상 (요약)
- **신규:** 9잡 오케스트레이터, load telemetry/밸런서, Intake `:7875` 서버, 2-레인 가중 큐.
- **확장:** `edt_dispatcher.py`(admission 게이트), `edt_queue.py`(2-레인), `edt_service.py`(1→9잡),
  `constants.py`(warm 하한 11·상한 16·LB 파라미터).
- **재사용:** `scheduler.py`(sbatch/readiness), `remote_job.py`(컨테이너·grpc), `job_workspace.py`,
  `edtmgr`/`RealEdtBackend`, peetsfea 0.3.4 `validate_sweep_toml_text`/`sample_fixed_candidates_from_toml_text`,
  `single_simulation_store`(결과 기록).

## 6. 수용 기준 (통합)
- 정상상태 **동시 ~100** 유지(컨테이너별 7–16 유동), 잡 장애·10h 만료에 견고.
- 로드밸런서로 **부하 스파이크 완화·활용률 상승** 데이터 확인.
- Intake sweep → 검증 → 샘플 → 큐 → 시뮬 → 결과 기록 round-trip, **85:15 탐색 플로어** 유지.

## 7. 제외 (다음 GOAL = Phase 5+6)
- 결과 DB 스키마 확장 + 대시보드 `:8080`(read-only) + `curl .../results.csv` CSV export.
- 아카이브 저장소(project_dir 누적 → 20GB 묶음 압축, 2TB 초과 시 오래된 묶음 FIFO 삭제).
