# GOAL — 전체 production 시스템 + 정상상태 동시 ~100 연속 가동

> [`PLANS/MASTER_PLAN.md`](PLANS/MASTER_PLAN.md) **전체(Phase 1~6)** 를 구현하고,
> 정상상태에서 **동시 ~100개 시뮬을 끊김 없이 무한 연속** 가동하는 것이 최종 목표.
> 요청(:7875)이 없을 땐 **전역 설계공간 baseline 랜덤 샘플링**으로 슬롯을 채워 계속 시뮬한다.
> 실행은 `systemctl --user start|restart peetsfea-runner`. 의존: peetsfea **0.3.4**([`PLANS/peetsfea_main.md`](PLANS/peetsfea_main.md)).

## 0. 한 줄 요약
단일 계정에서 **동시 ~100개 AEDT 시뮬을 정상상태로 무한 연속** 가동한다. Intake(:7875)로 들어온
우선순위 sweep을 먼저 처리하되, 비어 있으면 `0.3.x_sweep.toml` 전체 설계공간을 **baseline 랜덤
샘플링**해 슬롯을 끊김 없이 채운다(요청 없이도 계속 돈다). 결과는 DB+대시보드+아카이브로.

## 1. 전체 구성과 현황 (Phase 1~6 + 운영)
| Phase | 내용 | 상태 |
|------|------|------|
| 1 | 코어 단일 컨테이너: edtmgr warm 풀 + 슬롯 디스패처 + 실 solve | ✅ 완료(로컬·클러스터 실 AEDT) |
| 2 | 9잡/~100 오케스트레이션(잡 lifecycle, 10h 폐기·재기동) | ✅ 완료(실 SLURM e2e) |
| 3 | 로드 밸런서(ramp-up, CPU/mem 피드백 + admission) | ✅ 완료(실 admission-gated solve) |
| 4 | Intake :7875 + 2-레인 가중 큐(baseline + 우선순위, 85:15) | ✅ 완료(실 우선순위 solve) |
| 5 | 결과 DB 확장 + 대시보드 :8080(read-only) + `results.csv` export | ✅ 완료(`edt_dashboard`, 단위테스트) |
| 6 | 아카이브 저장소(20GB 묶음 압축, 2TB FIFO) | ✅ 완료(`edt_archive`, 단위테스트) |
| **운영** | **systemctl 상시 가동, 동시 ~100 무한 연속(baseline 자기공급)** | ⏳ 스케일 실가동(control plane 결선됨) |

## 2. Phase별 (요약·수용)
- **Phase 1 — 코어:** 컨테이너당 edtmgr/ansysedt warm 풀, 슬롯 순차 실행, 60/65분 타이밍, peetsfea warm-AEDT 접속.
- **Phase 2 — 9잡/~100:** 9개 SLURM 잡(=컨테이너) 상시 유지, 컨테이너당 warm ≥11·≤16, 합 ~100 동시(불균형 허용), 죽으면 재기동·10h 만료 폐기. 잡당 컨테이너가 entrypoint로 슬롯 서비스 실행.
- **Phase 3 — 로드밸런서:** CPU·메모리 EWMA 피드백 + AIMD + 시작 스태거. **새 시뮬은 부하 여유 시에만 시작**(ramp-up) → 무거운 패스 구간 시간축 분산.
- **Phase 4 — Intake + 2-레인 큐:** :7875가 sweep+N 수신 → peetsfea 범위검증(넓으면 거절) → 샘플 → 우선순위 레인. baseline 레인은 전역 풀샘플 ~1000 버퍼 상시 리필. **85:15**(우선순위 backlog에도 baseline 15% 플로어, 우선순위 비면 100% baseline).
- **Phase 5 — DB + 대시보드:** 모든 누적 데이터(load·timing·패스·입출력 파라미터·아카이브 ref)를 DuckDB 확장. `localhost:8080` read-only 대시보드 + `curl localhost:8080/results.csv`(입출력 파라미터 누적). 시뮬에 영향 없음.
- **Phase 6 — 아카이브:** 시뮬 종료 시 project_directory를 누적 → **20GB 묶음 압축(solid)**, 2TB 초과 시 가장 오래된 묶음 파일 삭제(FIFO).

## 3. 운영 목표 — 정상상태 동시 ~100 무한 연속 (최종 수용)
- **상시 가동:** `systemctl --user start|restart peetsfea-runner`. 콜드 스타트(진행분 복구 없음).
- **동시 ~100 유지:** 9 컨테이너 × warm ≥11(합 99) ~ ≤16. 슬롯이 비면 즉시 다음 후보 디스패치.
- **요청 없이도 계속 돈다:** 우선순위(7875)가 비면 **baseline이 전역 설계공간을 랜덤 샘플링**(배치마다
  seed 롤링, ~1000 버퍼 리필)해 슬롯을 끊김 없이 채운다 → **무한 연속 시뮬**, 전역 커버리지 축적.
- **요청 오면 우선:** 7875로 들어온 subset sweep을 **85% 우선** 소비(단 baseline 15% 탐색 플로어 유지).
- **부하 균등:** 로드밸런서가 ramp-up으로 동시 시작을 분산해 노드 부하 스파이크를 막는다.
- **잡 재활용:** 10h 만료 잡은 진행 중 시뮬을 폐기하고 재기동(드레인 없음, 단순).
- **결과 흐름:** 각 시뮬 결과 → DB 적재 → 대시보드 조회 + `results.csv` → project_dir 아카이브.

## 4. 최종 수용 기준
`systemctl --user start peetsfea-runner`로 띄우면:
1. **동시 ~100 시뮬이 정상상태로 무한 유지**(요청이 없어도 baseline으로 계속 채워짐).
2. 7875로 sweep 투입 시 N개가 **우선 처리**되고, backlog 중에도 baseline **~15%** 유지.
3. 모든 결과가 **DB에 적재 → 대시보드/`results.csv`로 조회**되고, project_dir이 **아카이브**(20GB 묶음, 2TB FIFO)된다.
4. 로드밸런서로 동시 시작 대비 **부하 스파이크 완화·활용률 상승**이 데이터로 확인된다.
5. 잡 장애·10h 만료에 견고(폐기·재기동).

## 5. 현재까지 검증된 것 (Phase 1~4)
실 AEDT(로컬·클러스터)로 end-to-end 검증 완료:
- 로컬: `smoke_edt_real_primitive`(실 solve), `smoke_edt_steady_state`(2-레인+admission+실 solve).
- 클러스터: `verify_slurm_orchestration`(CLUSTER_ORCH_PASS), `verify_slurm_slot_service`
  (SLURM 잡 → entrypoint → 실 슬롯 서비스 → 실 HFSS solve = SLURM_SLOT_SERVICE_PASS).
- 신규 13개 모듈, 57+ 단위테스트, mypy strict 클린.

## 6. 남은 것 — 운영 실가동만
Phase 1~6 코드 + 컨트롤 플레인 결선 완료. 남은 것은 **스케일 실가동 한 단계**뿐:
- **컨트롤 플레인(`edt_control_plane`):** systemd `--user` 진입점. 9 SLURM 잡 상시 유지 +
  대시보드:8080 + Intake:7875 + 잡 poll/재기동/10h 만료를 SIGTERM까지 돌린다. 단위테스트 통과.
- **배포·결선:** systemd 유닛 `peetsfea-runner` → `edt_control_plane.main`. 배포 체크아웃
  (`~/mnt/8tb/peetsfea-runner`)을 이 브랜치로 갱신 + `systemctl --user daemon-reload`.
- **스케일 기동:** 단일 잡/슬롯·9잡 오케스트레이션 메커니즘은 검증됨(SLURM_SLOT_SERVICE_PASS,
  CLUSTER_ORCH_PASS). `job_count=9`·`slot_count≤16`로 단계적 실기동 → 동시 ~100 정상상태 관측.
