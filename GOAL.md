# GOAL — 전체 production 시스템 + 정상상태 동시 ~100 연속 가동

> [`PLANS/MASTER_PLAN.md`](PLANS/MASTER_PLAN.md) **전체(Phase 1~6)** 를 구현하고,
> 정상상태에서 **동시 ~100개 시뮬을 끊김 없이 무한 연속** 가동하는 것이 최종 목표.
> 요청(:7875)이 없을 땐 **전역 설계공간 baseline 랜덤 샘플링**으로 슬롯을 채워 계속 시뮬한다.
> 실행은 `systemctl --user start|restart peetsfea-runner`. 의존: peetsfea **0.3.5**([`PLANS/peetsfea_main.md`](PLANS/peetsfea_main.md)).
> 0.3.5 = **GPU 자동 가속**(solve 시점 `nvidia-smi`로 GPU 탐지 → 있으면 켜고 없으면 CPU 폴백, API 변경 없음) +
> solver cores 고정(=4) + `solve_telemetry`에 `gpu_used`/`gpu_device_name`/`solver_cores` 기록.
> 러너는 컨테이너에 GPU만 노출(`NVIDIA_VISIBLE_DEVICES=all`)하고 파티션/노드를 기록 → 자동 GPU vs CPU 벤치마크.

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
- **결과 흐름(단일 로컬 DB):** 슈퍼컴엔 DB 없음. 각 시뮬 결과 envelope를 **gate 경유 ssh 터널로 로컬
  데몬:7876에 push** → 로컬 단일 DuckDB 적재 → 대시보드:8080/`results.csv` 조회.
  (7875=공개 sweep intake, 7876=7875 사용자가 모르는 슈퍼컴 전용 결과 백채널. 다계정이면 계정마다
  정터널 하나씩, 전부 같은 로컬 :7876로 모여 `account_id`로 합산.)
- **대용량 산출물 전송 + gpfs 절약(:7877):** 시뮬 성공 시 컨테이너가 project_dir(aedt)를 **`tar` 스트림으로
  로컬 데몬:7877에 HTTP POST**(7876과 동일한 gate 경유 ssh 터널, **sshd 불필요** — HTTP tar 스트림).
  로컬은 스트리밍으로 버퍼에 추출 → **`ArchiveStore`(20GB 묶음 압축, 2TB FIFO)**. 전송 성공 시 **gpfs 원본을
  즉시 삭제**해 슈퍼컴 디스크를 항상 비운다(무조건 절약). 7877도 7875 사용자가 모르는 슈퍼컴 전용 통로.

## 4. 최종 수용 기준
`systemctl --user start peetsfea-runner`로 띄우면:
1. **동시 ~100 시뮬이 정상상태로 무한 유지**(요청이 없어도 baseline으로 계속 채워짐).
2. 7875로 sweep 투입 시 N개가 **우선 처리**되고, backlog 중에도 baseline **~15%** 유지.
3. 모든 결과 envelope가 **로컬 단일 DB에 적재 → 대시보드/`results.csv`로 조회**되고, project_dir(aedt)은
   **:7877 HTTP tar 스트림으로 로컬에 전송 → 아카이브(20GB 묶음, 2TB FIFO)** 되며 **gpfs 원본은 삭제**된다.
4. 로드밸런서로 동시 시작 대비 **부하 스파이크 완화·활용률 상승**이 데이터로 확인된다.
5. 잡 장애·10h 만료에 견고(폐기·재기동).

## 5. 현재까지 검증된 것 (Phase 1~4)
실 AEDT(로컬·클러스터)로 end-to-end 검증 완료:
- 로컬: `smoke_edt_real_primitive`(실 solve), `smoke_edt_steady_state`(2-레인+admission+실 solve).
- 클러스터: `verify_slurm_orchestration`(CLUSTER_ORCH_PASS), `verify_slurm_slot_service`
  (SLURM 잡 → entrypoint → 실 슬롯 서비스 → 실 HFSS solve = SLURM_SLOT_SERVICE_PASS).
- 신규 13개 모듈, 57+ 단위테스트, mypy strict 클린.

## 6. 남은 것
Phase 1~6 코드 + 컨트롤 플레인 결선 + 결과 ingest(:7876) 완료. 남은 것:

### 6.1 대용량 산출물 전송 :7877 (미구현 — 설계 확정, 나중에 구현)
**HTTP tar 스트림 push, sshd 불필요.** 7876과 동일한 gate 경유 ssh 터널 인프라 재사용.
- **로컬 데몬:** `:7877` HTTP 수신 서버(`POST /bulk/<request_id>`, tar.gz 스트림 → 버퍼에 스트리밍 추출,
  상수 메모리) + 7877 역터널(`reverse_tunnel_argv(port=7877)`) + `ArchiveStore`(20GB 묶음/2TB FIFO) 결선.
  - **받는 즉시 풀어서 raw로 보관(개별 압축 보관 금지).** 전송 구간만 `tar.gz`(전송 효율)이고, 도착하면
    바로 압축 해제해 raw 파일로 버퍼에 둔다. 그래야 `ArchiveStore`가 **여러 project_dir를 모아 한 번에
    solid 압축**해 압축률이 좋아진다(개별 압축 보관 시 묶음이 "이미 압축된 덩어리들의 tar"가 되어 추가
    압축이 안 먹는다).
- **컨테이너:** 7877 정터널(`slot_service.sh`, 7876 옆) + 신규 `edt_bulk_transfer.BulkPushSink`:
  시뮬 성공 시 `tar czf -` 스트림을 `127.0.0.1:7877`로 POST → 성공 시 **gpfs 원본 삭제**(무조건 절약),
  실패 시 재시도→보존(디스패처 안 죽임).
- **신뢰 모델:** 터널 loopback 바인딩이라 외부 도달 불가 → 별도 인증/키 불필요(7876과 동일).
- **재사용:** `edt_ssh_tunnel`(터널), `edt_archive.ArchiveStore`(묶음/FIFO), `edt_result_ingest` HTTP 서버 패턴.

### 6.2 운영 실가동
- **배포·결선:** systemd 유닛 `peetsfea-runner` → `edt_control_plane`. 배포 체크아웃
  (`~/mnt/8tb/peetsfea-runner`)을 이 브랜치로 갱신 + `systemctl --user daemon-reload`.
- **스케일 기동:** 단일 잡/슬롯·9잡 오케스트레이션 메커니즘 검증됨(SLURM_SLOT_SERVICE_PASS,
  CLUSTER_ORCH_PASS). `job_count=9`·`slot_count≤16`로 단계적 실기동 → 동시 ~100 정상상태 관측.
