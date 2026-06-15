# GOAL — Phase 1: 코어 단일 컨테이너 (edtmgr + warm AEDT + 단일 시뮬)

> 이 문서는 [`PLANS/MASTER_PLAN.md`](PLANS/MASTER_PLAN.md) 의 **Phase 1(전체의 1/6)** 만
> 발췌한 "지금 먼저 구현할 범위" 문서다. 전체 목표·아키텍처·6단계 계획은 MASTER_PLAN 참조.
> 적용 버전: peetsfea-main → **0.3.2**

## 0. Phase 1 한 줄 요약
**단일 잡 / 단일 enroot 컨테이너** 안에서 `edtmgr` 10개 + `ansysedt` 10개를 warm으로 상시
기동하고, 대기 큐의 fixed toml을 10개 슬롯에 **순차 디스패치**하여 시뮬을 실행한다.
ansysedt는 시뮬 사이에 죽지 않고 EDT 라이선스를 계속 점유한다. (9잡 확장·LB·서비스·아카이브는 후속 Phase)

## 1. 범위 (In Scope)
| 항목 | 내용 |
|------|------|
| 컨테이너 | 잡 1개 = enroot 컨테이너 1개 (runner + peetsfea-main 설치) |
| ansysedt | 컨테이너당 **10개 상시 기동** (warm) |
| edtmgr | 컨테이너당 **10개 상시 기동** (ansysedt 1개당 1개, **runner에 위치**) |
| 슬롯 | 10개, 각 슬롯에서 fixed toml **순차 연속** 실행 |
| 입력 | 대기 큐(이 단계에선 수동 시드 허용)의 fixed toml |
| 결과 | 기존 `single_simulation_store`(DuckDB)에 기록 |

## 2. edtmgr (AEDT 매니저)
- **목적:** ansysedt는 켜고 끄는 비용이 크고 끄면 EDT 라이선스를 놓친다. edtmgr는
  **별도 시뮬이 아닌 관리용 pyaedt 세션**을 상시 물고 (1) ansysedt warm 유지
  (2) 라이선스 점유 유지.
- **구성:** 각 edtmgr가 자신의 ansysedt를 `-ng -grpcsrv <port>` 로 띄우고 관리 세션을
  `close_on_exit=False` 로 붙여 둔다.
- **대여 프로토콜(컨테이너 내부 로컬 IPC):**
  - `acquire`: 관리 세션 점유만 잠깐 놓고(ansysedt는 살려 둠 → 라이선스 유지)
    `{pid, grpc_port}` 반환 → 시뮬 pyaedt가 같은 ansysedt에 grpc 접속.
  - `release`/`done`: 시뮬이 성공 반환하면 관리 세션 재부착, 다음 요청 대기.
- **타임아웃/장애:**
  - 시뮬 자체 워치독 **60분** abort → 마지막 완료 패스 기준 리포트 산출.
  - edtmgr 백스톱 **65분** 미반환 → 시뮬 pyaedt + ansysedt `SIGKILL(-9)` → 재기동 → 대기.
  - 대여 중 ansysedt 사망 감지 시 즉시 재기동(해당 대여 실패 처리).

## 3. 시뮬레이션 실행 정책
- 시뮬 1개 목표 ~**40분**(마지막 패스 후 리포트 저장), 하드 abort **60분**, edtmgr 강제종료 **65분**.
- 실행 진입점은 기존 단일 시뮬 API 계약 재사용:
  `primitive(candidate_toml_text, output_dir=, seed=, mode=)`
  (`peetsfea_runner/single_simulation_api.py:62,116`) —
  단, 자체 ansysedt 기동 대신 edtmgr가 준 grpc 세션에 접속(0.3.2 계약).

## 4. 자원 / 파일시스템 규칙
- `/dev/shm`, `/tmp` **사용 금지** → 잡 전용 `job_tmpfs` / `job_disk`.
- 잡 시작 시 `/enroot/{USERNAME}_{SLURM_JOB_ID}` 생성, 잡 종료 시 삭제.
- 라이선스는 충분(상한 미고려), edtmgr 관리세션으로 상시 점유만 유지.

## 5. Phase 1 변경 계획
### peetsfea-runner
1. **edtmgr(신규 모듈):** 컨테이너 내 10개 관리 서버 + 대여 프로토콜 + 60/65분 타이밍 +
   liveness 재기동. ansysedt grpc 기동/접속은 기존 remote_job grpc 런치 패턴 참고.
2. **슬롯 디스패처:** 대기 큐 → 슬롯 `acquire` → `single_simulation` primitive 실행 →
   `release` → 결과 기록. 기존 `single_simulation_*` 경로 재사용.
3. **파일시스템:** `/enroot/{USER}_{SJOB}` lifecycle, `/dev/shm`·`/tmp` 비사용 보장.
4. peetsfea 기대 버전 `0.3.1` → `0.3.2`
   (`single_simulation_api.py:19`, `single_simulation_remote.py:194`).
5. AGENTS.md 관례 준수: CLI 신설 없이 `run_pipeline(config)`/서비스 진입점, `.venv/bin/python`.

### peetsfea-main → 0.3.2
1. **기존 warm ansysedt 접속:** edtmgr가 준 `(pid, grpc_port)` 세션에 접속해 실행
   (자체 ansysedt 기동/종료 금지).
2. **완료 시 깨끗이 반환:** 프로젝트 정리 후 edtmgr에 release 가능 상태로 마무리.
3. **패스/시간 예산:** ~40분 목표, **60분 하드 abort 시 마지막 완료 패스 리포트 산출**.
4. **라이선스/AEDT 수명 비소유:** edtmgr가 관리.
5. 버전 `0.3.1` → `0.3.2`.

## 6. 수용 기준 (Acceptance)
- 컨테이너 1개에서 fixed toml N개가 **10슬롯으로 순차 처리**되어 각 결과/리포트가 산출됨.
- 시뮬 사이에 ansysedt가 **죽지 않고** EDT 라이선스를 **유지**함을 확인.
- 60분 abort 시 마지막 패스 리포트가 남고, 65분 미반환 시 edtmgr가 강제 정리·재기동함.
- `/dev/shm`·`/tmp` 미사용, `/enroot/{USER}_{SJOB}` 생성·정리 확인.

## 7. Phase 1 제외(후속 Phase) — 자세히는 MASTER_PLAN
- Phase 2: 9잡/90 동시 오케스트레이션, 5h 만료 시 폐기.
- Phase 3: 로드밸런서(CPU/mem 피드백 제어 + 스태거).
- Phase 4: Intake `:7875`(sweep toml + N → 랜덤 샘플 → 큐).
- Phase 5: 결과 DB 확장 + 대시보드 `:8080`(read-only).
- Phase 6: 아카이브 저장소(2TB 압축, FIFO eviction).
