# peetsfea-runner — peetsfea 연동(runner 쪽 책임)

> runner가 peetsfea(0.3.2)를 **어떻게 호출/소비하는지** 적는다. main 개발자에게 요구하는 계약은
> `PLANS/peetsfea_main.md`, 전체 아키텍처는 `PLANS/MASTER_PLAN.md` 참조.

## 1. 경계 (누가 무엇을)
- **peetsfea(main):** 시뮬 실행(설계 빌드/해석/리포트), sweep 범위 검증, 샘플링.
- **runner:** ansysedt 수명·라이선스 점유(edtmgr), 작업 디렉토리/환경 주입, 시간 백스톱,
  결과 DB 적재, 산출물 아카이브, Intake/큐/디스패치.

## 2. edtmgr ↔ 프리미티브 대여 (runner 구현)
- edtmgr가 ansysedt를 warm으로 띄워 두고(`-ng -grpcsrv <port>`, 관리 pyaedt `close_on_exit=False`)
  라이선스를 점유. 슬롯 디스패처가 `acquire` 하면 `{pid, grpc_port}`를 얻는다.
- 그 `grpc_port`를 peetsfea 프리미티브에 인자로 넘겨 **같은 ansysedt에 attach**해 실행시킨다
  (현재 진입점 `primitive(candidate_toml_text, output_dir=, seed=, mode=)`
  — `peetsfea_runner/single_simulation_api.py:62,116` — 에 grpc 좌표 인자 추가).
- 완료 후 edtmgr가 관리 세션을 재부착(re-acquire)해 다음 fixed toml 처리.

## 3. 시간 백스톱 (runner 구현)
- peetsfea는 60분에 스스로 abort+리포트. runner/edtmgr는 그 위에 **65분 백스톱**:
  미반환 시 시뮬 pyaedt + ansysedt `SIGKILL(-9)` 후 재기동(MASTER_PLAN §2.1).
- 외부 SIGKILL·grpc 끊김에도 부분 산출물은 보존, 데이터 정합만 보장.

## 4. 작업 디렉토리 / 환경 주입 (runner 제공)
- `/dev/shm`·`/tmp` 금지. runner가 잡 전용 `job_tmpfs`/`job_disk` 기반 경로와 env를 주입한다
  (`PEETS_RAMDISK_ROOT`, `PEETS_RAMDISK_TMPDIR`, `PEETS_DISK_ROOT`, `ANSYS_WORK_DIR` 등).
- 잡 시작 시 `/enroot/{USER}_{SJOB}` 생성, 종료 시 삭제.

## 5. 결과 적재 (runner 구현)
- 프리미티브가 돌려준 구조화 결과를 결과 DB(DuckDB)에 기록.
  기존 store 스키마 키와 매핑: `setup_pass_counts_json`, `solve_telemetry_json`,
  `csv_text_by_report_json`, `csv_paths_json` 등 (`peetsfea_runner/single_simulation_store.py`).
- 산출물 디렉토리(`project_name.aedt`/`.aedtresults` 등)를 아카이브 저장소로 넘긴다
  (20GB 묶음 압축, 2TB FIFO — MASTER_PLAN §2.8).
- 실패 봉투 `terminal_state="failed"` 처리(`single_simulation_api.py:139-161`).

## 6. Intake(:7875) → 큐 (runner 구현)
- 온전한 sweep toml + N 수신 → peetsfea `validate_sweep_toml_text`로 **기준 범위 이내 검증**
  (넓으면 거절) → `sample_fixed_candidates_from_toml_text(text, N, seed)`로 fixed N개 →
  대기 큐 적재 → 슬롯 가용 시 순차 디스패치.

## 7. 버전 / 타입 기대값 (runner 쪽)
- `EXPECTED_PEETSFEA_VERSION` = `"0.3.3"`
  (`peetsfea_runner/single_simulation_api.py:19`, `single_simulation_remote.py:194`). `pyproject` 핀 `@0.3.3`.
- sqsh 계약 버전 `_ENROOT_IMAGE_CONTRACT_VERSION`(`scheduler.py:138`)은 0.3.3 이미지에 맞춰 bump 필요
  (현 클러스터 이미지는 `2026-05-07-aedt-sqsh-v3-sshfs`로 더 오래됨 — 검증 finding).
- runner는 strict 타입체킹(AGENTS.md §2)이므로 peetsfea가 `py.typed`를 제공해야 의존 코드 타입이 풀린다.

## 8. enroot 이미지 런타임 의존 (runner/이미지 쪽)
- peetsfea가 cadquery/OCP로 지오메트리를 빌드하므로 컨테이너에 **`libGL.so.1` 등 GL 라이브러리**가 필요하다
  (없으면 `import cadquery`가 `ImportError: libGL.so.1`).
- `enroot_image_bootstrap.sh`의 runtime_packages(현재 `openssh-client sshfs fuse3 ca-certificates`)에
  **`libgl1`(+ 필요 시 `libglu1-mesa libxrender1 libxext6 libsm6`)** 추가. (로컬 e2e에서 런타임 apt 설치로 우회 확인.)
