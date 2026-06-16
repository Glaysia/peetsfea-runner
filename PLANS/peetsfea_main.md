# peetsfea 0.3.5 — 사양서 (runner 의존 계약)

> peetsfea-runner가 의존 패키지 `peetsfea`에 요구하는 **계약 사양**이다. 현재 배포본은 **0.3.4**이며,
> 이 문서는 다음 버전 **0.3.5**의 사양을 정의한다. 0.3.5의 신규 항목은 **§9 자동 GPU 가속** 하나이고,
> 나머지(§2~§8)는 0.3.4에서 확립돼 **0.3.5도 그대로 유지**해야 하는 계약이다.
> runner 쪽 연동 책임은 `PLANS/peetsfea_runner.md`, 전체 아키텍처는 `PLANS/MASTER_PLAN.md`.

## 0. 역할 경계 (불변)
| 소유 | 책임 |
|------|------|
| **peetsfea** | 시뮬 자체: toml 스키마·범위검증·샘플링, pyaedt로 설계 빌드/해석/리포트, **솔버 자원(코어/GPU) 결정** |
| **peetsfea-runner** | 오케스트레이션: edtmgr(ansysedt 수명·라이선스 점유), 큐/디스패치, 시간 백스톱, DB/아카이브, 컨테이너·GPU 노출 |

**원칙:** peetsfea는 ansysedt를 직접 켜고/끄거나 라이선스를 관리하지 않는다. ansysedt 수명은 edtmgr가 소유한다.

## 1. 버전 / 패키징
1. `peetsfea.__version__ == "0.3.5"`.
2. 컨테이너에 runner와 함께 설치 가능(Python 3.12). **`py.typed`** 동봉(runner는 strict 타입체킹).
3. **패키지 데이터 동봉(0.3.4 확립, 유지):** 기준 sweep(`peetsfea/data/0.3.x_sweep.toml`)·fixed 예제·
   ferrite 데이터셋(`peetsfea/data/mu_p.tab`)을 **패키지 내부에 동봉**하고 경로를 package-relative/
   `importlib.resources`로 해석(설치 환경에서도 동작). 소스 레이아웃(`parents[N]/...`) 가정 금지.

## 2. Warm AEDT 접속 (핵심, 불변)
edtmgr가 ansysedt를 warm으로 띄워 두고 라이선스를 점유한다. 시뮬 프리미티브는 **새 ansysedt를 띄우지
말고** edtmgr가 준 grpc 세션에 attach해 실행한다.
1. 프리미티브가 `grpc_port`(필요 시 `aedt_pid`)를 받아 그 세션에 attach(`new_desktop=False`,
   `close_on_exit=False`).
2. 완료 시 **프로젝트만 닫고** AEDT는 살린 채 반환 → edtmgr가 같은 ansysedt를 재획득해 재사용.
3. 진입점:
   `run_ssw_random_sample_reports_from_toml_text(candidate_toml_text, *, output_dir, seed, mode, grpc_port, aedt_pid=None, ...)`.

## 3. 패스 / 시간 예산 & abort (불변)
1. 목표 ~40분, **60분 하드 abort** 시 마지막 완료 패스 기준 리포트 산출(`solve_hard_abort_seconds`, 기본 3600).
2. 65분 강제종료는 runner/edtmgr 백스톱(peetsfea는 소유하지 않으나 외부 SIGKILL에 안전해야 함).

## 4. TOML 범위 검증 + 샘플링 (불변)
1. 기준 sweep 스키마/범위(SSOT) 정의(`examples`/`data`의 `0.3.x_sweep.toml`).
2. `validate_sweep_toml_text(text) -> None`: 들어온 온전한 sweep toml의 모든 range가 기준 range
   **이내인지 검증**, 넓으면 예외(정규화·필드 보완 없음 — "subset"은 값 범위의 부분집합).
3. `sample_fixed_candidates_from_toml_text(text, count, seed) -> list[str]`: 결정론적 N개 fixed candidate.

## 5. 결과 / 리포트 (불변)
1. 구조화된 **TypedDict 반환**: `setup_pass_counts`, `solve_telemetry`, `csv_paths`, `csv_text_by_report`,
   `design_id`, `point_hash`, `point_values` 등(runner store 스키마와 1:1).
2. CSV 스키마: 입력 파라미터 + 출력 변수(`k_ratio`, `Lrx_uH` 등) 유지(축약 금지).
3. 산출물 디렉토리(`*.aedt`/`*.aedtresults`)를 식별 가능하게 남겨 runner가 아카이브.

## 6. 파일시스템 규율 (불변)
`/dev/shm`·`/tmp` 직접 사용 금지. runner가 주는 작업 디렉토리/환경변수(`PEETS_RAMDISK_ROOT`,
`PEETS_DISK_ROOT`, `ANSYS_WORK_DIR` 등; `job_tmpfs`/`job_disk` 기반)만 사용. 전역/홈 잔여물 금지.

## 7. 오류 처리 (불변)
실패 시 구조화된 예외/반환(stage·type·message). 외부 `SIGKILL`/grpc 끊김에 부분 산출물 보존, 데이터 정합 보장.

## 8. 라이선스 / AEDT 수명 (불변)
peetsfea는 ansysedt 기동·종료·재기동, EDT 라이선스 획득/반납에 관여하지 않는다(전부 edtmgr). 라이선스는
충분하므로 peetsfea가 라이선스 가용성을 판단·대기하지 않는다.

---

## 9. 🆕 0.3.5 신규 — 자동 GPU 가속 (API 변경 없음)
**요지:** runner는 GPU 파티션이든 CPU 파티션이든 **컨테이너에 GPU만 노출**하고(즉
`NVIDIA_VISIBLE_DEVICES=all`로 GPU가 보이게), **peetsfea가 솔브 시 GPU 접근 가능 여부를 스스로 감지해
가능하면 GPU 가속을 켠다.** runner가 `gpus=` 같은 인자를 넘기지 않는다 — **API/시그니처 변경 없음.**

### 9.1 동작
1. **자동 감지:** 솔브 직전 peetsfea가 GPU 가용성을 판정한다(예: CUDA 디바이스/`nvidia-smi` 응답/
   가시 GPU 수). 가용하면 **AEDT analyze에 GPU 가속을 활성**한다(내부적으로 pyaedt
   `analyze(..., gpus=N, cores=…)` 또는 `set_custom_hpc_options`/ACF로 설정).
2. **CPU 폴백:** GPU 미가용, 또는 GPU 활성 실패(HPC Pack 라이선스 부재·드라이버 문제 등)면
   **조용히 CPU로 폴백**해 기존(0.3.4)대로 솔브한다 — 잡이 실패하면 안 된다.
3. **코어 자동:** GPU/CPU 모두에서 가용 코어를 적절히 쓴다(현재 `analyze_setup(name)`은 AEDT 기본만
   쓰므로, 노드 코어 수를 반영하도록 `cores` 자동 설정 권장 — runner는 파티션별로 cpu2=100·그외=32
   코어를 잡아 주므로 그 안에서 peetsfea가 솔버 코어를 정한다).

### 9.2 결과 기록 (Q5 자동 벤치마크 지원)
- `solve_telemetry`에 **GPU 사용 여부·디바이스명·솔버 코어 수·솔브 시간**을 포함한다.
- 그래야 runner가 잡을 전 파티션 랜덤 분배(MASTER_PLAN §2.10)하는 것만으로 **별도 벤치마크 없이**
  파티션(CPU vs GPU)별 성능 데이터가 결과 DB에 누적되어 **Q5를 나중에 데이터로 결정**할 수 있다.

### 9.3 주의
- HFSS **주파수영역**(driven terminal/modal) 솔버는 GPU 가속 효과가 제한적일 수 있다(GPU는 주로
  HFSS Transient/SBR+). 따라서 "켜면 무조건 빨라짐"이 아니라 **켤 수 있으면 켜고, 빨라졌는지는
  §9.2 텔레메트리로 측정**한다.

## 10. 공개 API (시그니처)
- `run_ssw_random_sample_reports_from_toml_text(candidate_toml_text, *, output_dir, seed, mode, grpc_port, aedt_pid=None)`
  — 내부에서 GPU 자동 감지·활성(§9).
- `validate_sweep_toml_text(sweep_text) -> None`
- `sample_fixed_candidates_from_toml_text(sweep_text, count, seed) -> list[str]`
- `peetsfea.__version__ == "0.3.5"`

## 11. 잔여 협의
- GPU 감지 방법(CUDA 런타임 import vs `nvidia-smi` 호출 vs 환경변수)과 폴백 판정 기준.
- GPU 활성 시 `gpus`/`cores` 자동 산정 규칙(노드 코어/메모리 대비).
- `solve_telemetry`의 정확한 키 집합(runner store/대시보드와 1:1).
