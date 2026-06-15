# peetsfea (main) → 0.3.2 — 추가할 것 & 일차 이유

> peetsfea-runner가 의존 패키지 `peetsfea`(0.3.2)에 **추가로 요구하는 것**과 **그게 필요한 일차 이유**만 적는다.
> runner 내부 사정(edtmgr 구현·DB·아카이브·부트스트랩 등)은 여기 없다 → `PLANS/peetsfea_runner.md` / `PLANS/MASTER_PLAN.md`.
> 원칙: **peetsfea는 ansysedt를 직접 켜고/끄거나 라이선스를 관리하지 않는다.**

| # | 0.3.2에 추가할 것 | 일차 이유 |
|---|------------------|-----------|
| 1 | `__version__ == "0.3.2"` + `py.typed` 동봉(공개 API 타입) | runner가 0.3.2를 기대하고 의존 코드를 strict 타입체킹함 |
| 2 | **이미 떠 있는 ansysedt에 접속**해 실행: 프리미티브가 `grpc_port`(필요 시 `pid`)를 받아 그 세션에 attach, **자체 기동/종료 금지**, `close_on_exit=False` | runner가 ansysedt를 warm+라이선스 점유 상태로 띄워 두고 빌려줌 — 매 시뮬마다 켜면 너무 느리고 라이선스를 놓침 |
| 3 | 끝나면 **프로젝트만 닫고** AEDT는 살린 채 반환 | runner(edtmgr)가 같은 ansysedt를 즉시 재획득해 다음 시뮬에 재사용 |
| 4 | **60분 하드 abort + 마지막 완료 패스 리포트** 산출(내부 워치독) | runner가 시뮬 시간 상한이 필요; 60분 안에 스스로 리포트를 남겨야 함(못 지키면 외부에서 65분에 강제종료됨) |
| 5 | **sweep 범위 검증 API** `validate_sweep_toml_text(text)`: 들어온 온전한 sweep toml의 모든 range가 기준 sweep(`examples/0.3.x_sweep.toml`)의 range **이내**인지 확인, 넓으면 예외 | runner의 Intake(:7875)가 기준 범위를 벗어난 입력을 **애초에 거절**해야 함 |
| 6 | **샘플링 API** `sample_fixed_candidates_from_toml_text(text, count, seed) -> list[str]`(동일 seed면 결정론적) | runner의 Intake가 sweep 1건을 fixed candidate N개로 펼쳐 큐에 넣음 |
| 7 | **구조화된 결과 반환**: 출력 변수(`k_ratio`, `Lrx_uH` 등)+입력 파라미터 CSV, setup별 pass count·solve 시간, 산출물 디렉토리(`*.aedt`/`*.aedtresults`) 식별자 | runner가 결과를 DB에 적재하고 산출물 디렉토리를 아카이브함 |
| 8 | **작업 디렉토리/환경변수 준수**(`/dev/shm`·`/tmp` 직접 사용 금지, runner가 주는 경로 사용) | 클러스터 자원 정책(잡 전용 tmpfs/disk만 사용) |
| 9 | **실패 시 구조화된 예외/반환**(stage·type·message) | runner가 `terminal_state="failed"`로 기록 |

## 제안 공개 API (시그니처는 협의)
- `run_ssw_random_sample_reports_from_toml_text(candidate_toml_text, *, output_dir, seed, mode, grpc_port, aedt_pid=None)`
- `validate_sweep_toml_text(sweep_text: str) -> None`  *(위반 시 예외)*
- `sample_fixed_candidates_from_toml_text(sweep_text: str, count: int, seed: int) -> list[str]`
- `peetsfea.__version__ == "0.3.2"`

## 0.3.2 연동 검증 (2026-06-16) — 계약 일치 ✅ / 버그 1개
runner ↔ peetsfea 0.3.2 점검 결과 계약이 거의 완벽히 맞는다:
- 프리미티브 `run_ssw_random_sample_reports_from_toml_text(candidate_toml_text, *, output_dir,
  seed, mode, grpc_port, aedt_pid, ...)` — attach 시 `keep_desktop_alive=True`(AEDT 살림, 프로젝트만
  닫음) ✅
- 반환 `SswRandomSampleReportResult`는 **TypedDict**라 키(`setup_pass_counts/solve_telemetry/
  csv_paths/csv_text_by_report`)가 runner store 스키마와 1:1 ✅
- `SOLVE_HARD_ABORT_SECONDS=3600`(60분) < runner 백스톱 3900(65분) — Q2 순서 정확 ✅
- `validate_sweep_toml_text` / `sample_fixed_candidates_from_toml_text` 노출 ✅
- runner 38 테스트 그린(0.3.2 설치).

### 🔴 수정 필요 (실 solve 차단) — 기준 sweep toml 패키징
- `DEFAULT_REFERENCE_TOML_PATH = Path(__file__).resolve().parents[2] / "examples" / "0.3.2_sweep.toml"`
  (`ssw_design_space.py:17`)는 **소스 레이아웃 가정**이라, pip 설치 시
  `<venv>/lib/python3.12/examples/0.3.2_sweep.toml`(존재 안 함)로 잡힌다. 게다가 pyproject
  `include=["src","tests","entry"]`에 **`examples`가 빠져** 패키지에 동봉되지 않는다.
- 이 reference는 run 경로에서 실제로 읽힌다(`build_ssw_aedt_identity(sample.toml_path,
  reference_toml_path)`) → 설치 환경에서 실 candidate solve가 **FileNotFound로 실패**.
- **수정(peetsfea):** ① `0.3.2_sweep.toml`(기준 sweep SSOT)을 패키지 데이터로 동봉,
  ② `DEFAULT_REFERENCE_TOML_PATH`를 `importlib.resources`로 해석(소스/설치 양쪽 동작).
- 임시 우회(runner): 프리미티브에 `reference_toml_path=<실경로>` 명시 전달 가능(요청 시 디스패처 옵션 추가).

## 협의 필요
- grpc 접속에 `pid`까지 필요한지, `grpc_port`만으로 충분한지.
- 범위 검증 엄밀도: `[start, end]` 상하한만 볼지, `count`·`is_int`(정수/실수 플래그)까지 정합 볼지.
- 결과 dict의 정확한 키 집합(runner DB 스키마와 1:1).
- 기준 sweep 범위 SSOT의 위치/버전(`examples/0.3.x_sweep.toml`).
