# peetsfea (main) → 0.3.2 — runner 의존 계약

> 이 문서는 **peetsfea-runner가 의존 패키지 `peetsfea`(=peetsfea-main)에 요구하는 사항**을
> 정리한 계약서다. peetsfea-main은 다른 PC에서 개발 중이며 이 요구에 맞춰 **0.3.2**로 올린다.
> 상위 설계: `PLANS/MASTER_PLAN.md`, 1차 구현 범위: `GOAL.md`.

## 0. 역할 분담 (경계)
| 소유 | 책임 |
|------|------|
| **peetsfea (main)** | 도메인/시뮬레이션 자체: 기준 sweep 스키마·범위 검증·샘플링, pyaedt로 설계 빌드/해석, 리포트/출력 변수 추출 |
| **peetsfea-runner** | 오케스트레이션: edtmgr(ansysedt 수명·라이선스 점유), 큐/디스패치, 타임아웃 백스톱, DB/아카이브, 부트스트랩 |

핵심 원칙: **peetsfea는 ansysedt를 직접 켜고/끄거나 라이선스를 관리하지 않는다.** 그건 edtmgr(runner) 몫.

## 1. 버전 / 패키징
1. `peetsfea.__version__ == "0.3.2"`. (runner의 기대값 `EXPECTED_PEETSFEA_VERSION`도 0.3.2로 맞춤 —
   `peetsfea_runner/single_simulation_api.py:19`, `single_simulation_remote.py:194`.)
2. 컨테이너에 runner와 함께 설치 가능해야 한다(같은 venv/Python 3.12).
3. **타입 정보 제공:** `py.typed` 동봉 + 공개 API 타입 어노테이션. runner는 strict 타입체킹을
   하므로(AGENTS.md §2) peetsfea 공개 심볼이 타입 체커에서 해석돼야 한다.

## 2. Warm AEDT 접속 계약 (가장 중요)
edtmgr가 ansysedt를 띄워 두고 라이선스를 점유한다. 시뮬 프리미티브는 **새 ansysedt를 띄우지 말고**
edtmgr가 넘겨준 **이미 떠 있는 grpc 세션에 접속**해서 실행해야 한다.

1. 프리미티브가 **grpc 접속 좌표**(예: `grpc_port`, 필요 시 `pid`)를 인자로 받아 그 세션에 붙는다.
   - 제안 시그니처(협의 가능):
     `run_ssw_random_sample_reports_from_toml_text(candidate_toml_text, *, output_dir, seed, mode, grpc_port, aedt_pid=None)`
   - 현재 진입점은 `primitive(candidate_toml_text, output_dir=, seed=, mode=)`
     (`peetsfea_runner/single_simulation_api.py:62,116`). 여기에 grpc 좌표 인자만 추가.
2. pyaedt 연결은 **기존 데스크톱에 attach**(new_desktop 금지), `close_on_exit=False`,
   종료 시 **데스크톱/ansysedt를 닫지 않는다**.
3. 실행 후 프로젝트만 깨끗이 닫아(close project) edtmgr가 같은 ansysedt를 **재획득(re-acquire)** 할 수
   있는 상태로 반환한다. 모달 다이얼로그·고아 프로세스·열린 프로젝트를 남기지 않는다.

## 3. 패스 / 시간 예산 & abort
1. 목표 **~40분**: 마지막 패스를 끝낸 뒤 리포트를 산출/저장.
2. **60분 하드 abort**: 내부 워치독으로 60분에 중단하되 **마지막으로 완료된 패스 기준 리포트**를 반드시 남긴다.
3. **65분 강제종료는 runner/edtmgr의 백스톱**이다. peetsfea는 65분을 소유하지 않지만, 60분 abort를
   스스로 못 지키면 edtmgr가 `SIGKILL`함을 전제로 동작해야 한다(외부 종료에 안전).
4. **패스 텔레메트리 노출:** setup별 pass count, solve 시간 등(아래 §5 결과 구조에 포함).

## 4. TOML 범위 검증 + 샘플링 (정규화 아님)
runner의 Intake(`localhost:7875`)에는 **온전한(complete) sweep toml**이 들어온다. 필드를 채워 주는
"정규화"는 **없다**. 대신 들어온 toml의 **각 파라미터 range(상하한)가 기준 sweep toml의 range 이내
(= range subset)**여야 하며, 그 범위를 벗어나면 **애초에 실패**시킨다.
- 기준 sweep SSOT 예: `examples/0.3.0_sweep.toml`(spec 0.3.1, schema `peetsfea.ssw_coil.step.v1`,
  자유변수 ~20차원). range 형식 `[is_int, start, end, count]`, fixed는 `[*, value, value, 1]`로 freeze.
- 즉 "subset"은 **필드의 부분집합이 아니라 값 범위의 부분집합**이다.

1. **기준 sweep 스키마/범위 정의(0.3.2):** 각 public field의 허용 range(상하한)·타입을 SSOT로 정의.
2. **범위 검증 API:** 들어온 온전한 sweep toml의 모든 range가 기준 range **이내인지 검증**.
   하나라도 기준보다 넓거나 스키마/필드가 어긋나면 **실패(에러)**. (기본값 채우기·필드 보완 없음.)
   - 제안: `validate_sweep_toml_text(sweep_text: str) -> None` (위반 시 예외, 위반 항목 명시).
3. **랜덤 샘플링 API:** (검증 통과한) sweep toml + 개수 N + seed → **fixed candidate toml × N**.
   동일 (toml, seed)면 **결정론적**으로 동일 샘플.
   - 제안: `sample_fixed_candidates_from_toml_text(sweep_text: str, count: int, seed: int) -> list[str]`.
4. 위 API는 runner가 import해 쓸 수 있도록 **안정된 공개 경로**로 노출.

## 5. 결과 / 리포트 출력 계약
1. 결과는 `output_dir` 아래에 쓰고, 프리미티브는 **구조화된 dict**를 반환한다(runner가 DB에 적재).
   기존 결과 저장 스키마와 정합(`peetsfea_runner/single_simulation_store.py`):
   `setup_pass_counts_json`, `solve_telemetry_json`, `csv_text_by_report_json`, `csv_paths_json` 등.
2. **CSV 스키마 유지:** 입력 파라미터(coil_*/ferrite_* 등) + 출력 변수(`k_ratio`, `Lrx_uH` 등).
   결과만/경로만 담긴 축약 스키마로 후퇴 금지.
3. 산출물 디렉토리(`project_name.aedt`, `project_name.aedtresults` 등)를 식별 가능한 형태로 남겨
   runner가 아카이브(20GB 묶음 압축)할 수 있게 한다.

## 6. 파일시스템 규율
1. `/dev/shm`, `/tmp`를 직접 쓰지 않는다. runner가 주는 작업 디렉토리/환경변수를 사용한다
   (`PEETS_RAMDISK_ROOT`, `PEETS_DISK_ROOT`, `ANSYS_WORK_DIR` 등; `job_tmpfs`/`job_disk` 기반).
2. 전역/홈에 잔여물을 남기지 않는다(부트스트랩 멱등성·웜캐시를 깨지 않도록).

## 7. 라이선스 / AEDT 수명 (재확인)
- peetsfea는 ansysedt 기동·종료·재기동, EDT 라이선스 획득/반납에 **관여하지 않는다**. 전부 edtmgr가 소유.
- 라이선스는 충분하므로 peetsfea가 라이선스 가용성을 판단·대기하지 않는다(runner 정책).

## 8. 오류 처리
1. 실패 시 **구조화된 예외/반환**으로 stage·type·message를 제공한다. runner는 이를
   `terminal_state="failed"` 봉투에 담아 기록한다(`single_simulation_api.py:139-161` 참조).
2. 외부 `SIGKILL`(65분 백스톱)·grpc 끊김 상황에서 부분 산출물이 있으면 그대로 두고, 데이터 정합만 보장.

## 9. 정리: runner가 0.3.2에 기대하는 공개 API (제안, 최종 협의)
| 목적 | 제안 심볼 |
|------|-----------|
| warm AEDT에 붙어 단일 시뮬 실행 | `run_ssw_random_sample_reports_from_toml_text(..., grpc_port=, aedt_pid=)` |
| sweep toml 범위 검증(기준 이내인지) | `validate_sweep_toml_text(sweep_text)` |
| sweep → fixed toml N개 샘플 | `sample_fixed_candidates_from_toml_text(sweep_text, count, seed)` |
| 버전 | `peetsfea.__version__ == "0.3.2"` |

## 10. 잔여 협의 사항
- grpc 접속에 `pid`까지 필요한지, `grpc_port`만으로 충분한지.
- 기준 sweep 범위/스키마의 정확한 정의 위치와 버전(예: `examples/0.3.x_sweep.toml`)와
  검증 엄밀도(경계 포함 여부, count·is_int 일치까지 볼지).
- 결과 dict의 정확한 키 집합(현 store 스키마와 1:1 맞춤).
- 샘플링 분포/제약(범위·조합 규칙)의 소유 위치.
