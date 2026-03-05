# 11-02: 계정 용량 제어 + 균등 분배 + 8-window bundle 실행 계획

## 1. 목적

1. 계정별 제출 상한을 사용자 전체 잡 기준으로 강제한다.
2. window 처리량 기준 계정 균등 분배를 구현한다.
3. `1 job = 최대 8 window(.aedt)` bundle 실행으로 정합화한다.

## 2. 고정 정책

1. 계정 상한:
   - `RUNNING <= 10`
   - `PENDING <= 3`
2. 용량 집계 범위:
   - `squeue -u $USER` 기준 사용자 전체 잡
3. 균등 지표:
   - `window_throughput_score = completed_windows + inflight_windows` (현재 run)
4. 제약 계정 처리:
   - 제약 계정 제외 후 가능한 계정끼리 재균등

## 3. 스케줄러 아키텍처

## 3.1 핵심 루프

1. `collect_capacity(host)`로 계정별 상태 수집.
2. `build_eligible_accounts()`로 제출 가능한 계정 집합 생성.
3. `pick_account_by_balance()`로 다음 제출 계정 선택.
4. 전역 window queue에서 최대 8개 pop -> bundle 생성.
5. bundle 실행 future 등록.
6. 완료된 future의 window 결과를 개별 반영.

## 3.2 상태 수집 규칙

원격 명령:

```bash
ssh <host_alias> 'squeue -u $USER -h -o "%T"'
```

파싱 규칙:

1. `RUNNING` 계열: `R`, `CG` 카운트
2. `PENDING` 계열: `PD` 카운트
3. 그 외 상태는 참고 지표로만 저장

허용 제출량 계산:

1. `allow_running = max(0, 10 - running_count)`
2. `allow_pending = max(0, 3 - pending_count)`
3. `allowed_submit = allow_running + allow_pending`

## 4. 균등 분배 상세

### 4.1 선택 알고리즘

1. eligible 계정만 대상으로 score 계산.
2. score 최소 계정 우선.
3. tie-break 1: `running_jobs` 최소.
4. tie-break 2: `account_id` 오름차순.

### 4.2 score 집계 정의

1. `completed_windows`: 해당 run에서 `SUCCEEDED + FAILED + QUARANTINED` 합.
2. `inflight_windows`: `ASSIGNED + UPLOADING + RUNNING + COLLECTING` 합.
3. score는 실시간 계산 또는 5초 캐시.

## 5. bundle 모델 전환

## 5.1 타입 변경

`JobSpec`를 bundle 중심으로 전환:

1. `job_id`
2. `account_id`
3. `host_alias`
4. `window_inputs: tuple[WindowTaskRef, ...]` (1..8)

`WindowTaskRef`:

1. `window_id`
2. `input_path`
3. `output_path`
4. `attempt_no`

## 5.2 원격 실행 입력 변경

기존:

1. `run_remote_job_attempt(..., aedt_path=Path)`

변경:

1. `run_remote_job_attempt(..., window_inputs=[...])`
2. case 디렉토리 생성 시 `case_01..case_N`에 서로 다른 aedt 복사

## 5.3 결과 매핑

1. case 요약(`case_01:0`, `case_02:1`)을 bundle 내부 window 순서와 매핑.
2. window별 상태 개별 갱신:
   - success -> `SUCCEEDED`
   - fail -> `FAILED` 또는 `RETRY_QUEUED/QUARANTINED`

## 6. 재시도/격리 규칙

1. 재시도 단위는 window.
2. bundle 내 성공 window는 재실행하지 않음.
3. 실패 window만 `attempt_no+1`로 재큐잉.
4. `job_retry_count` 소진 시 `QUARANTINED`.

## 7. 삭제 수명주기

1. 업로드 성공 직후 해당 window 입력 `.aedt` 삭제 시도.
2. 성공 시 `.aedt.ready`도 즉시 삭제.
3. 삭제 실패는 window 단위 재시도.
4. 재시도 소진 시 `_delete_failed` 이동 + `DELETE_QUARANTINED`.

## 8. 모듈별 구현 변경

1. `peetsfea_runner/scheduler.py`
   - capacity-aware submission loop
   - fairness selection 함수
   - bundle 생성기 추가
2. `peetsfea_runner/pipeline.py`
   - window queue 기반 orchestration
   - bundle 결과 -> window 상태 분해 반영
3. `peetsfea_runner/remote_job.py`
   - 다건 입력(case별 서로 다른 aedt) 지원
4. `peetsfea_runner/state_store.py`
   - account capacity snapshot insert API
   - window 상태/이벤트 업데이트 API

## 9. 관측/로그

필수 로그 라인:

1. `capacity account=<id> running=<r> pending=<p> allowed_submit=<n>`
2. `scheduler pick account=<id> score=<s> windows=<k>`
3. `bundle submitted job_id=<job> account=<id> window_count=<k>`
4. `bundle result job_id=<job> success_windows=<x> failed_windows=<y>`

## 10. 테스트 계획

1. mock squeue로 `R10/PD3` 상한 준수 검증.
2. 한 계정이 상한 초과일 때 다른 계정 재균등 동작 검증.
3. bundle window 수 1..8 경계 검증.
4. case 결과가 정확한 window에 매핑되는지 검증.
5. 실패 window만 재시도되는지 검증.
6. ready 삭제/실패 격리 동작 검증.

## 11. 완료 기준

1. 계정별 제출이 `R10/PD3` 정책을 넘지 않는다.
2. 분배가 score 기반으로 계정 간 편차를 완화한다.
3. Slurm 1 job이 서로 다른 최대 8개 `.aedt`를 처리한다.
4. 재시도/격리가 window 단위로 동작한다.
