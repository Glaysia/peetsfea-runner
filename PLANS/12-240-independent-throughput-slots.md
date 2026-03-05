# PLAN 12: 240/400 Independent Throughput Slots Architecture

## 1. 목적

PLAN 12의 목적은 원격 실행을 단순히 안정화하는 것이 아니다.

1. 현재 기준으로 `3 accounts * 10 jobs/account * 8 windows/job = 240`개의 활성 슬롯이 끊기지 않고 유지되는 구조를 만든다.
2. 향후 계정이 5개로 늘어나면 같은 구조로 `5 * 10 * 8 = 400`까지 확장 가능해야 한다.
3. 슬롯 하나가 현재 `.aedt` 처리를 끝내면 전체 batch 종료를 기다리지 않고 즉시 다음 `.aedt`를 받아 계속 실행해야 한다.
4. 설계는 처음부터 다계정 일반화로 작성하되, 실제 재현과 디버깅은 우선 `gate1-harry` 단일 계정 기준으로 수행한다.

## 2. 현재 구조가 목표와 다른 이유

### 2.1 중앙 batch flush 구조

1. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)는 `queued_windows`를 크게 모아 `_flush_queued_windows()`로 넘긴다.
2. `run_window_bundles()` 호출은 queue를 비우거나 inflight future가 끝날 때까지 반환하지 않는다.
3. 따라서 현재 구조는 "빈 슬롯 즉시 refill"이 아니라 "batch 호출 단위 처리"다.

### 2.2 bundle 중심 lifecycle

1. `_run_bundle_with_retry()`는 실행, 재시도, 수집, 격리를 bundle/job 중심으로 처리한다.
2. 목표는 slot/window 중심 lifecycle인데 현재는 job 묶음이 중심이다.
3. 이 구조에서는 일부 slot이 비어도 다른 slot 완료를 기다리는 경향이 생긴다.

### 2.3 interactive remote control

1. 현재 원격 실행은 `srun --pty` + `pexpect` + `screen` + prompt 복귀 판정에 의존한다.
2. 이 방식은 장기적인 독립 slot 운영보다 대화형 세션 제어에 가깝다.
3. slot 수가 커질수록 prompt 대기, 세션 상태 불일치, `screen` lifecycle이 병목이 된다.

### 2.4 전역 temp / port / project 경로 충돌

1. 과거 로그에는 `Error launching GRPC server on port: 38715. Port may be in use.`가 남아 있다.
2. `Temp directory: /tmp`, `Project directory: /home1/harry261/Ansoft` 사용 흔적도 있다.
3. 이는 slot별 temp, gRPC port, AEDT 작업 디렉터리가 충분히 분리되지 않았다는 뜻이다.

## 3. 단일 계정 probe에서 확인한 사실

1. 2026-03-06에 `gate1-harry` 단일 계정으로 `run_remote_job_attempt()` 2-window probe를 실행했다.
2. `/tmp/...` 원격 workspace 생성과 업로드는 통과했다.
3. 이후 `srun --pty ... bash` 뒤 `_expect_prompt()`에서 장시간 대기했다.
4. 같은 시각 `squeue`에는 새 Slurm job이 `RUNNING`으로 올라왔지만 `screen -ls`에는 세션이 없었다.
5. 이 결과는 compute node 진입 직후의 interactive prompt 제어가 이미 병목이라는 뜻이며, 이후 240/400 slot 유지 구조와 맞지 않는다.

## 4. 설계 원칙

1. 진입점은 계속 `run_pipeline(config)` 단일 경로를 유지한다.
2. CLI, 서브커맨드, 별도 엔트리포인트는 추가하지 않는다.
3. 설계는 다계정 일반화로 한다.
4. 테스트와 원격 디버깅은 우선 단일 계정 기준으로 한다.
5. `/tmp`는 구조의 주제가 아니지만, `~` 용량이 약 100GB로 작기 때문에 업로드 직후 staging부터 실행, collect 직전 산출물, 삭제 전 임시 보관까지 slot 작업 수명주기 전체를 `/tmp`에서 처리한다.
6. 최종 목표는 "많은 job 제출"이 아니라 "항상 빈 slot이 생기면 즉시 refill되는 구조"다.

## 5. PLAN 12 단계 분할

### 12-01. Slot Refill Scheduler 전환

1. 중앙 batch flush 구조를 제거한다.
2. 계정 수와 무관하게 빈 slot을 즉시 refill하는 scheduler로 전환한다.
3. 설계는 multi-account safe여야 하지만 테스트는 1계정 fixture를 기본으로 한다.

참고 문서:
- [`12-01-slot-refill-scheduler-transition.md`](/home/peetsmain/peetsfea-runner/PLANS/12-01-slot-refill-scheduler-transition.md)

### 12-02. Slot-Owned Lifecycle / State Model

1. bundle/job 중심 lifecycle을 slot/window 중심으로 바꾼다.
2. retry, collect, quarantine, artifact 기록 단위를 slot 기준으로 재정의한다.
3. 계정 하나가 막혀도 다른 계정 slot refill은 계속 유지되어야 한다.

참고 문서:
- [`12-02-slot-owned-lifecycle-and-state-model.md`](/home/peetsmain/peetsfea-runner/PLANS/12-02-slot-owned-lifecycle-and-state-model.md)

### 12-03. Non-Interactive Remote Execution / Isolation

1. `screen`/prompt 기반 interactive 제어를 제거한다.
2. submit/poll/collect 중심의 비대화형 primitive로 바꾼다.
3. `/tmp`, `TMPDIR`, gRPC port, workspace, AEDT temp/project 경로를 slot별로 격리한다.
4. `~` 아래에는 대용량 intermediate나 실행 중 산출물을 쌓지 않는다.

참고 문서:
- [`12-03-noninteractive-remote-execution-and-isolation.md`](/home/peetsmain/peetsfea-runner/PLANS/12-03-noninteractive-remote-execution-and-isolation.md)

## 6. 공개 타입/API 방향

`run_pipeline(config)`는 유지한다.

내부 계약은 아래처럼 바꾼다.

1. 기존:
   - bundle 실행 중심
   - batch flush 중심
   - interactive remote session 중심
2. 변경:
   - slot lifecycle 중심
   - refill loop 중심
   - account-agnostic remote primitive 중심

고정 파라미터 해석:

1. `max_jobs`: 계정별 동시 slot 그룹 상한
2. `windows_per_job=8`: slot 그룹 내부 window fan-out 크기
3. `accounts_registry`: 현재 3계정, 향후 5계정까지 확장 가능한 capacity source

## 7. 테스트/수용 기준

### 7.1 현재 acceptance

1. 입력 큐가 충분할 때 3계정 기준 활성 slot 수가 240에 근접하게 유지된다.
2. slot 완료 후 다음 `.aedt` 할당까지 refill latency 상한이 명시된다.
3. 실제 원격 검증은 단일 계정 기준으로 재현 가능하다.

### 7.2 확장 acceptance

1. 같은 구조로 5계정 기준 400 슬롯까지 확장 가능해야 한다.
2. 계정 수 증가는 로직 재작성 없이 config 확장으로 수용되어야 한다.

### 7.3 공통 acceptance

1. 한 slot 실패가 다른 slot refill을 막지 않는다.
2. `screen`/interactive prompt 의존은 제거 대상임이 문서에 명시된다.
3. slot별 temp, port, workspace 충돌 회피 규칙이 모든 하위 문서에 반영된다.
4. 업로드부터 삭제 직전까지의 대용량 작업 데이터는 `~`가 아니라 `/tmp` 수명주기 안에서 처리된다.

## 8. 가정 및 기본값

1. 실제 코드 변경은 `12-01`, `12-02`, `12-03` 순서로 진행한다.
2. 단일 계정 probe에서 확인된 `srun --pty` 정체와 gRPC port 충돌은 현재 모델을 폐기해야 한다는 충분한 근거로 사용한다.
3. `240`과 `400`은 각각 현재/확장 acceptance 숫자이며, 설계는 특정 숫자에 하드코딩하지 않는다.
