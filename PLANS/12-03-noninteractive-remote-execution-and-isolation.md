# 12-03: Non-Interactive Remote Execution and Slot Isolation 계획

## 1. 목적

1. `srun --pty` + `pexpect` + `screen` + prompt 복귀 판정 기반 원격 실행 모델을 제거한다.
2. submit / poll / collect 중심의 비대화형 remote primitive로 교체한다.
3. slot별 `/tmp` workspace, `TMPDIR`, gRPC port, AEDT temp/project 경로를 분리해 동시 실행 충돌을 제거한다.
4. `~` 용량이 약 100GB로 작으므로 업로드부터 삭제 직전까지의 대용량 작업 데이터는 전부 `/tmp`에서 처리한다.

## 2. 현재 문제

1. [`peetsfea_runner/remote_job.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/remote_job.py)는 interactive shell과 `screen` window를 전제로 한다.
2. 단일 계정 probe에서도 `srun --pty` 뒤 prompt 복귀를 기다리며 장시간 멈췄다.
3. 과거 로그에는 gRPC 포트 충돌이 있었고, `/tmp`와 `/home1/.../Ansoft` 같은 전역 경로 사용 흔적이 남아 있다.
4. 이 구조는 240/400 slot 유지 목표에 맞지 않는다.

## 3. 구현 범위

### 포함

1. interactive remote control 제거
2. 비대화형 submit/poll/collect contract 정의
3. slot별 workspace / temp / port 분리
4. 원격 결과 수집과 cleanup 계약 재정의
5. 단일 계정 실제 probe 기준 검증 시나리오 문서화

### 제외

1. scheduler refill 구조는 12-01에서 다룸
2. state model 정교화는 12-02에서 다룸

## 4. 새 remote primitive 계약

### 4.1 submit

1. slot에 할당된 window 집합을 원격 `/tmp` workspace에 staging
2. 비대화형 Slurm submit 실행
3. submit 결과로 job 식별자와 metadata 확보

### 4.2 poll

1. job 상태를 주기적으로 조회
2. `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, timeout 등 terminal 상태 판정
3. prompt 복귀 대신 명시적 상태 조회 결과만 사용

### 4.3 collect

1. terminal 상태 후 결과 파일과 로그 수집
2. exit code, case summary, artifact root를 비대화형으로 확보
3. slot별 collect 결과를 state store에 반영
4. collect 완료 전까지 intermediate 산출물은 `~`로 승격하지 않는다.

### 4.4 cleanup

1. slot workspace cleanup
2. 실패 시 cleanup 실패를 별도 오류로 기록
3. cleanup 실패가 다른 slot 진행을 막지 않음
4. 삭제 전까지 필요한 모든 staging과 임시 보관은 `/tmp` 내부에서 끝낸다.

## 5. slot 격리 규칙

### 5.1 workspace

1. 원격 workspace는 slot 식별자를 포함해야 한다.
2. 예시:
   - `/tmp/peetsfea/<account_id>/<slot_id>/<attempt_id>`
3. 입력 업로드 직후 파일도 이 workspace 아래로만 들어가며 `~` 아래 작업 경로는 사용하지 않는다.

### 5.2 temp

1. `TMPDIR`는 slot workspace 내부 하위 temp 디렉터리로 고정한다.
2. 전역 `/tmp` 공유는 허용하되, slot 하위 디렉터리로 namespace를 분리한다.

### 5.3 gRPC port

1. 각 slot은 고유 port를 사용해야 한다.
2. port 충돌은 retry 대상이 아니라 격리 규칙 위반으로 본다.
3. port 할당 방식은 deterministic하거나 충돌 검증 가능한 방식이어야 한다.

### 5.4 AEDT project/temp

1. AEDT가 쓰는 project/temp 경로도 slot 식별자를 포함해야 한다.
2. `/home1/.../Ansoft` 같은 전역 공용 경로는 기본 경로로 사용하지 않는다.
3. project 사본, solver intermediate, collect 전 결과물도 모두 slot별 `/tmp` 경로에 위치해야 한다.

## 6. 모듈별 구현 항목

1. [`peetsfea_runner/remote_job.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/remote_job.py)
   - `pexpect`/prompt 의존 제거
   - `screen` 관련 함수 제거
   - 비대화형 submit/poll/collect 흐름으로 교체
2. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)
   - remote attempt 결과 반영 계약 수정
   - slot isolate metadata 연결

## 7. 테스트 계획

1. 단일 계정 실제 probe에서 interactive prompt 없이 submit/running/completion/collect가 가능한지
2. `screen` 제거 후에도 per-slot 로그와 exit code 수집이 유지되는지
3. 동시 slot synthetic 테스트에서 gRPC port 충돌이 없는지
4. `/tmp` workspace와 `TMPDIR` 경로가 slot별로 분리되는지
5. AEDT project/temp 경로가 전역 공용 위치로 수렴하지 않는지
6. 업로드부터 삭제 전 cleanup까지 대용량 작업 파일이 `~` 아래에 남지 않는지

## 8. 완료 기준

1. `srun --pty` + `pexpect` + `screen` 경로가 주 실행 경로에서 제거된다.
2. prompt 복귀 대기가 아니라 명시적 submit/poll/collect로 상태를 판정한다.
3. slot별 workspace, temp, gRPC port, AEDT project 경로 충돌 회피 규칙이 적용된다.
4. 단일 계정 기준 실제 probe를 다시 수행해 interactive 병목이 제거됐음을 검증할 수 있다.
5. 업로드부터 삭제 직전까지의 대용량 작업 데이터가 `~`가 아니라 `/tmp` 경로에서만 처리된다.

## 9. 가정

1. 설계는 다계정 일반화로 작성하지만 실제 원격 검증은 우선 1계정으로 수행한다.
2. 다만 `~` 용량 제약 때문에 실제 대용량 작업 수명주기는 `/tmp` 전체 경로에서 처리한다.
