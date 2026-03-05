# 12-02: Slot-Owned Lifecycle and State Model 전환 계획

## 1. 목적

1. bundle/job 중심 lifecycle을 slot/window 중심으로 전환한다.
2. retry, collect, quarantine, artifact 기록 단위를 개별 slot 기준으로 재정의한다.
3. 특정 계정이나 특정 slot 실패가 다른 slot refill을 막지 않는 상태 모델을 확정한다.

## 2. 현재 문제

1. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)의 `_run_bundle_with_retry()`는 실패와 재시도를 bundle 중심으로 처리한다.
2. 성공 window와 실패 window가 같은 job 결과에 묶여 상태 반영이 거칠다.
3. slot을 독립된 long-lived execution unit으로 보기보다, job 묶음의 부분 결과로 취급하고 있다.

## 3. 구현 범위

### 포함

1. slot 개념 도입
2. slot과 window의 관계 정의
3. slot 단위 retry/격리/수집 규칙 정리
4. state store와 이벤트 기록 구조 조정
5. artifact 기록을 slot/window 기준으로 재정렬

### 제외

1. remote primitive 교체 자체는 12-03에서 다룸
2. scheduler refill loop 자체는 12-01에서 다룸

## 4. 상태 모델

### 4.1 핵심 개체

1. `window_task`
   - 입력 `.aedt` 하나에 대응하는 실제 작업 단위
2. `execution_slot`
   - 실제로 queue에서 window를 받아 실행하는 독립 실행 단위
3. `remote_attempt`
   - slot이 특정 window를 특정 계정/attempt로 실행한 기록

### 4.2 slot 상태

slot 상태 고정 집합:

1. `IDLE`
2. `ASSIGNED`
3. `UPLOADING`
4. `RUNNING`
5. `COLLECTING`
6. `RETRY_WAITING`
7. `QUARANTINED`

window 상태는 기존 `window_tasks`를 유지하되, slot 이벤트와 연결해 해석한다.

### 4.3 전이 규칙

1. `IDLE -> ASSIGNED`
   - scheduler가 새 window를 slot에 할당
2. `ASSIGNED -> UPLOADING`
   - 입력 staging/upload 시작
3. `UPLOADING -> RUNNING`
   - remote 실행 시작
4. `RUNNING -> COLLECTING`
   - remote completion 감지 후 artifact 회수
5. `COLLECTING -> IDLE`
   - 성공적으로 마무리되고 다음 window 대기
6. `RUNNING/COLLECTING -> RETRY_WAITING`
   - retry 가능한 실패
7. `RETRY_WAITING -> ASSIGNED`
   - 동일 slot 또는 다른 slot에 재할당
8. `RUNNING/COLLECTING -> QUARANTINED`
   - retry 소진 또는 비복구성 실패

## 5. lifecycle 규칙

### 5.1 retry

1. retry 단위는 bundle이 아니라 window다.
2. 성공한 window는 재실행하지 않는다.
3. 실패 window만 attempt를 증가시켜 재큐잉한다.
4. 한 slot 실패는 해당 window만 재시도 대상으로 만들고, 다른 slot 진행에는 영향이 없어야 한다.

### 5.2 artifact

1. artifact 기록은 slot 실행 경로와 window 결과를 함께 남긴다.
2. bundle 루트 중심 artifact 기록은 축소한다.
3. 대용량 intermediate artifact는 업로드부터 삭제 직전까지 `/tmp` slot workspace 안에서만 유지한다.
4. `~` 아래에는 최종적으로 꼭 남겨야 하는 소형 메타데이터 또는 명시적 보존 대상만 둔다.
5. 최소 기록:
   - input window id
   - account id
   - slot id
   - attempt no
   - artifact root

### 5.3 quarantine

1. slot 자체는 가능한 재사용 가능한 실행 단위로 유지한다.
2. quarantine는 slot이 아니라 최종 실패한 window에 걸린다.
3. 계정 하나가 불안정해도 다른 계정 slot은 refill을 계속한다.

## 6. 모듈별 구현 항목

1. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)
   - `_run_bundle_with_retry()` 분해
   - slot-owned lifecycle coordinator 도입
2. [`peetsfea_runner/state_store.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/state_store.py)
   - slot/attempt/event 기록 보강
   - window와 slot 관계 추적 API 추가

## 7. 테스트 계획

1. 단일 계정에서 한 window 실패가 다른 window/slot 진행을 막지 않는지
2. retry가 bundle 전체가 아니라 실패 window에만 걸리는지
3. 성공 window artifact와 실패 window 상태가 정확히 분리되는지
4. mock 다계정에서 한 계정 장애가 다른 계정 slot refill을 막지 않는지
5. quarantine가 slot이 아니라 window에 기록되는지

## 8. 완료 기준

1. lifecycle 중심 개체가 bundle이 아니라 slot/window로 전환된다.
2. retry, quarantine, collect, artifact 기록이 slot/window 기준으로 반영된다.
3. 특정 slot 또는 계정 실패가 전체 scheduler 진행을 멈추지 않는다.
4. 12-03의 remote primitive 교체를 얹을 수 있는 상태 모델이 마련된다.

## 9. 가정

1. 이 단계에서는 remote 실행 방법보다 상태 모델 정리가 우선이다.
2. 필요한 경우 기존 job_id는 호환용 식별자로만 유지하고, 내부 의미는 slot/window 중심으로 이동한다.
