# 12-01: Slot Refill Scheduler 전환 계획

## 1. 목적

1. `_flush_queued_windows()` 기반 중앙 batch flush 구조를 제거한다.
2. 계정 수와 무관하게 빈 slot을 즉시 refill하는 scheduler로 전환한다.
3. 현재 3계정 240 slot, 향후 5계정 400 slot 구조를 같은 scheduler로 수용한다.
4. 설계는 다계정 일반화로 작성하되, 테스트와 디버깅은 단일 계정 기준으로 시작한다.

## 2. 현재 문제

1. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)의 `_flush_queued_windows()`는 많은 window를 모아서 한 번에 흘려보낸다.
2. [`peetsfea_runner/scheduler.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/scheduler.py)의 `run_window_bundles()`는 호출이 길게 블로킹되어 중앙 제어가 batch 경계에 묶인다.
3. 현재 구조는 "빈 slot 발견 -> 즉시 refill"이 아니라 "batch 호출 안에서 가능한 만큼만 제출"이다.
4. 이 구조는 queue가 충분해도 일부 slot이 비는 순간이 생길 수 있고, refill latency가 길어진다.

## 3. 구현 범위

### 포함

1. 중앙 scheduler를 batch flush 구조에서 refill loop 구조로 전환
2. 빈 slot 감지와 즉시 할당 로직 도입
3. 계정별 capacity 조회와 slot refill 연결
4. multi-account safe한 account selection 유지
5. 단일 계정 fixture 기준 테스트 추가

### 제외

1. remote primitive 교체 자체는 12-03에서 다룸
2. slot lifecycle/state model 정교화는 12-02에서 다룸

## 4. 핵심 설계

### 4.1 제어 단위

1. 전역 queue에는 `WindowTaskRef`가 유지된다.
2. scheduler는 batch 단위 submit이 아니라 "빈 실행 slot" 단위로 판단한다.
3. 빈 slot이 생기면 즉시 queue에서 다음 window를 할당한다.

### 4.2 scheduler 루프

고정 루프:

1. 현재 계정별 capacity snapshot 수집
2. 계정별 active slot 수 계산
3. refill 가능한 slot 수 계산
4. 전역 queue에서 필요한 window를 pop
5. slot 실행 future 등록
6. 완료된 future 반영
7. queue가 남아 있으면 즉시 다음 refill 반복

### 4.3 account 일반화

1. 계정 수는 1개든 5개든 같은 루프를 사용한다.
2. account selection은 현재 `allowed_submit`과 계정별 inflight 상태를 같이 본다.
3. 특정 계정이 막혀도 다른 계정이 계속 refill될 수 있어야 한다.

### 4.4 기존 함수 처리

1. `run_window_bundles()`는 제거하거나, 내부적으로 "짧게 반환하는 refill primitive"로 축소한다.
2. `_flush_queued_windows()`는 제거한다.
3. `run_pipeline()`은 한 번의 큰 batch orchestration이 아니라 지속적인 refill coordinator가 된다.

## 5. 모듈별 구현 항목

1. [`peetsfea_runner/pipeline.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/pipeline.py)
   - `windows_batch_size` 계산 제거
   - `_flush_queued_windows()` 제거
   - refill loop 도입
   - inflight slot 추적 구조 도입
2. [`peetsfea_runner/scheduler.py`](/home/peetsmain/peetsfea-runner/peetsfea_runner/scheduler.py)
   - `run_window_bundles()`를 refill 중심 API로 대체 또는 축소
   - account 선택과 inflight slot 계산을 짧은 step 함수로 분리

## 6. 테스트 계획

1. 단일 계정에서 slot 하나 완료 즉시 다음 window가 할당되는지
2. queue 크기가 작아도 불필요한 batch 대기가 사라지는지
3. mock 다계정에서 계정 수가 늘어나도 동일 loop가 동작하는지
4. 한 계정 `allowed_submit=0`일 때 다른 계정 refill이 계속되는지
5. 기존 `run_window_bundles()` 블로킹 경계가 제거되는지

## 7. 완료 기준

1. `_flush_queued_windows()`가 더 이상 scheduler의 중심 경로가 아니다.
2. slot 완료 후 전체 batch 종료를 기다리지 않고 즉시 refill이 일어난다.
3. 계정 1개와 다계정 mock 시나리오 모두 같은 refill loop로 처리된다.
4. 이후 12-02에서 slot lifecycle 정교화를 연결할 수 있는 구조가 마련된다.

## 8. 가정

1. 이 단계에서는 remote 실행 primitive는 기존 구현을 임시 유지할 수 있다.
2. 핵심은 "빈 slot 즉시 refill" 구조를 먼저 세우는 것이다.
