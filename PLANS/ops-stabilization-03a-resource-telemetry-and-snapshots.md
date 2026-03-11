# OPS Stabilization 03A: Resource Telemetry and Snapshots

## 목적

이 문서는 node, worker, slot 수준에서 자원 사용량을 수집하고 저장하는 계획서다.
핵심은 GUI와 운영 판단에 필요한 `할당 메모리`, `실사용 메모리`, `부하`, `프로세스 수`를 느린 주기의 신뢰 가능한 snapshot으로 남기는 것이다.

## 수집 대상

### Node

- total memory
- used memory
- free memory
- load average
- tmp / scratch 사용량
- running worker 수
- active slot 수

### Worker

- configured slots
- active slots
- idle slots
- worker RSS
- worker CPU
- tunnel health

### Slot

- slot RSS
- slot CPU
- active process count
- last progress timestamp
- artifact bytes written

## 저장 주기

- health/capacity: 짧은 주기
- resource snapshot: 긴 주기
- GUI overview: pre-aggregated snapshot 재사용

정확한 주기는 구현 시 정하되, 원칙은 "화면이 느려지지 않을 정도로 드물고, 운영 판단은 가능할 정도로 충분"이다.

## 데이터 모델 후보

- `node_resource_snapshots`
- `worker_resource_snapshots`
- `slot_resource_snapshots`
- `resource_summary_snapshots`

## 구현 체크리스트

1. node / worker / slot snapshot 스키마를 분리한다.
2. `할당 메모리`와 `실사용 메모리`를 같은 화면에서 비교 가능하게 저장한다.
3. load와 process count를 함께 저장해 병목 설명력을 높인다.
4. GUI가 raw table 대신 pre-aggregated summary를 읽도록 준비한다.
5. snapshot 수집 실패와 stale snapshot을 경보로 연결한다.

## 테스트 계획

- snapshot이 주기적으로 기록되는 테스트
- 메모리와 load 값이 summary에 반영되는 테스트
- stale snapshot이 health 경보로 표기되는 테스트
- GUI 요약이 raw scan 없이 summary snapshot을 쓰는 테스트

## 수용 기준

- 운영자가 node/worker/slot 수준 자원 사용량을 기록으로 확인할 수 있다.
- GUI에서 `할당 메모리`, `실사용 메모리`, `부하`를 보여줄 데이터가 준비된다.
- snapshot 수집이 GUI 성능을 망치지 않는다.
