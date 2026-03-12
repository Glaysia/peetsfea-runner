# OPS Stabilization 03C: Night Operations Rollout and Service Boundary

## 목적

이 문서는 오늘 밤 전체 일괄 전환을 실제 운영 절차로 옮기는 계획서다.
핵심은 canary, full cutover, fallback, 관측 절차를 한 문서 안에서 고정하고, NAS/공개 저장소와 runner의 경계를 미리 정의하는 것이다.

## 야간 롤아웃 순서

1. 문서 분해와 work item 매핑 완료
2. `01a -> 01b -> 02a -> 02b -> 02c -> 03a -> 03b -> 03c` 순으로 구현
3. service stop
4. major 변경이면 DB reset
5. 새 구조로 service restart
6. `examples/sample.aedt` 다건 canary
7. canary 통과 시 full backlog 처리
8. 실패 시 fallback

## 야간 운영 체크리스트

- worker alive 수
- slot active / idle
- refill latency
- tunnel health
- `할당 메모리`
- `실사용 메모리`
- 부하
- 최근 실패 및 stale 경보

## fallback 규칙

- canary 실패 시 heavy input으로 넘어가지 않는다.
- fallback은 `01a/01b` 안정화만 유지한 기존 처리 경로 복구를 뜻한다.
- 실패 원인은 다음 야간 배치 전까지 문서와 로그에 남긴다.

## canary 제거 조건

- slot pool refill이 안정적으로 동작한다.
- SSH 터널 기반 control plane이 끊기지 않는다.
- GUI에서 핵심 health와 resource 지표가 보인다.
- service restart 후 상태 복구가 된다.

## service boundary

- runner는 control plane과 execution orchestration에 집중한다.
- 공개 storage는 별도 namespace와 quota를 가진 계층으로 분리한다.
- runner 내부 output 디렉터리를 곧바로 공개 NAS처럼 다루지 않는다.
- NAS는 이번 전환의 구현 범위가 아니라 후속 서비스화 workstream이다.

## 구현 체크리스트

1. 야간 cutover 절차를 한 문서 기준으로 고정한다.
2. canary 통과 조건과 fallback 조건을 명문화한다.
3. 운영자 관측 항목을 GUI와 로그 항목에 매핑한다.
4. NAS와 runner의 책임 경계를 명확히 남긴다.

## 테스트 계획

- canary 성공 시 full backlog 단계로 넘어가는 운영 테스트
- canary 실패 시 fallback으로 빠지는 운영 테스트
- service restart 후 상태 복구 확인 테스트
- GUI와 로그만으로 야간 상태 판단이 가능한지 점검 테스트

## 수용 기준

- 야간 전환 절차가 문서 기준으로 흔들리지 않는다.
- canary와 full rollout, fallback 경계가 명확하다.
- 이후 NAS/공개 저장소 설계가 runner 내부 상태 모델과 충돌하지 않게 경계가 정리된다.
