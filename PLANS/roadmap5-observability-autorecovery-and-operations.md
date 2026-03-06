# Roadmap 5: Observability Autorecovery And Operations

## Summary
- 24/7 서비스는 단순히 실행되는 것만으로 부족하고, 운영자가 왜 지금 목표 쓰루풋이 안 나오는지 즉시 볼 수 있어야 한다.
- 일부 계정 또는 일부 worker만 죽어도 service 전체 재시작 없이 자동 복구되어야 한다.
- 웹 상태 페이지와 로그는 운영 문제를 설명하는 도구여야지, 총량만 보여주는 화면으로 끝나면 안 된다.

## Must-Have Improvements
- 계정별 active worker 수, active slot 수, completed slot 수, failed/quarantined 수를 항상 노출한다.
- 목표 쓰루풋 대비 현재 활성 슬롯 수를 바로 볼 수 있게 한다.
- refill latency, 계정별 capacity drop, worker churn을 관측 가능하게 한다.
- 특정 계정만 죽어도 service 재시작 없이 자동 복구한다.
- readiness 부족, launch 실패, solve 실패, collect 실패를 운영 화면과 로그에서 구분해 보여준다.
- worker가 `10` 아래로 떨어진 계정을 자동으로 감지하고 보충 worker를 띄우는 복구 루프를 운영 요구사항으로 둔다.
- 운영자가 "왜 지금 240이 안 나오나"를 `웹 + journal`만으로 추적할 수 있게 한다.

## Acceptance Signals
- 웹과 로그만으로 어떤 계정이 몇 개의 worker를 실제로 유지하고 있는지 즉시 알 수 있다.
- 목표 `240/400` 대비 현재 활성 슬롯 수가 바로 보인다.
- 일부 계정 worker가 죽으면 service 전체 재시작 없이 자동 복구된다.
- 실패한 worker와 quarantined worker 수가 계정별로 드러난다.
- 운영자가 병목 계정과 실패 원인을 짧은 시간 안에 식별할 수 있다.

## Assumptions
- 운영성 요구사항은 구현 편의보다 우선한다.
- service 전체 재시작은 예외 복구 수단이지 정상 운영 수단이 아니다.
- 웹 상태 페이지와 journal은 장기 운영에서 1차 진단 도구로 유지한다.
