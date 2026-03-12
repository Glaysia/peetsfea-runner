# Roadmap Tonight 01A: Drain Bootstrap and Control Toggle

## 목적

이 문서는 tonight 재시작의 제어점을 정의한다.
목표는 `dispatch.mode=run|drain`를 명시적 운영 토글로 만들고, `drain`이 신규 submit만 막고 inflight worker는 자연 종료시키도록 고정하는 것이다.

## 현재 상태

- 현재 continuous loop에는 운영자가 즉시 읽을 수 있는 `dispatch.mode` 제어점이 없다.
- restart 전에는 service stop 외에 “새 bundle 제출만 멈추는” 수단이 필요하다.
- tonight 창은 `00:00`, `02:00`, `04:00`, `06:00`로 정해져 있다.
- 현재 코드베이스에는 `tmp/runtime/dispatch.mode`를 실제로 읽는 구현이 아직 없다.

## 핵심 변경

- repo root 기준 control path는 `tmp/runtime/dispatch.mode`로 고정한다.
- 허용 값은 `run`, `drain` 두 개다.
- 파일이 없으면 기본값은 `run`이다.
- 공백/대소문자는 무시하고 정규화한다.
- 알 수 없는 값은 `run`으로 간주하되 경고를 남긴다.

`dispatch.mode` 계약은 아래와 같다.

- `run`
  - 정상 dispatch
  - bundle submit 허용
- `drain`
  - 신규 bundle submit 금지
  - 이미 제출된 worker는 취소하지 않음
  - inflight worker의 결과 수집과 종료는 정상 진행

현재 fallback 계약은 아래와 같다.

- `dispatch.mode`가 아직 미구현이면 tonight maintenance는 hard stop fallback으로 수행한다.
- fallback 제어 명령은 `systemctl --user stop peetsfea-runner`다.
- `dispatch.mode` 구현 전까지는 `drain -> restart -> run`이 목표 상태이고, 실제 실행은 `status -> stop -> canary -> start` 순서를 따른다.

## 운영 절차

1. `00:00` 창에서 `dispatch.mode` 읽기 경로를 bootstrap한다.
2. 이후 모든 창 전환은 먼저 `tmp/runtime/dispatch.mode`를 `drain`으로 둔다.
3. 신규 submit 정지가 확인되면 restart 절차로 넘어간다.
4. restart와 canary가 끝나면 `dispatch.mode`를 `run`으로 되돌린다.
5. `run` 복귀 전에는 real `.aedt` dispatch를 재개하지 않는다.

현재 구현 기준 fallback 절차는 아래와 같다.

1. `systemctl --user status peetsfea-runner --no-pager -n 20`
2. `systemctl --user stop peetsfea-runner`
3. service 완전 종료 확인
4. sample canary 수행
5. `systemctl --user start peetsfea-runner`
6. `systemctl --user status peetsfea-runner --no-pager -n 20`

운영자 체크리스트는 아래와 같다.

- `drain` 전환 후 active worker 수는 즉시 줄지 않아도 된다.
- `drain` 전환 후 신규 submit log는 멈춰야 한다.
- `run` 복귀 후 신규 submit이 다시 보이면 정상이다.
- fallback 경로에서는 stop 후 `Active: inactive`가 확인돼야 다음 단계로 간다.

## 테스트/수용 기준

- `drain` 상태에서 신규 bundle 제출이 멈춘다.
- inflight worker는 `drain` 상태에서도 정상 종료한다.
- `run` 복귀 후 dispatch가 재개된다.
- control file 부재 시 기본값이 `run`이다.
- 잘못된 값은 `run`으로 처리되고 경고가 남는다.

수용 기준은 다음과 같다.

- tonight restart가 `stop only`가 아니라 `drain -> restart -> run` 모델로 설명된다.
- 운영자가 토글 위치와 의미를 바로 이해할 수 있다.

## 참고 문서

- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-01b-restart-rediscovery-and-db-retention-policy.md](./roadmap-tonight-01b-restart-rediscovery-and-db-retention-policy.md)
- [roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md](./roadmap-tonight-01c-canary-real-cutover-and-go-no-go-runbook.md)
