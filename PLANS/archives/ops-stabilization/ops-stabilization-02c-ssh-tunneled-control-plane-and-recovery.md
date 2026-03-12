# OPS Stabilization 02C: SSH-Tunneled Control Plane and Recovery

## 목적

이 문서는 외부 포트 개방 없이 worker와 main PC가 안정적으로 통신하는 control plane과 recovery 모델을 정의한다.
핵심은 `localhost-only HTTP control API over SSH tunnel`을 steady-state 기본 경로로 고정하는 것이다.

## 통신 원칙

- main PC에는 localhost bound control API를 둔다.
- worker는 기동 시 main PC로 SSH 터널을 수립한다.
- worker 내부 slot들은 그 터널을 통해 lease, heartbeat, complete, snapshot 요청을 보낸다.
- per-call SSH는 bootstrap, recovery, emergency control 용도로만 남긴다.

## control plane 책임

- worker 등록
- slot lease 부여
- heartbeat 수신
- completion / failure 수신
- resource snapshot 수신
- stale lease 판단

## recovery 대상

- `main restart`
- `tunnel loss`
- `worker death`
- `slot hang`
- `artifact registration delay`

## recovery 규칙

### main restart

- worker job은 계속 살아 있는다.
- main PC는 `squeue/sacct`와 저장 상태를 대조해 살아 있는 worker를 재발견한다.
- stale lease는 timeout 기준으로 만료 또는 재연결 처리한다.

### tunnel loss

- worker가 우선 재수립을 시도한다.
- 일정 시간 넘게 실패하면 worker는 `DEGRADED`로 표기된다.
- main PC는 worker 전체 replacement가 필요한지 별도로 판단한다.

### worker death

- main PC가 replacement worker를 제출한다.
- 다른 살아 있는 worker는 그대로 유지한다.

## 내부 인터페이스 개념

- `worker_id`
- `slurm_job_id`
- `slot_id`
- `lease_token`
- `tunnel_session_id`
- `heartbeat_ts`

## 구현 체크리스트

1. localhost-only control API 경계를 문서와 상태 모델에 반영한다.
2. worker별 SSH 터널 수립/재수립 흐름을 정의한다.
3. tunnel loss와 main restart를 분리한 recovery 로직을 정의한다.
4. stale lease와 worker degradation 조건을 정의한다.
5. emergency path로서 per-call SSH의 사용 범위를 제한한다.

## 테스트 계획

- worker가 SSH 터널을 수립한 뒤 localhost API 호출이 유지되는 테스트
- main restart 후 worker 재발견과 lease 복구 테스트
- tunnel loss 후 재수립 성공과 장기 실패 `DEGRADED` 표기 테스트
- worker death 시 replacement worker만 새로 제출되는 테스트

## 수용 기준

- 외부 포트 개방 없이 steady-state 통신이 유지된다.
- main restart와 tunnel loss가 별도 failure class로 다뤄진다.
- 실행 중 worker 전체를 main PC가 매번 재주입하지 않아도 control plane이 유지된다.
