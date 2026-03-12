# Roadmap Tonight 01B: Restart Rediscovery and DB Retention Policy

## 목적

이 문서는 tonight restart-safe 복구 기준과 DB 보존 정책을 고정한다.
목표는 `peetsfea_runner.duckdb`를 단순 캐시가 아니라 런타임 상태 저장소로 다루고, 삭제가 필요한 경우를 예외로 한정하는 것이다.

## 현재 상태

- runner는 restart 후 slurm worker를 rediscover한다.
- worker 상태, `observed_node`, `tunnel_state`, `heartbeat_ts`는 DuckDB에 저장된다.
- 현재 DB 경로 기본값은 `/home/peetsmain/peetsfea-runner/peetsfea_runner.duckdb`다.
- DB를 무조건 삭제하면 restart-safe 복구와 충돌한다.

## 핵심 변경

- DB 기본 정책은 `유지`다.
- DB 삭제는 아래 경우만 허용한다.
  - schema 변경
  - state 의미 변경
  - ingest 의미 변경
- restart-safe 운영 변경에서는 DB를 지우지 않는다.
- live continuous service DB와 sample canary DB는 분리한다.

판단표는 아래로 고정한다.

| 변경 유형 | DB 처리 |
| --- | --- |
| `dispatch.mode` 도입 | 유지 |
| canary gate 강화 | 유지 |
| `worker_bundle_multiplier` 조정 | 유지 |
| bad-node 정책 추가 | 유지 |
| schema 변경 | 삭제 |
| state 의미 변경 | 삭제 |
| ingest 의미 변경 | 삭제 |

rediscovery 성공 조건은 아래와 같이 둔다.

- active slurm worker가 DB에 다시 연결된다.
- `observed_node`가 다시 보인다.
- `tunnel_state`가 `CONNECTED` 또는 명확한 `DEGRADED`로 보인다.
- `heartbeat_ts`가 stale 범위를 벗어나지 않는다.

경로 정책은 아래와 같이 둔다.

- live service DB
  - `/home/peetsmain/peetsfea-runner/peetsfea_runner.duckdb`
- validation lane DB
  - `/home/peetsmain/peetsfea-runner/tmp/tonight-canary/<window>/state.duckdb`
- validation lane output root
  - `/home/peetsmain/peetsfea-runner/tmp/tonight-canary/<window>/output`
- validation lane은 live DB를 재사용하지 않는다.

## 운영 절차

1. restart 전 `dispatch.mode=drain`으로 신규 submit을 멈춘다.
2. 변경 유형을 위 판단표로 분류한다.
3. DB 유지 변경이면 service stop 후 DB를 건드리지 않고 restart한다.
4. DB 삭제 변경이면 service가 완전히 내려간 뒤 DB를 삭제하고 restart한다.
5. sample canary는 dedicated canary DB/output root로 별도 수행한다.
6. continuous service restart 직후 slurm rediscovery와 tunnel heartbeat를 확인한다.
7. rediscovery가 깨지면 real `.aedt`로 넘어가지 않는다.

restart 복구와 stale tunnel의 경계는 아래로 둔다.

- stale tunnel은 control-plane 복구 문제다.
- worker가 slurm에서 계속 `RUNNING`이면 즉시 worker loss로 보지 않는다.
- DB 유지 restart에서 stale tunnel이 보여도 rediscovery가 되면 다음 판단으로 넘긴다.

## 테스트/수용 기준

- DB 유지 restart에서 active worker rediscovery가 된다.
- schema/state/ingest 변경일 때만 DB 삭제로 분기한다.
- `observed_node`, `tunnel_state`, `heartbeat_ts`를 restart 후 다시 확인할 수 있다.
- stale tunnel과 worker loss를 같은 bucket으로 보지 않는다.

수용 기준은 다음과 같다.

- DB 삭제 규칙이 tonight 범위에서 과도하지 않다.
- restart-safe 변경과 DB reset 변경의 경계가 구현자에게 명확하다.

## 참고 문서

- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md](./roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md)
- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
