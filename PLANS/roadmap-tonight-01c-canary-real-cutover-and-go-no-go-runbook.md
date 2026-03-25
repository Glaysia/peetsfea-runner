# Roadmap Tonight 01C: Canary Real Cutover and Go/No-Go Runbook

## 목적

이 문서는 sample canary에서 real `.aedt`로 넘어가는 tonight runbook을 고정한다.
목표는 각 창에서 무엇을 보고, 어떤 조건에서 hold/fallback/next-window로 갈지 운영자가 바로 결정할 수 있게 만드는 것이다.

## 현재 상태

- tonight cutover는 `00:00`, `02:00`, `04:00`, `06:00` 네 개 창으로 운영한다.
- sample canary는 `10분`으로 고정되어 있다.
- real `.aedt` 재개 조건은 restart 안정성과 CSV schema gate를 동시에 만족해야 한다.
- 현재 user service는 `continuous_mode=true`라서 service 재기동만으로는 canary gate가 실행되지 않는다.
- 현재 코드에는 `dispatch.mode` soft drain 구현이 아직 없다.

## 핵심 변경

- cutover 순서는 `drain gate -> restart gate -> CSV gate -> real resume`로 고정한다.
- sample canary 성공만으로는 부족하고 CSV schema gate까지 green이어야 한다.
- sample canary는 continuous user service와 분리된 one-shot validation run으로 수행한다.
- `dispatch.mode`가 들어가기 전 tonight 실제 실행 순서는 `service status -> service stop -> canary -> service start` fallback을 사용한다.
- 각 창의 `Go/No-Go`는 아래 우선순위를 따른다.
  1. CSV gate
  2. restart/rediscovery gate
  3. throughput 또는 resource gate

## 운영 절차

공통 절차는 아래와 같다.

1. `dispatch.mode=drain`
2. service stop/restart
3. `continuous_mode=False` validation lane으로 `10분 sample canary`
4. CSV schema gate 확인
5. restart rediscovery 확인
6. 조건 충족 시 real `.aedt` 복귀
7. 실패 시 hold 또는 fallback

현재 구현 기준 fallback 절차는 아래와 같다.

1. `systemctl --user status peetsfea-runner --no-pager -n 20`
2. `systemctl --user stop peetsfea-runner`
3. DB 유지/삭제 규칙 적용
4. one-shot validation lane 수행
5. validation lane output에서 CSV schema gate 확인
6. `systemctl --user start peetsfea-runner`
7. `systemctl --user status peetsfea-runner --no-pager -n 20`
8. rediscovery 확인 후 real 지속 여부 판단

reference validation lane은 아래처럼 고정한다.

```python
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline

repo = Path("/home/peetsmain/peetsfea-runner")
window = "00"
root = repo / "tmp" / "tonight-canary" / window
input_dir = root / "input"
output_dir = root / "output"
delete_failed_dir = root / "delete_failed"
db_path = root / "state.duckdb"
input_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)
delete_failed_dir.mkdir(parents=True, exist_ok=True)

sample_src = repo / "examples" / "sample.aedt"
sample_dst = input_dir / "sample.aedt"
sample_dst.write_bytes(sample_src.read_bytes())
(input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")

config = PipelineConfig(
    input_queue_dir=str(input_dir),
    output_root_dir=str(output_dir),
    delete_failed_quarantine_dir=str(delete_failed_dir),
    metadata_db_path=str(db_path),
    accounts_registry=(
        AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
        AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
        AccountConfig(account_id="account_04", host_alias="gate1-hmlee31", max_jobs=10),
        AccountConfig(account_id="account_05", host_alias="gate1-dw16", max_jobs=10),
    ),
    execute_remote=True,
    remote_execution_backend="slurm_batch",
    partition="cpu2",
    cpus_per_job=20,
    mem="960G",
    time_limit="05:00:00",
    remote_root="~/aedt_runs",
    control_plane_host="127.0.0.1",
    control_plane_port=8765,
    control_plane_ssh_target="peetsmain@172.16.166.83",
    control_plane_return_host="172.16.166.83",
    control_plane_return_port=5722,
    control_plane_return_user="peetsmain",
    remote_ssh_port=22,
    tunnel_heartbeat_timeout_seconds=90,
    tunnel_recovery_grace_seconds=30,
    delete_input_after_upload=False,
    continuous_mode=False,
    ingest_poll_seconds=120,
    ready_sidecar_suffix=".ready",
    slots_per_job=5,
    worker_bundle_multiplier=1,
    cores_per_slot=4,
    worker_requeue_limit=1,
    run_rotation_hours=24,
    pending_buffer_per_account=3,
    capacity_scope="all_user_jobs",
    balance_metric="license_max_520",
    input_source_policy="sample_only",
    public_storage_mode="disabled",
)
result = run_pipeline(config)
print(result.summary)
print(result.success, result.exit_code)
```

shell invocation은 아래 형태로 고정한다.

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline

repo = Path("/home/peetsmain/peetsfea-runner")
window = "00"
root = repo / "tmp" / "tonight-canary" / window
input_dir = root / "input"
output_dir = root / "output"
delete_failed_dir = root / "delete_failed"
db_path = root / "state.duckdb"
input_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)
delete_failed_dir.mkdir(parents=True, exist_ok=True)
(input_dir / "sample.aedt").write_bytes((repo / "examples" / "sample.aedt").read_bytes())
(input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")

config = PipelineConfig(
    input_queue_dir=str(input_dir),
    output_root_dir=str(output_dir),
    delete_failed_quarantine_dir=str(delete_failed_dir),
    metadata_db_path=str(db_path),
    accounts_registry=(
        AccountConfig(account_id="account_02", host_alias="gate1-dhj02", max_jobs=10),
        AccountConfig(account_id="account_03", host_alias="gate1-jji0930", max_jobs=10),
        AccountConfig(account_id="account_04", host_alias="gate1-hmlee31", max_jobs=10),
        AccountConfig(account_id="account_05", host_alias="gate1-dw16", max_jobs=10),
    ),
    execute_remote=True,
    remote_execution_backend="slurm_batch",
    partition="cpu2",
    cpus_per_job=20,
    mem="960G",
    time_limit="05:00:00",
    remote_root="~/aedt_runs",
    control_plane_host="127.0.0.1",
    control_plane_port=8765,
    control_plane_ssh_target="peetsmain@172.16.166.83",
    control_plane_return_host="172.16.166.83",
    control_plane_return_port=5722,
    control_plane_return_user="peetsmain",
    remote_ssh_port=22,
    tunnel_heartbeat_timeout_seconds=90,
    tunnel_recovery_grace_seconds=30,
    delete_input_after_upload=False,
    continuous_mode=False,
    ingest_poll_seconds=120,
    ready_sidecar_suffix=".ready",
    slots_per_job=5,
    worker_bundle_multiplier=1,
    cores_per_slot=4,
    worker_requeue_limit=1,
    run_rotation_hours=24,
    pending_buffer_per_account=3,
    capacity_scope="all_user_jobs",
    balance_metric="license_max_520",
    input_source_policy="sample_only",
    public_storage_mode="disabled",
)
result = run_pipeline(config)
print(result.summary)
print(result.success, result.exit_code)
PY
```

운영 규칙은 아래와 같이 둔다.

- `window`는 `00`, `02`, `04`, `06` 중 현재 창에 맞춘다.
- `worker_bundle_multiplier`는 창별 target 값을 따른다.
  - `00`: `1`
  - `02` 이후 CSV gate green이면 target 값 반영
- validation lane 산출물은 live continuous service output과 섞지 않는다.
- validation lane DB는 live service DB와 섞지 않는다.

실관측 신호는 아래와 같이 둔다.

| 구간 | Green 신호 | Red 신호 |
| --- | --- | --- |
| sample canary | `CUTOVER_READY`, `SLURM_TRUTH_REFRESHED`, `CANARY_PASSED`, `FULL_ROLLOUT_READY` | `CANARY_FAILED`, `ROLLBACK_TRIGGERED` |
| sample canary 보조 신호 | `CONTROL_TUNNEL_DEGRADED`는 경고지만 단독으로 fail 아님 | `reason=output_variables_csv_schema_invalid`, `reason=output_variables_csv_missing`, `reason=materialized_output_missing`, `reason=submit_missing` |
| continuous service 재기동 후 | `WORKER_LOOP_START`, `WORKER_LOOP_ACTIVE`, `WORKER_LOOP_OK` 또는 `WORKER_LOOP_IDLE` | `WORKER_LOOP_ERROR`, `WORKER_LOOP_BLOCKED`, 지속 `WORKER_LOOP_RECOVERING` |

확인 순서는 아래처럼 고정한다.

1. validation lane 표준출력의 `CANARY_PASSED` 또는 `CANARY_FAILED`
2. validation lane output의 `output_variables.csv`
3. user service 재기동 후 `systemctl --user status peetsfea-runner --no-pager -n 20`
4. 필요 시 worker event에서 `WORKER_LOOP_OK`, `WORKER_LOOP_BLOCKED`, `WORKER_LOOP_ERROR`

창별 규칙은 아래로 고정한다.

- `00:00`
  - CSV integrity와 restart-safe 복구가 둘 다 green이어야 한다.
  - 하나라도 red면 real `.aedt`로 가지 않는다.
- `02:00`
  - `00:00` gate가 green일 때만 multiplier 변경을 적용한다.
  - red면 throughput 작업을 미루고 CSV/restart 문제를 반복 수정한다.
- `04:00`
  - resource/bad-node 정책은 CSV gate와 restart gate가 계속 green인 상태에서만 적용한다.
- `06:00`
  - final hardening 전에 `00:00`과 `02:00`에서 잡은 gate가 아직 green인지 재확인한다.

창별 체크리스트는 아래처럼 고정한다.

| 창 | stop 전 확인 | canary 확인 | start 후 확인 |
| --- | --- | --- | --- |
| `00:00` | `systemctl --user status`, 코드 반영 여부, DB 유지 판단 | `CANARY_PASSED`, sentinel CSV, `reason=ok` | `WORKER_LOOP_START`, `WORKER_LOOP_OK/IDLE`, rediscovery green |
| `02:00` | `00:00` gate green 이력, multiplier target 재확인 | `CANARY_PASSED`, CSV green, multiplier target 반영 | `WORKER_LOOP_OK`, `failed_slots` 급증 없음 |
| `04:00` | bad-node 후보, `/tmp` 기준, exclude 대상 정리 | CSV green 유지, resource 변경 후 canary green | `WORKER_LOOP_OK`, node 오류 감소 여부 |
| `06:00` | 누적 변경 재확인, final target 고정 | CSV regression 없음, canary green | `WORKER_LOOP_OK/IDLE`, `WORKER_LOOP_ERROR` 부재 |

창별 최종 실행 요약은 아래처럼 고정한다.

| 창 | 코드 범위 | DB | multiplier | 즉시 no-go | go 후 지속 조건 |
| --- | --- | --- | --- | --- | --- |
| `00:00` | CSV hotfix, canary gate | 유지 | `1` | `CANARY_FAILED`, CSV schema red, rediscovery red | `CANARY_PASSED`, `FULL_ROLLOUT_READY`, `WORKER_LOOP_OK/IDLE` |
| `02:00` | throughput knob | 유지 | `4`, fallback `2` | CSV red 재발, restart red, `failed_slots` 급증 | CSV green 유지, idle slot 감소 또는 throughput 상승 |
| `04:00` | bad-node policy | 유지 | 직전 green 값 유지 | canary red, restart red, node 오류 악화 | CSV/restart green 유지, node 오류 완화 |
| `06:00` | final hardening | 유지 | 안정값 고정 | CSV regression 재발, `WORKER_LOOP_ERROR` | `WORKER_LOOP_OK/IDLE`, 장시간 관측 green |

Go/No-Go 표는 아래와 같이 고정한다.

| 조건 | 판단 | 조치 |
| --- | --- | --- |
| sample canary pass + CSV schema valid + rediscovery 정상 | Go | real `.aedt` 복귀 |
| CSV missing 또는 schema invalid | No-Go | real 차단, `04` workstream 우선 |
| rediscovery 실패 | No-Go | real 차단, `01` workstream 우선 |
| bad-node signal만 존재, CSV/restart green | Conditional Go | `03` workstream 적용 후 재확인 |
| throughput 정체만 존재, CSV/restart green | Conditional Go | `02` workstream 적용 |

## 테스트/수용 기준

- canary success -> real 복귀 경로가 문서화되어 있다.
- canary fail -> real 차단 경로가 문서화되어 있다.
- `00:00`, `02:00`, `04:00`, `06:00` 창별 우선순위가 다르지 않다.
- CSV gate와 restart gate가 throughput보다 앞선다.

수용 기준은 다음과 같다.

- 운영자가 각 창에서 next-window로 넘길지 즉시 판단할 수 있다.
- sample lane과 real lane이 혼동되지 않는다.

## 참고 문서

- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md](./roadmap-tonight-04b-canary-csv-schema-gate-and-regression-watch.md)
- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
