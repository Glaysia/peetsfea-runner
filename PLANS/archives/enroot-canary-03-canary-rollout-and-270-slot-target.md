# Enroot Canary 03: Canary Rollout and 270-Slot Target

## 목적

이 문서는 canary-first rollout 원칙과 7계정 기준 `270 slots` 목표를 한 문서에서 연결한다.
핵심은 “계정당 `max_jobs=10`을 가능한 한 꽉 채우는 운영”과 “enroot는 먼저 validation lane canary에서만 쓴다”를 동시에 고정하는 것이다.

## canary-first 원칙

이번 전환의 첫 단계는 아래로 고정한다.

- `continuous_mode=False`
- validation lane one-shot run only
- sample input only
- live continuous service는 기존 host mode 유지
- canary green이 쌓이기 전에는 live 전체 전환을 하지 않음

## lane별 목표 용량

lane별 목표는 아래처럼 고정한다.

| lane | 계정 수 | jobs/account | slots/job | total slots |
| --- | --- | --- | --- | --- |
| `prune` | `5` | `10` | `5` | `250` |
| `preserve` | `2` | `10` | `1` | `20` |
| total | `7` | - | - | `270` |

계산식은 아래로 고정한다.

```text
5 * 10 * 5 + 2 * 10 * 1 = 270
```

계정별 lane 배치는 아래와 같다.

- prune
  - `gate1-harry`
  - `gate1-dhj02`
  - `gate1-jji0930`
  - `gate1-hmlee31`
  - `gate1-dw16`
- preserve
  - `gate1-r1jae262`
  - `gate1-wjddn5916`

## 운영 원칙

- `account.max_jobs=10`은 단순 상한이 아니라 목표 채움 기준으로 본다.
- enroot 전환 후 container concurrency는 `slot 1개 = container 1개`로 계산한다.
- 따라서 prune 계정은 계정당 `50 containers`, preserve 계정은 계정당 `10 containers`를 목표로 본다.

## cutover criteria

enroot canary cutover green 기준은 아래다.

- `CANARY_PASSED`
- CSV schema gate green
- `SLURM_TRUTH_REFRESHED`
- restart/rediscovery green

red 기준은 아래다.

- `CANARY_FAILED`
- `output_variables_csv_missing`
- `output_variables_csv_schema_invalid`
- `materialized_output_missing`
- ssh alias/config missing
- `enroot` missing
- image missing
- ansys root missing or mount fail

## 기존 정책 유지 범위

아래는 enroot 전환 후에도 기존 정책을 그대로 유지한다.

- `/tmp` 부족 판단
- `No space left on device`
- bad-node 등록과 cooldown
- tunnel degraded triage
- live DB 유지 정책

즉 enroot는 throughput과 환경 재현성 개선 도구이지, resource/bad-node 정책 대체물이 아니다.

## validation lane 예시

prune canary reference는 아래처럼 본다.

```python
from pathlib import Path

from peetsfea_runner import AccountConfig, PipelineConfig, run_pipeline

repo = Path("/home/peetsmain/peetsfea-runner")
window = "00"
root = repo / "tmp" / "tonight-canary" / window / "prune"
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
        AccountConfig(account_id="account_01", host_alias="gate1-harry", max_jobs=10),
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
    continuous_mode=False,
    slots_per_job=5,
    worker_bundle_multiplier=1,
    cores_per_slot=4,
    tasks_per_slot=1,
    input_source_policy="sample_only",
    public_storage_mode="disabled",
)
result = run_pipeline(config)
print(result.summary)
```

preserve canary는 아래 값만 바꿔 mirror 구성으로 본다.

- root path를 `.../preserve`
- accounts를 `gate1-r1jae262`, `gate1-wjddn5916`로 변경
- `slots_per_job=1`
- `cpus_per_job=32`
- `cores_per_slot=32`
- `tasks_per_slot=4`

## 창별 판단 포인트

| window | 핵심 확인 |
| --- | --- |
| `00:00` | ssh alias/config, image/root mount, CSV gate |
| `02:00` | canary green 유지, job/slot 채움률, throughput 증가 방향 |
| `04:00` | `/tmp`, bad-node, observed node 오류 추이 |
| `06:00` | restart green 유지, no-go 조건 부재, live 검토 여부 |

## 주의사항

- 270-slot 목표는 active live service가 즉시 달성해야 하는 값이 아니라, enroot 전환 설계상 최종 목표치다.
- canary 단계에서는 sample-only, separate DB/output, CSV gate를 우선한다.
- 이 문서는 설계 문서다. active `PLANS`를 대체하지 않는다.
