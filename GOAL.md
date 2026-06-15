# GOAL: Single Remote PyAEDT API Runner

Date: 2026-06-14

This document defines the new target architecture for the runner reset.
The current Slurm/bundle/tmp/batch-AEDT direction is no longer the goal.
Start from one correct remote simulation path, then expand only after that path
is stable and observable.

## Core Goal

Run exactly one simulation remotely through one controlled Python API process:

- one supercomputer account: `account_01` on `gate1-harry261`
- one Slurm job
- one Enroot container
- one PyAEDT slot
- one remote API server
- one submitted TOML job at a time
- one terminal simulation result written back to the local DuckDB

The first milestone is not throughput. The first milestone is correctness,
repeatability, and a clean control path for a single simulation.

## Retired Direction

The following existing runner direction should be treated as legacy and should
not be extended:

- multiple accounts as the primary operating assumption
- one job containing multiple PyAEDT slots
- worker bundle multiplier and prefetch tuning
- worker-owned refill loops
- AEDT batch execution from pre-existing `.aedt` input files
- remote active queues based on copying `.aedt` files to Slurm workdirs
- host `/tmp` as the runner scratch/runtime root
- Slurm throughput optimization before the single API path is correct

Old code may remain temporarily while the replacement path is built, but new
work should not deepen dependency on the retired model.

## What Stays

The reset still keeps these pieces:

- Slurm as the remote resource allocator
- Enroot as the container runtime
- the existing bidirectional SSH/control-plane idea
- sshfs-backed local workspace/output storage, because the remote account
  filesystem is capacity constrained
- a pinned container image with Python, PyAEDT, and AEDT runtime support
- local DuckDB as the durable local result store
- `peetsfea` `0.3.1` as the geometry/input contract dependency

## peetsfea 0.3.1 API Binding

The new runner must use the `peetsfea` 0.3.1 Python API instead of rebuilding
the geometry, AEDT setup, solve, and CSV export sequence in runner code.

The first remote simulation primitive is:

```python
from peetsfea.ssw_random_sample_reports import (
    run_ssw_random_sample_reports_from_toml_text,
)
```

The remote API server should call:

```python
run_ssw_random_sample_reports_from_toml_text(
    candidate_toml_text,
    output_dir=job_output_dir,
    seed=seed,
    mode=mode,
)
```

This API is the first single-simulation boundary. It already performs the
important domain work:

- accepts TOML text and writes it as `input.toml`
- checks and samples one point from the SSW design space
- builds `design_id`, `point_hash`, and `point_values`
- exports SSW STEP/AEDT port artifacts
- runs the PyAEDT/HFSS solve through `solve_ssw_aedt_ports`
- exports the standard SSW CSV reports
- returns `csv_text_by_report`, `csv_paths`, `solve_telemetry`, and metadata

The runner should not duplicate those steps. It should provide the remote
process, single-flight API, storage policy, SSH path, Slurm/container launch,
and local DuckDB persistence around this `peetsfea` API.

Implementation reference while developing locally:

`/home/peets/Projects/PythonProjects/peetsfea`

The first remote account target is fixed to `gate1-harry261`. New code and
verification for this reset should not fan out to the older multi-account lane.

## New Execution Model

1. The local runner starts or reuses one remote Slurm job.
2. That Slurm job starts one Enroot container.
3. Inside the container, one Python process runs the single-flight API server.
4. The Python process exposes a small API server over the existing SSH path.
5. The local runner sends one TOML payload to the API.
6. The TOML payload is a subset of `peetsfea` tag `0.3.1`:
   `examples/0.3.0_sweep.toml`.
7. The Slurm container mounts a local workspace/output directory through
   sshfs and exposes it inside the container, initially as `/workspace`.
8. The remote API calls `run_ssw_random_sample_reports_from_toml_text(...)`
   with an sshfs-backed `output_dir`, initially `/workspace/output/<request>`.
9. The `peetsfea` API validates/samples the TOML, builds AEDT artifacts, runs
   exactly one PyAEDT/HFSS solve, exports CSV reports, and returns a terminal
   result dictionary.
10. The local runner writes the result metadata, point values, solve telemetry,
   and CSV report text into the local DuckDB.
11. The remote Python API process stays alive until terminal result handling
    and explicit cleanup are complete.

Phase 1 should not attempt to invent a custom long-lived HFSS object lifecycle.
Use the `peetsfea` 0.3.1 high-level API first. If later profiling shows that
AEDT startup dominates, the next boundary is the existing `hfss_factory`
injection point, not a runner-side rewrite of the PyAEDT workflow.

## API Contract

The first API can be intentionally small:

- `GET /health`
  - confirms the API process, PyAEDT session state, AEDT availability, and
    current busy/idle state
- `POST /simulate`
  - accepts one TOML payload compatible with a subset of
    `examples/0.3.0_sweep.toml`
  - rejects requests while busy
  - calls `run_ssw_random_sample_reports_from_toml_text(...)`
  - runs one simulation primitive to terminal success or failure
  - returns a structured result envelope containing the `peetsfea` result
- `POST /shutdown`
  - optional first-phase control endpoint for explicit teardown

The API must be single-flight at first. No queue, no multi-slot scheduling, and
no hidden parallelism.

## TOML Scope

The accepted TOML is not arbitrary project input. It is a controlled subset of:

`/home/peets/git/peetsfea.git@0.3.1:examples/0.3.0_sweep.toml`

Initial implementation should pass the TOML text to the `peetsfea` 0.3.1 API
and preserve its validation errors. Unsupported fields should fail clearly
rather than being silently ignored.

## Temporary Storage Policy

Do not use the supercomputer host `/tmp` as the runner runtime root.

Allowed:

- container-internal temporary directories
- RAM-backed container scratch, especially `/dev/shm`, for tmp and AEDT/PyAEDT
  scratch when available
- RAM-backed Enroot runtime/cache/data/temp paths, initially under
  `/dev/shm/peetsfea-single-api-<job>`
- small job-local directories under an explicit configured remote work root for
  launch metadata and copied control files
- sshfs-backed local workspace/output directories mounted inside the container

Not allowed:

- `/tmp/$USER/peetsfea-runner` as the default runtime root
- Enroot runtime/cache/data/temp paths under host `/tmp`
- unbounded solver scratch under host `/tmp`
- using the remote account filesystem as the durable artifact/output store

## Result Storage

The local DuckDB is the durable result sink for the new path.
Large run artifacts and exported CSV files should be written through sshfs to
the local workspace/output root, not accumulated on the remote account
filesystem. DuckDB stores the result envelope, telemetry, CSV text, and retained
artifact references.

At minimum, each simulation result should record:

- local request id
- remote job id
- remote API session id
- input TOML hash
- `peetsfea` version
- `mode` and `seed`
- `design_id`
- `point_hash`
- `dimension_count`
- `free_owner_paths`
- `point_values`
- terminal state: success, failed, cancelled, or infrastructure_error
- start/end timestamps
- AEDT/PyAEDT version metadata when available
- `setup_pass_counts`
- `solve_telemetry`
- `csv_text_by_report`
- `csv_paths`
- artifact references, if artifacts are retained
- error message and stage on failure

The remote side may keep temporary artifacts only as needed for execution and
debugging. The durable acceptance signal is the local DuckDB row plus any
explicitly retained artifact references.

## First Acceptance Criteria

The reset is working only when all of these are true:

- `account_01` on `gate1-harry261` launches one Slurm job
- the job starts one Enroot container without using host `/tmp` as runner root
- Enroot runtime/cache/data/temp are not placed on the remote account
  filesystem
- the container mounts the local output workspace through sshfs
- tmp and AEDT scratch prefer a RAM-backed container path such as `/dev/shm`
- one remote API server becomes reachable through the SSH path
- `GET /health` reports idle and can import `peetsfea` version `0.3.1`
- `POST /simulate` accepts one valid subset TOML
- `POST /simulate` calls
  `peetsfea.ssw_random_sample_reports.run_ssw_random_sample_reports_from_toml_text`
- exactly one PyAEDT simulation runs to a terminal state
- the remote Python API process remains controlled until terminal result
  handling
- the local runner writes the result to DuckDB
- a second request while busy is rejected cleanly
- failure paths return structured errors instead of orphaning silent work

## Immediate Work Order

1. Freeze the legacy Slurm bundle/tmp/batch path.
2. Treat `run_ssw_random_sample_reports_from_toml_text()` as the first
   simulation primitive.
3. Define the remote request/response envelope around that primitive.
4. Define the DuckDB result schema from the returned `peetsfea` result fields.
5. Build the remote single-flight API process.
6. Build the local client/control wrapper.
7. Wire Slurm to start exactly one container and one API process.
8. Run one end-to-end remote simulation through the API.
9. Only after that works, decide whether to add queueing, multiple accounts, or
   higher throughput.

## Non-Goals For Now

- throughput optimization
- multiple Slurm jobs
- multiple containers
- multiple PyAEDT slots
- worker bundle multiplier
- automatic refill
- input queue sweeping
- output folder based durable truth
- batch `.aedt` execution as the main path
- runner-side reimplementation of `peetsfea` geometry/port/report logic

The new system should earn complexity one feature at a time.
