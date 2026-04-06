# Runtime V1

## Goal

Replace the current DB-centered control plane with a DB-free runtime that uses:

- `input_queue` as pending-input truth
- `output/.../*.aedt.out` as artifact truth
- `.aedt.done` as completion truth
- in-memory state for queueing, leasing, and worker/session tracking

The pipeline entrypoint remains `run_pipeline(config)`.

## Core Features To Keep

The runtime keeps exactly these five functional pillars:

1. Recursive input scan under `input_queue/<lane>`
2. Fixed-size Slurm worker pool management
3. Internal HTTP lease protocol between workers and the control plane
4. Persistent `ansysedt` slot sessions inside workers
5. Output materialization plus `.done` finalization

Everything else is optional and removed in v1 unless explicitly listed below.

## Non-Goals

The following are removed from the runtime:

- DuckDB and `StateStore`
- DB-backed status pages and detailed `/api/*` endpoints
- throughput dashboards and slot scoring
- license snapshots and resource snapshots
- bad-node quarantine logic
- restart-safe state reconstruction
- rollout/canary state machines in the live control path

## Durable Truth

The only durable truth sources are files.

- Pending input: `input_queue/**/*.aedt`
- Ready marker: `input_queue/**/*.aedt.ready`
- Lock marker: `input_queue/**/*.lock`
- Completed input: `input_queue/**/*.aedt.done`
- Output artifact root: `output/<lane>/**/*.aedt.out`

No DB file exists in v1.

## Runtime Model

The control plane owns:

- input discovery
- in-memory ready queue
- lease issuance
- worker pool reconciliation
- output finalization

Workers own:

- slot session lifecycle
- persistent `ansysedt` process reuse
- solve and export
- artifact upload

## Restart Model

Restart is always a cold start.

- Existing runner-owned Slurm workers are cancelled before a new pool is started.
- Old lease tokens become invalid immediately.
- `.done` inputs stay completed.
- Non-`.done` inputs are re-scanned and become eligible again.
- No attempt is made to reconstruct in-flight state from a prior process.

## Acceptance Criteria

The implementation is complete when all of the following are true:

1. Recursive symlink-heavy input trees are discovered without DB writes.
2. The control plane maintains the configured worker pool size.
3. Workers can lease inputs and solve them through persistent `ansysedt` sessions.
4. Successful inputs materialize output and become `.done`.
5. A service restart can replay non-`.done` work without DB recovery.
