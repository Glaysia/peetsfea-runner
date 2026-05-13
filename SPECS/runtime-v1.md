# Runtime V1

## Goal

Replace the current DB-centered control plane with a DB-free runtime that uses:

- `input_queue` as pending-input truth
- `output/.../*.aedt.out` as artifact truth
- `.aedt.done` as completion truth
- in-memory state for queueing, leasing, and worker/session tracking
- one persistent enroot worker container per Slurm job
- one sshfs mount from the worker container to the local 8TB workspace

The pipeline entrypoint remains `run_pipeline(config)`.

## Core Features To Keep

The runtime keeps exactly these functional pillars:

1. Recursive input scan under `input_queue/<lane>`
2. Fixed-size Slurm worker pool management
3. Internal HTTP lease protocol between workers and the control plane
4. Single persistent enroot worker container per Slurm job
5. One sshfs mount per worker container to the local workspace
6. Up to 10 long-lived logical AEDT slots per worker
7. Output materialization plus `.done` finalization

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

In sshfs worker mode, remote workers see the same durable tree through the
container mount. The canonical storage remains this repository workspace on the
local PC, currently reached by workers as
`peets@172.16.165.146:/home/peets/mnt/8tb/peetsfea-runner`.

No DB file exists in v1.

## Runtime Model

The control plane owns:

- input discovery
- in-memory ready queue
- lease issuance
- worker pool reconciliation
- completion validation and `.done` finalization

Workers own:

- sshfs mount lifecycle
- logical slot lifecycle
- persistent `ansysedt` process reuse
- solve and export
- direct output materialization under the mounted workspace

The default prune-lane worker model is `single_container_sshfs`: each Slurm job
starts one enroot container, mounts the local workspace once through sshfs, and
keeps up to 10 AEDT executions in flight until input is exhausted or the job
terminates. When a slot finishes, the worker immediately requests another lease
and fills the empty slot without restarting the worker container.

The worker container receives the workspace contract as environment:

- `PEETS_WORKSPACE_REMOTE`
- `PEETS_WORKSPACE_MOUNT_ROOT`

The mounted root is the only normal input/output/work storage path seen by
`sshfs_direct` workers. Lease `input_relpath` and `output_relpath` values are
always relative to the repository workspace root, not relative to
`input_queue_dir` or `output_root_dir`. Therefore a worker resolves
`input_queue/prune_results/case.aedt` as
`$PEETS_WORKSPACE_MOUNT_ROOT/input_queue/prune_results/case.aedt` and writes
`output/prune_results/case.aedt.out` as
`$PEETS_WORKSPACE_MOUNT_ROOT/output/prune_results/case.aedt.out`.

The container bootstrap must create the in-container mount targets before the
long-lived worker starts. The SSH identity directory is mounted at
`/etc/peetsfea_ssh` without a read-only mount suffix because the observed enroot
runtime rejects the previous `/root/.ssh:ro` directory mount shape on compute
nodes. The worker uses `IdentityFile=/etc/peetsfea_ssh/id_control`.

## Restart Model

Restart is always a cold start.

- Existing runner-owned Slurm workers are cancelled with `scancel` before a new
  pool is started.
- Service stop, service restart, and `RuntimeMaxSec` expiry all run the same
  cancellation path before the process exits.
- Cancellation targets only runner-owned workers for the configured service
  accounts. Other Slurm jobs owned by those accounts are not part of the
  runtime contract.
- Old lease tokens become invalid immediately.
- `.done` inputs stay completed.
- Non-`.done` inputs are re-scanned and become eligible again.
- No attempt is made to reconstruct in-flight state from a prior process.

## Acceptance Criteria

The implementation is complete when all of the following are true:

1. Recursive symlink-heavy input trees are discovered without DB writes.
2. The control plane maintains the configured worker pool size.
3. Workers can mount the local workspace through sshfs inside one persistent
   enroot container per Slurm job.
4. Workers can keep up to 10 logical AEDT slots active and refill completed
   slots until input is exhausted.
5. Successful inputs materialize output directly under the mounted
   `output/<lane>/**/*.aedt.out` path and become `.done`.
6. A service restart can replay non-`.done` work without DB recovery.
