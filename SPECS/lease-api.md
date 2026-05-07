# Lease API Spec

## Scope

This spec defines the internal control-plane API used by workers.

All lease endpoints are loopback-only through the reverse SSH tunnel path already used by workers.

## Endpoints

### `POST /internal/leases/request`

Request body:

- `run_id`
- `worker_id`
- `job_id`
- `account_id`
- `slurm_job_id`

Behavior:

- Returns the next available input if one exists.
- Allocates exactly one lease token per input.
- Never double-leases the same input.
- In sshfs worker mode, returns path metadata instead of requiring file
  transfer through the control plane.

Response:

- `ok`
- `lease_available`
- `lease_token`
- `slot_id`
- `input_name`
- `input_relpath`
- `output_relpath`
- `storage_mode`

If no input is available, returns `ok=true` with `lease_token=null`.

For `storage_mode="sshfs_direct"`, the worker joins `input_relpath` and
`output_relpath` against its configured sshfs mount root. Both relpaths are
relative to the repository workspace root. They include their top-level durable
truth prefix, for example `input_queue/prune_results/sample.aedt` and
`output/prune_results/sample.aedt.out`. They are not relative to the configured
input lane root or output lane root. The control plane does not send the `.aedt`
payload for the normal solve path.

The default prune-lane worker treats non-`sshfs_direct` leases as incompatible
with the single-container sshfs path and must fail them explicitly rather than
silently falling back to tar/scp data movement.

### `GET /internal/leases/input`

Query parameters:

- `run_id`
- `lease_token`

Behavior:

- Returns the raw `.aedt` payload for the active lease.
- Rejects unknown or stale tokens.
- Compatibility endpoint for non-sshfs workers and diagnostics.
- Not used by the default `single_container_sshfs` prune-lane worker model.

### `POST /internal/leases/heartbeat`

Request body:

- `run_id`
- `lease_token`
- `worker_id`
- optional session counters

Behavior:

- Refreshes the in-memory lease expiry.

### `POST /internal/leases/artifact`

Request:

- query: `run_id`, `lease_token`
- body: artifact tarball bytes

Behavior:

- Materializes the artifact into `output/.../*.aedt.out`
- Marks the lease as uploaded
- Does not finalize `.done` by itself unless exit status is terminal and successful
- Compatibility endpoint for non-sshfs workers and diagnostics.
- Not used by the default `single_container_sshfs` prune-lane worker model.

### `POST /internal/leases/complete`

Request body:

- `run_id`
- `lease_token`
- terminal metadata
- `output_materialized`
- optional `output_relpath`

Behavior:

- Marks the lease terminal
- On success, validates the expected output directory exists
- On success, renames input to `.done`

For `storage_mode="sshfs_direct"`, completion is accepted only after the worker
has written the output directory under the mounted workspace and reports
`output_materialized=true`.

### `POST /internal/leases/fail`

Request body:

- `run_id`
- `lease_token`
- failure reason

Behavior:

- Marks the lease failed in memory
- Leaves input unfinalized for retry or replay

## Token Rules

- Lease tokens are process-local.
- Tokens are invalid after service restart.
- A stale token returns a terminal error response and must not mutate current state.

## Minimal Public API

The only non-lease endpoint kept in v1 is:

- `GET /health`

All current DB-backed human-facing `/api/*` endpoints are removed.
