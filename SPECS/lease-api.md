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

Response:

- `ok`
- `lease_token`
- `slot_id`
- `input_name`

If no input is available, returns `ok=true` with `lease_token=null`.

### `GET /internal/leases/input`

Query parameters:

- `run_id`
- `lease_token`

Behavior:

- Returns the raw `.aedt` payload for the active lease.
- Rejects unknown or stale tokens.

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

### `POST /internal/leases/complete`

Request body:

- `run_id`
- `lease_token`
- terminal metadata

Behavior:

- Marks the lease terminal
- On success, renames input to `.done`

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
