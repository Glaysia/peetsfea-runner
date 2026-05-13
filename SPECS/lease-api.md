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
- Before allocating a lease, applies the Electronics Desktop license slot gate.
  The gate is a slot-start gate, not a Slurm worker-submit gate.

Response:

- `ok`
- `lease_available`
- `lease_token`
- `slot_id`
- `input_name`
- `input_relpath`
- `output_relpath`
- `storage_mode`
- optional `license_gate`
- optional `license_feature`
- optional `license_in_use`
- optional `license_ceiling`

If no input is available, returns `ok=true` with `lease_token=null`.

If the Electronics Desktop license slot gate is closed, returns `ok=true` and
`lease_available=false` without mutating queued input state or assigning a lease
token. The response includes `license_gate="license_closed"`,
`license_feature="electronics_desktop"`, the most recent `license_in_use`, and
`license_ceiling=350`. Workers must treat this the same as a temporary no-input
response and retry through their normal idle/backoff loop.

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

## Electronics Desktop License Slot Gate

The control plane polls `gate1-harry261` for
`electronics_desktop` license usage by running the `anlic` alias target
directly:

```bash
ANSYSLMD_LICENSE_FILE=1055@172.16.10.81 /opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil lmstat -a
```

The gate uses the header value from:

```text
Users of electronics_desktop:  (Total of 550 licenses issued;  Total of N licenses in use)
```

Rules:

- `N >= 350`: close the slot gate; no new lease is issued.
- `N <= 349`: open the slot gate; normal lease allocation may continue.
- Poll/cache TTL: `10` seconds.
- Concurrent lease requests share one in-process refresh lock so a worker burst
  does not create a burst of SSH/lmutil calls.
- Poll failure, SSH failure, timeout, or missing `electronics_desktop` line is
  fail-open. The control plane logs the failure and proceeds with normal lease
  allocation.
- Already leased/running slots are never killed by this gate.

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
