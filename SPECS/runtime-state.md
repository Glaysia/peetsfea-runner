# Runtime State Spec

## Goal

Replace DB tables with a single process-local runtime state object.

## Required Fields

`RuntimeState` must contain at least:

- `run_id`
- `known_inputs`
- `queued_inputs`
- `leased_by_token`
- `leased_by_input`
- `workers_by_id`
- `slots_by_worker`
- `recent_events`
- `counters`

## Known Inputs

Tracks the current discovered input snapshot.

Each record stores:

- input path
- ready path
- file mtime ns
- ready mtime ns
- queued flag
- finalized flag

## Leases

Each lease stores:

- lease token
- worker id
- job id
- slurm job id
- input path
- output path
- input relative path from the repository workspace root
- output relative path from the repository workspace root
- storage mode
- started at
- expires at
- output materialized flag
- current state

## Workers

Each worker record stores:

- logical worker id
- account id
- slurm job id
- current worker state
- last heartbeat
- container state
- sshfs mount state
- current active slot count
- current target slot count

## Slots

Each logical slot stores:

- slot id
- worker id
- optional ansys pid
- optional grpc port
- session state
- current leased input
- cases processed in session
- last restart reason

Slots do not imply separate containers. One worker record corresponds to one
Slurm job and one enroot container; slots are concurrent executions inside that
container.

## Event Buffer

`recent_events` is a bounded ring buffer for debug only.

- It is not durable.
- It is not used for recovery.
- It is safe to lose on restart.
