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
- `slot_sessions_by_worker`
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
- started at
- expires at
- artifact uploaded flag
- current state

## Workers

Each worker record stores:

- logical worker id
- account id
- slurm job id
- current worker state
- last heartbeat
- current active slot count
- current target slot count

## Slot Sessions

Each slot session stores:

- session id
- worker id
- ansys pid
- grpc port
- session state
- current leased input
- cases processed in session
- last restart reason

## Event Buffer

`recent_events` is a bounded ring buffer for debug only.

- It is not durable.
- It is not used for recovery.
- It is safe to lose on restart.
