# Worker Pool Spec

## Goal

The control plane maintains a fixed logical worker pool for the prune lane.

## Pool Size

- Logical worker count: `50`
- Accounts: `5`
- Target distribution: `10` workers per account

## Worker Identity

Workers have stable logical IDs:

- `worker_01`
- ...
- `worker_50`

The logical ID is stable across replacement attempts.

## Slurm Submit Contract

Each worker is submitted with:

- partition: `cpu2`
- nodes: `1`
- ntasks: `1`
- cpus per job: `48`
- mem: `288G`
- time limit: `05:00:00`

## Reconciliation Rules

The control plane continuously reconciles the logical pool.

- If a worker is missing, submit a replacement.
- If a worker is terminal, submit a replacement for the same logical ID.
- If the service starts or restarts, existing runner-owned workers are cancelled before new ones are submitted.

## No Balancing Layer

There is no dynamic scoring or throughput balancing in v1.

- No DB-based account score
- No license-based target distribution
- No bad-node exclusion logic

Only fixed per-account worker ownership is used.
