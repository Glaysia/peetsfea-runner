# Worker Pool Spec

## Goal

The control plane maintains a fixed logical worker pool for the prune lane.

Each logical worker is one Slurm job and one persistent enroot container. Slots
are logical AEDT executions inside that container, not separate containers.

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
- cpus per job: `40`
- mem: `960G`
- time limit: `05:00:00`

## Container Contract

Each submitted worker job starts exactly one enroot container for the whole job
lifetime.

- container image: `~/runtime/enroot/aedt.sqsh`
- required host device: `/dev/fuse`
- required SSH identity: mounted into the container at startup
- required packages in the image: `openssh-client`, `sshfs`, `fuse3`,
  `ca-certificates`
- primary workspace mount: sshfs to
  `peets@172.16.165.146:/home/peets/mnt/8tb/peetsfea-runner`

The container mounts sshfs once and uses that mount for input, output, and work
storage. The worker must not create one container per slot.

The generated worker payload must export the workspace mount contract before
starting the enroot container:

- `PEETS_WORKSPACE_REMOTE`
- `PEETS_WORKSPACE_MOUNT_ROOT`

After `enroot create`, the payload runs a short preparation start that creates
these container-side directories:

- `/etc/peetsfea_ssh`
- `/workspace`
- `/work/slots`

The SSH identity directory is mounted into the long-lived container as
`/etc/peetsfea_ssh` without a `:ro` suffix. The observed compute-node enroot
runtime rejects the previous `/root/.ssh:ro` mount shape with mount errors, while
the writable directory mount succeeds. sshfs uses
`IdentityFile=/etc/peetsfea_ssh/id_control` plus root-friendly ownership options
such as `idmap=user,uid=0,gid=0,umask=000`.

## Slot Contract

Each worker container maintains:

- target slots per job: `10`
- maximum slot concurrency: `10`
- shared Python/AEDT environment across all slots
- separate case directories per slot

When any slot reaches a terminal state, the worker immediately requests another
lease and reuses the freed slot until no input is available or the job exits.

The HFSS license limit is enforced at lease-request time. Workers remain alive
when the license gate is closed; they simply receive no new lease and continue
their normal idle/backoff polling. The gate must not submit or cancel Slurm jobs
and must not kill already running AEDT slots.

## Validation Contract

Prune validation lanes that exercise the enroot/sshfs worker path must use the
same worker shape as production:

- slots per job: `10`
- minimum slot target: `1`
- maximum slot concurrency: `10`
- cpus per job: `40`
- mem: `960G`

Validation may restrict the worker pool size or account set, but it must not
fall back to the old 30-48 slot prune shape.

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

The HFSS license slot gate is not a balancing layer. It is a global safety gate
for starting new AEDT slots and does not change account ownership or worker
distribution.
