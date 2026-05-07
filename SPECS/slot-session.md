# Slot Session Spec

## Goal

Each worker maintains long-lived logical slots inside one persistent worker
container so that AEDT work stays warm and the job keeps useful parallelism for
its whole lifetime.

## Worker Container Model

One Slurm job owns:

- one persistent enroot runtime context
- one sshfs mount to the local 8TB workspace
- one shared Python/AEDT environment
- up to 10 logical AEDT slots

The enroot container is not restarted per slot or per case during normal
operation.

## Slot Model

Each logical slot owns:

- one slot id
- one current lease, or idle state
- one distinct host-side case directory under the job workdir
- one matching container-side case directory under `/work/slots/...`
- optional warm AEDT/grpc state for that slot
- repeated project open / solve / export cycles

Slots share the same container, Python environment, installed packages, sshfs
mount, and AEDT installation. Isolation is by case directory, not by container
or virtualenv.

## Slot Targets

Per worker:

- minimum slot target: `1`
- maximum slot target: `10`

These are target concurrency levels, not guarantees.

## Memory Gate

Slot expansion uses only node memory pressure.

- Source: `/proc/meminfo`
- Metric: `MemAvailable`
- High watermark: `90%`
- Resume watermark: `80%`
- Probe interval: `5s`

Behavior:

- If pressure is below the high watermark, new slots may start up to the max.
- If pressure exceeds the high watermark, no new slots start.
- Existing slots are never killed by the gate.

## Solve Path

Each leased input is processed as:

1. Acquire lease
2. Resolve input and output paths inside the sshfs mount
3. Create a unique case working directory for the slot and run it through the
   container-side `/work/slots/...` path
4. Open project in the slot's AEDT context
5. Bind HFSS in the current context
6. Solve with `solve_in_batch=False`
7. Export and materialize artifacts directly to `output/.../*.aedt.out`
8. Complete or fail the lease
9. Immediately request the next lease for the freed slot

## Session Recycle Rules

A slot's AEDT state is restarted only when:

- gRPC disconnects
- project open or HFSS bind fails
- analyze fails and leaves the session unusable
- explicit recycle policy is hit

Successful cases do not trigger a slot recycle. Slot recycle must not restart
the worker container or remount sshfs unless the container-wide mount or runtime
is unhealthy.
