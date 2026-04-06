# Slot Session Spec

## Goal

Each worker maintains long-lived slot sessions so that `ansysedt` stays warm across multiple leased inputs.

## Session Model

Each slot session owns:

- one persistent enroot runtime context
- one persistent `ansysedt -grpcsrv` process
- repeated project open / solve / export cycles

The `ansysedt` process is not restarted per case during normal operation.

## Slot Targets

Per worker:

- minimum slot target: `30`
- maximum slot target: `48`

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
2. Download input
3. Open project in the live session
4. Bind HFSS in the current session
5. Solve with `solve_in_batch=False`
6. Export using the same live session
7. Upload artifact
8. Complete or fail the lease
9. Request the next lease

## Session Recycle Rules

A slot session is restarted only when:

- gRPC disconnects
- project open or HFSS bind fails
- analyze fails and leaves the session unusable
- explicit recycle policy is hit

Successful cases do not trigger a recycle.
