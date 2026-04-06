# Input Ingest Spec

## Scope

This spec defines how the control plane discovers and queues `.aedt` inputs.

## Input Root

- Inputs are only read from `input_queue/<lane>`.
- No alternate root is allowed.

## Scan Rules

- Scanning is recursive.
- Symlinked directories are followed.
- A visited-directory guard prevents infinite loops.
- Only files ending in `.aedt` are candidates.
- Files ending in `.aedt.done` are never candidates.
- Files with a sibling `.lock` are skipped for the current poll.

## Ready Rules

- A ready sidecar path is `input_path + ".ready"`.
- If `.ready` does not exist, the service creates it.
- Failure to create `.ready` does not crash the poll loop. The file is skipped and retried later.

## Identity Rules

Each input identity is:

- absolute input path
- file mtime ns
- ready mtime ns

This identity is used only for in-memory dedupe.

## Poll Behavior

Each ingest poll does the following:

1. Build a full recursive snapshot of candidate `.aedt` files
2. Drop any candidate that is already completed as `.done`
3. Drop any candidate already leased or queued in the current in-memory state
4. Enqueue the remaining candidates

The implementation must not do per-file durable writes during ingest.

## Race Handling

- If a file disappears between scan and stat/open, that file is skipped.
- The poll loop must continue.
- Missing-file races are normal and must not terminate the service.

## Success Boundary

An input leaves the active queue only when:

- artifact materialization succeeds, and
- the input is renamed to `.done`

If `.done` rename fails, the runtime logs the error and treats the input as not finalized.
