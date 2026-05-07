# Output And Finalization Spec

## Output Root

Artifacts are materialized under:

- `output/<lane>/<relative_input_name>.aedt.out`

The existing artifact directory structure is preserved.

## Artifact Upload Rules

The default prune-lane worker model is sshfs direct materialization.

- Workers write artifacts directly into the target `.aedt.out` directory through
  the sshfs-mounted local workspace.
- The mounted workspace is
  `peets@172.16.165.146:/home/peets/mnt/8tb/peetsfea-runner`.
- Lease output paths are repository-workspace-relative and include the
  top-level `output/` prefix.
- The control plane validates completion metadata and finalizes the input.
- Partial output writes are treated as non-final.

Tarball upload through `/internal/leases/artifact` remains a compatibility path
for non-sshfs workers and diagnostics, but it is not the normal prune-lane data
path.

## Success Rules

A slot is considered successful only when:

1. output materialization succeeds
2. expected output directory exists
3. the terminal exit code is `0`
4. input rename to `.done` succeeds

## Failure Rules

On failure:

- keep the `.aedt.out` directory
- write failure artifacts there
- do not rename input to `.done`
- leave the input eligible for retry or replay after restart

Failure artifacts should be written directly by the worker when sshfs is
available. If the sshfs mount is unavailable, the worker may report failure
through the lease API without claiming output materialization.

## Cleanup Rules

- `.ready` may be removed as part of successful finalization
- `.done` is the durable completion marker
- if `.done` rename fails, the runtime must log the error and keep the input active
