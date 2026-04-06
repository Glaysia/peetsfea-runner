# Output And Finalization Spec

## Output Root

Artifacts are materialized under:

- `output/<lane>/<relative_input_name>.aedt.out`

The existing artifact directory structure is preserved.

## Artifact Upload Rules

- Workers upload artifacts as tarballs.
- The control plane untars them into the target `.aedt.out` directory.
- Partial upload or extraction failure is treated as non-final.

## Success Rules

A slot is considered successful only when:

1. artifact upload succeeds
2. artifact materialization succeeds
3. the terminal exit code is `0`
4. input rename to `.done` succeeds

## Failure Rules

On failure:

- keep the `.aedt.out` directory
- write failure artifacts there
- do not rename input to `.done`
- leave the input eligible for retry or replay after restart

## Cleanup Rules

- `.ready` may be removed as part of successful finalization
- `.done` is the durable completion marker
- if `.done` rename fails, the runtime must log the error and keep the input active
