# DB Removal Plan

## Purpose

This document defines what is removed as part of the DB-free runtime refactor.

## Remove Entirely

- `peetsfea_runner/state_store.py`
- DuckDB dependency from runtime code
- DB path plumbing from `PipelineConfig`
- DB-based status endpoints
- DB-backed service startup and recovery

## Refactor Heavily

- `peetsfea_runner/pipeline.py`
- `peetsfea_runner/web_status.py`
- `peetsfea_runner/built_in_service.py`
- `peetsfea_runner/systemd_worker.py`
- `runner.py`

## Preserve

- `run_pipeline(config)` entrypoint
- current remote Slurm execution model
- internal lease protocol
- persistent `ansysedt` slot-session runtime in workers
- file-based output contract

## Test Deletions

Delete DB-specific tests, including:

- `tests/test_state_store.py`
- DB query assertions in pipeline/web/systemd tests

## Test Replacements

Replace them with:

- recursive input scan tests
- in-memory lease lifecycle tests
- artifact materialization tests
- cold-restart replay tests
- live service round-trip tests based on files, not DB rows
