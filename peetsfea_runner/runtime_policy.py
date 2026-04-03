from __future__ import annotations

from typing import Final


DEFAULT_REMOTE_ROOT: Final[str] = "~/aedt_runs"
REMOTE_RUNTIME_DIRNAME: Final[str] = "_runtime"
REMOTE_SCRATCH_SOFT_LIMIT_MB: Final[int] = 80 * 1024
REMOTE_SCRATCH_HARD_LIMIT_MB: Final[int] = 90 * 1024
RUNTIME_PROBE_CACHE_TTL_SECONDS: Final[int] = 30 * 60
RUNTIME_JANITOR_MIN_TTL_SECONDS: Final[int] = 12 * 60 * 60
JOB_TMPFS_SIZE_GB: Final[int] = 100
JOB_DISK_FILESYSTEM_SIZE_GB: Final[int] = 90


def join_remote_root(remote_root: str, suffix: str) -> str:
    normalized = str(remote_root).rstrip("/")
    if normalized:
        return f"{normalized}/{suffix}"
    return str(suffix)


def remote_runtime_root(remote_root: str) -> str:
    return join_remote_root(remote_root, REMOTE_RUNTIME_DIRNAME)
