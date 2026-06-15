"""잡 작업 디렉토리 라이프사이클 + 금지경로 가드 (GOAL §4).

- 잡 시작 시 `/enroot/{USER}_{SLURM_JOB_ID}` 생성, 종료 시 삭제.
- `/dev/shm`, `/tmp` 는 사용 금지 → 잡 전용 `job_tmpfs` / `job_disk` 만 쓴다.

AEDT 없이 동작하는 순수 파일시스템 모듈이라 단위테스트로 검증한다. enroot 루트는 설정 가능
(기본 `/enroot`)이라 테스트는 tmp 디렉토리를 루트로 쓴다.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

# 직접 사용 금지(접두) 경로. 잡은 job_tmpfs/job_disk만 쓴다.
FORBIDDEN_PATH_ROOTS: tuple[str, ...] = ("/dev/shm", "/tmp")


class ForbiddenPathError(RuntimeError):
    """`/dev/shm` 또는 `/tmp` 아래 경로를 쓰려 할 때."""


def is_forbidden_path(path: os.PathLike[str] | str) -> bool:
    resolved = Path(path).expanduser().resolve()
    for root in FORBIDDEN_PATH_ROOTS:
        root_path = Path(root)
        if resolved == root_path or root_path in resolved.parents:
            return True
    return False


def ensure_allowed_path(path: os.PathLike[str] | str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if is_forbidden_path(resolved):
        raise ForbiddenPathError(f"forbidden path (use job_tmpfs/job_disk, not /dev/shm·/tmp): {resolved}")
    return resolved


@dataclass(slots=True)
class JobWorkspaceConfig:
    enroot_root: Path = field(default_factory=lambda: Path("/enroot"))
    user: str = field(default_factory=lambda: os.environ.get("USER", "unknown"))
    job_id: str = field(default_factory=lambda: os.environ.get("SLURM_JOB_ID", "local"))


@dataclass(frozen=True, slots=True)
class JobPaths:
    """잡 작업 디렉토리 집합. ansysedt/시뮬은 여기만 쓴다(`/dev/shm`·`/tmp` 금지)."""

    root: Path
    job_tmpfs: Path
    job_disk: Path


def job_workspace_dir(config: JobWorkspaceConfig) -> Path:
    return Path(config.enroot_root) / f"{config.user}_{config.job_id}"


@contextmanager
def job_workspace(config: JobWorkspaceConfig | None = None) -> Iterator[JobPaths]:
    """`/enroot/{USER}_{SJOB}` 를 만들고(job_tmpfs/job_disk 포함) 종료 시 통째로 삭제."""

    cfg = config if config is not None else JobWorkspaceConfig()
    root = job_workspace_dir(cfg)
    job_tmpfs = root / "job_tmpfs"
    job_disk = root / "job_disk"
    root.mkdir(parents=True, exist_ok=True)
    job_tmpfs.mkdir(parents=True, exist_ok=True)
    job_disk.mkdir(parents=True, exist_ok=True)
    try:
        yield JobPaths(root=root, job_tmpfs=job_tmpfs, job_disk=job_disk)
    finally:
        shutil.rmtree(root, ignore_errors=True)


__all__ = [
    "FORBIDDEN_PATH_ROOTS",
    "ForbiddenPathError",
    "JobPaths",
    "JobWorkspaceConfig",
    "ensure_allowed_path",
    "is_forbidden_path",
    "job_workspace",
    "job_workspace_dir",
]
