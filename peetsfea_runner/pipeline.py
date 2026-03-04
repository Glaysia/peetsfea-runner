from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final


EXIT_CODE_SUCCESS: Final[int] = 0
EXIT_CODE_SSH_FAILURE: Final[int] = 10
EXIT_CODE_SLURM_FAILURE: Final[int] = 11
EXIT_CODE_SCREEN_FAILURE: Final[int] = 12
EXIT_CODE_REMOTE_RUN_FAILURE: Final[int] = 13
EXIT_CODE_DOWNLOAD_FAILURE: Final[int] = 14


@dataclass(slots=True)
class PipelineConfig:
    input_aedt_path: str
    host: str = "gate1-harry"
    partition: str = "cpu2"
    nodes: int = 1
    ntasks: int = 1
    cpus: int = 32
    mem: str = "320G"
    time_limit: str = "05:00:00"
    retry_count: int = 1
    remote_root: str = "~/aedt_runs"
    local_artifacts_dir: str = "./artifacts"

    def validate(self) -> Path:
        candidate = Path(self.input_aedt_path).expanduser()
        if candidate.suffix.lower() != ".aedt":
            raise ValueError("input_aedt_path must end with .aedt")
        if not candidate.exists():
            raise FileNotFoundError(f"AEDT file not found: {candidate}")
        if not candidate.is_file():
            raise ValueError(f"AEDT path is not a file: {candidate}")

        _ensure_positive("nodes", self.nodes)
        _ensure_positive("ntasks", self.ntasks)
        _ensure_positive("cpus", self.cpus)
        if self.retry_count < 0:
            raise ValueError("retry_count must be >= 0")
        if not self.host.strip():
            raise ValueError("host must not be empty")
        if not self.partition.strip():
            raise ValueError("partition must not be empty")
        if not self.mem.strip():
            raise ValueError("mem must not be empty")
        if not self.time_limit.strip():
            raise ValueError("time_limit must not be empty")
        if not self.remote_root.strip():
            raise ValueError("remote_root must not be empty")
        if not self.local_artifacts_dir.strip():
            raise ValueError("local_artifacts_dir must not be empty")

        return candidate.resolve()


@dataclass(slots=True)
class PipelineResult:
    success: bool
    exit_code: int
    run_id: str
    remote_run_dir: str
    local_artifacts_dir: str
    summary: str


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    if not isinstance(config, PipelineConfig):
        raise TypeError("config must be a PipelineConfig")

    _ = config.validate()

    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    remote_run_dir = _join_remote_root(config.remote_root, run_id)

    local_root = Path(config.local_artifacts_dir).expanduser().resolve()
    local_run_dir = local_root / run_id
    local_run_dir.mkdir(parents=True, exist_ok=True)

    summary = (
        "Phase 01-02 API implementation complete: input validation and run metadata "
        "preparation are ready. Remote execution workflow is not included in this phase."
    )
    return PipelineResult(
        success=True,
        exit_code=EXIT_CODE_SUCCESS,
        run_id=run_id,
        remote_run_dir=remote_run_dir,
        local_artifacts_dir=str(local_run_dir),
        summary=summary,
    )


def _ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _join_remote_root(remote_root: str, run_id: str) -> str:
    normalized = remote_root.rstrip("/")
    if normalized:
        return f"{normalized}/{run_id}"
    return run_id

