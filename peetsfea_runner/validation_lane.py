from __future__ import annotations

import shutil
from pathlib import Path

from .pipeline import AccountConfig, PipelineConfig
from .runtime_policy import DEFAULT_REMOTE_ROOT


_PRUNE_ACCOUNTS = (
    AccountConfig(account_id="account_01", host_alias="gate1-harry261", max_jobs=10),
)

_PRESERVE_ACCOUNTS = (
    AccountConfig(account_id="account_01", host_alias="gate1-harry261", max_jobs=10),
)
_PRUNE_SLURM_PARTITIONS_ALLOWLIST = ("cpu2",)


def _repo_local_ssh_config_path(repo_root: Path) -> str:
    candidate = repo_root / ".ssh" / "config"
    if candidate.is_file():
        return str(candidate)
    return ""


def build_enroot_validation_lane_config(
    *,
    repo_root: str | Path,
    lane: str,
    window: str,
    remote_container_image: str,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    partition: str = "",
    mem: str = "960G",
    time_limit: str = "05:00:00",
    remote_container_ansys_root: str = "/opt/ohpc/pub/Electronics/v252",
    remote_ansys_executable: str = "/mnt/AnsysEM/ansysedt",
    ssh_config_path: str = "",
) -> PipelineConfig:
    resolved_repo_root = Path(repo_root).expanduser().resolve()
    lane_key = str(lane).strip().lower()
    normalized_window = str(window).strip()
    if lane_key not in {"prune", "preserve"}:
        raise ValueError("lane must be 'prune' or 'preserve'")
    if not normalized_window:
        raise ValueError("window must not be empty")
    if not str(remote_container_image).strip():
        raise ValueError("remote_container_image must not be empty")

    sample_src = resolved_repo_root / "examples" / "sample_0318.aedt"
    if not sample_src.is_file():
        raise FileNotFoundError(f"sample canary source not found: {sample_src}")

    root = resolved_repo_root / "tmp" / "tonight-canary" / normalized_window / lane_key
    input_dir = root / "input"
    output_dir = root / "output"
    delete_failed_dir = root / "delete_failed"
    db_path = root / "state.duckdb"

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    delete_failed_dir.mkdir(parents=True, exist_ok=True)

    sample_dst = input_dir / "sample.aedt"
    shutil.copy2(sample_src, sample_dst)
    (input_dir / "sample.aedt.ready").write_text("", encoding="utf-8")

    resolved_ssh_config_path = ssh_config_path.strip() or _repo_local_ssh_config_path(resolved_repo_root)

    if lane_key == "prune":
        accounts = _PRUNE_ACCOUNTS
        cpus_per_job = 48
        slots_per_job = 48
        cores_per_slot = 4
        tasks_per_slot = 1
    else:
        accounts = _PRESERVE_ACCOUNTS
        cpus_per_job = 32
        slots_per_job = 2
        cores_per_slot = 16
        tasks_per_slot = 4

    return PipelineConfig(
        input_queue_dir=str(input_dir),
        output_root_dir=str(output_dir),
        delete_failed_quarantine_dir=str(delete_failed_dir),
        metadata_db_path=str(db_path),
        accounts_registry=accounts,
        execute_remote=True,
        remote_execution_backend="slurm_batch",
        partition=partition,
        slurm_partitions_allowlist=_PRUNE_SLURM_PARTITIONS_ALLOWLIST if lane_key == "prune" else (),
        cpus_per_job=cpus_per_job,
        mem="288G" if lane_key == "prune" and str(mem).strip() == "960G" else mem,
        time_limit=time_limit,
        remote_root=remote_root,
        continuous_mode=False,
        slots_per_job=slots_per_job,
        worker_payload_slot_limit=slots_per_job,
        slot_min_concurrency=5,
        slot_max_concurrency=48 if lane_key == "prune" else 2,
        slot_memory_pressure_high_watermark_percent=90,
        slot_memory_pressure_resume_watermark_percent=80,
        slot_memory_probe_interval_seconds=5,
        worker_bundle_multiplier=1,
        cores_per_slot=cores_per_slot,
        tasks_per_slot=tasks_per_slot,
        input_source_policy="input_queue_only",
        public_storage_mode="disabled",
        readiness_probe_timeout_seconds=180,
        preflight_probe_timeout_seconds=180,
        ssh_config_path=resolved_ssh_config_path,
        remote_container_runtime="enroot",
        remote_container_image=remote_container_image,
        remote_container_ansys_root=remote_container_ansys_root,
        remote_ansys_executable=remote_ansys_executable,
        host=accounts[0].host_alias,
    )
