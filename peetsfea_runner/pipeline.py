from __future__ import annotations

import csv
from collections import deque
import json
import os
import shutil
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterator

from .constants import (
    EXIT_CODE_DOWNLOAD_FAILURE,
    EXIT_CODE_REMOTE_CLEANUP_FAILURE,
    EXIT_CODE_REMOTE_RUN_FAILURE,
    EXIT_CODE_SCREEN_FAILURE,
    EXIT_CODE_SLURM_FAILURE,
    EXIT_CODE_SSH_FAILURE,
    EXIT_CODE_SUCCESS,
)
from .remote_job import (
    CaseExecutionSummary as _CaseExecutionSummary,
    RemoteJobAttemptResult,
    SlotInput,
    WorkflowError as _WorkflowError,
    _build_case_aggregation_command,
    _build_case_slot_command,
    _build_remote_dispatch_script_content,
    _build_remote_job_script_content,
    _classify_return_path_failure_stage,
    _build_wait_all_command,
    _count_screen_slots,
    _extract_meaningful_remote_failure_details,
    _has_remote_workflow_markers,
    _parse_marked_case_summary_lines,
    _parse_marked_failed_count,
    _parse_case_summary_lines,
    cleanup_orphan_session,
    cleanup_orphan_sessions_for_run,
    query_slurm_job_state,
    run_remote_job_attempt,
)
from .scheduler import (
    AccountCapacitySnapshot,
    AccountReadinessSnapshot,
    BundleSpec,
    SlotWorkerController,
    SlotTaskRef,
    bootstrap_account_runtime,
    query_account_preflight,
    query_account_readiness,
    query_account_capacity,
    run_slot_workers,
)
from .state_store import StateStore
from .runtime_policy import (
    DEFAULT_REMOTE_ROOT,
    REMOTE_SCRATCH_HARD_LIMIT_MB,
    REMOTE_SCRATCH_SOFT_LIMIT_MB,
    join_remote_root,
)
from .version import get_version


APP_VERSION = get_version()

_SAMPLE_CANARY_INPUT_NAMES = frozenset({"sample.aedt", "sample_0318.aedt"})
_CANARY_REPORT_MANIFEST_COLUMNS = frozenset(
    {"design_name", "reports_dir", "report_count", "native_report_count", "synthetic_report_count", "status", "error_log"}
)
_DISPATCH_MODE_ALLOWED = frozenset({"run", "drain"})
_BAD_NODE_COOLDOWN_HOURS = 8
_BAD_NODE_NO_SPACE_MARKER = "No space left on device"


def _log_stage(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[peetsfea][{timestamp}] {message}", flush=True)


def _dispatch_mode_control_path() -> Path:
    return Path(__file__).resolve().parent.parent / "tmp" / "runtime" / "dispatch.mode"


def _read_dispatch_mode(*, control_path: Path | None = None) -> tuple[str, str | None]:
    path = _dispatch_mode_control_path() if control_path is None else control_path
    try:
        raw_value = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "run", None
    except OSError as exc:
        return "run", f"dispatch.mode read failed path={path} fallback=run reason={exc}"

    normalized = raw_value.strip().lower()
    if normalized in _DISPATCH_MODE_ALLOWED:
        return normalized, None
    if not normalized:
        raw_label = "<empty>"
    else:
        raw_label = raw_value.strip()
    return "run", f"dispatch.mode invalid value={raw_label!r} path={path} fallback=run"


def _bad_nodes_control_path() -> Path:
    return Path(__file__).resolve().parent.parent / "tmp" / "runtime" / "bad_nodes.json"


def _load_bad_node_policy_entries(*, control_path: Path | None = None) -> tuple[list[dict[str, object]], list[str]]:
    path = _bad_nodes_control_path() if control_path is None else control_path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], []
    except OSError as exc:
        return [], [f"bad_nodes.json read failed path={path} fallback=none reason={exc}"]
    except json.JSONDecodeError as exc:
        return [], [f"bad_nodes.json parse failed path={path} fallback=none reason={exc}"]

    if isinstance(payload, list):
        raw_entries = payload
    elif isinstance(payload, dict):
        raw_entries = payload.get("bad_nodes")
        if raw_entries is None:
            raw_entries = payload.get("nodes")
        if not isinstance(raw_entries, list):
            return [], [f"bad_nodes.json invalid payload path={path} fallback=none reason=missing_list"]
    else:
        return [], [f"bad_nodes.json invalid payload path={path} fallback=none reason=unsupported_type"]

    normalized_entries: list[dict[str, object]] = []
    warnings: list[str] = []
    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} reason=not_object")
            continue
        normalized_entries.append(dict(entry))
    return normalized_entries, warnings


def _load_active_bad_nodes(
    *,
    control_path: Path | None = None,
    now: datetime | None = None,
) -> tuple[tuple[str, ...], list[str]]:
    path = _bad_nodes_control_path() if control_path is None else control_path
    reference = now or datetime.now(tz=timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)

    raw_entries, warnings = _load_bad_node_policy_entries(control_path=path)

    active_nodes: list[str] = []
    for index, entry in enumerate(raw_entries):
        node = str(entry.get("node", "")).strip()
        reason = str(entry.get("reason", "")).strip()
        first_seen_at = _parse_optional_iso(entry.get("first_seen_at"))
        expires_at = _parse_optional_iso(entry.get("expires_at"))
        if not node:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} reason=node_missing")
            continue
        if first_seen_at is None:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} node={node} reason=first_seen_at_missing")
        if not reason:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} node={node} reason=reason_missing")
        if expires_at is None:
            warnings.append(f"bad_nodes.json invalid entry path={path} index={index} node={node} reason=expires_at_missing")
            continue
        if expires_at <= reference:
            continue
        active_nodes.append(node)

    return tuple(dict.fromkeys(active_nodes)), warnings


def _register_bad_node_candidate(
    *,
    node: str,
    reason: str,
    control_path: Path | None = None,
    observed_at: datetime | None = None,
    cooldown_hours: int = _BAD_NODE_COOLDOWN_HOURS,
) -> tuple[dict[str, str] | None, list[str]]:
    path = _bad_nodes_control_path() if control_path is None else control_path
    reference = observed_at or datetime.now(tz=timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    normalized_node = str(node).strip()
    normalized_reason = str(reason).strip()
    if not normalized_node:
        return None, [f"bad_nodes.json register failed path={path} reason=node_missing"]
    if not normalized_reason:
        return None, [f"bad_nodes.json register failed path={path} node={normalized_node} reason=reason_missing"]

    entries, warnings = _load_bad_node_policy_entries(control_path=path)
    if any(
        "read failed" in warning or "parse failed" in warning or "invalid payload" in warning
        for warning in warnings
    ):
        return None, warnings

    expires_at = reference + timedelta(hours=max(1, cooldown_hours))
    registered_entry: dict[str, str] | None = None
    for entry in entries:
        if str(entry.get("node", "")).strip() != normalized_node:
            continue
        existing_first_seen = _parse_optional_iso(entry.get("first_seen_at")) or reference
        existing_expires_at = _parse_optional_iso(entry.get("expires_at")) or reference
        first_seen_at = min(existing_first_seen, reference)
        effective_expires_at = max(existing_expires_at, expires_at)
        entry["node"] = normalized_node
        entry["reason"] = normalized_reason
        entry["first_seen_at"] = first_seen_at.isoformat()
        entry["expires_at"] = effective_expires_at.isoformat()
        registered_entry = {
            "node": normalized_node,
            "reason": normalized_reason,
            "first_seen_at": entry["first_seen_at"],
            "expires_at": entry["expires_at"],
        }
        break
    if registered_entry is None:
        registered_entry = {
            "node": normalized_node,
            "reason": normalized_reason,
            "first_seen_at": reference.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        entries.append(dict(registered_entry))

    serialized_entries = sorted(entries, key=lambda item: str(item.get("node", "")).strip())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"bad_nodes": serialized_entries}, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        warnings.append(f"bad_nodes.json write failed path={path} node={normalized_node} reason={exc}")
        return None, warnings
    return registered_entry, warnings


def _is_sample_canary_input(path: Path) -> bool:
    return path.name in _SAMPLE_CANARY_INPUT_NAMES


def _canary_input_label(paths: list[Path]) -> str:
    names = [path.name for path in paths if path.name]
    if not names:
        return "sample_input"
    if len(set(names)) == 1:
        return names[0]
    return "sample_input_batch"


@dataclass(slots=True)
class AccountConfig:
    account_id: str
    host_alias: str
    max_jobs: int = 10
    enabled: bool = True
    platform: str = "linux"
    scheduler: str = "slurm"
    remote_execution_backend: str = "slurm_batch"


@dataclass(slots=True, frozen=True)
class _ReadyArtifactState:
    ready_path: Path
    ready_present: bool
    ready_mode: str
    ready_error: str | None = None
    locked: bool = False


@dataclass(slots=True)
class _SlurmTruthAccountState:
    last_running_count: int | None = None
    last_pending_count: int | None = None
    mismatch_streak: int = 0


@dataclass(slots=True)
class PipelineConfig:
    input_queue_dir: str
    output_root_dir: str = "./output"
    delete_input_after_upload: bool = True
    rename_input_to_done_on_success: bool = False
    delete_failed_quarantine_dir: str = "./output/_delete_failed"
    metadata_db_path: str = "./peetsfea_runner.duckdb"
    accounts_registry: tuple[AccountConfig, ...] = field(
        default_factory=lambda: (AccountConfig(account_id="account_01", host_alias="gate1-harry261", max_jobs=10),)
    )
    # Execution/runtime settings
    partition: str = "cpu2"
    nodes: int = 1
    ntasks: int = 1
    cpus_per_job: int = 16
    mem: str = "960G"
    time_limit: str = "05:00:00"
    remote_root: str = DEFAULT_REMOTE_ROOT
    execute_remote: bool = False
    remote_execution_backend: str = "slurm_batch"
    control_plane_host: str = "127.0.0.1"
    control_plane_port: int = 8765
    control_plane_ssh_target: str = ""
    control_plane_return_host: str = ""
    control_plane_return_port: int = 5722
    control_plane_return_user: str = ""
    tunnel_heartbeat_timeout_seconds: int = 90
    tunnel_recovery_grace_seconds: int = 30
    remote_ssh_port: int = 22
    ssh_config_path: str = ""
    remote_container_runtime: str = "enroot"
    remote_container_image: str = "~/runtime/enroot/aedt.sqsh"
    remote_container_ansys_root: str = "/opt/ohpc/pub/Electronics/v252"
    remote_ansys_executable: str = "/mnt/AnsysEM/ansysedt"
    slots_per_job: int = 4
    worker_bundle_multiplier: int = 1
    cores_per_slot: int = 4
    tasks_per_slot: int = 1
    job_retry_count: int = 1
    worker_requeue_limit: int = 1
    scan_recursive: bool = True
    retain_aedtresults: bool = True
    # License policy
    license_observe_only: bool = True
    # Backward-compatible alias for old callers/docs
    max_jobs_per_account: int = 10
    local_artifacts_dir: str = "./output"
    emit_output_variables_csv: bool = True
    host: str = "gate1-harry261"
    # 11-01 continuous ingest settings
    continuous_mode: bool = True
    ingest_poll_seconds: int = 30
    ready_sidecar_suffix: str = ".ready"
    run_rotation_hours: int = 24
    run_namespace: str = ""
    pending_buffer_per_account: int = 3
    capacity_scope: str = "all_user_jobs"
    balance_metric: str = "slot_throughput"
    input_source_policy: str = "input_queue_only"
    public_storage_mode: str = "disabled"
    remote_storage_inode_block_percent: int = 98
    remote_storage_min_free_mb: int = 20480
    readiness_probe_timeout_seconds: int = 180
    preflight_probe_timeout_seconds: int = 180
    launch_transient_same_account_retries: int = 1
    launch_transient_cooldown_threshold: int = 3
    launch_transient_cooldown_window_seconds: int = 300
    launch_transient_cooldown_seconds: int = 300

    def validate(self) -> tuple[Path, Path, list[Path], list[AccountConfig]]:
        input_root = Path(self.input_queue_dir).expanduser().resolve()
        if not input_root.exists():
            raise FileNotFoundError(f"input_queue_dir not found: {input_root}")
        if not input_root.is_dir():
            raise ValueError(f"input_queue_dir must be a directory: {input_root}")

        output_root = Path(self.output_root_dir).expanduser().resolve()
        delete_quarantine = Path(self.delete_failed_quarantine_dir).expanduser().resolve()
        output_root.mkdir(parents=True, exist_ok=True)
        delete_quarantine.mkdir(parents=True, exist_ok=True)

        _ensure_positive("nodes", self.nodes)
        _ensure_positive("ntasks", self.ntasks)
        _ensure_positive("cpus_per_job", self.cpus_per_job)
        _ensure_positive("slots_per_job", self.slots_per_job)
        _ensure_positive("worker_bundle_multiplier", self.worker_bundle_multiplier)
        _ensure_positive("cores_per_slot", self.cores_per_slot)
        _ensure_positive("tasks_per_slot", self.tasks_per_slot)
        _ensure_positive("control_plane_port", self.control_plane_port)
        _ensure_positive("control_plane_return_port", self.control_plane_return_port)
        _ensure_positive("remote_ssh_port", self.remote_ssh_port)
        _ensure_positive("tunnel_heartbeat_timeout_seconds", self.tunnel_heartbeat_timeout_seconds)
        _ensure_positive("tunnel_recovery_grace_seconds", self.tunnel_recovery_grace_seconds)
        _ensure_positive("ingest_poll_seconds", self.ingest_poll_seconds)
        _ensure_positive("run_rotation_hours", self.run_rotation_hours)
        if self.job_retry_count < 0:
            raise ValueError("job_retry_count must be >= 0")
        if self.worker_requeue_limit < 0:
            raise ValueError("worker_requeue_limit must be >= 0")
        if self.pending_buffer_per_account < 0:
            raise ValueError("pending_buffer_per_account must be >= 0")
        if self.capacity_scope != "all_user_jobs":
            raise ValueError("capacity_scope must be 'all_user_jobs'")
        if self.balance_metric != "slot_throughput":
            raise ValueError("balance_metric must be 'slot_throughput'")
        if self.input_source_policy != "input_queue_only":
            raise ValueError("input_source_policy must be 'input_queue_only'")
        if self.public_storage_mode not in {"disabled", "private_only", "public_nas"}:
            raise ValueError("public_storage_mode must be 'disabled', 'private_only', or 'public_nas'")
        if self.remote_storage_inode_block_percent < 0 or self.remote_storage_inode_block_percent > 100:
            raise ValueError("remote_storage_inode_block_percent must be in 0..100")
        if self.remote_storage_min_free_mb < 0:
            raise ValueError("remote_storage_min_free_mb must be >= 0")
        _ensure_positive("readiness_probe_timeout_seconds", self.readiness_probe_timeout_seconds)
        _ensure_positive("preflight_probe_timeout_seconds", self.preflight_probe_timeout_seconds)
        if self.launch_transient_same_account_retries < 0:
            raise ValueError("launch_transient_same_account_retries must be >= 0")
        if self.launch_transient_cooldown_threshold < 1:
            raise ValueError("launch_transient_cooldown_threshold must be >= 1")
        _ensure_positive("launch_transient_cooldown_window_seconds", self.launch_transient_cooldown_window_seconds)
        _ensure_positive("launch_transient_cooldown_seconds", self.launch_transient_cooldown_seconds)
        if self.remote_execution_backend not in {"foreground_ssh", "slurm_batch"}:
            raise ValueError("remote_execution_backend must be 'foreground_ssh' or 'slurm_batch'")
        if self.remote_container_runtime not in {"none", "enroot"}:
            raise ValueError("remote_container_runtime must be 'none' or 'enroot'")
        if not self.ready_sidecar_suffix.strip():
            raise ValueError("ready_sidecar_suffix must not be empty")
        for name in (
            "partition",
            "mem",
            "time_limit",
            "remote_root",
            "metadata_db_path",
            "control_plane_host",
            "remote_container_ansys_root",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        ssh_config_path = self.ssh_config_path.strip()
        if ssh_config_path:
            ssh_config = Path(ssh_config_path).expanduser().resolve()
            if not ssh_config.exists():
                raise FileNotFoundError(f"ssh_config_path not found: {ssh_config}")
            if not ssh_config.is_file():
                raise ValueError(f"ssh_config_path must be a file: {ssh_config}")
        if self.cpus_per_job < (self.slots_per_job * self.cores_per_slot):
            raise ValueError(
                f"cpus_per_job must be >= slots_per_job * cores_per_slot ({self.slots_per_job * self.cores_per_slot})"
            )

        accounts = [account for account in self.accounts_registry if account.enabled]
        if not accounts:
            raise ValueError("At least one enabled account is required.")
        for account in accounts:
            if not account.account_id.strip():
                raise ValueError("account_id must not be empty")
            if not account.host_alias.strip():
                raise ValueError("host_alias must not be empty")
            if account.max_jobs <= 0:
                raise ValueError("account.max_jobs must be > 0")
            platform = account.platform.strip().lower()
            scheduler = account.scheduler.strip().lower()
            if (platform, scheduler) not in {("linux", "slurm"), ("windows", "none")}:
                raise ValueError("account platform/scheduler must be linux/slurm or windows/none")
        if self.execute_remote and self.remote_container_runtime == "enroot":
            if any((account.platform.strip().lower(), account.scheduler.strip().lower()) != ("linux", "slurm") for account in accounts):
                raise ValueError("remote enroot execution requires all accounts to be linux/slurm")
        if self.execute_remote and any(
            (account.platform.strip().lower(), account.scheduler.strip().lower()) == ("linux", "slurm")
            for account in accounts
        ):
            if self.remote_execution_backend != "slurm_batch":
                raise ValueError("linux/slurm remote execution requires remote_execution_backend='slurm_batch'")
            if self.remote_container_runtime != "enroot":
                raise ValueError("linux/slurm remote execution requires remote_container_runtime='enroot'")
        if self.remote_container_runtime == "enroot":
            if self.remote_execution_backend != "slurm_batch":
                raise ValueError("remote_container_runtime='enroot' requires remote_execution_backend='slurm_batch'")
            if not self.remote_container_image.strip():
                raise ValueError("remote_container_image must not be empty when remote_container_runtime='enroot'")

        files: list[Path] = []
        if not self.continuous_mode:
            files = _scan_input_aedt_files(input_root=input_root, recursive=self.scan_recursive)
        for path in files:
            _ensure_ready_artifact(path, self.ready_sidecar_suffix)
        if not files and not self.continuous_mode:
            raise ValueError(f"No .aedt files found in input_queue_dir: {input_root}")
        return input_root, output_root, files, accounts


@dataclass(slots=True)
class PipelineResult:
    success: bool
    exit_code: int
    run_id: str
    remote_run_dir: str
    local_artifacts_dir: str
    summary: str
    total_jobs: int
    success_jobs: int
    failed_jobs: int
    quarantined_jobs: int
    total_slots: int = 0
    active_slots: int = 0
    success_slots: int = 0
    failed_slots: int = 0
    quarantined_slots: int = 0
    queued_slots: int = 0
    terminal_jobs: int = 0
    replacement_jobs: int = 0
    ready_accounts: tuple[str, ...] = ()
    blocked_accounts: tuple[str, ...] = ()
    bootstrapping_accounts: tuple[str, ...] = ()
    readiness_blocked_slots: int = 0
    version: str = APP_VERSION

    @property
    def blocked(self) -> bool:
        return self.readiness_blocked_slots > 0 or bool(self.blocked_accounts)

    @property
    def recovery_needed(self) -> bool:
        if self.blocked or self.total_slots == 0:
            return False
        return (
            self.failed_jobs > 0
            or self.quarantined_jobs > 0
            or self.failed_slots > 0
            or self.quarantined_slots > 0
            or self.terminal_jobs > self.replacement_jobs
        )


@dataclass(slots=True)
class _BundleRuntimeOutcome:
    job_id: str
    success: bool
    quarantined: bool
    exit_code: int
    message: str
    success_slots: int
    failed_slots: int
    quarantined_slots: int
    requeue_slots: tuple[SlotTaskRef, ...] = ()

    @property
    def terminal_worker(self) -> bool:
        return bool(self.requeue_slots) or not self.success


@dataclass(slots=True)
class _RemoteExecutionConfig:
    host: str
    remote_root: str
    partition: str
    nodes: int
    ntasks: int
    cpus_per_job: int
    mem: str
    time_limit: str
    slots_per_job: int
    cores_per_slot: int
    tasks_per_slot: int
    platform: str = "linux"
    scheduler: str = "slurm"
    remote_execution_backend: str = "foreground_ssh"
    control_plane_host: str = "127.0.0.1"
    control_plane_port: int = 8765
    control_plane_ssh_target: str = ""
    control_plane_return_host: str = ""
    control_plane_return_port: int = 5722
    control_plane_return_user: str = ""
    tunnel_heartbeat_timeout_seconds: int = 90
    tunnel_recovery_grace_seconds: int = 30
    remote_ssh_port: int = 22
    ssh_config_path: str = ""
    remote_container_runtime: str = "enroot"
    remote_container_image: str = "~/runtime/enroot/aedt.sqsh"
    remote_container_ansys_root: str = "/opt/ohpc/pub/Electronics/v252"
    remote_ansys_executable: str = "/mnt/AnsysEM/ansysedt"
    slurm_exclude_nodes: tuple[str, ...] = ()


def _scan_input_aedt_files(*, input_root: Path, recursive: bool) -> list[Path]:
    return list(_iter_input_aedt_files(input_root=input_root, recursive=recursive))


def _storage_boundary_message(*, config: PipelineConfig) -> str:
    return (
        f"input_source_policy={config.input_source_policy} "
        f"public_storage_mode={config.public_storage_mode} "
        "runner_scope=control_plane_orchestration "
        "storage_scope=separate_service_boundary"
    )


def _canary_design_reports_gate_reason(*, output_root: Path) -> str:
    manifest_paths = sorted(output_root.rglob("design_outputs/index.csv"))
    if not manifest_paths:
        return "design_outputs_manifest_missing"
    saw_valid_manifest = False
    for manifest_path in manifest_paths:
        try:
            with manifest_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                header = {column.strip() for column in (reader.fieldnames or []) if column and column.strip()}
                if not _CANARY_REPORT_MANIFEST_COLUMNS.issubset(header):
                    continue
                saw_valid_manifest = True
                workdir = manifest_path.parent.parent
                saw_native_reports = False
                saw_incomplete_design = False
                for row in reader:
                    reports_dir_value = str(row.get("reports_dir", "")).strip()
                    if not reports_dir_value:
                        continue
                    design_status = str(row.get("status", "")).strip()
                    try:
                        native_report_count = int(str(row.get("native_report_count", "")).strip() or "0")
                    except ValueError:
                        continue
                    reports_dir = workdir / reports_dir_value
                    if not reports_dir.is_dir():
                        continue
                    if native_report_count > 0 and any(path.is_file() for path in reports_dir.glob("*.csv")):
                        saw_native_reports = True
                    if design_status in {"synthetic_only_fallback", "no_reports_exported"}:
                        saw_incomplete_design = True
                if saw_native_reports and not saw_incomplete_design:
                    return "ok"
        except (OSError, csv.Error):
            continue
    if saw_valid_manifest:
        return "design_outputs_manifest_incomplete"
    return "design_outputs_manifest_schema_invalid"


def _canary_materialized_output_present(*, output_root: Path) -> bool:
    return any(output_root.rglob("run.log")) and any(output_root.rglob("exit.code"))


def _canary_return_path_ready(*, state_store: StateStore, run_id: str, config: PipelineConfig) -> tuple[bool, str]:
    if config.remote_execution_backend != "slurm_batch":
        return True, "non_slurm_batch_backend"
    workers = state_store.list_slurm_workers(run_id=run_id)
    if not workers:
        return False, "worker_not_registered"
    if not any(str(worker.get("worker_state") or "").upper() in {"RUNNING", "IDLE_DRAINING", "COMPLETED"} for worker in workers):
        return False, "worker_never_running"
    if not any(str(worker.get("tunnel_state") or "").upper() == "CONNECTED" for worker in workers):
        return False, "tunnel_not_connected"
    if not any(worker.get("heartbeat_ts") for worker in workers):
        return False, "heartbeat_missing"
    return True, "ok"


def _record_launch_transient_failure(
    *,
    account_id: str,
    history_by_account: dict[str, deque[float]],
    cooldowns_by_account: dict[str, tuple[float, str]],
    now_monotonic: float,
    threshold: int,
    window_seconds: int,
    cooldown_seconds: int,
    reason: str,
) -> tuple[bool, float | None]:
    history = history_by_account.setdefault(account_id, deque())
    history.append(now_monotonic)
    cutoff = now_monotonic - window_seconds
    while history and history[0] < cutoff:
        history.popleft()
    if len(history) < threshold:
        return False, None
    cooldown_until = now_monotonic + cooldown_seconds
    existing = cooldowns_by_account.get(account_id)
    if existing is not None:
        cooldown_until = max(cooldown_until, existing[0])
    cooldowns_by_account[account_id] = (cooldown_until, reason)
    return True, cooldown_until


def _iter_input_aedt_files(*, input_root: Path, recursive: bool) -> Iterator[Path]:
    if not recursive:
        yield from sorted(
            [
                path
                for path in input_root.iterdir()
                if path.is_file() and path.suffix.lower() == ".aedt"
            ],
            key=lambda p: str(p.relative_to(input_root)).lower(),
        )
        return

    visited_dirs: set[tuple[int, int]] = set()
    for root, dirs, filenames in os.walk(input_root, followlinks=True):
        try:
            root_stat = os.stat(root)
        except OSError:
            dirs[:] = []
            continue
        root_key = (root_stat.st_dev, root_stat.st_ino)
        if root_key in visited_dirs:
            dirs[:] = []
            continue
        visited_dirs.add(root_key)
        dirs.sort()
        for filename in sorted(filenames):
            path = Path(root) / filename
            if path.suffix.lower() != ".aedt":
                continue
            if not path.is_file():
                continue
            yield path


def _configured_target_slots(*, config: PipelineConfig, accounts: list[AccountConfig]) -> int:
    return sum(max(1, account.max_jobs) for account in accounts) * config.slots_per_job


def _worker_bundle_slot_limit(*, config: PipelineConfig) -> int:
    if config.remote_execution_backend != "slurm_batch":
        return config.slots_per_job
    return max(config.slots_per_job, config.slots_per_job * config.worker_bundle_multiplier)


def _continuous_backlog_limits(*, config: PipelineConfig, accounts: list[AccountConfig]) -> tuple[int, int]:
    low_watermark = max(config.slots_per_job, _configured_target_slots(config=config, accounts=accounts))
    high_watermark = max(low_watermark, low_watermark * 2)
    return low_watermark, high_watermark


def _blocked_readiness_snapshot(*, account: AccountConfig, reason: str) -> AccountReadinessSnapshot:
    return AccountReadinessSnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        ready=False,
        status="BLOCKED",
        reason=reason,
        home_ok=False,
        runtime_path_ok=False,
        env_ok=False,
        python_ok=False,
        module_ok=False,
        binaries_ok=False,
        ansys_ok=False,
        storage_ready=False,
        storage_reason=reason,
    )


def _update_readiness_snapshot(
    snapshot: AccountReadinessSnapshot,
    *,
    status: str,
    reason: str,
    ready: bool | None = None,
) -> AccountReadinessSnapshot:
    return replace(
        snapshot,
        status=status,
        reason=reason,
        ready=snapshot.ready if ready is None else ready,
    )


def _reconcile_slurm_truth(
    *,
    snapshot: AccountCapacitySnapshot,
    local_active_jobs: int,
    previous_state: _SlurmTruthAccountState | None,
) -> tuple[_SlurmTruthAccountState, list[tuple[str, str, str]]]:
    previous = previous_state or _SlurmTruthAccountState()
    next_state = _SlurmTruthAccountState(
        last_running_count=snapshot.running_count,
        last_pending_count=snapshot.pending_count,
        mismatch_streak=0,
    )
    events: list[tuple[str, str, str]] = []
    if (
        previous.last_running_count != snapshot.running_count
        or previous.last_pending_count != snapshot.pending_count
        or previous_state is None
    ):
        events.append(
            (
                "INFO",
                "SLURM_TRUTH_REFRESHED",
                (
                    f"account={snapshot.account_id} host={snapshot.host_alias} "
                    f"remote_running={snapshot.running_count} remote_pending={snapshot.pending_count} "
                    f"local_active_jobs={local_active_jobs} allowed_submit={snapshot.allowed_submit}"
                ),
            )
        )

    remote_visible = max(0, snapshot.running_count) + max(0, snapshot.pending_count)
    if remote_visible < max(0, local_active_jobs):
        mismatch_streak = previous.mismatch_streak + 1
        next_state.mismatch_streak = mismatch_streak
        stage = "SLURM_TRUTH_LAG" if mismatch_streak == 1 else "CAPACITY_MISMATCH"
        level = "INFO" if mismatch_streak == 1 else "WARN"
        events.append(
            (
                level,
                stage,
                (
                    f"account={snapshot.account_id} host={snapshot.host_alias} "
                    f"remote_visible={remote_visible} local_active_jobs={local_active_jobs} "
                    f"mismatch_streak={mismatch_streak}"
                ),
            )
        )
    return next_state, events


def _parse_optional_iso(ts: object) -> datetime | None:
    if ts is None:
        return None
    text = str(ts).strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _is_stale_worker_heartbeat(*, heartbeat_ts: object, timeout_seconds: int) -> bool:
    parsed = _parse_optional_iso(heartbeat_ts)
    if parsed is None:
        return True
    age_seconds = (datetime.now(tz=timezone.utc) - parsed).total_seconds()
    return age_seconds > max(1, timeout_seconds)


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    if not isinstance(config, PipelineConfig):
        raise TypeError("config must be a PipelineConfig")

    input_root, output_root, aedt_files, accounts = config.validate()
    state_store = StateStore(Path(config.metadata_db_path))
    state_store.initialize()
    if config.continuous_mode:
        run_id = state_store.ensure_continuous_run(
            rotation_hours=config.run_rotation_hours,
            namespace=config.run_namespace,
        )
    else:
        run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        state_store.start_run(run_id)
    remote_run_dir = _join_remote_root(config.remote_root, run_id)
    total_inputs_label = "streaming" if config.continuous_mode else str(len(aedt_files))
    canary_candidate = (not config.continuous_mode) and bool(aedt_files) and all(
        _is_sample_canary_input(path) for path in aedt_files
    )
    canary_label = _canary_input_label(aedt_files) if canary_candidate else "sample_input"
    _log_stage(
        f"pipeline start version={APP_VERSION} run_id={run_id} "
        f"total_inputs={total_inputs_label} execute_remote={config.execute_remote}"
    )

    queued_slots = _load_schedulable_slots(state_store=state_store, run_id=run_id, input_root=input_root)
    total_slots = len(queued_slots)
    discovered_count = 0
    if queued_slots:
        _log_stage(f"restored queued slots run_id={run_id} count={len(queued_slots)}")

    slurm_truth_state_by_account: dict[str, _SlurmTruthAccountState] = {}
    cutover_ready_emitted = False
    cutover_blocked_emitted = False
    launch_transient_history_by_account: dict[str, deque[float]] = {}
    account_cooldowns: dict[str, tuple[float, str]] = {}

    def _append_worker_event(*, level: str, stage: str, message: str) -> None:
        state_store.append_event(run_id=run_id, job_id="__worker__", level=level, stage=stage, message=message)

    _append_worker_event(
        level="INFO",
        stage="SERVICE_BOUNDARY",
        message=_storage_boundary_message(config=config),
    )
    if canary_candidate:
        _append_worker_event(
            level="INFO",
            stage="CANARY_STARTED",
            message=(
                f"run_id={run_id} {canary_label} validation_lane=started "
                f"continuous_mode={config.continuous_mode} execute_remote={config.execute_remote} "
                f"input_dir={config.input_queue_dir} output_root={config.output_root_dir} "
                f"db_path={config.metadata_db_path} "
                f"delete_failed_dir={config.delete_failed_quarantine_dir}"
            ),
        )

    dispatch_mode_path = _dispatch_mode_control_path()
    dispatch_mode_warnings: set[str] = set()
    dispatch_drain_emitted = False
    bad_node_policy_warnings: set[str] = set()
    bad_node_registrations: set[tuple[str, str]] = set()
    scratch_guard_events: set[tuple[str, str]] = set()
    runtime_probe_events: set[tuple[str, str]] = set()

    def _current_dispatch_mode() -> str:
        mode, warning = _read_dispatch_mode(control_path=dispatch_mode_path)
        if warning is not None and warning not in dispatch_mode_warnings:
            _log_stage(warning)
            _append_worker_event(level="WARN", stage="DISPATCH_MODE_INVALID", message=warning)
            dispatch_mode_warnings.add(warning)
        return mode

    def _dispatch_drained(*, queued_count: int, inflight_jobs: int = 0) -> bool:
        nonlocal dispatch_drain_emitted
        if _current_dispatch_mode() != "drain":
            return False
        if not dispatch_drain_emitted:
            message = (
                f"run_id={run_id} path={dispatch_mode_path} mode=drain "
                f"queued_slots={queued_count} inflight_jobs={inflight_jobs}"
            )
            _log_stage(f"dispatch drain active {message}")
            _append_worker_event(level="INFO", stage="DISPATCH_DRAIN_ACTIVE", message=message)
            dispatch_drain_emitted = True
        return True

    def _emit_bad_node_policy_warnings(warnings: list[str]) -> None:
        for warning in warnings:
            if warning in bad_node_policy_warnings:
                continue
            _log_stage(warning)
            _append_worker_event(level="WARN", stage="BAD_NODES_INVALID", message=warning)
            bad_node_policy_warnings.add(warning)

    def _register_bad_node(*, node: str, reason: str) -> None:
        registered_entry, warnings = _register_bad_node_candidate(node=node, reason=reason)
        _emit_bad_node_policy_warnings(warnings)
        if registered_entry is None:
            return
        key = (registered_entry["node"], registered_entry["reason"])
        if key in bad_node_registrations:
            return
        _append_worker_event(
            level="WARN",
            stage="BAD_NODE_REGISTERED",
            message=(
                f"node={registered_entry['node']} reason={registered_entry['reason']} "
                f"expires_at={registered_entry['expires_at']}"
            ),
        )
        bad_node_registrations.add(key)

    def _reconcile_terminal_worker_failure(
        *,
        job_id: str,
        attempt_no: int,
        slurm_job_id: str,
        worker_state: str,
        observed_node: str | None,
    ) -> None:
        failure_message = (
            "rediscovered worker terminal state="
            f"{worker_state} slurm_job_id={slurm_job_id} observed_node={observed_node or 'unknown'}"
        )
        failed_slots = state_store.fail_active_job_from_rediscovered_worker(
            run_id=run_id,
            job_id=job_id,
            attempt_no=attempt_no,
            failure_reason=failure_message,
        )
        if failed_slots:
            state_store.append_event(
                run_id=run_id,
                job_id=job_id,
                level="ERROR" if worker_state == "FAILED" else "WARN",
                stage="WORKER_TERMINAL_REDISCOVERED",
                message=failure_message,
            )
            for slot_id, _ in failed_slots:
                state_store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot_id,
                    level="ERROR" if worker_state == "FAILED" else "WARN",
                    stage="FAILED",
                    message=failure_message,
                )

    if config.execute_remote and config.remote_execution_backend == "slurm_batch":
        active_workers = state_store.list_active_slurm_workers(run_id=run_id)
        if active_workers:
            account_by_id = {account.account_id: account for account in accounts}
            refreshed = 0
            for worker in active_workers:
                account = account_by_id.get(str(worker["account_id"]))
                if account is None:
                    continue
                remote_cfg = _RemoteExecutionConfig(
                    host=account.host_alias,
                    remote_root=config.remote_root,
                    partition=config.partition,
                    nodes=config.nodes,
                    ntasks=config.ntasks,
                    cpus_per_job=config.cpus_per_job,
                    mem=config.mem,
                    time_limit=config.time_limit,
                    slots_per_job=config.slots_per_job,
                    cores_per_slot=config.cores_per_slot,
                    tasks_per_slot=config.tasks_per_slot,
                    platform=account.platform,
                    scheduler=account.scheduler,
                    remote_execution_backend=config.remote_execution_backend,
                    control_plane_host=config.control_plane_host,
                    control_plane_port=config.control_plane_port,
                    control_plane_ssh_target=config.control_plane_ssh_target,
                    control_plane_return_host=config.control_plane_return_host,
                    control_plane_return_port=config.control_plane_return_port,
                    control_plane_return_user=config.control_plane_return_user,
                    tunnel_heartbeat_timeout_seconds=config.tunnel_heartbeat_timeout_seconds,
                    tunnel_recovery_grace_seconds=config.tunnel_recovery_grace_seconds,
                    remote_ssh_port=config.remote_ssh_port,
                    ssh_config_path=config.ssh_config_path,
                    remote_container_runtime=config.remote_container_runtime,
                    remote_container_image=config.remote_container_image,
                    remote_container_ansys_root=config.remote_container_ansys_root,
                    remote_ansys_executable=config.remote_ansys_executable,
                )
                try:
                    worker_state, observed_node = query_slurm_job_state(
                        remote_cfg,
                        slurm_job_id=str(worker["slurm_job_id"]),
                    )
                except Exception:
                    continue
                effective_worker_state = "LOST" if worker_state == "UNKNOWN" else worker_state
                state_store.upsert_slurm_worker(
                    run_id=run_id,
                    worker_id=str(worker["worker_id"]),
                    job_id=str(worker["job_id"]),
                    attempt_no=int(worker["attempt_no"]),
                    account_id=str(worker["account_id"]),
                    host_alias=str(worker["host_alias"]),
                    slurm_job_id=str(worker["slurm_job_id"]),
                    worker_state=effective_worker_state,
                    observed_node=observed_node,
                    slots_configured=int(worker["slots_configured"]),
                    backend=str(worker["backend"]),
                    tunnel_session_id=str(worker["tunnel_session_id"]) if worker.get("tunnel_session_id") else None,
                    tunnel_state=str(worker["tunnel_state"]) if worker.get("tunnel_state") else None,
                    heartbeat_ts=str(worker["heartbeat_ts"]) if worker.get("heartbeat_ts") else None,
                    degraded_reason=str(worker["degraded_reason"]) if worker.get("degraded_reason") else None,
                )
                if effective_worker_state in {"FAILED", "LOST"}:
                    _reconcile_terminal_worker_failure(
                        job_id=str(worker["job_id"]),
                        attempt_no=int(worker["attempt_no"]),
                        slurm_job_id=str(worker["slurm_job_id"]),
                        worker_state=effective_worker_state,
                        observed_node=observed_node,
                    )
                if effective_worker_state in {"RUNNING", "IDLE_DRAINING"} and _is_stale_worker_heartbeat(
                    heartbeat_ts=worker.get("heartbeat_ts"),
                    timeout_seconds=config.tunnel_heartbeat_timeout_seconds,
                ):
                    state_store.update_slurm_worker_control_plane(
                        run_id=run_id,
                        worker_id=str(worker["worker_id"]),
                        tunnel_state="DEGRADED",
                        degraded_reason="tunnel heartbeat stale after main restart",
                        observed_node=observed_node,
                    )
                    _append_worker_event(
                        level="WARN",
                        stage="CONTROL_TUNNEL_LOST",
                        message=(
                            f"worker_id={worker['worker_id']} slurm_job_id={worker['slurm_job_id']} "
                            f"reason=tunnel heartbeat stale after main restart"
                        ),
                    )
                refreshed += 1
            if refreshed:
                _append_worker_event(
                    level="INFO",
                    stage="SLURM_WORKERS_REDISCOVERED",
                    message=f"run_id={run_id} count={refreshed}",
                )
        for worker in state_store.list_jobs_with_terminal_slurm_workers(run_id=run_id):
            effective_worker_state = "LOST" if str(worker["worker_state"]) == "UNKNOWN" else str(worker["worker_state"])
            _reconcile_terminal_worker_failure(
                job_id=str(worker["job_id"]),
                attempt_no=int(worker["attempt_no"]),
                slurm_job_id=str(worker["slurm_job_id"]),
                worker_state=effective_worker_state,
                observed_node=worker.get("observed_node"),
            )
    if config.execute_remote:
        for account in accounts:
            janitor_cfg = _RemoteExecutionConfig(
                host=account.host_alias,
                remote_root=config.remote_root,
                partition=config.partition,
                nodes=config.nodes,
                ntasks=config.ntasks,
                cpus_per_job=config.cpus_per_job,
                mem=config.mem,
                time_limit=config.time_limit,
                slots_per_job=config.slots_per_job,
                cores_per_slot=config.cores_per_slot,
                tasks_per_slot=config.tasks_per_slot,
                platform=account.platform,
                scheduler=account.scheduler,
                remote_execution_backend=config.remote_execution_backend,
                control_plane_host=config.control_plane_host,
                control_plane_port=config.control_plane_port,
                control_plane_ssh_target=config.control_plane_ssh_target,
                control_plane_return_host=config.control_plane_return_host,
                control_plane_return_port=config.control_plane_return_port,
                control_plane_return_user=config.control_plane_return_user,
                tunnel_heartbeat_timeout_seconds=config.tunnel_heartbeat_timeout_seconds,
                tunnel_recovery_grace_seconds=config.tunnel_recovery_grace_seconds,
                remote_ssh_port=config.remote_ssh_port,
                ssh_config_path=config.ssh_config_path,
                remote_container_runtime=config.remote_container_runtime,
                remote_container_image=config.remote_container_image,
                remote_container_ansys_root=config.remote_container_ansys_root,
                remote_ansys_executable=config.remote_ansys_executable,
            )
            try:
                cleanup_orphan_sessions_for_run(config=janitor_cfg, run_id=run_id)
            except _WorkflowError as exc:
                _log_stage(f"startup janitor failed run_id={run_id} account={account.account_id} reason={exc}")

    def _capacity_log(snapshot: AccountCapacitySnapshot) -> None:
        state_store.record_account_capacity_snapshot(
            account_id=snapshot.account_id,
            host=snapshot.host_alias,
            running_count=snapshot.running_count,
            pending_count=snapshot.pending_count,
            allowed_submit=snapshot.allowed_submit,
        )
        local_active_jobs = state_store.count_active_jobs_by_account(run_id=run_id).get(snapshot.account_id, 0)
        truth_state, truth_events = _reconcile_slurm_truth(
            snapshot=snapshot,
            local_active_jobs=local_active_jobs,
            previous_state=slurm_truth_state_by_account.get(snapshot.account_id),
        )
        slurm_truth_state_by_account[snapshot.account_id] = truth_state
        for level, stage, message in truth_events:
            _append_worker_event(level=level, stage=stage, message=message)
        _log_stage(
            f"capacity account={snapshot.account_id} running={snapshot.running_count} "
            f"pending={snapshot.pending_count} allowed_submit={snapshot.allowed_submit}"
        )

    def _capacity_error(account: AccountConfig, exc: Exception) -> None:
        _log_stage(f"capacity query failed account={account.account_id} host={account.host_alias} reason={exc}")

    def _ssh_config_kwargs() -> dict[str, str]:
        ssh_config_path = config.ssh_config_path.strip()
        if not ssh_config_path:
            return {}
        return {"ssh_config_path": ssh_config_path}

    def _container_runtime_kwargs() -> dict[str, str]:
        return {
            "remote_container_runtime": config.remote_container_runtime,
            "remote_container_image": config.remote_container_image,
            "remote_container_ansys_root": config.remote_container_ansys_root,
            "remote_ansys_executable": config.remote_ansys_executable,
        }

    def _runtime_probe_kwargs() -> dict[str, str]:
        return {
            **_container_runtime_kwargs(),
            "remote_root": config.remote_root,
        }

    def _capacity_lookup_with_cooldown(*, account: AccountConfig, pending_buffer_per_account: int) -> AccountCapacitySnapshot:
        snapshot = query_account_capacity(
            account=account,
            pending_buffer_per_account=pending_buffer_per_account,
            **_ssh_config_kwargs(),
        )
        local_active_jobs = state_store.count_active_jobs_by_account(run_id=run_id).get(account.account_id, 0)
        remote_visible_workers = max(0, snapshot.running_count) + max(0, snapshot.pending_count)
        submitted_not_visible_workers = max(0, local_active_jobs - remote_visible_workers)
        snapshot = AccountCapacitySnapshot(
            account_id=snapshot.account_id,
            host_alias=snapshot.host_alias,
            running_count=snapshot.running_count,
            pending_count=snapshot.pending_count,
            allowed_submit=max(0, snapshot.allowed_submit - submitted_not_visible_workers),
        )
        cooldown = account_cooldowns.get(account.account_id)
        now_monotonic = time.monotonic()
        if cooldown is None:
            return snapshot
        cooldown_until, cooldown_reason = cooldown
        if cooldown_until <= now_monotonic:
            del account_cooldowns[account.account_id]
            return snapshot
        return AccountCapacitySnapshot(
            account_id=snapshot.account_id,
            host_alias=snapshot.host_alias,
            running_count=snapshot.running_count,
            pending_count=snapshot.pending_count,
            allowed_submit=0,
        )

    def _note_account_cooldown(account_id: str, host_alias: str, reason: str) -> None:
        now_monotonic = time.monotonic()
        cooldown_until = now_monotonic + config.launch_transient_cooldown_seconds
        existing = account_cooldowns.get(account_id)
        if existing is not None:
            cooldown_until = max(cooldown_until, existing[0])
        triggered, triggered_until = _record_launch_transient_failure(
            account_id=account_id,
            history_by_account=launch_transient_history_by_account,
            cooldowns_by_account=account_cooldowns,
            now_monotonic=now_monotonic,
            threshold=config.launch_transient_cooldown_threshold,
            window_seconds=config.launch_transient_cooldown_window_seconds,
            cooldown_seconds=config.launch_transient_cooldown_seconds,
            reason=reason,
        )
        effective_until = triggered_until or cooldown_until
        account_cooldowns[account_id] = (effective_until, reason)
        remaining = max(0, int(effective_until - now_monotonic))
        _append_worker_event(
            level="WARN",
            stage="ACCOUNT_COOLDOWN",
            message=(
                f"account={account_id} host={host_alias} cooldown_seconds={remaining} "
                f"reason={reason} triggered={triggered}"
            ),
        )

    def _bundle_submitted(bundle: BundleSpec) -> None:
        completed, inflight = state_store.get_slot_throughput_score(run_id=run_id, account_id=bundle.account_id)
        _log_stage(
            f"scheduler pick account={bundle.account_id} score={completed + inflight} slots={bundle.slot_count}"
        )
        _log_stage(
            f"bundle submitted job_id={bundle.job_id} account={bundle.account_id} slot_count={bundle.slot_count}"
        )

    outcomes: list[_BundleRuntimeOutcome] = []
    max_inflight_jobs = 0
    terminal_jobs = 0
    replacement_jobs = 0
    ready_account_ids: list[str] = []
    blocked_account_ids: list[str] = []
    bootstrapping_account_ids: list[str] = []
    readiness_blocked_slots = 0
    next_job_index = state_store.get_next_job_index(run_id=run_id)
    controller: SlotWorkerController[_BundleRuntimeOutcome] | None = None

    def _record_batch(batch: object) -> None:
        nonlocal max_inflight_jobs, next_job_index, terminal_jobs, replacement_jobs
        max_inflight_jobs = max(max_inflight_jobs, int(batch.max_inflight_jobs))
        next_job_index += int(batch.submitted_jobs)
        terminal_jobs += int(batch.terminal_jobs)
        replacement_jobs += int(batch.replacement_jobs)
        outcomes.extend(batch.results)

    def _current_completed_slots() -> dict[str, int]:
        return {
            account.account_id: state_store.get_slot_throughput_score(run_id=run_id, account_id=account.account_id)[0]
            for account in accounts
        }

    def _record_readiness(snapshot: AccountReadinessSnapshot) -> None:
        state_store.record_account_readiness_snapshot(
            account_id=snapshot.account_id,
            host=snapshot.host_alias,
            ready=snapshot.ready,
            status=snapshot.status,
            reason=snapshot.reason,
            home_ok=snapshot.home_ok,
            runtime_path_ok=snapshot.runtime_path_ok,
            env_ok=snapshot.env_ok,
            python_ok=snapshot.python_ok,
            module_ok=snapshot.module_ok,
            binaries_ok=snapshot.binaries_ok,
            ansys_ok=snapshot.ansys_ok,
            uv_ok=snapshot.uv_ok,
            pyaedt_ok=snapshot.pyaedt_ok,
            storage_ready=snapshot.storage_ready,
            storage_reason=snapshot.storage_reason,
            inode_use_percent=snapshot.inode_use_percent,
            free_mb=snapshot.free_mb,
            scratch_root=snapshot.scratch_root,
            scratch_usage_mb=snapshot.scratch_usage_mb,
        )
        if snapshot.ready:
            _log_stage(
                f"account readiness ready account={snapshot.account_id} host={snapshot.host_alias} "
                f"reason={snapshot.reason}"
            )
        else:
            _log_stage(
                f"account readiness {snapshot.status.lower()} account={snapshot.account_id} host={snapshot.host_alias} "
                f"reason={snapshot.reason}"
            )
            state_store.append_event(
                run_id=run_id,
                job_id="__worker__",
                level="WARN",
                stage=snapshot.status,
                message=f"account={snapshot.account_id} host={snapshot.host_alias} reason={snapshot.reason}",
            )
        scratch_root = str(snapshot.scratch_root or config.remote_root)
        if snapshot.scratch_usage_mb is not None and snapshot.scratch_usage_mb >= REMOTE_SCRATCH_SOFT_LIMIT_MB:
            if snapshot.scratch_usage_mb >= REMOTE_SCRATCH_HARD_LIMIT_MB:
                stage = "REMOTE_SCRATCH_HARD_LIMIT"
                level = "ERROR"
            else:
                stage = "REMOTE_SCRATCH_SOFT_LIMIT"
                level = "WARN"
            event_key = (snapshot.account_id, stage)
            if event_key not in scratch_guard_events:
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level=level,
                    stage=stage,
                    message=(
                        f"account={snapshot.account_id} host={snapshot.host_alias} "
                        f"scratch_root={scratch_root} usage_mb={snapshot.scratch_usage_mb}"
                    ),
                )
                scratch_guard_events.add(event_key)
        if "tmpfs_mount_failed" in str(snapshot.reason):
            event_key = (snapshot.account_id, "RUNTIME_TMPFS_PROBE_FAILED")
            if event_key not in runtime_probe_events:
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="WARN",
                    stage="RUNTIME_TMPFS_PROBE_FAILED",
                    message=f"account={snapshot.account_id} host={snapshot.host_alias} reason={snapshot.reason}",
                )
                runtime_probe_events.add(event_key)

    def _resolve_dispatch_accounts() -> list[AccountConfig]:
        nonlocal ready_account_ids, blocked_account_ids, bootstrapping_account_ids, cutover_ready_emitted
        if not config.execute_remote:
            ready_account_ids = [account.account_id for account in accounts]
            blocked_account_ids = []
            bootstrapping_account_ids = []
            return list(accounts)

        ready_accounts: list[AccountConfig] = []
        ready_account_ids = []
        blocked_account_ids = []
        bootstrapping_account_ids = []
        for account in accounts:
            try:
                snapshot = query_account_readiness(
                    account=account,
                    command_timeout_seconds=config.readiness_probe_timeout_seconds,
                    remote_storage_inode_block_percent=config.remote_storage_inode_block_percent,
                    remote_storage_min_free_mb=config.remote_storage_min_free_mb,
                    **_runtime_probe_kwargs(),
                    **_ssh_config_kwargs(),
                )
            except Exception as exc:
                snapshot = _blocked_readiness_snapshot(account=account, reason=str(exc))
            _record_readiness(snapshot)

            if not snapshot.ready and snapshot.status == "BOOTSTRAP_REQUIRED":
                bootstrapping_account_ids.append(account.account_id)
                bootstrapping_snapshot = _update_readiness_snapshot(
                    snapshot,
                    status="BOOTSTRAPPING",
                    reason=snapshot.reason,
                    ready=False,
                )
                _record_readiness(bootstrapping_snapshot)
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="ACCOUNT_BOOTSTRAP_START",
                    message=f"account={account.account_id} host={account.host_alias} reason={snapshot.reason}",
                )
                try:
                    bootstrap_account_runtime(
                        account=account,
                        **_container_runtime_kwargs(),
                        **_ssh_config_kwargs(),
                    )
                except Exception as exc:
                    failed_snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="BOOTSTRAP_FAILED",
                        reason=str(exc),
                        ready=False,
                    )
                    _record_readiness(failed_snapshot)
                    blocked_account_ids.append(account.account_id)
                    continue
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="ACCOUNT_BOOTSTRAP_OK",
                    message=f"account={account.account_id} host={account.host_alias}",
                )
                try:
                    snapshot = query_account_preflight(
                        account=account,
                        command_timeout_seconds=config.preflight_probe_timeout_seconds,
                        remote_storage_inode_block_percent=config.remote_storage_inode_block_percent,
                        remote_storage_min_free_mb=config.remote_storage_min_free_mb,
                        **_runtime_probe_kwargs(),
                        **_ssh_config_kwargs(),
                    )
                except Exception as exc:
                    snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="PREFLIGHT_FAILED",
                        reason=str(exc),
                        ready=False,
                    )
            elif snapshot.ready:
                try:
                    snapshot = query_account_preflight(
                        account=account,
                        command_timeout_seconds=config.preflight_probe_timeout_seconds,
                        remote_storage_inode_block_percent=config.remote_storage_inode_block_percent,
                        remote_storage_min_free_mb=config.remote_storage_min_free_mb,
                        **_runtime_probe_kwargs(),
                        **_ssh_config_kwargs(),
                    )
                except Exception as exc:
                    snapshot = _update_readiness_snapshot(
                        snapshot,
                        status="PREFLIGHT_FAILED",
                        reason=str(exc),
                        ready=False,
                    )

            _record_readiness(snapshot)
            if snapshot.ready:
                ready_accounts.append(account)
                ready_account_ids.append(account.account_id)
            else:
                blocked_account_ids.append(account.account_id)
        if config.execute_remote and canary_candidate and ready_accounts and not cutover_ready_emitted:
            _append_worker_event(
                level="INFO",
                stage="CUTOVER_READY",
                message=(
                    f"run_id={run_id} canary={canary_label} ready_accounts={','.join(ready_account_ids)} "
                    f"blocked_accounts={','.join(blocked_account_ids) or 'none'}"
                ),
            )
            cutover_ready_emitted = True
        return ready_accounts

    if config.continuous_mode and config.execute_remote and not queued_slots:
        _resolve_dispatch_accounts()

    def _run_slot_batch(slot_batch: list[SlotTaskRef]) -> bool:
        nonlocal max_inflight_jobs, next_job_index, terminal_jobs, replacement_jobs, readiness_blocked_slots, cutover_blocked_emitted
        if not slot_batch:
            return False
        if _dispatch_drained(queued_count=len(slot_batch)):
            return False
        dispatch_accounts = _resolve_dispatch_accounts()
        if not dispatch_accounts:
            readiness_blocked_slots = len(slot_batch)
            if config.execute_remote and not cutover_blocked_emitted:
                _append_worker_event(
                    level="WARN",
                    stage="CUTOVER_BLOCKED",
                    message=(
                        f"run_id={run_id} queued_slots={len(slot_batch)} "
                        f"blocked_accounts={','.join(blocked_account_ids) or 'none'}"
                    ),
                )
                cutover_blocked_emitted = True
            return False
        completed_slots = _current_completed_slots()
        if config.execute_remote:
            batch = run_slot_workers(
                slot_queue=slot_batch,
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                bundle_slot_limit=_worker_bundle_slot_limit(config=config),
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=lambda bundle: _run_bundle_with_retry(
                    config=config,
                    state_store=state_store,
                    run_id=run_id,
                    remote_run_dir=remote_run_dir,
                    bundle=bundle,
                    on_account_cooldown=_note_account_cooldown,
                ),
                capacity_lookup=_capacity_lookup_with_cooldown,
                initial_completed_slots=completed_slots,
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_capacity_error=_capacity_error,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=lambda _bundle, outcome: outcome.requeue_slots,
                terminal_bundle_lookup=lambda _bundle, outcome: outcome.terminal_worker,
            )
        else:
            batch = run_slot_workers(
                slot_queue=slot_batch,
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=lambda bundle: _run_dry_bundle(
                    run_id=run_id,
                    state_store=state_store,
                    bundle=bundle,
                ),
                capacity_lookup=_local_capacity_lookup,
                initial_completed_slots=completed_slots,
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=lambda _bundle, outcome: (),
                terminal_bundle_lookup=lambda _bundle, outcome: False,
            )
        _record_batch(batch)
        return bool(
            int(getattr(batch, "submitted_jobs", 0))
            or int(getattr(batch, "terminal_jobs", 0))
            or int(getattr(batch, "replacement_jobs", 0))
            or getattr(batch, "results", ())
        )

    def _dispatch_queued_slots(*, max_slots: int | None = None) -> None:
        nonlocal queued_slots
        while True:
            if not queued_slots:
                queued_slots = _load_schedulable_slots(
                    state_store=state_store,
                    run_id=run_id,
                    input_root=input_root,
                )
                if not queued_slots:
                    break
            if max_slots is None or max_slots <= 0 or len(queued_slots) <= max_slots:
                batch_slots = queued_slots
                queued_slots = []
            else:
                batch_slots = queued_slots[:max_slots]
                queued_slots = queued_slots[max_slots:]
            dispatched = _run_slot_batch(batch_slots)
            if not dispatched:
                queued_slots = batch_slots + queued_slots
                break
            if max_slots is not None:
                break

    if not config.continuous_mode:
        _dispatch_queued_slots()

    def _on_slot_enqueued(slot: SlotTaskRef) -> None:
        nonlocal total_slots
        queued_slots.append(slot)
        total_slots += 1
        _dispatch_queued_slots()

    if config.continuous_mode:
        low_watermark, high_watermark = _continuous_backlog_limits(config=config, accounts=accounts)
        pending_scan_files: deque[Path] = deque()
        rescan_scan_files: deque[Path] = deque()
        next_scan_monotonic = 0.0

        def _start_controller(*, force: bool = False) -> bool:
            nonlocal controller, queued_slots, readiness_blocked_slots, cutover_blocked_emitted
            if controller is not None:
                return True
            if not queued_slots:
                return False
            if not force and len(queued_slots) < config.slots_per_job:
                return False
            if _dispatch_drained(queued_count=len(queued_slots)):
                return False
            dispatch_accounts = _resolve_dispatch_accounts()
            if not dispatch_accounts:
                readiness_blocked_slots = len(queued_slots)
                if config.execute_remote and not cutover_blocked_emitted:
                    _append_worker_event(
                        level="WARN",
                        stage="CUTOVER_BLOCKED",
                        message=(
                            f"run_id={run_id} queued_slots={len(queued_slots)} "
                            f"blocked_accounts={','.join(blocked_account_ids) or 'none'}"
                        ),
                    )
                    cutover_blocked_emitted = True
                return False
            controller = SlotWorkerController(
                accounts=dispatch_accounts,
                slots_per_job=config.slots_per_job,
                bundle_slot_limit=_worker_bundle_slot_limit(config=config) if config.execute_remote else None,
                pending_buffer_per_account=config.pending_buffer_per_account,
                worker=(
                    lambda bundle: _run_bundle_with_retry(
                        config=config,
                        state_store=state_store,
                        run_id=run_id,
                        remote_run_dir=remote_run_dir,
                        bundle=bundle,
                        on_account_cooldown=_note_account_cooldown,
                    )
                )
                if config.execute_remote
                else (
                    lambda bundle: _run_dry_bundle(
                        run_id=run_id,
                        state_store=state_store,
                        bundle=bundle,
                    )
                ),
                capacity_lookup=_capacity_lookup_with_cooldown if config.execute_remote else _local_capacity_lookup,
                initial_completed_slots=_current_completed_slots(),
                job_index_start=next_job_index,
                on_capacity_snapshot=_capacity_log,
                on_capacity_error=_capacity_error if config.execute_remote else None,
                on_bundle_submitted=_bundle_submitted,
                recovery_slots_lookup=(
                    (lambda _bundle, outcome: outcome.requeue_slots)
                    if config.execute_remote
                    else (lambda _bundle, outcome: ())
                ),
                terminal_bundle_lookup=(
                    (lambda _bundle, outcome: outcome.terminal_worker)
                    if config.execute_remote
                    else (lambda _bundle, outcome: False)
                ),
            )
            controller.enqueue_slots(queued_slots, flush_partial=force)
            queued_slots = []
            controller.step(wait_for_progress=False, flush_partial_bundles=force)
            return True

        while True:
            if controller is not None and _current_dispatch_mode() == "drain":
                released_slots = controller.release_unsubmitted_slots()
                if released_slots:
                    queued_slots = released_slots + queued_slots
                controller_snapshot = controller.snapshot()
                if not controller.has_work():
                    _record_batch(controller.finalize())
                    controller = None
                else:
                    _dispatch_drained(
                        queued_count=len(queued_slots),
                        inflight_jobs=controller_snapshot.inflight_jobs,
                    )
            controller_has_work = controller is not None and controller.has_work()
            if (
                not controller_has_work
                and not queued_slots
                and not pending_scan_files
                and not rescan_scan_files
                and total_slots > 0
            ):
                break

            controller_snapshot = controller.snapshot() if controller is not None else None
            backlog_slots = len(queued_slots)
            if controller_snapshot is not None:
                backlog_slots += (
                    controller_snapshot.queued_slots
                    + controller_snapshot.pending_slots
                    + controller_snapshot.inflight_slots
                )

            if controller is None and queued_slots:
                scan_backlog_empty = not pending_scan_files and not rescan_scan_files
                forced_start = scan_backlog_empty or len(queued_slots) >= config.slots_per_job
                if forced_start and _start_controller(force=scan_backlog_empty):
                    continue

            now = time.monotonic()
            if now >= next_scan_monotonic:
                scan_results = _scan_input_aedt_files(input_root=input_root, recursive=config.scan_recursive)
                queued_scan_paths = set(pending_scan_files)
                queued_scan_paths.update(rescan_scan_files)
                filtered_scan_results = [
                    path
                    for path in scan_results
                    if path not in queued_scan_paths
                ]
                has_active_ingest_backlog = bool(
                    pending_scan_files or rescan_scan_files or queued_slots or controller is not None
                )
                if has_active_ingest_backlog:
                    rescan_scan_files.extend(filtered_scan_results)
                else:
                    pending_scan_files.extend(filtered_scan_results)
                next_scan_monotonic = now + config.ingest_poll_seconds

            allow_rescan_ingest = bool(rescan_scan_files)
            scan_queue = rescan_scan_files if allow_rescan_ingest else pending_scan_files
            if scan_queue and (backlog_slots < high_watermark or allow_rescan_ingest):
                input_file = scan_queue.popleft()
                slots, _discovered_delta = _ingest_slot_queue(
                    config=config,
                    state_store=state_store,
                    run_id=run_id,
                    input_root=input_root,
                    output_root=output_root,
                    aedt_files=[input_file],
                )
                discovered_count += len(slots)
                if slots:
                    total_slots += len(slots)
                    if controller is not None and _current_dispatch_mode() != "drain":
                        controller.enqueue_slots(slots)
                        allow_partial_flush = not pending_scan_files and not rescan_scan_files
                        controller.step(wait_for_progress=False, flush_partial_bundles=allow_partial_flush)
                    else:
                        queued_slots.extend(slots)
                        if len(queued_slots) >= config.slots_per_job:
                            _start_controller()
                continue

            if controller is not None:
                controller_snapshot = controller.snapshot()
                controller_backlog = (
                    controller_snapshot.queued_slots
                    + controller_snapshot.pending_slots
                    + controller_snapshot.inflight_slots
                )
                wait_for_progress = not pending_scan_files and not rescan_scan_files
                progressed = controller.step(
                    wait_for_progress=wait_for_progress,
                    flush_partial_bundles=wait_for_progress,
                )
                if progressed:
                    continue
                if not controller.has_work():
                    _record_batch(controller.finalize())
                    controller = None
                    if not queued_slots and not pending_scan_files and not rescan_scan_files:
                        break
                    continue

            if controller is None and not pending_scan_files and not rescan_scan_files:
                if queued_slots:
                    if _dispatch_drained(queued_count=len(queued_slots)):
                        break
                    if _start_controller(force=True):
                        continue
                    readiness_blocked_slots = len(queued_slots)
                else:
                    break

            if controller is None and blocked_account_ids and backlog_slots >= high_watermark:
                readiness_blocked_slots = backlog_slots
                if config.execute_remote and not cutover_blocked_emitted:
                    _append_worker_event(
                        level="WARN",
                        stage="CUTOVER_BLOCKED",
                        message=(
                            f"run_id={run_id} backlog_slots={backlog_slots} "
                            f"blocked_accounts={','.join(blocked_account_ids) or 'none'}"
                        ),
                    )
                    cutover_blocked_emitted = True
                break
    else:
        slots, discovered_count = _ingest_slot_queue(
            config=config,
            state_store=state_store,
            run_id=run_id,
            input_root=input_root,
            output_root=output_root,
            aedt_files=aedt_files,
            on_slot_enqueued=None,
        )
        if slots:
            queued_slots.extend(slots)
            total_slots += len(slots)

        _dispatch_queued_slots()

    if config.continuous_mode and controller is not None:
        _record_batch(controller.finalize())

    remaining_queued_slots = len(queued_slots)
    dispatch_mode = _current_dispatch_mode()

    if total_slots == 0:
        summary = (
            f"version={APP_VERSION} total_slots=0 active_slots=0 success_slots=0 failed_slots=0 quarantined_slots=0 "
            f"active_jobs=0 ingest_discovered={discovered_count} ingest_enqueued=0 "
            f"queued_slots={remaining_queued_slots} dispatch_mode={dispatch_mode}"
        )
        canary_gate_ok = not canary_candidate
        canary_gate_reason = "not_canary"
        if canary_candidate:
            canary_gate_ok = False
            canary_gate_reason = "submit_missing"
            summary = f"{summary} canary_gate={canary_gate_reason}"
        if config.continuous_mode:
            state_store.update_run_summary(run_id=run_id, summary=summary)
        else:
            state_store.finish_run(run_id, state="SUCCEEDED" if canary_gate_ok else "FAILED", summary=summary)
        if canary_candidate:
            _append_worker_event(
                level="ERROR",
                stage="CANARY_FAILED",
                message=f"run_id={run_id} {canary_label} canary failed reason={canary_gate_reason}",
            )
            _append_worker_event(
                level="WARN",
                stage="ROLLBACK_TRIGGERED",
                message=f"fallback=01a_01b_only reason={canary_gate_reason}",
            )
        _log_stage(f"pipeline idle run_id={run_id} discovered={discovered_count}")
        return PipelineResult(
            success=canary_gate_ok,
            exit_code=EXIT_CODE_SUCCESS if canary_gate_ok else EXIT_CODE_REMOTE_RUN_FAILURE,
            run_id=run_id,
            remote_run_dir=remote_run_dir,
            local_artifacts_dir=str(output_root),
            summary=summary,
            total_jobs=0,
            success_jobs=0,
            failed_jobs=0,
            quarantined_jobs=0,
            total_slots=0,
            active_slots=0,
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
            queued_slots=remaining_queued_slots,
            terminal_jobs=0,
            replacement_jobs=0,
            ready_accounts=tuple(ready_account_ids),
            blocked_accounts=tuple(blocked_account_ids),
            bootstrapping_accounts=tuple(bootstrapping_account_ids),
            readiness_blocked_slots=0,
        )

    orphan_cleanup_error: _WorkflowError | None = None
    if config.execute_remote:
        for account in accounts:
            cleanup_cfg = _RemoteExecutionConfig(
                host=account.host_alias,
                remote_root=config.remote_root,
                partition=config.partition,
                nodes=config.nodes,
                ntasks=config.ntasks,
                cpus_per_job=config.cpus_per_job,
                mem=config.mem,
                time_limit=config.time_limit,
                slots_per_job=config.slots_per_job,
                cores_per_slot=config.cores_per_slot,
                tasks_per_slot=config.tasks_per_slot,
                platform=account.platform,
                scheduler=account.scheduler,
                remote_execution_backend=config.remote_execution_backend,
                control_plane_host=config.control_plane_host,
                control_plane_port=config.control_plane_port,
                control_plane_ssh_target=config.control_plane_ssh_target,
                control_plane_return_host=config.control_plane_return_host,
                control_plane_return_port=config.control_plane_return_port,
                control_plane_return_user=config.control_plane_return_user,
                tunnel_heartbeat_timeout_seconds=config.tunnel_heartbeat_timeout_seconds,
                tunnel_recovery_grace_seconds=config.tunnel_recovery_grace_seconds,
                remote_ssh_port=config.remote_ssh_port,
                ssh_config_path=config.ssh_config_path,
                remote_container_runtime=config.remote_container_runtime,
                remote_container_image=config.remote_container_image,
                remote_container_ansys_root=config.remote_container_ansys_root,
                remote_ansys_executable=config.remote_ansys_executable,
            )
            try:
                cleanup_orphan_sessions_for_run(config=cleanup_cfg, run_id=run_id)
            except _WorkflowError as exc:
                orphan_cleanup_error = exc
                _log_stage(f"orphan cleanup failed run_id={run_id} account={account.account_id} reason={exc}")

    outcomes = sorted(outcomes, key=lambda item: item.job_id)
    total_jobs = len(outcomes)
    failed_jobs = sum(1 for item in outcomes if not item.success)
    quarantined_jobs = sum(1 for item in outcomes if item.quarantined)
    success_jobs = total_jobs - failed_jobs
    failed_job_ids = [item.job_id for item in outcomes if not item.success]

    success_slots = sum(item.success_slots for item in outcomes)
    failed_slots = sum(item.failed_slots for item in outcomes)
    quarantined_slots = sum(item.quarantined_slots for item in outcomes)

    success = (
        failed_jobs == 0
        and quarantined_jobs == 0
        and failed_slots == 0
        and quarantined_slots == 0
        and orphan_cleanup_error is None
        and readiness_blocked_slots == 0
    )
    first_failure_code = next((item.exit_code for item in outcomes if item.exit_code != EXIT_CODE_SUCCESS), EXIT_CODE_SUCCESS)
    exit_code = EXIT_CODE_SUCCESS if success else first_failure_code
    if exit_code == EXIT_CODE_SUCCESS and readiness_blocked_slots > 0:
        exit_code = EXIT_CODE_REMOTE_RUN_FAILURE
    if orphan_cleanup_error is not None:
        exit_code = orphan_cleanup_error.exit_code

    canary_gate_reason = "ok"
    if canary_candidate and readiness_blocked_slots > 0:
        canary_gate_reason = "readiness_blocked"
    elif canary_candidate and orphan_cleanup_error is not None:
        canary_gate_reason = "orphan_cleanup_failed"
    if canary_candidate and success:
        return_path_ok, return_path_reason = _canary_return_path_ready(
            state_store=state_store,
            run_id=run_id,
            config=config,
        )
        if not return_path_ok:
            _append_worker_event(
                level="WARN",
                stage="CONTROL_TUNNEL_DEGRADED",
                message=f"run_id={run_id} canary_return_path={return_path_reason}",
            )
        if not _canary_materialized_output_present(output_root=output_root):
            success = False
            exit_code = EXIT_CODE_DOWNLOAD_FAILURE
            canary_gate_reason = "materialized_output_missing"
        else:
            report_gate_reason = _canary_design_reports_gate_reason(output_root=output_root)
            if report_gate_reason != "ok":
                success = False
                exit_code = EXIT_CODE_DOWNLOAD_FAILURE
                canary_gate_reason = report_gate_reason

    summary = (
        f"version={APP_VERSION} total_jobs={total_jobs} success_jobs={success_jobs} failed_jobs={failed_jobs} "
        f"quarantined_jobs={quarantined_jobs} total_slots={total_slots} "
        f"active_slots=0 success_slots={success_slots} failed_slots={failed_slots} "
        f"quarantined_slots={quarantined_slots} failed_job_ids={failed_job_ids} "
        f"active_jobs=0 max_inflight_jobs={max_inflight_jobs} "
        f"terminal_jobs={terminal_jobs} replacement_jobs={replacement_jobs} "
        f"queued_slots={remaining_queued_slots} dispatch_mode={dispatch_mode} "
        f"ready_accounts={ready_account_ids} blocked_accounts={blocked_account_ids} "
        f"bootstrapping_accounts={bootstrapping_account_ids} "
        f"readiness_blocked_slots={readiness_blocked_slots}"
    )
    if canary_candidate:
        summary = f"{summary} canary_gate={canary_gate_reason}"
    if orphan_cleanup_error is not None:
        summary = f"{summary} orphan_cleanup_error={orphan_cleanup_error}"

    if config.continuous_mode:
        state_store.update_run_summary(run_id=run_id, summary=summary)
    else:
        state_store.finish_run(run_id, state="SUCCEEDED" if success else "FAILED", summary=summary)
    if canary_candidate:
        if success:
            _append_worker_event(
                level="INFO",
                stage="CANARY_PASSED",
                message=f"run_id={run_id} {canary_label} canary passed reason={canary_gate_reason}",
            )
            _append_worker_event(
                level="INFO",
                stage="FULL_ROLLOUT_READY",
                message=(
                    f"run_id={run_id} next_stage=full_backlog "
                    f"fallback=disabled until next_failure {_storage_boundary_message(config=config)}"
                ),
            )
        else:
            _append_worker_event(
                level="ERROR",
                stage="CANARY_FAILED",
                message=f"run_id={run_id} {canary_label} canary failed reason={canary_gate_reason}",
            )
            _append_worker_event(
                level="WARN",
                stage="ROLLBACK_TRIGGERED",
                message=f"fallback=01a_01b_only reason={canary_gate_reason}",
            )
    _log_stage(
        f"pipeline end version={APP_VERSION} run_id={run_id} success={success} "
        f"total_jobs={total_jobs} failed_jobs={failed_jobs} quarantined_jobs={quarantined_jobs}"
    )

    return PipelineResult(
        success=success,
        exit_code=exit_code,
        run_id=run_id,
        remote_run_dir=remote_run_dir,
        local_artifacts_dir=str(output_root),
        summary=summary,
        total_jobs=total_jobs,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        quarantined_jobs=quarantined_jobs,
        total_slots=total_slots,
        active_slots=0,
        success_slots=success_slots,
        failed_slots=failed_slots,
        quarantined_slots=quarantined_slots,
        queued_slots=remaining_queued_slots,
        terminal_jobs=terminal_jobs,
        replacement_jobs=replacement_jobs,
        ready_accounts=tuple(ready_account_ids),
        blocked_accounts=tuple(blocked_account_ids),
        bootstrapping_accounts=tuple(bootstrapping_account_ids),
        readiness_blocked_slots=readiness_blocked_slots,
    )


def _load_schedulable_slots(*, state_store: StateStore, run_id: str, input_root: Path) -> list[SlotTaskRef]:
    slots: list[SlotTaskRef] = []
    for slot_id, input_path, output_path, attempt_no in state_store.list_schedulable_slot_tasks(run_id=run_id):
        input_ref = Path(input_path)
        output_ref = Path(output_path)
        try:
            relative_path = input_ref.relative_to(input_root)
        except Exception:
            relative_path = Path(input_ref.name)
        slots.append(
            SlotTaskRef(
                run_id=run_id,
                slot_id=slot_id,
                input_path=input_ref,
                relative_path=Path(relative_path),
                output_dir=output_ref,
                attempt_no=max(1, attempt_no),
            )
        )
    return slots


def _ingest_slot_queue(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    input_root: Path,
    output_root: Path,
    aedt_files: list[Path],
    on_slot_enqueued: Callable[[SlotTaskRef], None] | None = None,
) -> tuple[list[SlotTaskRef], int]:
    slots: list[SlotTaskRef] = []
    discovered_count = 0

    for input_file in aedt_files:
        relative_path = input_file.relative_to(input_root)
        output_dir = _build_slot_output_dir(
            output_root=output_root,
            relative_path=relative_path,
        )
        ready_state = _ensure_ready_artifact(input_file, config.ready_sidecar_suffix)
        if getattr(ready_state, "locked", False):
            discovered_count += 1
            continue
        file_stat = input_file.stat()
        ready_mtime_ns = None
        if ready_state.ready_present and ready_state.ready_path.exists():
            try:
                ready_mtime_ns = ready_state.ready_path.stat().st_mtime_ns
            except OSError:
                ready_mtime_ns = None
        discovered_count += 1

        if config.continuous_mode:
            inserted = state_store.register_ingest_candidate(
                input_path=str(input_file),
                ready_path=str(ready_state.ready_path),
                ready_present=ready_state.ready_present,
                ready_mode=ready_state.ready_mode,
                ready_error=ready_state.ready_error,
                ready_mtime_ns=ready_mtime_ns,
                file_size=file_stat.st_size,
                file_mtime_ns=file_stat.st_mtime_ns,
            )
            if not inserted:
                continue

        slot_id = _build_slot_id(
            relative_path=relative_path,
            mtime_ns=file_stat.st_mtime_ns,
            ready_mtime_ns=ready_mtime_ns,
        )
        state_store.create_slot_task(
            run_id=run_id,
            slot_id=slot_id,
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot_id,
            level="INFO",
            stage="READY" if ready_state.ready_present else "READY_INTERNAL",
            message=(
                f"ready_mode={ready_state.ready_mode}"
                if ready_state.ready_error is None
                else f"ready_mode={ready_state.ready_mode} error={ready_state.ready_error}"
            ),
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot_id,
            level="INFO",
            stage="QUEUED",
            message="ingested",
        )
        if config.continuous_mode:
            state_store.mark_ingest_state(input_path=str(input_file), state="QUEUED")
        slot_ref = SlotTaskRef(
            run_id=run_id,
            slot_id=slot_id,
            input_path=input_file,
            relative_path=relative_path,
            output_dir=output_dir,
            attempt_no=1,
        )
        if on_slot_enqueued is None:
            slots.append(slot_ref)
        else:
            on_slot_enqueued(slot_ref)

    return slots, discovered_count


def _run_dry_bundle(
    *,
    run_id: str,
    state_store: StateStore,
    bundle: BundleSpec,
) -> _BundleRuntimeOutcome:
    if not bundle.slot_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no slots",
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
        )

    first_slot = bundle.slot_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_slot.input_path),
        output_path=str(first_slot.output_dir),
        account_id=bundle.account_id,
    )
    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=0)
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="RUNNING",
        message=f"dry-run slot_count={bundle.slot_count}",
    )

    for slot in bundle.slot_inputs:
        _initialize_slot_output_dir(slot=slot)
        state_store.update_slot_task(
            run_id=run_id,
            slot_id=slot.slot_id,
            state="SUCCEEDED",
            attempt_no=0,
            job_id=bundle.job_id,
            account_id=bundle.account_id,
            failure_reason=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="INFO",
            stage="SUCCEEDED",
            message="execute_remote=False dry run",
        )
        state_store.mark_ingest_state(input_path=str(slot.input_path), state="SUCCEEDED")
        state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(slot.output_dir))

    state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUCCEEDED", attempt_no=0)
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="SUCCEEDED",
        message="execute_remote=False dry run",
    )
    return _BundleRuntimeOutcome(
        job_id=bundle.job_id,
        success=True,
        quarantined=False,
        exit_code=EXIT_CODE_SUCCESS,
        message="execute_remote=False dry run",
        success_slots=bundle.slot_count,
        failed_slots=0,
        quarantined_slots=0,
    )


def _run_bundle_with_retry(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    remote_run_dir: str,
    bundle: BundleSpec,
    on_account_cooldown: Callable[[str, str, str], None] | None = None,
) -> _BundleRuntimeOutcome:
    if not bundle.slot_inputs:
        return _BundleRuntimeOutcome(
            job_id=bundle.job_id,
            success=True,
            quarantined=False,
            exit_code=EXIT_CODE_SUCCESS,
            message="no slots",
            success_slots=0,
            failed_slots=0,
            quarantined_slots=0,
        )

    first_slot = bundle.slot_inputs[0]
    state_store.create_job(
        run_id=run_id,
        job_id=bundle.job_id,
        input_path=str(first_slot.input_path),
        output_path=str(first_slot.output_dir),
        account_id=bundle.account_id,
    )
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO",
        stage="PENDING",
        message=f"bundle queued account={bundle.account_id} slots={bundle.slot_count}",
    )

    excluded_nodes, bad_node_warnings = _load_active_bad_nodes()
    for warning in dict.fromkeys(bad_node_warnings):
        _log_stage(warning)
        state_store.append_event(
            run_id=run_id,
            job_id="__worker__",
            level="WARN",
            stage="BAD_NODES_INVALID",
            message=warning,
        )
    if excluded_nodes:
        state_store.append_event(
            run_id=run_id,
            job_id=bundle.job_id,
            level="INFO",
            stage="BAD_NODE_EXCLUDE_ACTIVE",
            message=(
                f"account={bundle.account_id} host={bundle.host_alias} "
                f"nodes={','.join(excluded_nodes)}"
            ),
        )

    remote_cfg = _RemoteExecutionConfig(
        host=bundle.host_alias,
        remote_root=config.remote_root,
        partition=config.partition,
        nodes=config.nodes,
        ntasks=config.ntasks,
        cpus_per_job=config.cpus_per_job,
        mem=config.mem,
        time_limit=config.time_limit,
        slots_per_job=config.slots_per_job,
        cores_per_slot=config.cores_per_slot,
        tasks_per_slot=config.tasks_per_slot,
        platform=bundle.platform,
        scheduler=bundle.scheduler,
        remote_execution_backend=config.remote_execution_backend,
        control_plane_host=config.control_plane_host,
        control_plane_port=config.control_plane_port,
        control_plane_ssh_target=config.control_plane_ssh_target,
        control_plane_return_host=config.control_plane_return_host,
        control_plane_return_port=config.control_plane_return_port,
        control_plane_return_user=config.control_plane_return_user,
        tunnel_heartbeat_timeout_seconds=config.tunnel_heartbeat_timeout_seconds,
        tunnel_recovery_grace_seconds=config.tunnel_recovery_grace_seconds,
        remote_ssh_port=config.remote_ssh_port,
        ssh_config_path=config.ssh_config_path,
        remote_container_runtime=config.remote_container_runtime,
        remote_container_image=config.remote_container_image,
        remote_container_ansys_root=config.remote_container_ansys_root,
        remote_ansys_executable=config.remote_ansys_executable,
        slurm_exclude_nodes=excluded_nodes,
    )
    with TemporaryDirectory(prefix=f"peetsfea_bundle_{bundle.job_id}_") as tmpdir:
        local_bundle_dir = Path(tmpdir) / "bundle"
        local_bundle_dir.mkdir(parents=True, exist_ok=True)
        retry_inputs_dir = local_bundle_dir / "_retry_inputs"
        retry_inputs_dir.mkdir(parents=True, exist_ok=True)
        pending_slots = list(bundle.slot_inputs)
        staged_input_paths: dict[str, Path] = {}
        for slot in pending_slots:
            staged_path = retry_inputs_dir / f"{slot.slot_id}.aedt"
            try:
                if slot.input_path.exists():
                    shutil.copy2(slot.input_path, staged_path)
                    staged_input_paths[slot.slot_id] = staged_path
            except OSError:
                continue
        deleted_slot_ids: set[str] = set()
        success_slots = 0
        failed_slots = 0
        quarantined_slots = 0
        terminal_exit_code = EXIT_CODE_SUCCESS
        terminal_message = "ok"
        last_attempt_no = 0
        worker_requeue_slots: list[SlotTaskRef] = []

        max_same_account_attempts = max(config.job_retry_count, config.launch_transient_same_account_retries)
        for attempt in range(1, max_same_account_attempts + 2):
            if not pending_slots:
                break
            last_attempt_no = attempt

            runnable_slots: list[SlotTaskRef] = []
            for slot in pending_slots:
                source_path = staged_input_paths.get(slot.slot_id, slot.input_path)
                if source_path.exists():
                    runnable_slots.append(slot)
                    continue
                state_store.quarantine_job(
                    run_id=run_id,
                    job_id=slot.slot_id,
                    attempt=attempt,
                    reason=f"input missing: {slot.input_path}",
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                )
                state_store.update_slot_task(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    state="QUARANTINED",
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    failure_reason=f"input missing: {slot.input_path}",
                )
                state_store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="ERROR",
                    stage="QUARANTINED",
                    message=f"input missing: {slot.input_path}",
                )
                state_store.mark_ingest_state(input_path=str(slot.input_path), state="QUARANTINED")
                quarantined_slots += 1

            if not runnable_slots:
                pending_slots = []
                terminal_exit_code = EXIT_CODE_REMOTE_RUN_FAILURE
                terminal_message = "all slots quarantined due to missing input files"
                break

            session_name = _build_session_name(run_id, bundle.job_index, attempt)
            _log_stage(
                f"job start job_id={bundle.job_id} attempt={attempt} session={session_name} slots={len(runnable_slots)}"
            )
            state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="SUBMITTED", attempt_no=attempt)
            state_store.append_event(
                run_id=run_id,
                job_id=bundle.job_id,
                level="INFO",
                stage="SUBMITTED",
                message=f"attempt={attempt} slots={len(runnable_slots)}",
            )

            for slot in runnable_slots:
                state_store.update_slot_task(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    state="LEASED",
                    attempt_no=attempt,
                    job_id=bundle.job_id,
                    account_id=bundle.account_id,
                    failure_reason=None,
                )
                state_store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="INFO",
                    stage="LEASED",
                    message=f"job={bundle.job_id} attempt={attempt}",
                )

            _mark_slot_lifecycle_stage(
                state_store=state_store,
                run_id=run_id,
                slots=runnable_slots,
                attempt_no=attempt,
                job_id=bundle.job_id,
                account_id=bundle.account_id,
                state="DOWNLOADING",
                message=f"job={bundle.job_id} attempt={attempt} leased_to_worker",
            )

            state_store.update_job_status(run_id=run_id, job_id=bundle.job_id, status="RUNNING", attempt_no=attempt)
            attempt_id = state_store.start_attempt(
                run_id=run_id,
                job_id=bundle.job_id,
                attempt_no=attempt,
                node=bundle.host_alias,
            )
            submitted_slurm_job_id: str | None = None

            def _worker_submitted(slurm_job_id: str, observed_node: str | None) -> None:
                nonlocal submitted_slurm_job_id
                submitted_slurm_job_id = slurm_job_id
                state_store.upsert_slurm_worker(
                    run_id=run_id,
                    worker_id=attempt_id,
                    job_id=bundle.job_id,
                    attempt_no=attempt,
                    account_id=bundle.account_id,
                    host_alias=bundle.host_alias,
                    slurm_job_id=slurm_job_id,
                    worker_state="SUBMITTED",
                    observed_node=observed_node,
                    slots_configured=config.slots_per_job,
                    backend=config.remote_execution_backend,
                    collect_probe_state=None,
                    marker_present=None,
                )

            def _worker_state_change(worker_state: str, observed_node: str | None) -> None:
                if submitted_slurm_job_id is None:
                    return
                state_store.upsert_slurm_worker(
                    run_id=run_id,
                    worker_id=attempt_id,
                    job_id=bundle.job_id,
                    attempt_no=attempt,
                    account_id=bundle.account_id,
                    host_alias=bundle.host_alias,
                    slurm_job_id=submitted_slurm_job_id,
                    worker_state=worker_state,
                    observed_node=observed_node,
                    slots_configured=config.slots_per_job,
                    backend=config.remote_execution_backend,
                    collect_probe_state=None,
                    marker_present=None,
                )
                if worker_state == "RUNNING":
                    _mark_slot_lifecycle_stage(
                        state_store=state_store,
                        run_id=run_id,
                        slots=runnable_slots,
                        attempt_no=attempt,
                        job_id=bundle.job_id,
                        account_id=bundle.account_id,
                        state="RUNNING",
                        message=f"job={bundle.job_id} attempt={attempt} worker_active",
                    )

            def _delete_after_upload() -> None:
                if config.delete_input_after_upload:
                    for slot in runnable_slots:
                        state_store.mark_slot_delete_pending(run_id=run_id, slot_id=slot.slot_id)
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="INFO",
                            stage="DELETE_PENDING",
                            message="uploaded; waiting for terminal materialization",
                        )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="UPLOADED")

            attempt_remote_cfg = remote_cfg
            refreshed_excluded_nodes, refreshed_bad_node_warnings = _load_active_bad_nodes()
            for warning in dict.fromkeys(refreshed_bad_node_warnings):
                _log_stage(warning)
                state_store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="WARN",
                    stage="BAD_NODES_INVALID",
                    message=warning,
                )
            if refreshed_excluded_nodes != attempt_remote_cfg.slurm_exclude_nodes:
                attempt_remote_cfg = replace(attempt_remote_cfg, slurm_exclude_nodes=refreshed_excluded_nodes)
            if refreshed_excluded_nodes:
                state_store.append_event(
                    run_id=run_id,
                    job_id=bundle.job_id,
                    level="INFO",
                    stage="BAD_NODE_EXCLUDE_ACTIVE",
                    message=(
                        f"account={bundle.account_id} host={bundle.host_alias} "
                        f"nodes={','.join(refreshed_excluded_nodes)} attempt={attempt}"
                    ),
                )

            try:
                result = run_remote_job_attempt(
                    config=attempt_remote_cfg,
                    run_id=run_id,
                    worker_id=attempt_id,
                    slot_inputs=[
                        SlotInput(
                            slot_id=slot.slot_id,
                            input_path=staged_input_paths.get(slot.slot_id, slot.input_path),
                        )
                        for slot in runnable_slots
                    ],
                    remote_job_dir=_join_remote_root(
                        _join_remote_root(_join_remote_root(remote_run_dir, bundle.account_id), bundle.job_id),
                        f"a{attempt}",
                    ),
                    local_job_dir=local_bundle_dir,
                    session_name=session_name,
                    on_upload_success=_delete_after_upload,
                    on_worker_submitted=_worker_submitted if config.remote_execution_backend == "slurm_batch" else None,
                    on_worker_state_change=_worker_state_change if config.remote_execution_backend == "slurm_batch" else None,
                )
            except Exception as exc:  # pragma: no cover
                result = RemoteJobAttemptResult(
                    success=False,
                    exit_code=EXIT_CODE_REMOTE_RUN_FAILURE,
                    session_name=session_name,
                    case_summary=_CaseExecutionSummary(success_cases=0, failed_cases=0, case_lines=[]),
                    message=f"unexpected exception: {exc}",
                    failed_case_lines=[],
                    failure_category="launch",
                )
            if result.slurm_job_id:
                final_worker_state = result.worker_terminal_state or ("COMPLETED" if result.success else "FAILED")
                state_store.upsert_slurm_worker(
                    run_id=run_id,
                    worker_id=attempt_id,
                    job_id=bundle.job_id,
                    attempt_no=attempt,
                    account_id=bundle.account_id,
                    host_alias=bundle.host_alias,
                    slurm_job_id=result.slurm_job_id,
                    worker_state=final_worker_state,
                    observed_node=result.observed_node,
                    slots_configured=config.slots_per_job,
                    backend=config.remote_execution_backend,
                    collect_probe_state=result.collect_probe_state,
                    marker_present=result.marker_present,
                )
                if final_worker_state == "LOST":
                    state_store.append_event(
                        run_id=run_id,
                        job_id=bundle.job_id,
                        level="WARN",
                        stage="WORKER_DEATH_DETECTED",
                        message=(
                            f"job={bundle.job_id} attempt={attempt} "
                            f"slurm_job_id={result.slurm_job_id} observed_node={result.observed_node or 'unknown'}"
                        ),
                    )
                if result.collect_probe_state is not None:
                    probe_message = (
                        f"job={bundle.job_id} attempt={attempt} slurm_job_id={result.slurm_job_id} "
                        f"probe={result.collect_probe_state} marker_present={result.marker_present}"
                    )
                    state_store.append_event(
                        run_id=run_id,
                        job_id=bundle.job_id,
                        level="INFO" if result.marker_present else "WARN",
                        stage="COLLECT_PROBE",
                        message=probe_message,
                    )
            _mark_slot_lifecycle_stage(
                state_store=state_store,
                run_id=run_id,
                slots=runnable_slots,
                attempt_no=attempt,
                job_id=bundle.job_id,
                account_id=bundle.account_id,
                state="UPLOADING",
                message=f"job={bundle.job_id} attempt={attempt} worker_collecting_results",
            )
            materialized_slot_ids = _materialize_slot_outputs(
                local_bundle_dir=local_bundle_dir,
                slots=runnable_slots,
                staged_input_paths=staged_input_paths,
                retain_aedtresults=config.retain_aedtresults,
            )
            if not result.success or result.failure_category:
                _materialize_slot_failure_artifacts(
                    local_bundle_dir=local_bundle_dir,
                    slots=runnable_slots,
                    staged_input_paths=staged_input_paths,
                )
            formatted_result_message = _format_failure_message(
                message=result.message,
                failure_category=result.failure_category,
            )

            try:
                cleanup_orphan_session(config=attempt_remote_cfg, session_name=session_name)
            except _WorkflowError as cleanup_error:
                state_store.append_event(
                    run_id=run_id,
                    job_id=bundle.job_id,
                    level="WARN",
                    stage="ORPHAN_CLEANUP_FAILED",
                    message=str(cleanup_error),
                )

            state_store.finish_attempt(
                run_id=run_id,
                attempt_id=attempt_id,
                exit_code=result.exit_code,
                error=None if result.success else formatted_result_message,
            )
            terminal_exit_code = result.exit_code
            terminal_message = formatted_result_message

            if (
                result.observed_node
                and _BAD_NODE_NO_SPACE_MARKER.lower() in formatted_result_message.lower()
            ):
                registered_entry, register_warnings = _register_bad_node_candidate(
                    node=result.observed_node,
                    reason=_BAD_NODE_NO_SPACE_MARKER,
                )
                for warning in dict.fromkeys(register_warnings):
                    _log_stage(warning)
                    state_store.append_event(
                        run_id=run_id,
                        job_id="__worker__",
                        level="WARN",
                        stage="BAD_NODES_INVALID",
                        message=warning,
                    )
                if registered_entry is not None:
                    state_store.append_event(
                        run_id=run_id,
                        job_id=bundle.job_id,
                        level="WARN",
                        stage="BAD_NODE_REGISTERED",
                        message=(
                            f"node={registered_entry['node']} reason={registered_entry['reason']} "
                            f"expires_at={registered_entry['expires_at']}"
                        ),
                    )

            if not result.success and result.failure_category:
                state_store.append_event(
                    run_id=run_id,
                    job_id=bundle.job_id,
                    level="ERROR",
                    stage=f"FAILURE_{result.failure_category.upper()}",
                    message=terminal_message,
                )
                return_path_stage = _classify_return_path_failure_stage(terminal_message)
                if return_path_stage is not None:
                    state_store.append_event(
                        run_id=run_id,
                        job_id=bundle.job_id,
                        level="ERROR",
                        stage=return_path_stage,
                        message=terminal_message,
                    )

            failed_indices = _parse_failed_case_indices(result.failed_case_lines, len(runnable_slots))
            if not result.success and not failed_indices:
                failed_indices = set(range(len(runnable_slots)))
            for index, slot in enumerate(runnable_slots):
                if slot.slot_id not in materialized_slot_ids:
                    failed_indices.add(index)
                    if result.success:
                        terminal_exit_code = EXIT_CODE_DOWNLOAD_FAILURE
                        terminal_message = "output materialization missing"

            retry_slots: list[SlotTaskRef] = []
            for index, slot in enumerate(runnable_slots):
                if index in failed_indices:
                    failure_message = terminal_message
                    effective_worker_requeue_limit = config.worker_requeue_limit
                    if result.failure_category == "launch_transient":
                        effective_worker_requeue_limit += max(1, config.launch_transient_same_account_retries)
                    should_requeue = (
                        slot.slot_id not in materialized_slot_ids
                        and slot.attempt_no <= effective_worker_requeue_limit
                    )
                    retry_limit = config.job_retry_count
                    if result.failure_category == "launch_transient":
                        retry_limit = max(retry_limit, config.launch_transient_same_account_retries)
                    if attempt <= retry_limit and result.failure_category not in {"storage"}:
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="FAILED")
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="RETRY_QUEUED",
                            attempt_no=attempt,
                            job_id=bundle.job_id,
                            account_id=bundle.account_id,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="WARN",
                            stage="RETRY_QUEUED",
                            message=f"attempt={attempt} reason={failure_message}",
                        )
                        if result.failure_category == "launch_transient":
                            state_store.append_event(
                                run_id=run_id,
                                job_id=bundle.job_id,
                                level="WARN",
                                stage="RETRY_BACKOFF",
                                message=(
                                    f"account={bundle.account_id} host={bundle.host_alias} "
                                    f"attempt={attempt} reason={failure_message}"
                                ),
                            )
                        retry_slots.append(
                            SlotTaskRef(
                                run_id=slot.run_id,
                                slot_id=slot.slot_id,
                                input_path=slot.input_path,
                                relative_path=slot.relative_path,
                                output_dir=slot.output_dir,
                                attempt_no=slot.attempt_no + 1,
                            )
                        )
                    elif should_requeue:
                        restore_source = staged_input_paths.get(slot.slot_id)
                        if restore_source is not None:
                            _restore_slot_input_from_stage(
                                source_path=restore_source,
                                target_path=slot.input_path,
                                ready_suffix=config.ready_sidecar_suffix,
                            )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="RETRY_QUEUED")
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="LEASE_EXPIRED",
                            attempt_no=slot.attempt_no + 1,
                            job_id=bundle.job_id,
                            account_id=None,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="WARN",
                            stage="LEASE_EXPIRED",
                            message=f"worker_requeue attempt_no={slot.attempt_no + 1} reason={failure_message}",
                        )
                        worker_requeue_slots.append(
                            SlotTaskRef(
                                run_id=slot.run_id,
                                slot_id=slot.slot_id,
                                input_path=slot.input_path,
                                relative_path=slot.relative_path,
                                output_dir=slot.output_dir,
                                attempt_no=slot.attempt_no + 1,
                            )
                        )
                        if result.failure_category == "launch_transient":
                            if on_account_cooldown is not None:
                                on_account_cooldown(bundle.account_id, bundle.host_alias, failure_message)
                    else:
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="FAILED")
                        state_store.quarantine_job(
                            run_id=run_id,
                            job_id=slot.slot_id,
                            attempt=attempt,
                            reason=failure_message,
                            exit_code=terminal_exit_code,
                        )
                        state_store.update_slot_task(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            state="QUARANTINED",
                            attempt_no=attempt,
                            job_id=bundle.job_id,
                            account_id=bundle.account_id,
                            failure_reason=failure_message,
                        )
                        state_store.append_slot_event(
                            run_id=run_id,
                            slot_id=slot.slot_id,
                            level="ERROR",
                            stage="QUARANTINED",
                            message=failure_message,
                        )
                        state_store.mark_ingest_state(input_path=str(slot.input_path), state="QUARANTINED")
                        _finalize_slot_input_cleanup(
                            config=config,
                            state_store=state_store,
                            run_id=run_id,
                            slot=slot,
                            deleted_slot_ids=deleted_slot_ids,
                        )
                        quarantined_slots += 1
                else:
                    state_store.update_slot_task(
                        run_id=run_id,
                        slot_id=slot.slot_id,
                        state="SUCCEEDED",
                        attempt_no=attempt,
                        job_id=bundle.job_id,
                        account_id=bundle.account_id,
                        failure_reason=None,
                    )
                    state_store.append_slot_event(
                        run_id=run_id,
                        slot_id=slot.slot_id,
                        level="INFO",
                        stage="SUCCEEDED",
                        message=f"job={bundle.job_id} attempt={attempt}",
                    )
                    state_store.mark_ingest_state(input_path=str(slot.input_path), state="SUCCEEDED")
                    state_store.record_artifact(run_id=run_id, job_id=bundle.job_id, artifact_root=str(slot.output_dir))
                    _finalize_slot_input_cleanup(
                        config=config,
                        state_store=state_store,
                        run_id=run_id,
                        slot=slot,
                        deleted_slot_ids=deleted_slot_ids,
                    )
                    success_slots += 1

            if not retry_slots:
                failed_slots = 0
                break
            pending_slots = retry_slots
            _log_stage(f"job retry scheduled job_id={bundle.job_id} next_attempt={attempt + 1}")

    quarantined = quarantined_slots > 0
    success = (failed_slots == 0) and (quarantined_slots == 0)
    final_status = "SUCCEEDED" if success else ("QUARANTINED" if quarantined else "FAILED")
    state_store.update_job_status(
        run_id=run_id,
        job_id=bundle.job_id,
        status=final_status,
        attempt_no=max(1, last_attempt_no),
        failure_reason=None if success else terminal_message,
    )
    state_store.append_event(
        run_id=run_id,
        job_id=bundle.job_id,
        level="INFO" if success else "ERROR",
        stage=final_status,
        message=terminal_message,
    )
    _log_stage(
        f"bundle result job_id={bundle.job_id} success_slots={success_slots} "
        f"failed_slots={failed_slots} quarantined_slots={quarantined_slots}"
    )
    return _BundleRuntimeOutcome(
        job_id=bundle.job_id,
        success=success,
        quarantined=quarantined,
        exit_code=EXIT_CODE_SUCCESS if success else terminal_exit_code,
        message=terminal_message,
        success_slots=success_slots,
        failed_slots=failed_slots,
        quarantined_slots=quarantined_slots,
        requeue_slots=tuple(worker_requeue_slots),
    )


def _build_slot_output_dir(*, output_root: Path, relative_path: Path) -> Path:
    return output_root / relative_path.parent / f"{relative_path.name}.out"


def _seed_slot_output_dir(*, slot: SlotTaskRef, seed_input_path: Path | None = None) -> None:
    slot.output_dir.mkdir(parents=True, exist_ok=True)
    source_input = seed_input_path if seed_input_path is not None else slot.input_path
    target_input = slot.output_dir / slot.input_path.name
    if source_input.exists() and not target_input.exists():
        shutil.copy2(source_input, target_input)


def _initialize_slot_output_dir(*, slot: SlotTaskRef, seed_input_path: Path | None = None) -> None:
    if slot.output_dir.exists():
        shutil.rmtree(slot.output_dir)
    _seed_slot_output_dir(slot=slot, seed_input_path=seed_input_path)


def _materialize_slot_outputs(
    *,
    local_bundle_dir: Path,
    slots: list[SlotTaskRef],
    staged_input_paths: dict[str, Path] | None = None,
    retain_aedtresults: bool = True,
) -> set[str]:
    materialized_slot_ids: set[str] = set()
    for index, slot in enumerate(slots, start=1):
        case_dir = local_bundle_dir / f"case_{index:02d}"
        if not case_dir.exists():
            continue
        seed_input_path = None if staged_input_paths is None else staged_input_paths.get(slot.slot_id)
        _initialize_slot_output_dir(slot=slot, seed_input_path=seed_input_path)
        copied_any = False
        for item in case_dir.iterdir():
            if item.name == "tmp":
                continue
            target_name = _rename_case_output_name(case_name=item.name, input_name=slot.input_path.name)
            if not retain_aedtresults and target_name.endswith(".aedtresults"):
                continue
            target_path = slot.output_dir / target_name
            if item.is_dir():
                shutil.copytree(item, target_path, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target_path)
            copied_any = True
        _copy_bundle_summary_artifacts(local_bundle_dir=local_bundle_dir, output_dir=slot.output_dir)
        if copied_any:
            materialized_slot_ids.add(slot.slot_id)
    return materialized_slot_ids


def _materialize_slot_failure_artifacts(
    *,
    local_bundle_dir: Path,
    slots: list[SlotTaskRef],
    staged_input_paths: dict[str, Path] | None = None,
) -> set[str]:
    artifact_names = (
        "bundle.exit.code",
        "remote_stdout.log",
        "remote_stderr.log",
        "remote_submission.log",
        "failure_category.txt",
        "failure_reason.txt",
        "failed_case_lines.txt",
    )
    artifact_paths = [local_bundle_dir / name for name in artifact_names if (local_bundle_dir / name).exists()]
    if not artifact_paths:
        return set()

    materialized_slot_ids: set[str] = set()
    for slot in slots:
        seed_input_path = None if staged_input_paths is None else staged_input_paths.get(slot.slot_id)
        _seed_slot_output_dir(slot=slot, seed_input_path=seed_input_path)

        copied_any = False
        for artifact_path in artifact_paths:
            shutil.copy2(artifact_path, slot.output_dir / artifact_path.name)
            copied_any = True

        bundle_exit_path = local_bundle_dir / "bundle.exit.code"
        output_exit_path = slot.output_dir / "exit.code"
        if not output_exit_path.exists() and bundle_exit_path.exists():
            shutil.copy2(bundle_exit_path, output_exit_path)
            copied_any = True

        output_run_log = slot.output_dir / "run.log"
        if not output_run_log.exists():
            remote_submission_log = local_bundle_dir / "remote_submission.log"
            remote_stderr_log = local_bundle_dir / "remote_stderr.log"
            failure_reason_path = local_bundle_dir / "failure_reason.txt"
            if remote_submission_log.exists():
                shutil.copy2(remote_submission_log, output_run_log)
                copied_any = True
            elif remote_stderr_log.exists():
                shutil.copy2(remote_stderr_log, output_run_log)
                copied_any = True
            elif failure_reason_path.exists():
                output_run_log.write_text(failure_reason_path.read_text(encoding="utf-8"), encoding="utf-8")
                copied_any = True
        _copy_bundle_summary_artifacts(local_bundle_dir=local_bundle_dir, output_dir=slot.output_dir)

        if copied_any:
            materialized_slot_ids.add(slot.slot_id)

    return materialized_slot_ids


def _copy_bundle_summary_artifacts(*, local_bundle_dir: Path, output_dir: Path) -> None:
    for name in ("case_summary.txt", "failed.count"):
        source_path = local_bundle_dir / name
        if source_path.exists():
            shutil.copy2(source_path, output_dir / name)


def _format_failure_message(*, message: str, failure_category: str | None) -> str:
    normalized_message = message.strip()
    if not failure_category:
        return normalized_message
    prefix = f"{failure_category.upper()}: "
    if normalized_message.startswith(prefix):
        return normalized_message
    return f"{prefix}{normalized_message}"


def _rename_case_output_name(*, case_name: str, input_name: str) -> str:
    if case_name.startswith("project.aedt"):
        return f"{input_name}{case_name[len('project.aedt'):]}"
    return case_name


def _done_path_for_input(path: Path) -> Path:
    return path.with_name(f"{path.name}.done")


def _delete_slot_input_files(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slots: list[SlotTaskRef],
    deleted_slot_ids: set[str],
) -> None:
    if not config.delete_input_after_upload:
        return
    for slot in slots:
        if slot.slot_id in deleted_slot_ids:
            continue
        _delete_slot_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            slot=slot,
        )
        deleted_slot_ids.add(slot.slot_id)


def _finalize_slot_input_cleanup(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slot: SlotTaskRef,
    deleted_slot_ids: set[str],
) -> None:
    if slot.slot_id in deleted_slot_ids:
        return
    if config.delete_input_after_upload and _slot_output_has_materialized_artifacts(
        output_dir=slot.output_dir,
        input_name=slot.input_path.name,
    ):
        _delete_slot_input_file(
            config=config,
            state_store=state_store,
            run_id=run_id,
            slot=slot,
        )
        deleted_slot_ids.add(slot.slot_id)
        return
    if (
        (not config.delete_input_after_upload)
        and config.rename_input_to_done_on_success
        and _slot_output_has_materialized_artifacts(
            output_dir=slot.output_dir,
            input_name=slot.input_path.name,
        )
    ):
        if _rename_slot_input_to_done(
            config=config,
            state_store=state_store,
            run_id=run_id,
            slot=slot,
        ):
            return

    retain_reason = "delete disabled"
    if config.delete_input_after_upload:
        retain_reason = "materialized output missing; input retained"
    elif config.rename_input_to_done_on_success:
        retain_reason = "rename to .done failed; input retained"
    state_store.mark_slot_delete_retained(run_id=run_id, slot_id=slot.slot_id)
    state_store.append_slot_event(
        run_id=run_id,
        slot_id=slot.slot_id,
        level="INFO" if not config.delete_input_after_upload else "WARN",
        stage="DELETE_RETAINED",
        message=retain_reason,
    )


def _rename_slot_input_to_done(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slot: SlotTaskRef,
) -> bool:
    path = slot.input_path
    ready_path = _ready_path_for_input(path, config.ready_sidecar_suffix)
    done_path = _done_path_for_input(path)
    for retry in range(3):
        try:
            if path.exists():
                if done_path.exists():
                    done_path.unlink()
                path.replace(done_path)
            if ready_path.exists():
                ready_path.unlink()
            state_store.mark_slot_delete_retained(run_id=run_id, slot_id=slot.slot_id)
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="INFO",
                stage="INPUT_DONE_RENAMED",
                message=str(done_path),
            )
            return True
        except OSError as exc:
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="WARN",
                stage="DONE_RENAME_RETRYING",
                message=f"retry={retry + 1} error={exc}",
            )
            time.sleep(0.2)
    return False


def _delete_slot_input_file(
    *,
    config: PipelineConfig,
    state_store: StateStore,
    run_id: str,
    slot: SlotTaskRef,
) -> None:
    path = slot.input_path
    ready_path = _ready_path_for_input(path, config.ready_sidecar_suffix)
    for retry in range(3):
        try:
            if path.exists():
                path.unlink()
            if ready_path.exists():
                ready_path.unlink()
            state_store.mark_slot_input_deleted(run_id=run_id, slot_id=slot.slot_id, retry_count=retry)
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="INFO",
                stage="INPUT_DELETED",
                message=f"retry={retry}",
            )
            state_store.mark_ingest_state(input_path=str(path), state="DELETED")
            return
        except OSError as exc:
            state_store.mark_slot_delete_retrying(run_id=run_id, slot_id=slot.slot_id, retry_count=retry + 1)
            state_store.append_slot_event(
                run_id=run_id,
                slot_id=slot.slot_id,
                level="WARN",
                stage="DELETE_RETRYING",
                message=str(exc),
            )
            time.sleep(0.2)

    quarantine_root = Path(config.delete_failed_quarantine_dir).expanduser().resolve()
    quarantine_target = quarantine_root / slot.relative_path
    quarantine_ready_target = _ready_path_for_input(quarantine_target, config.ready_sidecar_suffix)
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.exists():
            shutil.move(str(path), str(quarantine_target))
        if ready_path.exists():
            shutil.move(str(ready_path), str(quarantine_ready_target))
    finally:
        state_store.mark_slot_delete_quarantined(
            run_id=run_id,
            slot_id=slot.slot_id,
            retry_count=3,
            quarantine_path=str(quarantine_target),
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="ERROR",
            stage="DELETE_QUARANTINED",
            message=str(quarantine_target),
        )
        state_store.mark_ingest_state(input_path=str(path), state="DELETE_QUARANTINED")


def _slot_output_has_materialized_artifacts(*, output_dir: Path, input_name: str) -> bool:
    if not output_dir.exists():
        return False
    for item in output_dir.iterdir():
        if item.name == input_name:
            continue
        return True
    return False


def _mark_slot_lifecycle_stage(
    *,
    state_store: StateStore,
    run_id: str,
    slots: list[SlotTaskRef],
    attempt_no: int,
    job_id: str,
    account_id: str,
    state: str,
    message: str,
) -> None:
    for slot in slots:
        state_store.update_slot_task(
            run_id=run_id,
            slot_id=slot.slot_id,
            state=state,
            attempt_no=attempt_no,
            job_id=job_id,
            account_id=account_id,
            failure_reason=None,
        )
        state_store.append_slot_event(
            run_id=run_id,
            slot_id=slot.slot_id,
            level="INFO",
            stage=state,
            message=message,
        )


def _parse_failed_case_indices(case_lines: list[str], case_count: int) -> set[int]:
    failed_indices: set[int] = set()
    for line in case_lines:
        parts = line.split(":", 1)
        if len(parts) != 2:
            continue
        case_name = parts[0].strip()
        if not case_name.startswith("case_"):
            continue
        try:
            index = int(case_name.split("_", 1)[1])
        except ValueError:
            continue
        if 1 <= index <= case_count:
            failed_indices.add(index - 1)
    return failed_indices


def _local_capacity_lookup(*, account: AccountConfig, pending_buffer_per_account: int) -> AccountCapacitySnapshot:
    return AccountCapacitySnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        running_count=0,
        pending_count=0,
        allowed_submit=max(0, account.max_jobs),
    )


def _build_session_name(run_id: str, job_index: int, attempt: int) -> str:
    return f"aedt_{run_id}_{job_index:02d}_a{attempt}"


def _ensure_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def _join_remote_root(remote_root: str, suffix: str) -> str:
    return join_remote_root(remote_root, suffix)


def _run_with_retry(
    stage: str,
    retry_count: int,
    action,
    *,
    sleep_seconds: int = 10,
    retryable_exit_codes: set[int] | None = None,
) -> None:
    last_error: Exception | None = None
    for attempt in range(retry_count + 1):
        try:
            action()
            return
        except _WorkflowError as exc:
            last_error = exc
            if retryable_exit_codes is not None and exc.exit_code not in retryable_exit_codes:
                raise
            if attempt >= retry_count:
                raise
            time.sleep(sleep_seconds)
    if last_error is not None:
        raise last_error
    raise _WorkflowError(f"Unknown retry failure in {stage}.", exit_code=EXIT_CODE_REMOTE_RUN_FAILURE)


def _ready_path_for_input(input_path: Path, ready_suffix: str) -> Path:
    return Path(f"{input_path}{ready_suffix}")


def _lock_path_for_input(input_path: Path) -> Path:
    return Path(f"{input_path}.lock")


def _restore_slot_input_from_stage(*, source_path: Path, target_path: Path, ready_suffix: str) -> None:
    if not source_path.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    _ensure_ready_artifact(target_path, ready_suffix)


def _ensure_ready_artifact(input_path: Path, ready_suffix: str) -> _ReadyArtifactState:
    ready_path = _ready_path_for_input(input_path, ready_suffix)
    lock_path = _lock_path_for_input(input_path)
    if lock_path.exists():
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=ready_path.exists(),
            ready_mode="LOCKED",
            ready_error=f"lock present: {lock_path.name}",
            locked=True,
        )
    try:
        if not ready_path.exists():
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            ready_path.touch(exist_ok=False)
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=True,
            ready_mode="SIDECAR",
            locked=False,
        )
    except OSError as exc:
        return _ReadyArtifactState(
            ready_path=ready_path,
            ready_present=ready_path.exists(),
            ready_mode="INTERNAL_ONLY",
            ready_error=str(exc),
            locked=False,
        )


def _build_slot_id(*, relative_path: Path, mtime_ns: int, ready_mtime_ns: int | None = None) -> str:
    digest = sha1(f"{relative_path.as_posix()}:{mtime_ns}:{int(ready_mtime_ns or 0)}".encode("utf-8")).hexdigest()[:16]
    return f"w_{digest}"
