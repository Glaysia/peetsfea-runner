from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _run_prefix(namespace: str) -> str:
    normalized = namespace.strip()
    return f"{normalized}_" if normalized else ""


def _as_iso(dt: datetime | None = None) -> str:
    return (dt or datetime.now(tz=timezone.utc)).astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class _StoreData:
    runs: dict[str, dict[str, object]] = field(default_factory=dict)
    jobs: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    attempts: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    artifacts: list[dict[str, object]] = field(default_factory=list)
    events: list[dict[str, object]] = field(default_factory=list)
    file_lifecycle: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    worker_heartbeat: dict[tuple[str, str, int], dict[str, object]] = field(default_factory=dict)
    slot_tasks: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    slot_events: list[dict[str, object]] = field(default_factory=list)
    ingest_index: dict[str, dict[str, object]] = field(default_factory=dict)
    account_capacity_snapshots: list[dict[str, object]] = field(default_factory=list)
    account_readiness_snapshots: list[dict[str, object]] = field(default_factory=list)
    slurm_workers: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    quarantine_jobs: list[dict[str, object]] = field(default_factory=list)
    license_snapshots: list[dict[str, object]] = field(default_factory=list)
    license_control_state: dict[str, object] = field(default_factory=dict)
    license_account_states: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    node_resource_snapshots: list[dict[str, object]] = field(default_factory=list)
    worker_resource_snapshots: list[dict[str, object]] = field(default_factory=list)
    slot_resource_snapshots: list[dict[str, object]] = field(default_factory=list)
    resource_summary_snapshots: list[dict[str, object]] = field(default_factory=list)


_GLOBAL_LOCK = RLock()
_GLOBAL_STORES: dict[str, _StoreData] = {}


@dataclass(slots=True)
class StateStore:
    state_path: Path
    _lock: RLock = field(init=False, repr=False)
    _store_key: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.state_path = self.state_path.expanduser().resolve()
        self._lock = _GLOBAL_LOCK
        self._store_key = str(self.state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def _data(self) -> _StoreData:
        with self._lock:
            return _GLOBAL_STORES.setdefault(self._store_key, _StoreData())

    @staticmethod
    def _slot_task_row_to_dict(row: tuple[object, ...]) -> dict[str, object]:
        return {
            "run_id": str(row[0]),
            "slot_id": str(row[1]),
            "job_id": str(row[2]) if row[2] is not None else None,
            "account_id": str(row[3]) if row[3] is not None else None,
            "input_path": str(row[4]),
            "output_path": str(row[5]),
            "state": str(row[6]),
            "attempt_no": int(row[7] or 0),
            "failure_reason": str(row[8]) if row[8] is not None else None,
            "created_at": str(row[9]) if row[9] is not None else None,
            "updated_at": str(row[10]) if row[10] is not None else None,
            "lease_token": str(row[11]) if row[11] is not None else None,
            "worker_id": str(row[12]) if row[12] is not None else None,
            "slurm_job_id": str(row[13]) if row[13] is not None else None,
            "lease_started_at": str(row[14]) if row[14] is not None else None,
            "lease_heartbeat_ts": str(row[15]) if row[15] is not None else None,
            "lease_expires_at": str(row[16]) if row[16] is not None else None,
            "artifact_uploaded_at": str(row[17]) if row[17] is not None else None,
        }

    def _slot_task_dict(self, record: dict[str, object]) -> dict[str, object]:
        return {
            "run_id": str(record["run_id"]),
            "slot_id": str(record["slot_id"]),
            "job_id": record.get("job_id"),
            "account_id": record.get("account_id"),
            "input_path": str(record["input_path"]),
            "output_path": str(record["output_path"]),
            "state": str(record["state"]),
            "attempt_no": int(record.get("attempt_no") or 0),
            "failure_reason": record.get("failure_reason"),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "lease_token": record.get("lease_token"),
            "worker_id": record.get("worker_id"),
            "slurm_job_id": record.get("slurm_job_id"),
            "lease_started_at": record.get("lease_started_at"),
            "lease_heartbeat_ts": record.get("lease_heartbeat_ts"),
            "lease_expires_at": record.get("lease_expires_at"),
            "artifact_uploaded_at": record.get("artifact_uploaded_at"),
        }

    def initialize(self) -> None:
        _ = self._data

    def start_run(self, run_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            data = self._data
            data.runs[run_id] = {
                "run_id": run_id,
                "started_at": now,
                "finished_at": None,
                "state": "RUNNING",
                "summary": None,
            }

    def ensure_continuous_run(self, *, rotation_hours: int, namespace: str = "") -> str:
        now = datetime.now(tz=timezone.utc)
        now_iso = now.isoformat()
        prefix = _run_prefix(namespace)
        with self._lock:
            data = self._data
            running = [
                row
                for row in data.runs.values()
                if row.get("state") == "RUNNING" and (not prefix or str(row["run_id"]).startswith(prefix))
            ]
            running.sort(key=lambda row: str(row.get("started_at") or ""), reverse=True)
            if running:
                latest = running[0]
                started_at = datetime.fromisoformat(str(latest["started_at"]))
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                if now - started_at < timedelta(hours=rotation_hours):
                    return str(latest["run_id"])
                latest["finished_at"] = now_iso
                latest["state"] = "ROLLED"
                latest["summary"] = f"auto-rolled after {rotation_hours}h"
            new_run_id = f"{prefix}{now.strftime('%Y%m%d_%H%M%S')}"
            data.runs[new_run_id] = {
                "run_id": new_run_id,
                "started_at": now_iso,
                "finished_at": None,
                "state": "RUNNING",
                "summary": None,
            }
            return new_run_id

    def update_run_summary(self, *, run_id: str, summary: str) -> None:
        with self._lock:
            data = self._data
            if run_id in data.runs:
                data.runs[run_id]["summary"] = summary

    def finish_run(self, run_id: str, *, state: str, summary: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            data = self._data
            row = data.runs.setdefault(
                run_id,
                {"run_id": run_id, "started_at": now, "finished_at": None, "state": "RUNNING", "summary": None},
            )
            row["finished_at"] = now
            row["state"] = state
            row["summary"] = summary

    def create_job(self, *, run_id: str, job_id: str, input_path: str, output_path: str, account_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            data = self._data
            data.jobs[(run_id, job_id)] = {
                "run_id": run_id,
                "job_id": job_id,
                "input_path": input_path,
                "output_path": output_path,
                "account_id": account_id,
                "status": "PENDING",
                "created_at": now,
                "updated_at": now,
                "last_attempt_no": 0,
                "failure_reason": None,
            }
            data.file_lifecycle[(run_id, job_id)] = {
                "run_id": run_id,
                "job_id": job_id,
                "slot_id": None,
                "input_path": input_path,
                "input_deleted_at": None,
                "delete_retry_count": 0,
                "delete_final_state": "PENDING",
                "quarantine_path": None,
                "updated_at": now,
            }

    def create_slot_task(
        self,
        *,
        run_id: str,
        slot_id: str,
        input_path: str,
        output_path: str,
        account_id: str | None,
        state: str = "QUEUED",
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            data = self._data
            data.slot_tasks[(run_id, slot_id)] = {
                "run_id": run_id,
                "slot_id": slot_id,
                "job_id": None,
                "account_id": account_id,
                "input_path": input_path,
                "output_path": output_path,
                "state": state,
                "attempt_no": 0,
                "failure_reason": None,
                "created_at": now,
                "updated_at": now,
                "lease_token": None,
                "worker_id": None,
                "slurm_job_id": None,
                "lease_started_at": None,
                "lease_heartbeat_ts": None,
                "lease_expires_at": None,
                "artifact_uploaded_at": None,
            }
            data.file_lifecycle[(run_id, slot_id)] = {
                "run_id": run_id,
                "job_id": slot_id,
                "slot_id": slot_id,
                "input_path": input_path,
                "input_deleted_at": None,
                "delete_retry_count": 0,
                "delete_final_state": "PENDING",
                "quarantine_path": None,
                "updated_at": now,
            }

    def update_slot_task(
        self,
        *,
        run_id: str,
        slot_id: str,
        state: str,
        attempt_no: int | None = None,
        job_id: str | None = None,
        account_id: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.slot_tasks.get((run_id, slot_id))
            if record is None:
                return
            record["state"] = state
            record["updated_at"] = now
            record["failure_reason"] = failure_reason
            if attempt_no is not None:
                record["attempt_no"] = attempt_no
            if job_id is not None:
                record["job_id"] = job_id
            if account_id is not None:
                record["account_id"] = account_id

    def get_slot_task(self, *, run_id: str, slot_id: str) -> dict[str, object] | None:
        with self._lock:
            record = self._data.slot_tasks.get((run_id, slot_id))
            return None if record is None else self._slot_task_dict(record)

    def get_slot_task_by_lease_token(self, *, run_id: str, lease_token: str) -> dict[str, object] | None:
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] == run_id and record.get("lease_token") == lease_token:
                    return self._slot_task_dict(record)
        return None

    def acquire_slot_lease(
        self,
        *,
        run_id: str,
        worker_id: str,
        job_id: str,
        account_id: str,
        slurm_job_id: str | None,
        lease_token: str,
        lease_ttl_seconds: int,
    ) -> dict[str, object] | None:
        now = datetime.now(tz=timezone.utc)
        now_iso = now.isoformat()
        expires_at = (now + timedelta(seconds=max(1, lease_ttl_seconds))).isoformat()
        with self._lock:
            candidates = [
                record
                for record in self._data.slot_tasks.values()
                if record["run_id"] == run_id and record["state"] in {"QUEUED", "RETRY_QUEUED"}
            ]
            candidates.sort(key=lambda row: (str(row.get("created_at") or ""), str(row["slot_id"])))
            if not candidates:
                return None
            record = candidates[0]
            record["state"] = "LEASED"
            record["attempt_no"] = int(record.get("attempt_no") or 0) + 1
            record["job_id"] = job_id
            record["account_id"] = account_id
            record["updated_at"] = now_iso
            record["lease_token"] = lease_token
            record["worker_id"] = worker_id
            record["slurm_job_id"] = slurm_job_id
            record["lease_started_at"] = now_iso
            record["lease_heartbeat_ts"] = now_iso
            record["lease_expires_at"] = expires_at
            record["artifact_uploaded_at"] = None
            return self._slot_task_dict(record)

    def update_slot_lease_state(
        self,
        *,
        run_id: str,
        lease_token: str,
        state: str,
        failure_reason: str | None = None,
        artifact_uploaded: bool = False,
        extend_ttl_seconds: int | None = None,
    ) -> dict[str, object] | None:
        now = datetime.now(tz=timezone.utc)
        now_iso = now.isoformat()
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record.get("lease_token") != lease_token:
                    continue
                record["state"] = state
                record["updated_at"] = now_iso
                record["lease_heartbeat_ts"] = now_iso
                record["failure_reason"] = failure_reason
                if extend_ttl_seconds is not None:
                    record["lease_expires_at"] = (now + timedelta(seconds=max(1, extend_ttl_seconds))).isoformat()
                if artifact_uploaded:
                    record["artifact_uploaded_at"] = now_iso
                return self._slot_task_dict(record)
        return None

    def clear_slot_lease(
        self,
        *,
        run_id: str,
        lease_token: str,
        final_state: str,
        failure_reason: str | None = None,
    ) -> dict[str, object] | None:
        now = _utc_now_iso()
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record.get("lease_token") != lease_token:
                    continue
                record["state"] = final_state
                record["updated_at"] = now
                record["failure_reason"] = failure_reason
                record["lease_token"] = None
                record["lease_heartbeat_ts"] = None
                record["lease_expires_at"] = None
                return self._slot_task_dict(record)
        return None

    def requeue_worker_leases(self, *, run_id: str, worker_id: str, failure_reason: str) -> list[dict[str, object]]:
        now = _utc_now_iso()
        rows: list[dict[str, object]] = []
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record.get("worker_id") != worker_id:
                    continue
                if record["state"] not in {"LEASED", "DOWNLOADING", "RUNNING", "UPLOADING"}:
                    continue
                record["state"] = "RETRY_QUEUED"
                record["updated_at"] = now
                record["failure_reason"] = failure_reason
                record["lease_token"] = None
                record["lease_heartbeat_ts"] = None
                record["lease_expires_at"] = None
                record["artifact_uploaded_at"] = None
                rows.append(self._slot_task_dict(record))
        return rows

    def reap_expired_slot_leases(self, *, run_id: str, now: datetime | None = None) -> list[dict[str, object]]:
        reference = now or datetime.now(tz=timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        now_iso = reference.isoformat()
        rows: list[dict[str, object]] = []
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record["state"] not in {"LEASED", "DOWNLOADING", "RUNNING", "UPLOADING"}:
                    continue
                expires_at = record.get("lease_expires_at")
                if not expires_at or str(expires_at) > now_iso:
                    continue
                record["state"] = "RETRY_QUEUED"
                record["updated_at"] = now_iso
                record["failure_reason"] = "lease expired"
                record["lease_token"] = None
                record["lease_heartbeat_ts"] = None
                record["lease_expires_at"] = None
                record["artifact_uploaded_at"] = None
                rows.append(self._slot_task_dict(record))
        return rows

    def get_slot_throughput_score(self, *, run_id: str, account_id: str) -> tuple[int, int]:
        success = 0
        failed = 0
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record.get("account_id") != account_id:
                    continue
                if record["state"] == "SUCCEEDED":
                    success += 1
                elif record["state"] in {"FAILED", "QUARANTINED"}:
                    failed += 1
        return success, failed

    def count_active_jobs_by_account(self, *, run_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for record in self._data.jobs.values():
                if record["run_id"] != run_id or record["status"] not in {"PENDING", "SUBMITTED", "RUNNING"}:
                    continue
                account_id = str(record["account_id"])
                counts[account_id] = counts.get(account_id, 0) + 1
        return counts

    def upsert_slurm_worker(
        self,
        *,
        run_id: str,
        worker_id: str,
        job_id: str,
        attempt_no: int,
        account_id: str,
        host_alias: str,
        slurm_job_id: str,
        worker_state: str,
        slots_configured: int,
        backend: str,
        observed_node: str | None = None,
        tunnel_session_id: str | None = None,
        tunnel_state: str | None = None,
        heartbeat_ts: str | None = None,
        degraded_reason: str | None = None,
        collect_probe_state: str | None = None,
        marker_present: bool | None = None,
    ) -> None:
        now = _utc_now_iso()
        normalized_state = worker_state.strip().upper()
        normalized_tunnel_state = tunnel_state.strip().upper() if tunnel_state is not None else None
        with self._lock:
            data = self._data
            key = (run_id, worker_id)
            existing = data.slurm_workers.get(key, {})
            data.slurm_workers[key] = {
                "run_id": run_id,
                "worker_id": worker_id,
                "job_id": job_id,
                "attempt_no": attempt_no,
                "account_id": account_id,
                "host_alias": host_alias,
                "slurm_job_id": slurm_job_id,
                "worker_state": normalized_state,
                "observed_node": observed_node,
                "slots_configured": slots_configured,
                "backend": backend,
                "tunnel_session_id": tunnel_session_id if tunnel_session_id is not None else existing.get("tunnel_session_id"),
                "tunnel_state": normalized_tunnel_state if normalized_tunnel_state is not None else existing.get("tunnel_state"),
                "heartbeat_ts": heartbeat_ts if heartbeat_ts is not None else existing.get("heartbeat_ts"),
                "degraded_reason": degraded_reason if degraded_reason is not None else existing.get("degraded_reason"),
                "collect_probe_state": collect_probe_state if collect_probe_state is not None else existing.get("collect_probe_state"),
                "marker_present": marker_present if marker_present is not None else existing.get("marker_present"),
                "submitted_at": existing.get("submitted_at") or now,
                "started_at": existing.get("started_at") or (now if normalized_state in {"RUNNING", "IDLE_DRAINING", "COMPLETED", "FAILED", "LOST"} else None),
                "ended_at": now if normalized_state in {"COMPLETED", "FAILED", "LOST"} else existing.get("ended_at"),
                "last_seen_ts": now,
            }

    def get_slurm_worker(self, *, run_id: str, worker_id: str) -> dict[str, object] | None:
        rows = self.list_slurm_workers(run_id=run_id)
        for row in rows:
            if row["worker_id"] == worker_id:
                return row
        return None

    def update_slurm_worker_control_plane(
        self,
        *,
        run_id: str,
        worker_id: str,
        tunnel_state: str,
        tunnel_session_id: str | None = None,
        heartbeat_ts: str | None = None,
        observed_node: str | None = None,
        degraded_reason: str | None = None,
    ) -> None:
        now = heartbeat_ts or _utc_now_iso()
        with self._lock:
            record = self._data.slurm_workers.get((run_id, worker_id))
            if record is None:
                return
            record["tunnel_state"] = tunnel_state.strip().upper()
            record["tunnel_session_id"] = tunnel_session_id or record.get("tunnel_session_id")
            record["heartbeat_ts"] = now
            record["observed_node"] = observed_node or record.get("observed_node")
            record["degraded_reason"] = degraded_reason
            record["last_seen_ts"] = now

    def list_active_slurm_workers(self, *, run_id: str) -> list[dict[str, object]]:
        return self.list_slurm_workers(
            run_id=run_id,
            worker_states=("SUBMITTED", "PENDING", "RUNNING", "IDLE_DRAINING"),
        )

    def list_slurm_workers(
        self,
        *,
        run_id: str,
        worker_states: tuple[str, ...] | None = None,
    ) -> list[dict[str, object]]:
        with self._lock:
            rows = [dict(row) for row in self._data.slurm_workers.values() if row["run_id"] == run_id]
        if worker_states:
            wanted = {state.strip().upper() for state in worker_states}
            rows = [row for row in rows if str(row.get("worker_state") or "").upper() in wanted]
        rows.sort(key=lambda row: (str(row.get("submitted_at") or ""), str(row["worker_id"])))
        return rows

    def list_schedulable_slot_tasks(self, *, run_id: str) -> list[tuple[str, str, str, int]]:
        with self._lock:
            rows = [
                record
                for record in self._data.slot_tasks.values()
                if record["run_id"] == run_id and record["state"] in {"QUEUED", "RETRY_QUEUED"}
            ]
        rows.sort(key=lambda row: (str(row.get("created_at") or ""), str(row["slot_id"])))
        return [(str(row["slot_id"]), str(row["input_path"]), str(row["output_path"]), int(row.get("attempt_no") or 0)) for row in rows]

    def list_slot_tasks_by_states(self, *, run_id: str, states: tuple[str, ...]) -> list[dict[str, object]]:
        wanted = {state.strip().upper() for state in states}
        with self._lock:
            rows = [
                self._slot_task_dict(record)
                for record in self._data.slot_tasks.values()
                if record["run_id"] == run_id and str(record["state"]).upper() in wanted
            ]
        rows.sort(key=lambda row: (str(row.get("updated_at") or ""), str(row["slot_id"])))
        return rows

    def list_jobs_with_terminal_slurm_workers(self, *, run_id: str) -> list[dict[str, object]]:
        latest_by_job: dict[str, dict[str, object]] = {}
        for row in self.list_slurm_workers(run_id=run_id):
            job_id = str(row["job_id"])
            current = latest_by_job.get(job_id)
            if current is None or (int(row["attempt_no"]) > int(current["attempt_no"])) or (
                int(row["attempt_no"]) == int(current["attempt_no"]) and str(row.get("last_seen_ts") or "") > str(current.get("last_seen_ts") or "")
            ):
                latest_by_job[job_id] = row
        results: list[dict[str, object]] = []
        with self._lock:
            for job_id, row in latest_by_job.items():
                job = self._data.jobs.get((run_id, job_id))
                if job is None or str(job["status"]).upper() not in {"PENDING", "SUBMITTED", "RUNNING"}:
                    continue
                if str(row["worker_state"]).upper() not in {"FAILED", "LOST", "UNKNOWN"}:
                    continue
                results.append(
                    {
                        "job_id": job_id,
                        "attempt_no": int(row["attempt_no"]),
                        "slurm_job_id": str(row["slurm_job_id"]),
                        "worker_state": str(row["worker_state"]),
                        "observed_node": row.get("observed_node"),
                        "last_seen_ts": row.get("last_seen_ts"),
                    }
                )
        results.sort(key=lambda row: (str(row.get("last_seen_ts") or ""), str(row["job_id"])), reverse=True)
        return results

    def get_next_job_index(self, *, run_id: str) -> int:
        max_index = 0
        with self._lock:
            for key in self._data.jobs:
                stored_run_id, job_id = key
                if stored_run_id != run_id or not job_id.startswith("job_"):
                    continue
                suffix = job_id[4:]
                if suffix.isdigit():
                    max_index = max(max_index, int(suffix))
        return max_index + 1

    def mark_ingest_state(self, *, input_path: str, state: str) -> None:
        with self._lock:
            record = self._data.ingest_index.get(input_path)
            if record is not None:
                record["state"] = state

    def append_slot_event(self, *, run_id: str, slot_id: str, level: str, stage: str, message: str) -> None:
        self._data.slot_events.append(
            {
                "run_id": run_id,
                "slot_id": slot_id,
                "level": level,
                "stage": stage,
                "message": message,
                "ts": _utc_now_iso(),
            }
        )

    def count_slots_by_state(self, *, run_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id:
                    continue
                state = str(record["state"])
                counts[state] = counts.get(state, 0) + 1
        return counts

    def register_ingest_candidate(
        self,
        *,
        input_path: str,
        ready_path: str,
        ready_present: bool,
        ready_mode: str,
        ready_error: str | None,
        ready_mtime_ns: int | None,
        file_size: int,
        file_mtime_ns: int,
    ) -> bool:
        now = _utc_now_iso()
        with self._lock:
            existing = self._data.ingest_index.get(input_path)
            if existing is not None:
                prev_size = int(existing["file_size"])
                prev_mtime = int(existing["file_mtime_ns"])
                prev_state = str(existing["state"]).strip().upper()
                prev_ready_mtime_ns = int(existing.get("ready_mtime_ns") or 0)
                if prev_size == file_size and prev_mtime == file_mtime_ns:
                    should_rearm = (
                        ready_present
                        and ready_mtime_ns is not None
                        and ready_mtime_ns > prev_ready_mtime_ns
                        and prev_state in {"FAILED", "QUARANTINED", "DELETE_QUARANTINED"}
                    )
                    existing.update(
                        {
                            "ready_path": ready_path,
                            "ready_present": ready_present,
                            "ready_mode": ready_mode,
                            "ready_error": ready_error,
                            "ready_mtime_ns": ready_mtime_ns,
                            "discovered_at": now,
                        }
                    )
                    if should_rearm:
                        existing["last_rearmed_at"] = now
                        existing["state"] = "READY"
                        return True
                    return False
                existing.update(
                    {
                        "ready_path": ready_path,
                        "ready_present": ready_present,
                        "ready_mode": ready_mode,
                        "ready_error": ready_error,
                        "ready_mtime_ns": ready_mtime_ns,
                        "file_size": file_size,
                        "file_mtime_ns": file_mtime_ns,
                        "discovered_at": now,
                        "last_rearmed_at": None,
                        "state": "READY",
                    }
                )
                return True
            self._data.ingest_index[input_path] = {
                "input_path": input_path,
                "ready_path": ready_path,
                "ready_present": ready_present,
                "ready_mode": ready_mode,
                "ready_error": ready_error,
                "ready_mtime_ns": ready_mtime_ns,
                "file_size": file_size,
                "file_mtime_ns": file_mtime_ns,
                "discovered_at": now,
                "last_rearmed_at": None,
                "state": "READY",
            }
            return True

    def update_job_status(
        self,
        *,
        run_id: str,
        job_id: str,
        status: str,
        attempt_no: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.jobs.get((run_id, job_id))
            if record is None:
                return
            record["status"] = status
            record["updated_at"] = now
            record["failure_reason"] = failure_reason
            if attempt_no is not None:
                record["last_attempt_no"] = attempt_no

    def fail_active_job_from_rediscovered_worker(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt_no: int,
        failure_reason: str,
    ) -> list[tuple[str, str]]:
        now = _utc_now_iso()
        active_states = {"ASSIGNED", "LEASED", "DOWNLOADING", "UPLOADING", "RUNNING", "COLLECTING"}
        affected: list[tuple[str, str]] = []
        with self._lock:
            job = self._data.jobs.get((run_id, job_id))
            if job is not None and str(job["status"]).upper() in {"PENDING", "SUBMITTED", "RUNNING"}:
                job["status"] = "FAILED"
                job["updated_at"] = now
                job["last_attempt_no"] = attempt_no
                job["failure_reason"] = failure_reason
            for record in self._data.slot_tasks.values():
                if record["run_id"] != run_id or record.get("job_id") != job_id or record["state"] not in active_states:
                    continue
                record["state"] = "FAILED"
                record["updated_at"] = now
                record["attempt_no"] = attempt_no
                record["failure_reason"] = failure_reason
                affected.append((str(record["slot_id"]), str(record["input_path"])))
                ingest = self._data.ingest_index.get(str(record["input_path"]))
                if ingest is not None and str(ingest["state"]).upper() in {"READY", "QUEUED", "UPLOADED", "RETRY_QUEUED"}:
                    ingest["state"] = "FAILED"
        return affected

    def start_attempt(self, *, run_id: str, job_id: str, attempt_no: int, node: str | None = None) -> str:
        attempt_id = f"{job_id}_a{attempt_no}"
        with self._lock:
            self._data.attempts[(run_id, attempt_id)] = {
                "run_id": run_id,
                "attempt_id": attempt_id,
                "job_id": job_id,
                "attempt_no": attempt_no,
                "node": node,
                "started_at": _utc_now_iso(),
                "ended_at": None,
                "exit_code": None,
                "error": None,
            }
        return attempt_id

    def finish_attempt(
        self,
        *,
        run_id: str,
        attempt_id: str,
        exit_code: int,
        error: str | None = None,
    ) -> None:
        with self._lock:
            record = self._data.attempts.get((run_id, attempt_id))
            if record is None:
                return
            record["ended_at"] = _utc_now_iso()
            record["exit_code"] = exit_code
            record["error"] = error

    def record_artifact(
        self,
        *,
        run_id: str,
        job_id: str,
        artifact_root: str,
        size_bytes: int | None = None,
        checksum: str | None = None,
    ) -> None:
        self._data.artifacts.append(
            {
                "run_id": run_id,
                "job_id": job_id,
                "artifact_root": artifact_root,
                "size_bytes": size_bytes,
                "checksum": checksum,
                "created_at": _utc_now_iso(),
            }
        )

    def append_event(self, *, run_id: str, job_id: str, level: str, stage: str, message: str) -> None:
        self._data.events.append(
            {
                "run_id": run_id,
                "job_id": job_id,
                "level": level,
                "message": message,
                "stage": stage,
                "ts": _utc_now_iso(),
            }
        )

    def mark_input_deleted(self, *, run_id: str, job_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.file_lifecycle.get((run_id, job_id))
            if record is not None:
                record["input_deleted_at"] = now
                record["delete_retry_count"] = retry_count
                record["delete_final_state"] = "DELETED"
                record["updated_at"] = now

    def mark_slot_input_deleted(self, *, run_id: str, slot_id: str, retry_count: int) -> None:
        self.mark_input_deleted(run_id=run_id, job_id=slot_id, retry_count=retry_count)

    def mark_slot_delete_pending(self, *, run_id: str, slot_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.file_lifecycle.get((run_id, slot_id))
            if record is not None:
                record["delete_final_state"] = "DELETE_PENDING"
                record["updated_at"] = now

    def mark_slot_delete_retained(self, *, run_id: str, slot_id: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.file_lifecycle.get((run_id, slot_id))
            if record is not None:
                record["delete_final_state"] = "RETAINED"
                record["updated_at"] = now

    def mark_delete_retrying(self, *, run_id: str, job_id: str, retry_count: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.file_lifecycle.get((run_id, job_id))
            if record is not None:
                record["delete_retry_count"] = retry_count
                record["delete_final_state"] = "DELETE_RETRYING"
                record["updated_at"] = now

    def mark_slot_delete_retrying(self, *, run_id: str, slot_id: str, retry_count: int) -> None:
        self.mark_delete_retrying(run_id=run_id, job_id=slot_id, retry_count=retry_count)

    def mark_delete_quarantined(self, *, run_id: str, job_id: str, retry_count: int, quarantine_path: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            record = self._data.file_lifecycle.get((run_id, job_id))
            if record is not None:
                record["delete_retry_count"] = retry_count
                record["delete_final_state"] = "DELETE_QUARANTINED"
                record["quarantine_path"] = quarantine_path
                record["updated_at"] = now

    def mark_slot_delete_quarantined(
        self,
        *,
        run_id: str,
        slot_id: str,
        retry_count: int,
        quarantine_path: str,
    ) -> None:
        self.mark_delete_quarantined(
            run_id=run_id,
            job_id=slot_id,
            retry_count=retry_count,
            quarantine_path=quarantine_path,
        )

    def quarantine_job(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt: int,
        reason: str,
        exit_code: int,
    ) -> None:
        self._data.quarantine_jobs.append(
            {
                "run_id": run_id,
                "job_id": job_id,
                "attempt": attempt,
                "quarantined_at": _utc_now_iso(),
                "reason": reason,
                "exit_code": exit_code,
            }
        )

    def upsert_worker_heartbeat(
        self,
        *,
        service_name: str,
        host: str,
        pid: int,
        run_id: str | None,
        status: str,
    ) -> None:
        self._data.worker_heartbeat[(service_name, host, pid)] = {
            "service_name": service_name,
            "host": host,
            "pid": pid,
            "last_seen_ts": _utc_now_iso(),
            "run_id": run_id,
            "status": status,
        }

    def record_account_capacity_snapshot(
        self,
        *,
        account_id: str,
        host: str,
        running_count: int,
        pending_count: int,
        allowed_submit: int,
    ) -> None:
        self._data.account_capacity_snapshots.append(
            {
                "account_id": account_id,
                "host": host,
                "running_count": running_count,
                "pending_count": pending_count,
                "allowed_submit": allowed_submit,
                "ts": _utc_now_iso(),
            }
        )

    def record_account_readiness_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.account_readiness_snapshots.append(payload)

    def record_license_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.license_snapshots.append(payload)

    def get_latest_license_snapshot(self) -> dict[str, object] | None:
        if not self._data.license_snapshots:
            return None
        return dict(sorted(self._data.license_snapshots, key=lambda row: str(row.get("ts") or ""))[-1])

    def upsert_license_control_state(self, **payload: object) -> None:
        self._data.license_control_state = dict(payload)

    def get_license_control_state(self) -> dict[str, object]:
        if not self._data.license_control_state:
            return {
                "desired_total_active_slots": 0,
                "effective_in_use": None,
                "ceiling": 520,
                "last_polled_ts": None,
                "last_adjusted_ts": None,
                "poll_source_host": "",
                "status": "UNKNOWN",
                "error": None,
            }
        row = dict(self._data.license_control_state)
        row.setdefault("desired_total_active_slots", 0)
        row.setdefault("effective_in_use", None)
        row.setdefault("ceiling", 520)
        row.setdefault("last_polled_ts", None)
        row.setdefault("last_adjusted_ts", None)
        row.setdefault("poll_source_host", "")
        row.setdefault("status", "UNKNOWN")
        row.setdefault("error", None)
        return row

    def upsert_license_account_state(
        self,
        *,
        run_id: str,
        account_id: str,
        host: str,
        ready: bool,
        queued_slots: int,
        active_slots: int,
        max_active_slots: int,
        ts: str | None = None,
    ) -> None:
        self._data.license_account_states[(run_id, account_id)] = {
            "run_id": run_id,
            "account_id": account_id,
            "host": host,
            "ready": ready,
            "queued_slots": max(0, queued_slots),
            "active_slots": max(0, active_slots),
            "max_active_slots": max(0, max_active_slots),
            "ts": ts or _utc_now_iso(),
        }

    def clear_license_account_states(self, *, run_id: str) -> None:
        with self._lock:
            keys = [key for key in self._data.license_account_states if key[0] == run_id]
            for key in keys:
                del self._data.license_account_states[key]

    def list_license_account_states(self, *, max_age_seconds: int) -> list[dict[str, object]]:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(seconds=max(1, max_age_seconds))
        rows = []
        with self._lock:
            for record in self._data.license_account_states.values():
                ts_raw = record.get("ts")
                if ts_raw is None:
                    continue
                ts = datetime.fromisoformat(str(ts_raw))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    rows.append(dict(record))
        rows.sort(key=lambda row: (str(row["account_id"]), str(row["run_id"])))
        return rows

    def record_node_resource_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.node_resource_snapshots.append(payload)

    def list_latest_low_tmp_nodes(self, *, tmp_free_threshold_mb: int) -> list[tuple[str, int, str]]:
        latest: dict[str, dict[str, object]] = {}
        with self._lock:
            for row in self._data.node_resource_snapshots:
                host = str(row.get("host") or "")
                if not host or row.get("tmp_free_mb") is None:
                    continue
                current = latest.get(host)
                if current is None or str(row.get("ts") or "") > str(current.get("ts") or ""):
                    latest[host] = row
        results = []
        for host, row in sorted(latest.items()):
            tmp_free = int(row.get("tmp_free_mb") or 0)
            if tmp_free < tmp_free_threshold_mb:
                results.append((host, tmp_free, str(row.get("ts") or "")))
        return results

    def record_worker_resource_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.worker_resource_snapshots.append(payload)

    def record_slot_resource_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.slot_resource_snapshots.append(payload)

    def record_resource_summary_snapshot(self, **payload: object) -> None:
        payload = dict(payload)
        payload.setdefault("ts", _utc_now_iso())
        self._data.resource_summary_snapshots.append(payload)

    def count_active_slots(self, *, run_id: str) -> int:
        active_states = {"ASSIGNED", "LEASED", "DOWNLOADING", "UPLOADING", "RUNNING", "COLLECTING"}
        with self._lock:
            return sum(
                1
                for record in self._data.slot_tasks.values()
                if record["run_id"] == run_id and record["state"] in active_states
            )

    def count_active_slots_by_account(self, *, run_id: str) -> dict[str, int]:
        active_states = {"ASSIGNED", "LEASED", "DOWNLOADING", "UPLOADING", "RUNNING", "COLLECTING"}
        counts: dict[str, int] = {}
        with self._lock:
            for record in self._data.slot_tasks.values():
                account_id = record.get("account_id")
                if record["run_id"] != run_id or account_id is None or record["state"] not in active_states:
                    continue
                key = str(account_id)
                counts[key] = counts.get(key, 0) + 1
        return counts
