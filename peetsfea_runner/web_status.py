from __future__ import annotations

import json
import os
import secrets
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .pipeline import (
    LeaseServerContext,
    _finalize_slot_input_cleanup,
    materialize_pulled_slot_artifact,
    slot_task_ref_from_record,
)
from .license_policy import (
    LICENSE_FEATURE,
    LICENSE_FEATURE_CEILING,
    LICENSE_FEATURE_POLL_TTL_SECONDS,
    LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS,
    LICENSE_POLL_SOURCE_HOST,
)
from .state_store import StateStore
from .version import get_version

try:
    from .license_gate import check_license_gate
except ImportError:
    check_license_gate = None

APP_VERSION = get_version()


def _first_str_param(params: dict[str, list[str]], key: str) -> str | None:
    values = params.get(key) or []
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _is_loopback_client(host: str | None) -> bool:
    return host in {"127.0.0.1", "::1", "localhost"}


def _safe_relpath(path: Path, base: Path | None) -> str:
    if base is None:
        return path.name
    try:
        return str(path.relative_to(base))
    except ValueError:
        return path.name


def _workspace_root_from_context(lease_context: LeaseServerContext | None) -> Path | None:
    if lease_context is None:
        return None
    pull_workspace_path = str(getattr(lease_context, "pull_workspace_path", "") or "").strip()
    if not pull_workspace_path:
        return None
    return Path(pull_workspace_path).expanduser().resolve()


def _workspace_relative_path(
    path: Path,
    *,
    workspace_root: Path | None,
    fallback_root: Path | None,
    durable_prefix: str,
) -> str | None:
    if workspace_root is not None:
        workspace_relative = _relpath_under_root(path, workspace_root)
        if workspace_relative is not None:
            return workspace_relative
    if fallback_root is None:
        return None
    legacy_relative = _relpath_under_root(path, fallback_root)
    if legacy_relative is None:
        return None
    durable_root = durable_prefix.strip().strip("/")
    return f"{durable_root}/{legacy_relative}" if durable_root else legacy_relative


def _relpath_under_root(path: Path, root: Path) -> str | None:
    try:
        return str(path.expanduser().relative_to(root.expanduser()))
    except ValueError:
        pass
    try:
        return str(path.expanduser().resolve().relative_to(root.expanduser().resolve()))
    except ValueError:
        return None


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def _gate_value(result: object, key: str) -> object:
    if isinstance(result, dict):
        return result.get(key)
    return getattr(result, key, None)


def _context_license_ceiling(lease_context: LeaseServerContext | None) -> int:
    if lease_context is None:
        return LICENSE_FEATURE_CEILING
    configured = getattr(lease_context, "license_ceiling", LICENSE_FEATURE_CEILING)
    return int(configured or LICENSE_FEATURE_CEILING)


def _context_license_feature(lease_context: LeaseServerContext | None) -> str:
    return LICENSE_FEATURE


def _license_in_use_from_gate(result: object) -> object:
    for key in ("license_in_use", "electronics_desktop_in_use"):
        value = _gate_value(result, key)
        if value is not None:
            return value
    return None


def _license_feature_from_gate(result: object) -> str:
    feature = str(_gate_value(result, "license_feature") or "").strip()
    return feature or LICENSE_FEATURE


def _evaluate_license_gate(lease_context: LeaseServerContext | None) -> dict[str, object]:
    license_ceiling = _context_license_ceiling(lease_context)
    license_feature = _context_license_feature(lease_context)
    if lease_context is not None and not bool(getattr(lease_context, "license_gate_enabled", True)):
        return {
            "open": True,
            "fail_open": False,
            "reason": "license_gate_disabled",
            "license_feature": license_feature,
            "license_in_use": None,
            "license_ceiling": license_ceiling,
        }
    if check_license_gate is None:
        return {
            "open": True,
            "fail_open": True,
            "reason": "license_gate_helper_unavailable",
            "license_feature": license_feature,
            "license_in_use": None,
            "license_ceiling": license_ceiling,
        }
    try:
        kwargs: dict[str, object] = {}
        if lease_context is not None:
            kwargs = {
                "ssh_config_path": str(getattr(lease_context, "ssh_config_path", "") or ""),
                "timeout_seconds": int(
                    getattr(lease_context, "license_query_timeout_seconds", LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS)
                    or LICENSE_FEATURE_QUERY_TIMEOUT_SECONDS
                ),
                "ttl_seconds": int(
                    getattr(lease_context, "license_cache_ttl_seconds", LICENSE_FEATURE_POLL_TTL_SECONDS)
                    or LICENSE_FEATURE_POLL_TTL_SECONDS
                ),
                "ceiling": license_ceiling,
                "source_host": str(
                    getattr(lease_context, "license_source_host", LICENSE_POLL_SOURCE_HOST) or LICENSE_POLL_SOURCE_HOST
                ),
                "poll_env": str(getattr(lease_context, "license_poll_env", "") or ""),
                "poll_command": str(getattr(lease_context, "license_poll_command", "") or ""),
            }
        result = check_license_gate(**kwargs)
    except TypeError:
        result = check_license_gate()
    except Exception as exc:  # noqa: BLE001 - license gate failures are fail-open by design.
        return {
            "open": True,
            "fail_open": True,
            "reason": f"{type(exc).__name__}: {exc}",
            "license_feature": license_feature,
            "license_in_use": None,
            "license_ceiling": license_ceiling,
        }
    if isinstance(result, bool):
        return {
            "open": result,
            "fail_open": False,
            "reason": None,
            "license_feature": license_feature,
            "license_in_use": None,
            "license_ceiling": license_ceiling,
        }
    fail_open = bool(_gate_value(result, "fail_open"))
    is_open = _gate_value(result, "open")
    if is_open is None:
        is_open = _gate_value(result, "is_open")
    if is_open is None:
        is_open = _gate_value(result, "lease_allowed")
    if is_open is None:
        state = str(_gate_value(result, "license_gate") or _gate_value(result, "gate") or "").strip().lower()
        is_open = state not in {"license_closed", "closed"}
    return {
        "open": bool(is_open) or fail_open,
        "fail_open": fail_open,
        "reason": _gate_value(result, "reason") or _gate_value(result, "error"),
        "license_feature": _license_feature_from_gate(result),
        "license_in_use": _license_in_use_from_gate(result),
        "license_ceiling": _gate_value(result, "license_ceiling") or license_ceiling,
    }


def _append_license_gate_event(
    store: StateStore,
    *,
    run_id: str,
    worker_id: str,
    level: str,
    stage: str,
    message: str,
) -> None:
    append_event = getattr(store, "append_event", None)
    if append_event is None:
        return
    append_event(
        run_id=run_id,
        job_id="__license__",
        level=level,
        stage=stage,
        message=f"worker_id={worker_id} {message}",
    )


def make_status_handler(
    *,
    state_store: StateStore | None = None,
    lease_context: LeaseServerContext | None = None,
):
    store = state_store or StateStore(Path("./peetsfea_runner.state").expanduser().resolve())
    store.initialize()

    class StatusHandler(BaseHTTPRequestHandler):
        server_version = f"peetsfea/{APP_VERSION}"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def _send_json(self, payload: object, status: int = 200) -> None:
            if isinstance(payload, dict) and "version" not in payload:
                payload = {"version": APP_VERSION, **payload}
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, body: bytes, *, content_type: str, status: int = 200) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_raw_body(self) -> bytes:
            content_length = int(self.headers.get("Content-Length", "0"))
            return self.rfile.read(content_length) if content_length > 0 else b""

        def _read_json_body(self) -> dict[str, object]:
            raw = self._read_raw_body()
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def _reject_non_loopback(self) -> bool:
            client_host = self.client_address[0] if self.client_address else None
            if _is_loopback_client(client_host):
                return False
            self._send_json({"error": "forbidden"}, status=403)
            return True

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            if parsed.path == "/health":
                self._send_json({"ok": True, "service": "peetsfea-runner"})
                return

            if parsed.path == "/internal/leases/input":
                if self._reject_non_loopback():
                    return
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                run_id = _first_str_param(params, "run_id")
                lease_token = _first_str_param(params, "lease_token")
                if not run_id or not lease_token:
                    self._send_json({"error": "run_id_and_lease_token_required"}, status=400)
                    return
                slot_record = store.get_slot_task_by_lease_token(run_id=run_id, lease_token=lease_token)
                if slot_record is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                input_path = Path(str(slot_record["input_path"])).expanduser().resolve()
                if not input_path.exists():
                    self._send_json({"error": "input_not_found"}, status=404)
                    return
                store.update_slot_lease_state(
                    run_id=run_id,
                    lease_token=lease_token,
                    state="DOWNLOADING",
                    extend_ttl_seconds=lease_context.lease_ttl_seconds,
                )
                self._send_bytes(input_path.read_bytes(), content_type="application/octet-stream")
                return

            self._send_json({"error": "not_found"}, status=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not parsed.path.startswith("/internal/"):
                self._send_json({"error": "not_found"}, status=404)
                return
            if self._reject_non_loopback():
                return
            params = parse_qs(parsed.query)
            if parsed.path == "/internal/leases/artifact":
                payload: dict[str, object] = {}
            else:
                try:
                    payload = self._read_json_body()
                except json.JSONDecodeError as exc:
                    self._send_json({"error": "invalid_json", "detail": str(exc)}, status=400)
                    return
            run_id = str(payload.get("run_id") or _first_str_param(params, "run_id") or "").strip()
            worker_id = str(payload.get("worker_id") or "").strip()

            if parsed.path == "/internal/leases/request":
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                worker = store.get_slurm_worker(run_id=run_id, worker_id=worker_id)
                if worker is None:
                    self._send_json({"error": "worker_not_found"}, status=404)
                    return
                account_id = str(payload.get("account_id") or worker.get("account_id") or "").strip()
                if not account_id:
                    self._send_json({"error": "account_id_required"}, status=400)
                    return
                gate = _evaluate_license_gate(lease_context)
                license_feature = str(gate.get("license_feature") or LICENSE_FEATURE)
                license_in_use = gate.get("license_in_use")
                license_ceiling = int(gate.get("license_ceiling") or LICENSE_FEATURE_CEILING)
                if not gate["open"]:
                    _append_license_gate_event(
                        store,
                        run_id=run_id,
                        worker_id=worker_id,
                        level="INFO",
                        stage="LICENSE_GATE_CLOSED",
                        message=(
                            f"license_feature={license_feature} "
                            f"license_in_use={license_in_use} license_ceiling={license_ceiling}"
                        ),
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "lease_available": False,
                            "license_gate": "license_closed",
                            "license_feature": license_feature,
                            "license_in_use": license_in_use,
                            "license_ceiling": license_ceiling,
                        }
                    )
                    return
                if gate["fail_open"]:
                    _append_license_gate_event(
                        store,
                        run_id=run_id,
                        worker_id=worker_id,
                        level="WARN",
                        stage="LICENSE_GATE_FAIL_OPEN",
                        message=f"reason={gate.get('reason') or 'unknown'}",
                    )
                lease_token = secrets.token_urlsafe(24)
                slot_record = store.acquire_slot_lease(
                    run_id=run_id,
                    worker_id=worker_id,
                    job_id=worker_id,
                    account_id=account_id,
                    slurm_job_id=str(payload.get("slurm_job_id") or worker.get("slurm_job_id") or "").strip() or None,
                    lease_token=lease_token,
                    lease_ttl_seconds=lease_context.lease_ttl_seconds,
                )
                if slot_record is None:
                    self._send_json({"ok": True, "lease_available": False})
                    return
                store.mark_ingest_state(input_path=str(slot_record["input_path"]), state="LEASED")
                store.append_slot_event(
                    run_id=run_id,
                    slot_id=str(slot_record["slot_id"]),
                    level="INFO",
                    stage="LEASED",
                    message=f"worker_id={worker_id} slurm_job_id={slot_record.get('slurm_job_id') or 'unknown'}",
                )
                storage_mode = str(lease_context.worker_storage.storage_mode).strip() or "payload"
                input_path = Path(str(slot_record["input_path"]))
                output_path = Path(str(slot_record["output_path"]))
                if storage_mode == "sshfs_direct":
                    workspace_root = _workspace_root_from_context(lease_context)
                    workspace_input_relpath = _workspace_relative_path(
                        input_path,
                        workspace_root=workspace_root,
                        fallback_root=Path(lease_context.input_queue_dir),
                        durable_prefix="input_queue",
                    )
                    workspace_output_relpath = _workspace_relative_path(
                        output_path,
                        workspace_root=workspace_root,
                        fallback_root=Path(lease_context.output_root_dir),
                        durable_prefix="output",
                    )
                    if workspace_input_relpath is None or workspace_output_relpath is None:
                        store.clear_slot_lease(
                            run_id=run_id,
                            lease_token=lease_token,
                            final_state="RETRY_QUEUED",
                            failure_reason="lease path outside configured root",
                        )
                        store.mark_ingest_state(input_path=str(slot_record["input_path"]), state="RETRY_QUEUED")
                        self._send_json({"error": "lease_path_outside_root"}, status=409)
                        return
                    input_relpath = workspace_input_relpath
                    output_relpath = workspace_output_relpath
                else:
                    input_relpath = _safe_relpath(
                        input_path.resolve(),
                        Path(lease_context.input_queue_dir).expanduser().resolve(),
                    )
                    output_relpath = _safe_relpath(
                        output_path.resolve(),
                        Path(lease_context.output_root_dir).expanduser().resolve(),
                    )
                self._send_json(
                    {
                        "ok": True,
                        "lease_available": True,
                        "lease_token": lease_token,
                        "slot_id": str(slot_record["slot_id"]),
                        "attempt_no": int(slot_record.get("attempt_no") or 0),
                        "input_name": input_path.name,
                        "input_relpath": input_relpath,
                        "output_relpath": output_relpath,
                        "storage_mode": storage_mode,
                    }
                )
                return

            if parsed.path == "/internal/leases/heartbeat":
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                lease_token = str(payload.get("lease_token") or "").strip()
                if not run_id or not lease_token:
                    self._send_json({"error": "run_id_and_lease_token_required"}, status=400)
                    return
                current = store.get_slot_task_by_lease_token(run_id=run_id, lease_token=lease_token)
                if current is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                slot_state = str(payload.get("slot_state") or current.get("state") or "LEASED").strip().upper()
                updated = store.update_slot_lease_state(
                    run_id=run_id,
                    lease_token=lease_token,
                    state=slot_state,
                    failure_reason=str(current.get("failure_reason") or "").strip() or None,
                    extend_ttl_seconds=lease_context.lease_ttl_seconds,
                )
                if updated is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                self._send_json({"ok": True, "slot_state": slot_state})
                return

            if parsed.path == "/internal/leases/artifact":
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                lease_token = _first_str_param(params, "lease_token")
                if not run_id or not lease_token:
                    self._send_json({"error": "run_id_and_lease_token_required"}, status=400)
                    return
                slot_record = store.get_slot_task_by_lease_token(run_id=run_id, lease_token=lease_token)
                if slot_record is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                body = self._read_raw_body()
                if not body:
                    self._send_json({"error": "artifact_body_required"}, status=400)
                    return
                slot = slot_task_ref_from_record(run_id=run_id, slot_record=slot_record)
                tmp_handle = tempfile.NamedTemporaryFile(prefix="peetsfea-artifact-", suffix=".tgz", delete=False)
                tmp_path = Path(tmp_handle.name)
                try:
                    tmp_handle.write(body)
                    tmp_handle.close()
                    materialized = materialize_pulled_slot_artifact(
                        slot=slot,
                        archive_path=tmp_path,
                        retain_aedtresults=lease_context.retain_aedtresults,
                    )
                finally:
                    try:
                        tmp_handle.close()
                    except Exception:
                        pass
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                store.update_slot_lease_state(
                    run_id=run_id,
                    lease_token=lease_token,
                    state="UPLOADING",
                    artifact_uploaded=True,
                    extend_ttl_seconds=lease_context.lease_ttl_seconds,
                )
                store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="INFO",
                    stage="ARTIFACT_UPLOADED",
                    message=f"worker_id={slot_record.get('worker_id') or 'unknown'} materialized={materialized}",
                )
                self._send_json({"ok": True, "materialized": materialized, "state": "UPLOADING"})
                return

            if parsed.path == "/internal/leases/complete":
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                lease_token = str(payload.get("lease_token") or "").strip()
                if not run_id or not lease_token:
                    self._send_json({"error": "run_id_and_lease_token_required"}, status=400)
                    return
                slot_record = store.get_slot_task_by_lease_token(run_id=run_id, lease_token=lease_token)
                if slot_record is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                storage_mode = str(lease_context.worker_storage.storage_mode).strip() or "payload"
                if storage_mode == "payload" and not slot_record.get("artifact_uploaded_at"):
                    self._send_json({"error": "artifact_missing"}, status=409)
                    return
                output_dir = Path(str(slot_record["output_path"])).expanduser().resolve()
                if storage_mode == "sshfs_direct":
                    output_materialized = _to_bool(payload.get("output_materialized"))
                    if not output_materialized:
                        self._send_json({"error": "output_not_materialized"}, status=409)
                        return
                    workspace_root = _workspace_root_from_context(lease_context)
                    expected_output_relpath = _workspace_relative_path(
                        output_dir,
                        workspace_root=workspace_root,
                        fallback_root=Path(lease_context.output_root_dir),
                        durable_prefix="output",
                    )
                    reported_output_relpath = str(payload.get("output_relpath") or "").strip()
                    if expected_output_relpath is None:
                        self._send_json({"error": "output_path_outside_root"}, status=409)
                        return
                    if reported_output_relpath and reported_output_relpath != expected_output_relpath:
                        self._send_json({"error": "output_relpath_mismatch"}, status=409)
                        return
                if not output_dir.is_dir():
                    self._send_json({"error": "output_directory_missing"}, status=409)
                    return
                slot = slot_task_ref_from_record(run_id=run_id, slot_record=slot_record)
                store.clear_slot_lease(run_id=run_id, lease_token=lease_token, final_state="SUCCEEDED")
                store.mark_ingest_state(input_path=str(slot.input_path), state="SUCCEEDED")
                store.record_artifact(run_id=run_id, job_id=slot.slot_id, artifact_root=str(slot.output_dir))
                _finalize_slot_input_cleanup(
                    config=lease_context,
                    state_store=store,
                    run_id=run_id,
                    slot=slot,
                    deleted_slot_ids=set(),
                )
                store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="INFO",
                    stage="SUCCEEDED",
                    message=f"worker_id={slot_record.get('worker_id') or 'unknown'}",
                )
                self._send_json({"ok": True, "state": "SUCCEEDED"})
                return

            if parsed.path == "/internal/leases/fail":
                if lease_context is None:
                    self._send_json({"error": "lease_api_disabled"}, status=503)
                    return
                lease_token = str(payload.get("lease_token") or "").strip()
                if not run_id or not lease_token:
                    self._send_json({"error": "run_id_and_lease_token_required"}, status=400)
                    return
                slot_record = store.get_slot_task_by_lease_token(run_id=run_id, lease_token=lease_token)
                if slot_record is None:
                    self._send_json({"error": "lease_not_found"}, status=404)
                    return
                reason = str(payload.get("reason") or "slot failed").strip()
                attempt_no = int(slot_record.get("attempt_no") or 0)
                retry_allowed = attempt_no <= lease_context.worker_requeue_limit
                final_state = "RETRY_QUEUED" if retry_allowed else "FAILED"
                slot = slot_task_ref_from_record(run_id=run_id, slot_record=slot_record)
                store.clear_slot_lease(
                    run_id=run_id,
                    lease_token=lease_token,
                    final_state=final_state,
                    failure_reason=reason,
                )
                store.mark_ingest_state(input_path=str(slot.input_path), state=final_state)
                store.append_slot_event(
                    run_id=run_id,
                    slot_id=slot.slot_id,
                    level="WARN" if retry_allowed else "ERROR",
                    stage=final_state,
                    message=reason,
                )
                self._send_json({"ok": True, "state": final_state, "retry_queued": retry_allowed})
                return

            if parsed.path == "/internal/workers/register":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                worker = store.get_slurm_worker(run_id=run_id, worker_id=worker_id)
                if worker is not None:
                    store.update_slurm_worker_control_plane(
                        run_id=run_id,
                        worker_id=worker_id,
                        tunnel_state="CONNECTED",
                        tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                        observed_node=str(payload.get("observed_node") or "").strip() or None,
                    )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/heartbeat":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="CONNECTED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/degraded":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                reason = str(payload.get("reason") or "tunnel degraded").strip()
                stage = str(payload.get("stage") or "CONTROL_TUNNEL_LOST").strip().upper()
                store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="DEGRADED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                    degraded_reason=reason,
                )
                store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="WARN",
                    stage=stage,
                    message=f"worker_id={worker_id} reason={reason}",
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/workers/recovered":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                store.update_slurm_worker_control_plane(
                    run_id=run_id,
                    worker_id=worker_id,
                    tunnel_state="CONNECTED",
                    tunnel_session_id=str(payload.get("tunnel_session_id") or "").strip() or None,
                    observed_node=str(payload.get("observed_node") or "").strip() or None,
                    degraded_reason=None,
                )
                store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage="CONTROL_TUNNEL_RECOVERED",
                    message=f"worker_id={worker_id}",
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/events/worker":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                stage = str(payload.get("stage") or "WORKER_EVENT").strip().upper()
                message = str(payload.get("message") or "").strip() or f"worker_id={worker_id}"
                store.append_event(
                    run_id=run_id,
                    job_id="__worker__",
                    level="INFO",
                    stage=stage,
                    message=f"worker_id={worker_id} {message}",
                )
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/node":
                if not run_id:
                    self._send_json({"error": "run_id_required"}, status=400)
                    return
                store.record_node_resource_snapshot(**payload)
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/worker":
                if not run_id or not worker_id:
                    self._send_json({"error": "run_id_and_worker_id_required"}, status=400)
                    return
                store.record_worker_resource_snapshot(**payload)
                self._send_json({"ok": True})
                return

            if parsed.path == "/internal/resources/slot":
                if not run_id:
                    self._send_json({"error": "run_id_required"}, status=400)
                    return
                store.record_slot_resource_snapshot(**payload)
                self._send_json({"ok": True})
                return

            self._send_json({"error": "not_found"}, status=404)

    return StatusHandler


def start_status_server(
    *,
    state_store: StateStore | None = None,
    host: str,
    port: int,
    lease_context: LeaseServerContext | None = None,
):
    handler = make_status_handler(
        state_store=state_store,
        lease_context=lease_context,
    )
    return ThreadingHTTPServer((host, port), handler)
