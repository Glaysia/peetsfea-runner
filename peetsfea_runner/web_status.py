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
from .state_store import StateStore
from .version import get_version

APP_VERSION = get_version()


def _first_str_param(params: dict[str, list[str]], key: str) -> str | None:
    values = params.get(key) or []
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _is_loopback_client(host: str | None) -> bool:
    return host in {"127.0.0.1", "::1", "localhost"}


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
                self._send_json(
                    {
                        "ok": True,
                        "lease_available": True,
                        "lease_token": lease_token,
                        "slot_id": str(slot_record["slot_id"]),
                        "attempt_no": int(slot_record.get("attempt_no") or 0),
                        "input_name": Path(str(slot_record["input_path"])).name,
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
                if not slot_record.get("artifact_uploaded_at"):
                    self._send_json({"error": "artifact_missing"}, status=409)
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
