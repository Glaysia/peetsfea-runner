from __future__ import annotations

import io
import json
import tarfile
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from peetsfea_runner.pipeline import PipelineConfig, WorkerStorageConfig, build_lease_server_context
from peetsfea_runner.state_store import StateStore
from peetsfea_runner.web_status import start_status_server


def _start_server(store: StateStore, context: Any) -> tuple[Any, threading.Thread]:
    server = start_status_server(state_store=store, lease_context=context, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _post_json(*, server: Any, path: str, payload: dict[str, Any], query: str = "") -> tuple[int, dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8")
    request = Request(f"http://127.0.0.1:{server.server_address[1]}{path}{query}", data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    try:
        with urlopen(request, timeout=3) as response:
            status = response.status
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        status = exc.code
        body = json.loads(exc.read().decode("utf-8"))
    return status, body


def _post_raw(*, server: Any, path: str, data: bytes, query: str = "") -> tuple[int, dict[str, Any]]:
    request = Request(
        f"http://127.0.0.1:{server.server_address[1]}{path}{query}",
        data=data,
        method="POST",
    )
    request.add_header("Content-Type", "application/octet-stream")
    try:
        with urlopen(request, timeout=3) as response:
            status = response.status
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        status = exc.code
        body = json.loads(exc.read().decode("utf-8"))
    return status, body


def _get_binary(*, server: Any, path: str, query: str = "") -> tuple[int, bytes]:
    request = Request(
        f"http://127.0.0.1:{server.server_address[1]}{path}{query}",
        method="GET",
    )
    with urlopen(request, timeout=3) as response:
        return response.status, response.read()


def _build_context(
    *,
    input_queue_dir: Path,
    output_root_dir: Path,
    storage_mode: str,
) -> PipelineConfig:
    return PipelineConfig(
        input_queue_dir=str(input_queue_dir),
        output_root_dir=str(output_root_dir),
        worker_storage=WorkerStorageConfig(model="single_container_sshfs", storage_mode=storage_mode),
    )


def _build_tgz_payload(*, path: Path) -> bytes:
    marker = path / "artifact.marker"
    marker.write_text("artifact", encoding="utf-8")
    with io.BytesIO() as output:
        with tarfile.open(fileobj=output, mode="w:gz") as archive:
            archive.add(str(marker), arcname="artifact.marker")
        return output.getvalue()


def _request_lease(
    *,
    server: Any,
    run_id: str,
    worker_id: str,
    account_id: str,
) -> dict[str, Any]:
    status, body = _post_json(
        server=server,
        path="/internal/leases/request",
        payload={"run_id": run_id, "worker_id": worker_id, "account_id": account_id},
    )
    assert status == 200
    return body


def test_lease_request_includes_storage_fields() -> None:
    with TemporaryDirectory() as tmpdir:
        input_root = Path(tmpdir) / "input_queue"
        output_root = Path(tmpdir) / "output"
        input_file = input_root / "lane" / "sample.aedt"
        output_dir = output_root / "lane" / "sample.aedt.out"
        input_root.mkdir(parents=True)
        output_root.mkdir(parents=True)
        output_dir.parent.mkdir(parents=True)
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_text("project", encoding="utf-8")
        output_dir.mkdir(parents=True)

        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run-01")
        store.create_slot_task(
            run_id="run-01",
            slot_id="slot-01",
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id="account_01",
        )
        store.upsert_slurm_worker(
            run_id="run-01",
            worker_id="worker-01",
            job_id="job-01",
            attempt_no=1,
            account_id="account_01",
            host_alias="host",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=1,
            backend="slurm_batch",
        )

        context = build_lease_server_context(
            config=_build_context(input_queue_dir=input_root, output_root_dir=output_root, storage_mode="payload")
        )
        server, thread = _start_server(store=store, context=context)
        try:
            response = _request_lease(server=server, run_id="run-01", worker_id="worker-01", account_id="account_01")
            assert response["lease_available"] is True
            assert response["storage_mode"] == "payload"
            assert response["input_relpath"] == "lane/sample.aedt"
            assert response["output_relpath"] == "lane/sample.aedt.out"
        finally:
            server.shutdown()
            thread.join(timeout=1)


def test_sshfs_direct_complete_requires_output_materialized_and_output_dir() -> None:
    with TemporaryDirectory() as tmpdir:
        input_root = Path(tmpdir) / "input_queue"
        output_root = Path(tmpdir) / "output"
        input_file = input_root / "lane" / "sample.aedt"
        output_dir = output_root / "lane" / "sample.aedt.out"
        input_root.mkdir(parents=True)
        output_root.mkdir(parents=True)
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_text("project", encoding="utf-8")

        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run-sshfs")
        store.create_slot_task(
            run_id="run-sshfs",
            slot_id="slot-01",
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id="account_01",
        )
        store.upsert_slurm_worker(
            run_id="run-sshfs",
            worker_id="worker-01",
            job_id="job-01",
            attempt_no=1,
            account_id="account_01",
            host_alias="host",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=1,
            backend="slurm_batch",
        )

        config = _build_context(input_queue_dir=input_root, output_root_dir=output_root, storage_mode="sshfs_direct")
        config.delete_input_after_upload = False
        config.rename_input_to_done_on_success = True
        context = build_lease_server_context(config=config)
        server, thread = _start_server(store=store, context=context)
        try:
            lease = _request_lease(server=server, run_id="run-sshfs", worker_id="worker-01", account_id="account_01")
            assert lease["lease_available"] is True
            assert lease["storage_mode"] == "sshfs_direct"
            assert lease["input_relpath"] == "lane/sample.aedt"
            assert lease["output_relpath"] == "lane/sample.aedt.out"
            lease_token = lease["lease_token"]

            status, body = _get_binary(
                server=server,
                path="/internal/leases/input",
                query=f"?run_id=run-sshfs&lease_token={lease_token}",
            )
            assert status == 200
            assert body == b"project"

            missing_output_status, missing_output_body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={"run_id": "run-sshfs", "lease_token": lease_token, "output_materialized": True},
            )
            assert missing_output_status == 409
            assert missing_output_body["error"] == "output_directory_missing"

            output_dir.mkdir(parents=True)
            status, body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={"run_id": "run-sshfs", "lease_token": lease_token},
            )
            assert status == 409
            assert body["error"] == "output_not_materialized"

            (output_dir / "results.txt").write_text("done", encoding="utf-8")
            final_status, final_body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={
                    "run_id": "run-sshfs",
                    "lease_token": lease_token,
                    "output_materialized": True,
                    "output_relpath": "lane/sample.aedt.out",
                },
            )
            assert final_status == 200
            assert final_body["ok"] is True
            assert final_body["state"] == "SUCCEEDED"

            task = store.get_slot_task(run_id="run-sshfs", slot_id="slot-01")
            assert task is not None
            assert task["state"] == "SUCCEEDED"
            assert task["lease_token"] is None
            assert input_file.with_name(f"{input_file.name}.done").exists()
        finally:
            server.shutdown()
            thread.join(timeout=1)


def test_sshfs_direct_complete_rejects_output_relpath_mismatch_and_stale_token() -> None:
    with TemporaryDirectory() as tmpdir:
        input_root = Path(tmpdir) / "input_queue"
        output_root = Path(tmpdir) / "output"
        input_file = input_root / "lane" / "sample.aedt"
        output_dir = output_root / "lane" / "sample.aedt.out"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        input_file.write_text("project", encoding="utf-8")
        (output_dir / "results.txt").write_text("done", encoding="utf-8")

        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run-stale")
        store.create_slot_task(
            run_id="run-stale",
            slot_id="slot-01",
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id="account_01",
        )
        store.upsert_slurm_worker(
            run_id="run-stale",
            worker_id="worker-01",
            job_id="job-01",
            attempt_no=1,
            account_id="account_01",
            host_alias="host",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=1,
            backend="slurm_batch",
        )

        config = _build_context(input_queue_dir=input_root, output_root_dir=output_root, storage_mode="sshfs_direct")
        config.delete_input_after_upload = False
        config.rename_input_to_done_on_success = True
        context = build_lease_server_context(config=config)
        server, thread = _start_server(store=store, context=context)
        try:
            lease = _request_lease(server=server, run_id="run-stale", worker_id="worker-01", account_id="account_01")
            lease_token = lease["lease_token"]

            mismatch_status, mismatch_body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={
                    "run_id": "run-stale",
                    "lease_token": lease_token,
                    "output_materialized": True,
                    "output_relpath": "other/sample.aedt.out",
                },
            )
            assert mismatch_status == 409
            assert mismatch_body["error"] == "output_relpath_mismatch"
            assert not input_file.with_name(f"{input_file.name}.done").exists()

            stale_status, stale_body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={
                    "run_id": "run-stale",
                    "lease_token": "stale-token",
                    "output_materialized": True,
                    "output_relpath": "lane/sample.aedt.out",
                },
            )
            assert stale_status == 404
            assert stale_body["error"] == "lease_not_found"

            task = store.get_slot_task(run_id="run-stale", slot_id="slot-01")
            assert task is not None
            assert task["state"] == "LEASED"
            assert task["lease_token"] == lease_token
        finally:
            server.shutdown()
            thread.join(timeout=1)


def test_sshfs_direct_endpoints_stay_compatible() -> None:
    with TemporaryDirectory() as tmpdir:
        input_root = Path(tmpdir) / "input_queue"
        output_root = Path(tmpdir) / "output"
        input_file = input_root / "lane" / "sample.aedt"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_text("project", encoding="utf-8")
        output_root.mkdir(parents=True)

        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run-compat")
        store.create_slot_task(
            run_id="run-compat",
            slot_id="slot-01",
            input_path=str(input_file),
            output_path=str(output_root / "lane" / "sample.aedt.out"),
            account_id="account_01",
        )
        store.upsert_slurm_worker(
            run_id="run-compat",
            worker_id="worker-01",
            job_id="job-01",
            attempt_no=1,
            account_id="account_01",
            host_alias="host",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=1,
            backend="slurm_batch",
        )

        context = build_lease_server_context(
            config=_build_context(input_queue_dir=input_root, output_root_dir=output_root, storage_mode="sshfs_direct")
        )
        server, thread = _start_server(store=store, context=context)
        try:
            lease = _request_lease(server=server, run_id="run-compat", worker_id="worker-01", account_id="account_01")
            lease_token = lease["lease_token"]

            status, body = _get_binary(
                server=server,
                path="/internal/leases/input",
                query=f"?run_id=run-compat&lease_token={lease_token}",
            )
            assert status == 200
            assert body == b"project"

            artifact_payload = _build_tgz_payload(path=output_root)
            artifact_status, artifact_body = _post_raw(
                server=server,
                path="/internal/leases/artifact",
                query=f"?run_id=run-compat&lease_token={lease_token}",
                data=artifact_payload,
            )
            assert artifact_status == 200
            assert artifact_body["ok"] is True
            assert artifact_body["state"] == "UPLOADING"
        finally:
            server.shutdown()
            thread.join(timeout=1)


def test_payload_complete_still_requires_artifact_uploaded() -> None:
    with TemporaryDirectory() as tmpdir:
        input_root = Path(tmpdir) / "input_queue"
        output_root = Path(tmpdir) / "output"
        input_file = input_root / "lane" / "sample.aedt"
        input_root.mkdir(parents=True)
        output_root.mkdir(parents=True)
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_text("project", encoding="utf-8")
        output_dir = output_root / "lane" / "sample.aedt.out"
        output_dir.mkdir(parents=True)

        store = StateStore(Path(tmpdir) / "runtime.state")
        store.initialize()
        store.start_run("run-payload")
        store.create_slot_task(
            run_id="run-payload",
            slot_id="slot-01",
            input_path=str(input_file),
            output_path=str(output_dir),
            account_id="account_01",
        )
        store.upsert_slurm_worker(
            run_id="run-payload",
            worker_id="worker-01",
            job_id="job-01",
            attempt_no=1,
            account_id="account_01",
            host_alias="host",
            slurm_job_id="12345",
            worker_state="RUNNING",
            slots_configured=1,
            backend="slurm_batch",
        )

        context = build_lease_server_context(
            config=_build_context(input_queue_dir=input_root, output_root_dir=output_root, storage_mode="payload")
        )
        server, thread = _start_server(store=store, context=context)
        try:
            lease = _request_lease(server=server, run_id="run-payload", worker_id="worker-01", account_id="account_01")
            assert lease["lease_available"] is True
            lease_token = lease["lease_token"]

            status, body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={"run_id": "run-payload", "lease_token": lease_token},
            )
            assert status == 409
            assert body["error"] == "artifact_missing"

            stale_status, stale_body = _post_json(
                server=server,
                path="/internal/leases/complete",
                payload={"run_id": "run-payload", "lease_token": "stale-token"},
            )
            assert stale_status == 404
            assert stale_body["error"] == "lease_not_found"
        finally:
            server.shutdown()
            thread.join(timeout=1)
