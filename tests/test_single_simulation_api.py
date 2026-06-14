from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from peetsfea_runner.single_simulation_api import (
    DEFAULT_SINGLE_HOST_ALIAS,
    SingleSimulationService,
    start_single_simulation_api_server,
)


def _read_json(request: Request) -> tuple[int, dict[str, Any]]:
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _get_json(server: Any, path: str) -> tuple[int, dict[str, Any]]:
    request = Request(f"http://127.0.0.1:{server.server_address[1]}{path}", method="GET")
    return _read_json(request)


def _post_json(server: Any, path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8")
    request = Request(
        f"http://127.0.0.1:{server.server_address[1]}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return _read_json(request)


def test_single_simulation_api_health_and_simulate_use_peetsfea_primitive(tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []

    def fake_primitive(candidate_toml_text: str, *, output_dir: Path, seed: int, mode: str) -> dict[str, Any]:
        calls.append(
            {
                "candidate_toml_text": candidate_toml_text,
                "output_dir": output_dir,
                "seed": seed,
                "mode": mode,
            }
        )
        return {
            "mode": mode,
            "seed": seed,
            "design_id": "ssw_abc",
            "point_hash": "abc123",
            "dimension_count": 1,
            "free_owner_paths": ["modeled_objects[role=tx_ssw_coil].width_ratio"],
            "point_values": {"modeled_objects[role=tx_ssw_coil].width_ratio": 0.5},
            "setup_pass_counts": {"maximum_passes": 5, "minimum_passes": 1, "minimum_converged_passes": 1},
            "solve_telemetry": {"sample_count": 0},
            "csv_paths": {"Results1_Pass": "/remote/result.csv"},
            "csv_text_by_report": {"Results1_Pass": "freq,value\n1,2\n"},
        }

    service = SingleSimulationService(
        output_root=tmp_path / "remote-output",
        primitive=fake_primitive,
        version_loader=lambda: "0.3.1",
    )
    server = start_single_simulation_api_server(service=service)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, health = _get_json(server, "/health")
        assert status == 200
        assert health["host_alias"] == DEFAULT_SINGLE_HOST_ALIAS
        assert health["busy"] is False
        assert health["peetsfea_version"] == "0.3.1"

        toml_text = 'spec_version = "0.3.1"\n'
        status, envelope = _post_json(
            server,
            "/simulate",
            {
                "request_id": "req-001",
                "candidate_toml_text": toml_text,
                "seed": 7,
                "mode": "semi_dry",
            },
        )
        assert status == 200
        assert envelope["terminal_state"] == "success"
        assert envelope["request_id"] == "req-001"
        assert envelope["host_alias"] == DEFAULT_SINGLE_HOST_ALIAS
        assert envelope["input_toml_hash"] == hashlib.sha256(toml_text.encode("utf-8")).hexdigest()
        assert envelope["result"]["design_id"] == "ssw_abc"
        assert calls == [
            {
                "candidate_toml_text": toml_text,
                "output_dir": tmp_path / "remote-output" / "req-001",
                "seed": 7,
                "mode": "semi_dry",
            }
        ]
    finally:
        server.shutdown()
        server.server_close()


def test_single_simulation_api_rejects_second_request_while_busy(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    def slow_primitive(candidate_toml_text: str, *, output_dir: Path, seed: int, mode: str) -> dict[str, Any]:
        entered.set()
        release.wait(timeout=10)
        return {
            "mode": mode,
            "seed": seed,
            "design_id": "done",
            "point_hash": "hash",
            "dimension_count": 0,
            "free_owner_paths": [],
            "point_values": {},
            "setup_pass_counts": {},
            "solve_telemetry": {},
            "csv_paths": {},
            "csv_text_by_report": {},
        }

    service = SingleSimulationService(
        output_root=tmp_path / "remote-output",
        primitive=slow_primitive,
        version_loader=lambda: "0.3.1",
    )
    server = start_single_simulation_api_server(service=service)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload = {"candidate_toml_text": 'spec_version = "0.3.1"\n'}
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(_post_json, server, "/simulate", payload)
            assert entered.wait(timeout=5)
            status, response = _post_json(server, "/simulate", payload)
            assert status == 409
            assert response["error"]["type"] == "SimulationBusyError"
            release.set()
            first_status, first_response = first.result(timeout=10)
            assert first_status == 200
            assert first_response["terminal_state"] == "success"
    finally:
        release.set()
        server.shutdown()
        server.server_close()


def test_single_simulation_api_rejects_invalid_request(tmp_path: Path) -> None:
    service = SingleSimulationService(
        output_root=tmp_path / "remote-output",
        primitive=lambda *args, **kwargs: {},
        version_loader=lambda: "0.3.1",
    )
    server = start_single_simulation_api_server(service=service)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, response = _post_json(server, "/simulate", {"candidate_toml_text": "", "seed": 0})
        assert status == 400
        assert response["error"]["type"] == "SimulationRequestError"
    finally:
        server.shutdown()
        server.server_close()
