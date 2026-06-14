from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from peetsfea_runner.single_simulation_api import SingleSimulationService, start_single_simulation_api_server
from peetsfea_runner.single_simulation_client import SingleSimulationApiClient
from peetsfea_runner.single_simulation_control import run_single_simulation_through_api
from peetsfea_runner.single_simulation_store import SingleSimulationResultStore


def test_run_single_simulation_through_api_records_duckdb_result(tmp_path: Path) -> None:
    def fake_primitive(candidate_toml_text: str, *, output_dir: Path, seed: int, mode: str) -> dict[str, Any]:
        return {
            "mode": mode,
            "seed": seed,
            "design_id": "ssw_control",
            "point_hash": "hash-control",
            "dimension_count": 1,
            "free_owner_paths": ["a"],
            "point_values": {"a": 1},
            "setup_pass_counts": {"maximum_passes": 5},
            "solve_telemetry": {"sample_count": 0},
            "csv_paths": {},
            "csv_text_by_report": {},
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
        client = SingleSimulationApiClient(f"http://127.0.0.1:{server.server_address[1]}")
        store = SingleSimulationResultStore(tmp_path / "local.duckdb")

        result = run_single_simulation_through_api(
            client=client,
            result_store=store,
            candidate_toml_text='spec_version = "0.3.1"\n',
            request_id="req-control",
            seed=3,
            mode="semi_dry",
        )

        assert result.recorded is True
        assert result.envelope["terminal_state"] == "success"
        row = store.fetch_result("req-control")
        assert row is not None
        assert row["design_id"] == "ssw_control"
        assert row["host_alias"] == "gate1-harry261"
    finally:
        server.shutdown()
        server.server_close()
