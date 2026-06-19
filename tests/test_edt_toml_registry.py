from __future__ import annotations

import json
import random
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from peetsfea_runner.edt_toml_registry import (
    BUILTIN_TOML_ID,
    TomlRegistryRequestError,
    TomlRegistryService,
    start_toml_registry_server,
)
from peetsfea_runner.single_simulation_store import DbTomlRegistry, SingleSimulationResultStore


def _service(tmp_path: Path, *, rng: random.Random | None = None) -> TomlRegistryService:
    store = SingleSimulationResultStore(db_path=tmp_path / "r.duckdb")
    registry = DbTomlRegistry(store=store, rng=rng or random.Random(0))
    service = TomlRegistryService(
        registry=registry,
        builtin_toml_text="spec_version = 'builtin'\n",
        clock=lambda: 10.0,
    )
    service.initialize()
    return service


def _request(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=3) as response:
        data = json.loads(response.read().decode("utf-8"))
    assert isinstance(data, dict)
    return data


def test_builtin_is_persistent_and_immutable(tmp_path: Path) -> None:
    service = _service(tmp_path)
    listed = service.list_tomls()["tomls"]
    assert listed[0]["id"] == BUILTIN_TOML_ID
    assert listed[0]["active"] is True

    with pytest.raises(TomlRegistryRequestError):
        service.unregister_custom(BUILTIN_TOML_ID)
    with pytest.raises(TomlRegistryRequestError):
        service.set_active(BUILTIN_TOML_ID, {"active": False})

    restarted = _service(tmp_path)
    assert restarted.list_tomls()["tomls"][0]["id"] == BUILTIN_TOML_ID


def test_custom_toml_limit_active_toggle_and_ratios(tmp_path: Path) -> None:
    service = _service(tmp_path)
    added = [
        service.register_custom({"name": f"c{i}", "toml_text": f"spec_version = 'c{i}'\n"})["toml"]
        for i in range(6)
    ]
    with pytest.raises(TomlRegistryRequestError):
        service.register_custom({"name": "overflow", "toml_text": "spec_version = 'overflow'\n"})

    first_custom = str(added[0]["id"])
    assert service.set_active(first_custom, {"active": False}) == {"id": first_custom, "active": False}
    with pytest.raises(TomlRegistryRequestError):
        service.set_ratios({"ratios": {BUILTIN_TOML_ID: 100.0}})

    service.set_active(first_custom, {"active": True})
    active_ids = [str(t["id"]) for t in service.list_tomls()["tomls"] if t["active"]]
    ratios = {toml_id: 1.0 for toml_id in active_ids}
    ratios[BUILTIN_TOML_ID] = 100.0 - (len(active_ids) - 1)
    result = service.set_ratios({"ratios": ratios})
    assert result["ratios_set"] is True
    assert service.list_tomls()["ratios_set"] is True

    service.set_ratios({"ratios": None})
    assert service.list_tomls()["ratios_set"] is False


def test_lease_uses_registry_and_persists_seed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.lease_chunk(3)
    second = service.lease_chunk(2)
    assert first is not None and first["toml_id"] == BUILTIN_TOML_ID
    assert first["seed_base"] == 0 and first["count"] == 3
    assert second is not None and second["seed_base"] == 3

    restarted = _service(tmp_path)
    third = restarted.lease_chunk(1)
    assert third is not None and third["seed_base"] == 5


def test_registry_http_api(tmp_path: Path) -> None:
    service = _service(tmp_path)
    server = start_toml_registry_server(service=service, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        assert _request(base + "/health") == {"active_tomls": 1, "status": "ok"}
        created = _request(
            base + "/api/tomls/custom",
            method="POST",
            payload={"name": "narrow", "toml_text": "spec_version = 'narrow'\n"},
        )
        custom_id = created["toml"]["id"]
        _request(
            base + "/api/tomls/ratios",
            method="PUT",
            payload={"ratios": {BUILTIN_TOML_ID: 70, custom_id: 30}},
        )
        listed = _request(base + "/api/tomls")
        assert listed["active_count"] == 2 and listed["ratios_set"] is True
        _request(base + f"/api/tomls/{custom_id}/active", method="PATCH", payload={"active": False})
        assert _request(base + "/api/tomls")["ratios_set"] is False

        with pytest.raises(urllib.error.HTTPError) as exc:
            _request(base + "/submit", method="POST", payload={"toml_text": "x = 1\n"})
        assert exc.value.code == 410
    finally:
        server.shutdown()
