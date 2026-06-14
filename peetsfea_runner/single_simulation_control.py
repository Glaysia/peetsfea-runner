from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .single_simulation_client import SingleSimulationApiClient
from .single_simulation_store import SingleSimulationResultStore


@dataclass(slots=True, frozen=True)
class SingleSimulationRunResult:
    envelope: dict[str, Any]
    recorded: bool


def run_single_simulation_through_api(
    *,
    client: SingleSimulationApiClient,
    result_store: SingleSimulationResultStore,
    candidate_toml_text: str,
    request_id: str = "",
    seed: int = 0,
    mode: str = "full",
) -> SingleSimulationRunResult:
    envelope = client.simulate(
        candidate_toml_text=candidate_toml_text,
        request_id=request_id,
        seed=seed,
        mode=mode,
    )
    if "request_id" not in envelope and request_id:
        envelope["request_id"] = request_id
    result_store.record_envelope(envelope)
    return SingleSimulationRunResult(envelope=envelope, recorded=True)


__all__ = ["SingleSimulationRunResult", "run_single_simulation_through_api"]
