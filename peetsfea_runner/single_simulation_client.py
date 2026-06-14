from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@dataclass(slots=True, frozen=True)
class SingleSimulationApiClient:
    base_url: str
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/health")

    def simulate(
        self,
        *,
        candidate_toml_text: str,
        request_id: str = "",
        seed: int = 0,
        mode: str = "full",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "candidate_toml_text": candidate_toml_text,
            "seed": seed,
            "mode": mode,
        }
        if request_id:
            payload["request_id"] = request_id
        return self._request_json("POST", "/simulate", payload=payload)

    def shutdown(self) -> dict[str, Any]:
        return self._request_json("POST", "/shutdown", payload={})

    def _request_json(self, method: str, path: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=data,
            headers={"Content-Type": "application/json"} if data is not None else {},
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return _decode_response(response.read())
        except HTTPError as exc:
            body = exc.read()
            decoded = _decode_response(body) if body else {}
            decoded.setdefault("http_status", exc.code)
            return decoded


def _decode_response(body: bytes) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("single simulation API response must be a JSON object")
    return payload


__all__ = ["SingleSimulationApiClient"]
