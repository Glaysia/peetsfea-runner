from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from peetsfea_runner.hfss_license_gate import HfssLicenseGate


@dataclass(slots=True, frozen=True)
class _Snapshot:
    status: str
    hfss_in_use: int | None = None
    error: str | None = None


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_gate_closes_at_hfss_ceiling() -> None:
    gate = HfssLicenseGate(query_func=lambda **_: _Snapshot(status="OK", hfss_in_use=530))

    result = gate.check()

    assert result.is_open is False
    assert result.license_gate == "hfss_closed"
    assert result.hfss_in_use == 530
    assert result.license_ceiling == 530


def test_gate_opens_below_hfss_ceiling() -> None:
    gate = HfssLicenseGate(query_func=lambda **_: _Snapshot(status="OK", hfss_in_use=529))

    result = gate.check()

    assert result.is_open is True
    assert result.license_gate is None
    assert result.hfss_in_use == 529


def test_gate_is_fail_open_on_query_failure() -> None:
    def fail_query(**_: object) -> _Snapshot:
        raise TimeoutError("poll timed out")

    gate = HfssLicenseGate(query_func=fail_query)

    result = gate.check()

    assert result.is_open is True
    assert result.status == "FAIL_OPEN"
    assert result.hfss_in_use is None
    assert "poll timed out" in (result.error or "")


def test_gate_is_fail_open_when_hfss_usage_is_missing() -> None:
    gate = HfssLicenseGate(query_func=lambda **_: {"status": "OK"})

    result = gate.check()

    assert result.is_open is True
    assert result.status == "FAIL_OPEN"
    assert result.error == "missing elec_solve_hfss usage"


def test_gate_reuses_cache_until_ttl_expires() -> None:
    clock = _Clock()
    calls = 0

    def query(**_: object) -> _Snapshot:
        nonlocal calls
        calls += 1
        return _Snapshot(status="OK", hfss_in_use=100 + calls)

    gate = HfssLicenseGate(ttl_seconds=10, clock=clock, query_func=query)

    first = gate.check()
    second = gate.check()
    clock.advance(10.1)
    third = gate.check()

    assert first.hfss_in_use == 101
    assert second.hfss_in_use == 101
    assert second.source == "cache"
    assert third.hfss_in_use == 102
    assert calls == 2


def test_concurrent_stale_checks_share_one_refresh() -> None:
    calls = 0

    def query(**_: object) -> _Snapshot:
        nonlocal calls
        calls += 1
        return _Snapshot(status="OK", hfss_in_use=42)

    gate = HfssLicenseGate(query_func=query)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: gate.check(), range(8)))

    assert calls == 1
    assert [result.hfss_in_use for result in results] == [42] * 8
