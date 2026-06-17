from __future__ import annotations

import dataclasses

import pytest

from peetsfea_runner.edt_intake import IntakeRequestError, IntakeService, make_baseline_sampler


def _ok_validator(text: str) -> None:
    return None


def _reject_validator(text: str) -> None:
    raise ValueError("range exceeds reference")


def _fake_sampler(text: str, count: int, seed: int) -> list[str]:
    return [f"# fixed seed={seed} i={i}\nspec_version='0.3.4'\n" for i in range(count)]


class FakeSweepQueue:
    """enqueue_sweep만 가진 가짜 큐(DbPriorityQueue 자리)."""

    def __init__(self) -> None:
        self.sweeps: list[dict] = []

    def enqueue_sweep(self, *, request_id: str, sweep_toml_text: str, seed: int, count: int, mode: str = "full") -> None:
        self.sweeps.append(
            {"request_id": request_id, "sweep": sweep_toml_text, "seed": seed, "count": count, "mode": mode}
        )


def test_submit_enqueues_sweep_request_without_sampling() -> None:
    q = FakeSweepQueue()
    svc = IntakeService(queue=q, validator=_ok_validator)
    result = svc.submit({"sweep_toml_text": "spec_version='0.3.7'\n", "count": 50, "seed": 7})
    assert result["accepted"] == 50 and result["request_id"] == "sweep-1"
    # 핵심: 샘플링 없이 sweep 요청 1줄만 적재(무거운 cadquery는 컨테이너로 내림).
    assert len(q.sweeps) == 1
    s = q.sweeps[0]
    assert s["count"] == 50 and s["seed"] == 7 and s["request_id"] == "sweep-1"
    assert s["sweep"] == "spec_version='0.3.7'\n"


def test_intake_has_no_sampler_field() -> None:
    # web 정상화: IntakeService는 샘플러를 들지 않는다(=web에서 geometry 빌드 안 함).
    assert "sampler" not in {f.name for f in dataclasses.fields(IntakeService)}


def test_submit_rejects_out_of_range_as_bad_request() -> None:
    q = FakeSweepQueue()
    svc = IntakeService(queue=q, validator=_reject_validator)
    with pytest.raises(IntakeRequestError):
        svc.submit({"sweep_toml_text": "x\n", "count": 2})
    assert q.sweeps == []  # 거절 시 아무것도 안 들어감


def test_submit_bad_payload() -> None:
    svc = IntakeService(queue=FakeSweepQueue(), validator=_ok_validator)
    with pytest.raises(IntakeRequestError):
        svc.submit({"sweep_toml_text": "", "count": 2})
    with pytest.raises(IntakeRequestError):
        svc.submit({"sweep_toml_text": "x", "count": 0})
    with pytest.raises(IntakeRequestError):
        svc.submit({"sweep_toml_text": "x", "count": 2, "seed": "nope"})


def test_baseline_sampler_rolls_seed() -> None:
    refill = make_baseline_sampler("sweep", batch_size=5, sampler=_fake_sampler, seed_start=0)
    batch1 = refill()
    batch2 = refill()
    assert len(batch1) == 5 and len(batch2) == 5
    assert batch1[0].seed == 0 and batch2[0].seed == 1  # seed 롤링
    assert batch1[0].request_id == "base-0-0" and batch2[0].request_id == "base-1-0"
    assert all(it.candidate_toml_text for it in batch1)
