from __future__ import annotations

import pytest

from peetsfea_runner.edt_intake import IntakeRequestError, IntakeService, make_baseline_sampler
from peetsfea_runner.edt_queue import TwoLaneQueue


def _ok_validator(text: str) -> None:
    return None


def _reject_validator(text: str) -> None:
    raise ValueError("range exceeds reference")


def _fake_sampler(text: str, count: int, seed: int) -> list[str]:
    return [f"# fixed seed={seed} i={i}\nspec_version='0.3.4'\n" for i in range(count)]


def test_submit_validates_samples_and_enqueues_priority() -> None:
    q = TwoLaneQueue()
    svc = IntakeService(queue=q, validator=_ok_validator, sampler=_fake_sampler)
    result = svc.submit({"sweep_toml_text": "spec_version='0.3.4'\n", "count": 3, "seed": 7})
    assert result["accepted"] == 3
    assert len(result["request_ids"]) == 3
    p, b = q.depths()
    assert p == 3 and b == 0  # 우선순위 레인에 적재
    # 우선순위가 먼저 소비됨
    assert q.get().request_id.startswith("intake-")  # type: ignore[union-attr]


def test_submit_rejects_out_of_range_as_bad_request() -> None:
    q = TwoLaneQueue()
    svc = IntakeService(queue=q, validator=_reject_validator, sampler=_fake_sampler)
    with pytest.raises(IntakeRequestError):
        svc.submit({"sweep_toml_text": "x\n", "count": 2})
    assert q.depths() == (0, 0)  # 거절 시 아무것도 안 들어감


def test_submit_bad_payload() -> None:
    svc = IntakeService(queue=TwoLaneQueue(), validator=_ok_validator, sampler=_fake_sampler)
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
