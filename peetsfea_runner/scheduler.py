from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Generic, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class JobSpec:
    job_id: str
    job_index: int
    aedt_path: Path


@dataclass(slots=True)
class ScheduledBatchResult(Generic[T]):
    results: list[T]
    max_inflight: int


def calculate_effective_slots(*, max_jobs_per_account: int, license_cap_per_account: int, windows_per_job: int) -> int:
    if max_jobs_per_account <= 0:
        raise ValueError("max_jobs_per_account must be > 0")
    if license_cap_per_account <= 0:
        raise ValueError("license_cap_per_account must be > 0")
    if windows_per_job <= 0:
        raise ValueError("windows_per_job must be > 0")
    slots = min(max_jobs_per_account, license_cap_per_account // windows_per_job)
    if slots <= 0:
        raise ValueError("effective job slots must be >= 1")
    return slots


def run_jobs_with_slots(
    *,
    job_specs: list[JobSpec],
    max_slots: int,
    worker: Callable[[JobSpec], T],
) -> ScheduledBatchResult[T]:
    if max_slots <= 0:
        raise ValueError("max_slots must be > 0")

    lock = Lock()
    inflight = {"current": 0, "max": 0}

    def _tracked_worker(job: JobSpec) -> T:
        with lock:
            inflight["current"] += 1
            inflight["max"] = max(inflight["max"], inflight["current"])
        try:
            return worker(job)
        finally:
            with lock:
                inflight["current"] -= 1

    results: list[T] = []
    with ThreadPoolExecutor(max_workers=max_slots) as executor:
        futures: dict[Future[T], JobSpec] = {
            executor.submit(_tracked_worker, job_spec): job_spec for job_spec in job_specs
        }
        for future in as_completed(futures):
            results.append(future.result())

    return ScheduledBatchResult(results=results, max_inflight=inflight["max"])
