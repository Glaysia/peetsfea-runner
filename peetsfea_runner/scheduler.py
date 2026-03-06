from __future__ import annotations

import subprocess
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Generic, Protocol, Sequence, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class JobSpec:
    job_id: str
    job_index: int
    input_path: Path
    relative_path: Path
    output_dir: Path
    account_id: str
    host_alias: str


@dataclass(slots=True)
class ScheduledBatchResult(Generic[T]):
    results: list[T]
    max_inflight: int


class AccountConfigLike(Protocol):
    account_id: str
    host_alias: str
    max_jobs: int


@dataclass(slots=True, frozen=True)
class WindowTaskRef:
    run_id: str
    window_id: str
    input_path: Path
    relative_path: Path
    output_dir: Path
    attempt_no: int = 1


@dataclass(slots=True, frozen=True)
class BundleSpec:
    job_id: str
    job_index: int
    account_id: str
    host_alias: str
    window_inputs: tuple[WindowTaskRef, ...]

    @property
    def window_count(self) -> int:
        return len(self.window_inputs)


@dataclass(slots=True, frozen=True)
class AccountCapacitySnapshot:
    account_id: str
    host_alias: str
    running_count: int
    pending_count: int
    allowed_submit: int


@dataclass(slots=True)
class BalancedBatchResult(Generic[T]):
    results: list[T]
    max_inflight_jobs: int
    submitted_jobs: int


RUNNING_STATES = frozenset({"R", "CG", "RUNNING", "COMPLETING"})
PENDING_STATES = frozenset({"PD", "PENDING"})


def parse_squeue_state_counts(lines: Sequence[str]) -> tuple[int, int, dict[str, int]]:
    state_counts: dict[str, int] = {}
    for raw in lines:
        state = raw.strip().upper()
        if not state:
            continue
        state_counts[state] = state_counts.get(state, 0) + 1
    running_count = sum(count for state, count in state_counts.items() if state in RUNNING_STATES)
    pending_count = sum(count for state, count in state_counts.items() if state in PENDING_STATES)
    return running_count, pending_count, state_counts


def query_account_capacity(
    *,
    account: AccountConfigLike,
    pending_buffer_per_account: int,
    run_command: Callable[[list[str]], tuple[int, str, str]] | None = None,
    ssh_connect_timeout_seconds: int = 5,
    command_timeout_seconds: int = 8,
) -> AccountCapacitySnapshot:
    if pending_buffer_per_account < 0:
        raise ValueError("pending_buffer_per_account must be >= 0")
    if ssh_connect_timeout_seconds <= 0:
        raise ValueError("ssh_connect_timeout_seconds must be > 0")
    if command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be > 0")

    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={ssh_connect_timeout_seconds}",
        account.host_alias,
        "squeue -u $USER -h -o \"%T\"",
    ]
    if run_command is None:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"capacity query timed out account={account.account_id} host={account.host_alias} "
                f"timeout={command_timeout_seconds}s"
            ) from exc
        return_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    else:
        return_code, stdout, stderr = run_command(command)

    if return_code != 0:
        details = (stderr or stdout).strip()
        if not details:
            details = f"return code={return_code}"
        raise RuntimeError(f"capacity query failed account={account.account_id}: {details}")

    running_count, pending_count, _ = parse_squeue_state_counts(stdout.splitlines())
    allow_running = max(0, account.max_jobs - running_count)
    allow_pending = max(0, pending_buffer_per_account - pending_count)
    allowed_submit = allow_running + allow_pending
    return AccountCapacitySnapshot(
        account_id=account.account_id,
        host_alias=account.host_alias,
        running_count=running_count,
        pending_count=pending_count,
        allowed_submit=allowed_submit,
    )


def pick_balanced_account(
    *,
    capacities: Sequence[AccountCapacitySnapshot],
    completed_windows_by_account: dict[str, int],
    inflight_windows_by_account: dict[str, int],
) -> AccountCapacitySnapshot | None:
    eligible = [item for item in capacities if item.allowed_submit > 0]
    if not eligible:
        return None

    def _sort_key(item: AccountCapacitySnapshot) -> tuple[int, int, str]:
        score = completed_windows_by_account.get(item.account_id, 0) + inflight_windows_by_account.get(
            item.account_id, 0
        )
        return score, item.running_count, item.account_id

    return min(eligible, key=_sort_key)


def run_window_bundles(
    *,
    window_queue: list[WindowTaskRef],
    accounts: list[AccountConfigLike],
    windows_per_job: int,
    pending_buffer_per_account: int,
    worker: Callable[[BundleSpec], T],
    capacity_lookup: Callable[..., AccountCapacitySnapshot] = query_account_capacity,
    initial_completed_windows: dict[str, int] | None = None,
    job_index_start: int = 1,
    max_workers: int | None = None,
    idle_sleep_seconds: float = 1.0,
    on_capacity_snapshot: Callable[[AccountCapacitySnapshot], None] | None = None,
    on_bundle_submitted: Callable[[BundleSpec], None] | None = None,
    on_capacity_error: Callable[[AccountConfigLike, Exception], None] | None = None,
) -> BalancedBatchResult[T]:
    if windows_per_job <= 0:
        raise ValueError("windows_per_job must be > 0")
    if pending_buffer_per_account < 0:
        raise ValueError("pending_buffer_per_account must be >= 0")
    if idle_sleep_seconds <= 0:
        raise ValueError("idle_sleep_seconds must be > 0")
    if not accounts:
        raise ValueError("accounts must not be empty")
    if job_index_start <= 0:
        raise ValueError("job_index_start must be > 0")

    queue = deque(window_queue)
    completed_windows: dict[str, int] = dict(initial_completed_windows or {})
    inflight_windows: dict[str, int] = {}
    results: list[T] = []
    max_inflight_jobs = 0
    submitted_jobs = 0
    job_counter = job_index_start - 1

    if max_workers is None:
        max_workers = max(1, sum(max(1, account.max_jobs) for account in accounts))
    else:
        max_workers = max(1, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        inflight_futures: dict[Future[T], tuple[str, int]] = {}

        while queue or inflight_futures:
            done_futures = [future for future in tuple(inflight_futures) if future.done()]
            for future in done_futures:
                account_id, window_count = inflight_futures.pop(future)
                try:
                    results.append(future.result())
                finally:
                    inflight_windows[account_id] = max(0, inflight_windows.get(account_id, 0) - window_count)
                    completed_windows[account_id] = completed_windows.get(account_id, 0) + window_count

            if not queue:
                if inflight_futures:
                    wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                continue

            if len(inflight_futures) >= max_workers:
                wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                continue

            snapshots: list[AccountCapacitySnapshot] = []
            for account in accounts:
                try:
                    snapshot = capacity_lookup(account=account, pending_buffer_per_account=pending_buffer_per_account)
                except Exception as exc:
                    if on_capacity_error is not None:
                        on_capacity_error(account, exc)
                    continue
                snapshots.append(snapshot)
                if on_capacity_snapshot is not None:
                    on_capacity_snapshot(snapshot)

            selected = pick_balanced_account(
                capacities=snapshots,
                completed_windows_by_account=completed_windows,
                inflight_windows_by_account=inflight_windows,
            )
            if selected is None:
                if inflight_futures:
                    wait(tuple(inflight_futures), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                    continue
                raise RuntimeError("No eligible account available for bundle submission.")

            bundle_inputs: list[WindowTaskRef] = []
            while queue and len(bundle_inputs) < windows_per_job:
                bundle_inputs.append(queue.popleft())
            if not bundle_inputs:
                continue

            job_counter += 1
            bundle = BundleSpec(
                job_id=f"job_{job_counter:04d}",
                job_index=job_counter,
                account_id=selected.account_id,
                host_alias=selected.host_alias,
                window_inputs=tuple(bundle_inputs),
            )
            future = executor.submit(worker, bundle)
            inflight_futures[future] = (selected.account_id, len(bundle_inputs))
            inflight_windows[selected.account_id] = inflight_windows.get(selected.account_id, 0) + len(bundle_inputs)
            submitted_jobs += 1
            max_inflight_jobs = max(max_inflight_jobs, len(inflight_futures))
            if on_bundle_submitted is not None:
                on_bundle_submitted(bundle)

            # Keep loop responsive for freshly completed futures.
            if inflight_futures:
                wait(tuple(inflight_futures), timeout=0)

    return BalancedBatchResult(
        results=results,
        max_inflight_jobs=max_inflight_jobs,
        submitted_jobs=submitted_jobs,
    )


def run_window_workers(
    *,
    window_queue: list[WindowTaskRef],
    accounts: list[AccountConfigLike],
    windows_per_job: int,
    pending_buffer_per_account: int,
    worker: Callable[[BundleSpec], T],
    capacity_lookup: Callable[..., AccountCapacitySnapshot] = query_account_capacity,
    initial_completed_windows: dict[str, int] | None = None,
    job_index_start: int = 1,
    max_workers: int | None = None,
    idle_sleep_seconds: float = 1.0,
    on_capacity_snapshot: Callable[[AccountCapacitySnapshot], None] | None = None,
    on_bundle_submitted: Callable[[BundleSpec], None] | None = None,
    on_capacity_error: Callable[[AccountConfigLike, Exception], None] | None = None,
) -> BalancedBatchResult[T]:
    if windows_per_job <= 0:
        raise ValueError("windows_per_job must be > 0")
    if pending_buffer_per_account < 0:
        raise ValueError("pending_buffer_per_account must be >= 0")
    if idle_sleep_seconds <= 0:
        raise ValueError("idle_sleep_seconds must be > 0")
    if not accounts:
        raise ValueError("accounts must not be empty")
    if job_index_start <= 0:
        raise ValueError("job_index_start must be > 0")
    if not window_queue:
        return BalancedBatchResult(results=[], max_inflight_jobs=0, submitted_jobs=0)

    completed_windows = dict(initial_completed_windows or {})
    total_target_workers = min(len(window_queue), sum(max(1, account.max_jobs) for account in accounts))
    account_order: list[AccountConfigLike] = []
    while len(account_order) < total_target_workers:
        progressed = False
        for account in accounts:
            assigned_for_account = sum(1 for item in account_order if item.account_id == account.account_id)
            if assigned_for_account >= account.max_jobs:
                continue
            account_order.append(account)
            progressed = True
            if len(account_order) >= total_target_workers:
                break
        if not progressed:
            break

    if not account_order:
        raise RuntimeError("Unable to allocate worker targets.")

    worker_inputs: list[list[WindowTaskRef]] = [[] for _ in range(len(account_order))]
    for index, window in enumerate(window_queue):
        worker_inputs[index % len(account_order)].append(window)

    if max_workers is None:
        max_workers = max(1, len(account_order))
    else:
        max_workers = max(1, max_workers)

    bundles: list[BundleSpec] = []
    job_counter = job_index_start - 1
    for worker_index, account in enumerate(account_order):
        assigned = worker_inputs[worker_index]
        if not assigned:
            continue
        job_counter += 1
        bundle = BundleSpec(
            job_id=f"job_{job_counter:04d}",
            job_index=job_counter,
            account_id=account.account_id,
            host_alias=account.host_alias,
            window_inputs=tuple(assigned),
        )
        bundles.append(bundle)

    pending_bundles_by_account: dict[str, deque[BundleSpec]] = {}
    for bundle in bundles:
        pending_bundles_by_account.setdefault(bundle.account_id, deque()).append(bundle)

    inflight_windows: dict[str, int] = {}
    inflight_workers: dict[str, int] = {}
    results: list[T] = []
    max_inflight_jobs = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_bundle: dict[Future[T], BundleSpec] = {}

        while future_to_bundle or any(queue for queue in pending_bundles_by_account.values()):
            done_futures = [future for future in tuple(future_to_bundle) if future.done()]
            for future in done_futures:
                bundle = future_to_bundle.pop(future)
                results.append(future.result())
                inflight_windows[bundle.account_id] = max(0, inflight_windows.get(bundle.account_id, 0) - bundle.window_count)
                inflight_workers[bundle.account_id] = max(0, inflight_workers.get(bundle.account_id, 0) - 1)
                completed_windows[bundle.account_id] = completed_windows.get(bundle.account_id, 0) + bundle.window_count

            snapshots: list[AccountCapacitySnapshot] = []
            for account in accounts:
                try:
                    snapshot = capacity_lookup(account=account, pending_buffer_per_account=pending_buffer_per_account)
                except Exception as exc:
                    if on_capacity_error is not None:
                        on_capacity_error(account, exc)
                    continue
                snapshots.append(snapshot)
                if on_capacity_snapshot is not None:
                    on_capacity_snapshot(snapshot)

            if not snapshots:
                raise RuntimeError("No account capacity snapshots available for worker startup.")

            snapshot_by_account = {snapshot.account_id: snapshot for snapshot in snapshots}
            submitted_any = False
            for account in accounts:
                queue = pending_bundles_by_account.get(account.account_id)
                if not queue:
                    continue
                snapshot = snapshot_by_account.get(account.account_id)
                if snapshot is None:
                    continue
                external_occupied = max(
                    0,
                    max(0, snapshot.running_count) + max(0, snapshot.pending_count) - inflight_workers.get(account.account_id, 0),
                )
                available_workers = max(
                    0,
                    account.max_jobs - external_occupied - inflight_workers.get(account.account_id, 0),
                )
                while queue and available_workers > 0 and len(future_to_bundle) < max_workers:
                    bundle = queue.popleft()
                    future = executor.submit(worker, bundle)
                    future_to_bundle[future] = bundle
                    inflight_windows[bundle.account_id] = inflight_windows.get(bundle.account_id, 0) + bundle.window_count
                    inflight_workers[bundle.account_id] = inflight_workers.get(bundle.account_id, 0) + 1
                    available_workers -= 1
                    submitted_any = True
                    max_inflight_jobs = max(max_inflight_jobs, len(future_to_bundle))
                    if on_bundle_submitted is not None:
                        on_bundle_submitted(bundle)

            if submitted_any:
                time.sleep(idle_sleep_seconds)
                continue

            if future_to_bundle:
                wait(tuple(future_to_bundle), return_when=FIRST_COMPLETED, timeout=idle_sleep_seconds)
                continue

            time.sleep(idle_sleep_seconds)

    return BalancedBatchResult(
        results=results,
        max_inflight_jobs=max_inflight_jobs,
        submitted_jobs=len(bundles),
    )


def calculate_effective_slots(*, max_jobs_per_account: int) -> int:
    if max_jobs_per_account <= 0:
        raise ValueError("max_jobs_per_account must be > 0")
    return max_jobs_per_account


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
