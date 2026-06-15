from __future__ import annotations

from peetsfea_runner.edt_load import AdmissionController, LoadSample


class FakeLoad:
    def __init__(self, cpu: float = 10.0, mem: float = 10.0) -> None:
        self.cpu = cpu
        self.mem = mem

    def __call__(self) -> LoadSample:
        return LoadSample(cpu_percent=self.cpu, mem_percent=self.mem)


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_headroom_admits_and_stagger_blocks_immediate_second() -> None:
    load = FakeLoad(cpu=10, mem=10)
    clock = FakeClock()
    ctrl = AdmissionController(load_sampler=load, clock=clock, min_start_interval_seconds=2.0, ewma_alpha=1.0)

    assert ctrl.can_admit() is True  # t=0 승인
    clock.t = 1.0
    assert ctrl.can_admit() is False  # 스태거(2s 미만)
    clock.t = 3.0
    assert ctrl.can_admit() is True  # 간격 충족


def test_cpu_overload_blocks_and_shrinks_budget() -> None:
    load = FakeLoad(cpu=90, mem=10)  # > 85 watermark
    ctrl = AdmissionController(load_sampler=load, clock=FakeClock(), ewma_alpha=1.0, min_start_interval_seconds=0.0)
    for _ in range(5):
        assert ctrl.can_admit() is False


def test_mem_overload_blocks() -> None:
    load = FakeLoad(cpu=10, mem=95)
    ctrl = AdmissionController(load_sampler=load, clock=FakeClock(), ewma_alpha=1.0, min_start_interval_seconds=0.0)
    assert ctrl.can_admit() is False


def test_recovery_admits_after_overload_clears() -> None:
    load = FakeLoad(cpu=95, mem=10)
    clock = FakeClock()
    ctrl = AdmissionController(load_sampler=load, clock=clock, ewma_alpha=1.0, min_start_interval_seconds=0.0, aimd_increase=5.0)
    assert ctrl.can_admit() is False  # 과부하
    load.cpu = 10  # 부하 해소
    # AIMD budget이 충분히 회복되면 승인.
    admitted = any(ctrl.can_admit() for _ in range(5))
    assert admitted is True


def test_smoothed_ewma_tracks_load() -> None:
    load = FakeLoad(cpu=50, mem=40)
    ctrl = AdmissionController(load_sampler=load, clock=FakeClock(), ewma_alpha=1.0, min_start_interval_seconds=0.0)
    assert ctrl.smoothed is None
    ctrl.can_admit()
    s = ctrl.smoothed
    assert s is not None and s.cpu_percent == 50 and s.mem_percent == 40
