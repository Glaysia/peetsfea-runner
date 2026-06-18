from __future__ import annotations

import pytest

from peetsfea_runner.edtmgr import AedtSession, EdtBackend, EdtManager, EdtManagerError, SlotState


class FakeBackend(EdtBackend):
    """테스트용 ansysedt 어댑터. 실 AEDT 대신 호출만 기록한다."""

    def __init__(self) -> None:
        self.alive = False
        self.starts = 0
        self.lends = 0
        self.reclaims = 0
        self.kills = 0
        self._pid = 1000
        self._port = 50000

    def start(self) -> AedtSession:
        self.starts += 1
        self._pid += 1
        self._port += 1
        self.alive = True
        return AedtSession(pid=self._pid, grpc_port=self._port)

    def lend(self) -> AedtSession:
        self.lends += 1
        return AedtSession(pid=self._pid, grpc_port=self._port)

    def reclaim(self) -> None:
        self.reclaims += 1

    def is_alive(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.kills += 1
        self.alive = False


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _mgr(backstop: float = 3900.0) -> tuple[EdtManager, FakeBackend, FakeClock]:
    backend = FakeBackend()
    clock = FakeClock()
    return EdtManager(backend=backend, clock=clock, slot_id="slot_1", backstop_seconds=backstop), backend, clock


def test_ensure_warm_starts_once_when_alive() -> None:
    mgr, backend, _ = _mgr()
    s1 = mgr.ensure_warm()
    assert mgr.state is SlotState.WARM
    assert backend.starts == 1
    s2 = mgr.ensure_warm()
    assert s1 == s2
    assert backend.starts == 1  # 살아있으면 재기동 안 함


def test_acquire_release_cycle() -> None:
    mgr, backend, _ = _mgr()
    grant = mgr.acquire()
    assert mgr.state is SlotState.LENT
    assert backend.lends == 1
    assert grant.pid > 0 and grant.grpc_port > 0
    mgr.release()
    assert mgr.state is SlotState.WARM
    # 1솔브=1AEDT: release는 사용한 ansysedt를 죽이고 새 세션을 띄운다(누수/손상 차단). reclaim 재사용 안 함.
    assert backend.reclaims == 0
    assert backend.kills == 1
    assert backend.starts == 2  # 초기 warm + release 후 새 기동


def test_double_acquire_raises() -> None:
    mgr, _, _ = _mgr()
    mgr.acquire()
    with pytest.raises(EdtManagerError):
        mgr.acquire()


def test_release_when_not_lent_is_noop() -> None:
    mgr, backend, _ = _mgr()
    mgr.ensure_warm()
    mgr.release()
    assert backend.reclaims == 0
    assert backend.kills == 0  # 대여 중 아니면 죽이지도 않음(noop)


def test_backstop_force_restarts_after_deadline() -> None:
    mgr, backend, clock = _mgr(backstop=3900.0)
    mgr.acquire()
    clock.now = 3899.0
    assert mgr.poll() is False  # 아직 안 넘음
    clock.now = 3900.0
    assert mgr.poll() is True  # 65분 도달 → 강제 재기동
    assert backend.kills == 1
    assert backend.starts == 2  # 죽이고 다시 띄움
    assert mgr.state is SlotState.WARM
    assert mgr.lease_age_seconds() is None


def test_liveness_restart_when_dead_while_warm() -> None:
    mgr, backend, _ = _mgr()
    mgr.ensure_warm()
    backend.alive = False  # ansysedt가 죽음
    assert mgr.poll() is True
    assert backend.kills == 1
    assert backend.starts == 2
    assert mgr.state is SlotState.WARM


def test_recover_restarts_even_when_alive() -> None:
    # 새 정책: 실패 반환은 살아있어도 재부착하지 않고 죽이고 새 세션으로 교체(손상/누수 세션 재사용 차단).
    mgr, backend, _ = _mgr()
    mgr.acquire()
    mgr.recover()
    assert backend.reclaims == 0
    assert backend.kills == 1
    assert backend.starts == 2
    assert mgr.state is SlotState.WARM


def test_recover_restarts_when_dead() -> None:
    mgr, backend, _ = _mgr()
    mgr.acquire()
    backend.alive = False
    mgr.recover()  # 죽음 → 강제 재기동
    assert backend.kills == 1
    assert backend.starts == 2
    assert mgr.state is SlotState.WARM


def test_cannot_warm_while_lent() -> None:
    mgr, _, _ = _mgr()
    mgr.acquire()
    with pytest.raises(EdtManagerError):
        mgr.ensure_warm()
