from __future__ import annotations

import json
import threading
import urllib.request

from peetsfea_runner.edt_container_control import ContainerController
from peetsfea_runner.edt_license_ctrl import (
    ContainerScheduler,
    LicenseController,
    LicensePermitClient,
    start_license_ctrl_server,
)


def _ctrl(lic: int = 0, target: int = 100, ceiling: int = 150, clock=None) -> LicenseController:
    holder = {"lic": lic}
    c = LicenseController(lic_provider=lambda: holder["lic"], target=target, ceiling=ceiling,
                          clock=clock or (lambda: 1000.0))
    c._holder = holder  # type: ignore[attr-defined]  (테스트에서 lic 조정용)
    return c


def test_permit_caps_at_target_via_active_accounting() -> None:
    c = _ctrl(lic=0, target=3)
    c.poll()  # lic_mine=0 캐시
    assert c.permit("w0") and c.permit("w1") and c.permit("w2")  # 3개까지
    assert not c.permit("w3")  # active=3 == target → 거절 (lmstat 아직 0이어도 회계로 막음)
    assert c.status()["active_permits"] == 3


def test_release_frees_slot() -> None:
    c = _ctrl(lic=0, target=2)
    c.poll()
    assert c.permit("w0") and c.permit("w1") and not c.permit("w2")
    c.release("w0")
    assert c.permit("w2")  # 한 자리 났으니 허가


def test_effective_uses_lmstat_when_higher() -> None:
    c = _ctrl(lic=0, target=5)
    c._holder["lic"] = 5  # type: ignore[attr-defined]
    c.poll()  # lic_mine=5 >= target
    assert not c.permit("w0")  # active=0이어도 lmstat 5라 effective=5 → 거절


def test_concurrent_permits_do_not_exceed_target() -> None:
    c = _ctrl(lic=0, target=10)
    c.poll()
    granted = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        g = c.permit(f"w{i}")
        if g:
            with lock:
                granted.append(i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(granted) == 10  # herd 차단: target 초과 발급 없음


def test_poll_marks_youngest_when_over_ceiling() -> None:
    t = {"now": 1000.0}
    c = _ctrl(lic=0, target=100, ceiling=150, clock=lambda: t["now"])
    # 3개 솔브, 시작 시각 다르게(heartbeat로 등록)
    c.heartbeat("old", solve_started_at=100.0)
    c.heartbeat("mid", solve_started_at=200.0)
    c.heartbeat("young", solve_started_at=300.0)
    c._holder["lic"] = 160  # type: ignore[attr-defined]  150 초과
    c.poll()
    assert c.heartbeat("young", 300.0) is True  # youngest가 abort 표시
    assert c.heartbeat("old", 100.0) is False
    assert c.heartbeat("mid", 200.0) is False


def test_poll_no_abort_within_band() -> None:
    c = _ctrl(lic=120, target=100, ceiling=150)  # 100~150 밴드 안
    c.heartbeat("w0", 100.0)
    c.poll()
    assert c.heartbeat("w0", 100.0) is False  # 밴드 안 → abort 없음


def test_ttl_expires_stale_permit() -> None:
    t = {"now": 1000.0}
    c = _ctrl(lic=0, target=2, clock=lambda: t["now"])
    c.poll()
    assert c.permit("w0") and c.permit("w1") and not c.permit("w2")
    t["now"] += 1000.0  # TTL(180s) 훨씬 초과
    assert c.permit("w2")  # 끊긴 permit 만료 → 자리 남


def test_server_and_client_roundtrip() -> None:
    c = _ctrl(lic=0, target=2)
    c.poll()
    server = start_license_ctrl_server(controller=c, host="127.0.0.1", port=0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        port = server.server_address[1]
        url = f"http://127.0.0.1:{port}"
        client = LicensePermitClient(ctrl_url=url, worker_id="w0")
        assert client.acquire() is True
        # 두 번째 워커
        c2 = LicensePermitClient(ctrl_url=url, worker_id="w1")
        assert c2.acquire() is True
        c3 = LicensePermitClient(ctrl_url=url, worker_id="w2")
        assert c3.acquire() is False  # target=2 도달
        # health
        h = json.load(urllib.request.urlopen(f"{url}/health"))
        assert h["active_permits"] == 2 and h["target"] == 2
        # release → 자리
        client.release()
        assert c3.acquire() is True
        # heartbeat abort 경로
        assert c3.heartbeat(1000.0) is False
    finally:
        server.shutdown()


def test_client_fail_closed_when_ctrl_unreachable() -> None:
    client = LicensePermitClient(ctrl_url="http://127.0.0.1:1", worker_id="w0", timeout_seconds=0.3)
    assert client.acquire() is False  # 제어기 불가 → fail-closed
    assert client.heartbeat(1.0) is False


def test_per_job_groups_active_and_nominal_by_job_index() -> None:
    c = _ctrl(lic=0, target=100)
    # j0: 두 워커가 솔브중(permit), j1: 한 워커는 켜짐만(permit 거절 안 됨이라 active도 됨)
    assert c.permit("j0-w0-nA-1") is True
    assert c.permit("j0-w1-nA-2") is True
    assert c.permit("j1-w0-nB-3") is True
    # j1의 또다른 워커는 heartbeat만(=active 등록) — 그래도 per_job에 잡힘
    c.heartbeat("j1-w1-nB-4", 1000.0)
    pj = c.per_job()
    assert pj["0"]["active"] == 2
    assert pj["1"]["active"] == 2
    # nominal(켜진)도 잡 인덱스별로 집계 — 모든 ping 워커 포함
    assert pj["0"]["nominal"] == 2
    assert pj["1"]["nominal"] == 2
    # status()에도 노출
    assert c.status()["aedt_per_job"]["0"]["active"] == 2


def test_per_job_handles_malformed_worker_id() -> None:
    c = _ctrl(lic=0, target=100)
    c.permit("weird_id_no_prefix")
    assert c.per_job()["?"]["active"] == 1


def test_container_scheduler_integral_control_and_distribution() -> None:
    # LUT 폐지 → 적분 제어. tick은 control_period로 율제한(키퍼 루프 슬램 방지), plan_for는 잡별 분배.
    solve = {"v": 100}
    t = {"v": 0.0}
    sched = ContainerScheduler(
        snapshot_provider=lambda: {"license": {"solve_mine": solve["v"]}},
        controller=ContainerController(target_aedt=120, ki=0.4, dn_max=3, n_min=80, n_max=150, n_total=100),
        control_period_seconds=45.0,
        clock=lambda: t["v"],
    )
    sched.tick()                       # t=0 첫 tick: solve=100<120 → +3
    assert sched.decide_n() == 103
    sched.tick()                       # 주기 미경과 → 무시(율제한)
    assert sched.decide_n() == 103
    t["v"] = 45.0
    sched.tick()                       # 주기 경과 → +3
    assert sched.decide_n() == 106
    dist = sched.controller.distribute()            # N_total을 고정 잡들에 분배
    assert sum(dist) == 106 and max(dist) - min(dist) <= 1
    assert sched.plan_for(0) == dist[0]             # /job_plan?job=0 == 분배[0]
    t["v"] = 90.0
    solve["v"] = 200                   # solve>지령 → 감소
    sched.tick()
    assert sched.decide_n() == 103
