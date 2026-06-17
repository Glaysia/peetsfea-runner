from __future__ import annotations

import json
import threading
import urllib.request

from peetsfea_runner.edt_license_ctrl import (
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
