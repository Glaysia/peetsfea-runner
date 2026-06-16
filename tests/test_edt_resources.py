from __future__ import annotations

from peetsfea_runner.edt_resources import ResourcePoller, parse_remote

_SAMPLE = """###JOBS
681061|peetsfea-edt-3|RUNNING|1:18|n003|gpu1|32
681064|peetsfea-edt-6|PENDING|0:00||gpu2|32
###NODES
n003|4.62|32|48|682334|768000
###LIC
550|78|31
"""


def test_parse_remote_jobs_nodes_license() -> None:
    snap = parse_remote(_SAMPLE)
    assert snap["ok"] is True
    assert len(snap["jobs"]) == 2
    j = snap["jobs"][0]
    assert j["id"] == "681061" and j["state"] == "RUNNING" and j["node"] == "n003" and j["partition"] == "gpu1"
    # 노드 부하(컨테이너별 실시간 부하 원천)
    n = snap["nodes"]["n003"]
    assert n["cpuload"] == 4.62 and n["cputot"] == 48 and n["memfree_mb"] == 682334 and n["memtotal_mb"] == 768000
    # 라이선스
    assert snap["license"] == {"feature": "electronics_desktop", "issued": 550, "in_use": 78, "mine": 31}
    # 카운트
    assert snap["counts"] == {"running": 1, "pending": 1}


def test_parse_remote_empty_is_safe() -> None:
    snap = parse_remote("")
    assert snap["jobs"] == [] and snap["nodes"] == {} and snap["counts"]["running"] == 0


def test_poller_caches_snapshot_via_fake_runner() -> None:
    calls = {"n": 0}

    def fake(argv: list[str]) -> tuple[int, str]:
        calls["n"] += 1
        return 0, _SAMPLE

    poller = ResourcePoller(runner=fake, clock=lambda: 123.0)
    snap = poller.poll_once()
    assert snap["ts"] == 123.0
    assert snap["counts"]["running"] == 1
    assert poller.snapshot()["license"]["mine"] == 31
    assert calls["n"] == 1


def test_poller_ssh_failure_yields_empty_not_crash() -> None:
    def boom(argv: list[str]) -> tuple[int, str]:
        raise OSError("ssh down")

    poller = ResourcePoller(runner=boom, clock=lambda: 1.0)
    snap = poller.poll_once()  # raise 하지 않아야 함
    assert snap["ok"] is False and snap["jobs"] == []
