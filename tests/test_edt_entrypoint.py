from __future__ import annotations

from pathlib import Path

from peetsfea_runner.edt_entrypoint import _worker_env


def test_worker_env_namespaces_seed_and_output() -> None:
    base = {
        "EDT_OUTPUT_ROOT": "/out",
        "EDT_SLOT_COUNT": "11",
        "EDT_WORK_DIR": "/out/work",
        "EDT_PRIORITY_TOML": "/fixed.toml",
        "EDT_MAX_SIMS": "1",
        "EDT_RESULT_INGEST_URL": "http://127.0.0.1:7876/ingest",
    }
    root = Path("/out")
    e0 = _worker_env(base, 0, root, 1_000_000)
    e3 = _worker_env(base, 3, root, 1_000_000)

    # 워커로 표시 + 단일슬롯
    assert e0["EDT_WORKER_INDEX"] == "0" and e0["EDT_SLOT_COUNT"] == "1"
    # 워커별 seed 범위(request_id 충돌 방지) + 출력 하위디렉토리
    assert e0["EDT_BASELINE_SEED_START"] == "0"
    assert e3["EDT_BASELINE_SEED_START"] == "3000000"
    assert e0["EDT_OUTPUT_ROOT"] == "/out/worker_00"
    assert e3["EDT_OUTPUT_ROOT"] == "/out/worker_03"
    # 공유되면 안 되는 것들은 제거(각 워커가 자기 work/출력)
    assert "EDT_WORK_DIR" not in e0
    assert "EDT_PRIORITY_TOML" not in e0  # 우선순위는 호스트측
    assert "EDT_MAX_SIMS" not in e0
    # ingest는 상속(노드 loopback 터널 공유)
    assert e0["EDT_RESULT_INGEST_URL"] == "http://127.0.0.1:7876/ingest"


def test_worker_seed_ranges_do_not_overlap() -> None:
    # 워커 N개의 seed 시작값이 stride 간격 → baseline request_id(base-{seed}-{i})가 워커 간 충돌하지 않는다.
    stride = 1_000_000
    starts = [int(_worker_env({"EDT_OUTPUT_ROOT": "/o"}, i, Path("/o"), stride)["EDT_BASELINE_SEED_START"]) for i in range(11)]
    assert starts == sorted(starts) and len(set(starts)) == 11
    assert all(starts[i + 1] - starts[i] == stride for i in range(len(starts) - 1))
