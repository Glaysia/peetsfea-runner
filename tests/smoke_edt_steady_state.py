"""실 steady-state e2e (Phase 3+4) — build_steady_state_service를 실 AEDT로 1건 검증.

2-레인 큐(baseline 실 peetsfea 샘플) → admission(실 psutil 부하 게이트) → 디스패처 →
edtmgr warm ansysedt → peetsfea 진짜 프리미티브 실 solve → 결과 DB 기록.
1슬롯·baseline 작게(batch 3)로 1건만 돌려 와이어링이 실제로 흐르는지 본다.

    PEETS_SMOKE_PYTHON=/py312/bin/python PEETS_SMOKE_CONDA_MOUNT=$HOME/miniconda3/envs/py312:/py312 \
    tests/run_smoke_in_enroot.sh local-aedt-smoke /workspace/tests/smoke_edt_steady_state.py
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import peetsfea  # noqa: E402

from peetsfea_runner.edt_service import EdtServiceConfig, build_steady_state_service  # noqa: E402
from peetsfea_runner.peetsfea_data import load_peetsfea_data_toml_text  # noqa: E402


def main() -> int:
    fixed = load_peetsfea_data_toml_text("fixed")
    work = Path(__file__).resolve().parent.parent / "build" / "e2e_steady"
    config = EdtServiceConfig(
        output_root=work / "out",
        db_path=work / "results.duckdb",
        slot_count=1,
        reference_sweep_text=None,  # baseline의 random 무거운 후보 회피 — 우선순위 레인 + 고정 후보로 확정 테스트
        enable_load_balancer=True,
        work_dir=work,
    )
    svc = build_steady_state_service(config)
    # 우선순위 레인에 고정 후보 1건(=Intake 경로) 적재.
    from peetsfea_runner.edt_queue import QueueItem

    svc.queue.put_priority(QueueItem(request_id="steady-prio-0", candidate_toml_text=fixed, mode="full"))
    print(f"[steady-e2e] admission={'on' if svc.admission else 'off'} queue=TwoLaneQueue priority=1", flush=True)

    captured: list[dict[str, Any]] = []
    base_record = svc.dispatcher.record

    def record(env: Mapping[str, Any]) -> None:
        captured.append(dict(env))
        base_record(env)

    svc.dispatcher.record = record

    t0 = time.monotonic()
    thread = threading.Thread(target=svc.dispatcher.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 2400  # 고정 후보 solve(~14분) 충분히 대기
    while time.monotonic() < deadline and svc.dispatcher.processed < 1:
        time.sleep(3)

    svc.dispatcher.stop()
    for slot in svc.dispatcher.slots:
        slot.backend.kill()
    time.sleep(1.0)

    n = svc.dispatcher.processed
    print(f"[steady-e2e] processed={n} ({time.monotonic()-t0:.1f}s)", flush=True)
    assert n >= 1 and captured, "1건도 처리 안 됨"
    env = captured[0]
    print(f"[steady-e2e] request_id={env['request_id']} terminal={env['terminal_state']}", flush=True)
    assert env["request_id"] == "steady-prio-0", "우선순위 레인에서 나온 게 아님"
    assert env["terminal_state"] == "success", f"실패: {env.get('error')}"
    result = env["result"]
    print(f"[steady-e2e] setup_pass_counts={result.get('setup_pass_counts')} csv={list((result.get('csv_paths') or {}).keys())}", flush=True)
    print("[steady-e2e] STEADY_STATE_E2E_PASS", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"[steady-e2e] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
