"""실 프리미티브 e2e — 내 디스패처가 peetsfea 0.3.3 진짜 프리미티브를 warm ansysedt에 물려 실 solve.

GOAL §7의 마지막 조각: `_solve_primitive`(스텁) 대신 **peetsfea 진짜 프리미티브**
(`run_ssw_random_sample_reports_from_toml_text(..., grpc_port=...)`)를 SlotDispatcher로 호출.
edtmgr가 띄운 warm ansysedt를 빌려 → peetsfea가 샘플/지오메트리 빌드/AEDT solve/리포트 →
프로젝트만 닫고 반환(AEDT 살림) → 결과 TypedDict 기록.

    PEETS_SMOKE_PYTHON=/py312/bin/python \
    PEETS_SMOKE_CONDA_MOUNT=$HOME/miniconda3/envs/py312:/py312 \
    tests/run_smoke_in_enroot.sh local-aedt-smoke /workspace/tests/smoke_edt_real_primitive.py
"""

from __future__ import annotations

import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import peetsfea  # noqa: E402
from peetsfea.ssw_random_sample_reports import run_ssw_random_sample_reports_from_toml_text as REAL_PRIMITIVE  # noqa: E402

from peetsfea_runner.edt_aedt_backend import RealEdtBackend, default_ansysedt_executable  # noqa: E402
from peetsfea_runner.edt_dispatcher import SlotDispatcher  # noqa: E402
from peetsfea_runner.edt_queue import QueueItem, TomlQueue  # noqa: E402
from peetsfea_runner.edtmgr import EdtManager  # noqa: E402


def main() -> int:
    candidate = (Path(peetsfea.__file__).resolve().parent / "data" / "0.3.5_fixed.toml").read_text(encoding="utf-8")
    print(f"[real-e2e] peetsfea {peetsfea.__version__}, fixed candidate {len(candidate)} bytes", flush=True)

    work = Path(__file__).resolve().parent.parent / "build" / "e2e_real"
    backend = RealEdtBackend(slot_id="slot_00", executable=default_ansysedt_executable(), work_dir=work, grpc_startup_timeout=240.0)
    slot = EdtManager(backend=backend, clock=time.monotonic, slot_id="slot_00")

    queue = TomlQueue()
    queue.put(QueueItem("real_0", candidate, seed=0, mode="full"))

    recorded: dict[str, dict[str, Any]] = {}

    def record(env: Mapping[str, Any]) -> None:
        recorded[str(env["request_id"])] = dict(env)

    dispatcher = SlotDispatcher(
        slots=[slot],
        queue=queue,
        primitive=REAL_PRIMITIVE,
        output_root=work / "out",
        record=record,
        version_loader=lambda: peetsfea.__version__,
        drain=True,
    )

    t0 = time.monotonic()
    dispatcher.run()
    elapsed = time.monotonic() - t0

    env = recorded.get("real_0")
    assert env is not None, "결과 미기록"
    print(f"[real-e2e] terminal={env['terminal_state']} elapsed={elapsed:.1f}s", flush=True)
    if env["terminal_state"] != "success":
        print(f"[real-e2e] ERROR: {env.get('error')}", flush=True)
        backend.kill()
        return 1

    result = env["result"]
    print(f"[real-e2e] result keys: {sorted(result.keys())}", flush=True)
    print(f"[real-e2e] setup_pass_counts: {result.get('setup_pass_counts')}", flush=True)
    print(f"[real-e2e] csv reports: {list((result.get('csv_paths') or {}).keys())}", flush=True)

    # warm 재사용 확인(프리미티브가 프로젝트만 닫고 AEDT는 살렸는지 → reclaim 성공했는지)
    print(f"[real-e2e] ansysedt alive after run (reclaimed): {backend.is_alive()}", flush=True)

    ok = bool(result.get("csv_paths")) or bool(result.get("setup_pass_counts"))
    backend.kill()
    time.sleep(1.0)
    print("[real-e2e] REAL_PRIMITIVE_E2E_PASS" if ok else "[real-e2e] REAL_PRIMITIVE_E2E_FAIL(빈 결과)", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"[real-e2e] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
