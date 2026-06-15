"""실 AEDT 디스패처 스모크 — 디스패처가 준 grpc 세션에 시뮬이 실제로 접속하는지 검증.

1슬롯에 2아이템을 순차 처리한다. 스텁 프리미티브는 peetsfea HFSS solve 대신 edtmgr가 준
`grpc_port`에 pyaedt로 **실제 접속**해 warm ansysedt 핸드셰이크를 증명하고, 같은 ansysedt가
두 시뮬에 재사용(warm 유지)되는지 pid로 확인한다. (실 HFSS solve는 peetsfea 0.3.2 몫.)

    tests/run_smoke_in_enroot.sh local-aedt-smoke /workspace/tests/smoke_edt_dispatcher.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collections.abc import Mapping  # noqa: E402

from peetsfea_runner.edt_aedt_backend import RealEdtBackend, default_ansysedt_executable  # noqa: E402
from peetsfea_runner.edt_dispatcher import SlotDispatcher  # noqa: E402
from peetsfea_runner.edt_queue import QueueItem, TomlQueue  # noqa: E402
from peetsfea_runner.edtmgr import EdtManager  # noqa: E402


def _connect_stub(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
    """edtmgr가 빌려준 grpc 세션에 실제 pyaedt로 접속 → 버전 확인 → release."""
    from ansys.aedt.core import Desktop

    desktop = Desktop(new_desktop=False, port=grpc_port, non_graphical=True, close_on_exit=False)
    try:
        version = str(getattr(desktop, "aedt_version_id", "unknown"))
    finally:
        desktop.release_desktop(close_projects=False, close_on_exit=False)
    return {"connected": True, "grpc_port": grpc_port, "aedt_pid": aedt_pid, "aedt_version": version}


def main() -> int:
    executable = default_ansysedt_executable()
    print(f"[disp-smoke] ansysedt: {executable}")
    work_dir = Path(__file__).resolve().parent.parent / "build" / "smoke_disp"
    out_dir = work_dir / "out"
    work_dir.mkdir(parents=True, exist_ok=True)

    backend = RealEdtBackend(slot_id="slot_00", executable=executable, work_dir=work_dir, grpc_startup_timeout=240.0)
    slot = EdtManager(backend=backend, clock=time.monotonic, slot_id="slot_00")

    queue = TomlQueue()
    queue.extend([QueueItem("req_0", "x = 0\n"), QueueItem("req_1", "x = 1\n")])

    recorded: dict[str, dict[str, Any]] = {}

    def record(envelope: Mapping[str, Any]) -> None:
        recorded[str(envelope["request_id"])] = dict(envelope)

    dispatcher = SlotDispatcher(
        slots=[slot],
        queue=queue,
        primitive=_connect_stub,
        output_root=out_dir,
        record=record,
        version_loader=lambda: "smoke",
        drain=True,
    )

    t0 = time.monotonic()
    processed = dispatcher.run()
    print(f"[disp-smoke] processed={processed} ({time.monotonic()-t0:.1f}s)")

    assert processed == 2, f"기대 2건, 실제 {processed}"
    pids: list[int] = []
    for req in ("req_0", "req_1"):
        env = recorded.get(req)
        assert env is not None, f"{req} 결과 없음"
        assert env["terminal_state"] == "success", f"{req} 상태={env['terminal_state']} err={env.get('error')}"
        result = env["result"]
        assert result.get("connected") is True
        pids.append(int(result["aedt_pid"]))
        print(f"[disp-smoke] {req}: connected port={result['grpc_port']} pid={result['aedt_pid']} ver={result['aedt_version']}")

    assert pids[0] == pids[1], f"warm 재사용 실패: pid {pids[0]} != {pids[1]}"
    print(f"[disp-smoke] warm 재사용 확인: 두 시뮬 모두 ansysedt pid={pids[0]}")

    backend.kill()
    time.sleep(1.0)
    assert not backend.is_alive()
    print("[disp-smoke] PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[disp-smoke] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
