"""실 HFSS solve 스모크 — 디스패처→edtmgr가 빌려준 warm ansysedt에서 실제 solve+리포트 산출.

GOAL §7 "fixed toml가 슬롯에서 처리되어 결과/리포트가 산출됨"을 실 AEDT로 닫는다. peetsfea 0.3.2
프리미티브가 아직 없으므로, 그 자리에 **실 pyaedt solve 프리미티브**를 대입한다(0.3.2 나오면 교체).
edtmgr가 준 grpc 세션에 붙어 sample_short.aedt를 열고 analyze → solved 상태/리포트 추출 → release.

    tests/run_smoke_in_enroot.sh local-aedt-smoke /workspace/tests/smoke_edt_solve.py
"""

from __future__ import annotations

import shutil
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from peetsfea_runner.edt_aedt_backend import RealEdtBackend, default_ansysedt_executable  # noqa: E402
from peetsfea_runner.edt_dispatcher import SlotDispatcher  # noqa: E402
from peetsfea_runner.edt_queue import QueueItem, TomlQueue  # noqa: E402
from peetsfea_runner.edtmgr import EdtManager  # noqa: E402

PROJECT_SRC = Path("/workspace/build/solve_input/sample_short.aedt")


def _solve_primitive(text: str, *, output_dir: Path, seed: int, mode: str, grpc_port: int, aedt_pid: int) -> dict[str, Any]:
    """edtmgr가 빌려준 warm ansysedt(grpc_port)에 붙어 실제 HFSS solve + 리포트 추출."""
    from ansys.aedt.core import Hfss

    if output_dir.exists():
        shutil.rmtree(output_dir)  # 이전 run 잔재(.lock 등) 제거
    output_dir.mkdir(parents=True, exist_ok=True)
    project_copy = output_dir / "sample_short.aedt"
    shutil.copy2(PROJECT_SRC, project_copy)

    hfss = Hfss(
        project=str(project_copy),
        non_graphical=True,
        new_desktop=False,
        port=grpc_port,
        close_on_exit=False,
        remove_lock=True,
    )
    try:
        setup_names = list(hfss.setup_names)
        hfss.analyze(cores=4)
        solved = {name: bool(hfss.get_setup(name).is_solved) for name in setup_names}
        pass_counts: dict[str, int] = {}
        for name in setup_names:
            try:
                data = hfss.get_setup(name).get_solution_data()  # type: ignore[attr-defined]
                pass_counts[name] = int(getattr(data, "number_of_variations", 0))
            except Exception:
                pass
        report = {}
        try:
            quantities = hfss.post.available_report_quantities()
            if quantities:
                sol = hfss.post.get_solution_data(expressions=quantities[0])
                if sol is not None:
                    report = {"expression": quantities[0], "primary_sweep": getattr(sol, "primary_sweep", "")}
        except Exception:
            pass
        return {
            "solved": solved,
            "setup_names": setup_names,
            "setup_pass_counts": pass_counts,
            "report": report,
            "grpc_port": grpc_port,
            "aedt_pid": aedt_pid,
        }
    finally:
        # ansysedt는 살리고(관리세션은 edtmgr가 재부착) 프로젝트만 닫는다.
        # 주의: 앱(Hfss).release_desktop은 close_desktop 인자를 받는다(Desktop.release_desktop과 다름).
        hfss.release_desktop(close_projects=True, close_desktop=False)


def main() -> int:
    assert PROJECT_SRC.exists(), f"입력 프로젝트 없음: {PROJECT_SRC}"
    executable = default_ansysedt_executable()
    print(f"[solve-smoke] ansysedt: {executable}  project: {PROJECT_SRC.name}")

    work_dir = Path(__file__).resolve().parent.parent / "build" / "smoke_solve"
    backend = RealEdtBackend(slot_id="slot_00", executable=executable, work_dir=work_dir, grpc_startup_timeout=240.0)
    slot = EdtManager(backend=backend, clock=time.monotonic, slot_id="slot_00")

    queue = TomlQueue()
    queue.put(QueueItem("hfss_solve_0", "sample_short\n"))

    recorded: dict[str, dict[str, Any]] = {}

    def record(envelope: Mapping[str, Any]) -> None:
        recorded[str(envelope["request_id"])] = dict(envelope)

    dispatcher = SlotDispatcher(
        slots=[slot],
        queue=queue,
        primitive=_solve_primitive,
        output_root=work_dir / "out",
        record=record,
        version_loader=lambda: "smoke",
        drain=True,
    )

    t0 = time.monotonic()
    processed = dispatcher.run()
    print(f"[solve-smoke] processed={processed} ({time.monotonic()-t0:.1f}s)")

    assert processed == 1, f"기대 1건, 실제 {processed}"
    env = recorded.get("hfss_solve_0")
    assert env is not None
    assert env["terminal_state"] == "success", f"상태={env['terminal_state']} err={env.get('error')}"
    result = env["result"]
    print(f"[solve-smoke] setups={result['setup_names']} solved={result['solved']} report={result['report']}")
    assert result["setup_names"], "setup이 없음"
    assert any(result["solved"].values()), f"solved setup 없음: {result['solved']}"
    print(f"[solve-smoke] 실 solve 완료 → 결과/리포트 산출 확인 (pid={result['aedt_pid']})")

    backend.kill()
    time.sleep(1.0)
    assert not backend.is_alive()
    print("[solve-smoke] PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"[solve-smoke] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
