"""실 AEDT 스모크 — RealEdtBackend로 ansysedt를 진짜 띄워 warm/lend/reclaim/kill 검증.

pytest가 아니라 수동 실행 스크립트다(실 AEDT·라이선스 소모, 느림). AGENTS.md §6:
"목/페이크에만 의존하지 말 것" 에 따른 실 검증 경로.

    .venv/bin/python tests/smoke_edt_backend.py

성공 시 exit 0, 단계별 로그를 출력한다.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

# repo 루트를 import path에 추가(스크립트 직접 실행용).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from peetsfea_runner.edt_aedt_backend import RealEdtBackend, _grpc_port_open, default_ansysedt_executable  # noqa: E402


def main() -> int:
    executable = default_ansysedt_executable()
    print(f"[smoke] ansysedt 실행파일: {executable} (exists={Path(executable).exists()})")
    if not Path(executable).exists():
        print("[smoke] FAIL: ansysedt 실행파일이 없음")
        return 2

    work_dir = Path(__file__).resolve().parent.parent / "build" / "smoke_edt"
    work_dir.mkdir(parents=True, exist_ok=True)
    backend = RealEdtBackend(slot_id="smoke", executable=executable, work_dir=work_dir, grpc_startup_timeout=240.0)

    t0 = time.monotonic()
    print("[smoke] start() — ansysedt 기동 + 관리 세션 부착 ...")
    session = backend.start()
    print(f"[smoke] warm: pid={session.pid} grpc_port={session.grpc_port} ({time.monotonic()-t0:.1f}s)")
    assert backend.is_alive(), "기동 직후 alive 여야 함"
    assert _grpc_port_open(session.grpc_port), "grpc 포트가 열려 있어야 함"

    print("[smoke] lend() — 관리 세션 release, 좌표 반환 ...")
    lent = backend.lend()
    assert lent.grpc_port == session.grpc_port
    assert backend.is_alive(), "lend 후에도 ansysedt는 살아 있어야 함(라이선스 유지)"
    assert _grpc_port_open(lent.grpc_port), "lend 후에도 grpc 살아 있어야 함"

    print("[smoke] reclaim() — 관리 세션 재부착 ...")
    backend.reclaim()
    assert backend.is_alive()

    print("[smoke] kill() — SIGKILL ...")
    backend.kill()
    time.sleep(2.0)
    assert not backend.is_alive(), "kill 후 죽어 있어야 함"

    print(f"[smoke] PASS (총 {time.monotonic()-t0:.1f}s)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[smoke] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
