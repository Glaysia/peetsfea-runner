"""실 클러스터 Phase 2 end-to-end — SLURM 잡이 entrypoint로 실 슬롯 서비스 1 sim을 푼다.

SlurmJobLauncher로 잡 1개 제출 → 잡이 enroot 컨테이너에서 `peetsfea_runner.edt_entrypoint`
실행(EDT_MAX_SIMS=1) → edtmgr 슬롯 + peetsfea 0.3.4 실 프리미티브로 실 HFSS solve →
결과 기록 → 종료. slurm out에서 solve/processed 마커를 확인한다.

    .venv/bin/python tests/verify_slurm_slot_service.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from peetsfea_runner.edt_slurm_launcher import SlurmJobLauncher  # noqa: E402

SSH_HOST = "gate1-harry261"


def ssh(remote: str, timeout: float = 60) -> str:
    proc = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SSH_HOST, remote],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return proc.stdout


def main() -> int:
    launcher = SlurmJobLauncher(
        ssh_host=SSH_HOST,
        partitions=("cpu2",),  # 검증은 cpu2 고정
        time_limit="00:45:00",
        cpus_cpu2=8,
        mem="32G",
        job_name_prefix="peetsfea-slotsvc",
        job_command="bash $HOME/edt-deploy/run_slot_job.sh",
    )
    handle = launcher.submit(0)
    sid = handle.slurm_id
    print(f"[slotsvc] submitted slurm_id={sid}", flush=True)

    deadline = time.monotonic() + 2700  # 45분
    while time.monotonic() < deadline and launcher.is_alive(handle):
        time.sleep(20)
    print(f"[slotsvc] job alive after wait: {launcher.is_alive(handle)} ({time.monotonic()-handle.started_at:.0f}s)", flush=True)

    out = ssh(f"cat $HOME/slurm-{sid}.out 2>/dev/null")
    solved = "solved correctly" in out
    processed = "[entrypoint] processed=1" in out
    print("--- slurm out tail ---", flush=True)
    print("\n".join(out.strip().splitlines()[-15:]), flush=True)
    print(f"[slotsvc] solved={solved} processed_1={processed}", flush=True)

    if solved and processed:
        print("[slotsvc] SLURM_SLOT_SERVICE_PASS", flush=True)
        return 0
    print("[slotsvc] FAIL", flush=True)
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"[slotsvc] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
