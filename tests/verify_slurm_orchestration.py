"""실 클러스터 Phase 2 오케스트레이션 검증 (AEDT 없이 저비용).

JobOrchestrator + 실 SlurmJobLauncher로 게이트노드에 짧은 잡 2개를 제출하고 lifecycle을 본다:
submit → 둘 다 RUNNING → 1개 scancel → 죽음 감지 → 재기동 → 다시 2개 → shutdown(전부 scancel).

    .venv/bin/python tests/verify_slurm_orchestration.py
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from peetsfea_runner.edt_orchestrator import JobOrchestrator  # noqa: E402
from peetsfea_runner.edt_slurm_launcher import SlurmJobLauncher  # noqa: E402


def wait_until(cond: Callable[[], bool], timeout: float, interval: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


def main() -> int:
    launcher = SlurmJobLauncher(
        ssh_host="gate1-harry261",
        partition="cpu2",
        time_limit="00:05:00",
        cpus_per_task=2,
        mem="2G",
        job_name_prefix="peetsfea-orch-verify",
        job_command="echo job $EDT_JOB_INDEX on $(hostname); sleep 180",
    )
    orch = JobOrchestrator(launcher=launcher, clock=time.monotonic, job_count=2, max_lifetime_seconds=1e9)
    try:
        print("[orch] ensure_running (submit 2) ...", flush=True)
        orch.ensure_running()
        handles = orch.handles()
        print(f"[orch] submitted: {[(h.job_index, h.slurm_id) for h in handles]}", flush=True)

        assert wait_until(lambda: orch.running_count() == 2, timeout=180), "2 잡이 RUNNING 안 됨"
        print("[orch] 둘 다 활성 ✓", flush=True)

        victim = handles[0]
        launcher.kill(victim)
        print(f"[orch] scancel job {victim.slurm_id} (강제 종료)", flush=True)
        assert wait_until(lambda: not launcher.is_alive(victim), timeout=120), "victim이 안 죽음"
        print("[orch] victim 종료 확인 ✓", flush=True)

        orch.poll()  # 죽음 감지 → 재기동
        assert orch.restarts >= 1, f"재기동 안 됨 (restarts={orch.restarts})"
        assert wait_until(lambda: orch.running_count() == 2, timeout=180), "2개로 복구 안 됨"
        print(f"[orch] 재기동 ✓ (restarts={orch.restarts}, running=2)", flush=True)

        print("[orch] CLUSTER_ORCH_PASS", flush=True)
        return 0
    finally:
        orch.shutdown()
        print("[orch] shutdown: 전 잡 scancel", flush=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"[orch] FAIL: {type(exc).__name__}: {exc}")
        raise SystemExit(1)
