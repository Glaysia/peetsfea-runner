"""컨테이너 entrypoint — 잡 컨테이너 안에서 슬롯 서비스를 실행 (Phase 2).

각 SLURM 잡의 enroot 컨테이너가 이걸 돌린다: `build_steady_state_service`로 2-레인 큐(baseline
자기공급) + admission + edtmgr 슬롯 풀을 띄우고, peetsfea 진짜 프리미티브로 시뮬을 돌려 공유 DB에
기록한다. SIGTERM(=scancel)에 깨끗이 멈추고, 테스트용으로 `EDT_MAX_SIMS`만큼만 돌고 종료할 수 있다.

설정은 환경변수로 받는다(컨테이너 친화):
  EDT_OUTPUT_ROOT   결과 산출 루트(필수)
  EDT_DB_PATH       결과 DuckDB 경로(필수; 공유 $HOME 권장)
  EDT_WORK_DIR      잡 작업 디렉토리(job_disk; 미설정 시 OUTPUT_ROOT/work)
  EDT_SLOT_COUNT    슬롯 수(기본 SLOTS_PER_CONTAINER)
  EDT_REFERENCE_SWEEP  baseline용 기준 sweep toml 경로(없으면 baseline 휴면)
  EDT_MAX_SIMS      이만큼 처리 후 종료(테스트용; 0/미설정이면 무한)
  EDT_DISABLE_LB    "1"이면 로드밸런서 끄기
"""

from __future__ import annotations

import os
import signal
import threading
from pathlib import Path
from types import FrameType

from .constants import SLOTS_PER_CONTAINER
from .edt_service import EdtServiceConfig, SteadyStateService, build_steady_state_service


def _config_from_env() -> tuple[EdtServiceConfig, int]:
    output_root = Path(os.environ["EDT_OUTPUT_ROOT"]).expanduser()
    db_path = Path(os.environ["EDT_DB_PATH"]).expanduser()
    work_dir_env = os.environ.get("EDT_WORK_DIR")
    work_dir = Path(work_dir_env).expanduser() if work_dir_env else output_root / "work"
    slot_count = int(os.environ.get("EDT_SLOT_COUNT", str(SLOTS_PER_CONTAINER)))
    ref_env = os.environ.get("EDT_REFERENCE_SWEEP")
    reference_sweep_text = Path(ref_env).expanduser().read_text(encoding="utf-8") if ref_env else None
    max_sims = int(os.environ.get("EDT_MAX_SIMS", "0"))
    import socket

    config = EdtServiceConfig(
        output_root=output_root,
        db_path=db_path,
        slot_count=slot_count,
        work_dir=work_dir,
        reference_sweep_text=reference_sweep_text,
        enable_load_balancer=os.environ.get("EDT_DISABLE_LB") != "1",
        baseline_batch_size=int(os.environ.get("EDT_BASELINE_BATCH", "1000")),
        partition=os.environ.get("EDT_PARTITION", ""),  # 자동 벤치마크용 파티션/노드
        node=socket.gethostname(),
    )
    return config, max_sims


def run_slot_service(service: SteadyStateService, *, max_sims: int = 0) -> int:
    """슬롯 서비스를 실행. SIGTERM/SIGINT에 멈추고, max_sims>0이면 그만큼 처리 후 종료."""
    dispatcher = service.dispatcher

    def _handle_signal(signum: int, frame: FrameType | None) -> None:
        dispatcher.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    if max_sims > 0:
        # 처리 수가 max_sims에 도달하면 stop()을 거는 워치 스레드.
        def _watch() -> None:
            while not dispatcher._stop.is_set():  # noqa: SLF001
                if dispatcher.processed >= max_sims:
                    dispatcher.stop()
                    return
                dispatcher._stop.wait(2.0)  # noqa: SLF001

        threading.Thread(target=_watch, name="edt-maxsims", daemon=True).start()

    processed = dispatcher.run()
    for slot in dispatcher.slots:
        slot.backend.kill()
    return processed


def main() -> int:
    config, max_sims = _config_from_env()
    service = build_steady_state_service(config)
    # EDT_PRIORITY_TOML: 고정 후보를 우선순위 레인에 시드(검증/단발 처리용; baseline보다 먼저 소비).
    priority_toml = os.environ.get("EDT_PRIORITY_TOML")
    if priority_toml:
        from .edt_queue import QueueItem

        text = Path(priority_toml).expanduser().read_text(encoding="utf-8")
        service.queue.put_priority(QueueItem(request_id="prio-0", candidate_toml_text=text, mode="full"))
    print(f"[entrypoint] slots={config.slot_count} lb={'on' if service.admission else 'off'} max_sims={max_sims or '∞'} priority={'1' if priority_toml else '0'}", flush=True)
    processed = run_slot_service(service, max_sims=max_sims)
    print(f"[entrypoint] processed={processed}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
