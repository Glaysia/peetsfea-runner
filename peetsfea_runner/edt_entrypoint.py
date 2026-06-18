"""컨테이너 entrypoint — 잡 컨테이너 안에서 슬롯 서비스를 실행 (Phase 2).

각 SLURM 잡의 enroot 컨테이너가 이걸 돌린다: `build_steady_state_service`로 2-레인 큐(baseline
자기공급) + admission + edtmgr 슬롯 풀을 띄우고, peetsfea 진짜 프리미티브로 시뮬을 돌려 공유 DB에
기록한다. SIGTERM(=scancel)에 깨끗이 멈추고, 테스트용으로 `EDT_MAX_SIMS`만큼만 돌고 종료할 수 있다.

**process-per-slot (문제 3 정식 수정):** pyaedt의 Desktop은 프로세스 전역 싱글톤이라 한 프로세스에 슬롯이
여럿이면 `release_desktop()`이 서로의 전역 상태를 깨뜨린다. 그래서 `EDT_SLOT_COUNT=N`이면 entrypoint는
**supervisor**가 되어 단일슬롯 워커를 **N개 별도 OS 프로세스로 spawn**한다(각 프로세스 = AEDT 1개 = Desktop 1개,
충돌 없음). 워커는 `EDT_WORKER_INDEX`가 박힌 같은 entrypoint로, 자기 baseline seed 범위·출력 하위디렉토리를 갖는다.

설정은 환경변수로 받는다(컨테이너 친화):
  EDT_OUTPUT_ROOT   결과 산출 루트(필수)
  EDT_RESULT_INGEST_URL  결과 push 대상(기본 http://127.0.0.1:7876/ingest = gate 경유 로컬 데몬).
                    설정/기본값이면 결과를 로컬 단일 DB로 push(슈퍼컴엔 DB 없음).
  EDT_DB_PATH       (단독 모드 전용) 로컬 DuckDB 경로. ingest를 끄려면 EDT_RESULT_INGEST_URL=""로.
  EDT_WORK_DIR      잡 작업 디렉토리(job_disk; 미설정 시 OUTPUT_ROOT/work)
  EDT_SLOT_COUNT    슬롯 수 = spawn할 워커 프로세스 수(>1이면 supervisor 모드)
  EDT_WORKER_INDEX  (내부) 설정돼 있으면 단일슬롯 워커로 동작(supervisor가 주입)
  EDT_REFERENCE_SWEEP  baseline용 기준 sweep toml 경로(없으면 baseline 휴면)
  EDT_PARTITION     이 잡이 뜬 SLURM 파티션(자동 벤치마크 기록용)
  EDT_MAX_SIMS      이만큼 처리 후 종료(테스트용; 0/미설정이면 무한)
  EDT_DISABLE_LB    "1"이면 로드밸런서 끄기
  EDT_WORKER_STAGGER_SECONDS  워커 스폰 간격(기본 15s; AEDT 동시 콜드스타트 회피)
  EDT_WORKER_SEED_STRIDE  워커별 baseline seed 간격(기본 1e6; request_id 충돌 방지)
  EDT_JOB_INDEX     이 잡의 인덱스(0..N-1). seed 대역을 잡마다 분리(잡 간 중복탐색/덮어쓰기 방지).
  EDT_GPU_COUNT     이 노드에 할당된 GPU 수(>0이면 워커별 CUDA_VISIBLE_DEVICES=index%N로 GPU 분산 핀닝).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import FrameType

from collections.abc import Mapping
from typing import Any

from .constants import SLOTS_PER_CONTAINER
from .edt_bulk_transfer import DEFAULT_BULK_PORT, BulkPushSink
from .edt_result_ingest import DEFAULT_INGEST_PORT, HttpResultSink
from .edt_service import EdtServiceConfig, SteadyStateService, build_steady_state_service


def _config_from_env() -> tuple[EdtServiceConfig, int]:
    output_root = Path(os.environ["EDT_OUTPUT_ROOT"]).expanduser()
    # ingest 모드에선 DB를 안 만든다(슈퍼컴엔 DB 없음). db_path는 단독 모드 fallback용.
    db_path = Path(os.environ.get("EDT_DB_PATH", str(output_root / "results.duckdb"))).expanduser()
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
        baseline_batch_size=int(os.environ.get("EDT_BASELINE_BATCH", "16")),  # 작은 청크(백그라운드 보충)
        baseline_low_watermark=int(os.environ.get("EDT_BASELINE_WATERMARK", "64")),
        baseline_seed_start=int(os.environ.get("EDT_BASELINE_SEED_START", "0")),  # 워커별 seed 범위
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
    if service.baseline_refiller is not None:
        service.baseline_refiller.stop()  # 백그라운드 샘플 워커 정지.
    if service.priority_puller is not None:
        service.priority_puller.stop()  # 우선순위 lease 풀러 정지.
    for slot in dispatcher.slots:
        slot.backend.kill()
    return processed


def _worker_env(
    base_env: Mapping[str, str],
    index: int,
    output_root: Path,
    seed_stride: int,
    *,
    seed_base: int = 0,
    gpu_count: int = 0,
) -> dict[str, str]:
    """단일슬롯 워커용 환경: 워커마다 출력 하위디렉토리·seed 대역·GPU를 분리.

    seed 대역 = seed_base + index*seed_stride. seed_base에 잡 인덱스를 반영해(run_supervisor) **잡 간**
    중복 탐색/덮어쓰기를 막는다. GPU가 있으면 CUDA_VISIBLE_DEVICES로 워커를 GPU에 분산 핀닝한다.
    """
    env = dict(base_env)
    env["EDT_WORKER_INDEX"] = str(index)
    env["EDT_SLOT_COUNT"] = "1"
    env["EDT_OUTPUT_ROOT"] = str(output_root / f"worker_{index:02d}")  # 워커별 출력 디렉토리
    env["EDT_BASELINE_SEED_START"] = str(seed_base + index * seed_stride)  # 잡·워커별 disjoint seed 대역
    if gpu_count > 0:
        # 워커를 물리 GPU에 라운드로빈 핀닝 → 11워커가 GPU 0..N-1에 분산(GPU0 쏠림·VRAM OOM 방지).
        # peetsfea는 nvidia-smi로 GPU를 감지하지만, 실제 HFSS solve는 CUDA_VISIBLE_DEVICES에 갇힌다.
        env["CUDA_VISIBLE_DEVICES"] = str(index % gpu_count)
    env.pop("EDT_WORK_DIR", None)  # 각 워커가 자기 output_root/work를 쓰게(공유 금지)
    env.pop("EDT_PRIORITY_TOML", None)  # 우선순위는 호스트측; 워커는 baseline 자기공급
    env.pop("EDT_MAX_SIMS", None)
    return env


def _spawn_worker(
    index: int, output_root: Path, seed_stride: int, *, seed_base: int = 0, gpu_count: int = 0
) -> subprocess.Popen[bytes]:
    """단일슬롯 워커를 별도 OS 프로세스로 spawn. 각 워커 = AEDT 1개 = pyaedt Desktop 1개(충돌 없음)."""
    env = _worker_env(os.environ, index, output_root, seed_stride, seed_base=seed_base, gpu_count=gpu_count)
    return subprocess.Popen([sys.executable, "-m", "peetsfea_runner.edt_entrypoint"], env=env)


def run_supervisor(slot_count: int) -> int:
    """`EDT_SLOT_COUNT=N`이면 단일슬롯 워커 N개를 별도 프로세스로 띄우고 감독한다(process-per-slot).

    스태거 기동으로 AEDT 동시 콜드스타트를 피하고, 죽은 워커는 재기동, SIGTERM에 전부 정리한다.
    """
    output_root = Path(os.environ["EDT_OUTPUT_ROOT"]).expanduser()
    stagger = float(os.environ.get("EDT_WORKER_STAGGER_SECONDS", "15"))
    seed_stride = int(os.environ.get("EDT_WORKER_SEED_STRIDE", "1000000"))
    gpu_count = int(os.environ.get("EDT_GPU_COUNT", "0"))
    # 잡 인덱스로 seed 대역을 잡마다 분리: 잡 j의 워커들은 [j*slot_count*stride, (j+1)*slot_count*stride).
    # 이게 없으면 모든 잡의 worker i가 같은 seed를 탐색 → N배 중복 계산·서로 덮어씀(고질적 낭비).
    job_index = int(os.environ.get("EDT_JOB_INDEX", "0"))
    # EDT_BASELINE_SEED_EPOCH: 런처(keeper)가 store의 used-seed 프런티어 위로 잡아 주입. 재램프해도 이 위에서
    # 시작해 **이미 푼 설계를 재탕하지 않고** 새 공간을 탐색한다(0이면 기존 동작). 잡 band는 job_index로 분리.
    seed_epoch = int(os.environ.get("EDT_BASELINE_SEED_EPOCH", "0"))
    seed_base = seed_epoch + job_index * slot_count * seed_stride
    stop = threading.Event()

    def _on_signal(signum: int, frame: FrameType | None) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _on_signal)

    procs: dict[int, subprocess.Popen[bytes]] = {}
    print(
        f"[supervisor] spawning {slot_count} workers job={job_index} seed_base={seed_base} "
        f"gpu_count={gpu_count} (stagger={stagger:.0f}s)",
        flush=True,
    )
    for i in range(slot_count):
        if stop.is_set():
            break
        procs[i] = _spawn_worker(i, output_root, seed_stride, seed_base=seed_base, gpu_count=gpu_count)
        print(f"[supervisor] worker {i:02d} pid={procs[i].pid}", flush=True)
        stop.wait(stagger)  # 스태거(ramp-up)

    while not stop.is_set():  # 감독 루프: 죽은 워커 재기동
        for i, p in list(procs.items()):
            if p.poll() is not None and not stop.is_set():
                print(f"[supervisor] worker {i:02d} exited rc={p.returncode}; restarting", flush=True)
                procs[i] = _spawn_worker(i, output_root, seed_stride, seed_base=seed_base, gpu_count=gpu_count)
        stop.wait(5.0)

    print("[supervisor] stopping all workers", flush=True)
    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    deadline = time.monotonic() + 120.0
    for p in procs.values():
        try:
            p.wait(timeout=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            p.kill()
    return 0


def main() -> int:
    # supervisor 분기: EDT_SLOT_COUNT>1 이고 워커가 아니면 N개 워커 프로세스로 분리(pyaedt one-Desktop-per-process).
    slot_count = int(os.environ.get("EDT_SLOT_COUNT", str(SLOTS_PER_CONTAINER)))
    if slot_count > 1 and os.environ.get("EDT_WORKER_INDEX") is None:
        return run_supervisor(slot_count)

    config, max_sims = _config_from_env()
    # 결과 sink: 기본은 ingest push(:7876, gate 경유 로컬 단일 DB). EDT_RESULT_INGEST_URL=""면 로컬 DuckDB 단독.
    ingest_url = os.environ.get("EDT_RESULT_INGEST_URL", f"http://127.0.0.1:{DEFAULT_INGEST_PORT}/ingest")
    # 대용량 산출물 push(:7877): 성공 시 project_dir(aedt)를 로컬로 보내고 gpfs 원본 삭제. ""면 비활성(보관).
    bulk_host = os.environ.get("EDT_BULK_HOST", "127.0.0.1")
    bulk_port = int(os.environ.get("EDT_BULK_PORT", str(DEFAULT_BULK_PORT)))
    bulk = BulkPushSink(host=bulk_host, port=bulk_port) if bulk_host else None

    record = None
    if ingest_url:
        sink = HttpResultSink(url=ingest_url, fallback_dir=config.output_root / "ingest_spool")

        def record(envelope: Mapping[str, Any]) -> None:
            sink.record(envelope)  # 결과 JSON → 로컬 단일 DB(:7876)
            # 성공한 시뮬의 산출물(project_dir)을 :7877로 전송 → 아카이브 + gpfs 절약.
            # 실패 산출물 정리는 record(=솔브 도중)에서 하면 peetsfea의 telemetry 쓰기와 레이스(FileNotFound)다.
            # → orchestrator가 **컨테이너 완전 종료 후** 컨테이너별 output 디렉토리째 삭제(reap).
            if bulk is not None and envelope.get("terminal_state") == "success":
                out = envelope.get("output_dir")
                if isinstance(out, str) and out:
                    bulk.push(str(envelope.get("request_id") or ""), Path(out))

    service = build_steady_state_service(config, record=record)
    # EDT_PRIORITY_TOML: 고정 후보를 우선순위 레인에 시드(검증/단발 처리용; baseline보다 먼저 소비).
    priority_toml = os.environ.get("EDT_PRIORITY_TOML")
    if priority_toml:
        from .edt_queue import QueueItem

        text = Path(priority_toml).expanduser().read_text(encoding="utf-8")
        service.queue.put_priority(QueueItem(request_id="prio-0", candidate_toml_text=text, mode="full"))
    # EDT_PRIORITY_LEASE_URL: 컨트롤플레인(:7878, gate 정터널)에서 sweep chunk를 당겨 **컨테이너에서 샘플링**
    # 후 로컬 레인 보충. 블렌드(85:15)는 service.queue.get()이 처리. 무거운 cadquery는 여기(컨테이너)서만 돈다.
    lease_url = os.environ.get("EDT_PRIORITY_LEASE_URL")
    if lease_url:
        from .edt_priority_lease import PriorityPuller

        def _container_sampler(sweep_text: str, count: int, seed: int) -> list[str]:
            from peetsfea.ssw_design_space import sample_fixed_candidates_from_toml_text

            return list(sample_fixed_candidates_from_toml_text(sweep_text, count, seed))

        puller = PriorityPuller(
            queue=service.queue,
            lease_url=lease_url,
            sampler=_container_sampler,
            batch=int(os.environ.get("EDT_PRIORITY_LEASE_BATCH", "8")),
            low_watermark=int(os.environ.get("EDT_PRIORITY_LEASE_WATERMARK", "8")),
        )
        puller.start()
        service.priority_puller = puller
    # EDT_LICENSE_CTRL_URL: 중앙 라이선스 제어기(:7879). 솔브 직전 permit 획득(전역 상한), 솔브 중 abort 수신.
    ctrl_url = os.environ.get("EDT_LICENSE_CTRL_URL")
    if ctrl_url:
        import socket as _socket

        from .edt_license_ctrl import LicensePermitClient

        widx = os.environ.get("EDT_WORKER_INDEX", "0")
        jidx = os.environ.get("EDT_JOB_INDEX", "0")
        worker_id = f"j{jidx}-w{widx}-{_socket.gethostname()}-{os.getpid()}"  # 워커 프로세스마다 고유
        service.dispatcher.permit_client = LicensePermitClient(ctrl_url=ctrl_url, worker_id=worker_id)
    worker = os.environ.get("EDT_WORKER_INDEX", "-")
    print(f"[entrypoint] worker={worker} slots={config.slot_count} seed_start={config.baseline_seed_start} lb={'on' if service.admission else 'off'} ingest={'on' if ingest_url else 'off'} max_sims={max_sims or '∞'} priority={'1' if priority_toml else '0'} lease={'on' if lease_url else 'off'} lic_ctrl={'on' if ctrl_url else 'off'}", flush=True)
    processed = run_slot_service(service, max_sims=max_sims)
    print(f"[entrypoint] processed={processed}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
