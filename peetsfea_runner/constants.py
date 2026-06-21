from __future__ import annotations

from typing import Final


DEFAULT_SLURM_JOB_TIME_LIMIT: Final[str] = "00:45:00"

# --- AEDT 워커 ---------------------------------------------------------------
# 현행 production 경로는 1솔브 단명 컨테이너를 사용한다. 직접 entrypoint를 실행할 때의 기본도 1로 둔다.
SLOTS_PER_CONTAINER: Final[int] = 1
# 타이밍은 클러스터 CPU가 로컬 PC 대비 ~1.5배 느린 것을 반영해 모두 ×1.5 (실측: 1h→1.5h).
# 시뮬 1개 소프트 목표(~90분). 정보용(워치독 강제 아님).
SIM_SOFT_TARGET_SECONDS: Final[int] = 90 * 60
# 시뮬 자체 하드 abort(140분). peetsfea가 스스로 마지막 패스 리포트를 남기고 종료해야 함.
# 0.3.6 + 88워커 만재에선 지오메트리+메시+솔브 누적이 90분을 넘는 *유효* 솔브가 많다(성공 평균 53·최대 91분).
# 90/98분이면 슬로우테일이 BackstopTimeout으로 죽어 데이터가 0이 됐다(실측 실패의 최다 원인). 140/150분으로 올려 살린다.
SIM_HARD_ABORT_SECONDS: Final[int] = 140 * 60
# edtmgr 백스톱(150분): 대여한 채 이 시간까지 미반환이면 SIGKILL 후 재기동(하드 abort 140분 + 여유).
EDTMGR_BACKSTOP_KILL_SECONDS: Final[int] = 150 * 60

# --- SLURM 잡 출생 제어 -----------------------------------------------------
# 레거시 keepalive 모드의 기본 슬롯 수. 현행 관리 루프는 별도 squeue cap(15)을 사용한다.
JOBS_PER_ACCOUNT: Final[int] = 9
# 잡 수명 상한. 잡=고정 인프라(적분제어)라 길게 유지 — 4시간 주기 교체(드리프트/잔재 위생용).
JOB_MAX_LIFETIME_SECONDS: Final[int] = 4 * 60 * 60

# --- Phase 6: 아카이브 저장소 ------------------------------------------------
# 묶음 압축 임계(20GB): 완료 project_dir들을 누적하다 이만큼 모이면 한 파일로 압축.
ARCHIVE_BATCH_BYTES: Final[int] = 20 * 2**30
# 버퍼 상한(2TB): 초과 시 가장 오래된 묶음 파일부터 FIFO 삭제. (로컬 디스크 여유 확인 필요; EDT_BULK_BUFFER로 조정.)
ARCHIVE_BUFFER_BYTES: Final[int] = 2 * 2**40
# 7877 대용량 .aedt 백채널 포트(슈퍼컴 전용; 7876=결과 JSON, 7877=project_dir tar 스트림).
BULK_TRANSFER_PORT: Final[int] = 7877

EXIT_CODE_SUCCESS: Final[int] = 0
EXIT_CODE_SSH_FAILURE: Final[int] = 10
EXIT_CODE_SLURM_FAILURE: Final[int] = 11
EXIT_CODE_SCREEN_FAILURE: Final[int] = 12
EXIT_CODE_REMOTE_RUN_FAILURE: Final[int] = 13
EXIT_CODE_DOWNLOAD_FAILURE: Final[int] = 14
EXIT_CODE_REMOTE_CLEANUP_FAILURE: Final[int] = 15
