from __future__ import annotations

from typing import Final


DEFAULT_SLURM_JOB_TIME_LIMIT: Final[str] = "00:45:00"

# --- edtmgr / 슬롯 (Phase 1) -------------------------------------------------
# 컨테이너당 상시 기동하는 ansysedt(=슬롯) 수.
SLOTS_PER_CONTAINER: Final[int] = 10
# 시뮬 1개 소프트 목표(~40분). 정보용(워치독 강제 아님).
SIM_SOFT_TARGET_SECONDS: Final[int] = 40 * 60
# 시뮬 자체 하드 abort(60분). peetsfea가 스스로 마지막 패스 리포트를 남기고 종료해야 함.
SIM_HARD_ABORT_SECONDS: Final[int] = 60 * 60
# edtmgr 백스톱(65분): 대여한 채 이 시간까지 미반환이면 SIGKILL 후 재기동.
EDTMGR_BACKSTOP_KILL_SECONDS: Final[int] = 65 * 60

# --- Phase 2: 9잡 / ~100 동시 오케스트레이션 -------------------------------
# 계정당 SLURM 잡 수(= 컨테이너 수).
JOBS_PER_ACCOUNT: Final[int] = 9
# 잡 수명(10h). 만료 시 진행 중 시뮬을 그냥 폐기(드레인 없음, Q8) 후 재기동.
JOB_MAX_LIFETIME_SECONDS: Final[int] = 10 * 60 * 60
# 유연 토폴로지(Phase 2/3): edtmgr가 항상 보유하는 warm 하한 / 컨테이너당 슬롯 상한.
WARM_FLOOR_PER_CONTAINER: Final[int] = 11
MAX_SLOTS_PER_CONTAINER: Final[int] = 16

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
