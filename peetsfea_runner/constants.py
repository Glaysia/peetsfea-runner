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

EXIT_CODE_SUCCESS: Final[int] = 0
EXIT_CODE_SSH_FAILURE: Final[int] = 10
EXIT_CODE_SLURM_FAILURE: Final[int] = 11
EXIT_CODE_SCREEN_FAILURE: Final[int] = 12
EXIT_CODE_REMOTE_RUN_FAILURE: Final[int] = 13
EXIT_CODE_DOWNLOAD_FAILURE: Final[int] = 14
EXIT_CODE_REMOTE_CLEANUP_FAILURE: Final[int] = 15
