from __future__ import annotations

from typing import Final


EXIT_CODE_SUCCESS: Final[int] = 0
EXIT_CODE_SSH_FAILURE: Final[int] = 10
EXIT_CODE_SLURM_FAILURE: Final[int] = 11
EXIT_CODE_SCREEN_FAILURE: Final[int] = 12
EXIT_CODE_REMOTE_RUN_FAILURE: Final[int] = 13
EXIT_CODE_DOWNLOAD_FAILURE: Final[int] = 14
EXIT_CODE_REMOTE_CLEANUP_FAILURE: Final[int] = 15
