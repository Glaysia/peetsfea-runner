from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    QUEUED = "QUEUED"
    STAGED = "STAGED"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
    FAILED_LOCAL = "FAILED_LOCAL"
