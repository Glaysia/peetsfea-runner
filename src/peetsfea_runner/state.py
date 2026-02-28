from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    NEW = "NEW"
    PENDING = "PENDING"
    UPLOADED = "UPLOADED"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
    FAILED_LOCAL = "FAILED_LOCAL"
