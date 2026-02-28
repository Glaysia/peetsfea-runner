from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    NEW = "NEW"
    PENDING = "PENDING"
    UPLOADED = "UPLOADED"
    FAILED_UPLOAD = "FAILED_UPLOAD"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
    FAILED_LOCAL = "FAILED_LOCAL"
