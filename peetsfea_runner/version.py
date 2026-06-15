from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import tomllib


_VERSION_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}\.\d+$")


def _default_pyproject_path() -> Path:
    return Path(__file__).resolve().parent.parent / "pyproject.toml"


def validate_pyproject_version(data: dict[str, Any]) -> str:
    project = data.get("project")
    if not isinstance(project, dict):
        raise ValueError("Missing [project] table")
    version = project.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("Missing project.version")
    normalized = version.strip()
    if not _VERSION_PATTERN.fullmatch(normalized):
        raise ValueError(f"Invalid project.version '{normalized}'; expected YYYY.MM.DD.N")
    return normalized


def load_version(pyproject_path: Path | None = None) -> str:
    target = (pyproject_path or _default_pyproject_path()).expanduser().resolve()
    if not target.is_file():
        raise FileNotFoundError(f"pyproject.toml not found: {target}")
    data = tomllib.loads(target.read_text(encoding="utf-8"))
    try:
        return validate_pyproject_version(data)
    except ValueError as exc:
        raise ValueError(f"{exc} in {target}") from exc


@lru_cache(maxsize=1)
def get_version() -> str:
    try:
        return load_version()
    except (FileNotFoundError, ValueError):
        # 설치 환경(pyproject.toml 없음): 패키지 메타데이터로 폴백.
        try:
            from importlib.metadata import version as _meta_version

            return _meta_version("peetsfea-runner")
        except Exception:
            return "0.0.0+unknown"
