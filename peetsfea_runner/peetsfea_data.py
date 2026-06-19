"""Resolve packaged peetsfea TOML data files.

peetsfea 0.3.x uses package data names such as ``0.3.x_sweep.toml`` while the
TOML body carries the exact spec version. Keep runner code from hard-coding a
specific filename so minor 0.3.8.x drops can change packaging without breaking
the control plane.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence, cast

TomlKind = Literal["sweep", "fixed"]

_TOML_NAME_RE = re.compile(r"^(?P<version>[0-9]+(?:\.(?:[0-9]+|x|X))*)_(?P<kind>sweep|fixed)\.toml$")


@dataclass(frozen=True)
class _TomlCandidate:
    path: Path
    version_parts: tuple[int | None, ...]
    kind: TomlKind


def _peetsfea_data_dir() -> Path:
    import peetsfea

    return Path(peetsfea.__file__).resolve().parent / "data"


def _installed_peetsfea_version() -> str:
    import peetsfea

    return str(peetsfea.__version__)


def _parse_version_parts(version: str) -> tuple[int | None, ...]:
    parts: list[int | None] = []
    for raw_part in version.split("."):
        part = raw_part.strip()
        if part.lower() == "x":
            parts.append(None)
            continue
        if not part.isdecimal():
            break
        parts.append(int(part))
    return tuple(parts)


def _candidate_from_path(path: Path) -> _TomlCandidate | None:
    match = _TOML_NAME_RE.match(path.name)
    if match is None:
        return None
    kind_text = match.group("kind")
    if kind_text not in {"sweep", "fixed"}:
        return None
    return _TomlCandidate(
        path=path,
        version_parts=_parse_version_parts(match.group("version")),
        kind=cast(TomlKind, kind_text),
    )


def _numeric_prefix(parts: tuple[int | None, ...]) -> tuple[int, ...]:
    numeric: list[int] = []
    for part in parts:
        if part is None:
            break
        numeric.append(part)
    return tuple(numeric)


def _score(candidate: _TomlCandidate, installed_parts: tuple[int, ...]) -> tuple[int, int, tuple[int, ...], str]:
    prefix = _numeric_prefix(candidate.version_parts)
    has_wildcard = any(part is None for part in candidate.version_parts)
    prefix_matches = len(prefix) <= len(installed_parts) and prefix == installed_parts[: len(prefix)]
    exact_match = not has_wildcard and prefix == installed_parts

    if exact_match:
        rank = 50
    elif not has_wildcard and prefix_matches:
        rank = 40
    elif has_wildcard and prefix_matches:
        rank = 30
    elif not has_wildcard:
        rank = 20
    else:
        rank = 10
    return (rank, len(prefix), prefix, candidate.path.name)


def find_peetsfea_data_toml(
    kind: TomlKind,
    *,
    data_dir: Path | None = None,
    installed_version: str | None = None,
) -> Path:
    """Return the best packaged peetsfea TOML for ``kind``.

    Selection prefers files compatible with the installed peetsfea version. A
    wildcard label such as ``0.3.x`` is treated as compatible with every
    installed ``0.3.*`` version, which is the current peetsfea 0.3.8.x package
    convention.
    """

    root = data_dir if data_dir is not None else _peetsfea_data_dir()
    installed_parts_mixed = _parse_version_parts(installed_version or _installed_peetsfea_version())
    installed_parts = tuple(part for part in installed_parts_mixed if part is not None)
    candidates = [
        candidate
        for path in root.glob(f"*_{kind}.toml")
        if (candidate := _candidate_from_path(path)) is not None and candidate.kind == kind
    ]
    if not candidates:
        raise FileNotFoundError(f"no *_{kind}.toml found in {root}")
    return max(candidates, key=lambda candidate: _score(candidate, installed_parts)).path


def load_peetsfea_data_toml_text(kind: TomlKind, *, data_dir: Path | None = None) -> str:
    return find_peetsfea_data_toml(kind, data_dir=data_dir).read_text(encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1 or args[0] not in {"sweep", "fixed"}:
        print("usage: python -m peetsfea_runner.peetsfea_data {sweep|fixed}", file=sys.stderr)
        return 2
    print(find_peetsfea_data_toml(cast(TomlKind, args[0])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["TomlKind", "find_peetsfea_data_toml", "load_peetsfea_data_toml_text", "main"]
