from __future__ import annotations

FAMILY_ROOTS = frozenset({"0.3.8"})


def normalize_peetsfea_version_filter(version: str | None) -> str | None:
    value = (version or "").strip()
    if not value:
        return None
    lowered = value.lower()
    if lowered == "0.3.8.x" or value == "0.3.8" or value.startswith("0.3.8."):
        return "0.3.8"
    return value


def peetsfea_version_filter_clause(
    column: str, version: str | None, *, placeholder: str
) -> tuple[str, list[str]]:
    normalized = normalize_peetsfea_version_filter(version)
    if normalized is None:
        return "", []
    if normalized in FAMILY_ROOTS:
        return f"({column} = {placeholder} OR {column} LIKE {placeholder})", [
            normalized,
            f"{normalized}.%",
        ]
    return f"{column} = {placeholder}", [normalized]


__all__ = ["normalize_peetsfea_version_filter", "peetsfea_version_filter_clause"]
