"""Lead-bucket parsing helpers."""

from __future__ import annotations


def parse_day_ahead(lead: str) -> int:
    normalized = lead.strip().replace(" ", "+")
    if normalized.startswith("D+"):
        return int(normalized[2:])
    return int(normalized)
