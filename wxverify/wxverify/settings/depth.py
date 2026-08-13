"""Per-variable blend-depth resolution with provenance (§15).

The Forecast page, the forecast-of-record builder, and the verification
run snapshot all resolve depth through this ONE helper, so the live page,
the record, and the run incumbent can never disagree. An override key
(``forecast_blend_depth_<variable>``) wins when it parses to 1..6; anything
else — absent, foreign, out of range — falls through to the existing
global ``forecast_blend_depth`` setting.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Literal

from wxverify.settings.keys import get_number_setting, get_setting

#: The variables that accept a per-variable depth override (§15), and the
#: CANONICAL forecast-variable roster: the forecast page, sample validation,
#: the record builder and the verification simulator all derive their own
#: tuples from this one (NB-4) so the set can never drift between them.
DEPTH_VARIABLES: tuple[str, ...] = ("temperature", "wind", "precip")

DEPTH_MIN = 1
DEPTH_MAX = 6

DepthSource = Literal["global", "override"]


def depth_override_key(variable: str) -> str:
    """Settings key of the variable's optional depth override."""
    return f"forecast_blend_depth_{variable}"


@dataclass(frozen=True)
class EffectiveDepth:
    """A resolved depth plus where it came from (§15 provenance)."""

    depth: int
    source: DepthSource


def effective_blend_depth(conn: sqlite3.Connection, variable: str) -> EffectiveDepth:
    """Resolve one variable's effective depth: valid override, else global."""
    raw = get_setting(conn, depth_override_key(variable))
    if raw is not None:
        try:
            value = int(raw)
        except ValueError:
            value = None
        if value is not None and DEPTH_MIN <= value <= DEPTH_MAX:
            return EffectiveDepth(depth=value, source="override")
    return EffectiveDepth(
        depth=get_number_setting(conn, "forecast_blend_depth", 2, minimum=1),
        source="global",
    )


def effective_blend_depths(conn: sqlite3.Connection) -> dict[str, EffectiveDepth]:
    """Effective depth + provenance for every depth-configurable variable."""
    return {v: effective_blend_depth(conn, v) for v in DEPTH_VARIABLES}
