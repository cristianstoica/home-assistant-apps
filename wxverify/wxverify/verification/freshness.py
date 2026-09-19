"""The one freshness entry point both surfaces call (D17).

Thin by design. ``verification.manifest`` owns the derivation and imports
from ``verification.runs``, so a read-snapshot bracket around the whole
published-run derivation cannot live in either of them without closing a
cycle; it belongs here, above both, where the entire component comparison
can happen inside one bracket. The surfaces import
:func:`published_input_freshness` and :data:`RUN_INPUTS_NO_RUN` from this
module and nothing else in the freshness layer.
"""

from __future__ import annotations

import sqlite3

from wxverify.verification.manifest import (
    RUN_INPUTS_NO_RUN,
    RunInputFreshness,
    run_input_freshness,
)

__all__ = ["RUN_INPUTS_NO_RUN", "published_input_freshness"]


def published_input_freshness(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    run_id: int | None,
    recorded_basis: str | None,
    period_start: str | None,
    period_end: str | None,
) -> RunInputFreshness:
    """Input freshness of a site's published run -- the ONE call each surface makes.

    ``run_id is None`` returns :data:`RUN_INPUTS_NO_RUN`, so the facade is
    total and no caller has to construct that case itself. Both current
    surfaces still branch on the published run before calling -- they need
    the run row for verdicts and conclusions -- so this arm is defensive
    today. Everything else delegates to :func:`run_input_freshness` on the
    connection handed in, which is the whole derivation's only connection.
    """
    if run_id is None:
        return RUN_INPUTS_NO_RUN
    return run_input_freshness(
        conn,
        site_id,
        run_id=run_id,
        recorded_basis=recorded_basis,
        period_start=period_start,
        period_end=period_end,
    )
