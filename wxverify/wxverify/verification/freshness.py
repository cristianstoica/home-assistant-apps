"""The one freshness entry point both surfaces call (D17).

Thin by design. ``verification.manifest`` owns the derivation and imports
from ``verification.runs``, so a read-snapshot bracket around the whole
published-run derivation cannot live in either of them without closing a
cycle; it belongs here, above both, where the entire component comparison
can happen inside one bracket. The surfaces import
:func:`published_basis_report` and :data:`RUN_INPUTS_NO_RUN` from this
module and nothing else in the freshness layer.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.db.snapshot import read_snapshot
from wxverify.settings.depth import EffectiveDepth, effective_blend_depths
from wxverify.verification.manifest import (
    RUN_INPUTS_NO_RUN,
    RunInputFreshness,
    run_input_freshness,
)
from wxverify.verification.runs import published_run_id

__all__ = [
    "RUN_INPUTS_NO_RUN",
    "PublishedBasisReport",
    "published_basis_report",
    "published_input_freshness",
]


def published_input_freshness(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    run_id: int | None,
    recorded_basis: str | None,
    period_start: str | None,
    period_end: str | None,
) -> RunInputFreshness:
    """Input freshness of a site's published run -- the ONE freshness
    derivation; :func:`published_basis_report` calls it inside its read
    snapshot.

    ``run_id is None`` returns :data:`RUN_INPUTS_NO_RUN`, so the facade is
    total and no caller has to construct that case itself.
    :func:`published_basis_report` branches on the pointer and on the run
    row before calling, so this arm is defensive today; the manifest
    suite's O26 is the only bare caller. Everything else delegates to
    :func:`run_input_freshness` on the connection handed in, which is the
    whole derivation's only connection.
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


@dataclass(frozen=True)
class PublishedBasisReport:
    """One site's published-run identity and input freshness, from ONE read snapshot.

    ``run_row`` is the ``verification_runs`` row the verdict describes, handed
    back so no caller re-reads it outside the snapshot -- that re-read is what
    would put the run's identity and its freshness at two instants. ``None``
    with a non-None ``run_id`` means the pointer names a row that does not
    exist; each caller keeps its own error for that, and ``freshness`` is not
    meaningful on that path.

    ``blend_depths`` is the LIVE effective depth per variable, read from the
    same instant, because the page renders its depth-mismatch flag beside
    this verdict. The API surface ignores it.

    Never hand ``run_row`` to ``wxverify.verification.read_cache._cached``
    or anything that caches it: that helper deepcopies what it stores and
    ``deepcopy`` raises on a ``sqlite3.Row``.
    """

    run_id: int | None
    run_row: sqlite3.Row | None
    freshness: RunInputFreshness
    failed_newer_attempt: bool
    blend_depths: dict[str, EffectiveDepth]


def published_basis_report(
    conn: sqlite3.Connection, site_id: int
) -> PublishedBasisReport:
    """The operator's freshness report for one site -- READ PATH, ONE snapshot.

    The ONE derivation both surfaces use: ``GET /api/verification/status``
    and the ``/verification`` page. Unlike :func:`published_input_freshness`,
    which judges whatever run identity it is handed, this reads every input
    itself inside a single WAL read snapshot, so the pointer, the run row,
    the manifest rows, the configuration, the truth rows, both arrivals
    probes and the newer-failed probe all describe one database instant.
    The writer is not blocked meanwhile. The guarantee is per site: nothing
    here claims that two sites' reports, or anything read outside this
    function, share an instant.

    Read-only, like everything it calls (NB-9). A database error propagates:
    it is an operational failure, never a freshness verdict.
    """
    with read_snapshot(conn, label="published_basis"):
        run_id = published_run_id(conn, site_id)
        run_row: sqlite3.Row | None = None
        freshness = RUN_INPUTS_NO_RUN
        if run_id is not None:
            run_row = conn.execute(
                "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if run_row is not None:
                freshness = published_input_freshness(
                    conn,
                    site_id,
                    run_id=run_id,
                    recorded_basis=run_row["result_basis_fingerprint"],
                    period_start=run_row["period_start"],
                    period_end=run_row["period_end"],
                )
        failed_newer = (
            conn.execute(
                """
                SELECT 1 FROM verification_runs
                WHERE site_id = ? AND state = 'failed' AND id > ? LIMIT 1
                """,
                (site_id, run_id if run_id is not None else 0),
            ).fetchone()
            is not None
        )
        # The page compares these against each verdict's PINNED depth and
        # renders the result beside this verdict, so they are read here, at
        # the verdict's instant, not by the caller at some other one.
        blend_depths = effective_blend_depths(conn)
    return PublishedBasisReport(
        run_id=run_id,
        run_row=run_row,
        freshness=freshness,
        failed_newer_attempt=failed_newer,
        blend_depths=blend_depths,
    )
