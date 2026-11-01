"""Equivalence + safety-net tests for the read-path grid+EXISTS rewrites.

Covers three grid+EXISTS rewrites against independent, hand-written DISTINCT
reference queries built from the SAME shared predicate
(``active_competitor_clause``) the production code uses -- these tests pin
the GRID TRANSLATION (candidate enumeration + EXISTS probe vs a DISTINCT
scan), not the competitor predicate itself, which is exercised elsewhere
(``test_composite_cache_backed.py``, ``test_leaderboard_cache_backed.py``).
Also pins the drift-guard invariant that ``cell_grid_cte()``'s variable axis
covers every variable value the pairing writers actually emit, and the two
paired stray-cell/wedge regressions for the allowlist filter.

Isolation: every test opens its own fresh ``sqlite3.connect(":memory:")`` and
runs ``run_migrations`` (mirrors ``tests/test_forecast_data.py``).

Synthetic data only (public repo): fake site name, the repo's existing
40/-105 lat-lon convention, no real station/device identifiers.
"""

from __future__ import annotations

import sqlite3

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.db.migrations import run_migrations
from wxverify.db.switch import ensure_site_generation
from wxverify.forecast.service import VARIABLES as FORECAST_SERVICE_VARIABLES
from wxverify.obs.qc import TARGET_VARIABLES
from wxverify.scoring.composite import (
    MAX_DAY_AHEAD,
    _expected_active_cells,
)
from wxverify.scoring.leaderboard import resolve_window

_FAR_VALID_ATS = ("2035-06-30T00:00:00Z", "2035-06-30T01:00:00Z")
_FAR_LEAD_HOURS = (1, 2)
_OLD_VALID_AT = "2000-01-01T00:00:00Z"
_OLD_LEAD_HOURS = 1


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    return conn


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _add_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    valid_at: str,
    lead_hours: int,
    issued_at: str = "2035-06-29T00:00:00Z",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, 11.0, 10.0, 1.0, 1.0, 1.0, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            ensure_site_generation(conn, site_id),
        ),
    )


# ---------------------------------------------------------------------------
# The equivalence tests that used to live here (_expected_active_cells,
# _expected_active_feed_ids and _scoring_feeds each checked against an
# independent DISTINCT-form reference query) all ran composite_with_status
# or equivalent helpers with the daily close off (no day_close_site call,
# and _scoring_feeds(close_on=False) explicitly) -- the add-on's only
# install runs with the close on, where composite_with_status takes the
# _published_composite branch instead (composite.py:117-118). Removed along
# with their three DISTINCT-reference helpers
# (_reference_expected_cells/_reference_expected_feed_ids/
# _reference_scoring_feeds) and the stray/out-of-range score_cache guard
# tests below, which exercised the same close-off _cached_composite path.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Drift guards on the grid axes.
# ---------------------------------------------------------------------------


def test_cell_grid_covers_every_variable_the_pairing_writers_emit() -> None:
    """``cell_grid_cte()``'s variable axis is FORECAST_VARIABLES; every other
    module that materializes/consumes a (variable, ...) cell must agree, or
    the grid rewrite would silently exclude a variable a writer actually
    produces."""
    assert set(FORECAST_VARIABLES) == set(FORECAST_SERVICE_VARIABLES)
    assert set(FORECAST_VARIABLES) == set(TARGET_VARIABLES)


def test_cell_grid_lead_bound_matches_schema_check() -> None:
    """MAX_DAY_AHEAD must equal the forecast_pairs CHECK upper bound -- if
    the grid's lead axis were narrower, a real in-range day_ahead pair would
    be silently invisible to the composite/leaderboard universe; if wider,
    it would waste EXISTS probes on unreachable candidates."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _add_pair(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=MAX_DAY_AHEAD,
        valid_at=_FAR_VALID_ATS[0],
        lead_hours=_FAR_LEAD_HOURS[0],
    )
    resolved = resolve_window(conn, "all")
    cells = _expected_active_cells(conn, site_id=1, resolved=resolved)
    assert (feed_id, "temperature", MAX_DAY_AHEAD) in cells

    try:
        _add_pair(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=MAX_DAY_AHEAD + 1,
            valid_at=_FAR_VALID_ATS[1],
            lead_hours=_FAR_LEAD_HOURS[1],
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError(
            "forecast_pairs accepted a day_ahead beyond the schema CHECK --"
            " MAX_DAY_AHEAD may now be narrower than what the table allows"
        )
