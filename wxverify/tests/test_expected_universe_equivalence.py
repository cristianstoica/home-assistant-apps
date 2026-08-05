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
47/25 lat-lon convention, no real station/device identifiers.
"""

from __future__ import annotations

import math
import sqlite3

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import VARIABLES as FORECAST_SERVICE_VARIABLES
from wxverify.obs.qc import TARGET_VARIABLES
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.composite import (
    MAX_DAY_AHEAD,
    _expected_active_cells,
    composite_with_status,
)
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import _expected_active_feed_ids, resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.context import _scoring_feeds
from wxverify.worker.backfill import BACKFILL_VARIABLES

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
        VALUES (1, 'Test Site', 47.0, 25.0, 900.0, 'UTC')
        """
    )
    return conn


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _insert_meteoblue_member(conn: sqlite3.Connection, model: str) -> int:
    """No default meteoblue MEMBER feed is seeded (only the package) --
    following ``test_forecast_data.py::_seed_ranking_exclusion_fixture``."""
    return int(
        conn.execute(
            """
            INSERT INTO feeds
                (source, model, enabled, default_subscribed,
                 fetch_interval_minutes, max_lead_hours, is_virtual)
            VALUES ('meteoblue', ?, 1, 0, 360, 168, 0)
            """,
            (model,),
        ).lastrowid
    )


def _subscribe_package(conn: sqlite3.Connection, site_id: int, enabled: int) -> None:
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, ?)",
        (site_id, package_id, enabled),
    )


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
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, 11.0, 10.0, 1.0, 1.0, 1.0)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, day_ahead),
    )


def _reference_expected_cells(
    conn: sqlite3.Connection, *, site_id: int, cutoff: str | None
) -> set[tuple[int, str, int]]:
    """Independent DISTINCT-form reference for the composite cell universe.

    Deliberately NOT reusing ``_live_composite`` (which runs the full skill
    pipeline) or ``active_feed_cte``/``cell_grid_cte`` (the rewrite under
    test) -- only the shared, unchanged ``active_competitor_clause``
    predicate, in its column-expression form (``fp.site_id``, zero extra
    bind params), mirroring the pre-rewrite query shape.
    """
    window_clause = "" if cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (site_id,) if cutoff is None else (site_id, cutoff)
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id, fp.variable, fp.day_ahead
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        """,
        params,
    ).fetchall()
    return {
        (int(row["feed_id"]), str(row["variable"]), int(row["day_ahead"]))
        for row in rows
    }


def _reference_expected_feed_ids(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    cutoff: str | None,
) -> set[int]:
    window_clause = "" if cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (
        (site_id, variable, day_ahead)
        if cutoff is None
        else (site_id, variable, day_ahead, cutoff)
    )
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND fp.day_ahead = ?
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        """,
        params,
    ).fetchall()
    return {int(row["feed_id"]) for row in rows}


def _reference_scoring_feeds(
    conn: sqlite3.Connection, *, site_id: int, variable: str
) -> list[tuple[int, str, str]]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id, f.source, f.model
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND {active_competitor_clause(site_expr="fp.site_id")}
        ORDER BY f.source, f.model
        """,
        (site_id, variable),
    ).fetchall()
    return [(int(r["feed_id"]), str(r["source"]), str(r["model"])) for r in rows]


# ---------------------------------------------------------------------------
# _expected_active_cells (composite.py) vs the DISTINCT reference.
# ---------------------------------------------------------------------------


def test_expected_cells_match_distinct_form_across_windows() -> None:
    """A realistic multi-feed, multi-variable, multi-day_ahead fixture,
    covering: an always-on virtual feed, a meteoblue member with the
    package SUBSCRIBED (site A) and one with the package UNSUBSCRIBED
    (site B, default), a feed disabled at the feed level, and a
    default_subscribed=False feed with a site-level override -- checked
    against the DISTINCT reference for BOTH cache-backed windows
    ("rolling" and "all")."""
    conn = _make_db()
    site_a = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Site A', 47.0, 25.0, 900.0, 'UTC')"
        ).lastrowid
    )
    site_b = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Site B', 47.0, 25.0, 900.0, 'UTC')"
        ).lastrowid
    )
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    disabled_id = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (disabled_id,))
    override_id = _feed_id(conn, "visualcrossing", "blend")  # default_subscribed=0
    member_a = _insert_meteoblue_member(conn, "AIFS025")
    member_b = _insert_meteoblue_member(conn, "NCEP_GEFS")

    _subscribe_package(conn, site_a, enabled=1)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
        (site_a, override_id),
    )
    # Site B leaves the package at its default (unsubscribed) -- member_b is
    # NOT a competitor there despite carrying pairs.

    for site_id, feed_id in (
        (site_a, persistence_id),
        (site_a, member_a),
        (site_a, override_id),
        (site_a, disabled_id),
        (site_b, persistence_id),
        (site_b, member_b),
    ):
        for variable in ("temperature", "wind"):
            for i, (valid_at, lead_hours) in enumerate(
                zip(_FAR_VALID_ATS, _FAR_LEAD_HOURS, strict=True)
            ):
                _add_pair(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    variable=variable,
                    day_ahead=i,
                    valid_at=valid_at,
                    lead_hours=lead_hours,
                )
    # An out-of-cutoff pair, visible only under "all".
    _add_pair(
        conn,
        site_id=site_a,
        feed_id=persistence_id,
        variable="precip",
        day_ahead=3,
        valid_at=_OLD_VALID_AT,
        lead_hours=_OLD_LEAD_HOURS,
    )

    for site_id in (site_a, site_b):
        for window in ("rolling", "all"):
            resolved = resolve_window(conn, window)
            actual = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
            expected = _reference_expected_cells(
                conn, site_id=site_id, cutoff=resolved.cutoff
            )
            assert actual == expected, (site_id, window)

    # Sanity: the disabled feed and the unsubscribed member never appear at
    # all, in either representation (would silently pass above if BOTH
    # forms were wrong the same way).
    resolved_a = resolve_window(conn, "all")
    cells_a = _expected_active_cells(conn, site_id=site_a, resolved=resolved_a)
    assert all(feed_id != disabled_id for feed_id, _, _ in cells_a)
    resolved_b = resolve_window(conn, "all")
    cells_b = _expected_active_cells(conn, site_id=site_b, resolved=resolved_b)
    assert all(feed_id != member_b for feed_id, _, _ in cells_b)


def test_expected_cells_exclude_pairs_outside_the_window_cutoff() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _add_pair(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=_OLD_VALID_AT,
        lead_hours=_OLD_LEAD_HOURS,
    )
    rolling = resolve_window(conn, "rolling")
    all_window = resolve_window(conn, "all")

    rolling_cells = _expected_active_cells(conn, site_id=1, resolved=rolling)
    all_cells = _expected_active_cells(conn, site_id=1, resolved=all_window)

    assert (feed_id, "temperature", 1) not in rolling_cells
    assert (feed_id, "temperature", 1) in all_cells
    assert rolling_cells == _reference_expected_cells(
        conn, site_id=1, cutoff=rolling.cutoff
    )
    assert all_cells == _reference_expected_cells(conn, site_id=1, cutoff=None)


# ---------------------------------------------------------------------------
# _expected_active_feed_ids (leaderboard.py) vs the DISTINCT reference.
# ---------------------------------------------------------------------------


def test_expected_feed_ids_match_distinct_form_across_variables_and_leads() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")

    for feed_id in (feed_a, feed_b, persistence_id):
        for variable in ("temperature", "precip"):
            for i, (valid_at, lead_hours) in enumerate(
                zip(_FAR_VALID_ATS, _FAR_LEAD_HOURS, strict=True)
            ):
                _add_pair(
                    conn,
                    site_id=1,
                    feed_id=feed_id,
                    variable=variable,
                    day_ahead=i + 2,
                    valid_at=valid_at,
                    lead_hours=lead_hours,
                )

    populated_cells = {
        ("temperature", 2),
        ("temperature", 3),
        ("precip", 2),
        ("precip", 3),
    }
    for window in ("rolling", "all"):
        resolved = resolve_window(conn, window)
        for variable in FORECAST_VARIABLES:
            for day_ahead in range(MAX_DAY_AHEAD + 1):
                actual = _expected_active_feed_ids(
                    conn,
                    site_id=1,
                    variable=variable,
                    day_ahead=day_ahead,
                    resolved=resolved,
                )
                expected = _reference_expected_feed_ids(
                    conn,
                    site_id=1,
                    variable=variable,
                    day_ahead=day_ahead,
                    cutoff=resolved.cutoff,
                )
                assert actual == expected, (window, variable, day_ahead)
                if (variable, day_ahead) in populated_cells:
                    assert actual == {feed_a, feed_b, persistence_id}
                else:
                    # A cell nobody wrote a pair for -> both forms agree
                    # it is empty (not merely "equal to each other").
                    assert actual == set()


# ---------------------------------------------------------------------------
# _scoring_feeds (context.py) vs the DISTINCT reference, INCLUDING order.
# ---------------------------------------------------------------------------


def test_scoring_feeds_match_distinct_form_including_order() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # source=open-meteo
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")  # source=open-meteo
    persistence_id = _feed_id(conn, "virtual", "_persistence")  # source=virtual

    for feed_id in (feed_a, feed_b, persistence_id):
        _add_pair(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable="wind",
            day_ahead=0,
            valid_at=_FAR_VALID_ATS[0],
            lead_hours=_FAR_LEAD_HOURS[0],
        )

    actual = [(ft.id, ft.source, ft.model) for ft in _scoring_feeds(conn, 1, "wind")]
    expected = _reference_scoring_feeds(conn, site_id=1, variable="wind")
    assert actual == expected
    # Non-vacuous: source-alphabetical order actually differs from insertion
    # order here (open-meteo before virtual), so this pins ORDER BY, not
    # merely set membership.
    assert [row[1] for row in actual] == ["open-meteo", "open-meteo", "virtual"]


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
    assert set(FORECAST_VARIABLES) == set(BACKFILL_VARIABLES)


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


# ---------------------------------------------------------------------------
# Stray/out-of-range score_cache rows must not wedge the
# composite window forever. Both tests carry a paired negative: without it,
# "adding a stray row and still getting `hit`" could just mean the drop
# logic is a no-op that never gets exercised.
# ---------------------------------------------------------------------------


def _seed_canonical_cell(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 1)
        """,
        (site_id, feed_id),
    )


def _seed_persistence_and_feed_pairs(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, persistence_id: int
) -> None:
    observed = 10.0
    persistence_sq_error = 4.0
    feed_sq_error = 2.0
    feed_error = math.sqrt(feed_sq_error)
    persistence_error = math.sqrt(persistence_sq_error)
    for valid_hour in (0, 1):
        valid_at = f"2035-01-02T{valid_hour:02d}:00:00Z"
        issued_at = f"2035-01-01T{valid_hour:02d}:00:00Z"
        conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', ?, ?, 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                persistence_id,
                issued_at,
                valid_at,
                observed + persistence_error,
                observed,
                persistence_error,
                abs(persistence_error),
                persistence_sq_error,
            ),
        )
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', ?, ?, 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                valid_at,
                observed + feed_error,
                observed,
                feed_error,
                abs(feed_error),
                feed_sq_error,
            ),
        )


def _seed_fresh_complete_snapshot(
    conn: sqlite3.Connection, *, site_id: int, resolved_window: str = "rolling"
) -> tuple[str, str, int]:
    resolved = resolve_window(conn, resolved_window)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    expected_cells = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
    computed_at = isoformat_utc()
    for feed_id, variable, day_ahead in expected_cells:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=resolved.window_key,
            result=result,
            computed_at=computed_at,
        )
    return resolved.window_key, computed_at, min_n


def test_stray_variable_cell_does_not_wedge_the_composite_window() -> None:
    conn = _make_db()
    set_setting(conn, "min_n", "2")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_canonical_cell(conn, site_id=1, feed_id=feed_id)
    _seed_persistence_and_feed_pairs(
        conn, site_id=1, feed_id=feed_id, persistence_id=persistence_id
    )
    window_key, computed_at, min_n = _seed_fresh_complete_snapshot(conn, site_id=1)

    baseline = composite_with_status(conn, site_id=1, window="rolling")
    assert baseline.status == "hit"
    baseline_counts = {row["feed_id"]: row["component_count"] for row in baseline.rows}

    # Stray forecast_pairs row for the SAME active feed with a non-canonical
    # variable -- no CHECK forbids it, so this is exactly what a worker bug
    # could materialize.
    _add_pair(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="humidity",
        day_ahead=1,
        valid_at="2035-01-02T02:00:00Z",
        lead_hours=24,
        issued_at="2035-01-01T02:00:00Z",
    )
    # A matching score_cache row a worker rescore would leave behind: same
    # window_key/computed_at as the canonical snapshot.
    stray_result = strategy_for("temperature").aggregate(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=resolve_window(conn, "rolling").cutoff,
        min_n=min_n,
    )
    upsert_score_cache(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="humidity",
        day_ahead=1,
        window_key=window_key,
        result=stray_result,
        computed_at=computed_at,
    )

    result = composite_with_status(conn, site_id=1, window="rolling")
    assert result.status == "hit"
    for row in result.rows:
        assert row["component_count"] == baseline_counts[row["feed_id"]]

    # Paired negative: drop the canonical siblings, leaving ONLY the stray
    # row -- the allowlist must not turn every partial snapshot into a
    # false hit; a genuinely incomplete one still reports `rebuilding`.
    conn.execute("DELETE FROM score_cache WHERE site_id=1 AND variable != 'humidity'")
    negative = composite_with_status(conn, site_id=1, window="rolling")
    assert negative.status == "rebuilding"


def test_out_of_range_lead_cache_cell_does_not_wedge_the_composite_window() -> None:
    conn = _make_db()
    set_setting(conn, "min_n", "2")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_canonical_cell(conn, site_id=1, feed_id=feed_id)
    _seed_persistence_and_feed_pairs(
        conn, site_id=1, feed_id=feed_id, persistence_id=persistence_id
    )
    window_key, computed_at, min_n = _seed_fresh_complete_snapshot(conn, site_id=1)

    baseline = composite_with_status(conn, site_id=1, window="rolling")
    assert baseline.status == "hit"
    baseline_counts = {row["feed_id"]: row["component_count"] for row in baseline.rows}

    # A score_cache row with day_ahead beyond MAX_DAY_AHEAD -- score_cache
    # carries NO CHECK constraint on day_ahead, so a stale/mis-migrated
    # import could leave one behind with no corresponding forecast_pairs row
    # at all.
    stray_result = strategy_for("temperature").aggregate(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=resolve_window(conn, "rolling").cutoff,
        min_n=min_n,
    )
    upsert_score_cache(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=MAX_DAY_AHEAD + 1,
        window_key=window_key,
        result=stray_result,
        computed_at=computed_at,
    )

    result = composite_with_status(conn, site_id=1, window="rolling")
    assert result.status == "hit"
    for row in result.rows:
        assert row["component_count"] == baseline_counts[row["feed_id"]]

    # Paired negative: drop the canonical siblings, leaving only the
    # out-of-range row -- must still be `rebuilding`, not a false `hit`.
    conn.execute(
        f"DELETE FROM score_cache WHERE site_id=1 AND day_ahead != {MAX_DAY_AHEAD + 1}"
    )
    negative = composite_with_status(conn, site_id=1, window="rolling")
    assert negative.status == "rebuilding"
