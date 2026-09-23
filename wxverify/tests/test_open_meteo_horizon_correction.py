"""Open-Meteo horizon correction oracles -- obs-204 per-model request bounds.

Covers ``wxverify.config.OPEN_METEO_MAX_LEAD_HOURS`` (the per-model request
ceiling table that replaces the old flat 168h seed) and
``wxverify.db.migrations.correct_open_meteo_horizons``, the one-shot,
marker-gated data correction that raises an existing database's seven
Open-Meteo feed rows to those same horizons. Fixture and assertion style
mirrors ``tests/test_google_horizon_correction.py``.

Every fixture is built with ``create_schema`` + hand-inserted ``feeds``
rows (mirrors the Google-horizon suite), never through ``init_db``/a real
file, because these oracles only need a bare schema plus a handful of
``feeds`` rows.

Synthetic data only: the seven feed rows use the product's own public
Open-Meteo model identifiers (not station, site, or coordinate data).
"""

from __future__ import annotations

import sqlite3

from wxverify import config
from wxverify.db.migrations import (
    OPEN_METEO_HORIZON_CORRECTION_KEY,
    correct_open_meteo_horizons,
    create_schema,
    run_migrations,
)
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.feeds.open_meteo import RUN_CADENCE_HOURS
from wxverify.forecast.service import DAY_COUNT

_OPEN_METEO_MODELS = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "meteofrance_arpege_world",
    "jma_gsm",
    "ukmo_global_deterministic_10km",
)

_EXPECTED_HOURS: dict[str, int] = {
    "ecmwf_ifs": 217,
    "gfs_global": 217,
    "icon_global": 180,
    "gem_global": 217,
    "meteofrance_arpege_world": 168,
    "jma_gsm": 217,
    "ukmo_global_deterministic_10km": 168,
}


def _bare_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _insert_feed(
    conn: sqlite3.Connection,
    *,
    source: str,
    model: str,
    max_lead_hours: int,
    enabled: int = 1,
    disabled_reason: str | None = None,
    default_subscribed: int = 1,
    fetch_interval_minutes: int = 360,
    is_virtual: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO feeds
            (source, model, enabled, disabled_reason, default_subscribed,
             fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source,
            model,
            enabled,
            disabled_reason,
            default_subscribed,
            fetch_interval_minutes,
            max_lead_hours,
            is_virtual,
        ),
    )


def _seed_seven_at_168(conn: sqlite3.Connection) -> None:
    for model in _OPEN_METEO_MODELS:
        _insert_feed(conn, source="open-meteo", model=model, max_lead_hours=168)


def _lead(conn: sqlite3.Connection, *, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT max_lead_hours FROM feeds WHERE source = ? AND model = ?",
        (source, model),
    ).fetchone()
    assert row is not None, f"expected a ({source}, {model}) feed row"
    return int(row["max_lead_hours"])


def _assert_all_seven(conn: sqlite3.Connection, expected: dict[str, int]) -> None:
    for model, hours in expected.items():
        assert _lead(conn, source="open-meteo", model=model) == hours, model


# ---------------------------------------------------------------------------
# 1. Fresh-database seed
# ---------------------------------------------------------------------------


def test_fresh_database_seeds_all_seven_open_meteo_feeds_at_approved_horizons() -> None:
    """A brand-new database (``run_migrations`` on an empty file) seeds all
    seven Open-Meteo feeds at their approved horizons in one pass -- the
    old flat-168 seed would fail every assertion below except the two
    models that happen to still land on 168.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    _assert_all_seven(conn, _EXPECTED_HOURS)
    assert _lead(conn, source="open-meteo", model="icon_global") == 180
    assert _lead(conn, source="open-meteo", model="meteofrance_arpege_world") == 168
    assert (
        _lead(conn, source="open-meteo", model="ukmo_global_deterministic_10km") == 168
    )


# ---------------------------------------------------------------------------
# 2. Existing-database correction
# ---------------------------------------------------------------------------


def test_existing_database_all_at_168_corrected_to_approved_horizons() -> None:
    """A database whose seven Open-Meteo feeds all sit at the old flat 168h
    seed is corrected by ``run_migrations`` to the approved per-model
    horizons -- including the two models with no downward correction, which
    a mutant that applies ``DISPLAY_REQUEST_HOURS`` uniformly (rather than
    per-model) would bump to 217 here.
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    run_migrations(conn)
    _assert_all_seven(conn, _EXPECTED_HOURS)
    assert _lead(conn, source="open-meteo", model="meteofrance_arpege_world") == 168
    assert (
        _lead(conn, source="open-meteo", model="ukmo_global_deterministic_10km") == 168
    )


# ---------------------------------------------------------------------------
# 3. Idempotence
# ---------------------------------------------------------------------------


def test_correction_run_twice_changes_nothing_on_the_second_pass() -> None:
    conn = _bare_db()
    _seed_seven_at_168(conn)
    correct_open_meteo_horizons(conn)
    after_first = {
        model: _lead(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }
    assert after_first == _EXPECTED_HOURS
    assert get_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY) == "applied"

    correct_open_meteo_horizons(conn)
    after_second = {
        model: _lead(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }
    assert after_second == after_first


# ---------------------------------------------------------------------------
# 4. The marker, not the WHERE clause, is the gate
# ---------------------------------------------------------------------------


def test_marker_present_blocks_recorrection_of_a_reset_row() -> None:
    """Marker already present, every row hand-set (via the fixture) to 168
    -- ``correct_open_meteo_horizons`` must leave them at 168. This is the
    assertion an unconditional per-model ``UPDATE ... WHERE max_lead_hours
    = 168`` (no marker gate) would FAIL: without the marker check, every
    row would be corrected right back up on this very call. The marker's
    presence is what makes the row's current value the one respected, not
    the ``WHERE`` clause's match.
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    set_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY, "applied")

    correct_open_meteo_horizons(conn)

    for model in _OPEN_METEO_MODELS:
        assert _lead(conn, source="open-meteo", model=model) == 168


# ---------------------------------------------------------------------------
# 5. Crash-safety ordering
# ---------------------------------------------------------------------------


def test_partial_prior_correction_is_completed_on_the_next_run() -> None:
    """Simulates a crash after SOME of the seven ``UPDATE``s ran but before
    the marker write: two models are hand-corrected to their approved
    values while the marker is left unset. The next call must finish the
    remaining five and write the marker -- an implementation that instead
    trusted an early marker-absence check to mean "nothing done yet" and
    re-applied the WHERE clause naively would still converge here (the
    WHERE clause is ``= 168``, so already-corrected rows are simply
    skipped), which is exactly the guarantee under test.
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    conn.execute(
        "UPDATE feeds SET max_lead_hours = 217"
        " WHERE source = 'open-meteo' AND model = 'ecmwf_ifs'"
        " AND max_lead_hours = 168"
    )
    conn.execute(
        "UPDATE feeds SET max_lead_hours = 217"
        " WHERE source = 'open-meteo' AND model = 'gfs_global'"
        " AND max_lead_hours = 168"
    )
    assert get_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY) is None

    correct_open_meteo_horizons(conn)

    _assert_all_seven(conn, _EXPECTED_HOURS)
    assert get_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY) == "applied"


# ---------------------------------------------------------------------------
# 6. Multiple matching rows corrected in one pass
# ---------------------------------------------------------------------------


def test_correction_updates_every_matching_model_not_just_the_first() -> None:
    """``feeds`` carries ``UNIQUE(source, model)`` (migrations.py ~line
    108), so a single ``(source, model)`` pair can never hold two rows --
    "multiple matching rows" is realized across the seven per-model
    ``UPDATE``s the loop issues in one call, not within a single
    statement. This kills an implementation that stops after the first
    model (an early ``break``, or a hardcoded single ``UPDATE`` for
    ``ecmwf_ifs`` alone): only one model would end up corrected instead of
    five.
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    correct_open_meteo_horizons(conn)
    corrected = {
        model
        for model in _OPEN_METEO_MODELS
        if _lead(conn, source="open-meteo", model=model) != 168
    }
    assert corrected == {
        "ecmwf_ifs",
        "gfs_global",
        "gem_global",
        "jma_gsm",
        "icon_global",
    }


# ---------------------------------------------------------------------------
# 7. Parity
# ---------------------------------------------------------------------------


def test_open_meteo_model_set_and_hours_match_cadence_map_and_feed_seeds() -> None:
    """The key set of ``OPEN_METEO_MAX_LEAD_HOURS`` equals the key set of
    ``feeds.open_meteo.RUN_CADENCE_HOURS`` AND the key set of
    ``OPEN_METEO_FETCH_INTERVAL_MINUTES`` (Item E's fetch-interval table),
    and all three equal the set of Open-Meteo models ``FEED_SEEDS`` builds.
    Updated by Item E (E-T9) to cover the third, independently authored
    mapping rather than duplicating this test -- adding a model to one of
    the three without the other two now fails here. The expected hours are
    pinned as literals here, not read back from ``OPEN_METEO_MAX_LEAD_HOURS``
    -- ``FEED_SEEDS`` derives from that mapping, so comparing the two would
    be self-confirming.

    ``RUN_AVAILABILITY_LAG_MINUTES`` is deliberately excluded from this
    parity: it is a comprehension over ``RUN_CADENCE_HOURS``
    (``wxverify/feeds/open_meteo.py``), so a fourth key-set conjunct would be
    true for every possible program state and would weaken the assertion by
    making one of its terms unfalsifiable. ``test_open_meteo_run_attribution
    .py``'s E-T18 covers that table with the property it can actually
    violate.
    """
    assert dict(config.OPEN_METEO_MAX_LEAD_HOURS) == _EXPECTED_HOURS
    assert (
        set(config.OPEN_METEO_MAX_LEAD_HOURS)
        == set(config.OPEN_METEO_FETCH_INTERVAL_MINUTES)
        == set(RUN_CADENCE_HOURS)
    )
    seed_models = {
        seed.model for seed in config.FEED_SEEDS if seed.source == "open-meteo"
    }
    assert seed_models == set(_EXPECTED_HOURS)


# ---------------------------------------------------------------------------
# 10. The derivation is pinned
# ---------------------------------------------------------------------------


def test_display_request_hours_derives_from_forecast_service_day_count() -> None:
    """Keeps ``config.DISPLAY_REQUEST_HOURS``'s literal and
    ``forecast.service.DAY_COUNT`` from silently diverging -- a comment in
    ``config.py`` promises this relationship holds even though ``config``
    deliberately does not import ``forecast.service`` (which would close
    an import cycle).
    """
    assert config.DISPLAY_REQUEST_HOURS == (DAY_COUNT + 1) * 24 + 1


# ---------------------------------------------------------------------------
# 11. Virtual feeds untouched
# ---------------------------------------------------------------------------


def test_virtual_feeds_keep_168_through_the_migration() -> None:
    conn = _bare_db()
    _seed_seven_at_168(conn)
    _insert_feed(
        conn,
        source="virtual",
        model="_persistence",
        max_lead_hours=168,
        default_subscribed=0,
        fetch_interval_minutes=1440,
        is_virtual=1,
    )
    _insert_feed(
        conn,
        source="virtual",
        model="_multimodel_mean",
        max_lead_hours=168,
        default_subscribed=0,
        fetch_interval_minutes=1440,
        is_virtual=1,
    )
    correct_open_meteo_horizons(conn)
    assert _lead(conn, source="virtual", model="_persistence") == 168
    assert _lead(conn, source="virtual", model="_multimodel_mean") == 168


# ---------------------------------------------------------------------------
# 9. The WHERE clause, not just the marker, protects an operator override
# ---------------------------------------------------------------------------


def test_operator_lowered_horizon_and_foreign_source_survive_the_correction() -> None:
    """Two collisions the ``UPDATE``'s ``WHERE`` clause alone must resolve,
    with the marker left unset so only the ``WHERE`` clause is under test:

    - ``icon_global`` is hand-set to 96 (an operator's deliberate downward
      override) BEFORE the correction runs. ``AND max_lead_hours = 168``
      is what leaves it alone; drop that predicate and the per-model
      ``UPDATE`` matches every ``open-meteo``/``icon_global`` row
      regardless of its current value and overwrites 96 with 180. The
      other six feeds, still at 168, must still move to their expected
      targets in the same call -- without that second assertion, a
      correction that does nothing at all would also leave icon_global at
      96 and pass.
    - A synthetic ``meteoblue``/``ecmwf_ifs`` row at 168 shares its model
      name with a real Open-Meteo model but not its source.
      ``AND source = 'open-meteo'`` is what leaves it alone; drop that
      predicate and the ``ecmwf_ifs`` ``UPDATE`` (keyed only on ``model``)
      matches this row too and raises it to 217.
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    conn.execute(
        "UPDATE feeds SET max_lead_hours = 96"
        " WHERE source = 'open-meteo' AND model = 'icon_global'"
    )
    _insert_feed(conn, source="meteoblue", model="ecmwf_ifs", max_lead_hours=168)
    assert get_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY) is None

    correct_open_meteo_horizons(conn)

    assert _lead(conn, source="open-meteo", model="icon_global") == 96
    for model, hours in _EXPECTED_HOURS.items():
        if model == "icon_global":
            continue
        assert _lead(conn, source="open-meteo", model=model) == hours, model
    assert _lead(conn, source="meteoblue", model="ecmwf_ifs") == 168


# ---------------------------------------------------------------------------
# 10a. Correction never moves the schema version
# ---------------------------------------------------------------------------


def test_correction_alone_never_moves_user_version() -> None:
    """Mirrors ``tests/test_google_horizon_correction.py``'s
    ``test_correction_alone_never_moves_user_version`` (line 213):
    ``correct_open_meteo_horizons`` is a data fix and must leave ``PRAGMA
    user_version`` exactly where it found it. Driven by a DIRECT call,
    because ``run_migrations`` writes ``PRAGMA user_version``
    unconditionally on its way out and would mask a version write added
    inside the correction. The sentinel 99 is deliberately no real schema
    version -- seeded at the real target version the most likely mutant (a
    correction that bumps to the current target) would still pass.
    """
    conn = _bare_db()
    conn.execute("PRAGMA user_version = 99")
    _seed_seven_at_168(conn)

    correct_open_meteo_horizons(conn)

    row = conn.execute("PRAGMA user_version").fetchone()
    assert int(row[0]) == 99


# ---------------------------------------------------------------------------
# 12. Everything else untouched
# ---------------------------------------------------------------------------


def test_operator_editable_columns_and_non_open_meteo_rows_untouched() -> None:
    """The four operator-editable columns (``enabled``, ``disabled_reason``,
    ``fetch_interval_minutes``, ``default_subscribed``) and all
    non-Open-Meteo feed rows are unchanged by the correction. Set to
    non-default values first so the assertion can fail on an
    ``UPSERT``-style reconciliation pass that would reset them (the exact
    hazard the docstring on ``correct_open_meteo_horizons`` names).
    """
    conn = _bare_db()
    _seed_seven_at_168(conn)
    conn.execute(
        "UPDATE feeds SET enabled = 0, disabled_reason = 'operator-paused',"
        " fetch_interval_minutes = 720, default_subscribed = 0"
        " WHERE source = 'open-meteo' AND model = 'ecmwf_ifs'"
    )
    _insert_feed(conn, source="meteoblue", model="multimodel", max_lead_hours=168)
    _insert_feed(conn, source="google", model="blend", max_lead_hours=168)

    correct_open_meteo_horizons(conn)

    row = conn.execute(
        "SELECT enabled, disabled_reason, fetch_interval_minutes,"
        " default_subscribed, max_lead_hours"
        " FROM feeds WHERE source = 'open-meteo' AND model = 'ecmwf_ifs'"
    ).fetchone()
    assert (
        row["enabled"],
        row["disabled_reason"],
        row["fetch_interval_minutes"],
        row["default_subscribed"],
    ) == (0, "operator-paused", 720, 0)
    assert int(row["max_lead_hours"]) == 217
    assert _lead(conn, source="meteoblue", model="multimodel") == 168
    assert _lead(conn, source="google", model="blend") == 168
