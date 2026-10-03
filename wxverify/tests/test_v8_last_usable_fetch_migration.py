"""§14.6 "S": the v8 ``site_feed_state.last_usable_fetch_at`` migration --
seed sub-cases, idempotent re-run, and fresh-vs-migrated column parity
(plan §10.2/§10.5).

Fixture rule (mirrors ``tests/test_daily_truth_admission_migration.py`` and
``tests/test_fetch_obs_partial_cycle.py::_v6_db``): a fresh ``create_schema``
already contains ``last_usable_fetch_at``, so a column-probe against it would
no-op. The seed-recomputation fixtures here instead take a different,
implementer-recommended route to a genuine v7 database: build a full v8 DB,
write real rows through ``persist_fetch_result`` (so the forecast_samples/
site_feed_state shapes are exactly what production writes, not hand-crafted
SQL), THEN drop the column with a real ``ALTER TABLE ... DROP COLUMN`` and
roll ``user_version`` back to 7 -- discarding the column's pre-drop values so
``run_migrations`` must genuinely recompute them from the stored samples, not
just leave a value that happened to already be correct. The plain
schema-parity fixture (``_bare_v7_via_drop``) uses the simpler
create_schema-and-drop route directly, matching the sibling modules' pattern,
since it never needs realistic row data.

Synthetic data only: one fixture site, the default-seeded
``open-meteo/{ecmwf_ifs,gfs_global,icon_global}`` feeds.
"""

from __future__ import annotations

import sqlite3

from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    create_schema,
    migrate_v8_last_usable_fetch_at,
    run_migrations,
)
from wxverify.feeds.seam import FetchResult, NormalizedSample

_SITE_ID = 1


def _sample(
    *,
    model: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float = 10.0,
) -> NormalizedSample:
    return NormalizedSample(
        model=model,
        variable="temperature",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=value,
        source_raw="{}",
        model_run_id="run-1",
    )


def _v8_db_with_site() -> sqlite3.Connection:
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


def _drop_stamp_column_and_roll_back(conn: sqlite3.Connection) -> None:
    """Genuinely remove the v8 column and roll ``user_version`` back to 7,
    discarding whatever values it held. Every real caller of this must
    first assert the column is present (proving the ALTER did something)
    and immediately after assert it is absent (the anti-vacuity guard)."""
    cols_before = {
        r["name"] for r in conn.execute("PRAGMA table_info(site_feed_state)")
    }
    assert "last_usable_fetch_at" in cols_before
    conn.execute("ALTER TABLE site_feed_state DROP COLUMN last_usable_fetch_at")
    conn.execute("PRAGMA user_version = 7")
    cols_after = {r["name"] for r in conn.execute("PRAGMA table_info(site_feed_state)")}
    assert "last_usable_fetch_at" not in cols_after


def _stamp(conn: sqlite3.Connection, feed_id: int) -> object:
    row = conn.execute(
        "SELECT last_usable_fetch_at FROM site_feed_state "
        "WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()
    return None if row is None else row["last_usable_fetch_at"]


# ---------------------------------------------------------------------------
# Seed sub-cases.
# ---------------------------------------------------------------------------


def test_seed_recovers_the_stamp_for_a_genuine_forward_fetch() -> None:
    conn = _v8_db_with_site()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="ecmwf_ifs",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T06:00:00Z",
                )
            ]
        ),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert _stamp(conn, feed_id) == "2026-07-20T12:00:00Z"  # sanity: v8 wrote it

    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)

    assert _stamp(conn, feed_id) == "2026-07-20T12:00:00Z"


def test_seed_leaves_a_no_op_fetch_null() -> None:
    """A no-op fetch (all ``lead_hours < 1``) sets ``last_run_at`` but
    inserts zero forecast_samples rows, so the seed's EXISTS clause can
    never be satisfied -- the recomputed stamp stays NULL."""
    conn = _v8_db_with_site()
    feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="gfs_global",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T00:00:00Z",
                    lead_hours=0,
                )
            ]
        ),
        fetched_at="2026-07-20T12:00:00Z",
    )
    row = conn.execute(
        "SELECT last_run_at FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()
    assert row is not None and row["last_run_at"] == "2026-07-20T12:00:00Z"

    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)

    assert _stamp(conn, feed_id) is None


def test_seed_leaves_a_duplicates_only_fetch_null() -> None:
    """The stored forecast_samples row's ``fetched_at`` is fixed at its
    FIRST insert; a later duplicate-only fetch still advances
    ``last_run_at`` to the new ``fetched_at``, but no sample row's
    ``fetched_at`` matches that new value, so the seed's EXISTS clause
    cannot fire and the recomputed stamp stays NULL."""
    conn = _v8_db_with_site()
    feed_id = _feed_id(conn, "open-meteo", "icon_global")
    sample = _sample(
        model="icon_global",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
    )
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[sample]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    outcome2 = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[sample]),
        fetched_at="2026-07-20T18:00:00Z",
    )
    assert outcome2.inserted_count == 0
    row = conn.execute(
        "SELECT last_run_at FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()
    assert row is not None and row["last_run_at"] == "2026-07-20T18:00:00Z"
    stored_fetched_at = conn.execute(
        "SELECT fetched_at FROM forecast_samples WHERE feed_id=?", (feed_id,)
    ).fetchone()["fetched_at"]
    assert stored_fetched_at == "2026-07-20T12:00:00Z"  # never moved by the duplicate

    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)

    assert _stamp(conn, feed_id) is None


def test_seed_recovers_meteoblue_package_stamp_from_member_samples() -> None:
    """The seed's ``fs.feed_id IN (...)`` clause must reach through to the
    member feeds' stored samples to recover the PACKAGE row's stamp -- a
    meteoblue package carries no forward samples of its own (plan §10.3).

    Mutant (plan §14.6, MX1): collapsing that ``IN (...)`` to
    ``fs.feed_id = sfs.feed_id`` looks only at samples stored directly under
    the package's own feed id, which never exist, so the EXISTS clause can
    never fire and the package row's stamp would stay NULL.
    """
    conn = _v8_db_with_site()
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="meteoblue",
        fetch_feed_id=package_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="ICON",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T06:00:00Z",
                ),
                _sample(
                    model="GFS05",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T06:00:00Z",
                    value=11.0,
                ),
            ]
        ),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert _stamp(conn, package_id) == "2026-07-20T12:00:00Z"  # sanity: v8 wrote it

    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)

    assert _stamp(conn, package_id) == "2026-07-20T12:00:00Z"


def test_seed_filters_invalid_only_samples_stamp_stays_null() -> None:
    """A fetch whose only stored samples at ``last_run_at`` are all
    out-of-range must NOT be recovered by the seed -- the writer itself
    never stamps such a fetch (:mod:`forecast_fetcher`'s ``has_valid``
    gate), so the seed's own validity filter must agree.

    Mutant (plan §14.6, MX2): deleting the
    ``AND NOT invalid_forecast_sample_sql(fs)`` line lets the seed's EXISTS
    clause fire on the stored-but-invalid row (its ``fetched_at`` still
    equals ``last_run_at``), wrongly recovering a stamp the writer refused
    to set.

    999.0 (not NaN) is used deliberately: a NaN value binds NULL, and the
    NOT NULL ``value`` column then makes ``INSERT OR IGNORE`` skip the row
    entirely, so nothing would be left in ``forecast_samples`` for the seed
    to (wrongly) match against.
    """
    conn = _v8_db_with_site()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="ecmwf_ifs",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T06:00:00Z",
                    value=999.0,  # outside temperature's (-90, 70) range
                )
            ]
        ),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.inserted_count == 1  # the row really is stored, not IGNOREd
    stored = conn.execute(
        "SELECT fetched_at FROM forecast_samples WHERE feed_id=?", (feed_id,)
    ).fetchone()
    assert stored is not None and stored["fetched_at"] == "2026-07-20T12:00:00Z"
    last_run_at = conn.execute(
        "SELECT last_run_at FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()["last_run_at"]
    assert last_run_at == "2026-07-20T12:00:00Z"
    assert _stamp(conn, feed_id) is None  # the writer itself never stamped this fetch

    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)

    assert _stamp(conn, feed_id) is None


def test_seed_is_idempotent_on_rerun() -> None:
    conn = _v8_db_with_site()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="ecmwf_ifs",
                    issued_at="2026-07-20T00:00:00Z",
                    valid_at="2026-07-20T06:00:00Z",
                )
            ]
        ),
        fetched_at="2026-07-20T12:00:00Z",
    )
    _drop_stamp_column_and_roll_back(conn)
    run_migrations(conn)
    first = _stamp(conn, feed_id)
    assert first == "2026-07-20T12:00:00Z"

    # A second feed, made eligible for seeding (a stored sample whose
    # fetched_at == last_run_at, satisfying the EXISTS clause) but ALREADY
    # carrying a stamp that DIFFERS from last_run_at -- simulating an
    # already-recovered/legacy value. Re-running against unchanged data on
    # `feed_id` alone would not catch a seed that dropped its
    # `sfs.last_usable_fetch_at IS NULL` guard, since the recomputed value
    # there already equals the stored one; this row makes a dropped guard
    # observably overwrite a genuinely different stamp.
    other_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=other_feed_id,
        result=FetchResult(
            samples=[
                _sample(
                    model="gfs_global",
                    issued_at="2026-07-21T00:00:00Z",
                    valid_at="2026-07-21T06:00:00Z",
                )
            ]
        ),
        fetched_at="2026-07-21T12:00:00Z",
    )
    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = ? "
        "WHERE site_id = ? AND feed_id = ?",
        ("2026-07-19T00:00:00Z", _SITE_ID, other_feed_id),
    )
    assert _stamp(conn, other_feed_id) == "2026-07-19T00:00:00Z"

    # Re-running the seed (and the whole migration) a second time must not
    # change an already-recovered stamp -- the writer's own persist_fetch_result
    # calls from now on are what advance it, never a second migration pass.
    migrate_v8_last_usable_fetch_at(conn)
    run_migrations(conn)

    assert _stamp(conn, feed_id) == first
    assert _stamp(conn, other_feed_id) == "2026-07-19T00:00:00Z"


# ---------------------------------------------------------------------------
# Migration parity.
# ---------------------------------------------------------------------------


def test_fresh_and_migrated_schemas_agree_on_site_feed_state_columns() -> None:
    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    run_migrations(fresh)
    fresh_cols = [
        tuple(r) for r in fresh.execute("PRAGMA table_xinfo(site_feed_state)")
    ]

    migrated = sqlite3.connect(":memory:")
    migrated.row_factory = sqlite3.Row
    migrated.execute("PRAGMA foreign_keys=ON")
    create_schema(migrated)
    _drop_stamp_column_and_roll_back(migrated)
    run_migrations(migrated)
    migrated_cols = [
        tuple(r) for r in migrated.execute("PRAGMA table_xinfo(site_feed_state)")
    ]

    assert fresh_cols == migrated_cols


def test_run_migrations_lands_on_user_version_eight_from_a_v7_db() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    _drop_stamp_column_and_roll_back(conn)

    run_migrations(conn)

    assert TARGET_USER_VERSION == 8
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 8


# ---------------------------------------------------------------------------
# Older-version upgrade (coordinator addendum): a populated pre-v7 database
# taken straight through run_migrations. On this path migrate_v8 runs before
# create_indexes, so this checks the seed's LOGICAL result only -- the plan's
# separate "Seed EQP" oracle (tests/test_query_plan_regressions.py) is what
# checks the query plan, against an already-indexed v8 database.
# ---------------------------------------------------------------------------


def _v6_db() -> sqlite3.Connection:
    """A genuinely-v6 database (below v7): a fresh schema with the v7 trio
    and the v8 stamp column all dropped by real ``ALTER TABLE ... DROP
    COLUMN`` calls, then ``user_version`` rolled back to 6 -- mirrors
    ``tests/test_fetch_obs_partial_cycle.py::_v6_db``, extended one column
    further for the v8 migration under test here.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute("ALTER TABLE sites DROP COLUMN last_obs_cycle_at")
    conn.execute("ALTER TABLE stations DROP COLUMN history_next_attempt_at")
    conn.execute("ALTER TABLE stations DROP COLUMN history_last_error")
    conn.execute("ALTER TABLE stations DROP COLUMN history_error_count")
    conn.execute("ALTER TABLE site_feed_state DROP COLUMN last_usable_fetch_at")
    conn.execute("PRAGMA user_version = 6")
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(site_feed_state)")}
    assert "last_usable_fetch_at" not in cols  # anti-vacuity guard
    return conn


def test_populated_v6_db_upgrade_seeds_per_row_by_the_logical_rule() -> None:
    """A populated below-v7 database taken through ``run_migrations`` in one
    hop: each ``site_feed_state`` row is seeded where a valid sample has
    ``fetched_at == last_run_at``, NULL otherwise -- exercising the same
    seed SQL, but reached from two version-gates below (6 -> 7 -> 8) in a
    single ``run_migrations`` call, rather than the one-hop-below fixtures
    above."""
    conn = _v6_db()
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    conn.execute(
        """
        INSERT INTO feeds
            (id, source, model, enabled, default_subscribed,
             fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES (101, 'open-meteo', 'ecmwf_ifs', 1, 1, 360, 168, 0)
        """
    )
    conn.execute(
        """
        INSERT INTO feeds
            (id, source, model, enabled, default_subscribed,
             fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES (102, 'open-meteo', 'gfs_global', 1, 1, 360, 168, 0)
        """
    )

    # Feed 101: a valid sample whose fetched_at matches last_run_at -> seeded.
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (1, 101, 'temperature', '2026-07-20T00:00:00Z',
                '2026-07-20T06:00:00Z', 6, 10.0, '{}', 'run-1',
                '2026-07-20T12:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, last_run_at)
        VALUES (1, 101, '2026-07-20T12:00:00Z')
        """
    )

    # Feed 102: last_run_at set (a fetch happened) but no sample row's
    # fetched_at matches it (mirrors a no-op/duplicates-only fetch) -> NULL.
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (1, 102, 'temperature', '2026-07-20T00:00:00Z',
                '2026-07-20T06:00:00Z', 6, 10.0, '{}', 'run-1',
                '2026-07-20T06:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, last_run_at)
        VALUES (1, 102, '2026-07-20T12:00:00Z')
        """
    )

    run_migrations(conn)

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 8
    assert _stamp(conn, 101) == "2026-07-20T12:00:00Z"
    assert _stamp(conn, 102) is None
