"""Display rosters, the stored-sample validator, the
``idx_samples_invalid`` reconciliation, and the Ops bad-sample count.

The scored predicate (``invalid_forecast_sample_sql``) is pinned to a
hand-copied literal so an edit to ``FORECAST_VARIABLES``/``FORECAST_VALUE_RANGES``
or to the validator itself cannot slip past silently. The stored-sample
predicate (``invalid_stored_sample_sql``) is exercised through real inserted
rows, never a literal, since it is meant to track ``FORECAST_VALUE_RANGES``
and ``DISPLAY_VALUE_RANGES`` by construction.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from wxverify.collection.forecast_validation import (
    invalid_forecast_sample_sql,
    invalid_stored_sample_sql,
    out_of_range_display_sql,
)
from wxverify.db.migrations import (
    _index_predicate,  # noqa: PLC2701 -- the helper IS the comparison under test
    invalid_sample_index_ddl,
    run_migrations,
)
from wxverify.provider_ops import bad_sample_count_sql


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('DisplayRosterSite', 10.0, 20.0, 100.0, 'UTC')
            """
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE is_virtual = 0 ORDER BY id LIMIT 1"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    value: float,
    valid_at: str = "2026-01-01T06:00:00Z",
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id)
            VALUES (?, ?, ?, '2026-01-01T00:00:00Z', ?, 24, ?, '{}', 'run-1')
            """,
            (site_id, feed_id, variable, valid_at, value),
        ).lastrowid
    )


def test_invalid_forecast_sample_sql_fs_alias_matches_hand_copied_literal() -> None:
    """Catches: a display name added to ``FORECAST_VARIABLES`` (would widen
    the NOT IN list), or any other edit to the scored validator
    (``invalid_forecast_sample_sql``) -- the literal below is copied by hand
    from ``c08b19e``'s ``FORECAST_VALUE_RANGES``/``FORECAST_VARIABLES``, not
    computed by calling the function under test.
    """
    expected = (
        "(\n"
        "        (fs.variable='temperature' AND (fs.value < -90 OR fs.value > 70))"
        " OR (fs.variable='wind' AND (fs.value < 0 OR fs.value > 150))"
        " OR (fs.variable='precip' AND (fs.value < 0 OR fs.value > 500))\n"
        "        OR fs.variable NOT IN ('temperature', 'wind', 'precip')\n"
        "        OR fs.lead_hours < 1\n"
        "        OR fs.issued_at NOT LIKE '____-__-__T__:__:__Z'\n"
        "        OR fs.valid_at NOT LIKE '____-__-__T__:__:__Z'\n"
        "    )"
    )
    assert invalid_forecast_sample_sql("fs") == expected


def test_invalid_stored_sample_sql_shares_the_scored_predicates_tail() -> None:
    """Catches: the lead_hours/timestamp clauses drifting between the two
    predicates even though their value-range and roster clauses differ by
    design (the stored predicate adds the display roster/ranges)."""
    shared_tail = (
        "\n        OR fs.lead_hours < 1"
        "\n        OR fs.issued_at NOT LIKE '____-__-__T__:__:__Z'"
        "\n        OR fs.valid_at NOT LIKE '____-__-__T__:__:__Z'"
        "\n    )"
    )
    assert invalid_forecast_sample_sql("fs").endswith(shared_tail)
    assert invalid_stored_sample_sql("fs").endswith(shared_tail)


_STORED_SAMPLE_CASES: tuple[tuple[str, str, float, bool], ...] = (
    ("cloud_cover_low_bound_valid", "cloud_cover", 0.0, False),
    ("cloud_cover_high_bound_valid", "cloud_cover", 100.0, False),
    ("cloud_cover_above_range_invalid", "cloud_cover", 100.1, True),
    ("wind_gust_low_bound_valid", "wind_gust", 0.0, False),
    ("wind_gust_high_bound_valid", "wind_gust", 150.0, False),
    ("wind_gust_below_range_invalid", "wind_gust", -0.1, True),
    ("wind_direction_low_bound_valid", "wind_direction", 0.0, False),
    ("wind_direction_high_bound_valid", "wind_direction", 360.0, False),
    ("wind_direction_above_range_invalid", "wind_direction", 360.1, True),
    ("unknown_variable_invalid", "pressure", 0.0, True),
    ("scored_temperature_low_bound_valid", "temperature", -90.0, False),
    ("scored_temperature_high_bound_valid", "temperature", 70.0, False),
    ("scored_temperature_below_range_invalid", "temperature", -90.1, True),
    ("scored_temperature_above_range_invalid", "temperature", 70.1, True),
)


@pytest.mark.parametrize(
    "variable, value, expected_invalid",
    [case[1:] for case in _STORED_SAMPLE_CASES],
    ids=[case[0] for case in _STORED_SAMPLE_CASES],
)
def test_invalid_stored_sample_sql_display_and_scored_ranges(
    variable: str, value: float, expected_invalid: bool
) -> None:
    """Catches: a range typo in ``DISPLAY_VALUE_RANGES`` (a display row just
    inside or outside its real bound misclassified), a predicate that
    accepts any variable (the unknown-variable case would read as valid),
    or a scored-range regression hiding behind the display-roster addition.
    """
    conn = _fresh_conn()
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn)
    row_id = _insert_sample(
        conn, site_id=site_id, feed_id=feed_id, variable=variable, value=value
    )

    is_invalid = conn.execute(
        f"SELECT {invalid_stored_sample_sql('forecast_samples')} AS invalid"
        " FROM forecast_samples WHERE id = ?",
        (row_id,),
    ).fetchone()["invalid"]
    assert bool(is_invalid) == expected_invalid


def test_fresh_file_index_matches_generator_and_old_ddl_is_rebuilt_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: ``idx_samples_invalid`` and ``bad_sample_count_sql`` drifting
    apart (the fresh-file equality below), or a rebuild firing on every
    start instead of once (the second ``run_migrations`` call would log
    "rebuilding %s" again).
    """
    db_path = tmp_path / "display-roster-index.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)

    stored = conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='index' AND name='idx_samples_invalid'"
    ).fetchone()
    assert stored is not None
    assert _index_predicate(str(stored["sql"])) == _index_predicate(
        invalid_sample_index_ddl()
    )

    # Swap the stored DDL for the older text (the scored-only predicate),
    # the state a database built before this change carries on disk.
    old_ddl = (
        "CREATE INDEX IF NOT EXISTS idx_samples_invalid "
        "ON forecast_samples(site_id, feed_id) "
        f"WHERE {invalid_forecast_sample_sql('forecast_samples')}"
    )
    conn.execute("DROP INDEX idx_samples_invalid")
    conn.execute(old_ddl)
    conn.commit()

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(conn)
    rebuild_records_1 = [
        record
        for record in caplog.records
        if record.msg == "rebuilding %s: stored definition changed"
        and record.args == ("idx_samples_invalid",)
    ]
    assert len(rebuild_records_1) == 1, (
        "expected exactly one rebuild log line on the first run_migrations"
        " after the swap"
    )

    rebuilt = conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='index' AND name='idx_samples_invalid'"
    ).fetchone()
    assert rebuilt is not None
    assert _index_predicate(str(rebuilt["sql"])) == _index_predicate(
        invalid_sample_index_ddl()
    )

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(conn)
    rebuild_records_2 = [
        record
        for record in caplog.records
        if record.msg == "rebuilding %s: stored definition changed"
        and record.args == ("idx_samples_invalid",)
    ]
    assert len(rebuild_records_2) == 0, "a second run_migrations must not rebuild again"
    conn.close()


def test_bad_sample_count_sql_counts_zero_with_only_valid_display_rows() -> None:
    """Catches: valid display rows counted as bad (the Ops surface would
    report a spurious nonzero bad-sample count for a healthy display
    roster)."""
    conn = _fresh_conn()
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn)
    _insert_sample(
        conn, site_id=site_id, feed_id=feed_id, variable="temperature", value=10.0
    )
    _insert_sample(
        conn, site_id=site_id, feed_id=feed_id, variable="cloud_cover", value=50.0
    )
    _insert_sample(
        conn, site_id=site_id, feed_id=feed_id, variable="wind_gust", value=5.0
    )
    _insert_sample(
        conn, site_id=site_id, feed_id=feed_id, variable="wind_direction", value=180.0
    )

    sql = bad_sample_count_sql("?")
    row = conn.execute(sql, (site_id, feed_id)).fetchone()
    assert int(row["bad_sample_count"]) == 0


# ---------------------------------------------------------------------------
# out_of_range_display_sql, exercised directly on a bare table rather than
# through the version 18 upgrade's DELETE, where the ANDed
# invalid_stored_sample_sql predicate
# would mask a boundary-row ``<=``/``>=`` slip in this helper alone (a
# boundary row that fails the index predicate never reaches the DELETE).
# ---------------------------------------------------------------------------

_OUT_OF_RANGE_DISPLAY_CASES: tuple[tuple[str, str, float | None, bool], ...] = (
    # In range, both ends valid.
    ("cloud_cover_low_bound", "cloud_cover", 0.0, False),
    ("cloud_cover_high_bound", "cloud_cover", 100.0, False),
    ("cloud_cover_mid", "cloud_cover", 50.0, False),
    ("wind_gust_low_bound", "wind_gust", 0.0, False),
    ("wind_gust_high_bound", "wind_gust", 150.0, False),
    ("wind_direction_low_bound", "wind_direction", 0.0, False),
    ("wind_direction_high_bound", "wind_direction", 360.0, False),
    # Out of range.
    ("cloud_cover_below_range", "cloud_cover", -4.0, True),
    ("cloud_cover_above_range", "cloud_cover", 106.0, True),
    ("wind_gust_below_range", "wind_gust", -1.0, True),
    ("wind_gust_above_range", "wind_gust", 151.0, True),
    ("wind_direction_below_range", "wind_direction", -1.0, True),
    ("wind_direction_above_range", "wind_direction", 361.0, True),
    # Not display rows out of range: a scored variable, and a NULL value.
    ("scored_temperature_out_of_range_not_display", "temperature", 75.0, False),
    ("cloud_cover_null_value", "cloud_cover", None, False),
)


def test_out_of_range_display_sql_boundaries_and_non_display_rows() -> None:
    """Catches: ``<=``/``>=`` swapped for ``<``/``>`` (the boundary rows,
    valid at both ends, would flip to out-of-range); a predicate built from
    one display variable only (the other two variables' out-of-range rows
    would read as in-range); a predicate that also matches scored rows (the
    out-of-range ``temperature`` row would read as a display drop); and a
    NULL value misread as out-of-range (``value < low`` is NULL, not true,
    for a NULL value, so a NULL row must stay out of the result)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE t (id TEXT, variable TEXT, valid_at TEXT, value REAL)")
    conn.executemany(
        "INSERT INTO t (id, variable, valid_at, value)"
        " VALUES (?, ?, '2026-01-01T06:00:00Z', ?)",
        [
            (id_, variable, value)
            for id_, variable, value, _ in _OUT_OF_RANGE_DISPLAY_CASES
        ],
    )
    conn.commit()

    predicate = out_of_range_display_sql("t")
    out_of_range_ids = {
        str(row["id"]) for row in conn.execute(f"SELECT id FROM t WHERE {predicate}")
    }
    expected_ids = {
        id_ for id_, _, _, expected in _OUT_OF_RANGE_DISPLAY_CASES if expected
    }
    assert out_of_range_ids == expected_ids
    assert expected_ids, "non-vacuous: at least one row must be expected out of range"
    assert len(expected_ids) < len(_OUT_OF_RANGE_DISPLAY_CASES), (
        "non-vacuous: at least one row must be expected in range"
    )
    conn.close()
