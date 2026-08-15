"""Producer availability stamps (§6 outcome knowability).

Every pair producer defines availability through the REAL production code
path, never a raw INSERT:

* ``pair_real_models`` stamps ``first_known_at`` from the source sample's
  ``forecast_samples.fetched_at``;
* ``_insert_persistence_pair`` (via ``materialize_persistence``) stamps the
  SOURCE observation's ``observations.computed_at`` — the lagged observation
  serving as the persistence forecast, never the target's;
* a NULL source availability yields NULL ``first_known_at`` — a timestamp is
  never invented;
* ``materialize_multimodel_mean`` binds the published generation and leaves
  ``first_known_at`` NULL by design (pinned so a later phase changes it
  deliberately, not by accident).

Fixture timestamps are chosen pairwise-distinct so a stamp read from ANY
wrong column or wrong row lands on a different value and fails.
"""

from __future__ import annotations

import sqlite3

from wxverify.db.migrations import run_migrations
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence

_SAMPLE_FETCHED_AT = "2035-01-01T00:05:00Z"
_SOURCE_COMPUTED_AT = "2035-01-01T04:30:00Z"
_TARGET_COMPUTED_AT = "2035-01-01T07:45:00Z"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 48)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    fetched_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, 5.0, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, lead_hours, fetched_at),
    )


def _insert_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    valid_at: str,
    value: float,
    computed_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', ?, ?, 3, ?)
        """,
        (site_id, valid_at, value, computed_at),
    )


def _pair_first_known_ats(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> list[str | None]:
    return [
        None if row["first_known_at"] is None else str(row["first_known_at"])
        for row in conn.execute(
            """
            SELECT first_known_at FROM forecast_pairs
            WHERE site_id=? AND feed_id=?
            ORDER BY valid_at, issued_at
            """,
            (site_id, feed_id),
        )
    ]


# ---------------------------------------------------------------------------
# pair_real_models: first_known_at = the source sample's fetched_at.
# ---------------------------------------------------------------------------


def test_pair_real_models_stamps_source_sample_fetched_at() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Stamp Site")
    feed_id = _make_real_feed(conn, "model-stamp")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-01T06:00:00Z",
        lead_hours=6,
        fetched_at=_SAMPLE_FETCHED_AT,
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00Z",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )

    assert pair_real_models(conn, site_id) == 1

    # fetched_at is distinct from issued_at, valid_at, and the observation's
    # computed_at — a stamp read from any of those wrongly fails here.
    assert _pair_first_known_ats(conn, site_id, feed_id) == [_SAMPLE_FETCHED_AT]


def test_pair_real_models_null_fetched_at_stays_null() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Null Stamp Site")
    feed_id = _make_real_feed(conn, "model-null-stamp")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-01T06:00:00Z",
        lead_hours=6,
        fetched_at=None,
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00Z",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )

    assert pair_real_models(conn, site_id) == 1

    # Never invented: with the sample's fetched_at NULL, the pair's
    # availability is NULL even though other timestamps were available.
    assert _pair_first_known_ats(conn, site_id, feed_id) == [None]


# ---------------------------------------------------------------------------
# materialize_persistence: first_known_at = the SOURCE observation's
# computed_at (the lagged observation acting as the forecast).
# ---------------------------------------------------------------------------


def _persistence_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    assert row is not None, "default seeds must include the persistence feed"
    return int(row["id"])


def test_persistence_pair_stamps_source_observation_computed_at() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Persistence Site")
    feed_id = _persistence_feed_id(conn)
    # Source (lagged) observation and target observation carry DIFFERENT
    # computed_at values: reading the target's would produce the wrong stamp.
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T05:00:00Z",
        value=3.0,
        computed_at=_SOURCE_COMPUTED_AT,
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00Z",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )

    # Exactly one derivable pair: source 05Z -> target 06Z at lead 1 (05Z has
    # no earlier source). Asserted exactly so fixture drift is loud.
    assert materialize_persistence(conn, site_id) == 1
    rows = conn.execute(
        """
        SELECT issued_at, valid_at, first_known_at FROM forecast_pairs
        WHERE site_id=? AND feed_id=?
        ORDER BY valid_at
        """,
        (site_id, feed_id),
    ).fetchall()
    assert [(r["issued_at"], r["valid_at"], r["first_known_at"]) for r in rows] == [
        ("2035-01-01T05:00:00Z", "2035-01-01T06:00:00Z", _SOURCE_COMPUTED_AT)
    ]


def test_persistence_pair_null_source_computed_at_stays_null() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Null Persistence Site")
    feed_id = _persistence_feed_id(conn)
    # Source lacks computed_at; target HAS one -- a fabricated or
    # wrong-row stamp would be non-NULL here.
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T05:00:00Z",
        value=3.0,
        computed_at=None,
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00Z",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )

    materialize_persistence(conn, site_id)

    assert _pair_first_known_ats(conn, site_id, feed_id) == [None]


def test_persistence_fallback_path_stamps_source_computed_at_too() -> None:
    """The non-canonical-target fallback (per-lead point lookups) is a
    separate code path in the materializer and must stamp the same way."""
    conn = _conn()
    site_id = _make_site(conn, "Fallback Persistence Site")
    feed_id = _persistence_feed_id(conn)
    # Canonical source at 05:00Z; the target's stored string is the same
    # whole-hour instant in a NON-canonical spelling ('+00:00' offset instead
    # of 'Z'), which fails the isoformat_utc round-trip identity and forces
    # _materialize_target_fallback -- the separate per-lead lookup path.
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T05:00:00Z",
        value=3.0,
        computed_at=_SOURCE_COMPUTED_AT,
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00+00:00",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )

    materialize_persistence(conn, site_id)

    rows = conn.execute(
        """
        SELECT valid_at, first_known_at FROM forecast_pairs
        WHERE site_id=? AND feed_id=? AND valid_at='2035-01-01T06:00:00+00:00'
        """,
        (site_id, feed_id),
    ).fetchall()
    # Non-vacuous: the fallback path must have produced the pair AND stamped
    # the source's computed_at, not the target's.
    assert [(r["valid_at"], r["first_known_at"]) for r in rows] == [
        ("2035-01-01T06:00:00+00:00", _SOURCE_COMPUTED_AT)
    ]


# ---------------------------------------------------------------------------
# materialize_multimodel_mean: generation bound, first_known_at NULL by design.
# ---------------------------------------------------------------------------


def test_multimodel_mean_binds_published_generation_and_leaves_null_stamp() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Multimodel Site")
    feed_a = _make_real_feed(conn, "member-a")
    feed_b = _make_real_feed(conn, "member-b")
    mean_feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert mean_feed is not None
    for feed_id, fetched_at in ((feed_a, _SAMPLE_FETCHED_AT), (feed_b, None)):
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at="2035-01-01T00:00:00Z",
            valid_at="2035-01-01T06:00:00Z",
            lead_hours=6,
            fetched_at=fetched_at,
        )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-01T06:00:00Z",
        value=4.0,
        computed_at=_TARGET_COMPUTED_AT,
    )
    assert pair_real_models(conn, site_id) == 2

    assert materialize_multimodel_mean(conn, site_id) == 1

    row = conn.execute(
        """
        SELECT fp.first_known_at, fp.tz_generation_id, tg.state
        FROM forecast_pairs fp
        JOIN timezone_generations tg ON tg.id = fp.tz_generation_id
        WHERE fp.site_id=? AND fp.feed_id=?
        """,
        (site_id, int(mean_feed["id"])),
    ).fetchone()
    assert row is not None
    # NULL by design: a derived mean has no single source ingestion time.
    assert row["first_known_at"] is None
    assert row["state"] == "published"
