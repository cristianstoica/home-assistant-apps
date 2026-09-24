"""§14.6 "S" writer-side oracles: ``persist_fetch_result``'s
``site_feed_state.last_usable_fetch_at`` stamp (plan §10.3).

Covers the "Not a fetch" family (no-op, both all-usable-invalid shapes,
history persist, error path -- each must leave the stamp untouched), the
duplicates case (0 rows inserted but the stamp still advances), and "one
valid sample is enough" (a batch with an invalid sample before a valid one
still stamps).

Isolation: a fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test, mirroring ``tests/test_forecast_data.py``'s ``_make_db``. Synthetic
data only: one fixture site, the already-seeded ``open-meteo/ecmwf_ifs``
feed.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import UTC, datetime

from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.db.migrations import run_migrations
from wxverify.feeds.seam import FetchResult, NormalizedSample
from wxverify.forecast.data import load_feed_freshness
from wxverify.worker.feed_fetch import FeedFetchTarget, mark_feed_error

_SITE_ID = 1
_MODEL = "ecmwf_ifs"


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


def _sample(
    *,
    variable: str = "temperature",
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float,
    model: str = _MODEL,
) -> NormalizedSample:
    return NormalizedSample(
        model=model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=value,
        source_raw="{}",
        model_run_id="run-1",
    )


def _stamp(conn: sqlite3.Connection, feed_id: int) -> object:
    row = conn.execute(
        "SELECT last_usable_fetch_at FROM site_feed_state "
        "WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()
    return None if row is None else row["last_usable_fetch_at"]


def _last_run_at(conn: sqlite3.Connection, feed_id: int) -> object:
    row = conn.execute(
        "SELECT last_run_at FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (_SITE_ID, feed_id),
    ).fetchone()
    return None if row is None else row["last_run_at"]


# ---------------------------------------------------------------------------
# Duplicates: 0 rows inserted, stamp still advances.
# ---------------------------------------------------------------------------


def test_duplicates_only_fetch_inserts_nothing_but_still_advances_the_stamp() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    sample = _sample(
        issued_at="2026-07-20T00:00:00Z", valid_at="2026-07-20T06:00:00Z", value=10.0
    )

    outcome1 = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[sample]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome1.inserted_count == 1
    assert _stamp(conn, feed_id) == "2026-07-20T12:00:00Z"

    # Identical sample, second call: INSERT OR IGNORE keeps the first write.
    outcome2 = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[sample]),
        fetched_at="2026-07-20T18:00:00Z",
    )
    assert outcome2.inserted_count == 0
    assert outcome2.usable_sample_count == 1
    assert _stamp(conn, feed_id) == "2026-07-20T18:00:00Z"


# ---------------------------------------------------------------------------
# Not a fetch: each case leaves the stamp unchanged.
# ---------------------------------------------------------------------------


def _seed_baseline_stamp(conn: sqlite3.Connection, feed_id: int) -> None:
    """A real usable fetch establishes a baseline stamp, so a later
    "not a fetch" call can be proven NOT to move it (rather than merely
    staying at an ambient None)."""
    baseline = _sample(
        issued_at="2026-07-19T00:00:00Z", valid_at="2026-07-19T06:00:00Z", value=9.0
    )
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[baseline]),
        fetched_at="2026-07-19T00:30:00Z",
    )
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"


def test_no_op_fetch_leaves_the_stamp_unchanged() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    _seed_baseline_stamp(conn, feed_id)

    no_op = _sample(
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T00:00:00Z",
        lead_hours=0,  # < 1 -> never counted as usable
        value=10.0,
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[no_op]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.usable_sample_count == 0
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"


def test_all_usable_samples_nan_leaves_stamp_unchanged_but_last_run_at_advances() -> (
    None
):
    """NaN case: sqlite3 binds a NaN value as SQL NULL, so the shared
    validator's row reads NULL, not 0 -- and separately, the NOT NULL
    ``forecast_samples.value`` column makes the row-insert itself a no-op
    (INSERT OR IGNORE), so ``inserted_count`` stays 0 even though the
    sample was counted as "usable" (lead_hours>=1). Mutant (plan §14.6):
    ``row[0] != 0`` reads True for a NULL row in Python (``None != 0``),
    wrongly admitting the NaN sample and stamping.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    _seed_baseline_stamp(conn, feed_id)

    nan_sample = _sample(
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
        value=math.nan,
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[nan_sample]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.usable_sample_count == 1
    assert outcome.inserted_count == 0
    # Stamp must NOT move (correct = "2026-07-19T00:30:00Z", mutant would
    # read "2026-07-20T12:00:00Z" if the writer stamped the NaN sample).
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"
    # last_run_at still advances -- the else branch runs regardless of
    # sample validity, only the NEW stamp column is conditional.
    assert _last_run_at(conn, feed_id) == "2026-07-20T12:00:00Z"


def test_all_usable_out_of_range_leaves_stamp_unchanged_last_run_at_advances() -> None:
    """Out-of-range case: the validator reads a genuine 0, not NULL. Mutant
    (plan §14.6): ``row[0] is not None`` reads True for 0, wrongly admitting
    the out-of-range sample and stamping.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    _seed_baseline_stamp(conn, feed_id)

    bad_sample = _sample(
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
        value=999.0,  # outside temperature's (-90, 70) range
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[bad_sample]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.usable_sample_count == 1
    assert outcome.inserted_count == 1
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"
    assert _last_run_at(conn, feed_id) == "2026-07-20T12:00:00Z"


def test_history_persist_leaves_the_stamp_unchanged() -> None:
    """``advance_last_run_at=False`` (the historical-backfill callers) never
    stamps, even with a perfectly valid, non-duplicate sample."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    _seed_baseline_stamp(conn, feed_id)

    valid_sample = _sample(
        issued_at="2026-07-20T00:00:00Z", valid_at="2026-07-20T06:00:00Z", value=11.0
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[valid_sample]),
        fetched_at="2026-07-20T12:00:00Z",
        advance_last_run_at=False,
    )
    assert outcome.usable_sample_count == 1
    assert outcome.inserted_count == 1
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"
    # last_run_at ALSO does not advance on this path (the writer's CASE
    # keeps the row's existing value when advance_last_run_at is False) --
    # it stays at the baseline fetch's stamp, not the later fetched_at,
    # distinguishing this case from the two invalid-sample cases above
    # (where last_run_at DOES advance).
    assert _last_run_at(conn, feed_id) == "2026-07-19T00:30:00Z"


def test_error_path_leaves_the_stamp_unchanged() -> None:
    """The network/HTTP error path (``mark_feed_error``) never calls
    ``persist_fetch_result`` at all -- it cannot touch the stamp."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)
    _seed_baseline_stamp(conn, feed_id)

    target = FeedFetchTarget(
        site_id=_SITE_ID,
        feed_id=feed_id,
        lat=40.0,
        lon=-105.0,
        source="open-meteo",
        model=_MODEL,
        max_lead_hours=168,
    )
    mark_feed_error(conn, target, "boom: connection reset")
    assert _stamp(conn, feed_id) == "2026-07-19T00:30:00Z"


# ---------------------------------------------------------------------------
# One valid sample is enough.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Meteoblue package vs member feed identity.
# ---------------------------------------------------------------------------


def test_meteoblue_package_fetch_stamps_the_package_row() -> None:
    """A meteoblue fetch is registered against the package feed
    (``fetch_feed_id``) but its samples are routed to per-model MEMBER feed
    rows via ``_feed_id_for_sample`` (plan §10.3 -- the package carries no
    forward samples of its own). The freshness stamp UPDATE must still bind
    ``fetch_feed_id`` (the package), never the per-sample ``feed_id`` left
    over from the persistence loop.

    Mutant (plan §14.6, MX3): binding ``feed_id`` instead of
    ``fetch_feed_id`` in that UPDATE makes the bind value the last sample's
    member id (GFS05) -- the package stamp stays NULL, and the UPDATE
    affects no row in this fixture. Open-meteo tests elsewhere can't
    distinguish this because there ``feed_id == fetch_feed_id`` for every
    sample.
    """
    conn = _make_db()
    package_id = _feed_id(conn, "meteoblue", "multimodel")

    icon_sample = _sample(
        model="ICON",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
        value=10.0,
    )
    gfs_sample = _sample(
        model="GFS05",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
        value=11.0,
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="meteoblue",
        fetch_feed_id=package_id,
        result=FetchResult(samples=[icon_sample, gfs_sample]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.inserted_count == 2
    assert _stamp(conn, package_id) == "2026-07-20T12:00:00Z"

    icon_feed_id = _feed_id(conn, "meteoblue", "ICON")
    gfs_feed_id = _feed_id(conn, "meteoblue", "GFS05")
    # Anti-vacuity: the member feeds are genuinely registered (by the
    # persistence loop, so their ids resolve) but carry no stamp of their
    # own.
    assert _stamp(conn, icon_feed_id) is None
    assert _stamp(conn, gfs_feed_id) is None

    # A second fetch with a later fetched_at advances the PACKAGE row's
    # stamp, not a member row's.
    icon_sample2 = _sample(
        model="ICON",
        issued_at="2026-07-20T06:00:00Z",
        valid_at="2026-07-20T12:00:00Z",
        value=12.0,
    )
    persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="meteoblue",
        fetch_feed_id=package_id,
        result=FetchResult(samples=[icon_sample2]),
        fetched_at="2026-07-20T18:00:00Z",
    )
    assert _stamp(conn, package_id) == "2026-07-20T18:00:00Z"

    # No site_feed_state row other than the package carries a non-NULL stamp
    # -- persist_fetch_result only ever UPSERTs a row keyed on fetch_feed_id
    # (the package), so the member feeds have no site_feed_state row at all.
    rows = conn.execute(
        "SELECT feed_id, last_usable_fetch_at FROM site_feed_state WHERE site_id=?",
        (_SITE_ID,),
    ).fetchall()
    assert {row["feed_id"] for row in rows} == {package_id}
    assert all(row["last_usable_fetch_at"] is not None for row in rows)

    freshness = load_feed_freshness(
        conn, site_id=_SITE_ID, now=datetime(2026, 7, 20, 19, 0, tzinfo=UTC)
    )
    assert freshness[icon_feed_id].fetch_state == "fresh"
    assert freshness[icon_feed_id].evidence_feed_id == package_id
    assert freshness[gfs_feed_id].fetch_state == "fresh"
    assert freshness[gfs_feed_id].evidence_feed_id == package_id


def test_one_valid_sample_after_an_invalid_one_is_enough_to_stamp() -> None:
    """Mutant (plan §14.6): evaluate only the first usable sample -- the
    invalid one here -- and this fetch would never stamp."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", _MODEL)

    invalid_first = _sample(
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T06:00:00Z",
        value=math.nan,
    )
    valid_second = _sample(
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T07:00:00Z",
        value=11.0,
    )
    outcome = persist_fetch_result(
        conn,
        site_id=_SITE_ID,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[invalid_first, valid_second]),
        fetched_at="2026-07-20T12:00:00Z",
    )
    assert outcome.usable_sample_count == 2
    # Only the valid sample actually inserts a row (the NaN one is IGNOREd
    # by the NOT NULL value column) -- the stamp assertion below is what
    # proves the writer still evaluated past the first (invalid) sample.
    assert outcome.inserted_count == 1
    assert _stamp(conn, feed_id) == "2026-07-20T12:00:00Z"
