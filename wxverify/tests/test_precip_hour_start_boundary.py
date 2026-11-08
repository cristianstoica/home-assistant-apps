"""Part B -- the migration frame for the rain-label boundary (§11, §19.6).

``record_precip_hour_start_boundary`` is a new migration step: it finds the
newest ``fetched_at`` among Open-Meteo/meteoblue rain samples (the newest row
written under the OLD, end-of-hour label) and stores it as the
``PRECIP_HOUR_START_BOUNDARY_KEY`` runtime-state key. Any Open-Meteo/
meteoblue rain sample fetched at or before that stamp -- including a NULL or
unparseable stamp -- is excluded from pairing, serving and replay by
``precip_label_eligible_sql``.

Every new symbol is imported inside the test body that needs it, so a
missing or misnamed symbol fails only that test, never the whole module at
collection time.

Synthetic data only: no place name, coordinate, or non-UTC zone appears,
and no internal ticket number appears in a comment.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
)

# ---------------------------------------------------------------------------
# Shared DB harness
# ---------------------------------------------------------------------------

_BOUNDARY_KEY = "precip_hour_start_boundary"
_SENTINEL = "1970-01-01T00:00:00Z"
_K = "2026-01-01T12:00:00Z"


def _connect(path: str) -> sqlite3.Connection:
    # 0.16.12: autocommit mode, matching the app's own connections, so the
    # migration runner's own SAVEPOINT does not nest inside an implicit
    # legacy-mode BEGIN.
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _make_db() -> sqlite3.Connection:
    """In-memory connection, migrated schema, one enabled synthetic site."""
    # 0.16.12: autocommit mode, matching _connect above; existing .commit()
    # calls on this connection become harmless no-ops.
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    _make_site(conn)
    return conn


def _make_site(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    name: str = "ISTATIONX1",
    timezone: str = "UTC",
    enabled: int = 1,
) -> int:
    conn.execute(
        """
        INSERT INTO sites
            (id, name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, ?, 0.0, 0.0, 0.0, ?, ?)
        """,
        (site_id, name, timezone, enabled),
    )
    return site_id


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """Explicitly enable a feed that is not subscribed by default."""
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET enabled = 1
        """,
        (site_id, feed_id),
    )


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float,
    fetched_at: str | None,
) -> int:
    """A copy of ``test_forecast_extrema_coverage._insert_sample`` that
    takes an explicit ``fetched_at`` instead of defaulting it to
    ``issued_at`` -- needed because every M-test below drives the
    eligibility gate through the stamp, not through the issue time."""
    cur = conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            value,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def _insert_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    variable: str,
    valid_at: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO observations (site_id, variable, valid_at, value, n_stations)
        VALUES (?, ?, ?, ?, 1)
        """,
        (site_id, variable, valid_at, value),
    )


def _pending_job_count(
    conn: sqlite3.Connection, job_type: str, site_id: int | None = None
) -> int:
    if site_id is None:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type=? AND status='pending'",
            (job_type,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs"
            " WHERE type=? AND site_id=? AND status='pending'",
            (job_type, site_id),
        ).fetchone()
    return int(row["n"])


def _complete_jobs(conn: sqlite3.Connection, job_type: str) -> None:
    conn.execute("UPDATE jobs SET status='completed' WHERE type=?", (job_type,))


def _clear_boundary_and_seed(conn: sqlite3.Connection) -> None:
    """``run_migrations`` already called ``record_precip_hour_start_boundary``
    once (it runs unconditionally after migrations), writing the sentinel.
    Delete the key so the step under test runs again, against freshly seeded
    rows, as the setup paragraph (§19.6) directs."""
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)


# ---------------------------------------------------------------------------
# M1 -- the boundary value
# ---------------------------------------------------------------------------


def test_m1a_boundary_is_the_sentinel_with_no_affected_row() -> None:
    # With no Open-Meteo/meteoblue rain row at all, the boundary must be
    # the 1970 sentinel, not NULL or an error.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)

    record_precip_hour_start_boundary(conn)

    assert get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY) == _SENTINEL


def test_m1b_boundary_uses_julianday_order_not_text_order() -> None:
    # This is the test that tells a julianday-ordered MAX apart from a text
    # MAX: "Z" sorts after "." in plain text, so a text MAX would pick
    # 2026-01-01T10:00:00Z over 2026-01-01T10:00:00.500000Z, which is
    # chronologically later. A bug that orders the stamps as text instead
    # of by time reintroduces exactly that mistake.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T10:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T11:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00.500000Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T09:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T09:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T08:00:00Z",
        value=1.0,
        fetched_at=None,
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-02-01T00:00:00Z",
        valid_at="2026-02-01T01:00:00Z",
        value=1.0,
        fetched_at="2026-02-01T00:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-03-01T00:00:00Z",
        valid_at="2026-03-01T01:00:00Z",
        value=10.0,
        fetched_at="2026-03-01T00:00:00Z",
    )

    record_precip_hour_start_boundary(conn)

    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T10:00:00.500000Z"
    )


def test_m1c_only_null_stamped_rain_leaves_the_sentinel() -> None:
    # Pairs with M1(a) and M1(b): a present-but-all-NULL affected row set
    # must behave exactly like an absent one, never like a parseable stamp
    # of "now" or an error.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T08:00:00Z",
        value=1.0,
        fetched_at=None,
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T09:00:00Z",
        value=1.0,
        fetched_at=None,
    )

    record_precip_hour_start_boundary(conn)

    assert get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY) == _SENTINEL


# ---------------------------------------------------------------------------
# M2 -- no sample changes
# ---------------------------------------------------------------------------


def test_m2_boundary_step_changes_no_sample_row() -> None:
    # The step deletes pairs/score-cache rows and restarts generations but
    # must never touch forecast_samples -- the samples are evidence of what
    # was fetched, not of what is eligible.
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend)".rstrip(")"))
    sample_ids = [
        _insert_sample(
            conn,
            feed_id=om,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=1.0,
            fetched_at="2026-01-01T11:00:00Z",
        ),
        _insert_sample(
            conn,
            feed_id=om,
            variable="temperature",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=10.0,
            fetched_at="2026-01-01T11:00:00Z",
        ),
        _insert_sample(
            conn,
            feed_id=google,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=0.5,
            fetched_at="2026-01-01T11:00:00Z",
        ),
    ]
    before = [
        tuple(
            conn.execute("SELECT * FROM forecast_samples WHERE id=?", (sid,)).fetchone()
        )
        for sid in sample_ids
    ]

    record_precip_hour_start_boundary(conn)

    after = [
        tuple(
            conn.execute("SELECT * FROM forecast_samples WHERE id=?", (sid,)).fetchone()
        )
        for sid in sample_ids
    ]
    assert before == after


# ---------------------------------------------------------------------------
# M5 -- fresh install
# ---------------------------------------------------------------------------


def test_m5_fresh_install_reads_the_sentinel_with_no_rescore_job(
    tmp_path: Path,
) -> None:
    # A brand-new database must read the sentinel boundary key with no
    # pending rescore job.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.connection import Database

    db = Database(str(tmp_path / "fresh.db"))
    try:
        assert (
            get_runtime_state(db._conn, PRECIP_HOUR_START_BOUNDARY_KEY)  # noqa: SLF001
            == _SENTINEL
        )
        assert _pending_job_count(db._conn, "pair_and_score") == 0  # noqa: SLF001
    finally:
        db.close()


# No test here for the database import recomputing the precip-hour-start
# boundary key for an old backup and keeping it for a new one.


# ---------------------------------------------------------------------------
# M8 -- the loader
# ---------------------------------------------------------------------------


def test_m8_loader_gate_sits_before_the_row_number_pick() -> None:
    # The eligibility clause must sit before load_future_samples' row-number
    # pick.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.forecast.data import load_future_samples

    conn = _make_db()
    # Two labels in one hour exist only in a file below v16 (the rain
    # release, Part B, keeps one row per hour): the pick is a no-op on a
    # v16 file, and the v16 carry-over keeps the row it picks.
    conn.execute("DROP INDEX idx_samples_slot")
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    # Slot 1: two competing OM rain rows at the same valid_at.
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T06:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=2.0,
        fetched_at="2026-01-01T07:00:00Z",
    )
    # Slot 2: NULL-stamped OM rain, excluded.
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-03T00:00:00Z",
        value=5.0,
        fetched_at=None,
    )
    # OM temperature and Google rain fetched before K, both returned.
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=10.0,
        fetched_at="2026-01-01T07:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T01:00:00Z",
        value=0.5,
        fetched_at="2026-01-01T07:00:00Z",
    )
    live = load_future_samples(
        conn,
        site_id=1,
        since_valid_at="2026-01-01T00:00:00Z",
        served_only=False,
    )
    live_om_rain = [
        s
        for s in live
        if s.feed_id == om
        and s.variable == "precip"
        and s.valid_at == "2026-01-02T00:00:00Z"
    ]
    assert len(live_om_rain) == 1
    assert live_om_rain[0].value == 1.0

    assert not any(
        s.feed_id == om
        and s.variable == "precip"
        and s.valid_at == "2026-01-03T00:00:00Z"
        for s in live
    )
    assert any(s.feed_id == om and s.variable == "temperature" for s in live)
    assert any(s.feed_id == google and s.variable == "precip" for s in live)


# ---------------------------------------------------------------------------
# M14 -- the version fence
# ---------------------------------------------------------------------------


def test_m14_boot_refuses_a_file_newer_than_the_running_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # n is deliberately lower than the real TARGET_USER_VERSION, so the
    # file on disk is stamped with a version newer than the (monkeypatched)
    # running app; boot must refuse it rather than silently proceed.
    import wxverify.db.migrations as migrations
    from wxverify.db.connection import Database

    n = 9
    path = tmp_path / "newer.db"
    db = Database(str(path))
    db.close()

    monkeypatch.setattr(migrations, "TARGET_USER_VERSION", n)
    with pytest.raises(RuntimeError, match="newer than this app"):
        Database(str(path))


# The paired import-side version-fence test
# (`test_m14_import_rejects_a_backup_newer_than_the_running_app`) was
# removed with the database import (storage-release plan 5.5).


def test_m14_literal_version_fence_check() -> None:
    from wxverify.db.migrations import TARGET_USER_VERSION

    n = 9
    # Pins the version bump itself: n must stay below TARGET_USER_VERSION.
    assert n < TARGET_USER_VERSION


# ---------------------------------------------------------------------------
# M15 -- a missing key fails closed
# ---------------------------------------------------------------------------


def test_m15_missing_key_excludes_affected_rows_and_reopen_recomputes_from_stored_rows() -> (  # noqa: E501
    None
):
    # A missing boundary key must exclude affected rows (fail closed), and
    # reopening must recompute the key from the stored rows' own
    # fetched_at.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.forecast.data import load_future_samples

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    _insert_observation(
        conn, variable="precip", valid_at="2026-01-01T13:00:00Z", value=0.0
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=10.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    # 0.16.12: without an observation, no temperature pair can form at all,
    # so the "temperature stays eligible" half of this test needs one.
    _insert_observation(
        conn, variable="temperature", valid_at="2026-01-01T13:00:00Z", value=10.0
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        value=0.5,
        fetched_at="2026-01-01T13:00:00Z",
    )

    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)

    live = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    assert not any(s.feed_id == om and s.variable == "precip" for s in live)
    assert any(s.feed_id == om and s.variable == "temperature" for s in live)
    assert any(s.feed_id == google and s.variable == "precip" for s in live)

    # 0.16.12: a reopen recomputes the boundary from the stored rows' own
    # fetched_at rather than leaving the key missing forever (plan §19.6).
    record_precip_hour_start_boundary(conn)
    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T13:00:00Z"
    )
    live_after_reopen = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    assert not any(
        s.feed_id == om and s.variable == "precip" for s in live_after_reopen
    )

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# M16 -- a refetch keeps the old row; a new run is eligible
# ---------------------------------------------------------------------------


def test_m16_refetch_keeps_the_old_row_and_a_new_run_is_eligible() -> None:
    # An identical refetch of the same model run keeps the old row's
    # fetched_at and excluded status; a genuinely new run replaces the hour
    # (one row per hour since the rain release, Part B) and is eligible.
    from wxverify.collection.forecast_fetcher import (
        persist_fetch_result,
    )
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.feeds.seam import FetchResult, NormalizedSample
    from wxverify.forecast.data import load_future_samples

    conn = _make_db()
    site_id = 1
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)
    _insert_observation(
        conn, variable="precip", valid_at="2026-01-01T13:00:00Z", value=0.0
    )

    def _sample(*, issued_at: str, lead_hours: int, value: float) -> NormalizedSample:
        return NormalizedSample(
            model="ecmwf_ifs",
            variable="precip",
            issued_at=issued_at,
            valid_at="2026-01-01T13:00:00Z",
            lead_hours=lead_hours,
            value=value,
            source_raw=str(value),
            model_run_id=f"ecmwf_ifs:{issued_at}",
        )

    outcome_a = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[
                _sample(issued_at="2026-01-01T00:00:00Z", lead_hours=13, value=1.0)
            ]
        ),
        fetched_at="2026-01-01T11:00:00Z",
    )
    assert outcome_a.inserted_count == 1

    outcome_b = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[
                _sample(issued_at="2026-01-01T00:00:00Z", lead_hours=13, value=1.0)
            ]
        ),
    )
    assert outcome_b.inserted_count == 0
    assert outcome_b.usable_sample_count == 1

    row = conn.execute(
        "SELECT fetched_at, value FROM forecast_samples WHERE variable='precip'"
    ).fetchone()
    assert row["fetched_at"] == "2026-01-01T11:00:00Z"
    assert row["value"] == 1.0

    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    assert not any(s.feed_id == om and s.variable == "precip" for s in live)

    outcome_c = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[_sample(issued_at="2026-01-01T06:00:00Z", lead_hours=7, value=3.0)]
        ),
    )
    assert outcome_c.inserted_count == 1

    new_row = conn.execute(
        "SELECT id, fetched_at FROM forecast_samples"
        " WHERE variable='precip' AND issued_at='2026-01-01T06:00:00Z'"
    ).fetchone()
    assert new_row is not None
    precip_rows = conn.execute(
        "SELECT COUNT(*) FROM forecast_samples WHERE variable='precip'"
    ).fetchone()[0]
    assert precip_rows == 1, "the new run must replace the hour's row"
    from wxverify.core.timeutil import parse_utc

    assert parse_utc(str(new_row["fetched_at"])) > parse_utc(_K)

    live2 = load_future_samples(
        conn, site_id=site_id, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    matches = [s for s in live2 if s.feed_id == om and s.variable == "precip"]
    assert len(matches) == 1
    assert matches[0].value == 3.0


# ---------------------------------------------------------------------------
# M17 -- missing rain stays missing, and the mean keeps no group of one
# ---------------------------------------------------------------------------


def _m17_insert_samples_every_hour(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    value: float,
    fetched_at: str | None,
    excluded_hours: set[str] | None = None,
    excluded_fetched_at: str | None = None,
) -> None:
    excluded_hours = excluded_hours or set()
    for h in range(24):
        valid_at = f"2026-01-02T{h:02d}:00:00Z"
        stamp = excluded_fetched_at if valid_at in excluded_hours else fetched_at
        lead_hours = 18 + h
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at="2026-01-01T06:00:00Z",
            valid_at=valid_at,
            lead_hours=lead_hours,
            value=value,
            fetched_at=stamp,
        )


def _m17_build_forecast_tile(
    conn: sqlite3.Connection, site_id: int = 1, depth: str = "2"
):
    from datetime import UTC, datetime

    from wxverify.forecast.service import build_forecast
    from wxverify.settings.keys import set_setting

    set_setting(conn, "forecast_blend_depth", depth)
    view = build_forecast(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        now=datetime(2026, 1, 2, 2, tzinfo=UTC),
    )
    tile = next(t for t in view.tiles if t.date_iso == "2026-01-02")
    return tile


def _m17_seed_rain_ranks(conn: sqlite3.Connection, feed_ids: list[int]) -> None:
    """Rain release, C4: the published rain ranking for the tile's day ahead
    (0: 2026-01-02 is today at the build's clock), first feed selected."""
    from datetime import UTC, datetime

    from tests.helpers import seed_rain_ranks

    seed_rain_ranks(
        conn,
        site_id=1,
        published_at=datetime(2026, 1, 2, 2, tzinfo=UTC),
        ranked={0: feed_ids},
    )


def test_m17a_an_excluded_feed_is_dropped_from_the_blend() -> None:
    # The excluded feed's rain must be dropped from the blend entirely, not
    # just scored differently.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _subscribe(conn, 1, google)
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _m17_insert_samples_every_hour(
        conn,
        feed_id=google,
        variable="precip",
        value=0.5,
        fetched_at="2026-01-01T13:00:00Z",
    )
    # Rain release, C4: the excluded feed is even the rain ranking's
    # selected model; with its rain dropped it has no full day, so the tile
    # follows the next ranked model.
    _m17_seed_rain_ranks(conn, [om, google])

    tile = _m17_build_forecast_tile(conn)

    assert tile.precip.total_mm == 12.0
    assert tile.precip.wet_hours == 24
    assert [f.feed_id for f in tile.precip.meta.feeds] == [google]
    assert om not in tile.precip.meta.contributor_ids


def test_m17b_a_fully_excluded_rain_feed_is_not_available_not_zero() -> None:
    # A feed whose rain is entirely excluded must read not_available, with
    # total_mm and wet_hours both None -- never a zero total.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="temperature",
        value=10.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    # Rain release, C4: ranked, so not_available comes from the exclusion.
    _m17_seed_rain_ranks(conn, [om])

    tile = _m17_build_forecast_tile(conn)

    assert tile.precip.meta.state == "not_available"
    assert tile.precip.total_mm is None
    assert tile.precip.wet_hours is None


def test_m17c_one_eligible_hour_never_substitutes_for_the_day() -> None:
    # A single eligible hour out of 24 must never substitute for the full
    # day: the outcome is extrema_unavailable, with total_mm, wet_hours and
    # feeds all empty/None.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    eligible_hour = {"2026-01-02T00:00:00Z"}
    excluded_hours = {f"2026-01-02T{h:02d}:00:00Z" for h in range(1, 24)}
    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
        excluded_hours=excluded_hours,
        excluded_fetched_at="2026-01-01T11:00:00Z",
    )
    # (The "eligible_hour" set is implicit: every hour not in excluded_hours
    # keeps the eligible stamp -- only 00:00 keeps "2026-01-01T13:00:00Z".)
    assert eligible_hour  # documents intent; no behavior depends on this line
    _m17_seed_rain_ranks(conn, [om])

    tile = _m17_build_forecast_tile(conn)

    # Rain release, C4: one hour is no full day, so the ranked feed is no
    # winner.
    assert tile.precip.meta.state == "not_available"
    assert tile.precip.total_mm is None
    assert tile.precip.wet_hours is None
    assert tile.precip.meta.feeds == []


# ---------------------------------------------------------------------------
# M18 -- availability, rank eligibility and selection are separate
# ---------------------------------------------------------------------------


def _m18_setup() -> sqlite3.Connection:
    from wxverify.settings.keys import set_setting

    conn = _make_db()
    set_setting(conn, "min_n", "3")
    set_setting(conn, "forecast_blend_depth", "2")
    google = _feed_id(conn, "google", "blend")
    vc = _feed_id(conn, "visualcrossing", "blend")
    _subscribe(conn, 1, google)
    _subscribe(conn, 1, vc)
    return conn


def _m18_seed_feed_rain(conn: sqlite3.Connection, feed_id: int, value: float) -> None:
    for h in range(24):
        valid_at = f"2026-01-02T{h:02d}:00:00Z"
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="precip",
            issued_at="2026-01-01T06:00:00Z",
            valid_at=valid_at,
            lead_hours=18 + h,
            value=value,
            fetched_at="2026-01-01T13:00:00Z",
        )


def _m18_active_publication_id(conn: sqlite3.Connection) -> int:
    """Site 1's one active publication -- ``_m17_seed_rain_ranks`` (via
    ``tests.helpers.seed_rain_ranks``) must run BEFORE ``_m18_seed_rank``
    in every M18 test, so the rain ranking's publication is the one
    leaderboard rankings land in too: with the close on, both the rain
    tile and the leaderboard resolve the SAME ``active_publication`` (the
    site's latest cutoff), so a second, separate publication here would
    silently become the new "active" one and orphan the rain ranks.
    """
    row = conn.execute(
        "SELECT id FROM publications WHERE site_id = 1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None, (
        "_m17_seed_rain_ranks must seed site 1's publication before _m18_seed_rank"
    )
    return int(row["id"])


def _m18_seed_rank(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    n: int,
    skill: float | None,
) -> None:
    from tests.helpers import seed_publication_ranking
    from wxverify.scoring.metrics import MetricResult

    pub_id = _m18_active_publication_id(conn)
    # seed_rain_ranks (via _m17_seed_rain_ranks) defaults window_days=7, so
    # the publication's "rolling" window key is "w:7".
    seed_publication_ranking(
        conn,
        publication_id=pub_id,
        feed_id=feed_id,
        variable="precip",
        day_ahead=1,
        window_key="w:7",
        result=MetricResult(n=n, ets=skill, skill_score=skill, confident=True),
    )


# The rain tile does not blend the leaderboard's selected feeds; it shows
# the published rain ranking's winner. The M18 cases keep their
# leaderboard fixtures and pin that neither the leaderboard's ranks nor
# the blend depth reach the rain tile.


def test_m18a0_rain_tile_follows_the_rain_ranking_not_the_leaderboard() -> None:
    # Both feeds are confident in the leaderboard, om above google; the rain
    # ranking selects google, so the tile is google's rain alone, with both
    # (full-day, wet) group members agreeing.
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m17_seed_rain_ranks(conn, [google, om])
    _m18_seed_rank(conn, feed_id=om, n=3, skill=0.7)
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)

    tile = _m17_build_forecast_tile(conn)

    assert [f.feed_id for f in tile.precip.meta.feeds] == [google]
    assert tile.precip.total_mm == 12.0
    assert tile.precip.wet_hours == 24
    assert tile.precip.agreement_pct == 100


@pytest.mark.parametrize(
    "om_n,om_skill",
    [
        pytest.param(0, None, id="a1-no-pairs-no-row"),
        pytest.param(3, None, id="a2-confident-skill-none"),
        pytest.param(2, 0.9, id="a3-below-min-n"),
    ],
)
def test_m18a_leaderboard_unranked_feed_can_be_the_rain_winner(
    om_n: int,
    om_skill: float | None,
) -> None:
    # A feed present in the data but not confidently ranked in the
    # leaderboard (no row, skill None, or below min_n) is still the rain
    # tile's winner when the rain ranking selects it.
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m17_seed_rain_ranks(conn, [om, google])
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)
    if om_n > 0:
        _m18_seed_rank(conn, feed_id=om, n=om_n, skill=om_skill)

    tile = _m17_build_forecast_tile(conn)

    assert [f.feed_id for f in tile.precip.meta.feeds] == [om]
    assert tile.precip.meta.state == "normal"
    assert tile.precip.total_mm == 24.0
    assert tile.precip.wet_hours == 24


def test_m18b_blend_depth_does_not_reach_the_rain_tile() -> None:
    # The blend depth (2 or 3) changes nothing on the rain tile: the winner
    # is the rain ranking's selected model either way.
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    vc = _feed_id(conn, "visualcrossing", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m18_seed_feed_rain(conn, vc, 0.25)
    _m17_seed_rain_ranks(conn, [vc, google, om])
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)
    _m18_seed_rank(conn, feed_id=vc, n=3, skill=0.4)
    _m18_seed_rank(conn, feed_id=om, n=3, skill=0.3)

    tile_depth2 = _m17_build_forecast_tile(conn)
    assert [f.feed_id for f in tile_depth2.precip.meta.feeds] == [vc]
    assert tile_depth2.precip.meta.state == "normal"
    assert tile_depth2.precip.total_mm == 6.0
    assert tile_depth2.precip.wet_hours == 24

    tile_depth3 = _m17_build_forecast_tile(conn, depth="3")
    assert [f.feed_id for f in tile_depth3.precip.meta.feeds] == [vc]
    assert tile_depth3.precip.total_mm == 6.0
    assert tile_depth3.precip.wet_hours == 24
