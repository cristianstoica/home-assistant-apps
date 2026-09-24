"""Item L -- the "Last fetched" label (plan §11, oracles listed in §14.7).

The label is the newest ``site_feed_state.last_usable_fetch_at`` among the
feeds contributing to the page's tiles (:func:`_last_fetched_at` in
``forecast/service.py``), rendered by ``_tiles.html:9`` and re-derived
client-side by ``app.js:772``. The auto-poll fingerprint
(``forecast_fingerprint`` in ``forecast/data.py``) must also move when only
the fetch stamp changes, or the label and badges would freeze behind a 204.

Two isolation modes, mirroring the existing forecast test files:

* A fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per test
  (mirrors ``tests/test_forecast_service.py``) for the read-side
  (``build_forecast``/template) and writer-side (``persist_fetch_result``)
  oracles.
* A real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle worker +
  ``TestClient`` (mirrors ``tests/test_forecast_tiles_poll.py``) for the
  one HTTP-level regression test, which needs the real ``/forecast/tiles``
  route wiring.

Synthetic fixtures only (public repo) -- fake site name/coords, 2026-dated
stamps.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

import wxverify
from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.feeds.seam import FetchResult, NormalizedSample
from wxverify.forecast.data import forecast_fingerprint, samples_fingerprint
from wxverify.forecast.service import build_forecast
from wxverify.web.render import env as jinja_env
from wxverify.worker.feed_fetch import FeedFetchTarget, mark_feed_error

# Resolved from the IMPORTED package, not from this test file's own location:
# under the mutation-testing pythonpath override, `wxverify` resolves into
# the throwaway copy, and app.js -- a static asset never Python-imported --
# would otherwise always read the live tree regardless of that override.
PACKAGE_ROOT = Path(wxverify.__file__).resolve().parent

# ---------------------------------------------------------------------------
# :memory: harness (mirrors tests/test_forecast_service.py).
# ---------------------------------------------------------------------------


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


def _hours(day: str, start: int, count: int) -> list[str]:
    return [f"{day}T{h:02d}:00:00Z" for h in range(start, start + count)]


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )


def _seed_hourly(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: list[str],
    value: float = 10.0,
) -> None:
    for i, valid_at in enumerate(valid_ats):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=valid_at,
            lead_hours=i + 1,
            value=value,
        )


def _stamp_usable_fetch(
    conn: sqlite3.Connection, *, site_id: int = 1, feed_id: int, at: str
) -> None:
    """Record ``at`` as the feed's last usable forward fetch (the v8 column)."""
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_usable_fetch_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_usable_fetch_at=excluded.last_usable_fetch_at
        """,
        (site_id, feed_id, at, at),
    )


def _render_tiles(*, site_id: int, view: object) -> str:
    tmpl = jinja_env.get_template("forecast/_tiles.html")
    return tmpl.render(
        url=lambda p: str(p), site=SimpleNamespace(id=site_id), view=view
    )


def _wind_sample(
    *, issued_at: str, valid_at: str, lead_hours: int, value: float = 5.0
) -> NormalizedSample:
    return NormalizedSample(
        model="ecmwf_ifs",
        variable="wind",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=value,
        source_raw="{}",
        model_run_id="run-1",
    )


def _baseline_wind_batch() -> list[NormalizedSample]:
    """5-hour wind batch -- the same shape as the known-good ``available``
    fixture in tests/test_forecast_service.py's
    ``test_wind_partial_badge_when_under_coverage_tile_stays_populated``
    (wind alone does not need the >=18h clearing subset, nor a skill score,
    to leave ``CellMeta.available`` True).

    Fixed 2026-07-20 stamps -- only valid against a caller-supplied,
    equally-fixed ``now`` (``build_forecast(..., now=...)``). Never use this
    against a harness that reads the REAL wall clock (``load_future_samples``
    filters to ``since_valid_at=local_day_start(now, tz)``): see
    ``_future_wind_batch`` for that case."""
    return [
        _wind_sample(issued_at="2026-07-19T20:00:00Z", valid_at=v, lead_hours=i + 1)
        for i, v in enumerate(_hours("2026-07-20", 4, 5))
    ]


def _future_wind_batch() -> list[NormalizedSample]:
    """5-hour wind batch anchored to the REAL wall clock, for harnesses that
    call ``build_forecast`` without a fixed ``now`` (the route/HTTP test) --
    ``load_future_samples`` filters to today-local-midnight-or-later, so a
    fixed historical batch would silently produce an empty view there."""
    # microsecond=0: the shared validator's timestamp shape check
    # (FORECAST_TIMESTAMP_LIKE) requires no fractional seconds, or every
    # sample reads as invalid and the fetch never advances the stamp.
    issued = utc_now().replace(microsecond=0)
    return [
        _wind_sample(
            issued_at=isoformat_utc(issued),
            valid_at=isoformat_utc(issued + timedelta(hours=i + 1)),
            lead_hours=i + 1,
        )
        for i in range(5)
    ]


# ===========================================================================
# Fetch age: the label and its title, from a real stamp.
# ===========================================================================


def test_fetch_age_label_and_title() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 5),
        value=5.0,
    )
    # 40 min before now
    _stamp_usable_fetch(conn, feed_id=feed_id, at="2026-07-20T01:20:00Z")
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.updated_ago == "40 min ago"

    html = _render_tiles(site_id=1, view=view)
    assert "Last fetched: 40 min ago" in html
    assert 'title="When the app last downloaded forecasts."' in html


# ===========================================================================
# Unknown fallback: no parseable stamp anywhere -- paired with the fetch-age
# positive above, which fails if the suppression breaks (a stamp present
# would then ALSO read "unknown").
# ===========================================================================


def test_unknown_fallback_no_stamp_omits_data_updated_at() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 5),
        value=5.0,
    )
    # No site_feed_state row for feed_id at all -- injected absence, not an
    # ambient default: samples exist, only the fetch stamp is withheld.
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.updated_at is None
    assert view.updated_ago is None

    html = _render_tiles(site_id=1, view=view)
    assert "Last fetched: unknown" in html
    assert "data-updated-at" not in html


# ===========================================================================
# Malformed stamps: both readers skip a stamp that doesn't parse, without
# discarding a good stamp elsewhere. Paired with the unknown-fallback
# positive above (no stamp at all reads the same as an unparseable one).
# ===========================================================================


def test_unparseable_stamp_alone_reads_as_unknown_and_fingerprint_ignores_it() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 5),
        value=5.0,
    )
    # A str value that fails parse_utc (isoformat rejects it), not an absent
    # row -- this exercises the ValueError skip, distinct from the no-row
    # case above which never reaches parse_utc at all.
    _stamp_usable_fetch(conn, feed_id=feed_id, at="not-a-time")
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.updated_at is None
    assert view.updated_ago is None

    html = _render_tiles(site_id=1, view=view)
    assert "Last fetched: unknown" in html
    assert "data-updated-at" not in html

    assert forecast_fingerprint(conn, site_id=1) == samples_fingerprint(conn, site_id=1)


def test_unparseable_stamp_paired_with_valid_stamp_keeps_the_good_one() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    # Full local day on BOTH feeds so both land in the wind cell's
    # contributor_ids (mirrors test_newest_across_feeds_picks_the_later_...).
    valid_ats = _hours("2026-01-01", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=6.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_a, at="not-a-time")  # bad -- skipped
    _stamp_usable_fetch(conn, feed_id=feed_b, at="2026-01-01T11:40:00Z")  # good
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    # The good stamp still drives the label -- the bad one is skipped, not
    # allowed to blank out the whole page (contrast with the alone case,
    # where fingerprint falls back to samples_fingerprint entirely).
    assert view.updated_at == "2026-01-01T11:40:00Z"
    assert view.updated_ago == "20 min ago"

    samples = samples_fingerprint(conn, site_id=1)
    expected_total = int(datetime(2026, 1, 1, 11, 40, tzinfo=UTC).timestamp())
    assert forecast_fingerprint(conn, site_id=1) == f"{samples}-{expected_total}"


def test_blob_stamp_is_skipped_by_forecast_fingerprints_non_string_guard() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 5),
        value=5.0,
    )
    # A BLOB in the TEXT column, stored through the real connection (SQLite's
    # type affinity leaves a bound `bytes` value unconverted in a TEXT
    # column) -- pins the `isinstance(raw_stamp, str)` guard specifically,
    # distinct from the ValueError-on-parse cases above.
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_usable_fetch_at)
        VALUES (1, ?, 'x', ?)
        """,
        (feed_id, b"\x00\x01\x02"),
    )
    conn.commit()

    stored = conn.execute(
        "SELECT last_usable_fetch_at FROM site_feed_state WHERE site_id=1"
    ).fetchone()["last_usable_fetch_at"]
    assert isinstance(stored, bytes)  # sanity: really stored as a BLOB

    assert forecast_fingerprint(conn, site_id=1) == samples_fingerprint(conn, site_id=1)


# ===========================================================================
# Duplicates-only fetch, through the real writer: the label moves forward
# and forecast_fingerprint changes while samples_fingerprint does not.
# ===========================================================================


def test_duplicates_only_fetch_moves_label_advances_forecast_fingerprint_only() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    batch = _baseline_wind_batch()

    outcome1 = persist_fetch_result(
        conn,
        site_id=1,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=batch),
        fetched_at="2026-07-20T01:00:00Z",
    )
    assert outcome1.inserted_count == 5
    conn.commit()

    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    fp1_samples = samples_fingerprint(conn, site_id=1)
    fp1_forecast = forecast_fingerprint(conn, site_id=1)
    view1 = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view1.updated_ago == "1 h ago"

    # Duplicates-only re-fetch: identical samples, later fetched_at ->
    # INSERT OR IGNORE inserts 0 rows, but the stamp still advances (§10.3).
    outcome2 = persist_fetch_result(
        conn,
        site_id=1,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=batch),
        fetched_at="2026-07-20T01:30:00Z",
    )
    assert outcome2.inserted_count == 0
    conn.commit()

    fp2_samples = samples_fingerprint(conn, site_id=1)
    fp2_forecast = forecast_fingerprint(conn, site_id=1)
    view2 = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    assert fp2_samples == fp1_samples  # nothing new stored
    assert fp2_forecast != fp1_forecast  # but the poll fingerprint moved
    assert view2.updated_ago == "30 min ago"  # the label moved forward


# ===========================================================================
# Empty (no-op) and error fetches: neither the label nor
# forecast_fingerprint may move -- paired negatives against the duplicates
# case above, which proves a REAL fetch (even one that stores nothing new)
# DOES move them.
# ===========================================================================


def test_no_op_fetch_leaves_label_and_forecast_fingerprint_unchanged() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persist_fetch_result(
        conn,
        site_id=1,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=_baseline_wind_batch()),
        fetched_at="2026-07-20T01:00:00Z",
    )
    conn.commit()
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    fp_before = forecast_fingerprint(conn, site_id=1)
    view_before = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    no_op = NormalizedSample(
        model="ecmwf_ifs",
        variable="wind",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T00:00:00Z",
        lead_hours=0,  # < 1 -> never counted as usable
        value=5.0,
        source_raw="{}",
        model_run_id="run-1",
    )
    outcome = persist_fetch_result(
        conn,
        site_id=1,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[no_op]),
        fetched_at="2026-07-20T01:45:00Z",
    )
    assert outcome.usable_sample_count == 0
    conn.commit()

    fp_after = forecast_fingerprint(conn, site_id=1)
    view_after = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert fp_after == fp_before
    assert view_after.updated_ago == view_before.updated_ago == "1 h ago"


def test_error_fetch_leaves_label_and_forecast_fingerprint_unchanged() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persist_fetch_result(
        conn,
        site_id=1,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=_baseline_wind_batch()),
        fetched_at="2026-07-20T01:00:00Z",
    )
    conn.commit()
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    fp_before = forecast_fingerprint(conn, site_id=1)
    view_before = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    target = FeedFetchTarget(
        site_id=1,
        feed_id=feed_id,
        lat=40.0,
        lon=-105.0,
        source="open-meteo",
        model="ecmwf_ifs",
        max_lead_hours=168,
    )
    mark_feed_error(conn, target, "boom: connection reset")
    conn.commit()

    fp_after = forecast_fingerprint(conn, site_id=1)
    view_after = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert fp_after == fp_before
    assert view_after.updated_ago == view_before.updated_ago == "1 h ago"


# ===========================================================================
# Newest across feeds: two contributor feeds on the SAME cell, the label
# follows the newer stamp. Mutant (plan §14.7): `min` for `max`.
# ===========================================================================


def test_newest_across_feeds_picks_the_later_stamp_not_earlier() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    # Full local day (24h) on BOTH feeds so both individually clear the
    # >=18h coverage guard and both land in the wind cell's
    # contributor_ids (clearing_subset keeps every feed that clears alone).
    valid_ats = _hours("2026-01-01", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=6.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_a, at="2026-01-01T09:00:00Z")  # 3 h ago
    _stamp_usable_fetch(conn, feed_id=feed_b, at="2026-01-01T11:40:00Z")  # 20 min ago
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    # correct = "20 min ago" (feed_b, the later stamp); a `min`-for-`max`
    # mutant would instead surface feed_a's "3 h ago".
    assert view.updated_ago == "20 min ago"
    assert view.updated_at == "2026-01-01T11:40:00Z"


# ===========================================================================
# String vs parsed ordering: a pair whose raw-string order and instant
# order DIVERGE at the whole-second level (the plan's own fractional pair
# collapses to the same whole-second output either way and cannot
# discriminate this).
# ===========================================================================


def test_last_fetched_uses_parsed_instant_not_raw_string_order() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 1, 1, 11, 1, 1, tzinfo=UTC)
    valid_ats = _hours("2026-01-01", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="wind",
        issued_at="2025-12-31T18:00:00Z",
        valid_ats=valid_ats,
        value=6.0,
    )
    # feed_a's RAW STRING sorts later ("...T12..." > "...T11...") but its
    # INSTANT (10:00:00Z, from the +02:00 offset) is earlier than feed_b's
    # (11:00:00.7Z) -- these two orderings genuinely disagree, unlike the
    # plan's same-whole-second fractional pair.
    _stamp_usable_fetch(conn, feed_id=feed_a, at="2026-01-01T12:00:00+02:00")
    _stamp_usable_fetch(conn, feed_id=feed_b, at="2026-01-01T11:00:00.700000Z")
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    # correct = feed_b's instant, truncated to the whole second (11:00:00Z).
    # A raw-string-comparison mutant instead picks feed_a's lexically larger
    # string, whose instant is 10:00:00Z.
    assert view.updated_at == "2026-01-01T11:00:00Z"
    assert view.updated_ago == "1 min ago"


# ===========================================================================
# app.js source pin: the client-side re-derivation must say "Last fetched",
# never fall back to "Updated" (which the 60s refresh would otherwise
# silently restore).
# ===========================================================================


def test_appjs_relabels_relative_time_refresh_as_last_fetched() -> None:
    app_js = (PACKAGE_ROOT / "web" / "static" / "app.js").read_text(encoding="utf-8")
    assert '"Last fetched: " + text' in app_js
    assert '"Updated " + text' not in app_js


# ===========================================================================
# Route-level duplicates-only regression: a site that starts with fetch time
# UNKNOWN (samples exist, no feed ever stamped) must NOT 204 on its poll
# after a duplicates-only fetch, and the served fragment must carry the
# moved label with the "unknown" badge gone -- the fingerprint changing
# alone is not enough evidence; this asserts on the HTML.
# ===========================================================================


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str = "Test Site") -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )


def _current_fingerprint(site_id: int) -> str:
    return get_db().read_sync(lambda conn: forecast_fingerprint(conn, site_id=site_id))


def _max_sample_id(site_id: int) -> int | None:
    row = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT MAX(id) AS m FROM forecast_samples WHERE site_id = ?", (site_id,)
        ).fetchone()
    )
    return None if row["m"] is None else int(row["m"])


def test_route_duplicates_only_serves_moved_label_and_drops_unknown_badge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Start "fetch time unknown" (samples exist, no feed has ever been
    stamped) -- so ``forecast_fingerprint`` and ``samples_fingerprint``
    START EQUAL (no feed has a parseable stamp yet, so the "-<epoch>" suffix
    is absent). This equal-start is deliberate: it is the only fixture shape
    that can distinguish the route's real comparator from a reverted one --
    see the note at the final assertions."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    batch = _future_wind_batch()  # real wall clock: no fixed `now` here

    # Samples land WITHOUT going through persist_fetch_result, so no
    # site_feed_state row -- and hence no stamp -- exists yet.
    for i, sample in enumerate(batch):
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=sample.variable,
            issued_at=sample.issued_at,
            valid_at=sample.valid_at,
            lead_hours=i + 1,
            value=sample.value,
        )
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={site_id}")
        assert page.status_code == 200

        tiles_before = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert tiles_before.status_code == 200
        assert "fetch time unknown" in tiles_before.text  # sanity: starts unknown
        assert "Last fetched: unknown" in tiles_before.text

        old_fp = _current_fingerprint(site_id)
        max_id_before = _max_sample_id(site_id)

        # The FIRST-EVER forward fetch of these exact samples: duplicates
        # against the directly-inserted rows above (inserted_count == 0),
        # but it is also the first time this feed's fetch stamp is ever
        # recorded -- samples_fingerprint stays put while
        # forecast_fingerprint gains its "-<epoch>" suffix for the first
        # time. Only THIS fixture shape (equal-at-start, diverging-after)
        # can distinguish the two fingerprint functions at the route: any
        # fixture that starts with an existing stamp (a differently-shaped
        # "-<epoch>" fingerprint already in the query string) makes the two
        # functions' outputs unequal to the OLD fingerprint either way, so a
        # route reverted to samples_fingerprint would coincidentally still
        # answer 200 -- masking the very bug this test exists to catch.
        recent = isoformat_utc(utc_now().replace(microsecond=0))
        outcome = get_db().write_sync(
            lambda c: persist_fetch_result(
                c,
                site_id=site_id,
                source="open-meteo",
                fetch_feed_id=feed_id,
                result=FetchResult(samples=batch),
                fetched_at=recent,
            )
        )
        assert outcome.inserted_count == 0

        max_id_after = _max_sample_id(site_id)
        assert max_id_after == max_id_before  # the samples table is untouched

        moved = client.get(f"/forecast/tiles?site={site_id}&fingerprint={old_fp}")
        # Proving only that the fingerprint changed is not enough: assert on
        # the actual served HTML. A route reverted to samples_fingerprint
        # would answer 204 here (samples_fingerprint(new) == old_fp, since
        # both equal MAX(id) over the site's samples with no stamp yet at
        # old_fp's time) and this fails.
        assert moved.status_code == 200
        assert moved.status_code != 204
        assert 'id="forecast-tiles"' in moved.text
        assert f'data-updated-at="{recent}"' in moved.text
        assert "fetch time unknown" not in moved.text
        assert "Last fetched: unknown" not in moved.text


def test_last_fetched_ignores_contributors_of_an_unavailable_cell() -> None:
    """Direct-call, hand-built ``DayTile`` that deliberately violates the
    invariant ``_cell_meta_and_values`` always upholds in the real pipeline
    (a "not_available" cell always carries an EMPTY ``contributor_ids`` --
    see that function's early-return branch): here an unavailable cell is
    given a non-empty ``contributor_ids`` on purpose, stamped with the
    NEWER of two feeds. Correct code's ``if meta.available`` guard must
    still exclude it and report the older, available feed's stamp instead.

    This exists because no fixture built through ``build_forecast``'s public
    API can violate that invariant -- it is a defensive guard against a
    combination the current selection logic never produces. Constructing the
    adversarial ``CellMeta`` directly is the last deterministic construction
    available before declaring the mutant equivalent, per the crown-jewel
    discipline: exhaust direct-call fixtures before reporting a gap.
    """
    from wxverify.forecast.data import FeedFreshness
    from wxverify.forecast.service import (
        CellMeta,
        DayTile,
        PrecipCell,
        TempCell,
        WindCell,
        _last_fetched_at,
    )

    available_meta = CellMeta(
        state="normal",
        feeds=[],
        partial=False,
        stale=False,
        extrema_unavailable=False,
        extrema_state="normal",
        contributor_ids=(1,),
        fetch_unknown=False,
    )
    # Adversarial: state is "not_available" (meta.available is False) yet
    # contributor_ids is non-empty -- a shape the real selection code never
    # produces, constructed here only to exercise the guard in isolation.
    unavailable_meta_with_contributors = CellMeta(
        state="not_available",
        feeds=[],
        partial=False,
        stale=False,
        extrema_unavailable=False,
        extrema_state="not_available",
        contributor_ids=(2,),
        fetch_unknown=False,
    )
    empty_meta = CellMeta(
        state="not_available",
        feeds=[],
        partial=False,
        stale=False,
        extrema_unavailable=False,
        extrema_state="not_available",
        contributor_ids=(),
        fetch_unknown=False,
    )

    tile = DayTile(
        day_index=0,
        label="Today",
        date_iso="2026-01-01",
        temp=TempCell(meta=available_meta, high_c=10.0, low_c=2.0),
        wind=WindCell(meta=unavailable_meta_with_contributors, max_kmh=None),
        precip=PrecipCell(
            meta=empty_meta, total_mm=None, wet_hours=None, show_rain_glyph=False
        ),
        state="normal",
        confidence_state="normal",
        stale=False,
        partial=False,
        fetch_unknown=False,
    )

    freshness = {
        1: FeedFreshness(
            feed_id=1,
            latest_issued_at="2026-01-01T09:00:00Z",
            fetch_interval_minutes=60,
            fetch_state="fresh",
            last_usable_fetch_at="2026-01-01T10:00:00Z",
            evidence_feed_id=1,
        ),
        2: FeedFreshness(
            feed_id=2,
            latest_issued_at="2026-01-01T11:00:00Z",
            fetch_interval_minutes=60,
            fetch_state="fresh",
            # Deliberately the NEWER stamp, attached to the unavailable
            # cell's contributor -- correct code must not pick this one.
            last_usable_fetch_at="2026-01-01T12:00:00Z",
            evidence_feed_id=2,
        ),
    }

    result = _last_fetched_at([tile], freshness)

    assert result == "2026-01-01T10:00:00Z"
