"""The check's verdicts (plan §4 T12, §5 C2): ``download_verdict``,
``model_verdict``, ``rain_label_verdict`` and ``rain_label_counts``
(``collection/om_archive.py``, pure), plus two end-to-end
``check_site``/``fetch_run`` wirings (``worker/om_archive_fill.py``) proving
the pure verdicts are actually reached through a real request.

The numeric bullets of T12 (12 R1-matching give PASS, etc.) are pinned at
the ``CheckCounts``/``model_verdict`` level: that is exactly the counting
rule the bullets describe, and asserting it there is precise and
mutation-sensitive, rather than reproducing the same arithmetic through a
much heavier full-database fixture for every combination.

Synthetic data only: coordinates 0.0/0.0, model names already in
``ALLOWANCE_MINUTES``, zone UTC.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from wxverify.collection.om_archive import (
    CandidateName,
    CheckCounts,
    allowance_needed,
    download_verdict,
    model_verdict,
    rain_label_counts,
    rain_label_verdict,
    served_run,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import create_schema
from wxverify.db.switch import ensure_site_generation
from wxverify.worker.om_archive_fill import SiteCheck, check_site

# ---------------------------------------------------------------------------
# download_verdict -- which candidate one download matches.
# ---------------------------------------------------------------------------


def _candidates(
    *,
    r1: Mapping[datetime, float] | None = None,
    older: Mapping[datetime, float] | None = None,
    newer: Mapping[datetime, float] | None = None,
) -> dict[CandidateName, Mapping[datetime, float] | None]:
    return {"r1": r1, "older": older, "newer": newer}


def test_download_verdict_matches_r1() -> None:
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 10.0}
    candidates = _candidates(r1={hour: 10.0}, older={hour: 20.0}, newer={hour: 30.0})
    assert download_verdict(stored, candidates) == "r1"


def test_download_verdict_matches_older() -> None:
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 20.0}
    candidates = _candidates(r1={hour: 10.0}, older={hour: 20.0}, newer={hour: 30.0})
    assert download_verdict(stored, candidates) == "older"


def test_download_verdict_matches_newer() -> None:
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 30.0}
    candidates = _candidates(r1={hour: 10.0}, older={hour: 20.0}, newer={hour: 30.0})
    assert download_verdict(stored, candidates) == "newer"


def test_download_verdict_matches_nothing() -> None:
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 99.0}
    candidates = _candidates(r1={hour: 10.0}, older={hour: 20.0}, newer={hour: 30.0})
    assert download_verdict(stored, candidates) == "no_match"


def test_download_verdict_undecided_with_no_stored_hour() -> None:
    """An "inferred" download, with no stored hour at all, decides nothing
    -- there is nothing to compare against any candidate."""
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    candidates = _candidates(r1={hour: 10.0})
    assert download_verdict({}, candidates) == "undecided"


def test_download_verdict_undecided_with_no_archive_run_for_r1() -> None:
    """The archive has no run at all for R1 (e.g. every fallback also
    failed): undecided, not a no-match."""
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 10.0}
    candidates = _candidates()
    assert download_verdict(stored, candidates) == "undecided"


def test_download_verdict_undecided_past_a_short_runs_end() -> None:
    """A download whose stored hours all lie past R1's end: R1's rebuild
    and the older run's rebuild are identical there (both reach the same
    hour, i.e. literally the same dict), so both match -- two or more
    matches is undecided, never a single pick."""
    hour = datetime(2026, 6, 2, 7, tzinfo=UTC)
    stored = {hour: 10.0}
    identical = {hour: 10.0}
    candidates = _candidates(r1=identical, older=identical)
    assert download_verdict(stored, candidates) == "undecided"


# ---------------------------------------------------------------------------
# model_verdict -- CAN'T TELL / TOO FRESH / PASS / STALE / NO MATCH, in the
# exact counts T12 names.
# ---------------------------------------------------------------------------


def _counts(**overrides: int) -> CheckCounts:
    counts = CheckCounts()
    for key, value in overrides.items():
        setattr(counts, key, value)
    return counts


def test_model_verdict_12_r1_matches_is_pass() -> None:
    counts = _counts(decided=12, r1_match=12)
    assert model_verdict(counts) == "PASS"


def test_model_verdict_12_r1_plus_1_older_is_too_fresh() -> None:
    counts = _counts(decided=13, r1_match=12, older_match=1, older_gap_minutes=10)
    assert model_verdict(counts) == "TOO FRESH"
    assert allowance_needed(counts) == 10 + 30


def test_model_verdict_too_fresh_becomes_no_match_at_the_allowance_limit() -> None:
    """An allowance that would need to reach 24h means the archive cannot
    rebuild this model's downloads at all."""
    counts = _counts(decided=12, r1_match=11, older_match=1, older_gap_minutes=24 * 60)
    assert model_verdict(counts) == "NO MATCH"


def test_model_verdict_8_r1_plus_4_newer_is_stale() -> None:
    counts = _counts(decided=12, r1_match=8, newer_match=4)
    assert model_verdict(counts) == "STALE"


def test_model_verdict_1_r1_2_newer_9_no_match_is_no_match() -> None:
    counts = _counts(decided=12, r1_match=1, newer_match=2, no_match=9)
    assert model_verdict(counts) == "NO MATCH"


def test_model_verdict_9_decided_is_cant_tell() -> None:
    counts = _counts(decided=9, r1_match=9)
    assert model_verdict(counts) == "CAN'T TELL"


def test_model_verdict_exact_90_percent_boundary_is_pass() -> None:
    """90% of 10 decided is exactly 9: the boundary is PASS, not STALE."""
    counts = _counts(decided=10, r1_match=9)
    assert model_verdict(counts) == "PASS"
    counts_below = _counts(decided=10, r1_match=8, newer_match=1)
    assert model_verdict(counts_below) == "STALE"


# ---------------------------------------------------------------------------
# rain_label_counts / rain_label_verdict.
# ---------------------------------------------------------------------------


def _wet_fixture(
    hours: int, *, shift_minutes: int = 0
) -> tuple[dict[datetime, float], dict[datetime, float]]:
    """``hours`` wet instants, each stored 1.0mm; rebuilt the same, shifted
    by ``shift_minutes`` (0 = correctly labelled)."""
    stored: dict[datetime, float] = {}
    rebuilt: dict[datetime, float] = {}
    base = datetime(2026, 6, 2, 0, tzinfo=UTC)
    for i in range(hours):
        instant = base + timedelta(hours=i)
        stored[instant] = 1.0
        rebuilt[instant + timedelta(minutes=shift_minutes)] = 1.0
    return stored, rebuilt


def test_rain_label_verdict_correctly_labelled_wet_hours_is_rain_ok() -> None:
    stored, rebuilt = _wet_fixture(20)
    counts = rain_label_counts(stored, rebuilt)
    assert counts.wet_hours == 20
    assert counts.unshifted == 20
    verdict_counts = _counts(
        wet_hours=counts.wet_hours,
        rain_unshifted=counts.unshifted,
        rain_minus1=counts.minus1,
        rain_plus1=counts.plus1,
    )
    assert rain_label_verdict(verdict_counts) == "RAIN OK"


def test_rain_label_verdict_rain_one_hour_off_is_rain_label() -> None:
    """The rebuilt rain is shifted one hour late relative to storage: the
    unshifted count collapses while the +1h shift catches it instead, so
    unshifted is no longer strictly the highest."""
    stored, rebuilt = _wet_fixture(20, shift_minutes=60)
    counts = rain_label_counts(stored, rebuilt)
    verdict_counts = _counts(
        wet_hours=counts.wet_hours,
        rain_unshifted=counts.unshifted,
        rain_minus1=counts.minus1,
        rain_plus1=counts.plus1,
    )
    assert counts.unshifted < counts.plus1 or counts.unshifted < counts.minus1
    assert rain_label_verdict(verdict_counts) == "RAIN LABEL"


def test_rain_label_verdict_dry_fixture_is_cant_tell() -> None:
    stored, rebuilt = _wet_fixture(19)  # below RAIN_MIN_WET_HOURS (20)
    counts = rain_label_counts(stored, rebuilt)
    verdict_counts = _counts(
        wet_hours=counts.wet_hours,
        rain_unshifted=counts.unshifted,
        rain_minus1=counts.minus1,
        rain_plus1=counts.plus1,
    )
    assert rain_label_verdict(verdict_counts) == "RAIN LABEL CAN'T TELL"


# ---------------------------------------------------------------------------
# End-to-end wiring: check_site actually reaches these pure verdicts
# through fetch_run and httpx.MockTransport.
# ---------------------------------------------------------------------------

_MAX_LEAD_HOURS = 700  # generous: covers every synthetic download date below


def _build_db(path: Path) -> int:
    """A fresh schema with one site, one open-meteo (icon_eu) feed, enabled
    and default-subscribed (so no ``site_feed_state`` row is needed to be
    active), and a published time zone generation. Returns the site id."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('om-archive-check-fixture', 0.0, 0.0, 0.0, 'UTC')"
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO feeds (source, model, enabled, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('open-meteo', 'icon_eu', 1, 1, 180, ?, 0)",
        (_MAX_LEAD_HOURS,),
    )
    ensure_site_generation(conn, site_id)
    conn.commit()
    conn.close()
    return site_id


def _feed_id(path: Path) -> int:
    conn = sqlite3.connect(path)
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
    ).fetchone()
    assert row is not None
    feed_id = int(row[0])
    conn.close()
    return feed_id


def _deactivate_other_open_meteo_feeds(
    path: Path, site_id: int, keep_feed_id: int
) -> None:
    """Switch off, through ``site_feed_state`` -- the same override
    ``active_real_feeds`` reads -- every Open-Meteo feed besides
    ``keep_feed_id``.

    ``init_db``'s migrations seed eight Open-Meteo feeds (``config.FEED_SEEDS``)
    on every open, not just the one ``_build_db`` inserts by hand, and
    ``check_site`` checks every one of them that is active at the site. This
    makes only the model under test active, so ``check_site`` checks that one
    model alone, rather than the test having to characterize the other seven.
    Must run after ``init_db`` has seeded them -- the feeds row for a model
    other than ``keep_feed_id`` does not exist before that.
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    for row in conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND id != ?", (keep_feed_id,)
    ):
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, int(row[0])),
        )
    conn.commit()
    conn.close()


_download_counter = 0


def _seed_closed_download(
    path: Path,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    fetched_at: datetime,
    temperature: float,
    precip: float | None = None,
) -> None:
    """One closed day with one recorded download (day ahead 0) and its
    stored hour(s) in ``forecast_history``. Minimal: no ``site_feed_state``,
    no ``daily_truth``, no ``observations`` row -- those only feed the
    coverage counts (``extra_hours``/``missing_hours``), not the match
    decision this fixture drives."""
    global _download_counter
    _download_counter += 1
    download_id = _download_counter
    conn = sqlite3.connect(path)
    day_start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    now = isoformat_utc(fetched_at)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, attempts, created_at, state_changed_at, done_at)"
        " VALUES (?, ?, 'closed', ?, ?, 'UTC', 1, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            isoformat_utc(day_start),
            isoformat_utc(day_end),
            now,
            now,
            now,
        ),
    )
    valid_at = isoformat_utc(day_start + timedelta(hours=6))
    conn.execute(
        "INSERT INTO close_record_downloads (site_id, local_date, feed_id,"
        " day_ahead, download_id, download_source, download_date, fetched_at)"
        " VALUES (?, ?, ?, 0, ?, 'recorded', ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            download_id,
            local_date.isoformat(),
            now,
        ),
    )
    conn.execute(
        "INSERT INTO forecast_history (site_id, feed_id, variable, day_ahead,"
        " valid_at, local_date, value, lead_hours, issued_at)"
        " VALUES (?, ?, 'temperature', 0, ?, ?, ?, 6, ?)",
        (site_id, feed_id, valid_at, local_date.isoformat(), temperature, now),
    )
    if precip is not None:
        conn.execute(
            "INSERT INTO forecast_history (site_id, feed_id, variable, day_ahead,"
            " valid_at, local_date, value, lead_hours, issued_at)"
            " VALUES (?, ?, 'precip', 0, ?, ?, ?, 6, ?)",
            (site_id, feed_id, valid_at, local_date.isoformat(), precip, now),
        )
    conn.commit()
    conn.close()


def _known_run_handler(
    known_runs: dict[datetime, float],
    *,
    wet_instants: frozenset[datetime] = frozenset(),
    wind_value: float = 1.0,
    rain_label: str = "hour_end",
) -> Callable[[httpx.Request], httpx.Response]:
    """A handler answering only the ``run`` values in ``known_runs`` (each
    mapped to the constant temperature returned at every requested hour);
    every other run is a 400 (no run). Rain is 1.0mm at exactly
    ``wet_instants`` (given as the stored, already-hour-start clock times)
    and 0.0 elsewhere, so a one-hour mislabel changes which instant the
    rain lands on, rather than leaving a flat series a shift cannot
    distinguish. ``rain_label="hour_end"`` (the default, matching
    Open-Meteo's own convention) places each reply's 1.0mm one hour AFTER
    the matching ``wet_instants`` entry, since ``feeds/open_meteo.py``
    ``run_samples`` shifts the reply's rain back by one hour onto the
    hour's start when rebuilding -- so this reply, once rebuilt, lands
    the rain back at ``wet_instants`` unshifted. ``rain_label="same_clock"``
    places the 1.0mm at ``wet_instants`` itself with no such offset,
    reproducing a rain hour-label rule that is one hour off: once rebuilt,
    the rain lands one hour EARLY relative to the stored hour. Wind is a
    flat, non-null ``wind_value`` only so the live-lead window counts as
    covered without a fallback cascade.
    """
    if rain_label == "hour_end":
        wet_reply_instants = frozenset(w + timedelta(hours=1) for w in wet_instants)
    else:
        assert rain_label == "same_clock"
        wet_reply_instants = wet_instants

    def _handler(request: httpx.Request) -> httpx.Response:
        run_text = request.url.params["run"]
        run_at = datetime.strptime(run_text, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)
        temperature = known_runs.get(run_at)
        if temperature is None:
            return httpx.Response(400, json={"reason": "no run"}, request=request)
        forecast_hours = int(request.url.params["forecast_hours"])
        hours = [run_at + timedelta(hours=i) for i in range(forecast_hours)]
        times = [h.isoformat().replace("+00:00", "Z") for h in hours]
        hourly = {
            "temperature_2m": [temperature] * forecast_hours,
            "precipitation": [1.0 if h in wet_reply_instants else 0.0 for h in hours],
            "wind_speed_10m": [wind_value] * forecast_hours,
        }
        return httpx.Response(
            200,
            json={
                "latitude": 0.0,
                "longitude": 0.0,
                "hourly": {"time": times, **hourly},
            },
            request=request,
        )

    return _handler


def test_check_site_end_to_end_pass_through_a_real_request(tmp_path: Path) -> None:
    """A real ``check_site`` run, through ``fetch_run`` and
    ``httpx.MockTransport``, over 20 closed-day downloads all served
    from R1 with 20 wet rain hours correctly labelled: PASS and RAIN OK."""
    db_path = tmp_path / "wxv.db"
    site_id = _build_db(db_path)
    feed_id = _feed_id(db_path)
    fetched_at = datetime(2026, 6, 1, 0, 10, tzinfo=UTC)

    r1 = served_run("icon_eu", fetched_at)

    # Each download's stored hour must stay within the feed's declared
    # max_lead_hours of fetched_at (the live lead bound keep_live enforces),
    # so the 20 downloads start the day after fetched_at, not a month away.
    base_date = date(2026, 6, 2)
    wet_instants: set[datetime] = set()
    for i in range(20):
        local_date = base_date + timedelta(days=i)
        _seed_closed_download(
            db_path,
            site_id=site_id,
            feed_id=feed_id,
            local_date=local_date,
            fetched_at=fetched_at,
            temperature=10.0,
            precip=1.0,  # every download's stored hour is wet
        )
        wet_instants.add(
            datetime(local_date.year, local_date.month, local_date.day, 6, tzinfo=UTC)
        )

    handler = _known_run_handler(
        {r1: 10.0}, wet_instants=frozenset(wet_instants), wind_value=1.0
    )
    requests_by_model: dict[str, int] = {}

    def _counting_handler(request: httpx.Request) -> httpx.Response:
        model = request.url.params["models"]
        requests_by_model[model] = requests_by_model.get(model, 0) + 1
        return handler(request)

    db = init_db(str(db_path))
    # init_db's migrations just seeded seven other Open-Meteo feeds besides
    # the one this fixture built by hand; keep only that one active so
    # check_site checks the model under test alone.
    _deactivate_other_open_meteo_feeds(db_path, site_id, feed_id)
    try:

        async def _run() -> SiteCheck:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(_counting_handler)
            ) as client:
                return await check_site(db, site_id, client=client)

        result = asyncio.run(_run())
    finally:
        close_db()

    assert len(result.models) == 1
    model_check = next(m for m in result.models if m.feed_id == feed_id)
    assert model_check.model == "icon_eu"
    assert model_check.counts.decided == 20
    assert model_check.counts.r1_match == 20
    assert model_check.verdict == "PASS"
    assert model_check.rain_verdict == "RAIN OK"
    assert result.passed is True
    # Every request this run made was for the model under test: the other
    # seven seeded Open-Meteo feeds made none, having been deactivated.
    assert set(requests_by_model) == {"icon_eu"}
    assert requests_by_model["icon_eu"] > 0


def test_check_site_end_to_end_rain_one_hour_off_is_rain_label(tmp_path: Path) -> None:
    """The same 20 closed-day downloads, all served from R1 with correct
    temperature, but with the archive reply's rain placed at the stored
    hour's own clock time instead of Open-Meteo's hour-end convention
    (``rain_label="same_clock"``) -- one hour early once rebuilt. The
    model verdict is still PASS, since temperature alone decides it, but
    this catches a rain hour-label rule that is one hour off: it must be
    reported as RAIN LABEL, not passed as RAIN OK."""
    db_path = tmp_path / "wxv.db"
    site_id = _build_db(db_path)
    feed_id = _feed_id(db_path)
    fetched_at = datetime(2026, 6, 1, 0, 10, tzinfo=UTC)

    r1 = served_run("icon_eu", fetched_at)

    base_date = date(2026, 6, 2)
    wet_instants: set[datetime] = set()
    for i in range(20):
        local_date = base_date + timedelta(days=i)
        _seed_closed_download(
            db_path,
            site_id=site_id,
            feed_id=feed_id,
            local_date=local_date,
            fetched_at=fetched_at,
            temperature=10.0,
            precip=1.0,  # every download's stored hour is wet
        )
        wet_instants.add(
            datetime(local_date.year, local_date.month, local_date.day, 6, tzinfo=UTC)
        )

    handler = _known_run_handler(
        {r1: 10.0},
        wet_instants=frozenset(wet_instants),
        wind_value=1.0,
        rain_label="same_clock",
    )
    requests_by_model: dict[str, int] = {}

    def _counting_handler(request: httpx.Request) -> httpx.Response:
        model = request.url.params["models"]
        requests_by_model[model] = requests_by_model.get(model, 0) + 1
        return handler(request)

    db = init_db(str(db_path))
    _deactivate_other_open_meteo_feeds(db_path, site_id, feed_id)
    try:

        async def _run() -> SiteCheck:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(_counting_handler)
            ) as client:
                return await check_site(db, site_id, client=client)

        result = asyncio.run(_run())
    finally:
        close_db()

    assert len(result.models) == 1
    model_check = next(m for m in result.models if m.feed_id == feed_id)
    assert model_check.model == "icon_eu"
    assert model_check.counts.decided == 20
    assert model_check.counts.r1_match == 20
    assert model_check.verdict == "PASS"
    assert model_check.rain_verdict == "RAIN LABEL"


def test_check_site_a_refused_model_stops_its_own_requests(tmp_path: Path) -> None:
    """A 403 on the very first request excludes that model at once: no
    further HTTP calls for it, and it stays undecided (CAN'T TELL)."""
    db_path = tmp_path / "wxv.db"
    site_id = _build_db(db_path)
    feed_id = _feed_id(db_path)
    fetched_at = datetime(2026, 5, 1, 0, 10, tzinfo=UTC)

    base_date = date(2026, 6, 1)
    for i in range(5):
        _seed_closed_download(
            db_path,
            site_id=site_id,
            feed_id=feed_id,
            local_date=base_date + timedelta(days=i),
            fetched_at=fetched_at,
            temperature=10.0,
        )

    requests_by_model: dict[str, int] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        model = request.url.params["models"]
        requests_by_model[model] = requests_by_model.get(model, 0) + 1
        return httpx.Response(403, request=request)

    db = init_db(str(db_path))
    # Same as the PASS test above: keep only the model under test active, so
    # the other seven seeded Open-Meteo feeds make no request of their own
    # to inflate (or hide inside) the count below.
    _deactivate_other_open_meteo_feeds(db_path, site_id, feed_id)
    try:

        async def _run() -> SiteCheck:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(_handler)
            ) as client:
                return await check_site(db, site_id, client=client)

        result = asyncio.run(_run())
    finally:
        close_db()

    assert len(result.models) == 1
    model_check = next(m for m in result.models if m.feed_id == feed_id)
    assert model_check.model == "icon_eu"
    assert model_check.counts.decided == 0
    assert model_check.verdict == "CAN'T TELL"
    assert model_check.fetch_errors >= 1
    # Exactly one request for the refusal itself, then no more: the other
    # four downloads are skipped without any further HTTP call.
    assert requests_by_model == {"icon_eu": 1}
