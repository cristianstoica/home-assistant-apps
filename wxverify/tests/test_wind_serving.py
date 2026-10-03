"""Wind serving: eligibility floor, staging/switching/rescoring display,
the double read, build_hourly's weighted axis, and the state allowlist
(plan §11.4, §15.1 T59, T60, T65, T66, T67).

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test, mirroring ``tests/test_forecast_service.py`` (site id=1 fixed, the
same seeded feeds) since these tests exercise ``build_forecast``/
``build_hourly`` end-to-end rather than ``wind_blend`` in isolation.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from tests.test_forecast_service import (
    _feed_id,
    _hours,
    _insert_sample,
    _make_db,
    _seed_hourly,
    _seed_wind_training,
)
from wxverify.core.units import ms_to_kmh
from wxverify.db.wind_basis import set_wind_basis_state, wind_serving_mode
from wxverify.forecast import wind_blend
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.forecast.wind_blend import NOT_ENOUGH_FEEDS_NOTE
from wxverify.web.context import load_wind_banner

# --- T59: one eligible feed is still below MIN_FEEDS ----------------------


def test_t59_one_eligible_feed_is_not_enough() -> None:
    """``pair_max`` with exactly one trained, sufficiently-covered feed:
    still below ``MIN_FEEDS=2``, so wind is withheld with the
    not-enough-feeds note -- distinct from
    ``test_build_hourly_wind_untrained_feed_is_not_served`` (ZERO eligible
    feeds), which a ``len(eligible) == 0`` mutant of the ``MIN_FEEDS``
    check would still pass.

    mutant_min_feeds_off_by_one -> at
    ``assert payload["notes"] == {"wind": NOT_ENOUGH_FEEDS_NOTE}``:
    correct = ``{"wind": "Not enough feeds with a wind track record yet"}``
    (one eligible feed is still below the floor of two), mutant
    (``len(eligible) == 0`` in place of ``len(eligible) < MIN_FEEDS``) =
    ``{"wind": None}``, because with one eligible feed the mutant's
    zero-check is False and wind is served weighted off that single feed.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_id: 5.0}, observed=4.0)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=4.0,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == []
    assert payload["blend"]["wind_kmh"] == []
    assert payload["feeds"] == []
    assert payload["weights"] == {"wind": {}}
    assert payload["notes"] == {"wind": NOT_ENOUGH_FEEDS_NOTE}


def test_t59_one_eligible_feed_tile_is_not_available() -> None:
    """Tile side of T59, same fixture as
    ``test_t59_one_eligible_feed_is_not_enough``: one trained,
    sufficiently-covered feed in ``pair_max`` is still below
    ``MIN_FEEDS=2``, so the tile itself (not just the hourly payload) must
    read not-available with the not-enough-feeds note and no value.

    mutant_min_feeds_off_by_one -> at ``assert tile.wind.meta.state ==
    "not_available"``: correct = ``"not_available"`` (one eligible feed is
    still below the floor of two, so ``selection.available`` is False and
    ``_wind_meta_and_values`` takes its empty-state branch), mutant
    (``len(eligible) == 0`` in place of ``len(eligible) < MIN_FEEDS``) =
    ``"normal"`` (or similar available state), because with one eligible
    feed the mutant's zero-check is False and the tile is built from that
    single feed's values instead.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_id: 5.0}, observed=4.0)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=4.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.meta.state == "not_available"
    assert tile.wind.note == NOT_ENOUGH_FEEDS_NOTE
    assert tile.wind.max_kmh is None


# --- T60: staging/switching/rescoring display ------------------------------


def test_t60_staging_tile_and_hourly_match_golden() -> None:
    """Golden copy of base-commit 12a14bd's legacy (``staging``) wind
    output. Captured by running this exact fixture against the pre-change
    code, extracted read-only via ``git archive 12a14bd`` and imported off
    ``sys.path`` (``wxverify.__file__`` printed and asserted to come from
    the extracted tree, never the live package) -- see the mutation report
    for the capture script and its printed output. Staging must still
    render byte-for-byte like before the weighted-wind change landed.
    """
    conn = _make_db()
    set_wind_basis_state(conn, 1, "staging")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    hours = _hours("2026-07-20", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours,
        value=5.0,
    )
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.max_kmh == 18.0
    assert tile.wind.meta.state == "low_confidence"
    assert tile.wind.meta.available is True
    assert tile.wind.meta.feed_labels == "open-meteo / ecmwf_ifs"

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == hours
    assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert payload["states"]["wind"] == "low_confidence"
    per_feed = {f["feed_id"]: f["wind_kmh"] for f in payload["feeds"]}
    assert per_feed[feed_id] == pytest.approx([18.0] * 24)


def test_t60_switching_and_rescoring_tile_shows_dash_with_progress_note() -> None:
    """``switching`` with no cursor/station rows yet shows '--' (no value,
    unavailable) with the "starting" progress note; ``rescoring`` always
    reads "rescoring feeds" regardless of cursor state.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 3),
        value=5.0,
    )
    conn.commit()
    set_wind_basis_state(conn, 1, "switching")
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.max_kmh is None
    assert tile.wind.meta.available is False
    assert tile.wind.note == "Wind is being recomputed on the new basis: starting"

    conn2 = _make_db()
    feed_id2 = _feed_id(conn2, "open-meteo", "ecmwf_ifs")
    _seed_hourly(
        conn2,
        feed_id=feed_id2,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 3),
        value=5.0,
    )
    conn2.commit()
    set_wind_basis_state(conn2, 1, "rescoring")
    view2 = build_forecast(
        conn2, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile2 = view2.tiles[0]
    assert tile2.wind.max_kmh is None
    assert (
        tile2.wind.note == "Wind is being recomputed on the new basis: rescoring feeds"
    )


def _make_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon,
                               dem_elevation_m, enabled)
        VALUES (?, ?, 10.0, 20.0, 50.0, 1)
        """,
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_t60_staging_banner_shows_inventory_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In ``staging``, the dashboard banner (``load_wind_banner``) carries
    the rebuild note with the station-day inventory counts; paired with the
    injected absence case, ``pair_max`` with no rebuild note and no auth
    hold, where the banner is None -- the positive proves the note actually
    fires on real counts, not that ``pair_max`` merely happens not to
    trigger it.

    The site is pinned to a non-UTC zone (``Etc/GMT-3``, a neutral offset)
    and the clock ``wind_rebuild_progress`` reads (``wind_basis.utc_now``)
    is pinned to an instant chosen so the site's local date and the UTC
    date differ -- 22:00 UTC is already past local midnight at UTC+3 --
    so the station-day seeding below is anchored to the site's actual
    local "today" rather than to the machine's wall-clock date, and the
    result can't depend on the machine's time zone.
    """
    conn = _make_db()
    conn.execute("UPDATE sites SET timezone = 'Etc/GMT-3' WHERE id = 1")
    set_wind_basis_state(conn, 1, "staging")
    station_id = _make_station(conn, 1, "KTEST001")
    pinned_now = datetime(2026, 1, 2, 22, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.db.wind_basis.utc_now", lambda: pinned_now)
    site_today = pinned_now.astimezone(ZoneInfo("Etc/GMT-3")).date()
    assert site_today != pinned_now.date()
    yesterday = (site_today - timedelta(days=1)).isoformat()
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 24, 24, 0, '2026-01-01T00:00:00Z')",
        (station_id, yesterday),
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'pending', 0, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, (site_today - timedelta(days=2)).isoformat()),
    )
    conn.commit()
    banner = load_wind_banner(conn, 1)
    assert banner is not None
    assert banner.note == (
        "Wind history is being rebuilt in the background: 1 of 2 "
        "station-days fetched. Wind values are still on the old basis until "
        "the switch."
    )
    assert banner.holds == []

    conn2 = _make_db()
    set_wind_basis_state(conn2, 1, "pair_max")
    conn2.commit()
    assert load_wind_banner(conn2, 1) is None


# --- T65: the double read ---------------------------------------------------


def test_t65_double_read_differs_closes_wind(monkeypatch: pytest.MonkeyPatch) -> None:
    """The state read is monkeypatched to return ``staging`` on the first
    call (used as ``serving.state``) and ``switching`` on the second (the
    post-build re-check) -- a switch transaction landing mid-request -- so
    wind must be forced closed for this request even though the stored
    state was legacy when the cells were built.

    mutant_drop_second_read -> at
    ``assert tile.wind.max_kmh is None``: correct = ``None`` (the second
    read disagrees with the first, so wind is forced closed), mutant
    (deleting the
    ``if wind_blend.read_wind_state(conn, site_id) != serving.state:``
    block) = ``18.0``, because the legacy cells built from the first read
    are served as-is with no re-check.
    """
    conn = _make_db()
    set_wind_basis_state(conn, 1, "staging")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 3),
        value=5.0,
    )
    conn.commit()

    real_read = wind_blend.read_wind_state
    calls = {"n": 0}

    def fake_read(c: object, sid: int) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            return real_read(c, sid)  # type: ignore[arg-type]
        return "switching"

    monkeypatch.setattr(wind_blend, "read_wind_state", fake_read)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.max_kmh is None
    assert tile.wind.meta.available is False
    assert calls["n"] == 2


def test_t65_double_read_same_value_serves_normally() -> None:
    """Paired positive: with nothing patched, both reads see the same
    stored state and wind is served normally -- the double read exists to
    catch a DIFFERENCE between the two reads, not to close wind on every
    request.
    """
    conn = _make_db()
    set_wind_basis_state(conn, 1, "staging")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 3),
        value=5.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.max_kmh == 18.0
    assert tile.wind.meta.available is True


# --- T66: build_hourly in weighted mode -------------------------------------


def test_t66_weighted_widens_axis_temperature_unchanged_none_at_new_hours() -> None:
    """A wind-eligible feed with 24 hours of coverage, absent from the
    (narrower, 3-hour) temperature selection, widens the hour axis;
    temperature's values at its own existing hours are unchanged, and None
    at the new hours wind alone supplies.

    mutant_axis_from_temp_only -> at
    ``payload = build_hourly(...)``: correct = the call returns normally
    with ``payload["hours"] == wind_hours`` (the 24-hour wind axis), mutant
    (skipping ``variable == "wind"`` in the ``hour_set``-building loop so
    wind's hours are never unioned in) = ``KeyError:
    '2026-07-20T03:00:00Z'`` raised from ``series_for``'s
    ``out[index[sample.valid_at]] = value``, because wind's own samples
    (fed through ``series_for("wind", feed_id)`` later in the same
    function) still get indexed against the now-narrower ``index`` built
    from the 3-hour axis -- the mutant kills the test by crashing it, not
    by shrinking the asserted axis.
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    wind_hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=wind_hours,
            value=value,
        )
    temp_hours = _hours("2026-07-20", 0, 3)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=temp_hours,
        value=15.0,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == wind_hours
    temp_series = payload["blend"]["temp_c"]
    assert temp_series[:3] == pytest.approx([15.0, 15.0, 15.0])
    assert temp_series[3:] == [None] * (len(wind_hours) - 3)


def test_t66_weighted_widens_axis_precip_unchanged_none_at_new_hour() -> None:
    """Precip side of T66's axis-widening (plan lines 1463-1465): a precip
    feed whose 24 on-the-hour samples exactly cover the local day (making it
    the ``extrema_feeds`` candidate ``covers_local_day_exactly`` requires)
    sits alongside two wind-eligible feeds, one of which also carries a
    single OFF-hour sample (``T03:30:00Z``) on the same day. The hour axis
    widens to include that off-hour instant; ``precip_mm`` -- built by
    ``fixed_membership_series`` strictly from ``selections["precip"]
    .extrema_feeds``' own samples (service.py's ``precip_samples``), never
    from the wind feeds that caused the widening -- is unaffected at every
    pre-existing hour and ``None`` at the new one.

    mutant_precip_series_reindex -> at ``assert extended[off_hour] is None``:
    correct = ``None`` (``fixed_membership_series`` looks each hour up by
    its own ``valid_at`` key, and feed C supplies no sample at the off-hour
    instant), mutant (``fixed_membership_series`` rewritten to zip ``hours``
    against each member's values positionally -- by list index -- instead
    of by ``valid_at`` key) = ``2.0``, because positionally the off-hour's
    index (4) lands on feed C's 5th stored value (its own on-the-hour
    samples have no gap for an instant it was never given), so the mutant
    reads a real precip value where the correct code reads absence.
    Observed directly via the mutation protocol (`.tmp/myers-mut/copy`,
    reverted after).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    feed_c = _feed_id(conn, "open-meteo", "gem_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    wind_hours = _hours("2026-07-20", 0, 24)
    off_hour = "2026-07-20T03:30:00Z"

    def _seed_common(c: sqlite3.Connection, *, widen: bool) -> None:
        _seed_wind_training(
            c, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0
        )
        for fid, value in ((feed_a, 4.0), (feed_b, 7.0)):
            _seed_hourly(
                c,
                feed_id=fid,
                variable="wind",
                issued_at="2026-07-19T20:00:00Z",
                valid_ats=wind_hours,
                value=value,
            )
        if widen:
            _insert_sample(
                c,
                feed_id=feed_a,
                variable="wind",
                issued_at="2026-07-19T20:00:00Z",
                valid_at=off_hour,
                lead_hours=8,
                value=4.0,
            )
        _seed_hourly(
            c,
            feed_id=feed_c,
            variable="precip",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=wind_hours,
            value=2.0,
        )
        c.commit()

    _seed_common(conn, widen=True)
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == [*wind_hours[:4], off_hour, *wind_hours[4:]]
    extended_precip = payload["blend"]["precip_mm"]
    extended = dict(zip(payload["hours"], extended_precip, strict=True))
    assert extended[off_hour] is None

    control_conn = _make_db()
    _seed_common(control_conn, widen=False)
    control_payload = build_hourly(
        control_conn, site_id=1, timezone="UTC", day=0, now=now
    )
    assert control_payload["hours"] == wind_hours
    control = dict(
        zip(
            control_payload["hours"], control_payload["blend"]["precip_mm"], strict=True
        )
    )

    for hour in wind_hours:
        assert extended[hour] == pytest.approx(control[hour]), hour


def test_t66_weighted_renormalizes_missing_feed_hour() -> None:
    """At the one hour feed B has no sample, the weighted mean renormalizes
    to feed A alone (full weight), not the full-day weight split.

    mutant_no_renormalize -> at
    ``assert wind_kmh[idx] == pytest.approx(ms_to_kmh(4.0))``: correct =
    ``14.4`` (feed A's own 4.0 m/s, since it is the only contributor at
    this hour), mutant (``weighted_hourly`` applying each feed's full-day
    weight share without renormalizing over present contributors) =
    ``9.6`` (feed A's 4.0 m/s scaled by its 2/3 full-day weight alone,
    with no feed B value to make up the other 1/3).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    hours = _hours("2026-07-20", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours,
        value=4.0,
    )
    missing_hour = hours[12]
    hours_b = [h for h in hours if h != missing_hour]
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours_b,
        value=7.0,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    wind_kmh = payload["blend"]["wind_kmh"]
    hours_axis = payload["hours"]
    idx = hours_axis.index(missing_hour)
    assert wind_kmh[idx] == pytest.approx(ms_to_kmh(4.0))
    other_idx = hours_axis.index(hours[0])
    assert wind_kmh[other_idx] == pytest.approx(18.0)


def test_t66_weighted_hour_with_no_wind_contributor_is_none() -> None:
    """Both wind feeds cover only hours 0-19 (20h each, exactly
    ``MIN_RUN_HOURS``); a wider temperature feed (0-23) widens the axis to
    24 hours. At hours 20-23 neither wind feed has a sample, so
    ``weighted_hourly`` has no entry there and ``wind_kmh`` reads None,
    paired with hour 0 (both feeds present) reading a real value.

    mutant_default_zero_for_missing_hour -> at
    ``assert wind_kmh[20] is None``: correct = ``None`` (``weighted_hourly``
    only emits a key for hours with at least one contributing feed, and
    ``[weighted.get(hour) for hour in hours]`` defaults the absent key to
    None), mutant (``weighted.get(hour, 0.0)`` in place of
    ``weighted.get(hour)``) = ``0.0``, because the mutant substitutes a
    false zero reading for the hour's true absence of any contributor.
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    wind_hours = _hours("2026-07-20", 0, 20)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=wind_hours,
            value=value,
        )
    temp_hours = _hours("2026-07-20", 0, 24)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=temp_hours,
        value=15.0,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == temp_hours
    wind_kmh = payload["blend"]["wind_kmh"]
    assert wind_kmh[0] == pytest.approx(18.0)
    assert wind_kmh[20] is None
    assert wind_kmh[20:] == [None] * 4


def test_t66_weighted_states_wind_ignores_rebuilding() -> None:
    """``states["wind"]`` in ``pair_max`` is decided without consulting any
    ranking-rebuild flag -- weighted wind's own eligible feeds are always
    ``low_confidence=False``, and its ``rebuilding`` dict is always empty,
    so this must simply run cleanly and read "normal".

    mutant_consult_rebuilding -> at
    ``assert payload["states"]["wind"] == "normal"``: correct = ``"normal"``
    (the override short-circuits before ``_any_rebuilding`` is called),
    mutant (removing the
    ``False if variable == "wind" and wind_mode != "legacy" else ...``
    override, so ``_any_rebuilding`` is always called) raises
    ``KeyError`` instead of returning any state string, because
    ``_any_rebuilding`` indexes ``rebuilding_by_feed[c.feed_id]`` directly
    and wind's weighted-mode ``rebuilding["wind"]`` dict is always ``{}``.
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["states"]["wind"] == "normal"


def test_t66_closed_mode_wind_kmh_all_none_note_set() -> None:
    """In ``closed`` mode (``switching``), the wind line is all None and
    ``notes["wind"]`` carries the progress note; the other variables are
    unaffected.
    """
    conn = _make_db()
    set_wind_basis_state(conn, 1, "switching")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    hours = _hours("2026-07-20", 0, 3)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours,
        value=15.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours,
        value=5.0,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["blend"]["wind_kmh"] == [None, None, None]
    assert payload["notes"]["wind"] == (
        "Wind is being recomputed on the new basis: starting"
    )
    assert payload["blend"]["temp_c"] == pytest.approx([15.0, 15.0, 15.0])


# --- T67: wind_serving_mode is an allowlist ---------------------------------


def test_t67_wind_serving_mode_allowlist() -> None:
    """``staging`` -> legacy, ``pair_max`` -> weighted; ``switching``,
    ``rescoring``, the empty string, and an unknown string all -> closed --
    an allowlist of the two special states, not a denylist of them.

    mutant_denylist -> at
    ``assert wind_serving_mode("bogus-state") == "closed"``: correct =
    ``"closed"`` (anything not explicitly ``staging``/``pair_max`` is
    closed), mutant (a denylist mapping ``{"switching": "closed",
    "rescoring": "closed"}`` with ``.get(state, "legacy")``) = ``"legacy"``,
    because an unrecognized state falls through the denylist's default
    instead of being closed.
    """
    assert wind_serving_mode("staging") == "legacy"
    assert wind_serving_mode("pair_max") == "weighted"
    for state in ("switching", "rescoring", "", "bogus-state"):
        assert wind_serving_mode(state) == "closed"
