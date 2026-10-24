"""Wind serving: eligibility floor, stored states that no longer close wind,
the dashboard banner, and build_hourly's weighted axis (plan §11.4, §15.1
T59, T60, T66).

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test, mirroring ``tests/test_forecast_service.py`` (site id=1 fixed, the
same seeded feeds) since these tests exercise ``build_forecast``/
``build_hourly`` end-to-end rather than ``wind_blend`` in isolation.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

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
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.wind_basis import write_auth_hold
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


# --- T60: stored states and the dashboard banner ---------------------------


@pytest.mark.parametrize("state", ["switching", "rescoring"])
def test_t60_stored_switching_or_rescoring_key_serves_weighted_wind(
    state: str,
) -> None:
    """A stored ``switching`` or ``rescoring`` key no longer closes wind:
    nothing reads it, so the tile and the hourly line serve the weighted
    blend, with no progress note.

    Two trained feeds, A (MAE 1.0) and B (MAE 2.0), take weights 2/3 and
    1/3; A forecasts 4.0 m/s and B 7.0 m/s all day, so the daily high and
    every hour read 2/3 * 4.0 + 1/3 * 7.0 = 5.0 m/s, 18.0 km/h.

    mutant_wind_line_plain_mean -> at
    ``assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)``:
    correct = ``18.0`` at every hour, mutant (``"wind_kmh":
    blend_series("wind")`` in place of ``wind_line()``) = ``19.8`` at every
    hour, the unweighted mean of A's 14.4 and B's 25.2 km/h.
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
    set_runtime_state(conn, "wind_basis:1", state)
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.wind.max_kmh == pytest.approx(18.0)
    assert tile.wind.meta.available is True
    assert tile.wind.note is None

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert payload["notes"] == {"wind": None}


def test_t60_banner_is_none_without_holds_and_shows_a_held_title() -> None:
    """The dashboard banner (``load_wind_banner``) is None while no
    weather.com auth hold exists, and carries the hold's title once one is
    written -- paired, so the None proves the hold check rather than a
    banner that can never fire.

    mutant_banner_without_holds -> at the first ``is None``: correct = None
    (no hold), mutant (a ``WindBanner`` returned with an empty ``holds``) =
    ``WindBanner(holds=[])``.
    """
    history_title = (
        "This weather.com key cannot download wind history for past days,"
        " a known limit of the key"
    )
    conn = _make_db()
    assert load_wind_banner(conn, 1) is None

    write_auth_hold(
        conn, "history_all", status="held", since="2026-01-02T22:00:00Z", error=None
    )
    conn.commit()
    banner = load_wind_banner(conn, 1)
    assert banner is not None
    assert banner.holds == [history_title]


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
    mutant (removing the ``False if variable == "wind" else ...``
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


def test_t66_stored_switching_key_still_serves_the_weighted_line() -> None:
    """A stored ``switching`` key no longer closes the hourly wind line: it
    is the weighted blend (18.0 km/h at every hour, as in the T60 rewrite),
    ``notes["wind"]`` is None, and temperature is unaffected. The live
    weights come through the cache, which counts the call.

    mutant_live_and_as_of_swapped -> at the ``calls`` assertion: correct =
    one call more (``load_wind_serving`` with ``as_of`` None reads the live
    weights through ``live_wind_weights``, which counts every call), mutant
    (``load_wind_serving``'s two branches swapped, so a live request
    computes its weights uncached) = no call counted, while the weights,
    and so the line, stay the same.
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
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=hours,
        value=15.0,
    )
    set_runtime_state(conn, "wind_basis:1", "switching")
    conn.commit()
    before = wind_blend.wind_weights_cache_stats()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    after = wind_blend.wind_weights_cache_stats()
    assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert payload["notes"] == {"wind": None}
    assert payload["blend"]["temp_c"] == pytest.approx([15.0] * 24)
    assert (
        sum(after["calls"].values())  # type: ignore[union-attr]
        == sum(before["calls"].values()) + 1  # type: ignore[union-attr]
    )
