"""Downstream consequences of the obs-204 Open-Meteo horizon change.

Covers the request-shape side (``OpenMeteoAdapter.fetch_forecast`` /
``_historical_hourly_names`` in ``wxverify/feeds/open_meteo.py``) and the
pairing/display side (``wxverify.scoring.pairing.pair_real_models`` and
``wxverify.core.timeutil.day_ahead`` vs. ``wxverify.forecast.aggregate.
display_day_index``) of raising each feed's ``max_lead_hours``.

Isolation: ``tests.helpers.asof_conn`` (fresh fully-migrated in-memory
database, real seeded feeds), mirroring ``tests/test_roster_horizon_
provenance.py``; the request-shape tests use ``httpx.MockTransport``, the
same driver ``tests/test_open_meteo_metering.py`` uses -- no network call
is ever made.

Synthetic data only: a ``site-alpha`` site at generic coordinates in
``Europe/Berlin``/UTC, and the product's own public Open-Meteo model
identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime

import httpx

from tests.helpers import asof_conn, asof_make_site
from wxverify.core.timeutil import day_ahead
from wxverify.db.tz_generations import start_retrospective_correction
from wxverify.feeds.open_meteo import OpenMeteoAdapter, _historical_hourly_names
from wxverify.feeds.seam import ForecastRequest
from wxverify.forecast.aggregate import display_day_index
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.tz_rebuild import rebuild_generation_day

_UTC = "UTC"


def _req(*, model: str, max_lead_hours: int) -> ForecastRequest:
    return ForecastRequest(
        lat=50.0,
        lon=10.0,
        model=model,
        variables=("temperature",),
        max_lead_hours=max_lead_hours,
    )


# ---------------------------------------------------------------------------
# 8. Request length
# ---------------------------------------------------------------------------


def _fetch_forecast_params(req: ForecastRequest) -> dict[str, str]:
    captured: dict[str, str] = {}

    def _capturing_handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.url.params)
        return httpx.Response(
            200,
            json={"latitude": 50.0, "longitude": 10.0, "hourly": {"time": []}},
            request=request,
        )

    async def _drive() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_capturing_handler)
        ) as client:
            adapter = OpenMeteoAdapter(client)
            await adapter.fetch_forecast(req)

    asyncio.run(_drive())
    assert captured  # anti-vacuity: the transport was actually hit
    return captured


def test_fetch_forecast_sends_forecast_hours_equal_to_max_lead_hours() -> None:
    """The outgoing ``forecast_hours`` param is the feed's OWN
    ``max_lead_hours`` (unlike the historical path, ``fetch_forecast``
    applies no ``min(7, ...)`` cap): a capped feed (217) and
    ``icon_global`` (180, below the ceiling) both flow straight through.
    """
    capped = _fetch_forecast_params(_req(model="ecmwf_ifs", max_lead_hours=217))
    assert capped["forecast_hours"] == "217"

    icon = _fetch_forecast_params(_req(model="icon_global", max_lead_hours=180))
    assert icon["forecast_hours"] == "180"


def test_historical_previous_runs_day_count_capped_at_seven() -> None:
    """``_historical_hourly_names`` requests
    ``min(7, max_lead_hours // 24)`` previous-run days.

    - A capped feed (217h -> 217 // 24 == 9): the request must still cap
      at 7, not 9 -- a mutant that dropped the ``min(7, ...)`` clamp would
      request day8/day9 columns here.
    - ``icon_global`` (180h -> 180 // 24 == 7 exactly): the cap does not
      engage, so this also pins the boundary against an off-by-one (a
      mutant using ``range(1, req.max_lead_hours // 24)``, exclusive,
      would request only days 1-6).
    - A genuinely uncapped feed (72h -> 72 // 24 == 3): proves the ``min``
      reflects the real value below the cap rather than a hardcoded 7.
    """

    def _days(req: ForecastRequest) -> list[int]:
        names = _historical_hourly_names(req)
        return sorted({int(name.rsplit("day", 1)[1]) for name in names})

    assert _days(_req(model="ecmwf_ifs", max_lead_hours=217)) == [1, 2, 3, 4, 5, 6, 7]
    assert _days(_req(model="icon_global", max_lead_hours=180)) == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
    ]
    assert _days(_req(model="meteofrance_arpege_world", max_lead_hours=72)) == [
        1,
        2,
        3,
    ]


# ---------------------------------------------------------------------------
# 9. Pairing window discriminates
# ---------------------------------------------------------------------------


def _feed_id(conn: sqlite3.Connection, *, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _insert_sample_and_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, 10.0, '{}', 'run-x', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, lead_hours, issued_at),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', ?, 9.5, 3, ?)
        """,
        (site_id, valid_at, valid_at),
    )


def test_pair_real_models_window_boundary_at_raised_and_unraised_horizons() -> None:
    """The bucket ``pair_real_models`` admits is ``0 <= day_ahead <= 7``,
    measured from ISSUANCE (``pairing.py:53-56``). Any lead of ~192h or
    more lands outside that bucket on every date this suite exercises
    (ordinary and DST, see the companion test below), regardless of
    ``max_lead_hours`` -- so it cannot discriminate the horizon change and
    is not used as evidence of it here. The discriminating case has to
    sit inside the bucket:

    - lead 180h issued 00:30 local: ``day_ahead`` == 7 (inside the
      window) -- admitted for ``ecmwf_ifs`` (raised to 217) but excluded
      for ``ukmo_global_deterministic_10km`` (left at 168), because the
      ``max_lead_hours`` bound is what decides here, not the bucket. This
      is the pair the raised horizon exists to unlock.
    - lead 204h, same issuance (24h later, ``day_ahead`` == 8): excluded
      for BOTH feeds by the bucket rule alone -- justified by the bucket
      computed for this exact date, not a general hour threshold (see the
      companion DST test for why that distinction matters).
    - lead 168h/169h issued at local midnight, ordinary date, both at
      ``day_ahead`` == 7: admitted / excluded for BOTH feeds still left at
      168h (``ukmo_global_deterministic_10km`` and
      ``meteofrance_arpege_world``) -- the exact ``max_lead_hours`` SQL
      boundary for the unchanged feeds, isolated from the bucket guard
      since both leads land in the same bucket.

    The raised feeds' OWN new boundary (217h/218h) cannot be exercised
    through this bucket-gated path at all: at any local issuance hour --
    even crossing a fall-back day, which gives back at most one hour --
    217h lands at ``day_ahead`` >= 8, past the pairing window, so the row
    is dropped by the bucket guard before ``max_lead_hours`` is ever
    compared. That boundary is exercised instead by the request-shape
    test above (``forecast_hours`` == 217) and is not re-asserted here.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    ecmwf_id = _feed_id(conn, source="open-meteo", model="ecmwf_ifs")
    ukmo_id = _feed_id(
        conn, source="open-meteo", model="ukmo_global_deterministic_10km"
    )
    arpege_id = _feed_id(conn, source="open-meteo", model="meteofrance_arpege_world")

    # -- discriminator: lead 180h @ 00:30 local, day_ahead == 7 -----------
    issued_at = "2026-01-01T00:30:00Z"
    valid_at_180 = "2026-01-08T12:30:00Z"  # issued_at + 180h
    assert day_ahead(issued_at, valid_at_180, _UTC) == 7
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ecmwf_id,
        issued_at=issued_at,
        valid_at=valid_at_180,
        lead_hours=180,
    )
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ukmo_id,
        issued_at=issued_at,
        valid_at=valid_at_180,
        lead_hours=180,
    )

    # -- bucket-rule control: lead 204h @ 00:30 local, day_ahead == 8,
    # excluded for BOTH feeds regardless of max_lead_hours.
    valid_at_204 = "2026-01-09T12:30:00Z"  # issued_at + 204h
    assert day_ahead(issued_at, valid_at_204, _UTC) == 8
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ecmwf_id,
        issued_at=issued_at,
        valid_at=valid_at_204,
        lead_hours=204,
    )
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ukmo_id,
        issued_at=issued_at,
        valid_at=valid_at_204,
        lead_hours=204,
    )

    # -- unchanged-feed SQL boundary: lead 168h/169h @ local midnight,
    # both at day_ahead == 7.
    midnight_issued = "2026-01-01T00:00:00Z"
    valid_168 = "2026-01-08T00:00:00Z"
    valid_169 = "2026-01-08T01:00:00Z"
    assert day_ahead(midnight_issued, valid_168, _UTC) == 7
    assert day_ahead(midnight_issued, valid_169, _UTC) == 7
    for feed_id in (ukmo_id, arpege_id):
        _insert_sample_and_observation(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at=midnight_issued,
            valid_at=valid_168,
            lead_hours=168,
        )
        _insert_sample_and_observation(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at=midnight_issued,
            valid_at=valid_169,
            lead_hours=169,
        )

    written = pair_real_models(conn, site_id=site_id)
    # anti-vacuity: exactly the admitted set below, nothing more or less
    assert written == 3

    paired = {
        (int(row["feed_id"]), int(row["lead_hours"]))
        for row in conn.execute(
            "SELECT feed_id, lead_hours FROM forecast_pairs WHERE site_id = ?",
            (site_id,),
        ).fetchall()
    }
    assert paired == {
        (ecmwf_id, 180),
        (ukmo_id, 168),
        (arpege_id, 168),
    }


def test_day_ahead_bucket_rule_holds_across_dst_transitions() -> None:
    """The lead-180h-vs-204h discriminator above (``day_ahead`` 7 vs. 8)
    replayed on a Europe/Berlin fall-back date (2026-10-25, a
    25-local-hour day) and a Europe/Berlin spring-forward date
    (2026-03-29, a 23-local-hour day), both issued 00:30 local. The
    bucket rule holds on both -- 180h stays at ``day_ahead`` == 7, 204h
    moves to ``day_ahead`` == 8 -- confirming it is a genuine local
    calendar-day computation, not a fixed-hour threshold a DST transition
    could perturb enough to reach the 217h/218h boundary documented as
    untestable above.
    """
    cases = [
        (
            "Europe/Berlin",
            "2026-10-24T22:30:00Z",  # local 2026-10-25T00:30+02:00 (fall-back day)
            "2026-11-01T10:30:00Z",  # +180h
            "2026-11-02T10:30:00Z",  # +204h
        ),
        (
            "Europe/Berlin",
            "2026-03-28T23:30:00Z",  # local 2026-03-29T00:30+01:00 (spring-forward day)
            "2026-04-05T11:30:00Z",  # +180h
            "2026-04-06T11:30:00Z",  # +204h
        ),
    ]
    for tz, issued_at, valid_180, valid_204 in cases:
        assert day_ahead(issued_at, valid_180, tz) == 7
        assert day_ahead(issued_at, valid_204, tz) == 8


# ---------------------------------------------------------------------------
# 13. Two origins, not one
# ---------------------------------------------------------------------------


def test_display_day_and_pairing_bucket_diverge_for_a_stale_issuance() -> None:
    """A sample that is DISPLAY day 7 (measured from ``now``) but
    PAIRING/REBUILD ``day_ahead`` 8 (measured from issuance, outside the
    0-7 bucket both pairing paths share): issued 2026-01-01T00:00, valid
    2026-01-09T03:00 (lead 195h). Rendered on tile 7 when ``now`` is
    2026-01-02 -- an implementation that measured display from issuance
    instead (matching pairing) would place it on day 8, past
    ``DAY_COUNT`` (8, indices 0-7), and it would never render at all
    rather than rendering on tile 7 as it actually does. The two
    measurements genuinely diverge here.

    Excluded from BOTH paths that share the ``_MAX_DAY_AHEAD``/bucket-7
    rule: the live path (``pair_real_models``, ``pairing.py:56``) and the
    timezone-rebuild path (``tz_rebuild.rebuild_generation_day`` ->
    ``_rebuild_real_pairs``, ``_MAX_DAY_AHEAD`` at ``tz_rebuild.py:31``,
    applied at ``:218``). A paired admitted sample (lead 180h, bucket 7,
    same issuance) proves each path's guard is live rather than vacuously
    dropping everything it sees.
    """
    issued_at = "2026-01-01T00:00:00Z"
    valid_admitted = "2026-01-08T00:00:00Z"  # issued_at + 180h, bucket 7
    valid_excluded = "2026-01-09T03:00:00Z"  # issued_at + 195h, bucket 8
    now = "2026-01-02T00:00:00Z"
    now_dt = datetime.fromisoformat(now.replace("Z", "+00:00")).astimezone(UTC)

    assert day_ahead(issued_at, valid_admitted, _UTC) == 7
    assert day_ahead(issued_at, valid_excluded, _UTC) == 8
    assert display_day_index(valid_excluded, timezone=_UTC, now=now_dt) == 7

    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    ecmwf_id = _feed_id(conn, source="open-meteo", model="ecmwf_ifs")
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ecmwf_id,
        issued_at=issued_at,
        valid_at=valid_admitted,
        lead_hours=180,
    )
    _insert_sample_and_observation(
        conn,
        site_id=site_id,
        feed_id=ecmwf_id,
        issued_at=issued_at,
        valid_at=valid_excluded,
        lead_hours=195,
    )

    # -- live pairing path: pair_real_models -------------------------------
    written = pair_real_models(conn, site_id=site_id)
    assert written == 1
    live_valid_ats = {
        str(row["valid_at"])
        for row in conn.execute(
            "SELECT valid_at FROM forecast_pairs WHERE site_id = ?", (site_id,)
        ).fetchall()
    }
    assert live_valid_ats == {valid_admitted}

    # -- timezone-rebuild path: rebuild_generation_day / _MAX_DAY_AHEAD ----
    generation_id = start_retrospective_correction(conn, site_id, _UTC)
    rebuild_generation_day(
        conn,
        site_id=site_id,
        generation_id=generation_id,
        timezone=_UTC,
        day=date(2026, 1, 8),
        count=False,
    )
    rebuild_generation_day(
        conn,
        site_id=site_id,
        generation_id=generation_id,
        timezone=_UTC,
        day=date(2026, 1, 9),
        count=False,
    )
    # Scoped to the real ecmwf_ifs feed: the rebuild also derives
    # persistence pairs (virtual feed) from the lagged observations these
    # two samples happen to seed, which is unrelated to the guard under
    # test here.
    rebuilt_valid_ats = {
        str(row["valid_at"])
        for row in conn.execute(
            """
            SELECT valid_at FROM forecast_pairs
            WHERE tz_generation_id = ? AND feed_id = ?
            """,
            (generation_id, ecmwf_id),
        ).fetchall()
    }
    assert rebuilt_valid_ats == {valid_admitted}


# ---------------------------------------------------------------------------
# 14. Calendar cases
# ---------------------------------------------------------------------------


def test_day_ahead_fall_back_dst_day_does_not_overcount() -> None:
    """Europe/Berlin's autumn 2026 fall-back lands on 2026-10-25, a
    25-local-hour day (clocks step back 03:00 -> 02:00). A sample issued
    at that day's local midnight and valid exactly 24 elapsed hours later
    is still on the SAME local calendar day -- ``day_ahead`` must read 0,
    not 1. An elapsed-hours implementation (``lead_hours // 24``) would
    read 1 here, which is precisely the extra hour ``DISPLAY_REQUEST_
    HOURS`` (217, not 216) exists to buy back.
    """
    issued_at = "2026-10-24T22:00:00Z"  # local 2026-10-25T00:00:00+02:00
    valid_at = "2026-10-25T22:00:00Z"  # local 2026-10-25T23:00:00+01:00, same date
    assert day_ahead(issued_at, valid_at, "Europe/Berlin") == 0


def test_day_ahead_local_midnight_rollover_with_a_short_lead() -> None:
    """A sample valid just after local midnight, issued the previous local
    evening, crosses a calendar-day boundary on only 2 elapsed hours --
    ``day_ahead`` must read 1. An elapsed-hours implementation
    (``lead_hours // 24``) would read 0 here, since 2 hours is far short
    of a full day.
    """
    issued_at = "2026-01-01T22:00:00Z"  # local 2026-01-01T23:00:00+01:00
    valid_at = "2026-01-02T00:00:00Z"  # local 2026-01-02T01:00:00+01:00
    assert day_ahead(issued_at, valid_at, "Europe/Berlin") == 1
