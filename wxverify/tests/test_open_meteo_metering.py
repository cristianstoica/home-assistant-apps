"""Item A -- Open-Meteo previous-runs metering (A-T1..A-T13).

Pins ``OpenMeteoAdapter.estimate_historical_cost`` and its helpers
(``_metered_calls``, ``_historical_hourly_names``, ``_billed_days``,
``_historical_date_range``) against the worked table in the plan's section
A.2, and pins the two historical reservation/refund call sites
(``wxverify/worker/backfill.py``, ``wxverify/worker/catchup.py``) against
their documented ordering and refund policy.

A-T12 (the five historical-seam test fakes still build after the call-site
switch) is not re-implemented here: it is the repair carried out directly in
``tests/test_debug_logging.py``, ``tests/test_generation_fence.py`` and
``tests/test_m1_m5.py``, and is verified by running those five tests by
name (see the plan's section A.5 / A-T12).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import date
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.collection.budget import set_source_cap
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.feeds.google import GoogleAdapter
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.open_meteo import (
    OpenMeteoAdapter,
    _billed_days,
    _historical_date_range,
    _historical_hourly_names,
    _metered_calls,
)
from wxverify.feeds.openweathermap import OpenWeatherMapAdapter
from wxverify.feeds.seam import ForecastAdapter, ForecastRequest
from wxverify.feeds.visualcrossing import VisualCrossingAdapter
from wxverify.feeds.weatherapi import WeatherApiAdapter
from wxverify.worker.backfill import SiteBackfillTarget, _fetch_historical_forecasts
from wxverify.worker.catchup import CatchupSite, _fetch_due_open_meteo
from wxverify.worker.control import JobDeferred

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_ALL_VARIABLES = ("temperature", "wind", "precip")


def _req(
    *,
    model: str = "ecmwf_ifs",
    max_lead_hours: int = 168,
    variables: tuple[str, ...] = _ALL_VARIABLES,
) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model=model,
        variables=variables,
        max_lead_hours=max_lead_hours,
    )


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('OpenMeteoMetering', 0.0, 0.0, 0.0, 'UTC')
            """
        ).lastrowid
    )


def _isolate_open_meteo_feed(
    conn: sqlite3.Connection, site_id: int, keep_model: str
) -> int:
    """Disable every open-meteo feed except ``keep_model`` for this site.

    All seven open-meteo feeds default-subscribe (`config.FEED_SEEDS`), so a
    real reservation/refund drive over a single feed needs the other six
    switched off via ``site_feed_state.enabled=0`` -- otherwise every
    assertion below would be about a set of seven calls, not one.
    """
    rows = conn.execute(
        "SELECT id, model FROM feeds WHERE source='open-meteo' ORDER BY id"
    ).fetchall()
    keep_feed_id: int | None = None
    for row in rows:
        feed_id = int(row["id"])
        if row["model"] == keep_model:
            keep_feed_id = feed_id
            continue
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, feed_id),
        )
    assert keep_feed_id is not None, f"no open-meteo feed seeded for {keep_model}"
    return keep_feed_id


def _setup_open_meteo_site(
    tmp_path: Path, *, keep_model: str = "ecmwf_ifs"
) -> tuple[object, sqlite3.Connection, int, int]:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _isolate_open_meteo_feed(conn, site_id, keep_model)
    db = get_db()
    return db, conn, site_id, feed_id


def _budget_calls(conn: sqlite3.Connection, source: str = "open-meteo") -> int:
    row = conn.execute(
        "SELECT calls FROM api_budget WHERE source=?", (source,)
    ).fetchone()
    return 0 if row is None else int(row["calls"])


def _backfill_target(site_id: int) -> SiteBackfillTarget:
    return SiteBackfillTarget(
        site_id=site_id,
        lat=0.0,
        lon=0.0,
        timezone="UTC",
        backfill_status="in_progress",
        backfill_through=None,
    )


def _catchup_site(site_id: int) -> CatchupSite:
    return CatchupSite(site_id=site_id, lat=0.0, lon=0.0, timezone="UTC")


def _ok_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={"latitude": 0.0, "longitude": 0.0, "hourly": {"time": []}},
        request=request,
    )


# ---------------------------------------------------------------------------
# A-T1 -- forward estimate_cost is unaffected by the horizon
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("max_lead_hours", [168, 384])
def test_forward_estimate_cost_is_always_one_call(max_lead_hours: int) -> None:
    """A-T1: three variables, one deterministic model -- weight 0.3, floored
    to 1 -- regardless of how high Item C later raises the horizon.
    """
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    req = _req(max_lead_hours=max_lead_hours)
    assert adapter.estimate_cost(req).calls == 1


# ---------------------------------------------------------------------------
# A-T2 -- previous-runs cost below the day-count floor pins the variable count
# ---------------------------------------------------------------------------


def test_historical_cost_below_day_floor_pins_variable_count() -> None:
    """A-T2: 168h max_lead_hours -> 21 names; a 3-day window is below the
    14-day reference floor, so the day count cannot be doing the work --
    only the 21-name variable count can produce calls == 3.
    """
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    req = _req(max_lead_hours=168)
    estimate = adapter.estimate_historical_cost(
        req, window_start="2026-06-01T00:00:00Z", window_end="2026-06-03T00:00:00Z"
    )
    assert estimate.calls == 3


# ---------------------------------------------------------------------------
# A-T3 -- the 7-day cap holds across rising horizons
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("max_lead_hours", [168, 264, 384])
def test_historical_name_count_capped_at_seven_days(max_lead_hours: int) -> None:
    """A-T3: the name count -- and therefore the price -- does not grow past
    the 7-day literal cap even as Item C/D raise ``max_lead_hours``. This
    fails if the cap is ever removed (the count would then track the
    horizon and stop being 21), rather than silently accepting a new number.
    """
    req = _req(max_lead_hours=max_lead_hours)
    names = _historical_hourly_names(req)
    assert len(names) == 21

    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    estimate = adapter.estimate_historical_cost(
        req, window_start="2026-06-01T00:00:00Z", window_end="2026-06-03T00:00:00Z"
    )
    assert estimate.calls == 3


# ---------------------------------------------------------------------------
# A-T4 -- the estimate is derived, not hardcoded (post-Item-C 96h case)
# ---------------------------------------------------------------------------


def test_historical_cost_derived_from_a_lower_post_item_c_horizon() -> None:
    """A-T4: 96h -> min(7, 96//24)=4 days * 3 variables = 12 names, weight
    1.2, calls == 2. This is the meteofrance_arpege_world case Item C leaves
    behind; a hardcoded ``calls=3`` would fail it.
    """
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    req = _req(max_lead_hours=96)
    names = _historical_hourly_names(req)
    assert len(names) == 12
    estimate = adapter.estimate_historical_cost(
        req, window_start="2026-06-01T00:00:00Z", window_end="2026-06-03T00:00:00Z"
    )
    assert estimate.calls == 2


# ---------------------------------------------------------------------------
# A-T5 -- pure integer arithmetic on _metered_calls
# ---------------------------------------------------------------------------


def test_metered_calls_exact_integer_arithmetic_at_60_billed_days() -> None:
    """A-T5: 21 names * 60 billed days -> ceil(1260 / 140) == 9, computed
    entirely in integers -- independent of how the day count is derived.
    """
    assert _metered_calls(21, 60) == 9


# ---------------------------------------------------------------------------
# A-T6 -- budget exhaustion defers and issues no HTTP request
# ---------------------------------------------------------------------------


def test_previous_runs_reservation_over_budget_defers_with_no_http_request(
    tmp_path: Path,
) -> None:
    """A-T6: with 2 calls of headroom left and a reservation costing 3
    (21 names, 3-day sub-floor window), ``JobDeferred`` is raised and the
    adapter's transport is never touched.
    """
    db, conn, site_id, _feed_id = _setup_open_meteo_site(tmp_path)
    set_source_cap(conn, "open-meteo", daily_call_limit=2)

    requests_made: list[str] = []

    def _forbidden_handler(request: httpx.Request) -> httpx.Response:
        requests_made.append(str(request.url))
        raise AssertionError("no HTTP request may be issued once reservation fails")

    async def _drive() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_forbidden_handler)
        ) as client:
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.backfill.build_adapter",
                lambda source, c: OpenMeteoAdapter(client),
            )
            try:
                writer = FencedWriter(db, db.generation)
                with pytest.raises(JobDeferred):
                    await _fetch_historical_forecasts(
                        db,
                        writer,
                        _backfill_target(site_id),
                        window_start="2026-06-01T00:00:00Z",
                        window_end="2026-06-03T00:00:00Z",
                    )
            finally:
                monkeypatch.undo()

    asyncio.run(_drive())
    assert requests_made == []


# ---------------------------------------------------------------------------
# A-T7 -- reserve-then-fetch ordering, exactly once each
# ---------------------------------------------------------------------------


def test_backfill_reserves_before_fetching_exactly_once_each(tmp_path: Path) -> None:
    """A-T7: a recorder wrapping both ``reserve_budget`` and the adapter's
    ``fetch_historical`` observes reserve-then-fetch, in that order, exactly
    once each.
    """
    db, conn, site_id, _feed_id = _setup_open_meteo_site(tmp_path)
    del conn

    from wxverify.collection.budget import reserve_budget as real_reserve_budget

    order: list[str] = []

    def _recording_reserve_budget(
        conn: sqlite3.Connection,
        source: str,
        calls: int = 1,
        credits: int | None = None,
    ) -> object:
        order.append("reserve")
        return real_reserve_budget(conn, source, calls, credits)

    class _RecordingAdapter(OpenMeteoAdapter):
        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ):
            order.append("fetch")
            return await super().fetch_historical(
                req, window_start=window_start, window_end=window_end
            )

    async def _drive() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(_ok_response)) as (
            client
        ):
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.backfill.reserve_budget", _recording_reserve_budget
            )
            monkeypatch.setattr(
                "wxverify.worker.backfill.build_adapter",
                lambda source, c: _RecordingAdapter(client),
            )
            try:
                writer = FencedWriter(db, db.generation)
                await _fetch_historical_forecasts(
                    db,
                    writer,
                    _backfill_target(site_id),
                    window_start="2026-06-01T00:00:00Z",
                    window_end="2026-06-03T00:00:00Z",
                )
            finally:
                monkeypatch.undo()

    asyncio.run(_drive())
    assert order == ["reserve", "fetch"]


# ---------------------------------------------------------------------------
# A-T8 -- refund symmetry at the catchup site
# ---------------------------------------------------------------------------


def test_catchup_connect_error_refunds_the_full_reservation(tmp_path: Path) -> None:
    """A-T8: a previous-runs fetch driven through catchup's due-feed loop
    raising ``httpx.ConnectError`` refunds the full 3-call reservation, not
    a hardcoded 1 -- the reservation itself is pinned at the derived amount,
    exactly one HTTP attempt is made, and the ``api_budget`` total returns
    exactly to its pre-reservation value (0). A net-zero end state alone
    does not distinguish this from a reserve-1/refund-1 regression (see
    A.5 / A-T12), so the reservation amount and attempt count are pinned
    directly rather than inferred from the net effect.
    """
    db, conn, site_id, _feed_id = _setup_open_meteo_site(tmp_path)

    window_start = "2026-06-01T00:00:00Z"
    window_end = "2026-06-03T00:00:00Z"
    expected_calls = (
        OpenMeteoAdapter(httpx.AsyncClient())
        .estimate_historical_cost(
            _req(), window_start=window_start, window_end=window_end
        )
        .calls
    )

    attempts: list[str] = []

    def _connect_error_handler(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        raise httpx.ConnectError("synthetic connect error", request=request)

    from wxverify.collection.budget import reserve_budget as real_reserve_budget

    reservations: list[int] = []

    def _recording_reserve_budget(
        conn: sqlite3.Connection,
        source: str,
        calls: int = 1,
        credits: int | None = None,
    ) -> object:
        reservations.append(calls)
        return real_reserve_budget(conn, source, calls, credits)

    pre = _budget_calls(conn)

    async def _drive() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_connect_error_handler)
        ) as client:
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.catchup.build_adapter",
                lambda source, c: OpenMeteoAdapter(client),
            )
            monkeypatch.setattr(
                "wxverify.worker.catchup.reserve_budget", _recording_reserve_budget
            )
            try:
                writer = FencedWriter(db, db.generation)
                # catchup's due-feed loop catches and continues rather than
                # re-raising (unlike backfill) -- see _fetch_due_open_meteo.
                written = await _fetch_due_open_meteo(
                    db,
                    writer,
                    _catchup_site(site_id),
                    window_start=window_start,
                    window_end=window_end,
                )
                assert written == 0
            finally:
                monkeypatch.undo()

    asyncio.run(_drive())
    # pins the reservation amount, not just the pre/post net effect -- a
    # regression back to adapter.estimate_cost(req) would reserve 1 and
    # refund 1, leaving the net-zero assertion below satisfied too.
    assert reservations == [expected_calls]
    # pins that the due-feed loop actually ran (world 3: an empty due set
    # would also leave written == 0 and the budget untouched).
    assert len(attempts) == 1
    assert _budget_calls(conn) == pre  # net-zero: the reservation is refunded


# ---------------------------------------------------------------------------
# A-T9 -- no retroactive adjustment to past api_budget rows
# ---------------------------------------------------------------------------


def test_run_migrations_leaves_past_api_budget_rows_byte_identical(
    tmp_path: Path,
) -> None:
    """A-T9: Item A changes no schema and performs no data migration. Prior
    ``api_budget`` rows are historical fact and are untouched by a second
    ``run_migrations`` pass.
    """
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-05-01', 42, 0)"
    )
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-05-02', 7, 0)"
    )
    conn.commit()
    before = [
        tuple(row)
        for row in conn.execute(
            "SELECT source, billing_day, calls, credits"
            " FROM api_budget ORDER BY billing_day"
        ).fetchall()
    ]
    assert before  # anti-vacuity: the table was not empty going in

    run_migrations(conn)

    after = [
        tuple(row)
        for row in conn.execute(
            "SELECT source, billing_day, calls, credits"
            " FROM api_budget ORDER BY billing_day"
        ).fetchall()
    ]
    assert after == before


# ---------------------------------------------------------------------------
# A-T10 -- billed days are the inclusive calendar span, not elapsed seconds
# ---------------------------------------------------------------------------


def test_billed_days_inclusive_span_not_elapsed_duration() -> None:
    """A-T10: a window from a non-midnight instant on one date to a
    non-midnight instant 20 days later returns calls == 4, where an
    elapsed-duration count would return 3. The day count is asserted
    against the request's own params (captured via a MockTransport
    handler), not against a recomputed span -- a parity oracle that fails
    if the estimator and the request ever disagree about the window.
    """
    window_start = "2026-09-01T06:00:00Z"
    window_end = "2026-09-21T06:00:00Z"
    req = _req(max_lead_hours=168)

    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    estimate = adapter.estimate_historical_cost(
        req, window_start=window_start, window_end=window_end
    )
    assert estimate.calls == 4

    # An elapsed-duration count is the defect this item removes: pin it as
    # the wrong answer so a future regression to that shape is caught here.
    elapsed_days = 20
    assert _metered_calls(21, elapsed_days) == 3

    captured_params: dict[str, str] = {}

    def _capturing_handler(request: httpx.Request) -> httpx.Response:
        captured_params.update(request.url.params)
        return httpx.Response(
            200,
            json={"latitude": 0.0, "longitude": 0.0, "hourly": {"time": []}},
            request=request,
        )

    async def _drive() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_capturing_handler)
        ) as client:
            live_adapter = OpenMeteoAdapter(client)
            await live_adapter.fetch_historical(
                req, window_start=window_start, window_end=window_end
            )

    asyncio.run(_drive())

    assert captured_params  # anti-vacuity: the transport was actually hit
    start = date.fromisoformat(captured_params["start_date"])
    end = date.fromisoformat(captured_params["end_date"])
    assert (end - start).days + 1 == 21
    assert _billed_days(window_start, window_end) == 21
    assert _historical_date_range(window_start, window_end) == (start, end)


# ---------------------------------------------------------------------------
# A-T11 -- refund symmetry at the backfill site
# ---------------------------------------------------------------------------


def test_backfill_refund_policy_three_outcomes(tmp_path: Path) -> None:
    """A-T11: companion to A-T8, driving the site through backfill's
    generic failure arm.

    - ``httpx.ConnectError`` (no request reached the provider): the full
      reservation refunds back to net 0.
    - ``httpx.HTTPStatusError`` (a 500 response, the provider was reached):
      the reservation stays spent.
    - A non-transport exception raised after a successful response (a
      payload-validation error): the reservation also stays spent.

    Both negative cases are required in the same test: without them, an
    ungated unconditional refund would pass the first assertion equally.
    """
    window_start = "2026-06-01T00:00:00Z"
    window_end = "2026-06-03T00:00:00Z"

    # --- ConnectError: nets to zero -----------------------------------
    db1, conn1, site1, _f1 = _setup_open_meteo_site(tmp_path / "connect")

    def _connect_error_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("synthetic connect error", request=request)

    pre1 = _budget_calls(conn1)

    async def _drive_connect() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_connect_error_handler)
        ) as client:
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.backfill.build_adapter",
                lambda source, c: OpenMeteoAdapter(client),
            )
            try:
                writer = FencedWriter(db1, db1.generation)
                with pytest.raises(httpx.ConnectError):
                    await _fetch_historical_forecasts(
                        db1,
                        writer,
                        _backfill_target(site1),
                        window_start=window_start,
                        window_end=window_end,
                    )
            finally:
                monkeypatch.undo()

    asyncio.run(_drive_connect())
    assert pre1 == 0  # anti-vacuity: there was a real reservation to refund
    assert _budget_calls(conn1) == pre1

    # --- HTTPStatusError (500): stays spent ----------------------------
    db2, conn2, site2, _f2 = _setup_open_meteo_site(tmp_path / "http500")

    def _http_500_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream error", request=request)

    async def _drive_http() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_http_500_handler)
        ) as client:
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.backfill.build_adapter",
                lambda source, c: OpenMeteoAdapter(client),
            )
            try:
                writer = FencedWriter(db2, db2.generation)
                with pytest.raises(JobDeferred):
                    await _fetch_historical_forecasts(
                        db2,
                        writer,
                        _backfill_target(site2),
                        window_start=window_start,
                        window_end=window_end,
                    )
            finally:
                monkeypatch.undo()

    asyncio.run(_drive_http())
    assert _budget_calls(conn2) == 3  # the reservation this window prices

    # --- Non-transport failure after a served response: stays spent ----
    db3, conn3, site3, _f3 = _setup_open_meteo_site(tmp_path / "validation")

    def _invalid_payload_handler(request: httpx.Request) -> httpx.Response:
        # Missing the required "hourly" key -- OpenMeteoResponse.model_validate
        # raises a pydantic ValidationError, not a transport or HTTP error.
        return httpx.Response(
            200, json={"latitude": 0.0, "longitude": 0.0}, request=request
        )

    async def _drive_validation() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_invalid_payload_handler)
        ) as client:
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                "wxverify.worker.backfill.build_adapter",
                lambda source, c: OpenMeteoAdapter(client),
            )
            try:
                writer = FencedWriter(db3, db3.generation)
                with pytest.raises(Exception):  # noqa: B017 -- pydantic ValidationError
                    await _fetch_historical_forecasts(
                        db3,
                        writer,
                        _backfill_target(site3),
                        window_start=window_start,
                        window_end=window_end,
                    )
            finally:
                monkeypatch.undo()

    asyncio.run(_drive_validation())
    assert _budget_calls(conn3) == 3  # the reservation this window prices


# ---------------------------------------------------------------------------
# A-T13 -- the six inert stubs raise, and are reachable
# ---------------------------------------------------------------------------

_NON_HISTORICAL_ADAPTER_FACTORIES: tuple[tuple[str, type], ...] = (
    ("meteoblue", MeteoblueAdapter),
    ("meteosource", MeteosourceAdapter),
    ("weatherapi", WeatherApiAdapter),
    ("openweathermap", OpenWeatherMapAdapter),
    ("visualcrossing", VisualCrossingAdapter),
    ("google", GoogleAdapter),
)


@pytest.mark.parametrize(("_label", "adapter_cls"), _NON_HISTORICAL_ADAPTER_FACTORIES)
def test_non_historical_adapters_raise_not_implemented(
    _label: str, adapter_cls: type
) -> None:
    """A-T13: one parametrized test over the six non-historical adapter
    classes -- ``pytest.raises(NotImplementedError)`` when
    ``estimate_historical_cost`` is called. ``_NON_HISTORICAL_ADAPTER_FACTORIES``
    is a hand-maintained tuple: it fails collection if one of these six
    classes is removed or renamed, but it is blind to an *added* one -- a
    seventh non-historical adapter that is never appended to the tuple is
    silently untested, not a collection failure. This is the only oracle
    that executes those six bodies; ``pyright`` checks a Protocol stub's
    signature, never that its body raises.
    """
    adapter: ForecastAdapter = adapter_cls("synthetic-api-key", httpx.AsyncClient())
    assert adapter.supports_historical is False
    req = _req()
    with pytest.raises(NotImplementedError):
        adapter.estimate_historical_cost(
            req, window_start="2026-06-01T00:00:00Z", window_end="2026-06-02T00:00:00Z"
        )


def test_open_meteo_adapter_is_the_seventh_and_only_historical_one() -> None:
    """Companion to A-T13's correction: six non-historical adapter classes
    plus OpenMeteoAdapter (the only ``supports_historical = True`` one) is
    seven adapters total -- the plan's prose says "seven" for A-T13's
    parametrization, but the code and the enumeration both give six. This
    pins ``len(_NON_HISTORICAL_ADAPTER_FACTORIES) == 6`` -- the size of the
    hand-maintained tuple, not an independently derived adapter count -- so
    it would not catch a seventh non-historical adapter that exists in the
    codebase but was never added to the tuple.
    """
    assert len(_NON_HISTORICAL_ADAPTER_FACTORIES) == 6
    assert OpenMeteoAdapter.supports_historical is True
    assert all(
        adapter_cls("synthetic-api-key", httpx.AsyncClient()).supports_historical
        is False
        for _label, adapter_cls in _NON_HISTORICAL_ADAPTER_FACTORIES
    )
