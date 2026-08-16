"""Google adapter pagination oracles — 0.12.0 §15 families 1-6.

Covers ``wxverify.feeds.google``'s paginated horizon collection (W2), the
horizon-derived budget reservation (W3), and partial-sequence budget honesty
(W4). Families 7-9 (horizon correction, snapshot provenance, composite
disclosure) are out of scope for this module.

Every fixture in this file is synthetic: a fake site at ``lat=40.0,
lon=-105.0`` (``America/Denver`` convention), an obviously-fake API key, and
obviously-fake page tokens (``"token-page-N"``).

Two drive modes are used throughout:

- **Direct adapter drive** (``_fetch``) — builds a real ``GoogleAdapter``
  around an ``httpx.MockTransport`` and calls ``fetch_forecast`` directly.
  Used for oracles that only need the adapter's return value / raised
  exception.
- **Real fetch_feed_once drive** (``_drive_fetch_feed_once``) — routes a
  real ``GoogleAdapter`` (same MockTransport substitution) through the
  actual worker persistence path, against a real temporary SQLite database.
  Used for the oracles that must observe a database effect (persisted rows,
  ``api_budget`` state), per §15 family 4's "confirm the effect, not the
  emission."
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

import wxverify.feeds.google as google_feed
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.feeds.google import (
    GOOGLE_MAX_HOURS,
    GOOGLE_PAGE_SIZE,
    GoogleAdapter,
    GooglePageSequenceError,
    _expected_pages,
)
from wxverify.feeds.seam import ForecastRequest
from wxverify.worker.feed_fetch import fetch_feed_once

# ---------------------------------------------------------------------------
# Shared constants and fixtures
# ---------------------------------------------------------------------------

SITE_LAT = 40.0
SITE_LON = -105.0

_VARIABLES = ("temperature", "wind", "precip")

# Fixed synthetic issued_at, used wherever the test does not care about the
# real cadence-snap arithmetic (already covered by test_new_providers.py).
_ISSUED_AT = "2026-06-01T00:00:00Z"


def _req(max_lead_hours: int) -> ForecastRequest:
    return ForecastRequest(
        lat=SITE_LAT,
        lon=SITE_LON,
        model="blend",
        variables=_VARIABLES,
        max_lead_hours=max_lead_hours,
    )


def _hour(start: datetime) -> dict[str, object]:
    """One synthetic Google forecastHours record, all three metrics present."""
    return {
        "interval": {"startTime": isoformat_utc(start)},
        "temperature": {"degrees": 12.0, "unit": "CELSIUS"},
        "wind": {"speed": {"value": 10.0, "unit": "KILOMETERS_PER_HOUR"}},
        "precipitation": {"qpf": {"quantity": 0.0, "unit": "MILLIMETERS"}},
    }


def _empty_hour(start: datetime) -> dict[str, object]:
    """A record whose metric objects are all absent (structurally-whole-but-empty)."""
    return {"interval": {"startTime": isoformat_utc(start)}}


def _chunks(total_hours: int, start: datetime) -> list[list[dict[str, object]]]:
    """``total_hours`` contiguous hourly records, split into GOOGLE_PAGE_SIZE pages."""
    records = [_hour(start + timedelta(hours=i)) for i in range(total_hours)]
    return [
        records[i : i + GOOGLE_PAGE_SIZE]
        for i in range(0, len(records), GOOGLE_PAGE_SIZE)
    ]


def _tokenize(
    chunks: list[list[dict[str, object]]],
) -> list[tuple[list[dict[str, object]], str | None]]:
    """Pair each chunk with the token it returns; the last page returns None."""
    pages: list[tuple[list[dict[str, object]], str | None]] = []
    for i, chunk in enumerate(chunks):
        token = None if i == len(chunks) - 1 else f"token-page-{i + 2}"
        pages.append((chunk, token))
    return pages


def _make_handler(
    pages: list[tuple[list[dict[str, object]], str | None]],
    requests_log: list[dict[str, str]] | None = None,
    fail_at_index: int | None = None,
):
    """Sequential MockTransport handler serving ``pages`` in call order.

    Requests are inherently sequential here (the adapter awaits each
    response before issuing the next), so a simple call counter is a
    faithful stand-in for the ``pageToken`` the real API would echo.
    ``fail_at_index`` raises ``httpx.ConnectError`` instead of serving that
    page (and every page is served in order up to it).
    """
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        idx = call_count["n"]
        call_count["n"] += 1
        if requests_log is not None:
            requests_log.append(dict(request.url.params))
        if fail_at_index is not None and idx == fail_at_index:
            raise httpx.ConnectError("synthetic connect failure")
        forecast_hours, next_token = pages[idx]
        return httpx.Response(
            200, json={"forecastHours": forecast_hours, "nextPageToken": next_token}
        )

    return handler


async def _fetch(handler, req: ForecastRequest):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GoogleAdapter("synthetic-api-key", client)
        return await adapter.fetch_forecast(req)


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
            VALUES ('GooglePagination', ?, ?, 900.0, 'UTC')
            """,
            (SITE_LAT, SITE_LON),
        ).lastrowid
    )


def _google_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='google' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)
        VALUES (?, ?, 1, 0)
        """,
        (site_id, feed_id),
    )


def _sample_count(conn: sqlite3.Connection, feed_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_samples WHERE feed_id=?", (feed_id,)
        ).fetchone()["n"]
    )


def _budget_calls(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT calls FROM api_budget WHERE source='google'").fetchone()
    return 0 if row is None else int(row["calls"])


async def _drive_fetch_feed_once(db, site_id: int, feed_id: int, handler):
    """Route a real GoogleAdapter (MockTransport-substituted) through the
    actual worker fetch+persist path — the "real fetch_feed path" §15
    family 4 requires.
    """
    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:

        def _build(source: str, client: httpx.AsyncClient) -> GoogleAdapter:
            return GoogleAdapter("synthetic-api-key", mock_client)

        return await fetch_feed_once(db, site_id, feed_id, adapter_builder=_build)
    finally:
        await mock_client.aclose()


def _setup_google_site(tmp_path: Path):
    """A real temp DB with one site subscribed to the google feed. Returns
    (db, conn, site_id, feed_id)."""
    from wxverify.db.connection import get_db

    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    db = get_db()
    return db, conn, site_id, feed_id


# ---------------------------------------------------------------------------
# Family 1 — Pagination assembly
# ---------------------------------------------------------------------------


def test_pagination_assembly_seven_requests_for_168_hour_horizon() -> None:
    """168h horizon: 7 sequential requests, token n = response n-1's token,
    request 1 carries none, pageSize=24 on every request, and the assembled
    retained sample set covers a contiguous lead range with no gap/duplicate.

    Regression pin (§15 item 10): fails on bc617be, which makes exactly one
    request and never reads/echoes a pageToken at all.
    """
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(google_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    try:
        # 2h lag before the raw sequence starts (representative of the real
        # RUN_AVAILABILITY_LAG_MINUTES gap between issued_at and "now").
        start = datetime(2026, 6, 1, 2, 0, 0, tzinfo=UTC)
        pages = _tokenize(_chunks(168, start))
        requests_log: list[dict[str, str]] = []
        handler = _make_handler(pages, requests_log=requests_log)

        result = asyncio.run(_fetch(handler, _req(168)))

        assert len(requests_log) == 7
        assert "pageToken" not in requests_log[0]
        for i in range(1, 7):
            assert requests_log[i]["pageToken"] == f"token-page-{i + 1}"
        assert all(r["pageSize"] == str(GOOGLE_PAGE_SIZE) for r in requests_log)

        leads = {sample.lead_hours for sample in result.samples}
        assert leads == set(range(2, 169))  # 167 contiguous leads, no gap/dup
        assert len(result.samples) == len(leads) * 3  # 3 variables retained
    finally:
        monkeypatch.undo()


# ---------------------------------------------------------------------------
# Family 2 — Completeness failure modes (seven independent oracles)
# ---------------------------------------------------------------------------


def _two_page_start() -> datetime:
    return datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)


# Two hours before `_two_page_start()`, aligning every family-2/mid-sequence
# fixture's raw window with the real clock's neighborhood so a zero
# persisted-row count reflects the no-partial-persistence guard, never an
# independent "every lead is negative" artifact of running against the
# unpatched wall clock (2026-08-15) against 2026-06-01 fixture data.
_ALIGNED_ISSUED_AT = "2026-05-31T22:00:00Z"


def _assert_zero_persisted(
    tmp_path: Path, handler, *, match: str, max_lead_hours: int
) -> None:
    """``max_lead_hours`` re-points the DB-path feed row at the SAME horizon
    (and therefore the same expected page count) as its paired
    direct-adapter fixture -- ``_setup_google_site`` otherwise leaves the
    feed at 168h / 7 pages. Measured, 4 of the 7 call sites below need
    that: the ones whose scenario-specific failure is a sequence-level
    check (hourly spacing, total record count), which runs only once the
    page loop has completed. At 7 expected pages the per-page "token before
    the horizon" check is still armed on the fixture's last page, which
    carries no token, so it raises the generic "no nextPageToken before the
    requested horizon" first and the sequence check is never reached -- the
    loop stops on that guard rather than exhausting the fixture. The other
    3 call sites fail inside a per-page check that fires on the same page
    under either horizon. ``match`` then pins the DB-path failure to the
    SAME failure mode as the direct-adapter assertion above each call site,
    rather than accepting any ``GooglePageSequenceError``.
    """
    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    conn.execute(
        "UPDATE feeds SET max_lead_hours = ? WHERE id = ?", (max_lead_hours, feed_id)
    )
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        google_feed, "snap_run", lambda fetch_time=None: _ALIGNED_ISSUED_AT
    )
    try:
        with pytest.raises(GooglePageSequenceError, match=match):
            asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))
        assert _sample_count(conn, feed_id) == 0
    finally:
        monkeypatch.undo()


def test_completeness_missing_next_page_token_before_horizon(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    # Page 0 (index 0 < pages-1=1) omits its token — the "successful but
    # short" case the plan names explicitly.
    pages = [(chunks[0], None), (chunks[1], None)]
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="no nextPageToken"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path, _make_handler(pages), match="no nextPageToken", max_lead_hours=48
    )


def test_completeness_page_echoes_sent_token(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    # Page 1 echoes the very token it was sent instead of progressing.
    pages = [(chunks[0], "token-page-2"), (chunks[1], "token-page-2")]
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="echoed the pageToken"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="echoed the pageToken",
        max_lead_hours=48,
    )


def test_completeness_duplicate_start_time_across_page_boundary(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    # First record of page 1 duplicates the last record of page 0.
    chunks[1][0] = _hour(start + timedelta(hours=23))
    pages = _tokenize(chunks)
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="not exactly one hour"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="not exactly one hour",
        max_lead_hours=48,
    )


def test_completeness_non_monotonic_start_time(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    # First record of page 1 moves BACKWARD relative to page 0's last record.
    chunks[1][0] = _hour(start + timedelta(hours=22))
    pages = _tokenize(chunks)
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="not exactly one hour"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="not exactly one hour",
        max_lead_hours=48,
    )


def test_completeness_one_hour_gap(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    # First record of page 1 skips an hour past page 0's last record.
    chunks[1][0] = _hour(start + timedelta(hours=25))
    pages = _tokenize(chunks)
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="not exactly one hour"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="not exactly one hour",
        max_lead_hours=48,
    )


def test_completeness_empty_forecast_hours_on_middle_page(tmp_path: Path) -> None:
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    chunks = _chunks(72, start)  # 3 pages of 24
    chunks[1] = []
    pages = [
        (chunks[0], "token-page-2"),
        (chunks[1], "token-page-3"),
        (chunks[2], None),
    ]
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="empty forecastHours"):
        asyncio.run(_fetch(handler, _req(72)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="empty forecastHours",
        max_lead_hours=72,
    )


def test_completeness_total_record_count_one_short(tmp_path: Path) -> None:
    start = _two_page_start()
    chunks = _chunks(48, start)
    chunks[1] = chunks[1][:-1]  # 23 records instead of 24 — one short overall
    pages = [(chunks[0], "token-page-2"), (chunks[1], None)]
    handler = _make_handler(pages)

    with pytest.raises(GooglePageSequenceError, match="accumulated 47 records"):
        asyncio.run(_fetch(handler, _req(48)))

    _assert_zero_persisted(
        tmp_path,
        _make_handler(pages),
        match="accumulated 47 records",
        max_lead_hours=48,
    )


# ---------------------------------------------------------------------------
# Family 3 — Raw-versus-retained
# ---------------------------------------------------------------------------


def test_raw_sequence_whole_but_retained_sample_count_strictly_smaller() -> None:
    """A complete 168-record raw sequence whose last few records exceed
    ``req.max_lead_hours`` is accepted, and the retained sample count is
    strictly less than the raw record count.

    Regression pin (§15 item 10): a completeness check written against the
    retained count (rather than the raw count) fails this oracle.
    """
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(google_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    try:
        start = datetime(2026, 6, 1, 2, 0, 0, tzinfo=UTC)  # 2h lag, as family 1
        chunks = _chunks(168, start)
        raw_count = sum(len(chunk) for chunk in chunks)
        assert raw_count == 168
        pages = _tokenize(chunks)
        handler = _make_handler(pages)

        result = asyncio.run(_fetch(handler, _req(168)))

        retained_count = len({sample.valid_at for sample in result.samples})
        assert retained_count == 167
        assert retained_count < raw_count
    finally:
        monkeypatch.undo()


# ---------------------------------------------------------------------------
# Family 4 — No-partial persistence
# ---------------------------------------------------------------------------


def test_mid_sequence_failure_persists_zero_rows(tmp_path: Path) -> None:
    """A failure discovered mid-sequence (after real progress, before the
    last page) leaves ``forecast_samples`` at zero — the effect is
    confirmed, not merely that an exception escaped.

    Regression pin (§15 item 10): on bc617be a single-page fetch cannot
    exhibit a "mid-sequence" failure at all (there is only ever one page),
    so this scenario has no analogue there — see the evidence report.
    """
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    chunks = _chunks(72, start)  # 3 pages
    # Page 1 omits its token -- the second of the seven pages the feed's
    # 168h horizon expects, so genuinely mid-sequence.
    pages = [(chunks[0], "token-page-2"), (chunks[1], None), (chunks[2], None)]
    requests_log: list[dict[str, str]] = []
    handler = _make_handler(pages, requests_log=requests_log)

    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        google_feed, "snap_run", lambda fetch_time=None: _ALIGNED_ISSUED_AT
    )
    try:
        with pytest.raises(GooglePageSequenceError):
            asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))

        # Confirm this really was mid-sequence: page 0 succeeded before page 1 failed.
        assert len(requests_log) == 2
        assert _sample_count(conn, feed_id) == 0
    finally:
        monkeypatch.undo()


def test_successful_seven_page_fetch_persists_all_retained_samples(
    tmp_path: Path,
) -> None:
    """Positive control for the no-partial-persistence guard (family 4): a
    genuinely successful 168-hour / 7-page fetch through the real
    ``fetch_feed_once`` persistence path persists every retained sample --
    167 retained leads (2..168, per family 1/3's arithmetic) x 3 variables
    = 501 rows -- proving the zero counts elsewhere in this family reflect
    the guard actually firing, not a harness that persists nothing
    unconditionally.
    """
    start = datetime(2026, 6, 1, 2, 0, 0, tzinfo=UTC)  # 2h lag, as family 1/3
    pages = _tokenize(_chunks(168, start))
    handler = _make_handler(pages)

    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(google_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    try:
        asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))
        assert _sample_count(conn, feed_id) == 501  # 167 leads x 3 variables
    finally:
        monkeypatch.undo()


def test_metrics_absent_response_returns_empty_samples_without_raising() -> None:
    """A structurally-whole sequence whose metric objects are all None
    parses, passes the completeness check, and yields zero samples without
    raising.

    Preservation invariant (§15 item 10): pass-before AND pass-after — the
    ``is not None`` guards this depends on already existed on bc617be
    (``google.py:181,195,209`` there); this oracle exists only to prove W2
    does not disturb it.
    """
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    records = [_empty_hour(start + timedelta(hours=i)) for i in range(24)]
    pages = [(records, None)]
    handler = _make_handler(pages)

    result = asyncio.run(_fetch(handler, _req(24)))

    assert result.samples == []


# ---------------------------------------------------------------------------
# Family 5 — Budget reservation exactness
# ---------------------------------------------------------------------------


_HORIZON_TABLE: tuple[tuple[int, int], ...] = (
    (1, 1),
    (23, 1),
    (24, 1),
    (25, 2),
    (48, 2),
    (167, 7),
    (168, 7),
    (169, 8),
    (240, 10),
    (241, 10),
    (480, 10),
)


@pytest.mark.parametrize(("max_lead_hours", "expected_calls"), _HORIZON_TABLE)
def test_estimate_cost_matches_hand_written_table(
    max_lead_hours: int, expected_calls: int
) -> None:
    import math

    client = httpx.AsyncClient()
    try:
        adapter = GoogleAdapter("synthetic-api-key", client)
        req = _req(max_lead_hours)
        cost = adapter.estimate_cost(req)
        assert cost.calls == expected_calls
        assert cost.calls == math.ceil(
            min(max_lead_hours, GOOGLE_MAX_HOURS) / GOOGLE_PAGE_SIZE
        )
        assert cost.calls == _expected_pages(req)
    finally:
        asyncio.run(client.aclose())


@pytest.mark.parametrize(("max_lead_hours", "expected_calls"), _HORIZON_TABLE)
def test_estimate_cost_equals_observed_request_count(
    max_lead_hours: int, expected_calls: int
) -> None:
    """The binding oracle: for each horizon, the observed request count
    made by ``fetch_forecast`` equals ``estimate_cost(...).calls`` — pinning
    the reservation to the actual spend, not to a formula.
    """
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    requested_hours = min(max_lead_hours, GOOGLE_MAX_HOURS)
    chunks = _chunks(requested_hours, start)
    pages = _tokenize(chunks)
    requests_log: list[dict[str, str]] = []
    handler = _make_handler(pages, requests_log=requests_log)

    client = httpx.AsyncClient()
    adapter = GoogleAdapter("synthetic-api-key", client)
    assert adapter.estimate_cost(_req(max_lead_hours)).calls == expected_calls

    async def _drive():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            live_adapter = GoogleAdapter("synthetic-api-key", c)
            await live_adapter.fetch_forecast(_req(max_lead_hours))

    asyncio.run(_drive())
    asyncio.run(client.aclose())

    assert len(requests_log) == expected_calls


def test_api_budget_shows_seven_calls_after_successful_fetch(tmp_path: Path) -> None:
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    pages = _tokenize(_chunks(168, start))
    handler = _make_handler(pages)

    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))

    assert _budget_calls(conn) == 7


def test_api_budget_stays_at_seven_after_page_five_connect_failure(
    tmp_path: Path,
) -> None:
    """A connect failure on page 5 (index 4) is re-raised as
    ``GooglePageSequenceError`` — non-refundable — so the whole 7-call
    reservation is kept even though only 5 requests were attempted.
    """
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    pages = _tokenize(_chunks(168, start))
    handler = _make_handler(pages, fail_at_index=4)

    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    with pytest.raises(GooglePageSequenceError):
        asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))

    assert _budget_calls(conn) == 7


def test_api_budget_zero_after_page_one_connect_failure(tmp_path: Path) -> None:
    """A connect failure on page 1 (index 0) propagates as a raw
    ``httpx.ConnectError`` — refundable — so the reservation nets to zero.
    """
    start = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    pages = _tokenize(_chunks(168, start))
    handler = _make_handler(pages, fail_at_index=0)

    db, conn, site_id, feed_id = _setup_google_site(tmp_path)
    with pytest.raises(httpx.ConnectError):
        asyncio.run(_drive_fetch_feed_once(db, site_id, feed_id, handler))

    assert _budget_calls(conn) == 0


# ---------------------------------------------------------------------------
# Family 6 — One run identity
# ---------------------------------------------------------------------------


def test_snap_run_called_exactly_once_across_a_seven_page_sequence() -> None:
    """Every sample from a seven-page sequence shares one ``issued_at`` and
    one ``model_run_id``, and ``snap_run`` is called exactly once for the
    whole sequence — pinned with a counter that would return a DIFFERENT
    value on a second call, so a call-count regression would also produce a
    split ``issued_at``/``model_run_id`` across the sample set.

    Preservation half (call count): passes before and after — bc617be calls
    ``snap_run`` once per fetch too (one request, per §15 item 10).
    Regression-pin half (multi-page assembly): fails on bc617be, which
    makes one request and retains only that page's leads (12-24), never
    leads spanning seven pages.
    """
    call_log: list[str] = []

    def _counting_snap_run(fetch_time: str | None = None) -> str:
        call_log.append("call")
        return f"2026-06-01T{len(call_log):02d}:00:00Z"  # a DIFFERENT value each call

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(google_feed, "snap_run", _counting_snap_run)
    try:
        start = datetime(2026, 6, 1, 2, 0, 0, tzinfo=UTC)
        pages = _tokenize(_chunks(168, start))
        requests_log: list[dict[str, str]] = []
        handler = _make_handler(pages, requests_log=requests_log)

        result = asyncio.run(_fetch(handler, _req(168)))

        assert len(call_log) == 1
        assert len(requests_log) == 7
        issued_ats = {sample.issued_at for sample in result.samples}
        model_run_ids = {sample.model_run_id for sample in result.samples}
        assert len(issued_ats) == 1
        assert len(model_run_ids) == 1
        leads = {sample.lead_hours for sample in result.samples}
        assert len(leads) > GOOGLE_PAGE_SIZE  # spans more than one page's worth
    finally:
        monkeypatch.undo()
