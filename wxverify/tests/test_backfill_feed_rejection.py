"""Setup backfill's per-feed refusal skip (plan 2026-10-02-icon-eu.md §4.8,
§8.5, BF-T1..T5).

Today, any non-backoff ``httpx.HTTPStatusError`` from a feed's historical
fetch ends in a bare ``raise`` inside ``_fetch_historical_forecasts``, which
aborts the whole site's chunk and fails the job for retry -- even when the
provider refused only one feed. The fix lets setup backfill skip a feed the
provider refuses with a 4xx status other than 408/429, record the error,
and carry on with the site's other feeds; an all-feeds refusal still raises
so the job retries exactly as before.

Three synthetic Open-Meteo feeds (A < B < C by id, the three lowest-id
seeded models) stand in for the roster; every other seeded feed is
disabled for the site so only these three are fetched. A fake historical
adapter answers each feed with either a normalized sample or a configured
HTTP refusal, monkeypatched at ``wxverify.worker.backfill.build_adapter``
(the name backfill imports into its own module namespace, so patching it
there -- not at ``wxverify.feeds.registry`` -- is what the production code
actually calls).

Synthetic data only (public repo): site name "Testsite", open-ocean
coordinates with no identifiable place, and a refusal request URL carrying
a synthetic ``latitude=`` query parameter (never a real coordinate) so a
leak of it would be visible to the "no coordinate rendering" assertions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    NormalizedSample,
)
from wxverify.worker.backfill import run_backfill_site
from wxverify.worker.control import JobDeferred
from wxverify.worker.processor import _complete_and_continue  # noqa: SLF001

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

_SITE_NAME = "Testsite"
_SITE_LAT = 12.345678
_SITE_LON = -40.123456
_REQUEST_LAT = "12.345678"  # mirrored into the refusal request URL


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
            VALUES (?, ?, ?, 0.0, 'UTC')
            """,
            (_SITE_NAME, _SITE_LAT, _SITE_LON),
        ).lastrowid
    )


def _three_open_meteo_feed_ids(
    conn: sqlite3.Connection, site_id: int
) -> tuple[int, int, int]:
    """Keep the three lowest-id seeded Open-Meteo feeds (A < B < C) enabled
    for this site; disable every other Open-Meteo feed via
    ``site_feed_state``, so backfill fetches exactly these three.
    """
    rows = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id"
    ).fetchall()
    ids = [int(row["id"]) for row in rows]
    assert len(ids) >= 3, "fewer than three seeded open-meteo feeds"
    a, b, c = ids[0], ids[1], ids[2]
    for feed_id in ids[3:]:
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, feed_id),
        )
    return a, b, c


def _refusal(status: int, *, with_host: bool) -> httpx.HTTPStatusError:
    """Build the HTTPStatusError httpx itself raises for ``status``: its
    message names the request URL, which carries a synthetic latitude=
    parameter, so a WARNING carrying the exception or its sanitized text
    would be caught by the log assertions below.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={_REQUEST_LAT}&model=x"
        if with_host
        else f"/forecast?latitude={_REQUEST_LAT}&model=x"
    )
    request = httpx.Request("GET", url)
    response = httpx.Response(status, request=request)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return exc
    raise AssertionError(f"raise_for_status did not raise for {status}")


def _ok_sample(model: str) -> FetchResult:
    return FetchResult(
        samples=[
            NormalizedSample(
                model=model,
                variable="temperature",
                issued_at="2026-06-01T00:00:00Z",
                valid_at="2026-06-01T06:00:00Z",
                lead_hours=6,
                value=11.0,
                source_raw="11.0 synthetic",
                model_run_id=f"{model}:2026-06-01T00:00:00Z",
            )
        ],
        grid=None,
    )


class _FakeHistoricalAdapter:
    """Answers ``fetch_historical`` per-model from a caller-supplied map:
    either a result (success) or an exception instance (raised)."""

    supports_historical = True

    def __init__(self, outcomes: dict[str, FetchResult | BaseException]) -> None:
        self._outcomes = outcomes

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1)

    def estimate_historical_cost(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        raise AssertionError("backfill should use historical replay")

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        outcome = self._outcomes[req.model]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _patch_build_adapter(
    monkeypatch: pytest.MonkeyPatch, outcomes: dict[str, FetchResult | BaseException]
) -> None:
    def fake_build_adapter(
        source: str, client: httpx.AsyncClient
    ) -> _FakeHistoricalAdapter:
        assert source == "open-meteo"
        return _FakeHistoricalAdapter(outcomes)

    monkeypatch.setattr("wxverify.worker.backfill.build_adapter", fake_build_adapter)


def _model_of(conn: sqlite3.Connection, feed_id: int) -> str:
    row = conn.execute("SELECT model FROM feeds WHERE id=?", (feed_id,)).fetchone()
    assert row is not None
    return str(row["model"])


def _enqueue_and_claim_backfill_job(
    conn: sqlite3.Connection, site_id: int, payload: dict[str, object]
) -> int:
    result = enqueue_if_absent(
        conn, "backfill_site", site_id, f"backfill:{site_id}", payload
    )
    assert result.created
    job = claim_next_job(conn)
    assert job is not None
    assert job.type == "backfill_site"
    return job.id


def _sample_count(conn: sqlite3.Connection, site_id: int, feed_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_samples WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    return int(row["n"])


def _budget_calls(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT SUM(calls) AS n FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    return 0 if row["n"] is None else int(row["n"])


def _last_error(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT last_error, error_count, enabled
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()


_WINDOW_START = "2026-06-01T00:00:00Z"
_WINDOW_END_15D = (
    "2026-06-16T00:00:00Z"  # 15 days: one 7-day chunk, then a continuation
)
_CHUNK_END = "2026-06-08T00:00:00Z"  # window_start + 7 days (BACKFILL_CHUNK_DAYS)
_WINDOW_END_7D = "2026-06-08T00:00:00Z"  # exactly one chunk: the last chunk


def _payload(window_end: str) -> dict[str, object]:
    return {
        "site_id": None,  # filled by the caller
        "window_start": _WINDOW_START,
        "window_end": window_end,
        "cursor_start": _WINDOW_START,
        "station_history_complete": True,
    }


# ---------------------------------------------------------------------------
# BF-T1 -- core: one feed refused, the other two persist, backfill continues
# ---------------------------------------------------------------------------


def test_bf_t1_one_refused_feed_does_not_abort_the_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: _ok_sample(a_model),
            b_model: _refusal(400, with_host=True),
            c_model: _ok_sample(c_model),
        },
    )

    payload = _payload(_WINDOW_END_15D)
    payload["site_id"] = site_id
    job_id = _enqueue_and_claim_backfill_job(conn, site_id, payload)

    db = get_db()
    writer = FencedWriter(db, db.generation)
    caplog.clear()
    continuation = asyncio.run(run_backfill_site(db, writer, site_id, payload))

    # -- no exception escaped; A and C persisted ---------------------------
    assert continuation is not None
    for feed_id in (a_id, c_id):
        assert _sample_count(conn, site_id, feed_id) == 1, (
            f"feed {feed_id} did not persist"
        )
    assert _sample_count(conn, site_id, b_id) == 0

    # -- continuation and site row -----------------------------------------
    assert continuation.payload["cursor_start"] == _CHUNK_END
    pending = conn.execute(
        """
        SELECT COUNT(*) AS n FROM jobs
        WHERE type='backfill_site' AND site_id=? AND status='pending'
        """,
        (site_id,),
    ).fetchone()["n"]
    assert pending == 0  # not yet enqueued until _complete_and_continue runs

    _complete_and_continue(conn, job_id, continuation)

    row = conn.execute(
        "SELECT payload, status FROM jobs WHERE type='backfill_site' AND site_id=?",
        (site_id,),
    ).fetchall()
    pending_rows = [r for r in row if r["status"] == "pending"]
    assert len(pending_rows) == 1
    assert json.loads(pending_rows[0]["payload"])["cursor_start"] == _CHUNK_END

    site_row = conn.execute(
        "SELECT backfill_through, backfill_status FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["backfill_through"] == _CHUNK_END
    assert site_row["backfill_status"] == "in_progress"

    # -- B's recorded error --------------------------------------------------
    b_state = _last_error(conn, site_id, b_id)
    assert b_state is not None
    assert str(b_state["last_error"]).startswith("HTTP 400")
    assert b_state["error_count"] == 1
    assert b_state["enabled"] is None

    backoff_count = conn.execute(
        "SELECT COUNT(*) AS n FROM domain_backoffs"
    ).fetchone()["n"]
    assert backoff_count == 0

    # B's reservation (1 call) plus A's and C's (1 each) stay spent: no
    # refund happens on an HTTPStatusError refusal.
    assert _budget_calls(conn) == 3

    # -- logging: exactly one WARNING, no leaked URL or coordinate ---------
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 1, [r.getMessage() for r in caplog.records]
    assert "status=400" in warnings[0].getMessage()
    for record in caplog.records:
        if record.levelno >= logging.WARNING:
            msg = record.getMessage()
            assert "://" not in msg
            assert "latitude" not in msg


# ---------------------------------------------------------------------------
# BF-T2 -- all three feeds refused: the all-refused guard re-raises
# ---------------------------------------------------------------------------


def test_bf_t2_all_feeds_refused_raises_and_leaves_backfill_through_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: _refusal(403, with_host=True),
            b_model: _refusal(400, with_host=True),
            c_model: _refusal(404, with_host=True),
        },
    )

    payload = _payload(_WINDOW_END_15D)
    payload["site_id"] = site_id

    db = get_db()
    writer = FencedWriter(db, db.generation)
    before = conn.execute(
        "SELECT backfill_through FROM sites WHERE id=?", (site_id,)
    ).fetchone()["backfill_through"]
    assert before is None

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run_backfill_site(db, writer, site_id, payload))

    after = conn.execute(
        "SELECT backfill_through FROM sites WHERE id=?", (site_id,)
    ).fetchone()["backfill_through"]
    assert after == before

    for feed_id in (a_id, b_id, c_id):
        state = _last_error(conn, site_id, feed_id)
        assert state is not None and state["last_error"] is not None


# ---------------------------------------------------------------------------
# BF-T3 -- positive control: all three succeed
# ---------------------------------------------------------------------------


def test_bf_t3_all_feeds_succeed_persists_everything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: _ok_sample(a_model),
            b_model: _ok_sample(b_model),
            c_model: _ok_sample(c_model),
        },
    )

    payload = _payload(_WINDOW_END_15D)
    payload["site_id"] = site_id

    db = get_db()
    writer = FencedWriter(db, db.generation)
    continuation = asyncio.run(run_backfill_site(db, writer, site_id, payload))

    assert continuation is not None
    assert continuation.payload["cursor_start"] == _CHUNK_END
    for feed_id in (a_id, b_id, c_id):
        assert _sample_count(conn, site_id, feed_id) == 1

    site_row = conn.execute(
        "SELECT backfill_through, backfill_status FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["backfill_through"] == _CHUNK_END
    assert site_row["backfill_status"] == "in_progress"


# ---------------------------------------------------------------------------
# BF-T4 -- status table: which statuses continue vs defer vs raise
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "with_host", "expect"),
    [
        (403, True, "continue"),
        (404, True, "continue"),
        (429, True, "deferred"),
        (500, True, "deferred"),
        (408, True, "raises"),
        (302, True, "raises"),
        (408, False, "raises"),
        (302, False, "raises"),
        (429, False, "raises"),
        (500, False, "raises"),
    ],
)
def test_bf_t4_status_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    with_host: bool,
    expect: str,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: _ok_sample(a_model),
            b_model: _refusal(status, with_host=with_host),
            c_model: _ok_sample(c_model),
        },
    )

    payload = _payload(_WINDOW_END_15D)
    payload["site_id"] = site_id

    db = get_db()
    writer = FencedWriter(db, db.generation)

    if expect == "continue":
        continuation = asyncio.run(run_backfill_site(db, writer, site_id, payload))
        assert continuation is not None
        for feed_id in (a_id, c_id):
            assert _sample_count(conn, site_id, feed_id) == 1
        b_state = _last_error(conn, site_id, b_id)
        assert b_state is not None and b_state["last_error"] is not None
    elif expect == "deferred":
        with pytest.raises(JobDeferred):
            asyncio.run(run_backfill_site(db, writer, site_id, payload))
    else:
        assert expect == "raises"
        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(run_backfill_site(db, writer, site_id, payload))


# ---------------------------------------------------------------------------
# BF-T5 -- last chunk: a refusal in the final chunk still completes the site
# ---------------------------------------------------------------------------


def test_bf_t5_last_chunk_completes_despite_a_refused_feed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: _ok_sample(a_model),
            b_model: _refusal(400, with_host=True),
            c_model: _ok_sample(c_model),
        },
    )

    payload = _payload(_WINDOW_END_7D)  # exactly one 7-day chunk: window == chunk
    payload["site_id"] = site_id

    db = get_db()
    writer = FencedWriter(db, db.generation)
    continuation = asyncio.run(run_backfill_site(db, writer, site_id, payload))

    assert continuation is None
    site_row = conn.execute(
        "SELECT backfill_through, backfill_status FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["backfill_status"] == "complete"
    assert site_row["backfill_through"] == _WINDOW_END_7D


# ---------------------------------------------------------------------------
# BF-T6 -- a refused feed alongside feeds that answer but store nothing: the
# all-refused guard counts answered feeds, not written samples
# ---------------------------------------------------------------------------


def test_bf_t6_refused_feed_with_empty_answering_feeds_still_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    a_id, b_id, c_id = _three_open_meteo_feed_ids(conn, site_id)
    a_model, b_model, c_model = (_model_of(conn, i) for i in (a_id, b_id, c_id))

    _patch_build_adapter(
        monkeypatch,
        {
            a_model: FetchResult(samples=[], grid=None),
            b_model: _refusal(400, with_host=True),
            c_model: FetchResult(samples=[], grid=None),
        },
    )

    payload = _payload(_WINDOW_END_15D)
    payload["site_id"] = site_id

    db = get_db()
    writer = FencedWriter(db, db.generation)
    continuation = asyncio.run(run_backfill_site(db, writer, site_id, payload))

    assert continuation is not None
    assert continuation.payload["cursor_start"] == _CHUNK_END
    for feed_id in (a_id, c_id):
        assert _sample_count(conn, site_id, feed_id) == 0

    site_row = conn.execute(
        "SELECT backfill_through FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["backfill_through"] == _CHUNK_END

    b_state = _last_error(conn, site_id, b_id)
    assert b_state is not None
    assert b_state["last_error"] is not None
