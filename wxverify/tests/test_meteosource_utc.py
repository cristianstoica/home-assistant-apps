"""T1-T6: Meteosource is asked for UTC, a reply not marked ``"UTC"`` is
refused with a fixed message, and the refusal is recorded once -- never
retried like a transient failure.

Adapter tests (T1-T3) use ``httpx.MockTransport`` and patch
``meteosource_feed.snap_run``, as ``tests/test_new_providers.py:439-487``
does. T4-T6 drive the worker path and the CLI's status row.

Isolation: T4 and T5 open a real tmp-file database via ``init_db``/
``close_db`` (mirrors ``tests/test_new_providers.py``'s ``_init_tmp_db``);
T1-T3 and T6 touch none.

Synthetic fixtures only: a site named ``Testsite`` at ``0.0, 0.0`` in zone
``UTC``, key ``test-key``, synthetic dates and zone names.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

import wxverify.feeds.meteosource as meteosource_feed
from wxverify import config
from wxverify.__main__ import _has_errors, _provider_outcome_row
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.seam import ForecastRequest
from wxverify.provider_ops import FeedRef
from wxverify.worker.feed_fetch import (
    DownloadSlot,
    FeedFetchTarget,
    Refused,
    record_download_try,
)
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.round import _round_progress, enqueue_due_round_tries

_ABSENT = object()

_ISSUED_AT = "2026-01-01T00:00:00Z"

#: Four hours, one apart, crossing a plain UTC midnight.
_MIDNIGHT_HOURS = (
    "2026-06-10T22:00:00",
    "2026-06-10T23:00:00",
    "2026-06-11T00:00:00",
    "2026-06-11T01:00:00",
)
#: Four hours, one apart, crossing both a UTC midnight and the 2026-03-29
#: clock-change date.
_MARCH_CHANGE_HOURS = (
    "2026-03-28T22:00:00",
    "2026-03-28T23:00:00",
    "2026-03-29T00:00:00",
    "2026-03-29T01:00:00",
)
#: Four hours, one apart, crossing both a UTC midnight and the 2026-11-01
#: clock-change date.
_NOVEMBER_CHANGE_HOURS = (
    "2026-10-31T22:00:00",
    "2026-10-31T23:00:00",
    "2026-11-01T00:00:00",
    "2026-11-01T01:00:00",
)


def _ms_request(max_lead_hours: int = 24) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="blend",
        variables=("temperature",),
        max_lead_hours=max_lead_hours,
    )


# ---------------------------------------------------------------------------
# T1: the request's query carries timezone=UTC
# ---------------------------------------------------------------------------


def test_t1_request_includes_timezone_utc() -> None:
    """The adapter's request always asks Meteosource for a UTC reply."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"timezone": "UTC", "hourly": {"data": []}})

    async def _fetch() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = MeteosourceAdapter("test-key", client)
            await adapter.fetch_forecast(_ms_request())

    asyncio.run(_fetch())

    assert len(captured) == 1
    params = dict(captured[0].url.params)
    assert params.get("timezone") == "UTC", (
        f"M1 -> at this assertion: correct = 'UTC', mutant = {params.get('timezone')!r}"
    )


# ---------------------------------------------------------------------------
# T2: a UTC reply is stored at its own hours, across a UTC midnight and the
# two 2026 clock-change dates.
# ---------------------------------------------------------------------------


def test_t2_a_utc_reply_is_stored_at_its_own_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reply marked ``"UTC"`` is stored at the hours it gives, and leads
    counted from the patched ``issued_at`` stay exactly one hour apart
    across a plain UTC midnight and the two 2026 clock-change dates --
    never shifted by interpreting the naive date in some other zone."""
    monkeypatch.setattr(
        meteosource_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    groups = (_MIDNIGHT_HOURS, _MARCH_CHANGE_HOURS, _NOVEMBER_CHANGE_HOURS)
    all_hours = [hour for group in groups for hour in group]
    payload = {
        "timezone": "UTC",
        "hourly": {"data": [{"date": hour, "temperature": 20.0} for hour in all_hours]},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async def _fetch() -> list[object]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = MeteosourceAdapter("test-key", client)
            result = await adapter.fetch_forecast(_ms_request(max_lead_hours=1_000_000))
            return list(result.samples)

    samples = asyncio.run(_fetch())
    by_valid_at = {sample.valid_at: sample for sample in samples}

    for hour in all_hours:
        expected_valid_at = f"{hour}Z"
        assert expected_valid_at in by_valid_at, (
            f"M2 -> at this assertion: correct = {expected_valid_at!r} present"
            f" among stored valid_at values, mutant = {sorted(by_valid_at)!r}"
        )

    issued = datetime.fromisoformat(_ISSUED_AT.replace("Z", "+00:00"))
    for group in groups:
        leads = [by_valid_at[f"{hour}Z"].lead_hours for hour in group]
        deltas = [
            later - earlier for earlier, later in zip(leads, leads[1:], strict=False)
        ]
        assert deltas == [1, 1, 1], (
            f"M2 -> at this assertion: correct = [1, 1, 1] (one hour apart"
            f" throughout {group!r}), mutant = {deltas!r}"
        )
        first_instant = datetime.fromisoformat(f"{group[0]}+00:00")
        expected_first_lead = int((first_instant - issued).total_seconds() // 3600)
        assert leads[0] == expected_first_lead, (
            f"M2 -> at this assertion: correct = {expected_first_lead}"
            f" (lead counted from the patched issued_at), mutant = {leads[0]}"
        )


# ---------------------------------------------------------------------------
# T3: every non-UTC timezone is refused, always with the fixed message.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "timezone_value",
    [
        pytest.param(_ABSENT, id="absent"),
        pytest.param(None, id="null"),
        pytest.param("", id="empty-string"),
        pytest.param("utc", id="lowercase"),
        pytest.param("Synthetic/Zone", id="zone-name"),
        pytest.param(0, id="number"),
        pytest.param(["UTC"], id="list"),
        pytest.param({"id": "UTC"}, id="object"),
    ],
)
def test_t3_every_non_utc_timezone_is_refused_with_the_fixed_message(
    timezone_value: object,
) -> None:
    """Any reply not marked exactly ``"UTC"`` is refused, with the fixed
    text -- never a message built from the reply's own timezone value, and
    never accepted by mistake for an absent, null or wrongly-typed value."""
    payload: dict[str, object] = {"hourly": {"data": []}}
    if timezone_value is not _ABSENT:
        payload["timezone"] = timezone_value

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async def _fetch() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = MeteosourceAdapter("test-key", client)
            await adapter.fetch_forecast(_ms_request())

    with pytest.raises(meteosource_feed.MeteosourceTimezoneError) as exc_info:
        asyncio.run(_fetch())

    actual = str(exc_info.value)
    assert actual == meteosource_feed._TIMEZONE_REFUSED, (  # noqa: SLF001
        f"M3/M3b -> at this assertion: correct ="
        f" {meteosource_feed._TIMEZONE_REFUSED!r}, mutant = {actual!r}"  # noqa: SLF001
    )


# ---------------------------------------------------------------------------
# T4: the worker path through the production job end.
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 0.0, 0.0, 0.0, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


def test_t4_a_refused_reply_completes_the_job_and_stores_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A refused reply ends the job cleanly, once -- not through the retry
    ladder -- and stores nothing; the refused zone's name never reaches any
    stored column or the log."""
    caplog.set_level("DEBUG")
    conn = _init_tmp_db(tmp_path)
    try:
        site_id = _make_site(conn, "Testsite")
        feed_id = int(
            conn.execute(
                "SELECT id FROM feeds WHERE source='meteosource' AND model='blend'"
            ).fetchone()["id"]
        )
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
            (site_id, feed_id),
        )
        enqueue_if_absent(
            conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
        )
        job = claim_next_job(conn)
        assert job is not None

        marker = "Synthetic/Zone"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "timezone": marker,
                    "hourly": {
                        "data": [{"date": "2026-06-01T12:00:00", "temperature": 20.0}]
                    },
                },
            )

        async def _run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as ms_client:

                def _builder(
                    source: str, client: httpx.AsyncClient
                ) -> MeteosourceAdapter:  # noqa: ARG001
                    return MeteosourceAdapter("test-key", ms_client)

                monkeypatch.setattr("wxverify.worker.processor.build_adapter", _builder)
                await run_claimed_job(get_db(), job, lane="main")

        asyncio.run(_run())

        job_row = conn.execute(
            "SELECT status, retry_count, result FROM jobs WHERE id = ?", (job.id,)
        ).fetchone()
        assert job_row["status"] == "completed", (
            "M5 -> at this assertion: correct = 'completed', mutant ="
            f" {job_row['status']!r}"
        )
        assert job_row["retry_count"] == 0, (
            f"M5 -> at this assertion: correct = 0, mutant = {job_row['retry_count']!r}"
        )
        assert job_row["result"] is None, (
            f"M4 -> at this assertion: correct = None, mutant = {job_row['result']!r}"
        )

        download_rows = list(
            conn.execute(
                "SELECT outcome, reason FROM forecast_downloads WHERE feed_id = ?",
                (feed_id,),
            )
        )
        assert len(download_rows) == 1
        assert download_rows[0]["outcome"] == "refused", (
            "M5 -> at this assertion: correct = 'refused', mutant ="
            f" {download_rows[0]['outcome']!r}"
        )
        assert download_rows[0]["reason"] == meteosource_feed._TIMEZONE_REFUSED  # noqa: SLF001

        state_row = conn.execute(
            "SELECT last_error, error_count, last_run_at"
            " FROM site_feed_state WHERE site_id = ? AND feed_id = ?",
            (site_id, feed_id),
        ).fetchone()
        assert state_row["last_error"] == meteosource_feed._TIMEZONE_REFUSED  # noqa: SLF001
        assert state_row["error_count"] == 1
        assert state_row["last_run_at"] is not None

        sample_count = conn.execute(
            "SELECT COUNT(*) FROM forecast_samples WHERE feed_id = ?", (feed_id,)
        ).fetchone()[0]
        assert sample_count == 0

        assert marker not in caplog.text, (
            "the refused zone's own name must never reach the log"
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# T5: a refused round try ends the feed's round.
# ---------------------------------------------------------------------------


def test_t5_a_refused_round_try_ends_the_round(tmp_path: Path) -> None:
    """A try 1 round row of outcome ``"refused"`` ends the feed's round:
    ``_round_progress`` reports it ended, and the scheduler's next tick, at
    try 2's offset, enqueues nothing more for that feed."""
    conn = _init_tmp_db(tmp_path)
    try:
        site_id = _make_site(conn, "Testsite")
        feed_id = int(
            conn.execute(
                "SELECT id FROM feeds WHERE source='meteosource' AND model='blend'"
            ).fetchone()["id"]
        )
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
            (site_id, feed_id),
        )

        round_date = date(2026, 6, 15)
        start = datetime(2026, 6, 15, 0, 10, tzinfo=UTC)
        record_download_try(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            slot=DownloadSlot(
                slot="round", round_date=round_date, round_try=1, started_at=start
            ),
            outcome="refused",
            ended_at=start,
            reason=meteosource_feed._TIMEZONE_REFUSED,  # noqa: SLF001
        )
        conn.commit()

        last_try, ended = _round_progress(conn, site_id, feed_id, round_date)
        assert (last_try, ended) == (1, True), (
            "M6 -> at this assertion: correct = (1, True), mutant ="
            f" {(last_try, ended)!r}"
        )

        enqueue_due_round_tries(conn, now_utc=start + timedelta(minutes=5))
        pending = conn.execute(
            "SELECT 1 FROM jobs WHERE job_key = ?", (f"round:{feed_id}",)
        ).fetchone()
        assert pending is None, (
            f"M6 -> at this assertion: correct = no enqueued round:{feed_id} job,"
            f" mutant = a job was enqueued ({pending!r})"
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# T6: the CLI's status row for a refused outcome.
# ---------------------------------------------------------------------------


def test_t6_provider_outcome_row_reports_a_refused_outcome_as_an_error() -> None:
    """``_provider_outcome_row`` on a ``Refused`` outcome reports status
    ``"error"`` with the fixed message, and ``_has_errors`` is true."""
    target = FeedFetchTarget(
        site_id=1,
        feed_id=2,
        lat=0.0,
        lon=0.0,
        source="meteosource",
        model="blend",
        max_lead_hours=24,
    )
    outcome = Refused(target=target, error=meteosource_feed._TIMEZONE_REFUSED)  # noqa: SLF001
    feed = FeedRef(
        feed_id=2, source="meteosource", model="blend", enabled=True, is_virtual=False
    )

    row = asyncio.run(_provider_outcome_row(1, feed, outcome, smoke=False))

    assert row["status"] == "error", (
        f"M7 -> at this assertion: correct = 'error', mutant = {row['status']!r}"
    )
    assert row["message"] == meteosource_feed._TIMEZONE_REFUSED  # noqa: SLF001
    assert _has_errors([row]) is True
