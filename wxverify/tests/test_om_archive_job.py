"""The job wiring (plan Sec4 T9, T10) and the job-path part of T13:
``worker/om_archive_fill.py``'s ``run_om_archive_job``/``enqueue_om_archive``/
``FillJobState``/``quiet_end_local``, and ``worker/processor.py``'s
``dispatch`` branch for ``backfill_site``.

Isolation: every connection is a real, file-backed SQLite database under
``tmp_path`` (``init_db`` -- the real schema and boot seed, no mocked
persistence); archive replies come through ``httpx.MockTransport``, which
counts the requests actually made. The clock the job reads
(``om_archive_fill.utc_now``) is frozen to a fixed, made-up instant safely
outside the quiet window (2026-06-11T10:00Z) for every test that is not
itself testing the quiet window, so no test's outcome depends on the real
wall clock.

Synthetic data only: made-up sites at coordinates 0.0, 0.0 (12.3456,
65.4321 for T13's leak checks, matching ``tests/test_om_archive_fetch_run.py``)
in zone UTC, made-up dates from 2026-06-11 on, and the boot-seeded feed
models already named in ``ALLOWANCE_MINUTES``/``RUN_CADENCE_HOURS``.

The T13 leak checks here cover only what step 4 adds: ``jobs.last_error``,
log lines from job runs, and the ``om-archive fill`` CLI. ``fetch_run``'s
own no-leak behaviour (every HTTP status, and the CLI's ``om-archive
check``) is already covered by ``tests/test_om_archive_fetch_run.py`` and is
not duplicated here.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import httpx
import pytest

import wxverify.collection.budget as budget_module
import wxverify.worker.om_archive_fill as om_archive_fill
import wxverify.worker.processor as processor_module
from wxverify.__main__ import main
from wxverify.collection.budget import (
    current_billing_day,
    next_billing_window,
    reserve_budget,
    set_source_cap,
)
from wxverify.collection.om_archive import MAX_DAY_AHEAD, served_run
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.migrations import create_schema
from wxverify.db.queue import Job, claim_next_job, enqueue_if_absent
from wxverify.db.switch import ensure_site_generation
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.om_archive_fill import (
    OM_ARCHIVE_JOB_KEY,
    UNREADABLE_UNITS_LIMIT,
    FillJobState,
    enqueue_om_archive,
    next_unit,
    quiet_end_local,
    run_om_archive_job,
)
from wxverify.worker.processor import dispatch, run_claimed_job
from wxverify.worker.round import set_scoring_round_local_time_sync

# ---------------------------------------------------------------------------
# Shared dates / clock.
# ---------------------------------------------------------------------------

_FIRST_OBSERVED = date(2026, 6, 18)
_S_FIRST = _FIRST_OBSERVED - timedelta(days=7)  # 2026-06-11: MAX_DAY_AHEAD
#: A made-up instant safely outside the fill's quiet window (22:30-02:10 at
#: the default round), so every test not itself about the quiet window gets
#: a deterministic, non-blocking clock.
_SAFE_NOW = datetime(2026, 6, 11, 10, 0, tzinfo=UTC)


def _z(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _freeze_fill_clock(
    monkeypatch: pytest.MonkeyPatch, *, now: datetime = _SAFE_NOW
) -> None:
    monkeypatch.setattr(om_archive_fill, "utc_now", lambda: now)


def _freeze_budget_clock(
    monkeypatch: pytest.MonkeyPatch, *, now: datetime = _SAFE_NOW
) -> None:
    monkeypatch.setattr(budget_module, "utc_now", lambda: now)


# ---------------------------------------------------------------------------
# Fixture construction (copied/adapted from tests/test_om_archive_fill.py;
# not imported, per the module's own isolation convention).
# ---------------------------------------------------------------------------


def _build_site(db_path: Path, *, lat: float = 0.0, lon: float = 0.0) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('om-archive-job-fixture', ?, ?, 0.0, 'UTC')",
        (lat, lon),
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    ensure_site_generation(conn, site_id)
    conn.commit()
    conn.close()
    return site_id


def _feed_id(db_path: Path, source: str, model: str) -> int:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None
    feed_id = int(row[0])
    conn.close()
    return feed_id


def _insert_site_days_row(db_path: Path, site_id: int, local_date: date) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    now = _z(start)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, zone_changeover, attempts, created_at,"
        " state_changed_at, done_at)"
        " VALUES (?, ?, 'closed', ?, ?, 'UTC', 0, 1, ?, ?, ?)",
        (site_id, local_date.isoformat(), _z(start), _z(end), now, now, now),
    )
    conn.commit()
    conn.close()


def _configure_site_for_fill(
    db_path: Path, site_id: int, feed_ids: dict[str, int], *, days: int
) -> None:
    """Disable every active Open-Meteo feed but ``feed_ids``, seed a
    CONTINUOUS run of hourly observations (all three ``FILL_VARIABLES``)
    from ``_FIRST_OBSERVED`` (2026-06-18) through every date any cell could
    need, and give the site a first record ``days`` download-days after
    S_first (2026-06-11) -- enough for ``_fill_site``/``next_unit`` to see a
    pending unit for each of ``feed_ids``. No forecast_history or ledger
    row is written, so every unit's first cell is empty (asks the archive).

    Deliberately NO observation before ``_FIRST_OBSERVED``: that date IS
    the site's earliest rain history (the real add-on's station data
    starts somewhere; S_first sits 7 days before it only so day_ahead 7 of
    the earliest unit lands exactly on it), so the earliest few download
    days' day_ahead 0-6 cells are correctly `no_hours`, not `written` --
    moving the anchor earlier would change what S_first itself resolves
    to (``first_download_date`` derives it FROM the earliest observation).
    """
    first_record = _S_FIRST + timedelta(days=days)
    last_needed_date = first_record + timedelta(days=MAX_DAY_AHEAD)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    other_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source = 'open-meteo'"
        ).fetchall()
        if int(row[0]) not in feed_ids.values()
    ]
    for other_id in other_ids:
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, other_id),
        )
    rows: list[tuple[int, str, str]] = []
    current = _FIRST_OBSERVED
    while current < last_needed_date:
        for hour in range(24):
            stamp = _z(
                datetime(current.year, current.month, current.day, hour, tzinfo=UTC)
            )
            for variable in ("precip", "temperature", "wind"):
                rows.append((site_id, variable, stamp))
        current += timedelta(days=1)
    conn.executemany(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, ?, ?, 0.0, 1)",
        rows,
    )
    conn.commit()
    conn.close()
    _insert_site_days_row(db_path, site_id, first_record)


def _advance_ledger_past(
    db_path: Path, site_id: int, feed_id: int, before: date
) -> None:
    """Pre-mark every cell of the days before ``before`` as handled
    (``present``, 0 hours) for ``feed_id``'s day_ahead-0 row of each
    ``FILL_VARIABLES`` variable, so ``next_unit`` skips straight to
    ``before`` as the feed's next download day. Lets a test target a unit
    whose full day_ahead 0-7 span is already inside the observed range,
    without faking an earlier first observation (which would change what
    S_first itself resolves to)."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    marker = before - timedelta(days=1)
    for variable in ("precip", "temperature", "wind"):
        conn.execute(
            "INSERT INTO om_archive_cells (site_id, feed_id, variable, day_ahead,"
            " local_date, download_date, outcome, hours, run_at, filled_at)"
            " VALUES (?, ?, ?, 0, ?, ?, 'present', 0, NULL, '2026-06-18T00:10:00Z')",
            (site_id, feed_id, variable, marker.isoformat(), marker.isoformat()),
        )
    conn.commit()
    conn.close()


def _build_job_fixture(
    tmp_path: Path,
    name: str,
    *,
    models: tuple[str, ...] = ("icon_eu",),
    days: int = 15,
    lat: float = 0.0,
    lon: float = 0.0,
) -> tuple[Path, int, dict[str, int]]:
    """A fresh database with a site whose only active Open-Meteo feeds are
    ``models``. No ``Database`` is left open."""
    db_path = tmp_path / name
    site_id = _build_site(db_path, lat=lat, lon=lon)
    db = init_db(str(db_path))
    close_db()
    del db
    feed_ids = {model: _feed_id(db_path, "open-meteo", model) for model in models}
    _configure_site_for_fill(db_path, site_id, feed_ids, days=days)
    return db_path, site_id, feed_ids


# ---------------------------------------------------------------------------
# Archive reply builders.
# ---------------------------------------------------------------------------


def _flat_response(run_at: datetime, hours: int) -> dict[str, object]:
    stamps = [run_at + timedelta(hours=i) for i in range(hours)]
    return {
        "latitude": 0.0,
        "longitude": 0.0,
        "hourly": {
            "time": [_z(s) for s in stamps],
            "temperature_2m": [10.0] * hours,
            "precipitation": [0.0] * hours,
            "wind_speed_10m": [5.0] * hours,
        },
    }


def _valid_reply(request: httpx.Request) -> httpx.Response:
    run_text = request.url.params["run"]
    run_at = datetime.strptime(run_text, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)
    hours = int(request.url.params["forecast_hours"])
    return httpx.Response(200, json=_flat_response(run_at, hours), request=request)


def _unreadable_reply(request: httpx.Request) -> httpx.Response:
    """A 200 whose body has no ``hourly`` field: a pydantic
    ``ValidationError``, caught as ``Retry`` with ``unreadable`` True."""
    return httpx.Response(
        200, json={"latitude": 0.0, "longitude": 0.0}, request=request
    )


def _status_reply(
    status: int, *, body: object = None
) -> Callable[[httpx.Request], httpx.Response]:
    def _reply(request: httpx.Request) -> httpx.Response:
        if body is None:
            return httpx.Response(status, request=request)
        return httpx.Response(status, json=body, request=request)

    return _reply


def _counted(
    calls: list[int], reply: Callable[[httpx.Request], httpx.Response]
) -> httpx.MockTransport:
    def _handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        return reply(request)

    return httpx.MockTransport(_handler)


def _sequenced(
    calls: list[int],
    default: Callable[[httpx.Request], httpx.Response],
    overrides: Mapping[int, Callable[[httpx.Request], httpx.Response]],
) -> httpx.MockTransport:
    """The Nth request (1-based) gets ``overrides[N]``, else ``default``."""

    def _handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        return overrides.get(calls[0], default)(request)

    return httpx.MockTransport(_handler)


def _r1_fails_older_serves(r1: datetime) -> Callable[[httpx.Request], httpx.Response]:
    def _reply(request: httpx.Request) -> httpx.Response:
        run_text = request.url.params["run"]
        run_at = datetime.strptime(run_text, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)
        if run_at == r1:
            return httpx.Response(400, json={"reason": "no run"}, request=request)
        return _valid_reply(request)

    return _reply


def _by_model(
    behaviors: Mapping[str, Callable[[httpx.Request], httpx.Response]],
    calls: list[int],
) -> httpx.MockTransport:
    def _handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        model = request.url.params["models"]
        return behaviors[model](request)

    return httpx.MockTransport(_handler)


# ---------------------------------------------------------------------------
# Job claim / drive helpers.
# ---------------------------------------------------------------------------


async def _claim_fresh_job(db: Database) -> Job:
    await db.write(enqueue_om_archive)
    job = await db.write(claim_next_job)
    assert job is not None, "fixture bug: nothing was enqueued/claimable"
    return job


async def _drive_until_done(
    db: Database,
    writer: FencedWriter,
    job: Job,
    client: httpx.AsyncClient,
    *,
    max_iterations: int = 150,
) -> None:
    """Call ``run_om_archive_job`` until it returns normally (no unit left),
    swallowing every ``JobDeferred``; a ``JobCancelled`` propagates."""
    for _ in range(max_iterations):
        try:
            await run_om_archive_job(db, writer, job, client=client)
            return
        except JobDeferred:
            continue
    raise AssertionError("job did not finish within max_iterations")


# ---------------------------------------------------------------------------
# T9 -- quiet window.
# ---------------------------------------------------------------------------


def test_t9_quiet_window_default_round_defers_at_2300_to_0210(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches a quiet-window bound shifted or dropped: at the default
    00:10 round, 23:00 site-local is inside the window, and the job defers
    to 02:10 the next day with no request made."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-quiet-default.db")
    _freeze_fill_clock(monkeypatch, now=datetime(2026, 6, 11, 23, 0, tzinfo=UTC))
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    expected = isoformat_utc(datetime(2026, 6, 12, 2, 10, tzinfo=UTC))
    assert exc.next_attempt_at == expected, (
        f"M9a -> at this assertion: correct = {expected!r},"
        f" mutant = {exc.next_attempt_at!r}"
    )
    assert calls[0] == 0, f"M9b -> at this assertion: correct = 0, mutant = {calls[0]}"


def test_t9_quiet_window_default_round_runs_outside_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the test above: at 10:00 site-local (well outside
    22:30-02:10), the job does NOT defer for the quiet window and makes at
    least one request -- so the zero-request result above is not merely
    "the fill never asks anything"."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-quiet-outside.db")
    _freeze_fill_clock(monkeypatch)  # _SAFE_NOW, 10:00
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)

        async def _run() -> str:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value.next_attempt_at

        next_attempt_at = asyncio.run(_run())
    finally:
        close_db()

    quiet_end = isoformat_utc(datetime(2026, 6, 12, 2, 10, tzinfo=UTC))
    assert next_attempt_at != quiet_end, (
        "M9c -> at this assertion: correct = deferred to the unit's own pace,"
        f" not the quiet end ({quiet_end!r}); mutant = deferred to the quiet end"
    )
    assert calls[0] >= 1, (
        f"M9d -> at this assertion: correct = >=1, mutant = {calls[0]}"
    )


def test_t9_quiet_window_custom_round_defers_at_0430_to_0500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With the round moved to 03:00, the quiet window moves with it: it
    defers at 04:30 (inside) to 05:00, the SAME calendar day (the window
    starts the evening before)."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-quiet-0300-in.db")
    _freeze_fill_clock(monkeypatch, now=datetime(2026, 6, 11, 4, 30, tzinfo=UTC))
    db = init_db(str(db_path))
    try:
        asyncio.run(
            db.write(lambda conn: set_scoring_round_local_time_sync(conn, "03:00"))
        )
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    assert quiet_end_local(time(3, 0)) == time(5, 0), (
        "fixture bug: quiet_end_local(03:00) must be 05:00"
    )
    expected = isoformat_utc(datetime(2026, 6, 11, 5, 0, tzinfo=UTC))
    assert exc.next_attempt_at == expected, (
        f"M9e -> at this assertion: correct = {expected!r},"
        f" mutant = {exc.next_attempt_at!r}"
    )
    assert calls[0] == 0


def test_t9_quiet_window_custom_round_runs_at_0501(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive: at 05:01 (one minute past the 03:00 round's quiet
    end), the job is no longer in the quiet window and makes a request."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-quiet-0300-out.db")
    _freeze_fill_clock(monkeypatch, now=datetime(2026, 6, 11, 5, 1, tzinfo=UTC))
    db = init_db(str(db_path))
    try:
        asyncio.run(
            db.write(lambda conn: set_scoring_round_local_time_sync(conn, "03:00"))
        )
        writer = FencedWriter(db)

        async def _run() -> str:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value.next_attempt_at

        next_attempt_at = asyncio.run(_run())
    finally:
        close_db()

    quiet_end = isoformat_utc(datetime(2026, 6, 11, 5, 0, tzinfo=UTC))
    assert next_attempt_at != quiet_end, (
        f"M9f -> at this assertion: correct = not deferred to {quiet_end!r},"
        f" mutant = {next_attempt_at!r}"
    )
    assert calls[0] >= 1


# ---------------------------------------------------------------------------
# T9 -- ceiling.
# ---------------------------------------------------------------------------


def test_t9_ceiling_default_3000_defers_to_next_billing_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """At 3,000 counted calls already used today, the fill's own ceiling
    (min(3000, 10000-300)) is reached, so it defers to the next billing
    window and makes no request."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-ceiling-3000.db")
    _freeze_fill_clock(monkeypatch)
    _freeze_budget_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        billing_day = current_billing_day("UTC")
        asyncio.run(
            db.write(
                lambda conn: conn.execute(
                    "INSERT INTO api_budget (source, billing_day, calls)"
                    " VALUES ('open-meteo', ?, 3000)"
                    " ON CONFLICT(source, billing_day) DO UPDATE SET calls=3000",
                    (billing_day,),
                )
            )
        )
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    expected = next_billing_window("UTC")
    assert exc.next_attempt_at == expected, (
        f"M9g -> at this assertion: correct = {expected!r},"
        f" mutant = {exc.next_attempt_at!r}"
    )
    assert calls[0] == 0, f"M9h -> at this assertion: correct = 0, mutant = {calls[0]}"


def test_t9_ceiling_with_lower_daily_limit_defers_at_700(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``daily_call_limit`` set to 1,000, the ceiling is
    ``1000 - FILL_CALL_RESERVE`` = 700; at 700 used it defers, no request."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-ceiling-700.db")
    _freeze_fill_clock(monkeypatch)
    _freeze_budget_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        billing_day = current_billing_day("UTC")
        asyncio.run(
            db.write(
                lambda conn: set_source_cap(conn, "open-meteo", daily_call_limit=1000)
            )
        )
        asyncio.run(
            db.write(
                lambda conn: conn.execute(
                    "INSERT INTO api_budget (source, billing_day, calls)"
                    " VALUES ('open-meteo', ?, 700)"
                    " ON CONFLICT(source, billing_day) DO UPDATE SET calls=700",
                    (billing_day,),
                )
            )
        )
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    expected = next_billing_window("UTC")
    assert exc.next_attempt_at == expected, (
        f"M9i -> at this assertion: correct = {expected!r},"
        f" mutant = {exc.next_attempt_at!r}"
    )
    assert calls[0] == 0, f"M9j -> at this assertion: correct = 0, mutant = {calls[0]}"


def test_t9_ceiling_with_limit_300_makes_no_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``daily_call_limit`` 300, the ceiling is
    ``min(3000, 300-300)`` = 0: the fill can make no call at all, even with
    0 used today."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-ceiling-0.db")
    _freeze_fill_clock(monkeypatch)
    _freeze_budget_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        asyncio.run(
            db.write(
                lambda conn: set_source_cap(conn, "open-meteo", daily_call_limit=300)
            )
        )
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    expected = next_billing_window("UTC")
    assert exc.next_attempt_at == expected
    assert calls[0] == 0, f"M9k -> at this assertion: correct = 0, mutant = {calls[0]}"


# ---------------------------------------------------------------------------
# T9 -- 429 / domain backoff / feed error state / budget reservability.
# ---------------------------------------------------------------------------


def test_t9_429_defers_and_leaves_backoff_and_feed_state_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 429 defers the unit (``RETRY_WAIT``), and the fill never calls
    ``record_http_backoff`` or touches a feed's error columns: a 429
    through the fill leaves ``domain_backoffs`` empty and the feed's
    ``site_feed_state`` error columns at their seeded baseline, and the
    add-on's own next Open-Meteo reservation still succeeds."""
    calls = [0]
    db_path, site_id, feed_ids = _build_job_fixture(tmp_path, "t9-429.db")
    feed_id = feed_ids["icon_eu"]
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        asyncio.run(
            db.write(
                lambda conn: conn.execute(
                    "INSERT INTO site_feed_state"
                    " (site_id, feed_id, enabled, last_run_at, last_error, error_count)"
                    " VALUES (?, ?, 1, NULL, NULL, 0)",
                    (site_id, feed_id),
                )
            )
        )
        writer = FencedWriter(db)

        async def _run() -> JobDeferred:
            job = await _claim_fresh_job(db)
            reply = _status_reply(429)
            async with httpx.AsyncClient(transport=_counted(calls, reply)) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())

        async def _reserve() -> None:
            await db.write(lambda conn: reserve_budget(conn, "open-meteo", 1))

        asyncio.run(_reserve())  # must not raise JobDeferred
    finally:
        close_db()

    assert calls[0] == 1, f"M9l -> at this assertion: correct = 1, mutant = {calls[0]}"
    wait_minutes = (
        datetime.fromisoformat(exc.next_attempt_at.replace("Z", "+00:00")) - _SAFE_NOW
    ).total_seconds() / 60
    assert 14 <= wait_minutes <= 16, (
        f"M9m -> at this assertion: correct = about 15 minutes,"
        f" mutant = {wait_minutes!r}"
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    backoff_count = conn.execute(
        "SELECT COUNT(*) FROM domain_backoffs WHERE domain='api.open-meteo.com'"
    ).fetchone()[0]
    assert backoff_count == 0, (
        f"M9n -> at this assertion: correct = 0, mutant = {backoff_count}"
    )
    state_row = conn.execute(
        "SELECT last_run_at, last_error, error_count FROM site_feed_state"
        " WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert tuple(state_row) == (None, None, 0), (
        "M9o -> at this assertion: correct = (None, None, 0),"
        f" mutant = {tuple(state_row)!r}"
    )
    conn.close()


def test_t9_success_does_not_clear_an_existing_domain_backoff_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired case: with an existing (already-expired) ``domain_backoffs``
    row before the run, a successful unit does not clear it either -- the
    fill never calls ``clear_domain_backoff``."""
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-backoff-stays.db")
    _freeze_fill_clock(monkeypatch)
    seeded_next_attempt = isoformat_utc(_SAFE_NOW - timedelta(minutes=5))
    db = init_db(str(db_path))
    try:
        asyncio.run(
            db.write(
                lambda conn: conn.execute(
                    "INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)"
                    " VALUES ('api.open-meteo.com', ?, 2)",
                    (seeded_next_attempt,),
                )
            )
        )
        writer = FencedWriter(db)

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(_valid_reply)
            ) as client:
                with pytest.raises(JobDeferred):
                    await run_om_archive_job(db, writer, job, client=client)

        asyncio.run(_run())
    finally:
        close_db()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT next_attempt_at, retry_count FROM domain_backoffs"
        " WHERE domain='api.open-meteo.com'"
    ).fetchone()
    conn.close()
    assert row is not None, (
        "M9p -> at this assertion: correct = the backoff row still present,"
        " mutant = it was cleared"
    )
    assert (row["next_attempt_at"], row["retry_count"]) == (seeded_next_attempt, 2), (
        f"M9q -> at this assertion: correct = {(seeded_next_attempt, 2)!r},"
        f" mutant = {(row['next_attempt_at'], row['retry_count'])!r}"
    )


# ---------------------------------------------------------------------------
# T9 -- 400 (no run) and the R1-400-older-serves case.
# ---------------------------------------------------------------------------


def test_t9_400_is_no_run_and_the_warning_masks_the_reason_not_the_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Every candidate run answers 400 with a reason echoing the request's
    own latitude value (0.0): the WARNING carries the status and the
    MASKED reason, never the raw value and never a URL."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-400.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        reply = _status_reply(400, body={"error": {"message": "invalid near 0.0"}})

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(transport=_counted(calls, reply)) as client:
                with pytest.raises(JobDeferred), caplog.at_level(logging.WARNING):
                    await run_om_archive_job(db, writer, job, client=client)

        asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] >= 1, "fixture bug: at least one request must be made"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    no_run_lines = [line for line in warnings if "om archive no run" in line]
    assert no_run_lines, (
        "M9r -> at this assertion: correct = at least one 'no run' WARNING,"
        f" mutant = none found (records={warnings!r})"
    )
    joined = " ".join(no_run_lines)
    assert "status=400" in joined, (
        f"M9s -> at this assertion: correct = 'status=400' present, mutant = {joined!r}"
    )
    assert "invalid near 0.0" not in joined, (
        "M9t -> at this assertion: correct = the raw reason text absent"
        f" (masked), mutant = present verbatim ({joined!r})"
    )
    assert "***" in joined, (
        f"M9u -> at this assertion: correct = a masked '***' token present,"
        f" mutant = {joined!r}"
    )
    assert "http" not in joined.lower(), (
        f"M9v -> at this assertion: correct = no URL in the WARNING,"
        f" mutant = {joined!r}"
    )


def test_t9_r1_400_but_older_run_serves_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R1 (the served run) answers 400, but an older run within
    ``OLDER_RUNS_REACH`` serves valid data: the unit still ends up
    ``written``, not ``no_run``.

    Targets the unit at S = ``_FIRST_OBSERVED`` (2026-06-18), not S_first
    (2026-06-11): day_ahead 0-7 of S_first land BEFORE the site's first
    observation (by construction -- S_first sits exactly 7 days earlier),
    so that unit has no observed hour to write at all. ``_advance_ledger_past``
    pre-marks every earlier day as handled so ``next_unit`` picks S =
    ``_FIRST_OBSERVED``, whose full day_ahead 0-7 span IS observed.
    """
    db_path, site_id, feed_ids = _build_job_fixture(tmp_path, "t9-r1-400-older.db")
    feed_id = feed_ids["icon_eu"]
    _advance_ledger_past(db_path, site_id, feed_id, _FIRST_OBSERVED)
    _freeze_fill_clock(monkeypatch)
    fetch_at = datetime(
        _FIRST_OBSERVED.year,
        _FIRST_OBSERVED.month,
        _FIRST_OBSERVED.day,
        0,
        10,
        tzinfo=UTC,
    )  # default round 00:10
    r1 = served_run("icon_eu", fetch_at)
    calls = [0]
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        reply = _r1_fails_older_serves(r1)

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(transport=_counted(calls, reply)) as client:
                with pytest.raises(JobDeferred):
                    await run_om_archive_job(db, writer, job, client=client)

        asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] >= 2, (
        "M9w -> at this assertion: correct = >=2 (R1 fails, an older run"
        f" serves), mutant = {calls[0]}"
    )
    conn = sqlite3.connect(db_path)
    written = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells"
        " WHERE site_id=? AND feed_id=? AND outcome='written'",
        (site_id, feed_id),
    ).fetchone()[0]
    no_run = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells"
        " WHERE site_id=? AND feed_id=? AND outcome='no_run'",
        (site_id, feed_id),
    ).fetchone()[0]
    conn.close()
    assert written > 0, (
        f"M9x -> at this assertion: correct = >0 written cells, mutant = {written}"
    )
    assert no_run == 0, (
        f"M9y -> at this assertion: correct = 0 no_run cells, mutant = {no_run}"
    )


# ---------------------------------------------------------------------------
# T9 -- refusal / exclusion.
# ---------------------------------------------------------------------------


def test_t9_403_excludes_the_model_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A single 403 excludes the model immediately -- no 3-in-a-row streak
    needed: every remaining cell of the feed gets a ``refused`` ledger row
    in the SAME run, so a later ``next_unit`` finds nothing left for it."""
    calls = [0]
    db_path, site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-403.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        reply = _status_reply(403)

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(transport=_counted(calls, reply)) as client:
                with pytest.raises(JobDeferred), caplog.at_level(logging.ERROR):
                    await run_om_archive_job(db, writer, job, client=client)

        asyncio.run(_run())

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        remaining = next_unit(conn, site_id)
        conn.close()
    finally:
        close_db()

    assert calls[0] == 1, (
        "M9z -> at this assertion: correct = 1 (excluded at once, no retry"
        f" chain), mutant = {calls[0]}"
    )
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    stopped_lines = [line for line in errors if "om archive fill stopped" in line]
    assert len(stopped_lines) == 1, (
        f"M9aa -> at this assertion: correct = exactly 1 ERROR line,"
        f" mutant = {len(stopped_lines)} ({stopped_lines!r})"
    )
    assert "model=icon_eu" in stopped_lines[0] and "status 403" in stopped_lines[0], (
        f"M9ab -> at this assertion: correct = names icon_eu and status 403,"
        f" mutant = {stopped_lines[0]!r}"
    )
    assert remaining is None, (
        f"M9ac -> at this assertion: correct = None (every cell accounted"
        f" for), mutant = {remaining!r}"
    )


def test_t9_three_refused_units_in_a_row_excludes_model_a_while_b_keeps_serving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Model A (icon_eu) gets an HTTP 400 for every candidate run, every
    unit, in a row; model B (gfs_global) always serves. A's remaining cells
    end up ``refused``, exactly one ERROR line names A, and B ends up
    fully served -- no ``refused`` cell at all."""
    calls = [0]
    # Use the default `days` window (15): it must reach past `_FIRST_OBSERVED`
    # so B has observed pre-close days to fill, or every B cell stays unwritten.
    db_path, site_id, feed_ids = _build_job_fixture(
        tmp_path, "t9-3-refused.db", models=("icon_eu", "gfs_global")
    )
    feed_a, feed_b = feed_ids["icon_eu"], feed_ids["gfs_global"]
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        behaviors: dict[str, Callable[[httpx.Request], httpx.Response]] = {
            "icon_eu": _status_reply(400, body={"reason": "no run"}),
            "gfs_global": _valid_reply,
        }
        transport = _by_model(behaviors, calls)

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            async with httpx.AsyncClient(transport=transport) as client:
                with caplog.at_level(logging.ERROR):
                    await _drive_until_done(db, writer, job, client)

        asyncio.run(_run())
    finally:
        close_db()

    conn = sqlite3.connect(db_path)
    a_refused = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells WHERE site_id=? AND feed_id=?"
        " AND outcome='refused'",
        (site_id, feed_a),
    ).fetchone()[0]
    b_refused = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells WHERE site_id=? AND feed_id=?"
        " AND outcome='refused'",
        (site_id, feed_b),
    ).fetchone()[0]
    b_written = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells WHERE site_id=? AND feed_id=?"
        " AND outcome='written'",
        (site_id, feed_b),
    ).fetchone()[0]
    conn.close()

    assert a_refused > 0, (
        f"M9ad -> at this assertion: correct = >0 refused cells for A,"
        f" mutant = {a_refused}"
    )
    assert b_refused == 0, (
        f"M9ae -> at this assertion: correct = 0 refused cells for B,"
        f" mutant = {b_refused}"
    )
    assert b_written > 0, (
        f"M9af -> at this assertion: correct = >0 written cells for B,"
        f" mutant = {b_written}"
    )
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    stopped = [line for line in errors if "om archive fill stopped" in line]
    assert len(stopped) == 1, (
        f"M9ag -> at this assertion: correct = exactly 1 ERROR line,"
        f" mutant = {len(stopped)} ({stopped!r})"
    )
    assert "model=icon_eu" in stopped[0], (
        f"M9ah -> at this assertion: correct = names icon_eu, mutant = {stopped[0]!r}"
    )
    assert "model=gfs_global" not in stopped[0]


# ---------------------------------------------------------------------------
# T9 -- claim order.
# ---------------------------------------------------------------------------


def test_t9_a_due_main_lane_job_claims_before_a_young_fill_row(
    tmp_path: Path,
) -> None:
    """A fill row younger than ``CHAIN_PROMOTION_AGE`` sits in tier 2: a due
    ``fetch_feed`` row (not a chain type, always tier 1) is claimed first;
    with nothing else due, the fill row is claimed next."""
    db_path, site_id, feed_ids = _build_job_fixture(tmp_path, "t9-claim-order.db")
    feed_id = feed_ids["icon_eu"]
    db = init_db(str(db_path))
    try:

        async def _run() -> tuple[Job, Job]:
            await db.write(enqueue_om_archive)
            await db.write(
                lambda conn: enqueue_if_absent(
                    conn,
                    "fetch_feed",
                    site_id,
                    f"fetch:{feed_id}",
                    {"feed_id": feed_id},
                )
            )
            first = await db.write(claim_next_job)
            second = await db.write(claim_next_job)
            assert first is not None and second is not None
            return first, second

        first, second = asyncio.run(_run())
    finally:
        close_db()

    assert first.type == "fetch_feed", (
        f"M9ai -> at this assertion: correct = 'fetch_feed', mutant = {first.type!r}"
    )
    assert second.type == "backfill_site", (
        f"M9aj -> at this assertion: correct = 'backfill_site',"
        f" mutant = {second.type!r}"
    )


def test_t9_an_aged_fill_row_promotes_to_tier_one(tmp_path: Path) -> None:
    """A fill row left unclaimed past ``CHAIN_PROMOTION_AGE`` (5 minutes)
    promotes to tier 1 and is claimed ahead of a YOUNG fill row of the same
    type, at a different site."""
    db_path, aged_site_id, feed_ids = _build_job_fixture(tmp_path, "t9-promote-aged.db")
    young_site_id = _build_site(db_path)
    _configure_site_for_fill(db_path, young_site_id, feed_ids, days=15)
    db = init_db(str(db_path))
    try:

        async def _run() -> Job:
            await db.write(enqueue_om_archive)  # queues for every site with work
            aged_cutoff = isoformat_utc(datetime.now(tz=UTC) - timedelta(minutes=10))

            def _age(conn: sqlite3.Connection) -> None:
                conn.execute(
                    "UPDATE jobs SET next_attempt_at=?"
                    " WHERE site_id=? AND type='backfill_site'",
                    (aged_cutoff, aged_site_id),
                )

            await db.write(_age)
            claimed = await db.write(claim_next_job)
            assert claimed is not None
            return claimed

        claimed = asyncio.run(_run())
    finally:
        close_db()

    assert claimed.site_id == aged_site_id, (
        f"M9ak -> at this assertion: correct = {aged_site_id},"
        f" mutant = {claimed.site_id!r}"
    )


# ---------------------------------------------------------------------------
# T9 -- unreadable replies.
# ---------------------------------------------------------------------------


def test_t9_five_unreadable_in_a_row_cancels_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """``UNREADABLE_UNITS_LIMIT`` units in a row deferred by a 200 reply
    that does not read end the job with ``JobCancelled`` and one ERROR
    line; the runs before it do not raise."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-unreadable-5.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)

        async def _run() -> list[BaseException]:
            job = await _claim_fresh_job(db)
            raised: list[BaseException] = []
            async with httpx.AsyncClient(
                transport=_counted(calls, _unreadable_reply)
            ) as client:
                for _ in range(UNREADABLE_UNITS_LIMIT):
                    try:
                        await run_om_archive_job(db, writer, job, client=client)
                    except (JobDeferred, JobCancelled) as exc:
                        raised.append(exc)
            return raised

        with caplog.at_level(logging.ERROR):
            raised = asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] == UNREADABLE_UNITS_LIMIT, (
        f"M9al -> at this assertion: correct = {UNREADABLE_UNITS_LIMIT},"
        f" mutant = {calls[0]}"
    )
    assert [type(exc) for exc in raised[:-1]] == [JobDeferred] * (
        UNREADABLE_UNITS_LIMIT - 1
    ), (
        f"M9am -> at this assertion: correct = {UNREADABLE_UNITS_LIMIT - 1}"
        f" JobDeferred, mutant = {[type(e).__name__ for e in raised[:-1]]!r}"
    )
    assert isinstance(raised[-1], JobCancelled), (
        f"M9an -> at this assertion: correct = JobCancelled on the"
        f" {UNREADABLE_UNITS_LIMIT}th run, mutant = {type(raised[-1]).__name__}"
    )
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    paused = [line for line in errors if "om archive fill paused" in line]
    assert len(paused) == 1, (
        f"M9ao -> at this assertion: correct = exactly 1 ERROR line,"
        f" mutant = {len(paused)} ({paused!r})"
    )
    assert f"{UNREADABLE_UNITS_LIMIT} unreadable replies in a row" in paused[0]


def test_t9_four_unreadable_a_done_unit_then_four_more_do_not_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A unit done between two runs of unreadable replies resets the
    count: 4 unreadable, then a unit that finds a run, then 4 more
    unreadable -- 9 runs total, none of them cancels the job."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(
        tmp_path, "t9-unreadable-4-1-4.db"
    )
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        transport = _sequenced(calls, _unreadable_reply, {5: _valid_reply})

        async def _run() -> list[JobDeferred]:
            job = await _claim_fresh_job(db)
            deferrals: list[JobDeferred] = []
            async with httpx.AsyncClient(transport=transport) as client:
                for _ in range(9):
                    try:
                        await run_om_archive_job(db, writer, job, client=client)
                    except JobDeferred as exc:
                        deferrals.append(exc)
            return deferrals

        deferrals = asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] == 9, f"M9ap -> at this assertion: correct = 9, mutant = {calls[0]}"
    assert len(deferrals) == 9, (
        "M9aq -> at this assertion: correct = 9 JobDeferred and 0 JobCancelled,"
        f" mutant = {len(deferrals)} JobDeferred"
    )


def test_t9_a_429_between_unreadable_replies_neither_adds_to_nor_resets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two unreadable, a 429, two more unreadable: the 429 does not count
    towards the unreadable streak (it leaves it at 4, not 5 and not reset
    to 0), so the 6th run (unreadable again) is the one that reaches 5 and
    cancels."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-unreadable-429.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)
        transport = _sequenced(calls, _unreadable_reply, {3: _status_reply(429)})

        async def _run() -> list[BaseException]:
            job = await _claim_fresh_job(db)
            raised: list[BaseException] = []
            async with httpx.AsyncClient(transport=transport) as client:
                for _ in range(6):
                    try:
                        await run_om_archive_job(db, writer, job, client=client)
                    except (JobDeferred, JobCancelled) as exc:
                        raised.append(exc)
            return raised

        raised = asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] == 6
    assert [type(exc) for exc in raised[:5]] == [JobDeferred] * 5, (
        "M9ar -> at this assertion: correct = the 429 run (3rd) is a plain"
        " JobDeferred too, not a JobCancelled,"
        f" mutant = {[type(e).__name__ for e in raised[:5]]!r}"
    )
    assert isinstance(raised[5], JobCancelled), (
        "M9as -> at this assertion: correct = the 6th run (the 5th"
        " UNREADABLE one, since the 429 did not count) cancels,"
        f" mutant = {type(raised[5]).__name__}"
    )


def test_t9_malformed_job_payload_reads_as_zero_pure_function() -> None:
    """``FillJobState.from_payload`` on garbage, non-dict JSON, or no text
    at all: always zero state, never a crash and never a phantom streak."""
    zero = FillJobState()
    assert FillJobState.from_payload("{not json") == zero, (
        "M9at -> at this assertion: correct = the zero state, mutant = something else"
    )
    assert FillJobState.from_payload("[1, 2, 3]") == zero, (
        "M9au -> at this assertion: correct = the zero state (not a dict),"
        " mutant = something else"
    )
    assert FillJobState.from_payload(None) == zero
    assert FillJobState.from_payload(12345) == zero


def test_t9_a_hand_corrupted_job_payload_does_not_pre_cancel_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Integration-level pairing: with the claimed job row's own ``payload``
    column hand-corrupted to unreadable text, the job still treats the
    unreadable-streak as starting from 0 -- it does NOT immediately cancel
    as if several unreadable replies had already happened."""
    calls = [0]
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-corrupt-payload.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)

        async def _run() -> BaseException:
            job = await _claim_fresh_job(db)
            await db.write(
                lambda conn: conn.execute(
                    "UPDATE jobs SET payload = ? WHERE id = ?", ("{not json", job.id)
                )
            )
            async with httpx.AsyncClient(
                transport=_counted(calls, _unreadable_reply)
            ) as client:
                with pytest.raises(JobDeferred) as excinfo:
                    await run_om_archive_job(db, writer, job, client=client)
                return excinfo.value

        exc = asyncio.run(_run())
    finally:
        close_db()

    assert isinstance(exc, JobDeferred), (
        "M9av -> at this assertion: correct = JobDeferred (streak starts"
        " at 0, so one unreadable reply never cancels),"
        f" mutant = {type(exc).__name__}"
    )


# ---------------------------------------------------------------------------
# T9 -- ending early.
# ---------------------------------------------------------------------------


def test_t9_a_disabled_site_ends_the_job_early_with_no_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A site disabled between claiming the job and running it ends the
    job (``JobCancelled``) with one WARNING line and no request made."""
    calls = [0]
    db_path, site_id, _feed_ids = _build_job_fixture(tmp_path, "t9-disabled-site.db")
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        writer = FencedWriter(db)

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            await db.write(
                lambda conn: conn.execute(
                    "UPDATE sites SET enabled = 0 WHERE id = ?", (site_id,)
                )
            )
            async with httpx.AsyncClient(
                transport=_counted(calls, _valid_reply)
            ) as client:
                with pytest.raises(JobCancelled), caplog.at_level(logging.WARNING):
                    await run_om_archive_job(db, writer, job, client=client)

        asyncio.run(_run())
    finally:
        close_db()

    assert calls[0] == 0, f"M9aw -> at this assertion: correct = 0, mutant = {calls[0]}"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    ended = [line for line in warnings if "om archive fill ended early" in line]
    assert len(ended) == 1, (
        f"M9ax -> at this assertion: correct = exactly 1 WARNING line,"
        f" mutant = {len(ended)} ({ended!r})"
    )
    assert f"site={site_id}" in ended[0] and "gone or disabled" in ended[0], (
        f"M9ay -> at this assertion: correct = names the site and the"
        f" reason, mutant = {ended[0]!r}"
    )


# ---------------------------------------------------------------------------
# T10 -- wiring.
# ---------------------------------------------------------------------------


def test_t10_backfill_site_keyed_om_archive_reaches_the_fill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``dispatch`` routes a ``backfill_site``/``om-archive`` job to
    ``run_om_archive_job`` -- proved by patching the NAME ``dispatch``
    actually calls (bound in ``processor``'s own namespace at import time),
    never the one in ``om_archive_fill``."""
    db_path, _site_id, _feed_ids = _build_job_fixture(
        tmp_path, "t10-dispatch-reaches.db"
    )
    recorded: list[int] = []

    async def _stub(db: Database, writer: FencedWriter, job: Job) -> None:
        del db, writer
        recorded.append(job.id)

    monkeypatch.setattr(processor_module, "run_om_archive_job", _stub)
    db = init_db(str(db_path))
    try:

        async def _run() -> None:
            job = await _claim_fresh_job(db)
            writer = FencedWriter(db)
            await dispatch(db, writer, job)

        asyncio.run(_run())
    finally:
        close_db()

    assert recorded, (
        "M10a -> at this assertion: correct = run_om_archive_job called once,"
        " mutant = never called"
    )


def test_t10_backfill_site_with_another_key_still_raises(tmp_path: Path) -> None:
    """A ``backfill_site`` job keyed anything but ``om-archive`` still ends
    at the retired type's raise -- the branch for the fill is keyed, not
    type-only."""
    db_path, _site_id, _feed_ids = _build_job_fixture(tmp_path, "t10-other-key.db")
    other_key_job = Job(
        id=1,
        type="backfill_site",
        site_id=1,
        job_key="some-other-key",
        payload={},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    assert OM_ARCHIVE_JOB_KEY != "some-other-key", "fixture bug: keys must differ"
    db = init_db(str(db_path))
    try:

        async def _run() -> None:
            writer = FencedWriter(db)
            with pytest.raises(RuntimeError, match="unknown job type backfill_site"):
                await dispatch(db, writer, other_key_job)

        asyncio.run(_run())
    finally:
        close_db()


def test_t10_enqueue_om_archive_queues_once_while_work_remains(
    tmp_path: Path,
) -> None:
    """``enqueue_om_archive`` queues exactly one ``backfill_site`` job,
    keyed ``om-archive``, per site with a unit left; a second call dedupes
    to 0 (the row from the first call is still pending)."""
    db_path, site_id, _feed_ids = _build_job_fixture(tmp_path, "t10-enqueue-once.db")
    db = init_db(str(db_path))
    try:

        async def _run() -> tuple[int, int]:
            first = await db.write(enqueue_om_archive)
            second = await db.write(enqueue_om_archive)
            return first, second

        first, second = asyncio.run(_run())
    finally:
        close_db()

    assert first == 1, f"M10b -> at this assertion: correct = 1, mutant = {first}"
    assert second == 0, (
        f"M10c -> at this assertion: correct = 0 (deduped), mutant = {second}"
    )
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT type, job_key, site_id FROM jobs WHERE site_id = ?", (site_id,)
    ).fetchone()
    conn.close()
    assert row == ("backfill_site", "om-archive", site_id), (
        f"M10d -> at this assertion: correct = ('backfill_site', 'om-archive',"
        f" {site_id}), mutant = {row!r}"
    )


def test_t10_enqueue_om_archive_queues_nothing_once_every_s_last_row_exists(
    tmp_path: Path,
) -> None:
    """Once every (feed, variable)'s ``day_ahead = 0`` ledger row sits at
    S_last, the site has no unit left: ``enqueue_om_archive`` queues
    nothing for it."""
    db_path, site_id, feed_ids = _build_job_fixture(
        tmp_path, "t10-enqueue-exhausted.db"
    )
    feed_id = feed_ids["icon_eu"]
    s_last = _S_FIRST + timedelta(days=14)  # first_record - 1 (days=15 window)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    for variable in ("precip", "temperature", "wind"):
        conn.execute(
            "INSERT INTO om_archive_cells (site_id, feed_id, variable, day_ahead,"
            " local_date, download_date, outcome, hours, run_at, filled_at)"
            " VALUES (?, ?, ?, 0, ?, ?, 'present', 0, NULL, '2026-06-18T00:10:00Z')",
            (site_id, feed_id, variable, s_last.isoformat(), s_last.isoformat()),
        )
    conn.commit()
    conn.close()
    db = init_db(str(db_path))
    try:
        queued = asyncio.run(db.write(enqueue_om_archive))
    finally:
        close_db()

    assert queued == 0, f"M10e -> at this assertion: correct = 0, mutant = {queued}"
    conn = sqlite3.connect(db_path)
    jobs_count = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE site_id = ? AND type = 'backfill_site'",
        (site_id,),
    ).fetchone()[0]
    conn.close()
    assert jobs_count == 0, (
        f"M10f -> at this assertion: correct = 0, mutant = {jobs_count}"
    )


# ---------------------------------------------------------------------------
# T13 -- the job path: no coordinate leak via jobs.last_error, job-run log
# lines, and the ``om-archive fill`` CLI.
# ---------------------------------------------------------------------------

_LAT = 12.3456
_LON = 65.4321
_GRID_LAT = "12.35"
_GRID_LON = "65.43"
_LEAK_STRINGS = (str(_LAT), str(_LON), _GRID_LAT, _GRID_LON)


def _assert_no_leak(*texts: str) -> None:
    for text in texts:
        for leak in _LEAK_STRINGS:
            assert leak not in text, (leak, text)


@pytest.fixture
def quiet_httpx_wire() -> Iterator[None]:
    """Set the ``httpx``/``httpcore`` loggers to WARNING, as production's
    ``__main__._configure_logging`` does outside DEBUG -- otherwise httpx's
    own per-request INFO line carries the coordinates into caplog. Copied
    from ``tests/test_om_archive_fetch_run.py``'s fixture of the same name."""
    loggers = [logging.getLogger("httpx"), logging.getLogger("httpcore")]
    previous = [wire_logger.level for wire_logger in loggers]
    for wire_logger in loggers:
        wire_logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        for wire_logger, level in zip(loggers, previous, strict=True):
            wire_logger.setLevel(level)


def test_positive_control_sanitized_exception_does_not_mask_a_plain_number() -> None:
    """Proves the no-leak checks below are not vacuous: if a raw exception
    carrying the latitude ever escaped to ``jobs.last_error`` via
    ``sanitized_exception`` (which only redacts URLs, not bare numbers),
    the leak WOULD show -- so the absence asserted below is meaningful."""
    exc = ValueError(f"bad coordinate {_LAT}")
    text = sanitized_exception(exc)
    assert str(_LAT) in text, (
        "fixture bug: sanitized_exception must not mask a plain number,"
        " or the no-leak checks below would be vacuous"
    )


def _build_leak_job_fixture(tmp_path: Path, name: str) -> tuple[Path, int, int]:
    db_path, site_id, feed_ids = _build_job_fixture(tmp_path, name, lat=_LAT, lon=_LON)
    return db_path, site_id, feed_ids["icon_eu"]


@pytest.mark.parametrize(
    ("label", "status", "body"),
    [
        ("400", 400, {"error": {"message": f"near {_LAT}"}}),
        ("401", 401, None),
        ("403", 403, None),
        ("404", 404, None),
    ],
)
def test_t13_job_path_status_leaks_no_coordinate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    quiet_httpx_wire: None,
    label: str,
    status: int,
    body: object,
) -> None:
    del quiet_httpx_wire
    db_path, _site_id, _leak_feed_id = _build_leak_job_fixture(
        tmp_path, f"t13-{label}.db"
    )
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        real_async_client = httpx.AsyncClient
        reply = _status_reply(status, body=body)
        transport = httpx.MockTransport(reply)

        async def _run() -> int:
            job = await _claim_fresh_job(db)
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    om_archive_fill.httpx,
                    "AsyncClient",
                    lambda *a, **k: real_async_client(transport=transport),  # noqa: ARG005
                )
                with caplog.at_level(logging.DEBUG):
                    await run_claimed_job(db, job, lane="test")
            return job.id

        job_id = asyncio.run(_run())
        last_error = asyncio.run(
            db.read(
                lambda conn: conn.execute(
                    "SELECT last_error FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()[0]
            )
        )
    finally:
        close_db()

    joined_log = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined_log, last_error or "")


def test_t13_job_path_no_hourly_leaks_no_coordinate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    quiet_httpx_wire: None,
) -> None:
    """A 200 reply with no ``hourly`` field embeds the request's latitude
    and longitude in pydantic's own ``ValidationError`` input echo -- never
    allowed to leak."""
    del quiet_httpx_wire
    db_path, _site_id, _leak_feed_id = _build_leak_job_fixture(
        tmp_path, "t13-no-hourly.db"
    )
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        real_async_client = httpx.AsyncClient

        def _no_hourly(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, json={"latitude": _LAT, "longitude": _LON}, request=request
            )

        transport = httpx.MockTransport(_no_hourly)

        async def _run() -> int:
            job = await _claim_fresh_job(db)
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    om_archive_fill.httpx,
                    "AsyncClient",
                    lambda *a, **k: real_async_client(transport=transport),  # noqa: ARG005
                )
                with caplog.at_level(logging.DEBUG):
                    await run_claimed_job(db, job, lane="test")
            return job.id

        job_id = asyncio.run(_run())
        last_error = asyncio.run(
            db.read(
                lambda conn: conn.execute(
                    "SELECT last_error FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()[0]
            )
        )
    finally:
        close_db()

    joined_log = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined_log, last_error or "")


def test_t13_job_path_timeout_leaks_no_coordinate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path, _site_id, _leak_feed_id = _build_leak_job_fixture(
        tmp_path, "t13-timeout.db"
    )
    _freeze_fill_clock(monkeypatch)
    db = init_db(str(db_path))
    try:
        real_async_client = httpx.AsyncClient

        def _timeout(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            raise httpx.ConnectTimeout("synthetic timeout")

        transport = httpx.MockTransport(_timeout)

        async def _run() -> int:
            job = await _claim_fresh_job(db)
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    om_archive_fill.httpx,
                    "AsyncClient",
                    lambda *a, **k: real_async_client(transport=transport),  # noqa: ARG005
                )
                with caplog.at_level(logging.DEBUG):
                    await run_claimed_job(db, job, lane="test")
            return job.id

        job_id = asyncio.run(_run())
        last_error = asyncio.run(
            db.read(
                lambda conn: conn.execute(
                    "SELECT last_error FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()[0]
            )
        )
    finally:
        close_db()

    joined_log = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined_log, last_error or "")


def test_cli_om_archive_fill_leaks_no_coordinate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    restore_logging_state: None,
) -> None:
    """``om-archive fill``, driven end to end through ``main()``, with a
    refusal whose reason echoes the archive's rounded grid point: no leak
    in stdout or stderr."""
    del restore_logging_state
    db_path, site_id, _leak_feed_id = _build_leak_job_fixture(
        tmp_path, "t13-cli-fill.db"
    )

    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            403,
            json={"reason": f"blocked for {_GRID_LAT},{_GRID_LON}"},
            request=request,
        )

    real_async_client = httpx.AsyncClient

    def _mock_async_client(*args: object, **kwargs: object) -> httpx.AsyncClient:
        del args, kwargs
        return real_async_client(transport=httpx.MockTransport(_handler))

    try:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(om_archive_fill.httpx, "AsyncClient", _mock_async_client)
            code = main(
                ["--db", str(db_path), "om-archive", "fill", "--site-id", str(site_id)]
            )
    finally:
        close_db()

    assert calls >= 1
    # Unlike `check`, a 403 here EXCLUDES the fixture's only model at once
    # (ModelExcluded), which marks every remaining cell `refused` -- so
    # `next_unit` then finds nothing left and the site counts as finished.
    assert code == 0
    httpx_logger = logging.getLogger("httpx")
    assert httpx_logger.level == logging.WARNING
    out, err = capsys.readouterr()
    _assert_no_leak(out, err)
