"""The nightly gap-fill pause: SP1 to SP6.

Written before ``close_chain.enqueue_due_close_work``, ``close_p4`` and
stage 2's body exist, and fails first for that reason: every test here
drives the real night chain end to end
(``tests.helpers.install_round_state_standin``/``install_p4_standins``, the
pattern of ``tests/test_close_timeline.py``), so each reaches
``install_round_state_standin`` before doing anything else -- a
``ModuleNotFoundError`` today, since ``wxverify.worker.close_p4`` does not
exist.

The pause itself reads the COUNTED day before D's own
``close_station_days`` row (written at stage 2's completion), not a
live recomputation. Since nothing writes that table yet, each test seeds
the counted day's ``close_station_days`` row for station C directly --
the data stage 2 is specified to have produced, not a recomputation this
test could share a bug with -- then drives the chain for D itself through
``run_due_jobs`` to observe D's own written presence/pause and gap-fill
try-log rows once stage 2 exists.

The mutant each test ID kills is named inline on the exact assertion it
kills at.

Real SQLite only (``tests.helpers.day_close_site``/``standard_night``'s
own building blocks, real ``run_migrations`` via ``init_db`` on a
``tmp_path`` file). Station ids are synthetic (``ISTATION01`` to
``ISTATION03``), the zone is ``UTC``, and every date is the fixed
synthetic ``tests.helpers.D``/the derived round date ``R``. No live id,
coordinate, site name or real zone name appears.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    clear_close_chain_log_map,
    day_close_site,
    disable_other_enqueuers,
    disable_station_pacing,
    install_p4_standins,
    install_round_state_standin,
    parse_utc,
    patch_close_clock,
    patch_weathercom_transport,
    run_due_jobs,
    write_weathercom_key,
)
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.site_days import begin_close, day_record, finish_close, mark_missed

R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND
_STATION_C = "ISTATION03"
_SEED_DATE = (D - timedelta(days=10)).isoformat()
_SEED_CHANGED_AT = isoformat_utc_micro(
    datetime(D.year, D.month, D.day, tzinfo=UTC) - timedelta(days=10)
)


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    -- autouse HERE, inside this one close-chain test file, never across
    the whole suite."""
    clear_close_chain_log_map()


def _add_station_c(conn: sqlite3.Connection, site_id: int) -> int:
    """Add station C (``ISTATION03``) to the site, effective D-10, the
    same pattern ``tests.helpers.day_close_site`` uses for A and B."""
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, _STATION_C),
    )
    assert cur.lastrowid is not None
    station_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO station_changes"
        " (site_id, station_id, change, effective_date, reason, changed_at)"
        " VALUES (?, ?, 'added', ?, 'test_seed', ?)",
        (site_id, station_id, _SEED_DATE, _SEED_CHANGED_AT),
    )
    return station_id


def _seed_close_station_day(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    station_id: int,
    *,
    five_minute: int = 0,
    current_obs: int = 0,
    hourly: int = 0,
    answered: bool = False,
    refused_lost: bool = False,
    paused: bool = False,
) -> None:
    """Seed the one ``close_station_days`` row stage 2 would have written
    for ``local_date`` -- nothing writes this table yet, so a
    test that needs a COUNTED day's presence builds it directly."""
    conn.execute(
        "INSERT INTO close_station_days (site_id, local_date, station_id,"
        " five_minute, current_obs, hourly, answered, refused_lost, paused)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            station_id,
            five_minute,
            current_obs,
            hourly,
            int(answered),
            int(refused_lost),
            int(paused),
        ),
    )


def _close_day(conn: sqlite3.Connection, site_id: int, local_date: date) -> None:
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, local_date, now_utc=t)
    finish_close(conn, site_id, local_date, now_utc=t)


def _miss_day(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, reason: str
) -> None:
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    mark_missed(conn, site_id, local_date, now_utc=t, reason=reason)


def _seed_abc_site(tmp_path, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Build the site fresh (no default D-3/D-2/D-1 history, unlike
    ``tests.helpers.standard_night``): stations A, B and C on D's list,
    D-2 and D-1 each built by the caller to the shape its SP row needs.
    Returns ``(db, writer, site_id, {pws_id: station_id})``.
    """
    from wxverify.db.connection import FencedWriter, close_db, init_db
    from wxverify.db.runtime_state import set_runtime_state

    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    from wxverify import config

    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- test fixture builder, real writer conn

    from wxverify.db.site_days import ensure_site_days, start_site_days

    site_id = day_close_site(conn, timezone="UTC", with_records=False)
    c_station_id = _add_station_c(conn, site_id)

    d_minus_2 = D - timedelta(days=2)
    start_site_days(
        conn,
        site_id,
        d_minus_2,
        now_utc=datetime(d_minus_2.year, d_minus_2.month, d_minus_2.day, tzinfo=UTC),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)

    stations = {
        str(row["pws_station_id"]): int(row["id"])
        for row in conn.execute(
            "SELECT id, pws_station_id FROM stations WHERE site_id = ? ORDER BY id",
            (site_id,),
        )
    }
    assert stations[_STATION_C] == c_station_id

    set_runtime_state(conn, "worker_started_at", isoformat_utc(NOW))
    conn.commit()
    return db, FencedWriter(db), site_id, stations


def _seed_five_minute_day(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> None:
    for hour in range(24):
        for minute, speed in ((0, 5.0), (5, 5.5)):
            when = datetime(
                local_date.year,
                local_date.month,
                local_date.day,
                hour,
                minute,
                tzinfo=UTC,
            )
            conn.execute(
                "INSERT INTO station_wind_records"
                " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
                "  precip_total_mm, fetched_at)"
                " VALUES (?, ?, 'five_minute', ?, ?, 10.0, 0.0, ?)",
                (
                    station_id,
                    isoformat_utc(when),
                    local_date.isoformat(),
                    speed,
                    isoformat_utc(when),
                ),
            )


def _seed_current_obs_reading(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> None:
    when = datetime(local_date.year, local_date.month, local_date.day, 6, 0, tzinfo=UTC)
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
        "  precip_total_mm, fetched_at)"
        " VALUES (?, ?, 'current_obs', ?, 5.0, 10.0, 0.0, ?)",
        (
            station_id,
            isoformat_utc(when),
            local_date.isoformat(),
            isoformat_utc(when),
        ),
    )


async def _close_station_day_row(db, site_id, local_date, station_id):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT five_minute, current_obs, hourly, paused FROM close_station_days"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?",
            (site_id, local_date.isoformat(), station_id),
        ).fetchone()
    )


async def _try_log_rows(db, site_id, local_date, station_id):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT outcome FROM close_try_log WHERE site_id = ? AND local_date = ?"
            " AND station_id = ? AND slot = 'gap_fill'",
            (site_id, local_date.isoformat(), station_id),
        ).fetchall()
    )


def _default_transport_handler(request):  # type: ignore[no-untyped-def]
    """A full, well-formed hourly reply for any
    gap-fill call this file's non-pause SP cases let through."""
    import httpx

    return httpx.Response(200, json={"observations": []})


def _prepare_chain(tmp_path, monkeypatch: pytest.MonkeyPatch, db):  # type: ignore[no-untyped-def]
    disable_station_pacing(monkeypatch)
    write_weathercom_key(monkeypatch, tmp_path)
    patch_weathercom_transport(monkeypatch, _default_transport_handler)


# ---------------------------------------------------------------------------
# SP1
# ---------------------------------------------------------------------------


def test_sp1_a_station_with_nothing_for_two_days_is_paused(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP1: C had nothing on D − 1; C has nothing on D. C ``paused`` on D;
    no gap-fill request for C; C's try-log row ``skipped``; D closes.

    M58 -> at the transport-call assertion below: correct = zero gap-fill
    requests for C (the pause withholds the call), mutant M58 (a paused
    station still passed to ``fill_gaps``) = at least one request for C.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path, monkeypatch)
        _prepare_chain(tmp_path, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        for pws_id in ("ISTATION01", "ISTATION02", _STATION_C):
            _seed_close_station_day(
                conn,
                site_id,
                d_minus_2,
                stations[pws_id],
                five_minute=12,
                answered=True,
            )
        _close_day(conn, site_id, d_minus_1)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_1,
            stations[_STATION_C],
            five_minute=0,
            current_obs=0,
            hourly=0,
            answered=False,
        )
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        # C: nothing on D either.
        conn.commit()

        requests: list[tuple] = []

        def _counting_handler(request):  # type: ignore[no-untyped-def]
            requests.append((request,))
            return _default_transport_handler(request)

        patch_weathercom_transport(monkeypatch, _counting_handler)

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        c_requests = [r for (r,) in requests if _STATION_C in str(r.url)]
        assert c_requests == [], (
            "M58 -> at this assertion: correct = zero gap-fill requests for"
            " C, mutant M58 (a paused station still passed to fill_gaps) ="
            " at least one request for C"
        )
        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        assert row["paused"] == 1, "C must be recorded paused on D"
        tries = await _try_log_rows(db, site_id, D, stations[_STATION_C])
        assert [t["outcome"] for t in tries] == ["skipped"], (
            "C's gap-fill try-log row must be 'skipped'"
        )
        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closed", "the pause must not block the close"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# SP2
# ---------------------------------------------------------------------------


def test_sp2_a_returning_station_is_not_paused(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP2: C had nothing on D − 1; C has a current-conditions reading on
    D. C not paused; its gap hours filled.

    M56 -> at the paused assertion below: correct = ``paused`` 0 (D itself
    has a reading, so the pause never applies regardless of yesterday),
    mutant M56 (the pause applies even when the station has readings on
    D) = ``paused`` 1.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path, monkeypatch)
        _prepare_chain(tmp_path, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        for pws_id in ("ISTATION01", "ISTATION02", _STATION_C):
            _seed_close_station_day(
                conn,
                site_id,
                d_minus_2,
                stations[pws_id],
                five_minute=12,
                answered=True,
            )
        _close_day(conn, site_id, d_minus_1)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_1,
            stations[_STATION_C],
            five_minute=0,
            current_obs=0,
            hourly=0,
            answered=False,
        )
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        _seed_current_obs_reading(conn, stations[_STATION_C], D)
        conn.commit()

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        assert row["paused"] == 0, (
            "M56 -> at this assertion: correct = paused 0, mutant M56 (the"
            " pause applies even when the station has readings on D) = 1"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# SP3
# ---------------------------------------------------------------------------


def test_sp3_a_day_lost_to_refused_access_does_not_pause(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP3: C's D − 1 lost to refused access (``answered`` 0, ``refused_at``
    set); D − 2 had readings. C not paused on D.

    M53 -> at the paused assertion below: correct = ``paused`` 0 (D − 1 is
    skipped as lost to refused access, so D − 2 -- which had readings --
    decides), mutant M53 (the pause counts a day lost to refused access)
    = ``paused`` 1, wrongly treating the refused day as "nothing".
    """

    async def _run() -> None:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path, monkeypatch)
        _prepare_chain(tmp_path, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_2,
            stations[_STATION_C],
            five_minute=12,
            current_obs=0,
            hourly=0,
            answered=True,
        )
        _close_day(conn, site_id, d_minus_1)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_1,
            stations[_STATION_C],
            five_minute=0,
            current_obs=0,
            hourly=0,
            answered=False,
            refused_lost=True,
        )
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        # C: nothing on D either, so the pause check actually walks back.
        conn.commit()

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        assert row["paused"] == 0, (
            "M53 -> at this assertion: correct = paused 0 (D-2 decides,"
            " D-1 is skipped), mutant M53 (the pause counts a refused-"
            "access day) = paused 1"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# SP4
# ---------------------------------------------------------------------------


def test_sp4_a_missed_day_is_skipped_the_earlier_day_decides(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP4: D − 1 ``missed``; D − 2 had nothing for C. C paused (the
    missed day skipped, D − 2 decides).

    The base fixture (D-2 ALSO says nothing) cannot by itself
    discriminate M54 from the correct rule: both give ``paused`` 1 here,
    one by correctly skipping to D-2, the other by wrongly treating the
    missed D-1 itself as "nothing". A paired positive is added below,
    identical except that D-2 HAS readings for C, which is what actually
    forces the two to diverge.

    M54 -> at the paired-positive assertion below: correct = ``paused`` 0
    (D-1 is skipped as missed, D-2 -- which has readings -- decides),
    mutant M54 (the pause counts a missed day as "nothing") = ``paused``
    1 regardless, since it never looks past the missed D-1 at all.
    """

    async def _run(*, d_minus_2_has_readings: bool, suffix: str) -> int:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path / suffix, monkeypatch)
        _prepare_chain(tmp_path / suffix, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_2,
            stations[_STATION_C],
            five_minute=12 if d_minus_2_has_readings else 0,
            current_obs=0,
            hourly=0,
            answered=d_minus_2_has_readings,
        )
        _miss_day(conn, site_id, d_minus_1, reason="never_attempted")
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        # C: nothing on D either, in both cases, so the pause check walks
        # back past the missed D-1 regardless.
        conn.commit()

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        return int(row["paused"])

    paused_plan_case = asyncio.run(
        _run(d_minus_2_has_readings=False, suffix="plan_case")
    )
    assert paused_plan_case == 1, (
        "the base case (D-1 missed, D-2 also nothing): C must be"
        " paused on D; see the paired case below for M54's kill"
    )

    paused_paired = asyncio.run(_run(d_minus_2_has_readings=True, suffix="paired_case"))
    assert paused_paired == 0, (
        "M54 -> at this assertion: correct = paused 0 (D-1 skipped as"
        " missed, D-2 -- which HAS readings -- decides), mutant M54 (the"
        " pause counts a missed day as 'nothing') = paused 1, never"
        " looking past D-1"
    )


# ---------------------------------------------------------------------------
# SP5
# ---------------------------------------------------------------------------


def test_sp5_current_conditions_readings_count_as_something(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP5: C had only current-conditions readings on D − 1. Not nothing:
    C not paused.

    M55 -> at the paused assertion below: correct = ``paused`` 0 (Home
    Assistant readings count), mutant M55 (the pause leaves
    out ``current_obs`` readings) = ``paused`` 1, treating a day with only
    current-conditions readings as a day with nothing.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path, monkeypatch)
        _prepare_chain(tmp_path, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        for pws_id in ("ISTATION01", "ISTATION02", _STATION_C):
            _seed_close_station_day(
                conn,
                site_id,
                d_minus_2,
                stations[pws_id],
                five_minute=12,
                answered=True,
            )
        _close_day(conn, site_id, d_minus_1)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_1,
            stations[_STATION_C],
            five_minute=0,
            current_obs=3,
            hourly=0,
            answered=False,
        )
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        # C: nothing on D either, so the pause check actually walks back.
        conn.commit()

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        assert row["paused"] == 0, (
            "M55 -> at this assertion: correct = paused 0 (current_obs"
            " readings count as something), mutant M55 (the pause leaves"
            " out current_obs readings) = paused 1"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# SP6
# ---------------------------------------------------------------------------


def test_sp6_the_counted_day_is_the_latest_one_listing_the_station(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SP6: C not on D − 1's list, on D's list; D − 2 had nothing for C
    and listed it. C paused (the counted day is the latest one listing
    it).

    M57 -> at the paused assertion below: correct = ``paused`` 1 (D-2 is
    the latest day that actually listed C, and it said nothing), mutant
    M57 (the pause reads the latest closed day whatever its station
    list) = reads D-1 instead, which never listed C, and the station
    would not be found there at all -- a different, wrong answer than
    "paused" from the correct rule's own D-2 read.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = _seed_abc_site(tmp_path, monkeypatch)
        _prepare_chain(tmp_path, monkeypatch, db)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        conn = db._conn  # noqa: SLF001
        d_minus_2 = D - timedelta(days=2)
        d_minus_1 = D - timedelta(days=1)
        _close_day(conn, site_id, d_minus_2)
        _seed_close_station_day(
            conn,
            site_id,
            d_minus_2,
            stations[_STATION_C],
            five_minute=0,
            current_obs=0,
            hourly=0,
            answered=False,
        )
        # C was added effective D-10 (_add_station_c), so it was written onto
        # D-1's saved station list when D-1 opened (site_days.py :355-409).
        # Strip it directly, as stage 2's own data would never do this
        # listing: D-1 must not list C.
        conn.execute(
            "DELETE FROM site_day_stations WHERE site_id = ? AND local_date = ?"
            " AND station_id = ?",
            (site_id, d_minus_1.isoformat(), stations[_STATION_C]),
        )
        _close_day(conn, site_id, d_minus_1)
        for station_id in (stations["ISTATION01"], stations["ISTATION02"]):
            _seed_five_minute_day(conn, station_id, D)
        conn.commit()

        clock.set(BOUND)
        await run_due_jobs(db, clock)

        row = await _close_station_day_row(db, site_id, D, stations[_STATION_C])
        assert row["paused"] == 1, (
            "M57 -> at this assertion: correct = paused 1 (D-2 is the"
            " latest day that listed C and it said nothing), mutant M57"
            " (the pause reads the latest closed day whatever its station"
            " list) = looks at D-1, which never listed C at all"
        )

    asyncio.run(_run())
