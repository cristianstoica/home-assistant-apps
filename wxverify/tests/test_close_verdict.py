"""The verdict and admission (plan 2026-10-21-daily-close-close-job.md
section 15.8): VD1 to VD9, AD1.

Written before ``verification.truth.evaluate_day_window``/``DayEvaluation``
(the public name) and ``verification.truth.write_close_truth`` exist, and
fails first for that reason: every VD test below imports one of them
inline, which raises ``ImportError`` at once (``verification/truth.py`` at
``a4a919c`` holds only ``_evaluate_day``/``_DayEvaluation``, the
underscore-prefixed pre-split names). AD1 imports
``verification.completeness.close_admission_basis`` and
``verification.truth.DayEvaluation`` inline, for the same reason
(``close_admission_basis`` does not exist at all yet).

Each VD row is also compared, field by field, with the CURRENT
``_evaluate_day`` called on the same hours under the site's published
generation (plan 15.8's preamble): VD1-VD5/AD1 do this as their own
cross-check (catching drift between the new entry point and the unchanged
rules it must keep reproducing); VD9 IS this cross-check, directly.

Section 16.2 of the plan names a mutant for VD6 (M48), VD7 (M50), VD8
(M49/M49b) and AD1 (M52); it names none for VD1-VD5 or VD9, which pin
today's UNCHANGED coverage minimums (6.2/6.3) as ``evaluate_day_window``'s
own answer, so their assertions carry plain messages. For AD1's "one hour
missing in one quantity" case, 16.2's own mutant (M52, "close_admission_basis
judges temperature only") can only be killed by a case whose missing hour is
NOT in temperature -- the gap is placed in the wind quantity, flagged
inline and in the hand-back message as the smallest case that kills it.

VD4/VD5 read a literal "short by one hour" in the plan prose as giving an
ineligible/NULL result; the near-complete gate (6.3, ``expected_slots - 1``
= 23 of 24) actually keeps a single missing hour ELIGIBLE, as VD3's own
"eligible at 23 of 24" establishes for the identical shortfall. Built with
two missing hours instead, so the asserted result is actually true of the
unchanged gate; flagged inline and in the hand-back message as a plan-text
inconsistency, not a code bug.

Real SQLite only (``tests.helpers.asof_conn``/``day_close_site``, real
``run_migrations``). Station ids are synthetic (``ISTATION01``,
``ISTATION02``), the zone is ``UTC`` throughout (so local-day bounds are
exactly midnight-to-midnight, 24 expected slots, no DST), and the date is
the fixed synthetic ``tests.helpers.D``. No live id, coordinate, site name
or real zone name appears.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    asof_conn,
    clear_close_chain_log_map,
    day_close_site,
    disable_other_enqueuers,
    disable_station_pacing,
    install_p4_standins,
    install_round_state_standin,
    note_day_clock,
    patch_close_clock,
    patch_weathercom_transport,
    run_due_jobs,
    standard_night,
    write_weathercom_key,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.site_days import (
    DayStateError,
    begin_close,
    day_record,
    fail_close,
    finish_close,
    mark_missed,
)
from wxverify.db.switch import site_generation_id
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.truth import _evaluate_day  # the PRE-split name, still live

R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test file,
    never across the whole suite."""
    clear_close_chain_log_map()


def _seed_obs(conn, site_id: int, variable: str, hour: int, value: float) -> None:  # type: ignore[no-untyped-def]
    """One ``observations`` row at D's UTC hour ``hour`` (UTC zone, so the
    local hour and the UTC hour coincide)."""
    when = datetime(D.year, D.month, D.day, hour, tzinfo=UTC)
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, ?, ?, ?, 1)",
        (site_id, variable, isoformat_utc(when), value),
    )


def _seed_hours(conn, site_id: int, variable: str, hours, value_fn) -> None:  # type: ignore[no-untyped-def]
    """``_seed_obs`` for every hour in ``hours``, value from ``value_fn(hour)``."""
    for hour in hours:
        _seed_obs(conn, site_id, variable, hour, value_fn(hour))


def _bounds():  # type: ignore[no-untyped-def]
    return local_day_bounds(D, "UTC")


def _old_outcome(conn, site_id: int, quantity: str):  # type: ignore[no-untyped-def]
    """The CURRENT ``_evaluate_day``'s outcome for ``quantity`` on D, under
    the site's published generation -- the cross-check 15.8's preamble asks
    every VD row to make against the new ``evaluate_day_window`` entry
    point.
    """
    gen_id = site_generation_id(conn, site_id)
    assert gen_id is not None
    old = _evaluate_day(
        conn, site_id=site_id, local_date=D.isoformat(), tz_generation_id=gen_id
    )
    return old.outcomes[quantity]


# ---------------------------------------------------------------------------
# VD1
# ---------------------------------------------------------------------------


def test_vd1_the_18_hour_minimum_is_the_exact_boundary() -> None:
    """VD1: readings in hours 0 to 16; then 0 to 17. High and low: 17
    covered hours give ``eligible`` 0, ``insufficient_coverage``; 18 give 1
    (6.3). Plan section 16.2 names no mutant for VD1; this pins the
    unchanged 18-hour minimum as ``evaluate_day_window``'s own answer.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()

    _seed_hours(conn, site_id, "temperature", range(17), lambda h: 10.0 + h)
    conn.commit()
    ev17 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    high17 = ev17.outcomes["temperature_high"]
    low17 = ev17.outcomes["temperature_low"]
    assert (high17.eligible, high17.exclusion_reason) == (
        False,
        "insufficient_coverage",
    ), "17 covered hours must leave the high ineligible, insufficient_coverage"
    assert (low17.eligible, low17.exclusion_reason) == (False, "insufficient_coverage")
    assert high17 == _old_outcome(conn, site_id, "temperature_high"), (
        "evaluate_day_window must reproduce _evaluate_day's own answer exactly"
    )

    _seed_obs(conn, site_id, "temperature", 17, 27.0)
    conn.commit()
    ev18 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    high18 = ev18.outcomes["temperature_high"]
    low18 = ev18.outcomes["temperature_low"]
    assert (high18.eligible, high18.exclusion_reason) == (True, None), (
        "18 covered hours must make the high eligible (the exact boundary)"
    )
    assert (low18.eligible, low18.exclusion_reason) == (True, None)
    assert high18 == _old_outcome(conn, site_id, "temperature_high")
    assert low18 == _old_outcome(conn, site_id, "temperature_low")


# ---------------------------------------------------------------------------
# VD2
# ---------------------------------------------------------------------------


def test_vd2_the_peak_window_is_checked_separately_from_coverage() -> None:
    """VD2: every hour but 12 to 17; every hour but 12 to 16; every hour
    but 3 to 8. High ``missing_peak_window``; high eligible; low
    ``missing_peak_window`` (6.3). Plan 16.2 names no mutant for VD2.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()

    # Case 1: missing 12-17 (6 hours) -- 18 covered, but high's whole peak
    # window [12, 18) is gone.
    hours = [h for h in range(24) if h not in range(12, 18)]
    _seed_hours(conn, site_id, "temperature", hours, lambda h: 10.0 + h)
    conn.commit()
    ev1 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    high1 = ev1.outcomes["temperature_high"]
    assert (high1.covered_hours, high1.eligible, high1.exclusion_reason) == (
        18,
        False,
        "missing_peak_window",
    ), "high must be ineligible for a missing peak window even with 18 covered hours"
    assert high1 == _old_outcome(conn, site_id, "temperature_high")

    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))
    conn.commit()

    # Case 2: missing 12-16 (5 hours) -- hour 17 survives, inside [12, 18).
    hours2 = [h for h in range(24) if h not in range(12, 17)]
    _seed_hours(conn, site_id, "temperature", hours2, lambda h: 10.0 + h)
    conn.commit()
    ev2 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    high2 = ev2.outcomes["temperature_high"]
    assert (high2.covered_hours, high2.eligible, high2.exclusion_reason) == (
        19,
        True,
        None,
    ), "hour 17 alone satisfies the high's peak window, so 19 covered hours is eligible"
    assert high2 == _old_outcome(conn, site_id, "temperature_high")

    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))
    conn.commit()

    # Case 3: missing 3-8 (6 hours) -- low's whole peak window [3, 9) is gone.
    hours3 = [h for h in range(24) if h not in range(3, 9)]
    _seed_hours(conn, site_id, "temperature", hours3, lambda h: 10.0 + h)
    conn.commit()
    ev3 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    low3 = ev3.outcomes["temperature_low"]
    assert (low3.covered_hours, low3.eligible, low3.exclusion_reason) == (
        18,
        False,
        "missing_peak_window",
    )
    assert low3 == _old_outcome(conn, site_id, "temperature_low")


# ---------------------------------------------------------------------------
# VD3
# ---------------------------------------------------------------------------


def test_vd3_wind_maximum_near_complete_is_expected_slots_minus_one() -> None:
    """VD3: readings missing in the last hour, then in the last two. Wind
    maximum eligible at 23 of 24; ``below_near_complete`` at 22 (6.3).
    Plan 16.2 names no mutant for VD3.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()

    _seed_hours(conn, site_id, "wind", range(23), lambda h: 5.0 + h)
    conn.commit()
    ev23 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    wind23 = ev23.outcomes["wind_max"]
    assert (wind23.covered_hours, wind23.eligible, wind23.exclusion_reason) == (
        23,
        True,
        None,
    ), "23 of 24 covered hours is the near-complete boundary -- still eligible"
    assert wind23 == _old_outcome(conn, site_id, "wind_max")

    conn.execute(
        "DELETE FROM observations WHERE site_id = ? AND variable = 'wind'"
        " AND valid_at = ?",
        (site_id, isoformat_utc(datetime(D.year, D.month, D.day, 22, tzinfo=UTC))),
    )
    conn.commit()
    ev22 = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    wind22 = ev22.outcomes["wind_max"]
    assert (wind22.covered_hours, wind22.eligible, wind22.exclusion_reason) == (
        22,
        False,
        "below_near_complete",
    ), "22 of 24 covered hours is one below the boundary -- ineligible"
    assert wind22 == _old_outcome(conn, site_id, "wind_max")


# ---------------------------------------------------------------------------
# VD4
# ---------------------------------------------------------------------------


def test_vd4_rain_total_never_counts_a_missing_hour_as_zero() -> None:
    """VD4: rain rising in every hour; then (plan-text deviation, see the
    module docstring) no readings in the last TWO hours -- a single
    missing hour stays near-complete and eligible (VD3 establishes 23 of
    24 as the boundary itself), so "short" here is built to actually
    breach it. The full day ``eligible`` 1 with the sum of the site's
    hourly values; the short day ``eligible`` 0 with the partial sum;
    never 0 for a missing hour (6.2). Plan 16.2 names no mutant for VD4.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()

    def _value(h: int) -> float:
        return 0.1 * (h + 1)

    _seed_hours(conn, site_id, "precip", range(24), _value)
    conn.commit()
    ev_full = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    total_full = ev_full.outcomes["precip_total"]
    expected_full = sum(_value(h) for h in range(24))
    assert total_full.eligible is True
    assert total_full.value == pytest.approx(expected_full)
    assert total_full == _old_outcome(conn, site_id, "precip_total")

    conn.execute(
        "DELETE FROM observations WHERE site_id = ? AND variable = 'precip'"
        " AND valid_at IN (?, ?)",
        (
            site_id,
            isoformat_utc(datetime(D.year, D.month, D.day, 22, tzinfo=UTC)),
            isoformat_utc(datetime(D.year, D.month, D.day, 23, tzinfo=UTC)),
        ),
    )
    conn.commit()
    ev_short = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    total_short = ev_short.outcomes["precip_total"]
    expected_short = sum(_value(h) for h in range(22))
    assert total_short.eligible is False
    assert total_short.exclusion_reason == "below_near_complete"
    assert total_short.value == pytest.approx(expected_short), (
        "the partial sum must be exactly the PRESENT hours' values -- never"
        " 0 contributed for a missing hour"
    )
    assert total_short == _old_outcome(conn, site_id, "precip_total")


# ---------------------------------------------------------------------------
# VD5
# ---------------------------------------------------------------------------


def test_vd5_one_wet_hour_is_never_ignored_a_dry_day_needs_near_complete() -> None:
    """VD5: threshold 0.25: one hour rising 0.25 (short coverage); the same
    at 0.24 (short coverage); a full dry day; a dry day short (plan-text
    deviation, see the module docstring: built with TWO missing hours, not
    one, so it is actually short of near-complete). Wet, ``eligible`` 1;
    NULL, ``dry_without_near_complete``; dry ``eligible`` 1; NULL (6.2).
    Plan 16.2 names no mutant for VD5.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    conn.execute("UPDATE sites SET rain_threshold_mm = 0.25 WHERE id = ?", (site_id,))
    bounds = _bounds()
    short_hours = range(10)  # well below the 23-hour near-complete threshold

    # (a) one wet hour at exactly the threshold, short coverage.
    _seed_hours(conn, site_id, "precip", short_hours, lambda h: 0.25 if h == 0 else 0.0)
    conn.commit()
    occ_a = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    ).outcomes["precip_occurrence"]
    assert (occ_a.value, occ_a.eligible, occ_a.exclusion_reason) == (1.0, True, None), (
        "one wet hour (value >= threshold) makes the day wet at ANY coverage"
    )
    assert occ_a == _old_outcome(conn, site_id, "precip_occurrence")
    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))

    # (b) the same hour just below the threshold, short coverage.
    _seed_hours(conn, site_id, "precip", short_hours, lambda h: 0.24 if h == 0 else 0.0)
    conn.commit()
    occ_b = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    ).outcomes["precip_occurrence"]
    assert (occ_b.value, occ_b.eligible, occ_b.exclusion_reason) == (
        None,
        False,
        "dry_without_near_complete",
    ), "no wet hour and short coverage must leave occurrence unknown"
    assert occ_b == _old_outcome(conn, site_id, "precip_occurrence")
    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))

    # (c) a full dry day.
    _seed_hours(conn, site_id, "precip", range(24), lambda h: 0.0)
    conn.commit()
    occ_c = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    ).outcomes["precip_occurrence"]
    assert (occ_c.value, occ_c.eligible, occ_c.exclusion_reason) == (0.0, True, None), (
        "a near-complete dry day is a known dry day, eligible, value 0.0"
    )
    assert occ_c == _old_outcome(conn, site_id, "precip_occurrence")
    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))

    # (d) a dry day short of near-complete (two hours missing).
    _seed_hours(conn, site_id, "precip", range(22), lambda h: 0.0)
    conn.commit()
    occ_d = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    ).outcomes["precip_occurrence"]
    assert (occ_d.value, occ_d.eligible, occ_d.exclusion_reason) == (
        None,
        False,
        "dry_without_near_complete",
    ), "a dry day short of near-complete coverage gets no verdict, not 0"
    assert occ_d == _old_outcome(conn, site_id, "precip_occurrence")


# ---------------------------------------------------------------------------
# VD6
# ---------------------------------------------------------------------------


def test_vd6_a_record_bounds_mismatch_fails_the_attempt_at_stage_4(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VD6: D's record bounds made one hour longer than ``local_day_bounds``
    gives (a seeded record). Stage 4 fails the attempt; no ``daily_truth``
    row written (6.1).

    M48 -> at the attempt-state assertion below: correct = the attempt
    fails at stage 4 (``retry_due``/``failed_stage`` 4) with no
    ``daily_truth`` row for D, mutant M48 (the bounds check removed) = the
    attempt closes normally over a day whose record spans an hour it does
    not actually have.

    Reaches ``install_round_state_standin`` before anything else needing
    P3's behaviour -- a ``ModuleNotFoundError`` today, since
    ``wxverify.worker.close_p4`` does not exist.

    Isolation fault fixed here: pushing D's ``day_end_utc`` an hour past
    ``standard_night``'s own full 5-minute day (D's exact 24 hours) opens a
    real one-hour gap the stage-2 gap fill tries to fill for both stations
    -- unlike every sibling VD/AD test in this file, which never shifts the
    window and so never needs a key or a transport. Without
    ``write_weathercom_key``/``patch_weathercom_transport``/
    ``disable_station_pacing`` (as every OTHER P3 test file that reaches a
    real gap-fill call installs, e.g. ``tests/test_close_pause.py``'s
    ``_prepare_chain``), the fill tries a REAL ``httpx.AsyncClient()``
    request to api.weather.com, which the suite's network guard denies (or,
    run alone/after some other file, actually resolves the host).
    """

    def _handler(request: object) -> object:
        import httpx  # noqa: PLC0415

        return httpx.Response(200, json={"observations": []})

    async def _run() -> None:
        write_weathercom_key(monkeypatch, tmp_path)
        patch_weathercom_transport(monkeypatch, _handler)  # type: ignore[arg-type]
        disable_station_pacing(monkeypatch)

        db, _writer, site_id, _stations = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert row is not None
        mismatched_end_dt = datetime.fromisoformat(
            row["day_end_utc"].replace("Z", "+00:00")
        ) + timedelta(hours=1)
        mismatched_end = isoformat_utc(mismatched_end_dt)
        conn.execute(
            "UPDATE site_days SET day_end_utc = ? WHERE site_id = ? AND local_date = ?",
            (mismatched_end, site_id, D.isoformat()),
        )
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        # D's day_end_utc was pushed an hour past its real value above, so
        # the attempt cannot begin at the round's own bound (BOUND is still
        # before the mismatched day_end_utc): the clock must reach the
        # record's (mismatched) day_end_utc for D to be eligible to close at
        # all, per plan line 192 condition 1 ("now_utc >= day_end_utc").
        clock.set(mismatched_end_dt)
        await run_due_jobs(db, clock)

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "M48 -> at this assertion: correct = ('retry_due', 1) (stage 4's"
            " bounds check fails the attempt when the record's own bounds"
            " disagree with local_day_bounds), mutant M48 (the bounds check"
            " removed) = ('closed', 1), closing over hours the day does not"
            " actually have"
        )
        truth_rows = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM daily_truth WHERE site_id = ? AND local_date = ?",
                (site_id, D.isoformat()),
            ).fetchone()[0]
        )
        assert truth_rows == 0, "no daily_truth row may be written on a bounds mismatch"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# VD7
# ---------------------------------------------------------------------------


def test_vd7_a_threshold_change_after_stage_4_never_reaches_a_closed_day(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VD7: the threshold changed after stage 4. D's rows keep the old
    threshold; D closes (6.1, 10.4).

    M50 -> at the stored-threshold assertion below: correct = D's
    ``daily_truth`` rows still carry the OLD threshold (0.2) after the
    site's rain threshold is changed between stage 4 and stage 6, mutant
    M50 (the threshold read again at stage 6 and written over the rows) =
    the rows carry the NEW threshold (0.9) instead.

    The threshold change is timed with ``stop_before=("day_close", 6)``,
    between stage 4's completion and stage 6's own run -- changing it only
    AFTER the whole night (stage 6 included) had already finished would
    make M50 a no-op: its re-read/UPDATE runs synchronously inside stage
    6's own write, at a moment when the threshold was still 0.2 either
    way, so a later external change could never reach it.

    Reaches ``install_round_state_standin`` before anything else needing
    P3's behaviour -- a ``ModuleNotFoundError`` today.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 6))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closing", (
            "stage 4 must have completed and stage 6 must still be pending"
            " -- VD7's own 'after stage 4' timing for the threshold change"
        )

        await db.write(
            lambda c: c.execute(
                "UPDATE sites SET rain_threshold_mm = 0.9 WHERE id = ?", (site_id,)
            )
        )

        await run_due_jobs(db, clock)

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closed", "the standard night must close D"

        thresholds = await db.read(
            lambda c: {
                row[0]
                for row in c.execute(
                    "SELECT rain_threshold_mm FROM daily_truth"
                    " WHERE site_id = ? AND local_date = ?"
                    " AND quantity LIKE 'precip%'",
                    (site_id, D.isoformat()),
                ).fetchall()
            }
        )
        assert thresholds == {0.2}, (
            "M50 -> at this assertion: correct = {0.2} (D's rows keep the"
            " threshold stage 4 read, unaffected by a later site change),"
            " mutant M50 (the threshold read again at stage 6 and written"
            " over the rows) = {0.9}"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# VD8
# ---------------------------------------------------------------------------


def test_vd8_write_close_truth_only_accepts_a_closing_day() -> None:
    """VD8: ``write_close_truth`` called with D ``open``, ``retry_due`` and
    ``closed``, the close on. ``DayStateError`` in each, no row written;
    allowed in ``closing`` (6.1, P1 §5.2). The ``open`` case is refused by
    ``write_close_truth``'s own state check: P1's guard alone takes
    ``TRUTH`` for an ``open`` day (``db/site_days.py:126-129``).

    M49 (``write_close_truth`` without its guard call) is equivalent here:
    ``write_close_truth`` checks for ``closing`` first, and P1's guard
    always allows ``TRUTH`` for a ``closing`` day (``db/site_days.py:126-139``),
    so removing the guard call changes nothing any test can see.

    M49b -> at the ``open``-case assertion below: correct = ``DayStateError``
    raised (``write_close_truth``'s OWN state check refuses an ``open``
    day, since P1's guard alone would accept it), mutant M49b
    (``write_close_truth`` without its ``closing`` state check) = no
    refusal at this point (P1's guard alone takes ``TRUTH`` for ``open``
    too), so a row would be written here as well.
    """
    from wxverify.verification.truth import (  # noqa: PLC0415
        evaluate_day_window,
        write_close_truth,
    )

    bounds = _bounds()
    # D's own day_end_utc (UTC zone, midnight-to-midnight): the earliest
    # instant begin_close/mark_missed will accept for D, and well after
    # D-1's end too, so one clock covers every state move below.
    after_d_end = note_day_clock(
        datetime(D.year, D.month, D.day, tzinfo=UTC) + timedelta(days=1)
    )
    d_minus_1 = D - timedelta(days=1)

    def _truth_count(conn, site_id: int) -> int:  # type: ignore[no-untyped-def]
        return conn.execute(
            "SELECT COUNT(*) FROM daily_truth WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()[0]

    def _build_eval(conn, site_id: int):  # type: ignore[no-untyped-def]
        return evaluate_day_window(
            conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
        )

    # Each case gets its OWN connection/site (pws_station_id is globally
    # UNIQUE, and the project's synthetic ids stay within ISTATION01-06),
    # not four sites sharing one conn. ``with_records=True`` gives every
    # site a real ``site_days`` row for D (``day_close_site`` fills records
    # through D via its own ``ensure_site_days(..., now_utc=NOW)`` call),
    # so D starts ``open`` in every case below instead of having no record
    # at all.

    # open -- no state move: D is left exactly as day_close_site makes it,
    # open, past its own end (after_d_end).
    conn_open = asof_conn()
    site_open = day_close_site(conn_open, close_on=True, with_records=True)
    _seed_hours(conn_open, site_open, "temperature", range(24), lambda h: 10.0 + h)
    conn_open.commit()
    with pytest.raises(DayStateError):
        write_close_truth(
            conn_open,
            _build_eval(conn_open, site_open),
            site_id=site_open,
            admission_basis="complete",
            now_utc=after_d_end,
        )
    assert _truth_count(conn_open, site_open) == 0, (
        "M49b -> at this assertion: correct = 0 (write_close_truth's"
        " OWN closing-state check refuses an open day before the guard is"
        " even asked), mutant M49b (write_close_truth without its closing"
        " state check) = 5 (P1's guard alone takes TRUTH for an open day"
        " too, so the write proceeds and five rows land)"
    )

    # retry_due
    conn_retry = asof_conn()
    site_retry = day_close_site(conn_retry, close_on=True, with_records=True)
    _seed_hours(conn_retry, site_retry, "temperature", range(24), lambda h: 10.0 + h)
    conn_retry.commit()
    mark_missed(
        conn_retry, site_retry, d_minus_1, now_utc=after_d_end, reason="never_attempted"
    )
    begin_close(conn_retry, site_retry, D, now_utc=after_d_end)
    fail_close(conn_retry, site_retry, D, now_utc=after_d_end, reason="network")
    with pytest.raises(DayStateError):
        write_close_truth(
            conn_retry,
            _build_eval(conn_retry, site_retry),
            site_id=site_retry,
            admission_basis="complete",
            now_utc=after_d_end,
        )
    assert _truth_count(conn_retry, site_retry) == 0

    # closed -- reached via finish_close, which itself needs D's
    # daily_truth rows written first while D is still closing; that write
    # is a precondition from an earlier step, done with write_close_truth
    # itself (allowed, since D is closing at that point).
    conn_closed = asof_conn()
    site_closed = day_close_site(conn_closed, close_on=True, with_records=True)
    _seed_hours(conn_closed, site_closed, "temperature", range(24), lambda h: 10.0 + h)
    conn_closed.commit()
    mark_missed(
        conn_closed,
        site_closed,
        d_minus_1,
        now_utc=after_d_end,
        reason="never_attempted",
    )
    begin_close(conn_closed, site_closed, D, now_utc=after_d_end)
    write_close_truth(
        conn_closed,
        _build_eval(conn_closed, site_closed),
        site_id=site_closed,
        admission_basis="complete",
        now_utc=after_d_end,
    )
    finish_close(conn_closed, site_closed, D, now_utc=after_d_end)
    with pytest.raises(DayStateError):
        write_close_truth(
            conn_closed,
            _build_eval(conn_closed, site_closed),
            site_id=site_closed,
            admission_basis="complete",
            now_utc=after_d_end,
        )
    assert _truth_count(conn_closed, site_closed) == 5, (
        "a closed day must refuse the second write and leave the five rows"
        " from the first (closing-day) write_close_truth call unchanged"
    )

    # closing -- allowed
    conn_closing = asof_conn()
    site_closing = day_close_site(conn_closing, close_on=True, with_records=True)
    _seed_hours(
        conn_closing, site_closing, "temperature", range(24), lambda h: 10.0 + h
    )
    conn_closing.commit()
    mark_missed(
        conn_closing,
        site_closing,
        d_minus_1,
        now_utc=after_d_end,
        reason="never_attempted",
    )
    begin_close(conn_closing, site_closing, D, now_utc=after_d_end)
    write_close_truth(
        conn_closing,
        _build_eval(conn_closing, site_closing),
        site_id=site_closing,
        admission_basis="complete",
        now_utc=after_d_end,
    )
    assert _truth_count(conn_closing, site_closing) == 5, (
        "a closing day must accept the write: one row per quantity (6.1)"
    )


# ---------------------------------------------------------------------------
# VD9
# ---------------------------------------------------------------------------


def test_vd9_the_split_matches_a_hand_computed_evaluation() -> None:
    """VD9: ``evaluate_day_window`` on a full, five-quantity day, checked
    against outcomes worked out BY HAND from the seed below -- not
    against ``_evaluate_day``. Since the split landed, ``_evaluate_day``
    is itself defined as a thin wrapper that calls ``evaluate_day_window``
    and ``replace``s in ``generation_id`` (verification/truth.py:229-234),
    so comparing the two would only compare the split against itself.

    Seed: temperature ``10.0 + h``, wind ``5.0 + h``, precip ``0.1 * h``,
    for every UTC hour ``h`` in 0..23 (the zone is UTC, so local hours
    equal UTC hours, 24 expected slots, no DST); the site's rain
    threshold is 0.2 mm, P1's own column DEFAULT (migrations.py:62),
    never set explicitly by ``day_close_site``/``asof_make_site``.

    Hand-worked expectations:
    - temperature_high: 24 covered hours (>= 18, the minimum), the
      afternoon window [12, 18) local fully covered, so eligible; the
      day's maximum is at h=23, value 33.0.
    - temperature_low: same 24 covered hours, the overnight window
      [3, 9) local fully covered, so eligible; the day's minimum is at
      h=0, value 10.0.
    - wind_max: 24 >= 23 (expected_slots - 1), eligible; the maximum is
      at h=23, value 28.0.
    - precip_total: 24 >= 23, eligible; wet hours are those >= the 0.2 mm
      threshold -- h=2 (0.2) through h=23 (2.3), 22 hours; dry hours are
      h=0 (0.0) and h=1 (0.1), 2 hours. The value itself (the sum of
      ``0.1 * h`` for h in 0..23) is computed the same way below rather
      than hand-typed as a decimal, so a float-rounding difference can
      never cause a false mismatch.
    - precip_occurrence: at least one wet hour, so value 1.0, eligible.
    """
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()
    _seed_hours(conn, site_id, "temperature", range(24), lambda h: 10.0 + h)
    _seed_hours(conn, site_id, "wind", range(24), lambda h: 5.0 + h)
    _seed_hours(conn, site_id, "precip", range(24), lambda h: 0.1 * h)
    conn.commit()

    new = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )

    assert new.threshold == 0.2, (
        "the site's rain_threshold_mm (migrations.py's DEFAULT)"
    )

    high = new.outcomes["temperature_high"]
    assert (high.value, high.eligible, high.exclusion_reason) == (33.0, True, None)
    assert (high.covered_hours, high.expected_slots, high.peak_window_ok) == (
        24,
        24,
        True,
    )

    low = new.outcomes["temperature_low"]
    assert (low.value, low.eligible, low.exclusion_reason) == (10.0, True, None)
    assert (low.covered_hours, low.expected_slots, low.peak_window_ok) == (
        24,
        24,
        True,
    )

    wind = new.outcomes["wind_max"]
    assert (wind.value, wind.eligible, wind.exclusion_reason) == (28.0, True, None)
    assert (wind.covered_hours, wind.expected_slots) == (24, 24)

    expected_precip_total = sum(0.1 * h for h in range(24))
    total = new.outcomes["precip_total"]
    assert total.value == pytest.approx(expected_precip_total)
    assert (total.eligible, total.exclusion_reason) == (True, None)
    assert (total.covered_hours, total.expected_slots) == (24, 24)
    assert (total.wet_hours, total.dry_hours) == (22, 2)

    occurrence = new.outcomes["precip_occurrence"]
    assert (occurrence.value, occurrence.eligible, occurrence.exclusion_reason) == (
        1.0,
        True,
        None,
    )
    assert (occurrence.wet_hours, occurrence.dry_hours) == (22, 2)


# ---------------------------------------------------------------------------
# AD1
# ---------------------------------------------------------------------------


def test_ad1_the_basis_judges_every_quantity_not_temperature_alone() -> None:
    """AD1: ``close_admission_basis`` with every quantity complete; with
    one hour missing in one quantity; with none. ``complete``, ``deadline``,
    ``deadline`` (7.1).

    M52 -> at the one-quantity-short assertion below: correct = ``deadline``
    (ANY quantity short of its expected slots fails completeness), mutant
    M52 (``close_admission_basis`` judges temperature only) = ``complete``
    instead -- discriminating ONLY because the missing hour here is placed
    in the WIND quantity, not temperature: if it were in temperature, the
    mutant would also read ``deadline`` and this case could not kill it.
    Plan 15.8's row does not say which quantity to use; wind is the
    smallest case that actually kills M52, flagged in the hand-back
    message.
    """
    from wxverify.verification.completeness import (  # noqa: PLC0415
        ADMISSION_COMPLETE,
        ADMISSION_DEADLINE,
        close_admission_basis,
    )
    from wxverify.verification.truth import evaluate_day_window  # noqa: PLC0415

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=False)
    bounds = _bounds()

    _seed_hours(conn, site_id, "temperature", range(24), lambda h: 10.0 + h)
    _seed_hours(conn, site_id, "wind", range(24), lambda h: 5.0 + h)
    _seed_hours(conn, site_id, "precip", range(24), lambda h: 0.0)
    conn.commit()
    ev_complete = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    assert close_admission_basis(ev_complete) == ADMISSION_COMPLETE, (
        "every quantity at its full expected slots must give 'complete'"
    )

    conn.execute(
        "DELETE FROM observations WHERE site_id = ? AND variable = 'wind'"
        " AND valid_at = ?",
        (site_id, isoformat_utc(datetime(D.year, D.month, D.day, 23, tzinfo=UTC))),
    )
    conn.commit()
    ev_wind_short = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    assert close_admission_basis(ev_wind_short) == ADMISSION_DEADLINE, (
        "M52 -> at this assertion: correct = 'deadline' (wind alone is one"
        " hour short of its expected slots; completeness reads every"
        " quantity), mutant M52 (close_admission_basis judges temperature"
        " only) = 'complete' instead, since temperature is still full here"
    )

    conn.execute("DELETE FROM observations WHERE site_id = ?", (site_id,))
    _seed_hours(conn, site_id, "temperature", range(20), lambda h: 10.0 + h)
    _seed_hours(conn, site_id, "wind", range(20), lambda h: 5.0 + h)
    _seed_hours(conn, site_id, "precip", range(20), lambda h: 0.0)
    conn.commit()
    ev_none = evaluate_day_window(
        conn, site_id=site_id, local_date=D, timezone="UTC", bounds=bounds
    )
    assert close_admission_basis(ev_none) == ADMISSION_DEADLINE, (
        "with no quantity at full coverage, the basis must still be 'deadline'"
    )
