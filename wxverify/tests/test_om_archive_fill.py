"""The ledger and fill (plan §4 T4-T8, T11; §2.3, §2.4):
``worker/om_archive_fill.py``'s ``next_unit``/``fill_unit``/``remove_fill``
and ``collection/om_archive.py``'s ``fillable_model``/``first_download_date``.

Isolation: every connection is a real, file-backed SQLite database under
``tmp_path`` (``init_db`` -- the real schema and boot seed, no mocked
persistence); archive replies come through ``httpx.MockTransport``, which
counts the requests actually made.

Synthetic data only: a made-up site at coordinates 0.0, 0.0 in zone UTC
(the clock-change zone is not needed here: T3 already pins ``day_cells``
across a clock change), made-up dates, and the boot-seeded feed models
already named in ``ALLOWANCE_MINUTES``/``RUN_CADENCE_HOURS``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from wxverify.collection.om_archive import (
    ARCHIVE_FIRST_DATE,
    FILL_EXCLUDED_MODELS,
    fillable_model,
    first_download_date,
)
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import (
    METEOSOURCE_PRUNE_TABLES,
    METEOSOURCE_UTC_FIX_KEY,
    create_schema,
    delete_pre_utc_meteosource,
)
from wxverify.db.switch import ensure_site_generation
from wxverify.feeds.open_meteo import RUN_CADENCE_HOURS
from wxverify.worker.om_archive_fill import (
    FillUnit,
    UnitDone,
    fill_unit,
    next_unit,
    remove_fill,
)

# ---------------------------------------------------------------------------
# Shared dates: S (the unit's download day), S-1 (the served run's day),
# F (first_record_date). icon_eu: 3h cadence, 240-minute allowance, so
# served_run(icon_eu, 2026-06-18T00:10Z) == 2026-06-17T18:00Z.
# ---------------------------------------------------------------------------

_S = date(2026, 6, 18)
_S_NEXT = date(2026, 6, 19)
_F = date(2026, 6, 20)
_MISSED = date(2026, 6, 21)
_R1 = datetime(2026, 6, 17, 18, 0, tzinfo=UTC)
_MAX_LEAD_HOURS = 30
# The hour, in day S, whose archive value is out of FORECAST_VALUE_RANGES
# for precip (0-500mm).
_OUT_OF_RANGE_HOUR = 5
# The hour, in day S, whose single observation is spelled "+00:00" rather
# than canonically "Z".
_NON_CANONICAL_HOUR = 10
# The hour, in day S, with no observation at all.
_NO_OBS_HOUR = 15
# The hour, in day S, with two observations texts for the same instant.
_DUP_HOUR = 20


def _z(value: datetime) -> str:
    """Canonical "Z" text."""
    return value.isoformat().replace("+00:00", "Z")


def _plus(value: datetime) -> str:
    """Non-canonical "+00:00" text (``datetime.isoformat``'s own default)."""
    return value.isoformat()


def _hour(local_date: date, hour: int) -> datetime:
    return datetime(local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fixture construction.
# ---------------------------------------------------------------------------


def _build_site(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('om-archive-fill-fixture', 0.0, 0.0, 0.0, 'UTC')"
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


def _set_max_lead_hours(db_path: Path, feed_id: int, hours: int) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE feeds SET max_lead_hours = ? WHERE id = ?", (hours, feed_id))
    conn.commit()
    conn.close()


def _insert_site_days(
    db_path: Path,
    site_id: int,
    local_date: date,
    *,
    state: str,
    missed_reason: str | None,
) -> None:
    conn = sqlite3.connect(db_path)
    day_start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    now = _z(day_start)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, zone_changeover, attempts, missed_reason,"
        " created_at, state_changed_at, done_at)"
        " VALUES (?, ?, ?, ?, ?, 'UTC', 0, 1, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            state,
            _z(day_start),
            _z(day_end),
            missed_reason,
            now,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()


def _insert_observation(
    conn: sqlite3.Connection, site_id: int, variable: str, valid_at: str
) -> None:
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, ?, ?, 0.0, 1)",
        (site_id, variable, valid_at),
    )


def _insert_forecast_history_row(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    valid_at: str,
    local_date: date,
    value: float,
) -> None:
    conn.execute(
        "INSERT INTO forecast_history (site_id, feed_id, variable, day_ahead,"
        " valid_at, local_date, value, lead_hours, issued_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, 6, '2026-06-17T18:00:00Z')",
        (
            site_id,
            feed_id,
            variable,
            day_ahead,
            valid_at,
            local_date.isoformat(),
            value,
        ),
    )


def _build_fixture(tmp_path: Path, name: str) -> tuple[Path, int, int, int]:
    """Build the T4 scenario: a closed day at F, a missed day at F+1, two
    pre-close download-day cells (day_ahead 0 at S, day_ahead 1 at S+1) of
    one active icon_eu unit, over all three ``FILL_VARIABLES``.

    Returns ``(db_path, site_id, feed_id, control_feed_id)``; no Database
    is left open.
    """
    db_path = tmp_path / name
    site_id = _build_site(db_path)
    db = init_db(str(db_path))
    close_db()
    del db
    feed_id = _feed_id(db_path, "open-meteo", "icon_eu")
    control_feed_id = _feed_id(db_path, "meteosource", "blend")
    _set_max_lead_hours(db_path, feed_id, _MAX_LEAD_HOURS)
    _insert_site_days(db_path, site_id, _F, state="closed", missed_reason=None)
    _insert_site_days(
        db_path, site_id, _MISSED, state="missed", missed_reason="never_attempted"
    )

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")

    # Day S (day_ahead 0): precip and wind are written cells; temperature
    # is the PRESENT cell (already has a forecast_history row, so the fill
    # must skip it entirely).
    for hour in range(24):
        instant = _hour(_S, hour)
        if hour == _NON_CANONICAL_HOUR:
            _insert_observation(conn, site_id, "precip", _plus(instant))
        else:
            _insert_observation(conn, site_id, "precip", _z(instant))
        if hour == _NO_OBS_HOUR:
            continue
        if hour == _DUP_HOUR:
            _insert_observation(conn, site_id, "wind", _plus(instant))
            _insert_observation(conn, site_id, "wind", _z(instant))
        else:
            _insert_observation(conn, site_id, "wind", _z(instant))

    # Day S+1 (day_ahead 1): precip is observed over its full 24 hours, to
    # prove the live lead bound -- not a missing observation -- is what
    # keeps hours 04:00-23:00 unwritten. temperature/wind are only observed
    # over the 4 hours the bound actually keeps (00:00-03:00).
    for hour in range(24):
        instant = _hour(_S_NEXT, hour)
        _insert_observation(conn, site_id, "precip", _z(instant))
        if hour < 4:
            _insert_observation(conn, site_id, "temperature", _z(instant))
            _insert_observation(conn, site_id, "wind", _z(instant))

    # The PRESENT cell: temperature, day_ahead 0, D=S already has a row.
    _insert_forecast_history_row(
        conn, site_id, feed_id, "temperature", 0, _z(_hour(_S, 6)), _S, 99.9
    )
    # The non-Open-Meteo canary: must survive the fill untouched.
    _insert_forecast_history_row(
        conn, site_id, control_feed_id, "temperature", 0, _z(_hour(_S, 6)), _S, 77.7
    )
    conn.commit()
    conn.close()
    return db_path, site_id, feed_id, control_feed_id


def _archive_handler(calls: list[int]) -> httpx.MockTransport:
    """Always answers run=``_R1`` with 60 flat hours of data (precip 0.0,
    temperature 10.0, wind 5.0 km/h), except hour index 12 (valid_at
    2026-06-18T05:00:00Z after the rain shift), whose precip value (600.0mm)
    is out of ``FORECAST_VALUE_RANGES``. Any other run is a 400 (no run),
    which would force a fallback the test does not expect."""

    def _handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        run_text = request.url.params["run"]
        run_at = datetime.strptime(run_text, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)
        if run_at != _R1:
            return httpx.Response(400, json={"reason": "no run"}, request=request)
        hours = [_R1 + timedelta(hours=i) for i in range(60)]
        times = [_z(h) for h in hours]
        precip = [600.0 if i == 12 else 0.0 for i in range(60)]
        return httpx.Response(
            200,
            json={
                "latitude": 0.0,
                "longitude": 0.0,
                "hourly": {
                    "time": times,
                    "temperature_2m": [10.0] * 60,
                    "precipitation": precip,
                    "wind_speed_10m": [5.0] * 60,
                },
            },
            request=request,
        )

    return httpx.MockTransport(_handler)


def _row_value(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    valid_at: str,
) -> float | None:
    row = conn.execute(
        "SELECT value FROM forecast_history WHERE site_id=? AND feed_id=?"
        " AND variable=? AND day_ahead=? AND valid_at=?",
        (site_id, feed_id, variable, day_ahead, valid_at),
    ).fetchone()
    return None if row is None else float(row[0])


def _cell_rows(
    conn: sqlite3.Connection, site_id: int, feed_id: int, variable: str, day_ahead: int
) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            "SELECT valid_at FROM forecast_history WHERE site_id=? AND feed_id=?"
            " AND variable=? AND day_ahead=? ORDER BY valid_at",
            (site_id, feed_id, variable, day_ahead),
        )
    ]


# ---------------------------------------------------------------------------
# T4 -- selection.
# ---------------------------------------------------------------------------


def test_t4_selection(tmp_path: Path) -> None:
    """Catches a window or a stamp that differs from the live download's and
    the close's.

    Only pre-close, empty, observed cells of the active Open-Meteo feed are
    written: the PRESENT cell (temperature) is skipped and its row stays
    byte-equal; an out-of-range archive value writes no row for its hour; an
    hour with no observation writes no row; a non-canonical observation text
    is carried through verbatim; two observation texts for one instant give
    one row, with the stamp the scan returns last; a mock reply that runs
    past the live lead bound (``snap_run`` + ``max_lead_hours``) writes no
    row past it, even though both the archive value and the observation
    exist there; the non-Open-Meteo feed's row is untouched.
    """
    calls = [0]
    db_path, site_id, feed_id, control_feed_id = _build_fixture(tmp_path, "t4.db")
    db = init_db(str(db_path))
    try:

        async def _run() -> UnitDone:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                result = await fill_unit(db, site_id, feed_id, _S, client=client)
                assert isinstance(result, UnitDone)
                return result

        result = asyncio.run(_run())
    finally:
        close_db()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Exactly one archive request: the needed window was covered by R1
    # alone, with no fallback to an older run.
    assert calls[0] == 1, "M4a -> at this assertion: correct = 1, mutant = " + str(
        calls[0]
    )

    # The PRESENT cell: unchanged, byte-equal.
    present_value = _row_value(
        conn, site_id, feed_id, "temperature", 0, _z(_hour(_S, 6))
    )
    assert present_value == 99.9, (
        f"M4b -> at this assertion: correct = 99.9, mutant = {present_value!r}"
    )
    assert len(_cell_rows(conn, site_id, feed_id, "temperature", 0)) == 1, (
        "the PRESENT cell must gain no new row"
    )

    # precip day_ahead 0: 23 of 24 hours (the out-of-range hour dropped).
    precip_day0 = _cell_rows(conn, site_id, feed_id, "precip", 0)
    assert len(precip_day0) == 23, (
        f"M4c -> at this assertion: correct = 23, mutant = {len(precip_day0)}"
    )
    out_of_range_valid_at = _z(_hour(_S, _OUT_OF_RANGE_HOUR))
    assert out_of_range_valid_at not in precip_day0, (
        "M4d -> at this assertion: correct = absent (out of FORECAST_VALUE_RANGES),"
        f" mutant = present ({out_of_range_valid_at})"
    )

    # precip day_ahead 0, the non-canonical hour: carried through verbatim.
    non_canonical_valid_at = _plus(_hour(_S, _NON_CANONICAL_HOUR))
    assert non_canonical_valid_at in precip_day0, (
        "M4e -> at this assertion: correct = the non-canonical '+00:00' text"
        f" present, mutant = absent (precip_day0={precip_day0!r})"
    )
    assert _z(_hour(_S, _NON_CANONICAL_HOUR)) not in precip_day0, (
        "the non-canonical hour must not ALSO appear re-spelled canonically"
    )

    # wind day_ahead 0: 23 of 24 hours (the no-observation hour dropped);
    # the duplicate-text hour produces exactly one row, with whichever
    # stamp hourly_values' scan happens to return last -- this oracle does
    # not bet on which of the two texts that is, only that it is exactly
    # one of them, never both and never a third spelling.
    wind_day0 = _cell_rows(conn, site_id, feed_id, "wind", 0)
    assert len(wind_day0) == 23, (
        f"M4f -> at this assertion: correct = 23, mutant = {len(wind_day0)}"
    )
    no_obs_valid_at = _z(_hour(_S, _NO_OBS_HOUR))
    assert no_obs_valid_at not in wind_day0, (
        f"M4g -> at this assertion: correct = absent,"
        f" mutant = present ({no_obs_valid_at})"
    )
    dup_canonical = _z(_hour(_S, _DUP_HOUR))
    dup_noncanonical = _plus(_hour(_S, _DUP_HOUR))
    dup_present = [v for v in wind_day0 if v in (dup_canonical, dup_noncanonical)]
    assert dup_present == [dup_canonical] or dup_present == [dup_noncanonical], (
        "M4h -> at this assertion: correct = exactly one of the two duplicate"
        f" texts present, mutant = {dup_present!r}"
    )

    # Day S+1 (day_ahead 1): only hours 00:00-03:00 survive the live lead
    # bound, even though precip's observations and archive values exist for
    # every hour of that day.
    precip_day1 = _cell_rows(conn, site_id, feed_id, "precip", 1)
    expected_day1 = [_z(_hour(_S_NEXT, h)) for h in range(4)]
    assert precip_day1 == expected_day1, (
        f"M4i -> at this assertion: correct = {expected_day1!r},"
        f" mutant = {precip_day1!r}"
    )
    temperature_day1 = _cell_rows(conn, site_id, feed_id, "temperature", 1)
    assert temperature_day1 == expected_day1
    wind_day1 = _cell_rows(conn, site_id, feed_id, "wind", 1)
    assert wind_day1 == expected_day1

    # The non-Open-Meteo feed's row: untouched.
    control_value = _row_value(
        conn, site_id, control_feed_id, "temperature", 0, _z(_hour(_S, 6))
    )
    assert control_value == 77.7, (
        f"M4j -> at this assertion: correct = 77.7, mutant = {control_value!r}"
    )
    assert len(_cell_rows(conn, site_id, control_feed_id, "temperature", 0)) == 1

    # The ledger: 6 cells, exactly one PRESENT and five WRITTEN; the
    # FillCounts totals the call returns match.
    ledger_rows = conn.execute(
        "SELECT variable, day_ahead, outcome, hours FROM om_archive_cells"
        " WHERE site_id=? AND feed_id=? ORDER BY variable, day_ahead",
        (site_id, feed_id),
    ).fetchall()
    assert len(ledger_rows) == 6
    outcomes = {(r["variable"], r["day_ahead"]): r["outcome"] for r in ledger_rows}
    assert outcomes[("temperature", 0)] == "present"
    assert outcomes[("precip", 0)] == "written"
    assert outcomes[("wind", 0)] == "written"
    assert outcomes[("precip", 1)] == "written"
    assert outcomes[("temperature", 1)] == "written"
    assert outcomes[("wind", 1)] == "written"

    assert result.counts.written == 5
    assert result.counts.present == 1
    assert result.counts.no_hours == 0
    assert result.counts.no_run == 0
    assert result.counts.refused == 0
    assert result.counts.rows == 23 + 23 + 4 + 4 + 4
    assert result.counts.calls == 1
    conn.close()


# ---------------------------------------------------------------------------
# T5 -- rerun.
# ---------------------------------------------------------------------------


def test_t5_rerun_makes_no_requests_and_adds_nothing(tmp_path: Path) -> None:
    """A second ``fill_unit`` of the same unit makes 0 requests and adds 0
    rows and 0 ledger rows: every cell already has a ledger row, so the
    second call returns before asking the archive anything. Paired with
    T4's first-run assertions (which prove the first call DOES write), so
    this zero result cannot be a vacuous "it never does anything" oracle.
    """
    calls = [0]
    db_path, site_id, feed_id, _control_feed_id = _build_fixture(tmp_path, "t5.db")
    db = init_db(str(db_path))
    try:

        async def _run() -> tuple[UnitDone, UnitDone]:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                first = await fill_unit(db, site_id, feed_id, _S, client=client)
                assert isinstance(first, UnitDone)
                second = await fill_unit(db, site_id, feed_id, _S, client=client)
                assert isinstance(second, UnitDone)
                return first, second

        first, second = asyncio.run(_run())
    finally:
        close_db()

    assert first.counts.written == 5, "fixture bug: the first run must write"
    assert calls[0] == 1, "fixture bug: the first run must make one request"

    assert calls[0] == 1, (
        "M5a -> at this assertion: correct = 1 (no second request),"
        f" mutant = {calls[0]}"
    )
    assert second.counts.written == 0, (
        f"M5b -> at this assertion: correct = 0, mutant = {second.counts.written}"
    )
    assert second.counts.present == 0
    assert second.counts.no_hours == 0
    assert second.counts.no_run == 0
    assert second.counts.refused == 0
    assert second.counts.rows == 0, (
        f"M5c -> at this assertion: correct = 0, mutant = {second.counts.rows}"
    )
    assert second.counts.calls == 0

    conn = sqlite3.connect(db_path)
    ledger_count = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()[0]
    assert ledger_count == 6, (
        f"M5d -> at this assertion: correct = 6, mutant = {ledger_count}"
    )
    conn.close()


# ---------------------------------------------------------------------------
# T6 -- resume. A smaller, single-day fixture: F = S+1, so only day_ahead 0
# is pre-close, and every hour of every variable is observed with no
# edge-case quirks -- this test is about the write failing, not selection.
# ---------------------------------------------------------------------------


def _build_resume_fixture(tmp_path: Path, name: str) -> tuple[Path, int, int]:
    db_path = tmp_path / name
    site_id = _build_site(db_path)
    db = init_db(str(db_path))
    close_db()
    del db
    feed_id = _feed_id(db_path, "open-meteo", "icon_eu")
    _set_max_lead_hours(db_path, feed_id, _MAX_LEAD_HOURS)
    _insert_site_days(db_path, site_id, _S_NEXT, state="closed", missed_reason=None)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    for hour in range(24):
        instant = _hour(_S, hour)
        for variable in ("precip", "temperature", "wind"):
            _insert_observation(conn, site_id, variable, _z(instant))
    conn.commit()
    conn.close()
    return db_path, site_id, feed_id


def test_t6_a_write_failure_leaves_nothing_and_the_next_run_fills_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure raised while writing a fetched unit leaves no row and no
    ledger row behind (the write transaction rolls back whole); the next
    (unpatched) run fills the unit from scratch, with totals equal to a run
    that hit no fault. Catches: the fetch and the write not sharing one
    all-or-nothing boundary (a partial write surviving a mid-write fault).
    """
    import wxverify.worker.om_archive_fill as om_archive_fill

    calls = [0]
    db_path, site_id, feed_id = _build_resume_fixture(tmp_path, "t6.db")
    db = init_db(str(db_path))
    real_write_unit = om_archive_fill._write_unit  # noqa: SLF001
    attempts = {"n": 0}

    def _flaky_write_unit(*args: object, **kwargs: object) -> object:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("synthetic write failure")
        return real_write_unit(*args, **kwargs)  # type: ignore[arg-type]

    try:
        monkeypatch.setattr(om_archive_fill, "_write_unit", _flaky_write_unit)

        async def _first() -> None:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                await fill_unit(db, site_id, feed_id, _S, client=client)

        with pytest.raises(RuntimeError, match="synthetic write failure"):
            asyncio.run(_first())

        conn = sqlite3.connect(db_path)
        history_count = conn.execute(
            "SELECT COUNT(*) FROM forecast_history WHERE site_id=? AND feed_id=?",
            (site_id, feed_id),
        ).fetchone()[0]
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM om_archive_cells WHERE site_id=? AND feed_id=?",
            (site_id, feed_id),
        ).fetchone()[0]
        conn.close()
        assert history_count == 0, (
            f"M6a -> at this assertion: correct = 0, mutant = {history_count}"
        )
        assert ledger_count == 0, (
            f"M6b -> at this assertion: correct = 0, mutant = {ledger_count}"
        )

        monkeypatch.setattr(om_archive_fill, "_write_unit", real_write_unit)

        async def _second() -> UnitDone:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                result = await fill_unit(db, site_id, feed_id, _S, client=client)
                assert isinstance(result, UnitDone)
                return result

        result = asyncio.run(_second())
    finally:
        close_db()

    assert result.counts.written == 3, (
        f"M6c -> at this assertion: correct = 3, mutant = {result.counts.written}"
    )
    # 24 hours x 3 variables, less the shared handler's one out-of-range
    # precip hour (index 12, valid_at 2026-06-18T05:00Z, _OUT_OF_RANGE_HOUR),
    # which falls inside this fixture's day 0 too and is correctly dropped.
    assert result.counts.rows == 24 * 3 - 1, (
        f"M6d -> at this assertion: correct = 71, mutant = {result.counts.rows}"
    )
    assert result.counts.present == 0
    assert result.counts.no_hours == 0
    assert result.counts.no_run == 0
    assert result.counts.refused == 0


# ---------------------------------------------------------------------------
# T7 -- removal selects exactly the fill.
# ---------------------------------------------------------------------------


def test_t7_remove_fill_selects_exactly_the_filled_rows(tmp_path: Path) -> None:
    """``remove_fill``, called inside the caller's own transaction, deletes
    exactly ``SUM(hours)`` of the ``written`` ledger rows; the deleted rows
    are exactly the ones the fill added (key sets equal); every row that
    was there before the fill is still there after the removal; it issues
    no BEGIN/COMMIT/ROLLBACK itself (``conn.in_transaction`` stays true, and
    a ROLLBACK then restores every row); called with no open transaction,
    it raises at once and touches nothing.
    """
    calls = [0]
    db_path, site_id, feed_id, _control_feed_id = _build_fixture(tmp_path, "t7.db")
    db = init_db(str(db_path))
    try:

        async def _fill() -> None:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                await fill_unit(db, site_id, feed_id, _S, client=client)

        asyncio.run(_fill())
    finally:
        close_db()

    def _all_history_rows(conn: sqlite3.Connection) -> set[tuple[object, ...]]:
        return {
            tuple(row)
            for row in conn.execute("SELECT * FROM forecast_history ORDER BY 1,2,3,4,5")
        }

    before = sqlite3.connect(db_path)
    before.row_factory = sqlite3.Row
    pre_remove_rows = _all_history_rows(before)
    written_sum = before.execute(
        "SELECT SUM(hours) FROM om_archive_cells WHERE outcome = 'written'"
    ).fetchone()[0]
    before.close()
    assert written_sum == 23 + 23 + 4 + 4 + 4, "fixture bug: T4's totals must hold"

    # "Called with no open transaction raises" -- a plain autocommit
    # connection, no BEGIN issued.
    autocommit = sqlite3.connect(db_path, isolation_level=None)
    autocommit.row_factory = sqlite3.Row
    with pytest.raises(RuntimeError):
        remove_fill(autocommit)
    assert not autocommit.in_transaction
    untouched_rows = _all_history_rows(autocommit)
    assert untouched_rows == pre_remove_rows, (
        "a refused remove_fill (no open transaction) must change nothing"
    )
    autocommit.close()

    # The real removal, inside an explicit transaction.
    txn = sqlite3.connect(db_path, isolation_level=None)
    txn.row_factory = sqlite3.Row
    txn.execute("BEGIN")
    removed = remove_fill(txn)
    assert removed == written_sum, (
        f"M7a -> at this assertion: correct = {written_sum}, mutant = {removed}"
    )
    assert txn.in_transaction, (
        "M7b -> at this assertion: correct = True (no implicit COMMIT/ROLLBACK),"
        f" mutant = {txn.in_transaction}"
    )
    post_remove_rows = _all_history_rows(txn)
    removed_keys = pre_remove_rows - post_remove_rows
    assert len(removed_keys) == written_sum, (
        f"M7c -> at this assertion: correct = {written_sum},"
        f" mutant = {len(removed_keys)}"
    )
    assert post_remove_rows < pre_remove_rows, "remove_fill must have deleted something"

    # A ROLLBACK restores every row.
    txn.execute("ROLLBACK")
    rolled_back_rows = _all_history_rows(txn)
    assert rolled_back_rows == pre_remove_rows, (
        "M7d -> at this assertion: correct = every pre-removal row restored,"
        f" mutant = {len(rolled_back_rows)} of {len(pre_remove_rows)} rows present"
    )
    txn.close()


# ---------------------------------------------------------------------------
# T8 -- nothing else changes.
# ---------------------------------------------------------------------------

_ALLOWED_TO_CHANGE = {"forecast_history", "om_archive_cells", "api_budget", "jobs"}


def _schema_tables(conn: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(
        sorted(
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
                " AND name NOT LIKE 'sqlite_%'"
            )
        )
    )


def _all_table_rows(conn: sqlite3.Connection) -> dict[str, list[tuple[object, ...]]]:
    return {
        table: [
            tuple(row)
            for row in conn.execute(f"SELECT * FROM {table}")  # noqa: S608
        ]
        for table in _schema_tables(conn)
    }


def test_t8_nothing_else_changes(tmp_path: Path) -> None:
    """An allowlist check: hashing (by full row comparison) every table
    before and after a fill, only ``forecast_history``, ``om_archive_cells``,
    ``api_budget`` and ``jobs`` may differ.
    """
    calls = [0]
    db_path, site_id, feed_id, _control_feed_id = _build_fixture(tmp_path, "t8.db")

    before_conn = sqlite3.connect(db_path)
    before_conn.row_factory = sqlite3.Row
    before = _all_table_rows(before_conn)
    before_conn.close()

    db = init_db(str(db_path))
    try:

        async def _fill() -> None:
            async with httpx.AsyncClient(transport=_archive_handler(calls)) as client:
                await fill_unit(db, site_id, feed_id, _S, client=client)

        asyncio.run(_fill())
    finally:
        close_db()

    after_conn = sqlite3.connect(db_path)
    after_conn.row_factory = sqlite3.Row
    after = _all_table_rows(after_conn)
    after_conn.close()

    assert set(before) == set(after), "the fill must create or drop no table"
    changed = {table for table in before if before[table] != after[table]}
    unexpected = changed - _ALLOWED_TO_CHANGE
    assert unexpected == set(), (
        f"M8 -> at this assertion: correct = no table outside {_ALLOWED_TO_CHANGE!r}"
        f" changes, mutant = unexpected changes in {unexpected!r}"
    )
    assert "forecast_history" in changed, "fixture bug: the fill must write something"
    assert "om_archive_cells" in changed, (
        "fixture bug: the fill must write a ledger row"
    )


# ---------------------------------------------------------------------------
# T11 -- schema, fillable_model, first_download_date, next_unit exhaustion.
# ---------------------------------------------------------------------------


def test_t11_om_archive_cells_is_in_the_meteosource_prune_list() -> None:
    assert "om_archive_cells" in METEOSOURCE_PRUNE_TABLES


def test_t11_an_om_archive_cells_row_is_pruned_with_its_meteosource_feed(
    tmp_path: Path,
) -> None:
    """A direct proof that ``om_archive_cells`` is actually swept by the
    Meteosource one-time prune, not merely named in the list: a
    Meteosource-sourced ledger row is deleted by
    ``delete_pre_utc_meteosource``."""
    db_path = tmp_path / "t11-prune.db"
    site_id = _build_site(db_path)
    db = init_db(str(db_path))
    close_db()
    del db
    meteosource_feed_id = _feed_id(db_path, "meteosource", "blend")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("DELETE FROM runtime_state WHERE key = ?", (METEOSOURCE_UTC_FIX_KEY,))
    conn.execute(
        "INSERT INTO om_archive_cells (site_id, feed_id, variable, day_ahead,"
        " local_date, download_date, outcome, hours, run_at, filled_at)"
        " VALUES (?, ?, 'precip', 0, '2026-06-18', '2026-06-18', 'written', 1,"
        " '2026-06-17T18:00:00Z', '2026-06-18T00:10:00Z')",
        (site_id, meteosource_feed_id),
    )
    conn.commit()

    delete_pre_utc_meteosource(conn)

    remaining = conn.execute(
        "SELECT COUNT(*) FROM om_archive_cells WHERE feed_id = ?",
        (meteosource_feed_id,),
    ).fetchone()[0]
    assert remaining == 0, (
        f"M11 -> at this assertion: correct = 0, mutant = {remaining}"
    )
    conn.close()


@pytest.mark.parametrize("model", sorted(RUN_CADENCE_HOURS))
def test_t11_every_roster_model_is_fillable(model: str) -> None:
    """``FILL_EXCLUDED_MODELS`` is empty (owner decision D7, 2026-11-12:
    jma_gsm is included), so every one of the eight roster models,
    jma_gsm included, is fillable."""
    assert fillable_model(model) is True
    assert model not in FILL_EXCLUDED_MODELS


def test_t11_fill_excluded_models_is_empty() -> None:
    assert frozenset() == FILL_EXCLUDED_MODELS


@pytest.mark.parametrize(
    ("first_observed", "expected"),
    [
        (date(2026, 6, 18), date(2026, 6, 11)),  # the ordinary case: -7 days
        (date(2026, 4, 2), date(2026, 4, 2)),  # exactly at ARCHIVE_FIRST_DATE
        (date(2026, 3, 20), date(2026, 4, 2)),  # before it: clamped
    ],
)
def test_t11_first_download_date(first_observed: date, expected: date) -> None:
    assert first_download_date(first_observed) == expected
    assert date(2026, 4, 2) == ARCHIVE_FIRST_DATE


def test_t11_next_unit_is_none_once_every_variable_has_its_last_s_row(
    tmp_path: Path,
) -> None:
    """Once every (feed, variable)'s latest ``day_ahead = 0`` ledger row is
    at S_last (the day before the first record), ``next_unit`` returns None
    for that feed; with only one active Open-Meteo feed at the site,
    ``next_unit`` then returns None for the site."""
    db_path = tmp_path / "t11-exhausted.db"
    site_id = _build_site(db_path)
    db = init_db(str(db_path))
    close_db()
    del db
    feed_id = _feed_id(db_path, "open-meteo", "icon_eu")
    s_last = _F - timedelta(days=1)
    _insert_site_days(db_path, site_id, _F, state="closed", missed_reason=None)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # One rain observation so _fill_site does not refuse the site outright.
    _insert_observation(conn, site_id, "precip", _z(_hour(_S, 0)))
    # Deactivate every other Open-Meteo feed, so only icon_eu is active.
    for row in conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND id != ?", (feed_id,)
    ):
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, int(row[0])),
        )
    for variable in ("precip", "temperature", "wind"):
        conn.execute(
            "INSERT INTO om_archive_cells (site_id, feed_id, variable, day_ahead,"
            " local_date, download_date, outcome, hours, run_at, filled_at)"
            " VALUES (?, ?, ?, 0, ?, ?, 'present', 0, NULL, '2026-06-18T00:10:00Z')",
            (
                site_id,
                feed_id,
                variable,
                s_last.isoformat(),
                s_last.isoformat(),
            ),
        )
    conn.commit()

    unit = next_unit(conn, site_id)
    assert unit is None, f"M11b -> at this assertion: correct = None, mutant = {unit!r}"
    conn.close()


def test_t11_next_unit_still_returns_a_unit_one_day_before_exhaustion(
    tmp_path: Path,
) -> None:
    """Paired positive for the test above: with the ledger's latest
    day_ahead-0 row one day short of S_last, ``next_unit`` still returns
    that next, unfinished day -- so the None result above is not merely
    "next_unit is always None" ambient behaviour."""
    db_path = tmp_path / "t11-not-exhausted.db"
    site_id = _build_site(db_path)
    db = init_db(str(db_path))
    close_db()
    del db
    feed_id = _feed_id(db_path, "open-meteo", "icon_eu")
    s_last = _F - timedelta(days=1)
    one_short = s_last - timedelta(days=1)
    _insert_site_days(db_path, site_id, _F, state="closed", missed_reason=None)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    _insert_observation(conn, site_id, "precip", _z(_hour(_S, 0)))
    for row in conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND id != ?", (feed_id,)
    ):
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, int(row[0])),
        )
    for variable in ("precip", "temperature", "wind"):
        conn.execute(
            "INSERT INTO om_archive_cells (site_id, feed_id, variable, day_ahead,"
            " local_date, download_date, outcome, hours, run_at, filled_at)"
            " VALUES (?, ?, ?, 0, ?, ?, 'present', 0, NULL, '2026-06-18T00:10:00Z')",
            (
                site_id,
                feed_id,
                variable,
                one_short.isoformat(),
                one_short.isoformat(),
            ),
        )
    conn.commit()

    unit = next_unit(conn, site_id)
    assert unit == FillUnit(feed_id=feed_id, download_date=s_last), (
        f"M11c -> at this assertion: correct ="
        f" {FillUnit(feed_id=feed_id, download_date=s_last)!r}, mutant = {unit!r}"
    )
    conn.close()
