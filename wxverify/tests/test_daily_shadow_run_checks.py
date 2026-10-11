"""§15.3 sub-batch 2a: run-level data checks against the real pipeline.

Every fixture is synthetic (§15.1): 2035 target dates, ``SYN-`` feed and
station identifiers, a site at latitude 0.0 / longitude 0.0, and the
timezone ``UTC``. Each test builds its own database in ``:memory:`` with
``run_migrations`` and inserts its own site directly (never
``asof_make_site``, whose coordinates are not 0.0/0.0). Every freeze note
built for ``--heldout`` names a house station by a ``SYN-`` id.

Departures from §15.1 and §15.3: databases are ``:memory:`` because these
tests call ``run()`` with a connection, so the file-opening path
(``main()``) is the owed item at batch1:236-240. The lower bound filler
covers leads 0-5 only, the leads it asserts on. Its Arm B case has no
stations at all.

Mutant ledger (§15.4):

M27 (``_run_temperature`` applies a lower bound to ``recorded_map`` for
training reads) -- implemented: yes; reviewed: pending; mutation-verified:
pending (checkpoint C, §15.4:1448).

M29 (``excluded_h_dates`` ignored by ``select_cases``) -- implemented: yes;
reviewed: pending; mutation-verified: pending (checkpoint C, §15.4:1448).
Killed at ``select_cases`` only. The ``main()`` route
(daily_shadow.py:7067-7069) is the owed item in
tests/test_daily_shadow_batch1_mutants.py:236-240.

M30 (the station-set comparison against the note is skipped) --
implemented: yes; reviewed: pending; mutation-verified: pending (checkpoint
C, §15.4:1448).

M31 (``stale_truth_rows``, daily_shadow.py:1989-2007, stops adding a stale
published row to its date sets) -- implemented: yes; reviewed: pending;
mutation-verified: pending (checkpoint C, §15.4:1448). The ``stale``
counter ``training_truth`` also tracks is equivalent, because the result
is discarded by both callers at daily_shadow.py:2447 and :2657.

M35 (``db_import_changed`` compares ``value`` only, not ``updated_at``) --
implemented: yes; reviewed: pending; mutation-verified: pending (checkpoint
C, §15.4:1448).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import set_setting
from wxverify.verification import daily_shadow
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc

_TIMEZONE = "UTC"
_QUANTITIES = daily_shadow.TEMPERATURE_QUANTITIES


def _header(files: dict[str, bytes]) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(files["header.json"]))


def _lines(files: dict[str, bytes]) -> list[dict[str, Any]]:
    return [
        cast("dict[str, Any]", json.loads(raw))
        for raw in files["lines.jsonl"].decode().splitlines()
    ]


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    """A synthetic site at lat/lon 0.0 (§15.1); never ``asof_make_site``."""
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 100.0, ?)",
        (name, _TIMEZONE),
    )
    return cast(int, cur.lastrowid)


def _make_site(conn: sqlite3.Connection, name: str) -> daily_shadow.Site:
    site_id = _insert_site(conn, name)
    ensure_published_generation(conn, site_id)
    set_setting(conn, "forecast_blend_depth", "1")
    return daily_shadow.load_site(conn, site_id=site_id)


def _insert_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    value: float,
) -> None:
    """One feed's 24 hourly samples for ``local_date`` (the heldout-wiring pattern)."""
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for hour in range(24):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


def _build_case_row(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    snapshot_date: date,
    target_date: date,
    value: float,
) -> None:
    """One scored lead row built entirely by production code (passes F1 by
    construction, since the replay reads the same ``forecast_samples``)."""
    issued_day = snapshot_date - timedelta(days=1)
    issued = f"{issued_day.isoformat()}T06:00:00Z"
    fetched = f"{issued_day.isoformat()}T06:05:00Z"
    _insert_day_samples(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=target_date,
        issued_at=issued,
        fetched_at=fetched,
        value=value,
    )
    now = resolve_snapshot_utc(_TIMEZONE, snapshot_date, "07:00")
    build_forecast_record(conn, site_id, snapshot_date.isoformat(), now=now)


def _insert_filler_record_row(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    snapshot_date: date,
    lead: int,
    target_date: date,
) -> None:
    """A minimal ``recorded`` row: content is irrelevant, only its identity
    and ``status`` matter for ``recorded_map``/training-day counting; it is
    never selected as a scored case in these fixtures."""
    snapshot_utc = resolve_snapshot_utc(_TIMEZONE, snapshot_date, "07:00")
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, methodology_version, app_version)
        VALUES (?, ?, ?, ?, '07:00', ?, ?, 'temperature', ?, 'recorded',
                'on_time', 1, 'x')
        """,
        (
            site.site_id,
            site.generation_id,
            _TIMEZONE,
            snapshot_date.isoformat(),
            isoformat_utc(snapshot_utc),
            target_date.isoformat(),
            lead,
        ),
    )


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
    stale: bool = False,
) -> None:
    day_start = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 0, tzinfo=UTC)
    )
    day_end = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, 23, tzinfo=UTC)
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             stale, tz_generation_id)
        VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 'UTC', ?, ?)
        """,
        (
            site_id,
            local_date.isoformat(),
            quantity,
            value,
            day_start,
            day_end,
            1 if stale else 0,
            tz_generation_id,
        ),
    )


def _insert_station(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    pws_station_id: str,
    enabled: bool = True,
    created_at: str = "2030-01-01T00:00:00.000Z",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)
        VALUES (?, ?, 0.0, 0.0, 100.0, ?, ?)
        """,
        (site_id, pws_station_id, 1 if enabled else 0, created_at),
    )
    return cast(int, cur.lastrowid)


def _insert_station_day(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    value: float,
    null_fetched_hour: int | None = None,
) -> None:
    """24 hourly ``station_observations`` readings of a constant value."""
    for hour in range(24):
        hour_dt = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        valid_at = isoformat_utc(hour_dt)
        fetched_at = (
            None
            if hour == null_fetched_hour
            else isoformat_utc(hour_dt + timedelta(minutes=5))
        )
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, fetched_at)
            VALUES (?, 'temperature', ?, ?, 'ok', ?)
            """,
            (station_id, valid_at, value, fetched_at),
        )


def _note(
    *,
    site_id: int,
    h_first: date,
    excluded_h_dates: tuple[date, ...] = (),
    station_set: tuple[daily_shadow.StationEntry, ...] = (),
    db_import: dict[str, str] | None = None,
) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=site_id,
        first_h_date=h_first,
        excluded_h_dates=excluded_h_dates,
        station_set=station_set,
        db_import=db_import,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _run(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    mode: daily_shadow.Mode,
    first: date,
    last: date,
    h_first: date,
    last_complete: date,
    excluded_h_dates: tuple[date, ...],
    note: daily_shadow.FreezeNote | None,
) -> dict[str, bytes]:
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode=mode,
        first=first,
        last=last,
        h_first=h_first,
        last_complete=last_complete,
        excluded_h_dates=excluded_h_dates,
        t2_last=None,
        note=note,
        house_station=None if note is None else note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


# --- 1. Stale truth (M31) ---------------------------------------------------

_STALE_H_FIRST = date(2035, 6, 15)
_STALE_K_DAY = _STALE_H_FIRST - timedelta(days=1)  # 2035-06-14
_STALE_WINDOW_START = _STALE_K_DAY - timedelta(days=30)  # Arm B start: 2035-05-15
_STALE_TRAINING_DAY = date(2035, 6, 1)  # inside Arm B's window, chosen stale
_NULL_FETCH_DAY = date(2035, 5, 20)


def _build_stale_truth_fixture(
    *, stale_training_day: date | None, stale_h_target: bool
) -> tuple[sqlite3.Connection, daily_shadow.Site, daily_shadow.FreezeNote]:
    """One lead-0 case at ``_STALE_H_FIRST``; a 31-day in-scope window
    (Arm A/B union) with matching station data, forced to Arm B by a single
    in-scope reading with a NULL ``fetched_at`` (G-a fails trivially while
    G-b's match stays clean)."""
    conn = _connect()
    site = _make_site(conn, "stale-truth-site")
    feed_a = _insert_real_feed(conn, "SYN-a")
    station_id = _insert_station(conn, site_id=site.site_id, pws_station_id="SYN-1")

    day = _STALE_WINDOW_START
    while day <= _STALE_K_DAY:
        for quantity in _QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=20.0,
                stale=(day == stale_training_day),
            )
        _insert_station_day(
            conn,
            station_id=station_id,
            local_date=day,
            value=20.0,
            null_fetched_hour=0 if day == _NULL_FETCH_DAY else None,
        )
        day += timedelta(days=1)

    for quantity in _QUANTITIES:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            tz_generation_id=site.generation_id,
            local_date=_STALE_H_FIRST,
            quantity=quantity,
            value=20.0,
            stale=stale_h_target,
        )

    _build_case_row(
        conn,
        site_id=site.site_id,
        feed_id=feed_a,
        snapshot_date=_STALE_H_FIRST,
        target_date=_STALE_H_FIRST,
        value=20.0,
    )
    conn.commit()
    note = _note(site_id=site.site_id, h_first=_STALE_H_FIRST)
    return conn, site, note


def _insert_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed, fetch_interval_minutes)
        VALUES ('SYN', ?, 1, 60)
        """,
        (model,),
    )
    return cast(int, cur.lastrowid)


def test_stale_truth_rows_counts_training_and_h_target_once_each() -> None:
    conn, site, note = _build_stale_truth_fixture(
        stale_training_day=_STALE_TRAINING_DAY, stale_h_target=True
    )
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2035, 1, 1),
        last=date(2035, 1, 1),
        h_first=_STALE_H_FIRST,
        last_complete=_STALE_H_FIRST,
        excluded_h_dates=(),
        note=note,
    )
    header = _header(files)
    for quantity in _QUANTITIES:
        counts = header["stale_truth_rows"][quantity]
        assert counts["training_window_days"] == 1
        assert counts["H_targets"] == 1
        assert counts["choosing_set_targets"] == 0

    gates = header["gates"]
    assert gates["ga_pass"] is False
    assert gates["arm"] == "B"
    assert gates["gb_stale_days"] == 1
    assert gates["gb_matched"] == 30
    assert gates["gb_denominator"] == 30

    stale_target_lines = [
        line for line in _lines(files) if line["target"] == _STALE_H_FIRST.isoformat()
    ]
    assert len(stale_target_lines) == len(_QUANTITIES)
    for line in stale_target_lines:
        assert line["unmatched"] == "truth_stale"
        assert line["training_days"] is None

    for quantity in _QUANTITIES:
        assert header["unmatched"]["H"][quantity]["truth_stale"] == {"0": 1}


def test_stale_truth_rows_zero_and_h_target_scored_when_nothing_is_stale() -> None:
    """Paired positive: with neither row stale, the counts are all zero and
    the H target gets a real scored line instead of ``truth_stale``."""
    conn, site, note = _build_stale_truth_fixture(
        stale_training_day=None, stale_h_target=False
    )
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2035, 1, 1),
        last=date(2035, 1, 1),
        h_first=_STALE_H_FIRST,
        last_complete=_STALE_H_FIRST,
        excluded_h_dates=(),
        note=note,
    )
    header = _header(files)
    for quantity in _QUANTITIES:
        counts = header["stale_truth_rows"][quantity]
        assert counts["training_window_days"] == 0
        assert counts["H_targets"] == 0
        assert counts["choosing_set_targets"] == 0

    gates = header["gates"]
    assert gates["gb_stale_days"] == 0
    assert gates["gb_matched"] == 31
    assert gates["gb_denominator"] == 31

    target_lines = [
        line for line in _lines(files) if line["target"] == _STALE_H_FIRST.isoformat()
    ]
    assert len(target_lines) == len(_QUANTITIES)
    for line in target_lines:
        assert line["unmatched"] is None


# --- 2. Station set (M30) ---------------------------------------------------


def _build_station_set_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site, date]:
    conn = _connect()
    site = _make_site(conn, "station-set-site")
    feed_a = _insert_real_feed(conn, "SYN-a")
    h_first = date(2035, 3, 1)
    for pws_station_id, created_at in (
        ("SYN-1", "2030-01-01T00:00:00.000Z"),
        ("SYN-2", "2030-01-02T00:00:00.000Z"),
        ("SYN-3", "2030-01-03T00:00:00.000Z"),
    ):
        _insert_station(
            conn,
            site_id=site.site_id,
            pws_station_id=pws_station_id,
            created_at=created_at,
        )
    for quantity in _QUANTITIES:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            tz_generation_id=site.generation_id,
            local_date=h_first,
            quantity=quantity,
            value=20.0,
        )
    _build_case_row(
        conn,
        site_id=site.site_id,
        feed_id=feed_a,
        snapshot_date=h_first,
        target_date=h_first,
        value=20.0,
    )
    conn.commit()
    return conn, site, h_first


def test_station_set_changed_reports_a_toggled_station() -> None:
    conn, site, h_first = _build_station_set_fixture()
    station_set = tuple(daily_shadow.load_station_set(conn, site_id=site.site_id))
    note = _note(site_id=site.site_id, h_first=h_first, station_set=station_set)

    conn.execute("UPDATE stations SET enabled=0 WHERE pws_station_id='SYN-2'")
    conn.commit()

    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=h_first,
        last=h_first,
        h_first=h_first,
        last_complete=h_first,
        excluded_h_dates=(),
        note=note,
    )
    changed = _header(files)["station_set_changed"]
    assert changed == {
        "added": [],
        "removed": [],
        "toggled": ["SYN-2"],
        "recreated": [],
    }


def test_station_set_changed_reports_a_recreated_station() -> None:
    conn, site, h_first = _build_station_set_fixture()
    station_set = tuple(daily_shadow.load_station_set(conn, site_id=site.site_id))
    note = _note(site_id=site.site_id, h_first=h_first, station_set=station_set)

    conn.execute("DELETE FROM stations WHERE pws_station_id='SYN-3'")
    _insert_station(
        conn,
        site_id=site.site_id,
        pws_station_id="SYN-3",
        created_at="2032-06-01T00:00:00.000Z",
    )
    conn.commit()

    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=h_first,
        last=h_first,
        h_first=h_first,
        last_complete=h_first,
        excluded_h_dates=(),
        note=note,
    )
    changed = _header(files)["station_set_changed"]
    assert changed == {
        "added": [],
        "removed": [],
        "toggled": [],
        "recreated": ["SYN-3"],
    }


def test_station_set_changed_reports_nothing_when_unchanged() -> None:
    """Paired positive: with no mutation after the note, all four lists
    are empty."""
    conn, site, h_first = _build_station_set_fixture()
    station_set = tuple(daily_shadow.load_station_set(conn, site_id=site.site_id))
    note = _note(site_id=site.site_id, h_first=h_first, station_set=station_set)

    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=h_first,
        last=h_first,
        h_first=h_first,
        last_complete=h_first,
        excluded_h_dates=(),
        note=note,
    )
    changed = _header(files)["station_set_changed"]
    assert changed == {"added": [], "removed": [], "toggled": [], "recreated": []}


# --- 3. Import record (M35) -------------------------------------------------


def _build_import_record_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """A minimal site with no cases at all: ``header_facts`` is computed
    unconditionally, so no case/truth data is needed to exercise it."""
    conn = _connect()
    site = _make_site(conn, "import-record-site")
    conn.commit()
    return conn, site


def _run_both_modes(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    h_first: date,
    note: daily_shadow.FreezeNote,
) -> tuple[dict[str, Any], dict[str, Any]]:
    counts_only = _header(
        _run(
            conn,
            site=site,
            mode="counts-only",
            first=h_first,
            last=h_first,
            h_first=h_first,
            last_complete=h_first,
            excluded_h_dates=(),
            note=note,
        )
    )
    heldout = _header(
        _run(
            conn,
            site=site,
            mode="heldout",
            first=h_first,
            last=h_first,
            h_first=h_first,
            last_complete=h_first,
            excluded_h_dates=(),
            note=note,
        )
    )
    return counts_only, heldout


def test_db_import_changed_true_when_noted_absent_and_copy_has_a_row() -> None:
    h_first = date(2035, 4, 1)
    conn, site = _build_import_record_fixture()
    note = _note(site_id=site.site_id, h_first=h_first, db_import=None)

    set_runtime_state(conn, "import_rebuild_state", "done")
    conn.commit()

    row = conn.execute(
        "SELECT value, updated_at FROM runtime_state WHERE key = 'import_rebuild_state'"
    ).fetchone()
    expected = {"value": row["value"], "updated_at": row["updated_at"]}

    for header in _run_both_modes(conn, site=site, h_first=h_first, note=note):
        assert header["db_import"] == expected
        assert header["db_import_changed"] is True


def test_db_import_changed_true_when_value_matches_but_updated_at_differs() -> None:
    """Kills M35: a mutant comparing ``value`` only would call this
    unchanged, since both sides hold ``value == "done"``."""
    h_first = date(2035, 4, 1)
    conn, site = _build_import_record_fixture()
    note = _note(
        site_id=site.site_id,
        h_first=h_first,
        db_import={"value": "done", "updated_at": "2035-01-05T03:00:00.000Z"},
    )
    conn.execute(
        "INSERT INTO runtime_state (key, value, updated_at)"
        " VALUES ('import_rebuild_state', 'done', '2035-01-08T03:00:00.000Z')"
    )
    conn.commit()

    for header in _run_both_modes(conn, site=site, h_first=h_first, note=note):
        assert header["db_import"] == {
            "value": "done",
            "updated_at": "2035-01-08T03:00:00.000Z",
        }
        assert header["db_import_changed"] is True


def test_db_import_changed_false_when_nothing_changed() -> None:
    """Negative control, paired with the two cases above: no row at all on
    either side gives ``db_import_changed`` false and every station-set
    list empty. Paired with the test below: the no-change control covers
    both shapes, no row on either side, and an identical row on both
    sides."""
    h_first = date(2035, 4, 1)
    conn, site = _build_import_record_fixture()
    note = _note(site_id=site.site_id, h_first=h_first, db_import=None)
    conn.commit()

    for header in _run_both_modes(conn, site=site, h_first=h_first, note=note):
        assert header["db_import"] is None
        assert header["db_import_changed"] is False
        assert header["station_set_changed"] == {
            "added": [],
            "removed": [],
            "toggled": [],
            "recreated": [],
        }


def test_db_import_changed_false_when_the_same_row_is_on_both_sides() -> None:
    h_first = date(2035, 4, 1)
    conn, site = _build_import_record_fixture()
    record = {"value": "done", "updated_at": "2035-01-05T03:00:00.000Z"}
    note = _note(site_id=site.site_id, h_first=h_first, db_import=record)
    conn.execute(
        "INSERT INTO runtime_state (key, value, updated_at)"
        " VALUES ('import_rebuild_state', 'done', '2035-01-05T03:00:00.000Z')"
    )
    conn.commit()

    for header in _run_both_modes(conn, site=site, h_first=h_first, note=note):
        assert header["db_import"] == record
        assert header["db_import_changed"] is False


# --- 4. Excluded H dates (M29) ----------------------------------------------

_EXCLUDED_H_FIRST = date(2035, 1, 10)
_EXCLUDED_LAST_COMPLETE = date(2035, 1, 11)
_EXCLUDED_DATE = date(2035, 1, 10)
_EXCLUDED_K_DAY = date(2035, 1, 10)  # K of the lead-0 case for target 2035-01-11
_EXCLUDED_WINDOW_START = _EXCLUDED_K_DAY - timedelta(days=29)  # Arm A start: 2034-12-12


def _build_excluded_dates_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """A lead-0 case for target 2035-01-11 (K = 2035-01-10) with a clean,
    fully matching 30-day Arm-A training window and station set -- Arm A is
    selected so the window includes K itself, 2035-01-10, the date excluded
    from H. Also a lead-0 row AT 2035-01-10 (the excluded date's own would-
    be case), present in the database so it can be used as a training day
    elsewhere even though it is never scored."""
    conn = _connect()
    site = _make_site(conn, "excluded-h-dates-site")
    feed_a = _insert_real_feed(conn, "SYN-a")
    station_id = _insert_station(conn, site_id=site.site_id, pws_station_id="SYN-1")

    day = _EXCLUDED_WINDOW_START
    while day <= _EXCLUDED_K_DAY:
        for quantity in _QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=20.0,
            )
        _insert_station_day(conn, station_id=station_id, local_date=day, value=20.0)
        _insert_filler_record_row(
            conn, site=site, snapshot_date=day, lead=0, target_date=day
        )
        day += timedelta(days=1)

    for quantity in _QUANTITIES:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            tz_generation_id=site.generation_id,
            local_date=date(2035, 1, 11),
            quantity=quantity,
            value=20.0,
        )
    _build_case_row(
        conn,
        site_id=site.site_id,
        feed_id=feed_a,
        snapshot_date=date(2035, 1, 11),
        target_date=date(2035, 1, 11),
        value=20.0,
    )
    conn.commit()
    return conn, site


def test_excluded_h_dates_removes_the_case_but_keeps_the_row_as_training() -> None:
    conn, site = _build_excluded_dates_fixture()
    note = _note(
        site_id=site.site_id,
        h_first=_EXCLUDED_H_FIRST,
        excluded_h_dates=(_EXCLUDED_DATE,),
    )
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2034, 1, 1),
        last=date(2034, 1, 1),
        h_first=_EXCLUDED_H_FIRST,
        last_complete=_EXCLUDED_LAST_COMPLETE,
        excluded_h_dates=(_EXCLUDED_DATE,),
        note=note,
    )
    header = _header(files)
    assert header["gates"]["arm"] == "A"
    assert header["excluded_h_dates_count"] == 1
    lines = _lines(files)
    assert [line for line in lines if line["target"] == "2035-01-10"] == []

    target_lines = [
        line for line in lines if line["target"] == "2035-01-11" and line["lead"] == 0
    ]
    assert len(target_lines) == len(_QUANTITIES)
    for line in target_lines:
        assert line["unmatched"] is None
        assert line["training_days"] == 30
        assert line["training_truth_arm"] == "A"
        assert line["training_window"] == ["2034-12-12", "2035-01-10"]


def test_h_date_appears_as_a_target_when_not_excluded() -> None:
    """Paired positive: without the exclusion, 2035-01-10 is selected as a
    case again (lines with that target exist) and the exclusion count is
    zero."""
    conn, site = _build_excluded_dates_fixture()
    note = _note(site_id=site.site_id, h_first=_EXCLUDED_H_FIRST, excluded_h_dates=())
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2034, 1, 1),
        last=date(2034, 1, 1),
        h_first=_EXCLUDED_H_FIRST,
        last_complete=_EXCLUDED_LAST_COMPLETE,
        excluded_h_dates=(),
        note=note,
    )
    header = _header(files)
    assert header["excluded_h_dates_count"] == 0
    lines = _lines(files)
    assert [line for line in lines if line["target"] == "2035-01-10"] != []


# --- 5. Lower bound (M27) ---------------------------------------------------

_LB_FIRST_DAY = date(2035, 1, 1)
_LB_H_FIRST = date(2035, 2, 15)
_LB_LEADS = range(6)


def _build_lower_bound_fixture(
    *, with_stations: bool
) -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """A 45-day training history before ``_LB_H_FIRST``, entirely before
    the H bound, plus one scored lead-0..5 case for ``_LB_H_FIRST`` built
    by production code at each lead. ``with_stations`` seeds matching
    station data over the whole history to force Arm A (G-a clean, G-b a
    full match); without it, Arm B is selected trivially (0 readings)."""
    conn = _connect()
    site = _make_site(conn, "lower-bound-site")
    feed_a = _insert_real_feed(conn, "SYN-a")
    station_id = (
        _insert_station(conn, site_id=site.site_id, pws_station_id="SYN-1")
        if with_stations
        else None
    )

    day = _LB_FIRST_DAY
    while day < _LB_H_FIRST:
        for quantity in _QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=20.0,
            )
        if station_id is not None:
            _insert_station_day(conn, station_id=station_id, local_date=day, value=20.0)
        for lead in _LB_LEADS:
            snapshot_date = day - timedelta(days=lead)
            if snapshot_date >= _LB_FIRST_DAY:
                _insert_filler_record_row(
                    conn,
                    site=site,
                    snapshot_date=snapshot_date,
                    lead=lead,
                    target_date=day,
                )
        day += timedelta(days=1)

    for quantity in _QUANTITIES:
        _insert_daily_truth(
            conn,
            site_id=site.site_id,
            tz_generation_id=site.generation_id,
            local_date=_LB_H_FIRST,
            quantity=quantity,
            value=20.0,
        )

    for lead in _LB_LEADS:
        _build_case_row(
            conn,
            site_id=site.site_id,
            feed_id=feed_a,
            snapshot_date=_LB_H_FIRST - timedelta(days=lead),
            target_date=_LB_H_FIRST,
            value=20.0,
        )
    conn.commit()
    return conn, site


def _assert_lower_bound(files: dict[str, bytes], *, expected_arm: str) -> None:
    lines = _lines(files)
    assert all(date.fromisoformat(line["target"]) >= _LB_H_FIRST for line in lines)
    by_lead = {
        (line["lead"], line["quantity"]): line
        for line in lines
        if line["target"] == _LB_H_FIRST.isoformat()
    }
    for lead in _LB_LEADS:
        for quantity in _QUANTITIES:
            line = by_lead[(lead, quantity)]
            assert line["unmatched"] is None, (lead, quantity, line["unmatched"])
            assert line["training_truth_arm"] == expected_arm
            assert line["training_days"] == 30, (lead, quantity, line["training_days"])


def test_lower_bound_counts_pre_h_first_training_days_arm_a() -> None:
    conn, site = _build_lower_bound_fixture(with_stations=True)
    note = _note(site_id=site.site_id, h_first=_LB_H_FIRST)
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2030, 1, 1),
        last=date(2030, 1, 1),
        h_first=_LB_H_FIRST,
        last_complete=_LB_H_FIRST,
        excluded_h_dates=(),
        note=note,
    )
    assert _header(files)["gates"]["arm"] == "A"
    _assert_lower_bound(files, expected_arm="A")


def test_lower_bound_counts_pre_h_first_training_days_arm_b() -> None:
    conn, site = _build_lower_bound_fixture(with_stations=False)
    note = _note(site_id=site.site_id, h_first=_LB_H_FIRST)
    files = _run(
        conn,
        site=site,
        mode="heldout",
        first=date(2030, 1, 1),
        last=date(2030, 1, 1),
        h_first=_LB_H_FIRST,
        last_complete=_LB_H_FIRST,
        excluded_h_dates=(),
        note=note,
    )
    assert _header(files)["gates"]["arm"] == "B"
    _assert_lower_bound(files, expected_arm="B")
