"""Date-bound pins for ``daily_shadow``: the upper choosing-set bound
(M26), ``main()``'s per-variable bounds (M79), and T2's own, independent
last day."""

from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast import daily_method
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
    """A synthetic site at lat/lon 0.0; never ``asof_make_site``."""
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


def _insert_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed, fetch_interval_minutes)
        VALUES ('SYN', ?, 1, 60)
        """,
        (model,),
    )
    return cast(int, cur.lastrowid)


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
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
        VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 'UTC', 0, ?)
        """,
        (
            site_id,
            local_date.isoformat(),
            quantity,
            value,
            day_start,
            day_end,
            tz_generation_id,
        ),
    )


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
    """One feed's 24 hourly temperature samples for ``local_date``."""
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
    """One scored lead-0 row built entirely by production code (passes F1
    by construction, since the replay reads the same ``forecast_samples``).
    ``snapshot_date == target_date`` throughout this file, so this is
    always the lead-0 case for its own snapshot date."""
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


def _spy_composers(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Wraps all four candidate composers with a call-through recorder, so
    production scoring still runs and every call is counted by name."""
    calls: list[str] = []
    for name in ("compose_iw", "compose_td_k", "compose_iw_bc", "compose_td_k_bc"):
        real = cast(
            "Callable[..., daily_method.CandidateValue]", getattr(daily_method, name)
        )

        def _wrapped(
            *args: object,
            _real: Callable[..., daily_method.CandidateValue] = real,
            _name: str = name,
            **kwargs: object,
        ) -> daily_method.CandidateValue:
            calls.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(daily_method, name, _wrapped)
    return calls


# --- 1. Choosing-set upper bound excludes targets after `last` (M26) --------------

_T1_SITE = "date-bounds-upper-site"
_IN_BOUND = date(2035, 1, 31)
_ONE_DAY_LATE = date(2035, 2, 1)
_FAR_LATE = date(2035, 2, 5)
_T1_TARGETS = (_IN_BOUND, _ONE_DAY_LATE, _FAR_LATE)


def _build_upper_bound_fixture(
    targets: tuple[date, ...],
) -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """One lead-0 case per ``target``, each its own snapshot date equal to
    its target. Called with ``_T1_TARGETS`` (the bound ``_IN_BOUND``, one
    day past it ``_ONE_DAY_LATE`` -- M26's leak --, and further past it
    ``_FAR_LATE``) and, for the paired composer-call baseline below, with
    only ``(_IN_BOUND,)``. No training-window data at all; F1 passes by
    construction (``_build_case_row``) regardless."""
    conn = _connect()
    site = _make_site(conn, _T1_SITE)
    feed_id = _insert_real_feed(conn, "SYN-upper")
    for target in targets:
        for quantity in _QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                tz_generation_id=site.generation_id,
                local_date=target,
                quantity=quantity,
                value=20.0,
            )
        _build_case_row(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            snapshot_date=target,
            target_date=target,
            value=20.0,
        )
    conn.commit()
    return conn, site


def test_upper_bound_excludes_targets_after_last_and_their_composer_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, site = _build_upper_bound_fixture(_T1_TARGETS)
    calls = _spy_composers(monkeypatch)

    files = daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="freeze-prepare",
        first=date(2035, 1, 1),
        last=_IN_BOUND,
        h_first=date(2036, 1, 1),
        last_complete=date(2030, 1, 1),
        excluded_h_dates=(),
        t2_last=None,
        note=None,
        house_station="SYN-HOUSE",
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )
    full_run_calls = len(calls)
    lines = _lines(files)

    late_targets = {_ONE_DAY_LATE.isoformat(), _FAR_LATE.isoformat()}
    leaked = [line for line in lines if line["target"] in late_targets]
    # M26 -> at this assertion: correct = [], mutant = non-empty (at least
    # the _ONE_DAY_LATE case's two quantity lines land in `lines`, since the
    # bound is read one day late and that case has a real recorded row).
    assert leaked == []

    in_bound = [line for line in lines if line["target"] == _IN_BOUND.isoformat()]
    # Paired positive: the case ON the bound is not itself suppressed by
    # whatever excludes the late ones, so the absence above is not vacuous.
    assert len(in_bound) == len(_QUANTITIES)

    with pytest.MonkeyPatch.context() as baseline_mp:
        baseline_conn, baseline_site = _build_upper_bound_fixture((_IN_BOUND,))
        baseline_calls = _spy_composers(baseline_mp)
        daily_shadow.run(
            baseline_conn,
            site=baseline_site,
            variable="temperature",
            mode="freeze-prepare",
            first=date(2035, 1, 1),
            last=_IN_BOUND,
            h_first=date(2036, 1, 1),
            last_complete=date(2030, 1, 1),
            excluded_h_dates=(),
            t2_last=None,
            note=None,
            house_station="SYN-HOUSE",
            incumbent_from_record=False,
            failed_dirs=(),
            header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
        )

    # Paired run: the choosing-set `cases` that reaches every composer call
    # is built once, upstream of `_temperature_freeze_prepare`
    # (daily_shadow.py:6342-6350), then reused unchanged across that
    # function's several passes over `temperature_lines`
    # (:6376, :6383, :6391-6392 via `recompute`) -- so the raw call count is
    # a product of the pass count (which varies with K-recomposition and
    # `temperature_tradeoffs`'s parameter sweep) and is not a fixed multiple
    # of `_QUANTITIES`. A baseline run built from ONLY the in-bound case
    # takes the same `first`/`last` bound and so runs the exact same
    # passes; if the upper bound correctly excludes the late cases from
    # `cases`, the full run above makes exactly as many composer calls as
    # this baseline, case for case and pass for pass. `full_run_calls` is
    # taken right after the full run (above) because `calls`' outer spy
    # stays active through the baseline's own `pytest.MonkeyPatch.context`,
    # so every baseline composer call also lands in `calls` -- reading
    # `len(calls)` after the baseline would double-count them.
    # M26 -> at this assertion: correct = baseline_calls (same `cases`
    # reach every pass in both runs), mutant = more than baseline_calls (the
    # full run's `cases` also includes _ONE_DAY_LATE, scored across every
    # one of the same passes).
    assert len(baseline_calls) > 0
    assert full_run_calls == len(baseline_calls)


# --- 2. main() gives each variable its own date bounds (M79) ----------------------

_M79_PRECIP_S0 = date(2026, 5, 1)
_M79_SNAPSHOT_UTC = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)
_M79_PRECIP_LAST = date(2026, 9, 9)  # hand-derived: hour 12 >= lag 3h -> D-1


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, daily_shadow.Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = _insert_site(conn, "m79-site")
        ensure_published_generation(conn, site_id)
        set_setting(conn, "forecast_blend_depth", "1")
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site


def _insert_precip_record_row(
    conn: sqlite3.Connection, *, site: daily_shadow.Site, snapshot_date: date
) -> None:
    """A minimal ``recorded`` precip row: only its identity and presence
    matter, so ``rain_s0`` (a plain ``MIN(snapshot_local_date)``) has a
    known literal answer; ``run`` is monkeypatched out in this test, so no
    further scoring of this row ever happens."""
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", snapshot_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, methodology_version, app_version)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', 1, '0.16.7')
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
        ),
    )


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def test_main_passes_each_variables_own_date_bounds_to_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = _make_copy(tmp_path, name="m79.sqlite3")
    conn = sqlite3.connect(copy)
    try:
        _insert_precip_record_row(conn, site=site, snapshot_date=_M79_PRECIP_S0)
        conn.commit()
    finally:
        conn.close()
    os.utime(copy, (_M79_SNAPSHOT_UTC.timestamp(), _M79_SNAPSHOT_UTC.timestamp()))

    calls: list[dict[str, object]] = []

    def _recorder(conn: sqlite3.Connection, **kwargs: object) -> dict[str, bytes]:
        del conn
        calls.append(kwargs)
        return {"header.json": b"{}"}

    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "run", _recorder)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))

    def _invoke(variable: str) -> None:
        argv = [
            "--copy",
            str(copy),
            "--site-id",
            str(site.site_id),
            "--out",
            str(tmp_path / f"out-{variable}"),
            "--variable",
            variable,
            "--commit-sha",
            "b" * 40,
            "--freeze-prepare",
            "--house-station",
            "SYN-HOUSE",
        ]
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = daily_shadow.main(argv)
        assert exit_code == 0, stdout.getvalue() + stderr.getvalue()

    _invoke("temperature")
    _invoke("precip")

    assert len(calls) == 2
    temperature_kwargs, precip_kwargs = calls

    assert temperature_kwargs["first"] == date(2026, 7, 25)
    assert temperature_kwargs["last"] == date(2026, 9, 7)

    # M79 -> at these two assertions: correct = (2026-05-01, 2026-09-09),
    # mutant = (2026-07-25, 2026-09-07) -- temperature's own choosing-set
    # bounds, since the fixture's S0 and hand-derived last_complete are
    # constructed to differ from CHOOSING_FIRST/CHOOSING_LAST.
    assert precip_kwargs["first"] == _M79_PRECIP_S0
    assert precip_kwargs["last"] == _M79_PRECIP_LAST

    # M79 -> at this assertion: correct = 2026-09-07 (t2_last always tracks
    # CHOOSING_LAST for both variables; unaffected by the bug, included so
    # the pair above is not read as "nothing passed through correctly").
    assert precip_kwargs["t2_last"] == date(2026, 9, 7)


# --- 3. T2's own last day is independent of the case-selection bound --------------

_T3_SITE = "date-bounds-t2-site"
_T3_FIRST = date(2035, 1, 1)
_T3_LAST = date(2035, 1, 31)
_T3_T2_LAST = date(2035, 1, 20)
_T3_TRUTH_LAST = date(2035, 2, 5)
_T3_APP_VERSION = "0.16.7"


def _rain_hours(*, wet_count: int) -> list[float]:
    """``wet_count`` hours at 0.25 mm, then zeros, for a 24-hour day."""
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _precip_daily_quantities(*, total_mm: float, wet_hours: float) -> str:
    return json.dumps({"displayed": {"total_mm": total_mm, "wet_hours": wet_hours}})


def _insert_precip_case_row(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    feed_id: int,
    local_date: date,
) -> None:
    """One dry (all-zero) lead-0 precip case at ``local_date``: a recorded
    row plus matching ``forecast_samples`` so the F1 replay reproduces
    ``total_mm=0.0`` exactly (0 wet hours x 0.25mm)."""
    issued_day = local_date - timedelta(days=1)
    issued_at = isoformat_utc(
        datetime(issued_day.year, issued_day.month, issued_day.day, 6, tzinfo=UTC)
    )
    fetched_at = isoformat_utc(
        datetime(issued_day.year, issued_day.month, issued_day.day, 6, 5, tzinfo=UTC)
    )
    issued = datetime.fromisoformat(issued_at)
    for hour, value in enumerate(_rain_hours(wet_count=0)):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-t2', ?)
            """,
            (
                site.site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", local_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', '{"rain_threshold_mm": 0.2}', 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            local_date.isoformat(),
            snapshot_utc,
            local_date.isoformat(),
            _T3_APP_VERSION,
            _precip_daily_quantities(total_mm=0.0, wet_hours=0.0),
        ),
    )


def _insert_station_hours_dry(
    conn: sqlite3.Connection, *, station_id: int, local_date: date
) -> None:
    for hour in range(24):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched = valid + timedelta(minutes=5)
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw,
                 fetched_at)
            VALUES (?, 'precip', ?, 0.0, 'ok', '0.0 mm precipTotal', ?)
            """,
            (station_id, isoformat_utc(valid), isoformat_utc(fetched)),
        )


def _build_t2_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """One dry lead-0 precip case at the upper end of the selection window
    (``_T3_LAST``, the paired positive proving the run still reaches that
    far); daily truth (occurrence + total) for every day from ``_T3_FIRST``
    through ``_T3_TRUTH_LAST`` -- 15 days further than ``_T3_T2_LAST`` and
    5 days further than ``_T3_LAST``, so neither bound is read past by
    truth availability alone."""
    conn = _connect()
    site = _make_site(conn, _T3_SITE)
    feed_id = _insert_real_feed(conn, "SYN-t2")
    station_id = conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)
        VALUES (?, 'SYN-T2-1', 0.0, 0.0, 100.0, 1, '2030-01-01T00:00:00.000Z')
        """,
        (site.site_id,),
    ).lastrowid
    assert station_id is not None
    _insert_station_hours_dry(conn, station_id=int(station_id), local_date=_T3_LAST)

    day = _T3_FIRST
    while day <= _T3_TRUTH_LAST:
        for quantity in ("precip_occurrence", "precip_total"):
            _insert_daily_truth(
                conn,
                site_id=site.site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=0.0,
            )
        day += timedelta(days=1)

    _insert_precip_case_row(conn, site=site, feed_id=feed_id, local_date=_T3_LAST)
    conn.commit()
    return conn, site


def test_t2_last_day_bounds_only_t2_independently_of_case_selection() -> None:
    conn, site = _build_t2_fixture()

    files = daily_shadow.run(
        conn,
        site=site,
        variable="precip",
        mode="freeze-prepare",
        first=_T3_FIRST,
        last=_T3_LAST,
        h_first=None,
        last_complete=_T3_LAST,
        excluded_h_dates=(),
        t2_last=_T3_T2_LAST,
        note=None,
        house_station=None,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )

    lines = _lines(files)
    targets = {line["target"] for line in lines}
    # Paired positive: case selection still reaches _T3_LAST (2035-01-31),
    # well past t2_last (2035-01-20) -- the two bounds are not conflated.
    assert _T3_LAST.isoformat() in targets
    assert all(date.fromisoformat(t) <= _T3_LAST for t in targets)

    # A second, independent fixture with t2_last widened to _T3_LAST: the
    # paired positive for the two mutants checked below.
    conn_wide, site_wide = _build_t2_fixture()
    files_wide = daily_shadow.run(
        conn_wide,
        site=site_wide,
        variable="precip",
        mode="freeze-prepare",
        first=_T3_FIRST,
        last=_T3_LAST,
        h_first=None,
        last_complete=_T3_LAST,
        excluded_h_dates=(),
        t2_last=_T3_LAST,
        note=None,
        house_station=None,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )

    narrow = cast("dict[str, Any]", _header(files)["t2"])
    wide = cast("dict[str, Any]", _header(files_wide)["t2"])
    assert narrow["last_day"] == _T3_T2_LAST.isoformat()
    # Mutant (a): the T2 truth read's "local_date <= ?" bound
    # (daily_shadow.py:5162). Dropped: narrow["days"] is 36 and the
    # 2035-01-31 station day is counted. Shifted one day late:
    # narrow["days"] is 21 (no reading on 01-21, so "stations" stays {}).
    assert narrow["days"] == 20  # truth days 2035-01-01..01-20
    assert narrow["stations"] == {}  # the only readings are on 2035-01-31
    assert wide["days"] == 31  # paired positive, T2 bounded at 01-31
    assert wide["stations"]["SYN-T2-1"]["days"] == 1  # 01-31 reading counted
    # Mutant (b): t2_last leaking into the case's own station read
    # (daily_shadow.py:6655) would change the case lines between the two
    # t2_last values; it doesn't -- t2_last is read only at the t2_report
    # call (daily_shadow.py:6768).
    assert files["lines.jsonl"] == files_wide["lines.jsonl"]
