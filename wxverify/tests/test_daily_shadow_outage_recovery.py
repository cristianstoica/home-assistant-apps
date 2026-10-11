"""Plan §15.3 "Outage recovery" and its two follow-on fixtures (§9.1).

Integration coverage for the rain ``--freeze-prepare`` incumbent-memory
chain (``daily_method.remembered_incumbent``) as built by
``daily_shadow.rain_lines``'s own self-referential S0 chain, driven
end-to-end through ``daily_shadow.main``. Named mutants (not run by this
file): M78, M93.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "d" * 40
_APP_VERSION = "0.16.7"
_FEED_A = "syn-a"
_FEED_B = "syn-b"
_FEED_C = "syn-c"

# Synthetic base date. Day n (n >= 1) is _BASE + (n - 1) days, so day 1 is
# S0 (the first snapshot date with a record row) in every fixture below.
_BASE = date(2034, 1, 1)

# Each fixture's one discriminating day: the station is wet, SYN-a forecasts
# dry (a miss), SYN-b forecasts wet (SYN-c, where present, also forecasts
# dry). _DISC is that day in the outage-recovery and not-eligible fixtures,
# _DISC_NO_ELIGIBLE in the no_eligible fixture. One miss at
# RAIN_MISS_WEIGHT=2.0 is a 2-point penalty gap that stays in every later
# window of these runs -- inside RAIN_SWITCH_MARGIN=3, so an incumbent SYN-a
# is never replaced on score alone.
_DISC = 22
_DISC_NO_ELIGIBLE = 5


def _d(n: int) -> date:
    return _BASE + timedelta(days=n - 1)


def _at_noon(n: int) -> datetime:
    day = _d(n)
    return datetime(day.year, day.month, day.day, 12, tzinfo=UTC)


def _rain_hours(*, wet_count: int) -> list[float]:
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _precip_daily_quantities() -> str:
    return json.dumps(
        {"displayed": {"total_mm": 0.0, "wet_hours": 0.0, "extrema_feed_ids": []}}
    )


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "outage-recovery-site")
        ensure_published_generation(conn, site_id)
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


def _insert_precip_row(
    conn: sqlite3.Connection, *, site: Site, local_date: date
) -> None:
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
            _APP_VERSION,
            _precip_daily_quantities(),
        ),
    )


def _insert_forecast_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    wet_count: int,
) -> None:
    values = _rain_hours(wet_count=wet_count)
    day_before = local_date - timedelta(days=1)
    issued_at = isoformat_utc(
        datetime(day_before.year, day_before.month, day_before.day, 6, tzinfo=UTC)
    )
    fetched_at = isoformat_utc(
        datetime(day_before.year, day_before.month, day_before.day, 6, 5, tzinfo=UTC)
    )
    issued = datetime.fromisoformat(issued_at)
    for hour, value in enumerate(values):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-outage', ?)
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


def _make_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, ?, 0.0, 0.0, 900.0, 1, '2026-01-01T00:00:00.000Z')",
        (site_id, pws_station_id),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _insert_station_day(
    conn: sqlite3.Connection, *, station_id: int, local_date: date, wet_count: int
) -> None:
    values = _rain_hours(wet_count=wet_count)
    cumulative = 0.0
    for hour, value in enumerate(values):
        cumulative += value
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched = valid + timedelta(minutes=5)
        source_raw = f"{round(cumulative, 10)} mm precipTotal"
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw,
                 fetched_at)
            VALUES (?, 'precip', ?, ?, 'ok', ?, ?)
            """,
            (
                station_id,
                isoformat_utc(valid),
                value,
                source_raw,
                isoformat_utc(fetched),
            ),
        )


def _insert_case_day(
    conn: sqlite3.Connection,
    *,
    site: Site,
    station_id: int,
    local_date: date,
    truth_wet_count: int,
    model_wet_counts: dict[int, int],
) -> None:
    """One calendar day: station truth, each listed feed's forecast, and
    the day's own forecast_of_record row."""
    _insert_station_day(
        conn, station_id=station_id, local_date=local_date, wet_count=truth_wet_count
    )
    for feed_id, wet_count in model_wet_counts.items():
        _insert_forecast_day(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=local_date,
            wet_count=wet_count,
        )
    _insert_precip_row(conn, site=site, local_date=local_date)


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _set_deterministic_now(copy: Path, *, mtime: datetime) -> date:
    """Fix the copy's mtime so main()'s ``copied_at`` is deterministic.

    Returns the ``last_complete`` date main() will derive from it.
    """
    stamp = mtime.timestamp()
    os.utime(copy, (stamp, stamp))
    return latest_knowable_target("UTC", mtime)


def _run_main(
    *, copy: Path, out: Path, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[int, str, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _result_payload(out_text: str) -> dict[str, object]:
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    payload = json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])
    return cast(dict[str, object], payload)


def _read_lines_jsonl(run_dir: Path) -> list[dict[str, object]]:
    path = run_dir / "lines.jsonl"
    out: list[dict[str, object]] = []
    for line in path.read_text().splitlines():
        if line.strip():
            out.append(cast(dict[str, object], json.loads(line)))
    return out


def _line_for(
    lines: list[dict[str, object]], *, snapshot_date: date, lead: int
) -> dict[str, object]:
    for entry in lines:
        if (
            entry["snapshot_date"] == snapshot_date.isoformat()
            and entry["lead"] == lead
        ):
            return entry
    raise AssertionError(f"no line for {snapshot_date} lead {lead}")


# --- Fixture 1: outage recovery (gap 31-40) + variant (a) --------------------


def _build_outage_fixture(
    tmp_path: Path, *, name: str, syn_a_covers_day_41: bool
) -> tuple[Path, int]:
    """Record rows on days 1-30 and 41-60; none on 31-40 (the plan's gap).

    One station, two feeds. Every day forecasts dry and is dry except day
    ``_DISC`` (22): the station is wet, SYN-a predicts dry (a miss),
    SYN-b predicts wet. Case 41's lead-0 target is day 41; SYN-a forecasts
    it unless ``syn_a_covers_day_41`` is False, in which case only SYN-b
    covers it (the "no forecast" variant).
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_a = asof_make_real_feed(conn, _FEED_A)
        feed_b = asof_make_real_feed(conn, _FEED_B)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id="SYN-PC-OUTAGE"
        )
        for n in list(range(1, 31)) + list(range(41, 61)):
            truth = 4 if n == _DISC else 0
            b_wet = 4 if n == _DISC else 0
            model_wet_counts = {feed_b: b_wet}
            if n != 41 or syn_a_covers_day_41:
                model_wet_counts[feed_a] = 0
            _insert_case_day(
                conn,
                site=site,
                station_id=station_id,
                local_date=_d(n),
                truth_wet_count=truth,
                model_wet_counts=model_wet_counts,
            )
        conn.commit()
    finally:
        conn.close()
    return copy, site.site_id


def test_outage_recovery_incumbent_carries_across_the_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot 30: SYN-a kept (incumbent, inside the margin). Snapshot 41
    (after the ten-day gap): the same incumbent, carried straight across
    it, still kept. Snapshot 42: the incumbent's own last-selected date
    has advanced to 41; still SYN-a."""
    copy, site_id = _build_outage_fixture(
        tmp_path, name="outage.db", syn_a_covers_day_41=True
    )
    out = tmp_path / "out"
    last_complete = _set_deterministic_now(copy, mtime=_at_noon(43))
    assert last_complete == _d(42)
    exit_code, stdout, stderr = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site_id
    )
    assert exit_code == 0, stderr
    run_dir = out / cast(str, _result_payload(stdout)["run_dir"])
    lines = _read_lines_jsonl(run_dir)

    line30 = _line_for(lines, snapshot_date=_d(30), lead=0)
    selection30 = cast(dict[str, object], line30["selection"])
    assert selection30["selected"] == "example-src/syn-a"
    assert selection30["incumbent_outcome"] == "kept"
    assert selection30["no_eligible"] is False

    line41 = _line_for(lines, snapshot_date=_d(41), lead=0)
    incumbent41 = cast(dict[str, object], line41["incumbent"])
    selection41 = cast(dict[str, object], line41["selection"])
    assert incumbent41["model"] == "example-src/syn-a"
    assert incumbent41["selected_on"] == _d(30).isoformat()
    assert selection41["selected"] == "example-src/syn-a"
    assert selection41["incumbent_outcome"] == "kept"
    # M78 -> first failure at line41["incumbent"]["model"] (correct
    # "example-src/syn-a", mutant None), then line41["selection"]["selected"]
    # (correct "example-src/syn-a", mutant "example-src/syn-b"): with the
    # incumbent dropped across the 31-40 gap, nothing shields SYN-a's 2-point
    # (one-miss) gap on day _DISC, so SYN-b's lower penalty wins outright.

    line42 = _line_for(lines, snapshot_date=_d(42), lead=0)
    incumbent42 = cast(dict[str, object], line42["incumbent"])
    selection42 = cast(dict[str, object], line42["selection"])
    assert incumbent42["model"] == "example-src/syn-a"
    assert incumbent42["selected_on"] == _d(41).isoformat()
    assert selection42["selected"] == "example-src/syn-a"
    assert selection42["incumbent_outcome"] == "kept"
    # SYN-b has the best score, 2 points lower, on every date from 25 on;
    # SYN-a stays selected -- kept by the margin, not as the best.
    for n in [*range(25, 31), 41, 42]:
        line = _line_for(lines, snapshot_date=_d(n), lead=0)
        selection = cast(dict[str, object], line["selection"])
        ranked = cast(list[dict[str, object]], selection["ranked"])
        assert [(r["key"], r["penalty"]) for r in ranked] == [
            ("example-src/syn-b", "0"),
            ("example-src/syn-a", "2"),
        ], n
        assert selection["best"] == "example-src/syn-b", n
        assert selection["selected"] == "example-src/syn-a", n
        assert selection["incumbent_outcome"] == "kept", n


def test_incumbent_replaced_with_no_forecast_after_the_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Variant (a): SYN-a has no forecast at all for case 41's target (day
    41) -- replaced at once, never reaching the margin question; SYN-b
    (the only model covering it) is selected."""
    copy, site_id = _build_outage_fixture(
        tmp_path, name="no-forecast.db", syn_a_covers_day_41=False
    )
    out = tmp_path / "out"
    _set_deterministic_now(copy, mtime=_at_noon(43))
    exit_code, stdout, stderr = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site_id
    )
    assert exit_code == 0, stderr
    run_dir = out / cast(str, _result_payload(stdout)["run_dir"])
    lines = _read_lines_jsonl(run_dir)

    line41 = _line_for(lines, snapshot_date=_d(41), lead=0)
    incumbent41 = cast(dict[str, object], line41["incumbent"])
    selection41 = cast(dict[str, object], line41["selection"])
    assert incumbent41["model"] == "example-src/syn-a"
    assert incumbent41["selected_on"] == _d(30).isoformat()
    assert selection41["incumbent_outcome"] == "replaced_no_forecast"
    assert selection41["selected"] == "example-src/syn-b"


# --- Fixture 2: variant (b), replaced for ineligibility ----------------------


def _build_not_eligible_fixture(tmp_path: Path, *, name: str) -> tuple[Path, int]:
    """Record rows on days 1-30 and 41-60 only; three feeds, one station.

    The station is wet on day ``_DISC`` (22) and dry every other day.
    SYN-a forecasts days 12-30 and 41-60, always dry. SYN-b forecasts
    days 1-29 and 41-60, wet on day 22 and dry otherwise. SYN-c forecasts
    days 1-29 and 41-60, always dry.

    At case 30, SYN-a is the only model forecasting day 30, so it is the
    only covering model: 18 counted days (12-29), a provisional ranking,
    and SYN-a selected -- the incumbent from case 29, SYN-b, has no
    forecast for day 30. At case 41, all three forecast day 41. Counted
    days come from days 1-30: SYN-a 19 (12-30), SYN-b 29, SYN-c 29. Two
    models reach the 20-day minimum, so it turns on and SYN-a -- at 19 --
    is no longer eligible. SYN-b scores 0 and SYN-c scores 2 (it misses
    day 22), so SYN-b is best: the incumbent SYN-a, last selected on 30,
    covers the target but is not ranked -- ``replaced_not_eligible`` --
    and SYN-b is selected.

    SYN-b and SYN-c must not forecast day 30: with record rows only on
    1-30, a model's counted-day total at case 41 differs from its total
    at case 30 only by day 30 itself, so if either covered target 30 with
    its 19+ counted days, SYN-a (18) would be dropped or not eligible at
    case 30 and could not be the incumbent at case 41.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_a = asof_make_real_feed(conn, _FEED_A)
        feed_b = asof_make_real_feed(conn, _FEED_B)
        feed_c = asof_make_real_feed(conn, _FEED_C)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id="SYN-PC-ELIGIBLE"
        )
        for n in list(range(1, 31)) + list(range(41, 61)):
            truth = 4 if n == _DISC else 0
            counts: dict[int, int] = {}
            if 12 <= n <= 30 or 41 <= n <= 60:
                counts[feed_a] = 0
            if n <= 29 or 41 <= n <= 60:
                counts[feed_b] = 4 if n == _DISC else 0
                counts[feed_c] = 0
            _insert_case_day(
                conn,
                site=site,
                station_id=station_id,
                local_date=_d(n),
                truth_wet_count=truth,
                model_wet_counts=counts,
            )
        conn.commit()
    finally:
        conn.close()
    return copy, site.site_id


def test_incumbent_replaced_when_not_eligible_after_the_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Variant (b): SYN-a is the only model covering day 30, so it is
    selected there on a provisional ranking at 18 counted days. At case 41
    SYN-b and SYN-c also cover the target, with 29 counted days each, so the
    minimum is on and SYN-a, at 19, is not eligible -- replaced for
    ineligibility; SYN-b selected."""
    copy, site_id = _build_not_eligible_fixture(tmp_path, name="not-eligible.db")
    out = tmp_path / "out"
    _set_deterministic_now(copy, mtime=_at_noon(42))
    exit_code, stdout, stderr = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site_id
    )
    assert exit_code == 0, stderr
    run_dir = out / cast(str, _result_payload(stdout)["run_dir"])
    lines = _read_lines_jsonl(run_dir)

    line30 = _line_for(lines, snapshot_date=_d(30), lead=0)
    selection30 = cast(dict[str, object], line30["selection"])
    assert selection30["selected"] == "example-src/syn-a"
    incumbent30 = cast(dict[str, object], line30["incumbent"])
    assert incumbent30["model"] == "example-src/syn-b"
    assert selection30["incumbent_outcome"] == "replaced_no_forecast"
    assert selection30["provisional"] is True
    assert selection30["eligible"] == [["example-src/syn-a", 18]]

    line41 = _line_for(lines, snapshot_date=_d(41), lead=0)
    incumbent41 = cast(dict[str, object], line41["incumbent"])
    selection41 = cast(dict[str, object], line41["selection"])
    assert incumbent41["model"] == "example-src/syn-a"
    assert incumbent41["selected_on"] == _d(30).isoformat()
    assert selection41["provisional"] is False
    assert selection41["not_eligible"] == [["example-src/syn-a", 19]]
    assert selection41["incumbent_outcome"] == "replaced_not_eligible"
    assert selection41["selected"] == "example-src/syn-b"


# --- Fixture 3: no_eligible clears the chain ---------------------------------


def _build_no_eligible_fixture(tmp_path: Path, *, name: str) -> tuple[Path, int]:
    """Continuous record rows on days 1-60. Day ``_DISC_NO_ELIGIBLE`` (5) is
    SYN-a's single miss, the same one-miss shape as day ``_DISC`` in the
    outage-recovery fixture. Day 35 carries a record row and station truth
    like every other day, but neither feed forecasts it, so case 35 (lead 0,
    target day 35) has no covering model at all: J=0, a no_eligible clear.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        feed_a = asof_make_real_feed(conn, _FEED_A)
        feed_b = asof_make_real_feed(conn, _FEED_B)
        station_id = _make_station(
            conn, site_id=site.site_id, pws_station_id="SYN-PC-CLEAR"
        )
        for n in range(1, 61):
            truth = 4 if n == _DISC_NO_ELIGIBLE else 0
            b_wet = 4 if n == _DISC_NO_ELIGIBLE else 0
            counts: dict[int, int] = {}
            if n != 35:
                counts = {feed_a: 0, feed_b: b_wet}
            _insert_case_day(
                conn,
                site=site,
                station_id=station_id,
                local_date=_d(n),
                truth_wet_count=truth,
                model_wet_counts=counts,
            )
        conn.commit()
    finally:
        conn.close()
    return copy, site.site_id


def test_no_eligible_clears_the_incumbent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 35 (target day 35) has no covering model: J=0, a no_eligible
    clear. Case 36 then starts with no incumbent, so SYN-a's permanent
    2-point (one-miss) gap -- shielded by the switch margin while it was
    incumbent -- decides the pick outright: SYN-b."""
    copy, site_id = _build_no_eligible_fixture(tmp_path, name="no-eligible.db")
    out = tmp_path / "out"
    _set_deterministic_now(copy, mtime=_at_noon(37))
    exit_code, stdout, stderr = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site_id
    )
    assert exit_code == 0, stderr
    run_dir = out / cast(str, _result_payload(stdout)["run_dir"])
    lines = _read_lines_jsonl(run_dir)

    line35 = _line_for(lines, snapshot_date=_d(35), lead=0)
    selection35 = cast(dict[str, object], line35["selection"])
    assert selection35["no_eligible"] is True
    assert selection35["selected"] is None
    incumbent35 = cast(dict[str, object], line35["incumbent"])
    assert incumbent35["model"] == "example-src/syn-a"
    assert incumbent35["selected_on"] == _d(34).isoformat()
    not_covering = cast(list[list[object]], selection35["not_covering"])
    assert [entry[0] for entry in not_covering] == [
        "example-src/syn-a",
        "example-src/syn-b",
    ]

    line36 = _line_for(lines, snapshot_date=_d(36), lead=0)
    selection36 = cast(dict[str, object], line36["selection"])
    incumbent36 = cast(dict[str, object], line36["incumbent"])
    assert incumbent36["model"] is None
    assert incumbent36["selected_on"] is None
    assert incumbent36["unrecognised_on"] is None
    assert line36["incumbent_row_unrecognised"] is False
    assert selection36["selected"] == "example-src/syn-b"
    # M93 -> first failure at line36["incumbent"]["model"] (correct None,
    # mutant "example-src/syn-a"), then line36["selection"]["selected"]
    # (correct "example-src/syn-b", mutant "example-src/syn-a"). Site: the S0
    # chain's write in daily_shadow.rain_lines, where a no_eligible case
    # records RecordDecision("clear"); the mutant records a gap (or nothing),
    # so the look-back at 36 reads through 35 to SYN-a's selection on 34, and
    # SYN-a's 2-point gap is inside the 3-point margin. The same mutation in
    # daily_method.record_decision is M93b: this run does not reach it; the
    # unit test in test_daily_method_selector_memory.py and the
    # --incumbent-from-record integration case are expected to kill it.
