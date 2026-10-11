"""§15.3 "changed header at the verdict": the changed-header rule applied
to a run whose own data checks already fail (§15.1 synthetic fixture).

``changed_header_outcome`` (daily_shadow.py:1194-1212) is applied last to
every candidate's outcome, INSUFFICIENT included: ``db_import_changed=True``
overrides first, then any non-empty ``station_set_changed`` list, else the
outcome passes through unchanged. ``test_daily_shadow_twin_rule.py:178-190``
already covers the override on a NOT_ASSESSED outcome (``twin_missing``);
this file covers it on an INSUFFICIENT outcome produced by a genuine F1
failure, run through the real pipeline (``daily_shadow.run(mode="heldout")``)
rather than a direct ``candidate_outcome`` call, and proves ``lines.jsonl``
never changes with the note (``lines.jsonl`` is built from ``lines``,
daily_shadow.py:6604-6606; ``lines`` reads the note only through
``note.td_k``, daily_shadow.py:6464, which all three notes share via
``_note``; the station set and ``db_import`` reach only ``header_facts``,
daily_shadow.py:5942-5956, and from there the header and
``candidate_outcome``, daily_shadow.py:6507-6508).

The fixture is a site at latitude 0.0 / longitude 0.0, timezone ``UTC``
(§15.1), with 20 record rows across 20 distinct 2035 dates built by
``build_forecast_record`` (the production path, so each row passes F1 "by
construction", per ``test_daily_shadow_run_checks.py:143-144``) and then 2
of the 20 forced to fail F1 by deleting their underlying
``forecast_samples`` afterwards, so the replay at verification time finds
none of the row's ``extrema_feed_ids`` (``f1_ok``, daily_shadow.py:1609-1617:
``any(feed_id not in feeds for feed_id in ids)``). 18/20 matched is below
the 95% bar (``F1_MIN_PERCENT = 95``, daily_shadow.py:189), so
``F1Tally.passes()`` is ``False`` and ``data_checks()`` reports
``f1_pass=False``; with only 20 record dates total,
``H_MIN_DATES_PER_LEAD = 60`` (daily_shadow.py:211) is never reached, so
``minimums_met`` is also always ``False`` regardless of F1. The fixture
needs no ``daily_truth`` rows at all (``_truth_reason(None)`` at
daily_shadow.py:2590-2597 returns ``"truth_absent"`` rather than raising,
so an all-unscored run still completes; the zero-case-row precedent at
``test_daily_shadow_run_checks.py`` (db_import_changed, noted-absent test)
confirms the full ``_temperature_heldout`` path tolerates no truth data)
-- but with no truth, no line is ``scored``, so every bootstrap resample's
quantity/lead group has zero cases and the resample is incomplete
(daily_shadow.py:2210-2227: ``n == 0`` sets ``complete = False``), leaving
``kept=0`` and ``discarded`` equal to every resample drawn. ``discard_ok``
(daily_shadow.py:2235-2238: ``100 * discarded <= MAX_DISCARD_SHARE_PERCENT
* (kept + discarded)``) is then ``False``, so ``candidate_outcome``
(daily_shadow.py:3036-3042) appends all three data-check reasons, in this
order: ``"f1_below_95"``, ``"minimums_not_met"``,
``"discarded_over_1_percent"``.

The three notes below share one connection and one set of record rows, so
every difference in a run's header traces to the note alone, never to a
rebuilt database:

(a) negative control -- the note matches the copy exactly (station set and
    db_import both equal to what is actually in the database) -- INSUFFICIENT.
(b) the note's station set shows SYN-2 disabled while the copy has it
    enabled (db_import still matches, isolating the station-set change) --
    ``station_set_changed`` lists SYN-2 under "toggled" -- INCONCLUSIVE.
(c) the note's db_import is null while the copy has a row (station set
    still matches, isolating the db_import change) -- ``db_import_changed``
    is true -- INCONCLUSIVE. This is the M41 kill: a mutant that reads only
    ``station_set_changed`` sees every list empty and leaves the outcome at
    INSUFFICIENT.

A fourth test calls ``changed_header_outcome`` directly over all four
outcome literals, confirming the same override and that M41 fails the
db_import-only sub-case there too.

Mutant ledger (§15.4):

M41 (``changed_header_outcome`` reads only ``station_set_changed``, never
``db_import_changed``) -- implemented: yes; reviewed: pending;
mutation-verified: pending (checkpoint C, §15.4:1448). Killed by case (c) in
``test_changed_header_overrides_f1_insufficient_outcome`` (every candidate's
outcome stays INCONCLUSIVE only under the correct code) and by
``test_changed_header_outcome_db_import_changed_forces_inconclusive`` with
outcome in {"PASS", "FAIL", "INSUFFICIENT"} (the "INCONCLUSIVE" input case
is equivalent under this oracle alone, since the mutant would also pass
it through unchanged).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import set_setting
from wxverify.verification import daily_shadow
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc

_TIMEZONE = "UTC"
_BASE_DATE = date(2035, 6, 1)
_ROWS = 20
_MISMATCH_COUNT = 2
_OUTPUT_FILES = frozenset({"header.json", "lines.jsonl", "house_station.json"})

_EMPTY_STATION_CHANGES: dict[str, list[str]] = {
    "added": [],
    "removed": [],
    "toggled": [],
    "recreated": [],
}
_REASONS_INSUFFICIENT: list[str] = [
    "f1_below_95",
    "minimums_not_met",
    "discarded_over_1_percent",
]
_REASONS_OVERRIDDEN: list[str] = [
    "f1_below_95",
    "minimums_not_met",
    "discarded_over_1_percent",
    "station_set_or_import_changed",
]


def _header(files: dict[str, bytes]) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(files["header.json"]))


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


def _insert_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed, fetch_interval_minutes)"
        " VALUES ('SYN', ?, 1, 60)",
        (model,),
    )
    return cast(int, cur.lastrowid)


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
    """One feed's 24 hourly samples for ``local_date`` (the heldout-wiring pattern,
    ``test_daily_shadow_run_checks.py:98-131``)."""
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
            VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-ch', ?)
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
    construction, since the replay reads the same ``forecast_samples``) --
    identical to ``test_daily_shadow_run_checks.py:134-158``."""
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


def _build_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site, list[date]]:
    """20 F1-counted rows across 20 dates, exactly 2 of which fail F1 by
    deleting their underlying samples after the row is built (so the row's
    ``extrema_feed_ids`` are absent from the replay, forcing the
    ``f1_ok`` mismatch branch at daily_shadow.py:1616-1617) -- no
    ``daily_truth`` data at all (see module docstring)."""
    conn = _connect()
    site = _make_site(conn, "changed-header-site")
    feed_id = _insert_real_feed(conn, "SYN-ch")
    for pws_station_id in ("SYN-1", "SYN-2", "SYN-3"):
        _insert_station(conn, site_id=site.site_id, pws_station_id=pws_station_id)
    dates = [_BASE_DATE + timedelta(days=i) for i in range(_ROWS)]
    for local_date in dates:
        _build_case_row(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            snapshot_date=local_date,
            target_date=local_date,
            value=20.0,
        )
    for mismatched_date in dates[-_MISMATCH_COUNT:]:
        conn.execute(
            "DELETE FROM forecast_samples"
            " WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'"
            " AND valid_at LIKE ?",
            (site.site_id, feed_id, f"{mismatched_date.isoformat()}%"),
        )
    set_runtime_state(conn, "import_rebuild_state", "done")
    conn.commit()
    return conn, site, dates


def _note(
    *,
    site_id: int,
    h_first: date,
    station_set: tuple[daily_shadow.StationEntry, ...],
    db_import: dict[str, str] | None,
) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=site_id,
        first_h_date=h_first,
        excluded_h_dates=(),
        station_set=station_set,
        db_import=db_import,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _run_heldout(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    dates: list[date],
    note: daily_shadow.FreezeNote,
) -> dict[str, bytes]:
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=dates[0],
        last=dates[-1],
        h_first=dates[0],
        last_complete=dates[-1],
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


def _matching_db_import(conn: sqlite3.Connection) -> dict[str, str]:
    row = conn.execute(
        "SELECT value, updated_at FROM runtime_state WHERE key = 'import_rebuild_state'"
    ).fetchone()
    return {
        "value": cast(str, row["value"]),
        "updated_at": cast(str, row["updated_at"]),
    }


def test_changed_header_overrides_f1_insufficient_outcome() -> None:
    """(a)-(c): the changed-header rule overrides a genuine F1 failure's
    INSUFFICIENT outcome to INCONCLUSIVE, and ``lines.jsonl`` never changes
    with it."""
    conn, site, dates = _build_fixture()
    matching_station_set = tuple(
        daily_shadow.load_station_set(conn, site_id=site.site_id)
    )
    matching_db_import = _matching_db_import(conn)

    # (a) negative control: the note matches the copy exactly.
    note_a = _note(
        site_id=site.site_id,
        h_first=dates[0],
        station_set=matching_station_set,
        db_import=matching_db_import,
    )
    files_a = _run_heldout(conn, site=site, dates=dates, note=note_a)
    header_a = _header(files_a)

    f1 = header_a["data_checks"]["f1"]
    assert f1["rows"] == 20
    assert f1["matched"] == 18
    assert f1["failed"] == [[d.isoformat(), 0] for d in dates[-_MISMATCH_COUNT:]]
    assert header_a["data_checks"]["f1_pass"] is False
    assert header_a["data_checks"]["minimums_met"] is False
    assert header_a["station_set_changed"] == _EMPTY_STATION_CHANGES
    assert header_a["db_import_changed"] is False
    for name in daily_shadow.TEMPERATURE_CANDIDATES:
        outcome = header_a["candidates"][name]["outcome"]
        assert outcome["outcome"] == "INSUFFICIENT"
        assert outcome["reasons"] == _REASONS_INSUFFICIENT
    assert header_a["verdict"] == "INSUFFICIENT"

    # (b) the note's station set shows SYN-2 disabled while the copy has it
    # enabled; db_import still matches, isolating the station-set change.
    toggled_station_set = tuple(
        (pws_station_id, False, created_at)
        if pws_station_id == "SYN-2"
        else (pws_station_id, enabled, created_at)
        for pws_station_id, enabled, created_at in matching_station_set
    )
    note_b = _note(
        site_id=site.site_id,
        h_first=dates[0],
        station_set=toggled_station_set,
        db_import=matching_db_import,
    )
    files_b = _run_heldout(conn, site=site, dates=dates, note=note_b)
    header_b = _header(files_b)
    assert set(files_b) == set(files_a) == _OUTPUT_FILES

    assert header_b["station_set_changed"] == {
        "added": [],
        "removed": [],
        "toggled": ["SYN-2"],
        "recreated": [],
    }
    assert header_b["db_import_changed"] is False
    for name in daily_shadow.TEMPERATURE_CANDIDATES:
        outcome = header_b["candidates"][name]["outcome"]
        assert outcome["outcome"] == "INCONCLUSIVE"
        assert outcome["reasons"] == _REASONS_OVERRIDDEN
    assert header_b["verdict"] == "INCONCLUSIVE"
    assert files_b["lines.jsonl"] == files_a["lines.jsonl"]

    # (c) the note's db_import is null while the copy has a row; the
    # station set still matches, isolating the db_import change. This is
    # the M41 kill.
    note_c = _note(
        site_id=site.site_id,
        h_first=dates[0],
        station_set=matching_station_set,
        db_import=None,
    )
    files_c = _run_heldout(conn, site=site, dates=dates, note=note_c)
    header_c = _header(files_c)
    assert set(files_c) == set(files_a) == _OUTPUT_FILES

    assert header_c["station_set_changed"] == _EMPTY_STATION_CHANGES
    assert header_c["db_import_changed"] is True
    for name in daily_shadow.TEMPERATURE_CANDIDATES:
        outcome = header_c["candidates"][name]["outcome"]
        # M41 -> at this assertion: correct = "INCONCLUSIVE", mutant =
        # "INSUFFICIENT" -- forced by db_import_changed=True with every
        # station_set_changed list empty (_EMPTY_STATION_CHANGES above), which
        # a mutant reading only station_set_changed treats as "nothing
        # changed" and leaves the INSUFFICIENT outcome untouched.
        assert outcome["outcome"] == "INCONCLUSIVE"
        assert outcome["reasons"] == _REASONS_OVERRIDDEN
    assert header_c["verdict"] == "INCONCLUSIVE"
    assert files_c["lines.jsonl"] == files_a["lines.jsonl"]


@pytest.mark.parametrize("outcome", ["PASS", "FAIL", "INCONCLUSIVE", "INSUFFICIENT"])
def test_changed_header_outcome_toggled_station_forces_inconclusive(
    outcome: str,
) -> None:
    """(d) with SYN-2 in "toggled" only, every outcome becomes INCONCLUSIVE."""
    changed = daily_shadow.changed_header_outcome(
        outcome,
        station_set_changed={
            "added": [],
            "removed": [],
            "toggled": ["SYN-2"],
            "recreated": [],
        },
        db_import_changed=False,
    )
    assert changed == "INCONCLUSIVE"


@pytest.mark.parametrize("outcome", ["PASS", "FAIL", "INCONCLUSIVE", "INSUFFICIENT"])
def test_changed_header_outcome_db_import_changed_forces_inconclusive(
    outcome: str,
) -> None:
    """(d) with four empty lists and db_import_changed=True, every outcome
    becomes INCONCLUSIVE.

    M41 -> at the assertion below: correct = "INCONCLUSIVE", mutant =
    ``outcome`` (the input, unchanged) for outcome in {"PASS", "FAIL",
    "INSUFFICIENT"} -- forced by every station_set_changed list being
    empty, which a mutant reading only station_set_changed reads as
    "nothing changed" and passes the given outcome straight through. For
    outcome == "INCONCLUSIVE" the two literals coincide, so that one
    parametrized case alone does not discriminate the mutant; the other
    three do.
    """
    changed = daily_shadow.changed_header_outcome(
        outcome,
        station_set_changed={
            "added": [],
            "removed": [],
            "toggled": [],
            "recreated": [],
        },
        db_import_changed=True,
    )
    assert changed == "INCONCLUSIVE"


@pytest.mark.parametrize("outcome", ["PASS", "FAIL", "INCONCLUSIVE", "INSUFFICIENT"])
def test_changed_header_outcome_unchanged_when_nothing_changed(outcome: str) -> None:
    """Paired positive: with nothing changed, the outcome passes through
    unchanged -- without this, the two tests above could be satisfied by a
    ``changed_header_outcome`` that always returns "INCONCLUSIVE"."""
    changed = daily_shadow.changed_header_outcome(
        outcome,
        station_set_changed={
            "added": [],
            "removed": [],
            "toggled": [],
            "recreated": [],
        },
        db_import_changed=False,
    )
    assert changed == outcome
