"""One rain trigger (plan §15.3): a precip ``--freeze-prepare`` run over two
synthetic models and one truth station, with 20 shared training days (8
rain-nearby, 12 dry) and a target day where the two models disagree.

SYN-a's training record has 4 misses at the 0.2 mm trigger (score 8/20,
own-best 0 misses at 0.1 mm); SYN-b has 1 miss at every grid trigger (score
2/20 everywhere), so the served ranking (always the single 0.2 mm trigger,
``daily_method.RAIN_TRIGGER_HUNDREDTHS``) and A15's own-lowest grid ranking
land on opposite firsts -- exactly the fixture needed to separate the two
rankings. Mutant M63 (the served ranking scores each model at its own
lowest-scoring grid value instead of the one 0.2 mm trigger) is named here
for the record; this file does not plant or run it.

The served ranking, S-PICK and A15's own-lowest grid are read from the
run's own JSON output -- ``lines.jsonl`` and ``trigger_grid.jsonl`` -- so
those tests exercise ``rain_line_json`` and ``trigger_grid_json`` as well
as the selector and the serving step. The one-trigger test reads call
recorders on ``select_model``, ``group_vote_chance`` and ``served_rain``.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast import daily_method
from wxverify.verification import daily_shadow
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "a" * 40
APP_VERSION = "0.16.7"

TARGET_DATE = date(2035, 9, 1)
TRAINING_DAYS: tuple[date, ...] = tuple(
    TARGET_DATE - timedelta(days=n) for n in range(20, 0, -1)
)
RAIN_NEARBY_DAYS: tuple[date, ...] = TRAINING_DAYS[:8]
DRY_DAYS: tuple[date, ...] = TRAINING_DAYS[8:]

MODEL_A = "syn-a"
MODEL_B = "syn-b"
KEY_A = f"example-src/{MODEL_A}"
KEY_B = f"example-src/{MODEL_B}"

_CALLS_PER_CASE = 2 + len(daily_method.TRIGGER_GRID_HUNDREDTHS)
"""``select_model`` calls per case with a ``recorded`` row under
``--freeze-prepare``: 1 served, then A15's grid (1 ``own_lowest`` + one per
fixed grid value). A case with no row or a non-recorded row makes none."""


def _syn_a_value(day: date) -> float:
    if day in RAIN_NEARBY_DAYS[:4]:
        return 0.15
    if day in RAIN_NEARBY_DAYS[4:]:
        return 1.0
    return 0.0


def _syn_b_value(day: date) -> float:
    if day in RAIN_NEARBY_DAYS[:7]:
        return 1.0
    return 0.0


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path) -> tuple[Path, daily_shadow.Site, int, int]:
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "one-rain-trigger-site")
        ensure_published_generation(conn, site_id)
        feed_a = asof_make_real_feed(conn, MODEL_A)
        feed_b = asof_make_real_feed(conn, MODEL_B)
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site, feed_a, feed_b


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    snapshot_date: date,
    app_version: str,
    daily_quantities: str | None,
    policy: str = '{"rain_threshold_mm": 0.2}',
) -> None:
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", snapshot_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', ?, 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
            policy,
            app_version,
            daily_quantities,
        ),
    )


def _insert_precip_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    values: list[float],
) -> None:
    assert len(values) == 24
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
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
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-x', ?)
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


def _insert_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, ?, 40.0, -105.0, 900.0)
        """,
        (site_id, pws_station_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_station_day(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    daily_total_mm: float,
) -> None:
    """24 ``ok`` hourly readings for one UTC-aligned local day: the whole
    total as a single hour-0 increment, 0.0 at every other hour, and a
    running-total ``source_raw`` that never drops and reconciles with the
    increments exactly -- never a ``counter_reset``/``unreconciled`` day.
    """
    cumulative = 0.0
    for hour in range(24):
        increment = daily_total_mm if hour == 0 else 0.0
        cumulative += increment
        valid_at = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        fetched_at = valid_at + timedelta(minutes=5)
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw,
                 fetched_at)
            VALUES (?, 'precip', ?, ?, 'ok', ?, ?)
            """,
            (
                station_id,
                isoformat_utc(valid_at),
                increment,
                f"{round(cumulative, 10)} mm precipTotal",
                isoformat_utc(fetched_at),
            ),
        )


def _issued_at_for(local_date: date) -> str:
    return isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
        - timedelta(hours=18)
    )


def _insert_case_day(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    feed_a: int,
    feed_b: int,
    local_date: date,
    value_a: float,
    value_b: float,
) -> None:
    _insert_precip_row(
        conn,
        site=site,
        snapshot_date=local_date,
        app_version=APP_VERSION,
        daily_quantities=None,
    )
    issued_at = _issued_at_for(local_date)
    for feed_id, value in ((feed_a, value_a), (feed_b, value_b)):
        values = [value] + [0.0] * 23
        _insert_precip_day_samples(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=local_date,
            issued_at=issued_at,
            fetched_at=issued_at,
            values=values,
        )


def _set_deterministic_now(copy: Path, *, mtime: datetime) -> date:
    stamp = mtime.timestamp()
    os.utime(copy, (stamp, stamp))
    return latest_knowable_target("UTC", mtime)


def _run_main(
    *, copy: Path, out: Path, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> Path:
    """Run ``--freeze-prepare`` and return the run's own output directory,
    resolved from the ``daily_shadow_result`` stdout line's ``run_dir``,
    relative to ``out``."""
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
    assert exit_code == 0, stdout.getvalue() + stderr.getvalue()
    for line in stdout.getvalue().splitlines():
        if line.startswith(daily_shadow.RESULT_PREFIX):
            text = line[len(daily_shadow.RESULT_PREFIX) :]
            result = cast("dict[str, object]", json.loads(text))
            return out / cast(str, result["run_dir"])
    raise AssertionError(
        f"no {daily_shadow.RESULT_PREFIX!r} line in stdout: {stdout.getvalue()!r}"
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        cast("dict[str, object]", json.loads(text))
        for text in path.read_text().splitlines()
        if text
    ]


def _target_entry(entries: list[dict[str, object]]) -> dict[str, object]:
    matches = [
        e
        for e in entries
        if e["snapshot_date"] == TARGET_DATE.isoformat() and e["lead"] == 0
    ]
    assert len(matches) == 1, matches
    return matches[0]


def _install_select_model_recorder(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[int, ...]]:
    original = daily_method.select_model
    calls: list[tuple[int, ...]] = []

    def recorder(
        window: daily_method.RainWindow,
        *,
        min_days: int,
        miss_weight: float,
        triggers_hundredths: Sequence[int],
        incumbent: str | None,
        switch_margin: int,
    ) -> daily_method.Selection:
        calls.append(tuple(triggers_hundredths))
        return original(
            window,
            min_days=min_days,
            miss_weight=miss_weight,
            triggers_hundredths=triggers_hundredths,
            incumbent=incumbent,
            switch_margin=switch_margin,
        )

    monkeypatch.setattr(daily_method, "select_model", recorder)
    return calls


def _install_served_rain_recorder(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    original = daily_method.served_rain
    calls: list[int] = []

    def recorder(
        selection: daily_method.Selection,
        ranked_totals: Mapping[str, float],
        samples_by_model: Mapping[str, Sequence[tuple[datetime, float]]],
        *,
        hours: Sequence[datetime],
        group_fraction: Fraction,
        trigger_hundredths: int,
        miss_weight: float,
        threshold_mm: float,
    ) -> daily_method.ServedRain:
        calls.append(trigger_hundredths)
        return original(
            selection,
            ranked_totals,
            samples_by_model,
            hours=hours,
            group_fraction=group_fraction,
            trigger_hundredths=trigger_hundredths,
            miss_weight=miss_weight,
            threshold_mm=threshold_mm,
        )

    monkeypatch.setattr(daily_method, "served_rain", recorder)
    return calls


def _install_group_vote_chance_recorder(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    original = daily_method.group_vote_chance
    calls: list[int] = []

    def recorder(
        ranked_totals: Sequence[tuple[str, float]],
        *,
        group_fraction: Fraction,
        trigger_hundredths: int,
    ) -> daily_method.GroupVote:
        calls.append(trigger_hundredths)
        return original(
            ranked_totals,
            group_fraction=group_fraction,
            trigger_hundredths=trigger_hundredths,
        )

    monkeypatch.setattr(daily_method, "group_vote_chance", recorder)
    return calls


@dataclass(frozen=True)
class _Recorded:
    line: dict[str, object]
    grid_entry: dict[str, object]
    served_select_triggers: list[tuple[int, ...]]
    served_rain_triggers: list[int]
    group_vote_triggers: list[int]


@pytest.fixture
def recorded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Recorded:
    copy, site, feed_a, feed_b = _make_copy(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        station_id = _insert_station(
            conn, site_id=site.site_id, pws_station_id="SYN-TRUTH"
        )
        for day in RAIN_NEARBY_DAYS:
            _insert_station_day(
                conn, station_id=station_id, local_date=day, daily_total_mm=1.0
            )
            _insert_case_day(
                conn,
                site=site,
                feed_a=feed_a,
                feed_b=feed_b,
                local_date=day,
                value_a=_syn_a_value(day),
                value_b=_syn_b_value(day),
            )
        for day in DRY_DAYS:
            _insert_station_day(
                conn, station_id=station_id, local_date=day, daily_total_mm=0.0
            )
            _insert_case_day(
                conn,
                site=site,
                feed_a=feed_a,
                feed_b=feed_b,
                local_date=day,
                value_a=_syn_a_value(day),
                value_b=_syn_b_value(day),
            )
        _insert_case_day(
            conn,
            site=site,
            feed_a=feed_a,
            feed_b=feed_b,
            local_date=TARGET_DATE,
            value_a=1.0,
            value_b=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    mtime = datetime(
        TARGET_DATE.year, TARGET_DATE.month, TARGET_DATE.day, 12, tzinfo=UTC
    ) + timedelta(days=2)
    _set_deterministic_now(copy, mtime=mtime)

    select_calls = _install_select_model_recorder(monkeypatch)
    served_rain_triggers = _install_served_rain_recorder(monkeypatch)
    group_vote_triggers = _install_group_vote_chance_recorder(monkeypatch)

    out = tmp_path / "out"
    run_dir = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site.site_id
    )

    line = _target_entry(_read_jsonl(run_dir / "lines.jsonl"))
    grid_entry = _target_entry(_read_jsonl(run_dir / "trigger_grid.jsonl"))

    assert select_calls and len(select_calls) % _CALLS_PER_CASE == 0
    served_select_triggers = select_calls[::_CALLS_PER_CASE]
    assert len(served_select_triggers) == len(served_rain_triggers)

    return _Recorded(
        line=line,
        grid_entry=grid_entry,
        served_select_triggers=served_select_triggers,
        served_rain_triggers=served_rain_triggers,
        group_vote_triggers=group_vote_triggers,
    )


def test_served_ranking_puts_syn_b_first_at_the_one_trigger(
    recorded: _Recorded,
) -> None:
    """The served line's JSON ranking ranks SYN-b first, at the 0.2 mm
    trigger, with its window counts; SYN-a is second with its own miss
    count, and the ranking is not provisional."""
    selection = cast("dict[str, object]", recorded.line["selection"])
    ranked = cast("list[dict[str, object]]", selection["ranked"])
    assert [r["key"] for r in ranked] == [KEY_B, KEY_A]
    fields = (
        "trigger_hundredths",
        "misses",
        "false_alarms",
        "penalty",
        "common_days",
        "rain_nearby_days",
        "dry_days",
    )
    assert {f: ranked[0][f] for f in fields} == {
        "trigger_hundredths": 20,
        "misses": 1,
        "false_alarms": 0,
        "penalty": "2",
        "common_days": 20,
        "rain_nearby_days": 8,
        "dry_days": 12,
    }
    assert {f: ranked[1][f] for f in fields} == {
        "trigger_hundredths": 20,
        "misses": 4,
        "false_alarms": 0,
        "penalty": "8",
        "common_days": 20,
        "rain_nearby_days": 8,
        "dry_days": 12,
    }
    assert selection["provisional"] is False


def test_s_pick_shows_no_symbol_for_the_served_selection(
    recorded: _Recorded,
) -> None:
    """S-PICK, read from the served line's JSON, is False: the selected
    model's target total never clears any trigger."""
    symbols = cast("dict[str, dict[str, object]]", recorded.line["symbols"])
    assert symbols["S-PICK"]["symbol"] is False


def test_trigger_grid_own_lowest_ranks_syn_a_first_at_its_own_value(
    recorded: _Recorded,
) -> None:
    """A15's own-lowest grid entry, read from the trigger-grid JSON, puts
    SYN-a first, each model scored at its own lowest-scoring grid value
    (10) -- the opposite order from the served ranking, which is what
    separates the two rankings."""
    grid = cast("list[dict[str, object]]", recorded.grid_entry["grid"])
    own = [g for g in grid if g["label"] == "own_lowest"]
    assert len(own) == 1
    assert own[0]["best"] == KEY_A
    assert own[0]["ranked"] == [[KEY_A, 10, 0, 0], [KEY_B, 10, 1, 0]]


def test_every_served_call_uses_the_one_rain_trigger(recorded: _Recorded) -> None:
    """select_model's served call, group_vote_chance and served_rain are all
    made with trigger 20 -- (20,) for select_model -- never a grid value, on
    every case the run processed."""
    assert recorded.served_select_triggers
    assert all(
        triggers == (daily_method.RAIN_TRIGGER_HUNDREDTHS,)
        for triggers in recorded.served_select_triggers
    )
    assert recorded.served_rain_triggers
    assert all(
        trigger == daily_method.RAIN_TRIGGER_HUNDREDTHS
        for trigger in recorded.served_rain_triggers
    )
    assert recorded.group_vote_triggers
    assert all(
        trigger == daily_method.RAIN_TRIGGER_HUNDREDTHS
        for trigger in recorded.group_vote_triggers
    )
