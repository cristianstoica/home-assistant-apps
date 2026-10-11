"""The temperature leak test (§15.3, plan round 3, Part B): M9, M28, M32,
M34, M150.

One scored case, snapshot ``S`` / lead 2 / target ``S+2``, run through the
real ``daily_shadow.run`` heldout path with the real station/forecast/truth
writers. The fixture builds a real ``forecast_record`` row for every
snapshot ``S-37`` to ``S`` at leads 0-5 via the production
``build_forecast_record`` writer, exactly as the plan's fixture literally
asks for -- not narrowed to one snapshot, since ``training_days()`` (the
training-window lookup) keys on ``(snapshot, target)`` pairs that only ever
land inside the training window when records exist for the WHOLE span of
snapshots that window's days are targets of, not just for ``S`` itself. The
run is driven into each arm NATURALLY -- never via a ``force_arm``
override, since the real pipeline (``_run_temperature``) always calls
``compute_gates(..., force_arm=None)``: there is no caller-reachable
override to bypass. Arm A is "as built" (both gates pass on their own); Arm
B is reached by giving exactly one in-scope station reading a NULL
``fetched_at``, which fails G-a on its own.

TWO feeds, ``model-a`` (warm-biased, ``_MODEL_A_BIAS`` = +2C) and
``model-b`` (cold-biased, ``_MODEL_B_BIAS`` = -3C), both written for every
seeded day so neither is starved below ``TEMP_MIN_DAYS`` at lead 2 -- a
single flat-10.0-everywhere feed let a wrongly-admitted day leak in with
ZERO effect on any computed aggregate (training error, a candidate's
value, persistence), which is why this file now also varies station
readings/truth/forecasts day-to-day (``_day_value``, alternating 9.0/11.0
by day-ordinal parity) rather than holding everything at one constant.
Day K alone stays pinned at ``_BASELINE_VALUE`` (10.0) so change (iii)'s
explicit median derivation has a fixed, known input. Published
``daily_truth`` is set to ``_day_value`` for every day in its range, which
is exactly what §6.5's Arm A rebuild computes from a single station's
reading (``LAPSE`` term is zero: the station's ``dem_elevation_m`` equals
the site's ``elevation_m``, both 900.0) -- so G-b passes and the run takes
Arm A as built, same as before.

``forecast_blend_depth`` stays pinned at 1 (``set_setting`` below) even
with two feeds: ``select_cell_feeds`` (record.py) picks exactly one feed
deterministically at depth 1, so the frozen record row's control is still
a single, unambiguous feed's value -- depth 2 would blend both feeds'
values into the control and add interacting arithmetic this file has no
need to pin (the oracle below is before/after byte equality on the (S, 2)
output line, never a hand-computed expected value).

For each arm, a fresh baseline is built, its liveness is checked (the (S, 2)
case is matched, with non-null candidate/benchmark values, a counted
``training_days`` count the run's own output reports as at least
``TEMP_MIN_DAYS``, a ``per_lead`` regime at both the line and every
candidate with BOTH feeds eligible, and a persistence benchmark that is not
flagged ``no_eligible``), and then three independent changes are each
applied to their OWN fresh copy of that baseline (never stacked) and
checked to leave the (S, 2) output line's RAW BYTES byte-identical and the
selected arm unchanged. (The output carries no PER-FEED training-day COUNT
anywhere in ``temperature_line_json``'s output, but it does carry a
per-feed signal: each candidate's ``result["eligible"]`` list and the
line's own ``feeds`` keys (``TempLine.feeds``) are each a set of feed
keys, not a count --
``_assert_live`` pins both feeds (``example-src/model-a`` and
``example-src/model-b``) present in both. Both feeds are written for every
one of the 38 seeded snapshot days, well over ``TEMP_MIN_DAYS`` = 20, which
is why both reach ``per_lead`` eligibility rather than one starving the
other out.):

  (i)   a change to data outside the TD(s) window that would move an
        OUT-OF-WINDOW day's rebuild if it leaked in (M9 under Arm A, M28
        under Arm B; the Arm-B step also discriminates M34, named below);
  (ii)  model-a's own forecast for the target day, issued before T(s) but
        re-fetched after it (M150 -- ``load_future_samples``'s ``as_of``
        cutoff drops only the ``fetched_at`` half of its check, leaving
        ``issued_at`` alone; the candidate/F1 replay path reads live
        ``forecast_samples``, independently of the frozen record row);
  (iii) Arm A only: a second station's late-fetched readings for day K
        (M32 -- ``ArmARebuilder.rebuild``'s ``known_by`` cutoff; this change
        also discriminates M34's Arm-A side, named below).

M34 ("persistence reads the published ``daily_truth`` row of day K whatever
the arm") is discriminated by TWO of the above, not a dedicated change of
its own: under Arm B, change (i) changes day K's published row, which M34's
bug would read regardless of arm (killed at the same assertion as M28);
under Arm A, change (iii) brings day K's published row up to date to a
DIFFERENT value (the two-station median, computed from ``_median_mad``'s
own body -- never hard-coded) than the arm's own correct training rebuild
(``_BASELINE_VALUE``, since the second station's late reading stays
excluded from training by a correct ``known_by`` cutoff) specifically so
M34's bug has a value to diverge on (killed at the same assertion as M32).

Each change's production site and why it moves the output under its named
mutant is documented on the change's own test function.

Scope: TEMPERATURE ONLY. Rain variant: not yet written -- the published
``daily_truth``/G-b machinery and the rain-only unmatched reasons differ
enough (``pre_wet_hours``, ``VersionUnmappedError``) that a rain leak test is
a separate authoring task, not a parametrization of this one; do not
skip-mark that gap, it is simply absent.

Design note (not a simplification of the fixture): the run itself is
restricted to one H day (``h_first=last_complete=_TARGET``), so
``select_cases`` only ever asks about target ``S+2``. With the full
leads-0-5 fixture, that still yields FOUR matched cases (snapshots
``S..S-3`` at leads ``2..5``, each targeting ``S+2``) because every one of
those four lead/snapshot combinations has its own ``recorded`` row; the
other three are harmless neighbors (``_case_lines`` filters to exactly
``(S, 2)``) and their own training windows are what the fixture's in-scope
union (``[S-34, S-1]``) is actually sized to, which is why the fixture's
truth/station ranges go out to ``S-37`` rather than stopping at ``S-31``.
Day S (= K+1) itself is confirmed OUTSIDE that union: ``_run_temperature``
(daily_shadow.py:5939-5968) builds ``case_rows`` from exactly those four
matched cases and calls ``temperature_window_days`` (:5581-5593), which
unions, over EACH case's own snapshot-derived K, both arms' windows
(:2008-2012: Arm A = K-29..K, Arm B = K-30..K-1) -- the widest right edge
across the four cases is Arm A's own K = S-1 (the S-snapshot case), so day
S is never in scope regardless of arm, and giving it a published
``daily_truth`` row (needed below) cannot put it in G-b's scope either,
since G-b's denominator only iterates the same in-scope union.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from tests.helpers import (
    asof_conn,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring import consensus as consensus_mod
from wxverify.settings.keys import set_setting
from wxverify.verification import daily_shadow
from wxverify.verification import truth as truth_mod
from wxverify.verification.record import build_forecast_record
from wxverify.verification.simulate import resolve_snapshot_utc

# S: the scored snapshot. K = S-1 is the latest knowable day at T(s)=07:00
# UTC (CONSENSUS_LAG_HOURS=3: day K's end-of-day+lag is 03:00 on S, which is
# not after 07:00; day S's end-of-day+lag is 03:00 on S+1, which IS after
# 07:00, so K stops at S-1).
_S = date(2035, 3, 10)
_K = _S - timedelta(days=1)
_TARGET = _S + timedelta(days=2)
_LEAD = 2

_BASELINE_VALUE = 10.0
# Change (i)'s additive raise: applied to whatever _day_value already
# holds for the raised day, never a flat replacement -- the plan's own
# wording is "raise ... by 5C", not "set to a fixed value".
_RAISE_DELTA_C = 5.0
# Change (iii)'s second station: a flat, independent reading (not run
# through _day_value) -- day K itself is pinned to _BASELINE_VALUE (see
# _day_value below), so the two-station median this change produces is a
# fixed, derivable number regardless of the day-to-day variation elsewhere.
_SECOND_STATION_VALUE_C = 15.0
# Each feed's constant bias (forecast - observed truth), chosen clearly
# different and non-zero so neither feed's training error is degenerate.
_MODEL_A_BIAS = 2.0
_MODEL_B_BIAS = -3.0

# Record rows (the plan's own fixture instruction): one 6-lead
# (lead 0-5) build per snapshot from S-37 to S.
_RECORD_START_OFFSET = -37
_RECORD_END_OFFSET = 0
_RECORD_LOOKAHEAD_DAYS = 6

# Published daily_truth: S-37 to S+2 (covers every in-scope day for both
# arms' windows over all four matched cases, PLUS day K+1(=S) and the
# target S+2 itself, both of which change (i)'s Arm-B step and the case's
# own scoring need as real, already-published rows to mutate/read rather
# than rows this fixture would have to insert from scratch mid-test).
_TRUTH_START_OFFSET = -37
_TRUTH_END_OFFSET = 2

# Station readings: S-37 to S-1 (Arm A's rebuild source for the same
# in-scope union). Day S (=K+1) gets its own separate baseline write below
# (_build_baseline), kept outside this range so it reads as the deliberate
# extra day it is, not folded into the loop's "in-scope union" framing.
_STATION_START_OFFSET = -37
_STATION_END_OFFSET = -1


def _day_value(day: date) -> float:
    """Day K is pinned to ``_BASELINE_VALUE`` (change (iii)'s explicit
    median derivation needs a fixed, known input); every other day
    alternates between 9.0 and 11.0 by ordinal parity. This replaces the
    prior fixture's single flat 10.0-everywhere value: with every day
    identical, a wrongly-admitted out-of-window day (change (i)) carried
    the SAME value as every in-window day, so even a leak that let it
    through would move nothing -- the test could pass for the wrong
    reason. Day-to-day variation makes a leaked day's value provably
    differ from a correctly-excluded run's, so a real leak moves the
    output and a correctly-inert run does not."""
    if day == _K:
        return _BASELINE_VALUE
    return 11.0 if day.toordinal() % 2 == 0 else 9.0


def _fixed_instant(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _hour(day: date, hour: int) -> str:
    return _fixed_instant(datetime(day.year, day.month, day.day, hour, tzinfo=UTC))


def _snapshot_utc_for(day: date) -> datetime:
    return resolve_snapshot_utc("UTC", day, "07:00")


def _t() -> datetime:
    return _snapshot_utc_for(_S)


def _note(site: daily_shadow.Site) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=site.site_id,
        first_h_date=_TARGET,
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _run_case(conn: sqlite3.Connection, site: daily_shadow.Site) -> dict[str, bytes]:
    note = _note(site)
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=_TARGET,
        last=_TARGET,
        h_first=_TARGET,
        last_complete=_TARGET,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


def _case_lines(files: dict[str, bytes]) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in files["lines.jsonl"].decode().splitlines()]
    return [
        line
        for line in lines
        if line["snapshot_date"] == _S.isoformat() and line["lead"] == _LEAD
    ]


def _case_line_bytes(files: dict[str, bytes]) -> list[bytes]:
    """The RAW bytes of each (S, 2) line of ``lines.jsonl`` (plan item 6):
    the oracle compares what was actually WRITTEN, not a reparsed/re-dumped
    structure that could mask a byte-level change (key order, float
    formatting, trailing whitespace) a parsed-dict comparison would hide."""
    out: list[bytes] = []
    for raw in files["lines.jsonl"].split(b"\n"):
        if not raw:
            continue
        parsed = json.loads(raw)
        if parsed["snapshot_date"] == _S.isoformat() and parsed["lead"] == _LEAD:
            out.append(raw)
    return out


def _write_station_hours(
    monkeypatch: pytest.MonkeyPatch,
    conn: sqlite3.Connection,
    *,
    station_id: int,
    day: date,
    value: float,
    fetched_at: str,
) -> None:
    """24 hourly readings for ``day``, via the real writer, clock faked to
    ``fetched_at`` for the duration (the writer's own ``isoformat_utc()``
    call has no caller-settable override -- this fakes the clock, not the
    system under test, same seam as the rest of the suite's as-of tests).
    The writer (``write_station_observation``, consensus.py:214-273) is a
    real upsert: calling this again for a day that already has readings,
    with a DIFFERENT value, updates them in place rather than inserting
    duplicates (confirmed from the function body: ``ON CONFLICT(station_id,
    variable, valid_at) DO UPDATE``) -- that is how change (i)'s Arm-A step
    below RAISES day S's existing baseline readings instead of inserting
    new ones."""

    def _fake_isoformat_utc(value: datetime | None = None) -> str:
        return fetched_at

    monkeypatch.setattr(consensus_mod, "isoformat_utc", _fake_isoformat_utc)
    for hour in range(24):
        consensus_mod.insert_station_observation(
            conn,
            station_id=station_id,
            variable="temperature",
            valid_at=_hour(day, hour),
            value=value,
            source_raw="synthetic",
        )


def _make_station(conn: sqlite3.Connection, site_id: int, label: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 900.0, 1)",
        (site_id, label),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
) -> None:
    """Own local copy of the pattern in
    ``test_daily_shadow_heldout_wiring.py``'s ``_insert_daily_truth`` -- not
    imported, since that name is private to that module (``tests`` is not
    this module's own test file, and importing a leading-underscore name
    across test modules was exactly one of this file's prior rejections)."""
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


def _update_daily_truth_value(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    local_date: date,
    quantity: str,
    value: float,
) -> None:
    """``_insert_daily_truth`` only inserts; changing an already-published
    day (change (i)'s Arm B step) needs an update, not a second insert."""
    conn.execute(
        "UPDATE daily_truth SET value = ? WHERE site_id = ? AND"
        " tz_generation_id = ? AND local_date = ? AND quantity = ?",
        (value, site_id, tz_generation_id, local_date.isoformat(), quantity),
    )


def _insert_hourly_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    value: float,
) -> None:
    """24 hourly ``forecast_samples`` rows for ``local_date``. Own local copy
    of the raw-SQL pattern in ``test_daily_shadow_heldout_wiring.py``'s
    ``_insert_day_samples`` (same reason as ``_insert_daily_truth`` above:
    not imported, since that name is private to that module)."""
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    rows: list[tuple[int, int, str, str, int, float, str]] = []
    for hour in range(24):
        valid_dt = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead_hours = max(1, int((valid_dt - issued).total_seconds() // 3600))
        rows.append(
            (
                site_id,
                feed_id,
                issued_at,
                _fixed_instant(valid_dt),
                lead_hours,
                value,
                fetched_at,
            )
        )
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
        """,
        rows,
    )


def _seed_record_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feeds: tuple[tuple[int, float], ...],
    snapshot_day: date,
) -> None:
    """One run per feed, issued the day before ``snapshot_day``, covering
    leads 0-5 (``snapshot_day`` through ``snapshot_day + 5``), then the
    real ``build_forecast_record`` write for that snapshot -- the plan's
    literal fixture instruction ("recorded rows for every snapshot s-37 to
    s at leads 0-5"). One run issued a single day ahead suffices for all
    six leads at once: a lead-5 target is 5 days out, but it only needs to
    be knowable (issued and fetched) before T(``snapshot_day``), the day
    the record is built for -- not before its own, later target day.
    ``feeds`` is (feed_id, bias) pairs; each feed's hourly value for a
    target day is ``_day_value(target) + bias``, so every feed's forecast
    error against that day's truth/station value is exactly its own
    constant bias -- clearly different and non-zero per feed, never the
    degenerate zero-everywhere error a single flat value would give."""
    day_before = snapshot_day - timedelta(days=1)
    issued_at = _fixed_instant(
        datetime(day_before.year, day_before.month, day_before.day, 6, tzinfo=UTC)
    )
    fetched_at = _fixed_instant(
        datetime(day_before.year, day_before.month, day_before.day, 6, 5, tzinfo=UTC)
    )
    for offset in range(_RECORD_LOOKAHEAD_DAYS):
        target_date = snapshot_day + timedelta(days=offset)
        for feed_id, bias in feeds:
            _insert_hourly_samples(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                local_date=target_date,
                issued_at=issued_at,
                fetched_at=fetched_at,
                value=_day_value(target_date) + bias,
            )
    build_forecast_record(
        conn, site_id, snapshot_day.isoformat(), now=_snapshot_utc_for(snapshot_day)
    )


class _Baseline:
    def __init__(
        self,
        conn: sqlite3.Connection,
        site: daily_shadow.Site,
        feed_id_a: int,
        feed_id_b: int,
        station_id: int,
    ) -> None:
        self.conn = conn
        self.site = site
        self.feed_id_a = feed_id_a
        self.feed_id_b = feed_id_b
        self.station_id = station_id


def _build_baseline(monkeypatch: pytest.MonkeyPatch, *, null_inject: bool) -> _Baseline:
    conn = asof_conn()
    site_id = asof_make_site(conn, "leak-site")
    ensure_published_generation(conn, site_id)
    # TWO feeds now exist; pinning the blend depth at 1 keeps the frozen
    # record row's control a single, unambiguous feed's value regardless
    # (select_cell_feeds, record.py, picks exactly one candidate at depth
    # 1) -- the same determinism guarantee the single-feed fixture relied
    # on, extended rather than dropped now that a second feed is present.
    set_setting(conn, "forecast_blend_depth", "1")
    feed_id_a = asof_make_real_feed(conn, "model-a")
    feed_id_b = asof_make_real_feed(conn, "model-b")
    station_id = _make_station(conn, site_id, "LEAK-STATION-01")
    site = daily_shadow.load_site(conn, site_id=site_id)

    before_t = _fixed_instant(_t() - timedelta(days=5))
    for offset in range(_STATION_START_OFFSET, _STATION_END_OFFSET + 1):
        day = _S + timedelta(days=offset)
        _write_station_hours(
            monkeypatch,
            conn,
            station_id=station_id,
            day=day,
            value=_day_value(day),
            fetched_at=before_t,
        )
    # Day S (=K+1) gets its own baseline reading too (plan item 2): change
    # (i)'s Arm-A step then RAISES this existing reading rather than
    # inserting one from nothing -- day S stays outside both arms' window
    # (module docstring, above) so this write is inert on its own, exactly
    # like the loop above.
    #
    # fetched_at is deliberately synthetic here: every one of day S's 24
    # hours (including the hours after T(s)'s own time-of-day, 07:00, on
    # that same calendar day) is stamped with the SAME before_t used for
    # every other seeded day (T(s)-5 days) -- in reality an observation
    # can't be fetched before the hour it reports on happens, but this
    # fixture is not modelling fetch plausibility, only the window-bound
    # exclusion that change (i)'s Arm-A step exercises below. Stamping
    # day S's reading with an unremarkable, clearly-knowable-by-T(s) fetch
    # time means the ONLY thing that can exclude it from Arm A's rebuild is
    # ``temperature_window``'s own day-range check, never a fetch-time
    # check this fixture could otherwise be accused of leaning on instead.
    _write_station_hours(
        monkeypatch,
        conn,
        station_id=station_id,
        day=_S,
        value=_day_value(_S),
        fetched_at=before_t,
    )

    if null_inject:
        # Arm B's natural G-a trigger: one in-scope reading with a NULL
        # fetched_at. The real writer (write_station_observation) can never
        # produce this -- it always stamps isoformat_utc(), never None, and
        # its no-op short-circuit doesn't compare fetched_at on a re-call --
        # so this one raw-SQL UPDATE is the minimal justified deviation from
        # "use the app's own writer" for this one precondition.
        conn.execute(
            "UPDATE station_observations SET fetched_at = NULL"
            " WHERE station_id = ? AND variable = 'temperature' AND valid_at = ?",
            (station_id, _hour(_K, 0)),
        )

    for offset in range(_TRUTH_START_OFFSET, _TRUTH_END_OFFSET + 1):
        day = _S + timedelta(days=offset)
        for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
            _insert_daily_truth(
                conn,
                site_id=site_id,
                tz_generation_id=site.generation_id,
                local_date=day,
                quantity=quantity,
                value=_day_value(day),
            )

    for offset in range(_RECORD_START_OFFSET, _RECORD_END_OFFSET + 1):
        snapshot_day = _S + timedelta(days=offset)
        _seed_record_samples(
            conn,
            site_id=site_id,
            feeds=((feed_id_a, _MODEL_A_BIAS), (feed_id_b, _MODEL_B_BIAS)),
            snapshot_day=snapshot_day,
        )

    conn.commit()
    return _Baseline(conn, site, feed_id_a, feed_id_b, station_id)


def _assert_live(files: dict[str, bytes], *, forced_arm: str) -> list[dict[str, Any]]:
    header = json.loads(files["header.json"])
    assert header["arm"] == forced_arm
    lines = _case_lines(files)
    assert len(lines) == len(daily_shadow.TEMPERATURE_QUANTITIES)
    for line in lines:
        assert line["unmatched"] is None
        assert line["control"] is not None
        assert line["truth"] is not None
        # The run's own reported training-day count and regime, not an
        # inference from the fixture's seeding ranges -- the liveness this
        # test needs is that the real pipeline counted real training days,
        # which only its own output can attest to.
        assert line["training_days"] is not None
        assert line["training_days"] >= daily_shadow.TEMP_MIN_DAYS
        assert line["regime"] != "no_eligible"
        assert len(line["candidates"]) > 0
        # Per-feed signal (module docstring, above): the line's own regime
        # and ``feeds`` keys, and each candidate's ``result["eligible"]``,
        # pin BOTH feeds eligible under ``per_lead`` -- not merely
        # "some feed eligible" (``!= "no_eligible"`` alone would also pass
        # under ``pooled`` or with only one feed eligible).
        assert line["regime"] == "per_lead"
        feed_keys = set(line["feeds"])
        assert feed_keys == {"example-src/model-a", "example-src/model-b"}
        for cand in line["candidates"].values():
            assert cand["result"]["regime"] == "per_lead"
            assert set(cand["result"]["eligible"]) == feed_keys
        for cand in line["candidates"].values():
            result = cand["result"]
            assert result["value"] is not None
            assert result["regime"] != "no_eligible"
            assert "no_eligible" not in result["flags"]
        persistence = line["benchmarks"]["persistence"]
        assert persistence["value"] is not None
        assert "no_eligible" not in persistence["flags"]
    return lines


@pytest.mark.parametrize("forced_arm", ["A", "B"])
def test_leak_baseline_is_live(
    monkeypatch: pytest.MonkeyPatch, forced_arm: str
) -> None:
    """Liveness precondition for every change below: the (S, 2) case is
    matched, with real (non-null, non-``no_eligible``) values and a real
    counted ``training_days`` figure, under both arms, reached naturally
    (A: as built; B: one NULL fetched_at). Liveness also requires the
    ``per_lead`` regime on the line and on every candidate, with both
    feeds eligible."""
    baseline = _build_baseline(monkeypatch, null_inject=(forced_arm == "B"))
    files = _run_case(baseline.conn, baseline.site)
    _assert_live(files, forced_arm=forced_arm)


@pytest.mark.parametrize("forced_arm", ["A", "B"])
def test_leak_window_end_change_is_inert(
    monkeypatch: pytest.MonkeyPatch, forced_arm: str
) -> None:
    """Change (i): a change outside the arm's own training window.

    M9 (daily_shadow.py: ``temperature_window`` -- Arm A's window end moved
    from K to K+1) under Arm A: day K+1 (=S) already has a baseline
    reading (``_build_baseline``); here that EXISTING reading is RAISED by
    ``_RAISE_DELTA_C`` (5C) via the real writer's upsert, all stamped
    ``fetched_at <= T(s)`` -- correct code's Arm-A window ends at K and
    never reads day S, so this must not move the (S, 2) line's
    training-window aggregates (training_days/regime/candidates/
    persistence). If M9's bug moved the window end to K+1, day S's raised
    value would enter Arm A's training set and move those fields.

    M28 (Arm B's window not shifted one day earlier, still ending at K) and
    M34 (persistence reads the published ``daily_truth`` row of day K
    whatever the arm) under Arm B: days K and K+1's PUBLISHED daily_truth
    are each raised by ``_RAISE_DELTA_C`` instead (Arm B reads published
    truth, not station readings, for its window) -- correct code's Arm-B
    window is [S-31, S-2] (``daily_shadow.temperature_window(_K, "B")``),
    excluding both K and K+1, so this must not move the line either. If
    M28's bug left Arm B's window ending at K, day K's raised published
    value would enter it; whatever the arm, if M34's bug made persistence
    read day K's published row directly instead of the arm's own rebuilt
    training value, day K's raised published value would move persistence
    too (this is the same change (i)/Arm-B step the M28 ledger entry uses --
    not a separate construction).

    Sensitivity: in both arms, the raised value differs from the
    unmodified ``_day_value``/``_BASELINE_VALUE`` by exactly
    ``_RAISE_DELTA_C`` -- a non-zero, known shift that a leaking mutant
    would carry into training_days/a feed's mean training error/a
    candidate's value/persistence, and the data-level probes below confirm
    the leaked data is real (present, in the right shape) rather than an
    inert addition the mutant would have nothing to leak.

    WATCH (Arm A only): raising day K+1(=S)'s STATION reading risks G-b
    disagreeing at day S if S were itself in scope with a stored truth row.
    This fixture avoids that even though day S now DOES have a published
    truth row (the fixture publishes truth through S+2, for change (i)'s
    Arm-B step and for the case's own target) -- S is still outside the
    in-scope union [S-34, S-1] (the widest span any of the four matched
    cases' own arm windows reach, confirmed in the module docstring above),
    so G-b's denominator (which only examines in-scope days) never looks at
    day S at all, regardless of its truth row's presence.
    """
    baseline = _build_baseline(monkeypatch, null_inject=(forced_arm == "B"))
    before_files = _run_case(baseline.conn, baseline.site)
    _assert_live(before_files, forced_arm=forced_arm)
    before = _case_line_bytes(before_files)

    if forced_arm == "A":
        raised_value = _day_value(_S) + _RAISE_DELTA_C
        _write_station_hours(
            monkeypatch,
            baseline.conn,
            station_id=baseline.station_id,
            day=_K + timedelta(days=1),  # = S
            value=raised_value,
            fetched_at=_fixed_instant(_t() - timedelta(hours=1)),
        )
        baseline.conn.commit()

        # Data-level probe: day S's 24 readings exist, each at the raised
        # value, all fetched_at <= T(s) -- only the Arm-A window bound
        # (temperature_window, daily_shadow.py:2008-2012) can exclude them
        # from training; nothing about their own fetch time does (see the
        # fetched_at comment on day S's baseline write in _build_baseline,
        # above -- this raise keeps that same deliberately-unremarkable
        # fetch time, raising only the value).
        rows = baseline.conn.execute(
            "SELECT value, fetched_at FROM station_observations"
            " WHERE station_id = ? AND variable = 'temperature'"
            " AND valid_at LIKE ?",
            (baseline.station_id, f"{_S.isoformat()}%"),
        ).fetchall()
        assert len(rows) == 24
        t_s = _t()
        for row in rows:
            assert row["value"] == raised_value
            assert parse_utc(str(row["fetched_at"])) <= t_s

        # A ``recorded`` row exists for (snapshot S-2, target S, lead 2),
        # so day S WOULD count as a lead-2 training day the moment the
        # window bound let it in -- the leak surface is real, not inert
        # for want of record coverage.
        recorded = baseline.conn.execute(
            "SELECT 1 FROM forecast_of_record WHERE site_id = ? AND"
            " snapshot_local_date = ? AND target_local_date = ? AND"
            " variable = 'temperature' AND display_lead = 2"
            " AND status = 'recorded'",
            (
                baseline.site.site_id,
                (_S - timedelta(days=2)).isoformat(),
                _S.isoformat(),
            ),
        ).fetchone()
        assert recorded is not None

        # Sensitivity: day S's raised value differs from day K's pinned
        # baseline -- if M9 let day S into the window, the window's
        # observed value for that day (and therefore training_days, each
        # feed's mean training error, every candidate and persistence)
        # would shift by this difference rather than stay inert.
        assert raised_value != _day_value(_K)
    else:
        window_b = daily_shadow.temperature_window(_K, "B")
        for day in (_K, _K + timedelta(days=1)):
            # Confirm the data-level probe's own premise: both days are
            # genuinely outside Arm B's window, so only M28/M34 could let
            # either changed value affect the case.
            assert not (window_b[0] <= day <= window_b[1])
            for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
                _update_daily_truth_value(
                    baseline.conn,
                    site_id=baseline.site.site_id,
                    tz_generation_id=baseline.site.generation_id,
                    local_date=day,
                    quantity=quantity,
                    value=_day_value(day) + _RAISE_DELTA_C,
                )
        baseline.conn.commit()

        # Data-level probe: the published rows of K and K+1 now differ
        # from their original _day_value for both quantities.
        for day in (_K, _K + timedelta(days=1)):
            for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
                row = baseline.conn.execute(
                    "SELECT value FROM daily_truth WHERE site_id = ? AND"
                    " tz_generation_id = ? AND local_date = ? AND"
                    " quantity = ?",
                    (
                        baseline.site.site_id,
                        baseline.site.generation_id,
                        day.isoformat(),
                        quantity,
                    ),
                ).fetchone()
                assert row is not None
                assert float(row["value"]) == _day_value(day) + _RAISE_DELTA_C
                assert float(row["value"]) != _day_value(day)
    baseline.conn.commit()

    after_files = _run_case(baseline.conn, baseline.site)
    after_header = json.loads(after_files["header.json"])
    assert after_header["arm"] == forced_arm
    after = _case_line_bytes(after_files)
    assert after == before


@pytest.mark.parametrize("forced_arm", ["A", "B"])
def test_leak_late_forecast_sample_is_inert(
    monkeypatch: pytest.MonkeyPatch, forced_arm: str
) -> None:
    """Change (ii): model-a's own forecast for the target day (S+2),
    issued BEFORE T(s) but re-fetched AFTER T(s).

    M150 (wxverify/forecast/data.py: ``load_future_samples``'s ``as_of``
    cutoff drops only the ``fetched_at`` half -- ``AND
    julianday(fs.fetched_at) <= julianday(?)`` -- while the ``issued_at``
    half of the same clause stays in place). Because this batch's
    ``issued_at`` is BEFORE T(s), the surviving ``issued_at`` check alone
    would not exclude it -- it is the removed ``fetched_at`` check
    specifically that must do the excluding, which is why ``fetched_at`` is
    set after T(s) while ``issued_at`` is not. The frozen record row
    (``control_value``, daily_shadow.py:547-599) never re-reads
    ``forecast_samples``, so it is immune by construction; the leak surface
    is the REPLAY path (``SnapshotReplay``/``ReplayCache._load``,
    daily_shadow.py:1538-1580, which calls ``load_future_samples`` with
    ``as_of=isoformat_utc(T(s))``), feeding ``temperature_feed_cases``'s
    ``_feed_value`` for every composer.

    Value (plan item 4): exactly model-a's newest run fetched by T(s) for
    every hour of S+2, raised by 5C. That "newest by T(s)" value is
    ``_day_value(S+2) + _MODEL_A_BIAS`` for every snapshot-day seed that
    covers S+2 (``_seed_record_samples`` always writes that same formula
    for a given target date, regardless of which snapshot day's run wrote
    it), so it is computed here rather than hard-coded. model-b's own
    coverage of S+2 is untouched by this change (confirmed by the second
    probe below) -- the late batch only ever targets ``feed_id_a``.

    This late batch targets the SAME feed and the SAME 24 ``valid_at`` slots
    as the legitimate, correctly-timed batch every snapshot's own 6-lead
    record seed (``_seed_record_samples``) already wrote for S+2 -- not a
    second, untrained feed with a single off-hour sample (a prior version of
    this test used that construction, and it is wrong: ``training_regime``'s
    own ``min_days`` gate would exclude an untrained feed from every
    composer before the leak could ever surface, and ``covers_local_day``
    rejects a single-hour sample outright, so ``_feed_value`` would return
    None for it regardless of any as_of cutoff -- that test could not have
    failed under this mutant). Because the late batch shares model-a's
    partition key (feed_id, variable, valid_at) with the legitimate batch
    and carries a strictly later ``issued_at`` than the legitimate batch
    (even though still before T(s)), ``load_future_samples``'s own
    ``ORDER BY fs.issued_at DESC`` ranks it first the moment the removed
    ``fetched_at`` check stops excluding it -- demonstrated below,
    independent of the mutant, by re-running that exact partition/order
    with no ``as_of`` filter at all and checking which row ranks first for
    every one of the 24 slots.
    """
    baseline = _build_baseline(monkeypatch, null_inject=(forced_arm == "B"))
    before_files = _run_case(baseline.conn, baseline.site)
    _assert_live(before_files, forced_arm=forced_arm)
    before = _case_line_bytes(before_files)

    # issued before T(s) (so a surviving issued_at check would not exclude
    # it on its own) but fetched after T(s) (the field M150 stops checking).
    late_issued_at = _fixed_instant(_t() - timedelta(hours=1))
    late_fetched_at = _fixed_instant(_t() + timedelta(hours=1))
    legitimate_value = _day_value(_TARGET) + _MODEL_A_BIAS
    leak_value = legitimate_value + _RAISE_DELTA_C
    _insert_hourly_samples(
        baseline.conn,
        site_id=baseline.site.site_id,
        feed_id=baseline.feed_id_a,
        local_date=_TARGET,
        issued_at=late_issued_at,
        fetched_at=late_fetched_at,
        value=leak_value,
    )
    baseline.conn.commit()

    # Capability probe, independent of any mutant: with no as_of filter at
    # all, load_future_samples' own partition (feed_id, variable, valid_at)
    # ordered by issued_at DESC ranks the late batch first for every one of
    # the target day's 24 slots -- proving the fixture's late batch really
    # is a winning competitor for that slot's value, not an inert addition
    # that M150 would have nothing to leak.
    ranked = baseline.conn.execute(
        """
        SELECT value, ROW_NUMBER() OVER (
            PARTITION BY feed_id, variable, valid_at ORDER BY issued_at DESC
        ) AS rn
        FROM forecast_samples
        WHERE feed_id = ? AND variable = 'temperature' AND valid_at LIKE ?
        """,
        (baseline.feed_id_a, f"{_TARGET.isoformat()}%"),
    ).fetchall()
    top_ranked = [row for row in ranked if row["rn"] == 1]
    assert len(top_ranked) == 24
    assert all(row["value"] == leak_value for row in top_ranked)

    # model-b's own coverage of S+2 is unaffected -- the late batch above
    # only ever inserted rows for feed_id_a. Mirror model-a's ranked probe:
    # model-b has one run per EACH seeded snapshot day whose 6-day
    # lookahead covers S+2 (snapshots S-3..S, four of them, so 4*24=96
    # raw rows exist for this feed/day before any as_of/rank filtering --
    # derived below from the seeding constants rather than a bare literal),
    # so the correct expectation is 24 rows at rn=1 (the latest-issued run
    # per hour), not a flat count over every row ever written.
    covering_snapshot_offsets = [
        offset
        for offset in range(_RECORD_START_OFFSET, _RECORD_END_OFFSET + 1)
        if 0 <= (_TARGET - (_S + timedelta(days=offset))).days < _RECORD_LOOKAHEAD_DAYS
    ]
    other_feed_raw = baseline.conn.execute(
        "SELECT value, fetched_at FROM forecast_samples WHERE feed_id = ? AND"
        " variable = 'temperature' AND valid_at LIKE ?",
        (baseline.feed_id_b, f"{_TARGET.isoformat()}%"),
    ).fetchall()
    assert len(other_feed_raw) == len(covering_snapshot_offsets) * 24
    t_s = _t()
    assert all(parse_utc(str(row["fetched_at"])) <= t_s for row in other_feed_raw)
    other_feed_ranked = baseline.conn.execute(
        """
        SELECT value, ROW_NUMBER() OVER (
            PARTITION BY feed_id, variable, valid_at ORDER BY issued_at DESC
        ) AS rn
        FROM forecast_samples
        WHERE feed_id = ? AND variable = 'temperature' AND valid_at LIKE ?
        """,
        (baseline.feed_id_b, f"{_TARGET.isoformat()}%"),
    ).fetchall()
    other_feed_top = [row for row in other_feed_ranked if row["rn"] == 1]
    assert len(other_feed_top) == 24
    assert all(
        row["value"] == _day_value(_TARGET) + _MODEL_B_BIAS for row in other_feed_top
    )

    after_files = _run_case(baseline.conn, baseline.site)
    after_header = json.loads(after_files["header.json"])
    assert after_header["arm"] == forced_arm
    after = _case_line_bytes(after_files)
    assert after == before


def test_leak_late_second_station_is_inert_for_arm_a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Change (iii), Arm A only (the plan names it Arm-A-only: Arm B does
    not rebuild from station readings at all, so a station-level leak has
    no Arm-B analogue to test here).

    M32 (daily_shadow.py ~1829-1839: ``ArmARebuilder.rebuild``'s ``known_by``
    cutoff removed, so a reading fetched after ``known_by`` is no longer
    excluded). A second enabled station is added (its first-ever reading
    for this variable, so ``qc_flag`` -- obs/qc.py:8-19, with the writer's
    own "previous" lookup scoped to ``station_id``, consensus.py:231-239 --
    is "ok" by construction, never a flag this fixture has to force) with
    every hour of day K's readings fetched AFTER T(s), at
    ``_SECOND_STATION_VALUE_C`` (15.0). Day K itself is pinned to
    ``_BASELINE_VALUE`` (10.0) by ``_day_value``, so the two-station
    median below is a fixed, known pair regardless of the day-to-day
    variation elsewhere in the fixture.

    Arm A's TRAINING rebuild (used for persistence/window_truth) is called
    with ``known_by=T(s)`` and must keep excluding the second station's
    late reading, so day K's TRAINING value stays ``_BASELINE_VALUE`` and
    the (S, 2) line is unchanged. Under M32, the cutoff is gone, both
    stations' day-K readings enter the median via ``_median_mad``
    (consensus.py:75-87, through ``compute_consensus``) -- computed below
    from the function's own body rather than hard-coded, confirming first
    that the MAD band is wide enough for both readings to stay inliers
    (the MAD floor for temperature is 0.5C and the band is
    3 * 1.4826 * max(median(|v-median|), 0.5); with two values 5C apart the
    MAD is 2.5, giving a band of 3*1.4826*2.5 = 11.1C around the median --
    wide enough that neither 10.0 nor 15.0 is an outlier) -- moving day K's
    training-window value and therefore persistence's.

    Also M34 (Arm-A side): G-b's OWN comparison rebuild (compute_gates,
    daily_shadow.py ~1947, ``rebuilder.rebuild(day, known_by=None, ...)``) is
    UNBOUNDED by construction -- it always sees every reading regardless of
    fetch time, to check "does our own rebuild match the officially
    published story", not "what was knowable at T". So as soon as the
    second station's day-K readings exist in station_observations, G-b's
    rebuild for day K shifts to the two-station value regardless of this
    mutant. To keep G-b passing (so the arm stays A), day K's published
    truth is brought up to date via the real nightly path --
    ``materialize_consensus`` for each of day K's hours, then
    ``regenerate_marked_truth`` -- exactly as a real nightly run would after
    new station data arrived. Doing this gives Arm-A's own M34 bug (reading
    day K's published row directly) a value to diverge on: a correct
    persistence reads the arm's training rebuild (``_BASELINE_VALUE``,
    since the late reading is excluded from training by a correct
    ``known_by`` cutoff); M34's bug would read the published row this step
    just set to the two-station median instead. The capability probe below
    checks that published value directly, independent of either mutant.
    """
    baseline = _build_baseline(monkeypatch, null_inject=False)
    before_files = _run_case(baseline.conn, baseline.site)
    _assert_live(before_files, forced_arm="A")
    before = _case_line_bytes(before_files)

    second_station = _make_station(
        baseline.conn, baseline.site.site_id, "LEAK-STATION-02"
    )
    after_t = _fixed_instant(_t() + timedelta(hours=1))
    _write_station_hours(
        monkeypatch,
        baseline.conn,
        station_id=second_station,
        day=_K,
        value=_SECOND_STATION_VALUE_C,
        fetched_at=after_t,
    )
    for hour in range(24):
        consensus_mod.materialize_consensus(
            baseline.conn,
            site_id=baseline.site.site_id,
            variable="temperature",
            valid_at=_hour(_K, hour),
        )
    truth_mod.regenerate_marked_truth(baseline.conn, site_id=baseline.site.site_id)
    baseline.conn.commit()

    # Capability probe, independent of either mutant: day K's published
    # truth really did move to the two-station median -- computed from
    # _median_mad's own body, not hard-coded -- confirming both that the
    # materialize/regenerate step took effect and that a correct run's arm
    # stays A only because this value, not _BASELINE_VALUE, is what G-b's
    # unbounded rebuild now agrees with.
    expected = consensus_mod.compute_consensus(
        [
            consensus_mod.StationReading(
                station_id=baseline.station_id,
                dem_elevation_m=900.0,
                variable="temperature",
                valid_at=_hour(_K, 0),
                value=_BASELINE_VALUE,
            ),
            consensus_mod.StationReading(
                station_id=second_station,
                dem_elevation_m=900.0,
                variable="temperature",
                valid_at=_hour(_K, 0),
                value=_SECOND_STATION_VALUE_C,
            ),
        ],
        variable="temperature",
        site_elevation_m=900.0,
        mad_floor=consensus_mod.MAD_FLOORS["temperature"],
    )
    assert expected is not None
    assert expected.value != _BASELINE_VALUE
    published = {
        row["quantity"]: row["value"]
        for row in baseline.conn.execute(
            "SELECT quantity, value FROM daily_truth WHERE site_id = ?"
            " AND local_date = ? AND tz_generation_id = ?",
            (baseline.site.site_id, _K.isoformat(), baseline.site.generation_id),
        ).fetchall()
    }
    for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
        assert published[quantity] == expected.value

    after_files = _run_case(baseline.conn, baseline.site)
    after_header = json.loads(after_files["header.json"])
    assert after_header["arm"] == "A"
    after = _case_line_bytes(after_files)
    assert after == before
