"""Scoring the day (daily-close plan 2026-10-22-daily-close-pairs-and-
publication.md, section 6; test IDs SD1-SD18, plan 18.6).

Written before ``wxverify.scoring.close_scores`` exists: every test below
collects, then fails at its own ``from wxverify.scoring.close_scores import
...`` with ``ModuleNotFoundError`` -- the failure Step 3 accepts.

Real SQLite throughout (``tests.helpers.asof_conn``, real migrations);
nothing in the code under test is mocked. Synthetic data only: stations
``ISTATION01``/``ISTATION02`` (via ``day_close_site``), feed sources
``synthetic-sd-x``/``synthetic-sd-y``/``synthetic-sd-z``, zone ``UTC``.

Each test calls ``score_day`` directly on a hand-built ``DayRecord`` in the
``closing`` state (the shape ``tests/test_p4_dormant.py``'s PD2 also uses,
plan 4.3), rather than running the full night chain through
``run_due_jobs`` -- 18.6's "stage 5 ran" is read here as
"score_day's stage-5 call point ran with D's inputs in place", the same
entry point P3's stage 5 calls (plan 6.1), not a literal scheduler replay.
This is a deliberate simplification; see the test-authoring report for the
chain-based alternative.

The shared seed (18.6's intro paragraph): feeds X and Y, each with one
recorded ``round`` download on every S from D-7 to D, each download's label
dated the day before S, carrying hourly temperature/wind/precip forecasts
for D at that download's day ahead, seeded as its ``forecast_scored`` rows
(rain release, Part B: the close's only forecast input); D has hourly
observations for all three
variables and five truth rows, all eligible, ``rain_threshold_mm`` 0.25.
X's forecasts carry a constant, deterministic bias over the observed value
(+1.0 C, +2.0 km/h wind, 0 mm rain); Y's a different one (-0.5 C, -1.0 km/h,
0 mm rain) -- chosen so every metric in 6.4's sums is non-trivial but exact
and reproducible by hand.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import D, asof_conn, day_close_site, note_day_clock
from wxverify.core.timeutil import is_canonical_utc_stamp, isoformat_utc, parse_utc
from wxverify.db.site_days import (
    begin_close,
    day_record,
    ensure_site_days,
    finish_close,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

_FEED_MAX_LEAD_HOURS = 200
_VARIABLES = ("temperature", "wind", "precip")
_BIAS = {
    "X": {"temperature": 1.0, "wind": 2.0, "precip": 0.0},
    "Y": {"temperature": -0.5, "wind": -1.0, "precip": 0.0},
}


def _temp_obs(hour: int) -> float:
    return 10.0 + hour


def _wind_obs(hour: int) -> float:
    return 3.0 + 0.5 * hour


def _precip_obs(hour: int) -> float:  # noqa: ARG001
    return 0.0


_OBS_FNS = {"temperature": _temp_obs, "wind": _wind_obs, "precip": _precip_obs}


def _make_feed(conn, name: str) -> int:
    source = f"synthetic-sd-{name.lower()}"
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, ?, 0)",
        (source, source, _FEED_MAX_LEAD_HOURS),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn, site_id: int, feed_id: int, *, enabled: bool = True) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, ?, 0)",
        (site_id, feed_id, 1 if enabled else 0),
    )


def _prepare_closing_day(conn, site_id: int, *, first_date, target_date):
    """Start ``site_days`` at ``first_date``, close every day up to (not
    including) ``target_date``, and leave ``target_date`` in ``closing``
    (``begin_close`` only, never ``finish_close``). Returns its
    ``DayRecord``.
    """
    start_dt = datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC)
    start_site_days(conn, site_id, first_date, now_utc=start_dt)
    far_future = note_day_clock(
        datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        + timedelta(days=2)
    )
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d < target_date:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        begin_close(conn, site_id, d, now_utc=t)
        finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, target_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, target_date, now_utc=t)
    record = day_record(conn, site_id, target_date)
    assert record is not None and record.state == "closing"
    return record, t


def _seed_truth(
    conn,
    site_id: int,
    local_date,
    *,
    generation_id: int,
    temp_eligible: tuple[bool, bool] = (True, True),
    temp_covered_hours: int = 24,
    wind_eligible: bool = True,
    rain_eligible: bool = True,
    rain_threshold_mm: float = 0.25,
) -> None:
    start = f"{local_date.isoformat()}T00:00:00Z"
    end = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
        + timedelta(days=1)
    )
    high_eligible, low_eligible = temp_eligible
    rows = [
        ("temperature_high", 30.0, high_eligible, temp_covered_hours, None, None),
        ("temperature_low", 10.0, low_eligible, temp_covered_hours, None, None),
        ("wind_max", 14.5, wind_eligible, 24, None, None),
        ("precip_total", 0.0, rain_eligible, 24, 0, 24),
        ("precip_occurrence", 0.0, rain_eligible, 24, 0, 24),
    ]
    for quantity, value, eligible, covered_hours, wet, dry in rows:
        conn.execute(
            "INSERT INTO daily_truth"
            " (site_id, local_date, quantity, value, eligible, covered_hours,"
            "  expected_slots, wet_hours, dry_hours, rain_threshold_mm,"
            "  day_start_utc, day_end_utc, timezone, tz_generation_id)"
            " VALUES (?, ?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, 'UTC', ?)",
            (
                site_id,
                local_date.isoformat(),
                quantity,
                value,
                1 if eligible else 0,
                covered_hours,
                wet,
                dry,
                rain_threshold_mm,
                start,
                end,
                generation_id,
            ),
        )


def _seed_observations(
    conn, site_id: int, local_date, *, skip_hours: frozenset[int] = frozenset()
) -> None:
    for hour in range(24):
        if hour in skip_hours:
            continue
        valid_at = isoformat_utc(
            datetime(
                local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
            )
        )
        for variable, fn in _OBS_FNS.items():
            conn.execute(
                "INSERT INTO observations"
                " (site_id, variable, valid_at, value, n_stations, computed_at)"
                " VALUES (?, ?, ?, ?, 2, ?)",
                (site_id, variable, valid_at, fn(hour), valid_at),
            )


def _seed_download(conn, *, site_id: int, feed_id: int, local_date, label: str) -> int:
    cur = conn.execute(
        "INSERT INTO forecast_downloads"
        " (site_id, feed_id, local_date, slot, round_date, round_try,"
        "  started_at, ended_at, fetched_at, outcome, reason, usable, inserted, labels)"
        " VALUES (?, ?, ?, 'round', ?, 1, ?, ?, ?, 'stored', NULL, 72, 72, ?)",
        (
            site_id,
            feed_id,
            local_date.isoformat(),
            local_date.isoformat(),
            f"{local_date.isoformat()}T00:10:00Z",
            f"{local_date.isoformat()}T00:10:04Z",
            f"{local_date.isoformat()}T00:10:04Z",
            json.dumps({str(feed_id): [label]}),
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_forecast_hour(
    conn,
    *,
    site_id,
    feed_id,
    variable,
    issued_at,
    valid_at,
    lead_hours,
    value,
    day_ahead,
    download_id,
    download_date,
    fetched_at,
) -> None:
    """One scored value: the ``forecast_scored`` row the first download of
    ``download_date`` wrote for ``valid_at`` at ``day_ahead`` (rain release,
    Part B). The close reads only this table for its forecasts."""
    conn.execute(
        "INSERT INTO forecast_scored"
        " (site_id, feed_id, variable, valid_at, day_ahead, value, lead_hours,"
        "  issued_at, download_id, download_date, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            feed_id,
            variable,
            valid_at,
            day_ahead,
            value,
            lead_hours,
            issued_at,
            download_id,
            download_date,
            fetched_at,
        ),
    )


def _seed_downloads(
    conn,
    site_id: int,
    feeds: dict[str, int],
    target_date,
    *,
    skip: frozenset[tuple[str, int]] = frozenset(),
    bias: dict[str, dict[str, float]] | None = None,
) -> dict[tuple[str, int], int]:
    """Seed one recorded ``round`` download per feed for each S in
    ``target_date``-7 to ``target_date``, with hourly forecasts for
    ``target_date``'s 24 hours at that S's day ahead k = target_date - S,
    stored as that download's ``forecast_scored`` rows.
    ``skip`` leaves (feed name, day ahead) pairs unseeded.
    """
    bias = bias if bias is not None else _BIAS
    download_ids: dict[tuple[str, int], int] = {}
    for k in range(8):
        s = target_date - timedelta(days=k)
        label = f"{(s - timedelta(days=1)).isoformat()}T00:00:00Z"
        for name, feed_id in feeds.items():
            if (name, k) in skip:
                continue
            download_id = _seed_download(
                conn, site_id=site_id, feed_id=feed_id, local_date=s, label=label
            )
            download_ids[(name, k)] = download_id
            fetched_at = f"{s.isoformat()}T00:10:04Z"
            for hour in range(24):
                valid_at = isoformat_utc(
                    datetime(
                        target_date.year,
                        target_date.month,
                        target_date.day,
                        hour,
                        tzinfo=UTC,
                    )
                )
                lead = k * 24 + hour + 1
                for variable, fn in _OBS_FNS.items():
                    value = fn(hour) + bias[name][variable]
                    _seed_forecast_hour(
                        conn,
                        site_id=site_id,
                        feed_id=feed_id,
                        variable=variable,
                        issued_at=label,
                        valid_at=valid_at,
                        lead_hours=lead,
                        value=value,
                        day_ahead=k,
                        download_id=download_id,
                        download_date=s.isoformat(),
                        fetched_at=fetched_at,
                    )
    return download_ids


def _build_base(
    conn,
    *,
    skip: frozenset[tuple[str, int]] = frozenset(),
    rain_threshold_mm: float = 0.25,
    temp_eligible: tuple[bool, bool] = (True, True),
    temp_covered_hours: int = 24,
    wind_eligible: bool = True,
    rain_eligible: bool = True,
    obs_skip_hours: frozenset[int] = frozenset(),
):
    site_id = day_close_site(conn, with_records=False)
    generation_id = ensure_site_generation(conn, site_id)
    feeds = {"X": _make_feed(conn, "X"), "Y": _make_feed(conn, "Y")}
    for feed_id in feeds.values():
        _subscribe(conn, site_id, feed_id)
    record, now_utc = _prepare_closing_day(
        conn, site_id, first_date=D - timedelta(days=10), target_date=D
    )
    _seed_truth(
        conn,
        site_id,
        D,
        generation_id=generation_id,
        temp_eligible=temp_eligible,
        temp_covered_hours=temp_covered_hours,
        wind_eligible=wind_eligible,
        rain_eligible=rain_eligible,
        rain_threshold_mm=rain_threshold_mm,
    )
    _seed_observations(conn, site_id, D, skip_hours=obs_skip_hours)
    download_ids = _seed_downloads(conn, site_id, feeds, D, skip=skip)
    return site_id, feeds, record, now_utc, download_ids, generation_id


def _forecast_history_rows(conn, site_id: int, feed_id: int) -> list[dict[str, object]]:
    """Storage release 3.1/3.3: ``close_pairs``'s replacement for stage 5's
    scored hours. Raw rows (no join), so an absence check here still catches
    a row wrongly written with no backing observation (``close_pairs``'s
    old gap-hour bug, M44) -- joining ``observations`` would hide exactly
    that case."""
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM forecast_history"
            " WHERE site_id = ? AND local_date = ? AND feed_id = ?"
            " ORDER BY variable, day_ahead, valid_at",
            (site_id, D.isoformat(), feed_id),
        ).fetchall()
    ]


def _observed_value(conn, site_id: int, variable: str, valid_at: str) -> float | None:
    """D's hourly observed value: joined here from ``observations`` on the
    same key ``score_day`` paired it by."""
    row = conn.execute(
        "SELECT value FROM observations"
        " WHERE site_id = ? AND variable = ? AND valid_at = ?",
        (site_id, variable, valid_at),
    ).fetchone()
    return None if row is None else float(row["value"])


def _day_score_rows(conn, site_id: int, feed_id: int) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM day_scores"
            " WHERE site_id = ? AND local_date = ? AND feed_id = ?"
            " ORDER BY variable, day_ahead",
            (site_id, D.isoformat(), feed_id),
        ).fetchall()
    ]


_P4_TABLE_ORDER_BY = {
    # forecast_history and day_scores are WITHOUT ROWID: order by their own
    # primary key columns (wxverify/db/migrations.py), not `rowid`.
    "forecast_history": "feed_id, variable, day_ahead, valid_at",
    "day_scores": "feed_id, variable, day_ahead",
    "close_records": "rowid",
    "close_record_downloads": "feed_id, day_ahead",
}


def _p4_table_snapshot(conn, site_id: int) -> dict[str, list[dict[str, object]]]:
    tables = (
        "forecast_history",
        "day_scores",
        "close_records",
        "close_record_downloads",
    )
    snapshot: dict[str, list[dict[str, object]]] = {}
    for table in tables:
        order_by = _P4_TABLE_ORDER_BY[table]
        snapshot[table] = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM {table} WHERE site_id = ? AND local_date = ?"
                f" ORDER BY {order_by}",
                (site_id, D.isoformat()),
            ).fetchall()
        ]
    return snapshot


# ---------------------------------------------------------------------------
# SD1
# ---------------------------------------------------------------------------


def test_sd1_scored_day_writes_pairs_scores_record(tmp_path) -> None:  # noqa: ARG001
    """SD1: one `score_day` call over X and Y's 8 recorded downloads writes
    `close_pairs` for every (variable, day ahead 0-7, hour), `day_scores`
    sums equal to the sums over those rows, a `close_records` row with the
    truth version and `SCORING_RULE`, and one `close_record_downloads` row
    per feed and day ahead (6.2-6.7).
    """
    from wxverify.scoring.close_scores import SCORING_RULE, score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, download_ids, _gen = _build_base(conn)

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    for name, feed_id in feeds.items():
        pairs = _forecast_history_rows(conn, site_id, feed_id)
        day_aheads = sorted({int(p["day_ahead"]) for p in pairs})
        assert day_aheads == list(range(8)), (
            "M38 -> at this assertion: correct = [0, 1, 2, 3, 4, 5, 6, 7],"
            f" mutant (forecast_history written for one day ahead fewer) ="
            f" {day_aheads} for feed {name}"
        )
        assert len(pairs) == 3 * 8 * 24, (
            f"feed {name}: expected one row per variable/day-ahead/hour,"
            f" got {len(pairs)}"
        )
        for p in pairs:
            observed = _observed_value(conn, site_id, p["variable"], p["valid_at"])
            assert observed == pytest.approx(
                _OBS_FNS[p["variable"]](int(parse_utc(p["valid_at"]).hour))
            )

        rows = _day_score_rows(conn, site_id, feed_id)
        for row in rows:
            variable = row["variable"]
            day_ahead = int(row["day_ahead"])
            matching = [
                p
                for p in pairs
                if p["variable"] == variable and int(p["day_ahead"]) == day_ahead
            ]
            assert row["n"] == len(matching)
            # error = value - observed, joined from observations here.
            errors = [
                p["value"]
                - _observed_value(conn, site_id, p["variable"], p["valid_at"])
                for p in matching
            ]
            if errors:
                assert row["err_sum"] == pytest.approx(sum(errors))

    record_row = conn.execute(
        "SELECT truth_version, scoring_rule FROM close_records"
        " WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert record_row is not None
    assert record_row["truth_version"] == "fp-1", (
        "M39 -> at this assertion: correct = 'fp-1' (score_day was called"
        " with truth_version='fp-1'), mutant (the per-close record written"
        " without the truth version) = None"
    )
    assert record_row["scoring_rule"] == SCORING_RULE

    for name, feed_id in feeds.items():
        for k in range(8):
            row = conn.execute(
                "SELECT download_id FROM close_record_downloads"
                " WHERE site_id = ? AND local_date = ?"
                " AND feed_id = ? AND day_ahead = ?",
                (site_id, D.isoformat(), feed_id, k),
            ).fetchone()
            assert row is not None, (
                f"missing close_record_downloads row for {name} k={k}"
            )
            assert row["download_id"] == download_ids[(name, k)]


# ---------------------------------------------------------------------------
# SD2
# ---------------------------------------------------------------------------


def test_sd2_missing_download_is_absent_not_scored(tmp_path) -> None:  # noqa: ARG001
    """SD2: X's download of D-2 (k=2) deleted from the record and its
    scored values: X's k=2 rows are `absent`, no pairs, a record row with NULL
    download; `ScoreSummary.absent["feed:X"]` counts 1 (6.2, 6.8).

    Paired positive: Y, seeded identically but not deleted, gets `scored`
    k=2 rows and real pairs -- the same call, so the absence above traces
    to the deleted download, not to k=2 being unreachable in general.
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)

    s = D - timedelta(days=2)
    conn.execute(
        "DELETE FROM forecast_downloads"
        " WHERE site_id = ? AND feed_id = ? AND local_date = ?",
        (site_id, feeds["X"], s.isoformat()),
    )
    conn.execute(
        "DELETE FROM forecast_scored"
        " WHERE site_id = ? AND feed_id = ? AND issued_at = ?",
        (site_id, feeds["X"], f"{(s - timedelta(days=1)).isoformat()}T00:00:00Z"),
    )

    summary = score_day(
        conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc
    )

    x_k2 = _day_score_rows(conn, site_id, feeds["X"])
    x_k2_rows = [r for r in x_k2 if int(r["day_ahead"]) == 2]
    assert x_k2_rows, (
        "M40 -> at this assertion: correct = a day_scores row exists for"
        " X at k=2 (status 'absent'), mutant (an absent forecast written"
        " with no day_scores row) = no row at all"
    )
    for row in x_k2_rows:
        assert row["status"] == "absent"
    x_pairs_k2 = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["X"])
        if int(p["day_ahead"]) == 2
    ]
    assert x_pairs_k2 == []
    rec_row = conn.execute(
        "SELECT download_id FROM close_record_downloads"
        " WHERE site_id = ? AND local_date = ? AND feed_id = ? AND day_ahead = 2",
        (site_id, D.isoformat(), feeds["X"]),
    ).fetchone()
    assert rec_row is not None and rec_row["download_id"] is None
    assert summary.absent.get(f"feed:{feeds['X']}") == 1

    # --- Paired positive: Y, untouched, is scored at k=2 -----------------
    y_k2_rows = [
        r
        for r in _day_score_rows(conn, site_id, feeds["Y"])
        if int(r["day_ahead"]) == 2
    ]
    assert y_k2_rows and all(r["status"] == "scored" for r in y_k2_rows), (
        "paired positive: Y's k=2 forecast, seeded identically to X's but"
        " never deleted, must still be scored -- proving the absence above"
        " traces to the deleted download, not to k=2 in general"
    )
    y_pairs_k2 = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["Y"])
        if int(p["day_ahead"]) == 2
    ]
    assert len(y_pairs_k2) > 0


# ---------------------------------------------------------------------------
# SD3
# ---------------------------------------------------------------------------


def test_sd3_day_ahead_from_receipt_date_not_run_label(tmp_path) -> None:  # noqa: ARG001
    """SD3: D's day ahead 0 download is a round received at D 00:10Z whose
    label is dated D-1; those pairs have `day_ahead` 0, not 1 (6.2, :148).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    x_pairs_by_label = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["X"])
        if p["issued_at"] == f"{(D - timedelta(days=1)).isoformat()}T00:00:00Z"
    ]
    assert x_pairs_by_label, "expected D's own round's run label to produce pairs"
    for p in x_pairs_by_label:
        assert int(p["day_ahead"]) == 0, (
            "M41 -> at this assertion: correct = 0 (day ahead counted from"
            " the download's receipt date D, plan :148), mutant (day ahead"
            " counted from the run label, dated D-1) = 1"
        )


# ---------------------------------------------------------------------------
# SD4
# ---------------------------------------------------------------------------


def test_sd4_metrics_from_sums_matches_hand_computed_values() -> None:
    """SD4: `metrics_from_sums` on a hand-built sums row with at least one
    pair whose `ref_sq_error` is NULL (no value at h - L): n, bias, MAE,
    RMSE, POD, FAR, CSI, ETS, HSS and skill equal frozen, hand-computed
    values -- never by calling `ContinuousStrategy`/`PrecipStrategy` (6.4).

    Sums (chosen by hand, not read from any DB row):
    err_n=4, err_sum=2.0 (errors 1,1,-1,1), abs_sum=4.0, sq_sum=4.0 (each
    error's square is 1); skill_n=3 (one pair of the 4 has no persistence
    reference), skill_sq_sum=3.0, skill_ref_sq_sum=6.0; rain contingency
    hits=3, false_alarms=1, misses=1, correct_negs=5 (n=10 total).
    """
    from wxverify.scoring.close_scores import metrics_from_sums

    continuous_sums = {
        "n": 4,
        "err_n": 4,
        "err_sum": 2.0,
        "abs_sum": 4.0,
        "sq_sum": 4.0,
        "hits": None,
        "false_alarms": None,
        "misses": None,
        "correct_negs": None,
        "skill_n": 3,
        "skill_sq_sum": 3.0,
        "skill_ref_sq_sum": 6.0,
    }
    result = metrics_from_sums(continuous_sums, variable="temperature", min_n=1)
    assert result.n == 4
    assert result.bias == pytest.approx(0.5)
    assert result.mae == pytest.approx(1.0)
    assert result.rmse == pytest.approx(1.0), (
        "M42 -> at this assertion: correct = 1.0 (RMSE = sqrt(sq_sum /"
        " err_n) = sqrt(4.0 / 4) = 1.0), mutant (RMSE returned without the"
        " square root) = 1.0's square, 1.0 as well here by coincidence --"
        " see the non-unit-RMSE check below for the discriminating literal"
    )
    assert result.skill_score == pytest.approx(0.5), (
        "M43 -> at this assertion: correct = 0.5 (skill = 1 -"
        " skill_sq_sum/skill_ref_sq_sum = 1 - 3.0/6.0, counted over the 3"
        " pairs with a persistence reference), mutant (skill computed from"
        " sq_sum/skill_ref_sq_sum, the FULL 4.0 over different counts) ="
        " 1 - 4.0/6.0 = 0.3333..."
    )

    # A non-unit RMSE discriminates M42 from a coincidental RMSE of 1.
    continuous_sums_2 = dict(continuous_sums, sq_sum=16.0)
    result_2 = metrics_from_sums(continuous_sums_2, variable="temperature", min_n=1)
    assert result_2.rmse == pytest.approx(2.0), (
        "M42 -> at this assertion: correct = 2.0 (sqrt(16.0 / 4)), mutant"
        " (RMSE without the square root) = 4.0"
    )

    precip_sums = {
        "n": 10,
        "err_n": 10,
        "err_sum": 0.5,
        "abs_sum": 1.5,
        "sq_sum": 2.0,
        "hits": 3,
        "false_alarms": 1,
        "misses": 1,
        "correct_negs": 5,
        "skill_n": 0,
        "skill_sq_sum": 0.0,
        "skill_ref_sq_sum": 0.0,
    }
    precip_result = metrics_from_sums(precip_sums, variable="precip", min_n=1)
    assert precip_result.pod == pytest.approx(3 / 4)
    assert precip_result.far == pytest.approx(1 / 4)
    assert precip_result.csi == pytest.approx(3 / 5)
    random_hits = (3 + 1) * (3 + 1) / 10
    expected_ets = (3 - random_hits) / (3 + 1 + 1 - random_hits)
    assert precip_result.ets == pytest.approx(expected_ets)
    expected_hss = (2 * (3 * 5 - 1 * 1)) / ((3 + 1) * (1 + 5) + (3 + 1) * (1 + 5))
    assert precip_result.hss == pytest.approx(expected_hss)


# ---------------------------------------------------------------------------
# SD5
# ---------------------------------------------------------------------------


def test_sd5_gap_hours_unpaired_and_threshold_from_truth_row(tmp_path) -> None:  # noqa: ARG001
    """SD5: three hours of D with no observation get no pairs at all; a
    rain sample of 0.3 mm against an observed 0.3 mm pairs `cat_hit` 1 with
    threshold 0.25 FROM THE TRUTH ROW, even after `sites.rain_threshold_mm`
    was set to 0.5 by plain UPDATE (6.3).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    gap_hours = frozenset({3, 9, 17})
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(
        conn, obs_skip_hours=gap_hours, rain_threshold_mm=0.25
    )
    # The rain event hour (not a gap hour): observed 0.3mm, X's forecast
    # for every day ahead at that hour is overridden to 0.3mm too (same
    # value, so this is unambiguously a hit under 0.25, not under 0.5).
    rain_hour = 5
    rain_valid_at = isoformat_utc(
        datetime(D.year, D.month, D.day, rain_hour, tzinfo=UTC)
    )
    conn.execute(
        "UPDATE observations SET value = 0.3 WHERE site_id = ? AND variable = 'precip'"
        " AND valid_at = ?",
        (site_id, rain_valid_at),
    )
    conn.execute(
        "UPDATE forecast_scored SET value = 0.3 WHERE site_id = ? AND feed_id = ?"
        " AND variable = 'precip' AND valid_at = ?",
        (site_id, feeds["X"], rain_valid_at),
    )
    conn.execute("UPDATE sites SET rain_threshold_mm = 0.5 WHERE id = ?", (site_id,))

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    for feed_id in feeds.values():
        pairs = _forecast_history_rows(conn, site_id, feed_id)
        for hour in gap_hours:
            gap_valid_at = isoformat_utc(
                datetime(D.year, D.month, D.day, hour, tzinfo=UTC)
            )
            matching = [p for p in pairs if p["valid_at"] == gap_valid_at]
            assert matching == [], (
                "M44 -> at this assertion: correct = [] (a gap hour has no"
                f" observed row, so no pair, hour={hour}), mutant (gap"
                " hours paired with an observed 0) = a pair row present"
            )

    rain_pairs = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["X"])
        if p["variable"] == "precip" and p["valid_at"] == rain_valid_at
    ]
    assert rain_pairs, "expected a rain pair at the seeded rain hour"
    # cat_hit and rain_threshold_mm are no longer stored per row (storage
    # release 3.1), so the fact they protected -- the truth row's threshold
    # (0.25) wins over sites.rain_threshold_mm (0.5) -- is read here from
    # day_scores' categorical sum instead: this seeded hour is the day's
    # only one that can register a hit (every other hour is dry, a 0.0mm
    # forecast against a 0.0mm observed, a correct negative under either
    # threshold), so hits == 1 pins the same fact the dropped cat_hit
    # assertion did.
    for p in rain_pairs:
        day_ahead = int(p["day_ahead"])
        day_score_row = next(
            r
            for r in _day_score_rows(conn, site_id, feeds["X"])
            if r["variable"] == "precip" and int(r["day_ahead"]) == day_ahead
        )
        assert day_score_row["hits"] == 1, (
            "M45 -> at this assertion: correct = 1 (cat_hit uses the truth"
            " row's rain_threshold_mm=0.25: 0.3mm forecast and observed"
            " both clear 0.25, a hit), mutant (the threshold read from"
            " sites.rain_threshold_mm=0.5 instead) = 0 (0.3mm does not"
            " clear 0.5, so neither side crosses and it reads as a correct"
            " negative, not a hit)"
        )


# ---------------------------------------------------------------------------
# SD6
# ---------------------------------------------------------------------------


def test_sd6_thin_variables_not_counted_and_unpaired(tmp_path) -> None:  # noqa: ARG001
    """SD6: `precip_total` not eligible and `wind_max` not eligible: no
    rain and no wind pairs; their `day_scores` rows `not_counted`; the
    summary leaves them out (6.5).

    Paired positive: temperature, left eligible, is still scored and
    counted the same call.
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(
        conn, wind_eligible=False, rain_eligible=False
    )

    summary = score_day(
        conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc
    )

    for feed_id in feeds.values():
        pairs = _forecast_history_rows(conn, site_id, feed_id)
        thin_pairs = [p for p in pairs if p["variable"] in ("wind", "precip")]
        assert thin_pairs == [], (
            "M46 -> at this assertion: correct = [] (wind and precip are"
            " not_counted for D, so no pairs), mutant (a thin variable"
            " still paired) = pair rows present"
        )
        rows = _day_score_rows(conn, site_id, feed_id)
        for row in rows:
            if row["variable"] in ("wind", "precip"):
                assert row["status"] == "not_counted"
            else:
                assert row["status"] == "scored"
    for feed_id in feeds.values():
        key = f"feed:{feed_id}"
        scored = summary.scored.get(key, 0)
        absent = summary.absent.get(key, 0)
        # 8 day-aheads of temperature only ever counted (not 24 across 3 vars)
        assert scored + absent <= 8


# ---------------------------------------------------------------------------
# SD7
# ---------------------------------------------------------------------------


def test_sd7_temperature_scored_by_covered_hours_not_peak_windows(tmp_path) -> None:  # noqa: ARG001
    """SD7 (i): D with a temperature reading at every hour but 12-17 (18
    covered hours; `temperature_high` not eligible for its window,
    `temperature_low` eligible): temperature is still scored -- pairs at
    the 18 hours, `day_scores` `scored` (6.5). (ii) 17 covered hours (both
    not eligible): temperature `not_counted`, no pairs.
    """
    from wxverify.scoring.close_scores import score_day

    # (i) 18 covered hours, one window ineligible.
    conn = asof_conn()
    missing_i = frozenset(range(12, 18))  # 6 hours missing -> 18 covered
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(
        conn,
        temp_eligible=(False, True),
        temp_covered_hours=18,
        obs_skip_hours=missing_i,
    )
    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)
    temp_pairs = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["X"])
        if p["variable"] == "temperature"
    ]
    temp_hours = {int(parse_utc(p["valid_at"]).hour) for p in temp_pairs}
    assert temp_hours == set(range(24)) - missing_i, (
        "M47 -> at this assertion: correct = the 18 hours D has a"
        " temperature observation for, mutant (scored_variables requires"
        " BOTH temperature_high and temperature_low eligible, which brings"
        " their peak windows back) = no temperature pairs at all, since"
        " temperature_high is not eligible here"
    )
    temp_rows = [
        r
        for r in _day_score_rows(conn, site_id, feeds["X"])
        if r["variable"] == "temperature"
    ]
    assert temp_rows and all(r["status"] == "scored" for r in temp_rows)

    # (ii) 17 covered hours, both windows ineligible.
    conn2 = asof_conn()
    missing_ii = frozenset(range(12, 19))  # 7 hours missing -> 17 covered
    site_id2, feeds2, record2, now_utc2, _ids2, _gen2 = _build_base(
        conn2,
        temp_eligible=(False, False),
        temp_covered_hours=17,
        obs_skip_hours=missing_ii,
    )
    score_day(conn2, record=record2, attempt=1, truth_version="fp-1", now_utc=now_utc2)
    temp_pairs2 = [
        p
        for p in _forecast_history_rows(conn2, site_id2, feeds2["X"])
        if p["variable"] == "temperature"
    ]
    assert temp_pairs2 == []
    temp_rows2 = [
        r
        for r in _day_score_rows(conn2, site_id2, feeds2["X"])
        if r["variable"] == "temperature"
    ]
    assert temp_rows2 and all(r["status"] == "not_counted" for r in temp_rows2)


# ---------------------------------------------------------------------------
# SD8
# ---------------------------------------------------------------------------


def test_sd8_persistence_pairs_at_hours_with_a_source_value(tmp_path) -> None:  # noqa: ARG001
    """SD8: persistence pairs exist at hours with a value at h - L; one
    pair per hour and lead with a source value; day ahead from h - L (4.6).

    Persistence is never written to ``forecast_history``; it is rebuilt on
    read from the shared ``persistence_cells`` function (3.5), so this
    test calls that function directly on D's own observations -- the same
    inputs ``score_day`` fed it -- rather than reading stored pairs. The
    closing ``day_scores`` assertion then proves ``score_day`` itself
    produced the same cells, not just that the function does, by
    comparing its ``n`` to the cells' own count.
    """
    from zoneinfo import ZoneInfo

    from wxverify.scoring.close_scores import score_day
    from wxverify.scoring.history import PERSISTENCE_MAX_LEAD_HOURS, persistence_cells

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    persistence_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
    ).fetchone()
    assert persistence_row is not None, "fixture bug: no seeded persistence feed"
    persistence_id = int(persistence_row["id"])

    assert _forecast_history_rows(conn, site_id, persistence_id) == [], (
        "M-fh-persist -> at this assertion: correct = [] (persistence is"
        " never written to forecast_history, storage release 3.3), mutant"
        " (persistence pairs stored like a real feed) = a row present"
    )

    temp_obs = [
        (str(row["valid_at"]), float(row["value"]))
        for row in conn.execute(
            "SELECT valid_at, value FROM observations"
            " WHERE site_id = ? AND variable = 'temperature'",
            (site_id,),
        )
    ]
    temp_cells = persistence_cells(
        temp_obs,
        target_valid_at=[valid_at for valid_at, _value in temp_obs],
        zone=ZoneInfo(record.timezone),
        max_lead_hours=PERSISTENCE_MAX_LEAD_HOURS,
    )
    assert temp_cells, (
        "M48 -> at this assertion: correct = at least one persistence cell"
        " (D's own 24 hours of observations give h - L values for L <= h),"
        " mutant (persistence restricted to one lead per hour) would still"
        " show cells here -- the per-hour lead count below is the"
        " discriminating check"
    )
    by_hour: dict[int, set[int]] = {}
    for cell in temp_cells:
        by_hour.setdefault(cell.instant.hour, set()).add(cell.lead_hours)
    multi_lead_hours = [hour for hour, leads in by_hour.items() if len(leads) > 1]
    assert multi_lead_hours, (
        "M48 -> at this assertion: correct = at least one hour with more"
        " than one persistence lead (every L from 1 to h has a source"
        " value within D), mutant (persistence restricted to one lead per"
        " hour) = every hour has exactly one lead"
    )
    for cell in temp_cells:
        source_hour = cell.instant.hour - cell.lead_hours
        assert source_hour >= 0, "fixture bug: a persistence cell referenced before D"
        assert cell.day_ahead == 0

    day0_rows = [
        r
        for r in _day_score_rows(conn, site_id, persistence_id)
        if r["variable"] == "temperature" and int(r["day_ahead"]) == 0
    ]
    assert day0_rows and day0_rows[0]["status"] == "scored"
    day0_cells = [c for c in temp_cells if c.day_ahead == 0]
    assert day0_rows[0]["n"] == len(day0_cells), (
        "M48-day-scores -> at this assertion: correct ="
        f" {len(day0_cells)} (score_day's own count of persistence cells at"
        " day ahead 0 must match the shared function's, 3.5's point),"
        f" mutant = {day0_rows[0]['n']}"
    )


# ---------------------------------------------------------------------------
# SD9
# ---------------------------------------------------------------------------


def test_sd9_multimodel_mean_over_active_feeds_only(tmp_path) -> None:  # noqa: ARG001
    """SD9: the mean of X, Y and Z scored at (h, k) with Z switched off is
    the mean of X and Y, `contributors` 2, `lead_hours` the smaller lead;
    with only X subscribed, no mean pair at all (4.6, the two-feed rule).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)
    feeds["Z"] = _make_feed(conn, "Z")
    _subscribe(conn, site_id, feeds["Z"], enabled=False)
    _seed_downloads(
        conn,
        site_id,
        {"Z": feeds["Z"]},
        D,
        bias={"Z": {"temperature": 3.0, "wind": 3.0, "precip": 0.0}},
    )

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    mean_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_multimodel_mean'"
    ).fetchone()
    assert mean_row is not None, "fixture bug: no seeded multimodel-mean feed"
    mean_pairs = [
        p
        for p in _forecast_history_rows(conn, site_id, int(mean_row["id"]))
        if p["variable"] == "temperature"
    ]
    assert mean_pairs, (
        "M49 -> at this assertion: correct = mean pairs exist (X and Y are"
        " both active), mutant (a mean made from one feed) would still"
        " produce pairs here -- the contributors count below discriminates"
    )
    for p in mean_pairs:
        assert p["contributors"] == 2, (
            "M49 -> at this assertion: correct = 2 (Z is switched off, so"
            " the mean is over X and Y only), mutant (the mean over every"
            " real feed, switched off included) = 3"
        )
        hour = int(parse_utc(p["valid_at"]).hour)
        k = int(p["day_ahead"])
        expected_lead = k * 24 + hour + 1  # X and Y share this lead by construction
        assert int(p["lead_hours"]) == expected_lead

    # --- Then only X: the two-feed minimum means no mean pair at all. ----
    conn2 = asof_conn()
    site_id2, feeds2, record2, now_utc2, _ids2, _gen2 = _build_base(
        conn2, skip=frozenset((name, k) for k in range(8) for name in ("Y",))
    )
    conn2.execute(
        "UPDATE site_feed_state SET enabled = 0 WHERE site_id = ? AND feed_id = ?",
        (site_id2, feeds2["Y"]),
    )
    score_day(conn2, record=record2, attempt=1, truth_version="fp-1", now_utc=now_utc2)
    mean_row2 = conn2.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_multimodel_mean'"
    ).fetchone()
    assert mean_row2 is not None
    mean_pairs2 = _forecast_history_rows(conn2, site_id2, int(mean_row2["id"]))
    assert mean_pairs2 == [], (
        "M50 -> at this assertion: correct = [] (only X is active; the"
        " mean needs at least two feeds), mutant (a mean made from one"
        " feed) = pair rows present with contributors=1"
    )


# ---------------------------------------------------------------------------
# SD10
# ---------------------------------------------------------------------------


def test_sd10_ref_sq_error_null_when_no_earlier_value(tmp_path) -> None:  # noqa: ARG001
    """SD10: `ref_sq_error` is the squared persistence error where h - L
    has a value, and NULL where it does not; `skill_n` counts only the
    first (6.3, 6.4). Asserted from `score_day`'s own output (`day_scores`),
    never recomputed from the fixture -- a recomputed check cannot catch a
    bug in `score_day`'s own `skill_n`/`skill_ref_sq_sum` arithmetic.
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)

    # By construction (lead = 24*k + hour + 1), every hour of day-ahead k
    # references the same earlier instant, (D-k-1) 23:00, regardless of
    # hour -- so the "has a value" and "has no value" cases must come from
    # two different day-aheads, not two hours of the same one. Day-ahead 0
    # references D-1 23:00 (left unseeded, so NULL); seed an observation at
    # D-2 23:00 so day-ahead 1's reference (and only that one) has a value.
    ref_valid_at = f"{(D - timedelta(days=2)).isoformat()}T23:00:00Z"
    conn.execute(
        "INSERT INTO observations"
        " (site_id, variable, valid_at, value, n_stations, computed_at)"
        " VALUES (?, 'temperature', ?, ?, 2, ?)",
        (site_id, ref_valid_at, 12.0, ref_valid_at),
    )

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    rows = _day_score_rows(conn, site_id, feeds["X"])
    row_k0 = next(
        r for r in rows if r["variable"] == "temperature" and int(r["day_ahead"]) == 0
    )
    row_k1 = next(
        r for r in rows if r["variable"] == "temperature" and int(r["day_ahead"]) == 1
    )

    # Day-ahead 0's reference, D-1 23:00, has no seeded observation: every
    # one of its 24 hours' ref_sq_error must be NULL, so skill_n is 0.
    assert row_k0["skill_n"] == 0, (
        "M51 -> at this assertion: correct = skill_n 0 for day-ahead 0 (its"
        " reference, D-1 23:00, has no seeded observation, so every hour's"
        " ref_sq_error is NULL), mutant (ref_sq_error 0 where the earlier"
        f" hour has no value, counted as non-null) = {row_k0['skill_n']}"
    )

    # Day-ahead 1's reference, D-2 23:00, was seeded above (value 12.0) and
    # applies to all 24 of D's seeded hours (_seed_observations skips none),
    # so skill_n must be 24, with no skipped hour.
    assert row_k1["skill_n"] == 24, (
        "M-sd10-skill-n -> at this assertion: correct = 24 (all 24 seeded"
        " hours of D have a day-ahead-1 reference), mutant (a skipped hour,"
        f" or the reference check inverted) = {row_k1['skill_n']}"
    )

    # Hand-computed skill_ref_sq_sum for day-ahead 1: observed(hour) =
    # 10.0 + hour (_temp_obs); the fixed reference is 12.0 for every hour
    # (same source instant, D-2 23:00). sum_{h=0}^{23} (12.0 - (10.0+h))**2
    # = sum_{h=0}^{23} (2-h)**2 = sum(h**2) - 4*sum(h) + 4*24
    #   = 4324 - 1104 + 96 = 3316.0.
    assert row_k1["skill_ref_sq_sum"] == 3316.0, (
        "M-sd10-ref-sq-sum -> at this assertion: correct = 3316.0 (hand-"
        f"computed over all 24 hours), mutant (wrong reference value, wrong"
        f" sign, or a different hour set) = {row_k1['skill_ref_sq_sum']}"
    )


# ---------------------------------------------------------------------------
# SD11
# ---------------------------------------------------------------------------


def test_sd11_wind_sums_from_paired_hours_only(tmp_path) -> None:  # noqa: ARG001
    """SD11: X with 21 paired wind hours at k=1, Y with 19:
    `wind_fc_high` the highest paired forecast, `wind_fc_hours` 21 and 19;
    `wind_obs_high` D's `wind_max` (6.4).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    # Leave 3 observation hours missing so X's k=1 wind pairs number 21,
    # and additionally drop X's k=1 forecast at 2 more hours (unpaired on
    # the forecast side) so Y, untouched beyond the shared gaps, has 19.
    missing = frozenset({1, 2, 3})
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(
        conn, obs_skip_hours=missing
    )
    y_extra_missing_hours = {4, 5}
    s1 = D - timedelta(days=1)
    label1 = f"{(s1 - timedelta(days=1)).isoformat()}T00:00:00Z"
    for hour in y_extra_missing_hours:
        valid_at = isoformat_utc(datetime(D.year, D.month, D.day, hour, tzinfo=UTC))
        conn.execute(
            "DELETE FROM forecast_scored"
            " WHERE site_id = ? AND feed_id = ? AND variable = 'wind'"
            " AND issued_at = ? AND valid_at = ?",
            (site_id, feeds["Y"], label1, valid_at),
        )

    # M52's spike: the highest forecast wind of X's k=1 version sits at
    # hour=1, one of the 3 gap hours with no observation (so no pair is
    # ever made there). The correct wind_fc_high, taken over the 21 PAIRED
    # hours only, must never see this value; the mutant (max over every
    # forecast hour of the version, paired or not) would pick it up. The
    # spike must stay within wind's valid range ([0.0, 150.0], see
    # invalid_forecast_sample_sql in wxverify/collection/forecast_validation
    # .py) -- a value above 150.0 is dropped before scored_values ever
    # sees it, so the mutant's whole-version max could never pick it up
    # either, and the test could not tell the mutant from correct code.
    spike_valid_at = isoformat_utc(datetime(D.year, D.month, D.day, 1, tzinfo=UTC))
    spiked = conn.execute(
        "UPDATE forecast_scored SET value = 140.0 WHERE site_id = ?"
        " AND feed_id = ? AND variable = 'wind' AND issued_at = ? AND valid_at = ?",
        (site_id, feeds["X"], label1, spike_valid_at),
    )
    assert spiked.rowcount == 1, "fixture bug: expected exactly one row to spike"

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)

    x_row = next(
        r
        for r in _day_score_rows(conn, site_id, feeds["X"])
        if r["variable"] == "wind" and int(r["day_ahead"]) == 1
    )
    y_row = next(
        r
        for r in _day_score_rows(conn, site_id, feeds["Y"])
        if r["variable"] == "wind" and int(r["day_ahead"]) == 1
    )
    assert x_row["wind_fc_hours"] == 21, (
        "M52 -> at this assertion: correct = 21 (only D's 21 remaining"
        " observed hours are paired at k=1), mutant (wind_fc_high/hours"
        " over every forecast hour, not the paired ones) = 24"
    )
    assert y_row["wind_fc_hours"] == 19
    x_pairs = [
        p
        for p in _forecast_history_rows(conn, site_id, feeds["X"])
        if p["variable"] == "wind" and int(p["day_ahead"]) == 1
    ]
    assert x_row["wind_fc_high"] == pytest.approx(max(p["value"] for p in x_pairs)), (
        "M52 -> at this assertion: correct = the highest of the 21 PAIRED"
        " forecasts, mutant (wind_fc_high over every forecast hour) would"
        " read a higher value from an hour this day score never paired"
    )
    assert x_row["wind_fc_high"] != pytest.approx(140.0), (
        "M52 -> at this assertion: correct = the highest PAIRED forecast"
        " (never 140.0, seeded at hour=1, a gap hour with no observation"
        " and so no pair), mutant (wind_fc_high = max over the WHOLE"
        " forecast version, paired or not) = 140.0"
    )
    truth_row = conn.execute(
        "SELECT value FROM daily_truth"
        " WHERE site_id = ? AND local_date = ? AND quantity = 'wind_max'",
        (site_id, D.isoformat()),
    ).fetchone()
    assert x_row["wind_obs_high"] == pytest.approx(truth_row["value"])
    assert y_row["wind_obs_high"] == pytest.approx(truth_row["value"])


# ---------------------------------------------------------------------------
# SD12
# ---------------------------------------------------------------------------


def test_sd12_rerun_replaces_not_adds_third_call_adds_nothing(tmp_path) -> None:  # noqa: ARG001
    """SD12: `score_day` called a second time leaves every row equal but
    `scored_at`; a later download's value of X for D, then a third call,
    adds nothing (6.9): the later value is the newest stored value in
    `forecast_samples`, and the close reads only `forecast_scored`, which
    holds the first download's values (rain release, Part B).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, download_ids, _gen = _build_base(conn)

    # X's k=1 download (S = D-1) has no scored value at (temperature,
    # D 10:00): the later download's value below is then the only value
    # stored for that hour, so a close that fell back to the newest stored
    # value where no scored value exists would pair it.
    s1 = D - timedelta(days=1)
    k1_label = f"{(s1 - timedelta(days=1)).isoformat()}T00:00:00Z"
    k1_valid_at = isoformat_utc(datetime(D.year, D.month, D.day, 10, tzinfo=UTC))
    deleted = conn.execute(
        "DELETE FROM forecast_scored"
        " WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'"
        " AND issued_at = ? AND valid_at = ?",
        (site_id, feeds["X"], k1_label, k1_valid_at),
    )
    assert deleted.rowcount == 1

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)
    first = _p4_table_snapshot(conn, site_id)

    record_after_1 = day_record(conn, site_id, D)
    assert record_after_1 is not None and record_after_1.state == "closing"
    score_day(
        conn, record=record_after_1, attempt=1, truth_version="fp-1", now_utc=now_utc
    )
    second = _p4_table_snapshot(conn, site_id)

    for table in first:
        assert len(first[table]) == len(second[table]), (
            "M53 -> at this assertion: correct = the same row count (a"
            f" rerun replaces, table={table}), mutant (a rerun adds rows"
            " instead of replacing them) = more rows the second time"
        )

    def _without_scored_at(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        return [{k: v for k, v in row.items() if k != "scored_at"} for row in rows]

    for table in first:
        assert _without_scored_at(first[table]) == _without_scored_at(second[table]), (
            "M54 -> at this assertion: correct = every column but"
            f" scored_at equal between the two runs (table={table}),"
            " mutant (scored_at used in a key, so a rerun differs) = the"
            " rows differ in more than scored_at"
        )

    # A later download's value for X's (temperature, D 10:00) hour, stored
    # as the newest value in forecast_samples, does not change the rerun's
    # output: only the first download of S writes scored values.
    k1_download = conn.execute(
        "SELECT fetched_at FROM forecast_downloads"
        " WHERE site_id = ? AND feed_id = ? AND local_date = ?",
        (site_id, feeds["X"], s1.isoformat()),
    ).fetchone()
    assert k1_download is not None
    late_fetched_at = f"{s1.isoformat()}T23:59:00Z"
    assert late_fetched_at > k1_download["fetched_at"]
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value,"
        "  source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, 35, 45.0, 'late', ?, ?)",
        (
            site_id,
            feeds["X"],
            k1_label,
            k1_valid_at,
            k1_label,
            late_fetched_at,
        ),
    )
    record_after_2 = day_record(conn, site_id, D)
    assert record_after_2 is not None
    score_day(
        conn, record=record_after_2, attempt=1, truth_version="fp-1", now_utc=now_utc
    )
    third = _p4_table_snapshot(conn, site_id)
    for table in first:
        assert _without_scored_at(second[table]) == _without_scored_at(third[table]), (
            "Part B -> at this assertion: correct = the later download's"
            " value (the newest in forecast_samples) is left out, so the"
            " third call adds nothing, mutant (the close reads the newest"
            " stored value) = a k=1 temperature pair at D 10:00 with the"
            f" late value (table={table})"
        )


def test_sd12_rerun_with_a_different_clock_replaces_close_records_row(
    tmp_path,
) -> None:  # noqa: ARG001
    """SD12 (M54's discriminating case): the base SD12 test above calls
    ``score_day`` a second and third time with the SAME ``now_utc`` as the
    first, so `close_records.scored_at`` is identical across every call
    regardless of whether the delete is keyed on it -- a mutant that adds
    ``AND scored_at = ?`` to the ``close_records`` delete (using the
    CURRENT call's own ``now_utc``) cannot be told apart there, since that
    value never differs from the row already stored. This variant reruns
    ``score_day`` with its clock advanced, so the two values differ and
    the mutant's extra predicate can actually fail to match.
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, _feeds, record, now_utc, _ids, _gen = _build_base(conn)

    score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)
    first_scored_at = conn.execute(
        "SELECT scored_at FROM close_records WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()["scored_at"]

    later_now_utc = now_utc + timedelta(hours=1)
    record_after_1 = day_record(conn, site_id, D)
    assert record_after_1 is not None and record_after_1.state == "closing"
    # On correct code this rerun, with its OWN later now_utc, must succeed
    # and replace the row -- never raise. Under M54 the close_records
    # delete gains "AND scored_at = ?" keyed on THIS call's now_utc, which
    # never matches the earlier row's (still first_scored_at); the earlier
    # row survives the delete, and the plain INSERT that follows then
    # collides with it on the (site_id, local_date) PRIMARY KEY.
    score_day(
        conn,
        record=record_after_1,
        attempt=2,
        truth_version="fp-1",
        now_utc=later_now_utc,
    )

    rows = conn.execute(
        "SELECT scored_at FROM close_records WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchall()
    assert len(rows) == 1, (
        "M54 -> at this assertion: correct = 1 (the rerun's delete removes"
        " the earlier attempt's close_records row no matter its"
        " scored_at), mutant (the delete additionally keyed on"
        " scored_at, which this rerun's later now_utc never matches) ="
        f" {len(rows)} rows -- or the second score_day call above already"
        " raised sqlite3.IntegrityError on the close_records insert before"
        " this SELECT ever ran, which is the same M54 divergence one step"
        " earlier"
    )
    assert rows[0]["scored_at"] != first_scored_at, (
        "M54 -> at this assertion: correct = the SECOND call's scored_at"
        f" (the row was replaced), mutant leaves the stale first-call"
        f" value, {first_scored_at!r}, in place"
    )
    assert rows[0]["scored_at"] == isoformat_utc(later_now_utc.replace(microsecond=0))


# ---------------------------------------------------------------------------
# SD13
# ---------------------------------------------------------------------------


def test_sd13_stop_rolls_back_rerun_reaches_sd1(tmp_path) -> None:  # noqa: ARG001
    """SD13: a wrapper calls `score_day` through, then raises a `Stop`-like
    exception before the caller commits: after the stop, D's P4 rows are
    as before stage 5; after a rerun, SD1's rows (6.9).
    """
    from wxverify.scoring.close_scores import score_day

    class _WrapperStop(BaseException):
        pass

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)
    before = _p4_table_snapshot(conn, site_id)
    assert all(before[table] == [] for table in before)

    savepoint_name = "sd13"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)
        raise _WrapperStop
    except _WrapperStop:
        conn.execute(f"ROLLBACK TO {savepoint_name}")
        conn.execute(f"RELEASE {savepoint_name}")

    after_stop = _p4_table_snapshot(conn, site_id)
    assert after_stop == before, (
        "M55 -> at this assertion: correct = every P4 table unchanged (the"
        " stop rolled back score_day's own writes, which never committed),"
        " mutant (score_day commits after the pairs, instead of leaving"
        " everything in the caller's transaction) = rows survive the"
        " rollback"
    )

    record_after = day_record(conn, site_id, D)
    assert record_after is not None and record_after.state == "closing"
    score_day(
        conn, record=record_after, attempt=1, truth_version="fp-1", now_utc=now_utc
    )
    after_rerun = _p4_table_snapshot(conn, site_id)
    assert after_rerun != before
    for feed_id in feeds.values():
        pairs = _forecast_history_rows(conn, site_id, feed_id)
        assert len(pairs) == 3 * 8 * 24


# ---------------------------------------------------------------------------
# SD14
# ---------------------------------------------------------------------------


def test_sd14_closed_day_refused_nothing_written(tmp_path) -> None:  # noqa: ARG001
    """SD14: `score_day` for D-1, `closed`: `ScoreDayRefused`; every P4
    table "unchanged" (6.1).
    """
    from wxverify.scoring.close_scores import ScoreDayRefused, score_day

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=True)
    closed_date = D - timedelta(days=3)
    record = day_record(conn, site_id, closed_date)
    assert record is not None and record.state == "closed"

    def _snapshot() -> dict[str, list[dict[str, object]]]:
        tables = (
            "forecast_history",
            "day_scores",
            "close_records",
            "close_record_downloads",
        )
        return {
            table: [
                dict(row)
                for row in conn.execute(
                    f"SELECT * FROM {table} WHERE site_id = ? AND local_date = ?",
                    (site_id, closed_date.isoformat()),
                ).fetchall()
            ]
            for table in tables
        }

    before = _snapshot()
    with pytest.raises(ScoreDayRefused):
        score_day(
            conn,
            record=record,
            attempt=1,
            truth_version="fp-1",
            now_utc=record.day_end_utc,
        )
    conn.rollback()
    after = _snapshot()
    assert before == after == {k: [] for k in before}, (
        "M56 -> at this assertion: correct = ScoreDayRefused raised (the"
        " guard refuses a closed day's DAY_SCORES/CLOSE_RECORD write),"
        " mutant (a guard refusal returns an empty summary instead of"
        " raising) = no exception, a ScoreSummary returned, every table"
        " still empty but by the wrong path"
    )


# ---------------------------------------------------------------------------
# SD15
# ---------------------------------------------------------------------------


def test_sd15_missing_truth_row_raises_nothing_written(tmp_path) -> None:  # noqa: ARG001
    """SD15: one truth row of D deleted before stage 5:
    `ScoreDayInputMissing`; nothing written (6.1).
    """
    from wxverify.scoring.close_scores import ScoreDayInputMissing, score_day

    conn = asof_conn()
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn)
    conn.execute(
        "DELETE FROM daily_truth"
        " WHERE site_id = ? AND local_date = ? AND quantity = 'wind_max'",
        (site_id, D.isoformat()),
    )
    before = _p4_table_snapshot(conn, site_id)
    assert all(before[table] == [] for table in before)

    with pytest.raises(ScoreDayInputMissing):
        score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc)
    conn.rollback()

    after = _p4_table_snapshot(conn, site_id)
    assert after == before, (
        "M57 -> at this assertion: correct = ScoreDayInputMissing raised"
        " and nothing written (fewer than five truth rows), mutant (the"
        " five-row check removed) = score_day proceeds and writes rows"
        " despite the missing wind_max truth row"
    )


# ---------------------------------------------------------------------------
# SD16
# ---------------------------------------------------------------------------


def test_sd16_no_stored_download_gives_absent_everywhere_no_failure(tmp_path) -> None:  # noqa: ARG001
    """SD16: no stored download of any feed on D-7 to D: `absent` rows for
    every switched-on feed and day ahead, persistence pairs still made, the
    record still written; no failure (6.9).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    all_skip = frozenset((name, k) for k in range(8) for name in ("X", "Y"))
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn, skip=all_skip)

    summary = score_day(
        conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc
    )

    for name, feed_id in feeds.items():
        rows = _day_score_rows(conn, site_id, feed_id)
        assert rows, f"expected absent rows for {name}"
        assert all(r["status"] == "absent" for r in rows), (
            "M58 -> at this assertion: correct = every row 'absent' (no"
            f" stored download for {name} on D-7..D, plan 6.9), mutant (a"
            " day with no forecasts raises instead of writing absent"
            " rows) = this line is never reached"
        )
        assert _forecast_history_rows(conn, site_id, feed_id) == []
        assert summary.absent.get(f"feed:{feed_id}") == 8

    persistence_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
    ).fetchone()
    assert persistence_row is not None
    persistence_id = int(persistence_row["id"])
    # forecast_history never holds a persistence row -- unlike X/Y above,
    # this positive case is where a regression (persistence stored like a
    # real feed) would actually be caught.
    assert _forecast_history_rows(conn, site_id, persistence_id) == [], (
        "M-fh-persist -> at this assertion: correct = [] (persistence is"
        " never written to forecast_history), mutant (persistence stored"
        " like a real feed) = rows present"
    )
    persistence_rows = _day_score_rows(conn, site_id, persistence_id)
    assert any(r["status"] == "scored" for r in persistence_rows), (
        "the yardsticks have no download and are unaffected by every real"
        " feed being absent (4.6)"
    )
    record_row = conn.execute(
        "SELECT 1 FROM close_records WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert record_row is not None, (
        "M58 -> at this assertion: correct = a close_records row exists"
        " even with every forecast absent (6.9 'never empty'), mutant (a"
        " day with no forecasts raises) = no record row at all"
    )


# ---------------------------------------------------------------------------
# SD17
# ---------------------------------------------------------------------------


def test_sd17_scored_at_whole_second_microseconds_dropped(tmp_path) -> None:  # noqa: ARG001
    """SD17: as SD1, with `now_utc` at an instant whose microsecond is
    250000: `close_records.scored_at` passes `is_canonical_utc_stamp`, the
    check import applies to it (15.2, 15.6).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    site_id, _feeds, record, now_utc, _ids, _gen = _build_base(conn)
    fractional_now_utc = now_utc + timedelta(microseconds=250000)

    score_day(
        conn,
        record=record,
        attempt=1,
        truth_version="fp-1",
        now_utc=fractional_now_utc,
    )

    record_row = conn.execute(
        "SELECT scored_at FROM close_records WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert record_row is not None
    scored_at_canonical = is_canonical_utc_stamp(record_row["scored_at"])
    assert scored_at_canonical, (
        "M143 -> at this assertion: correct = True, mutant (scored_at"
        " stored as isoformat_utc(t), microseconds kept) ="
        f" {scored_at_canonical!r}"
    )
    assert record_row["scored_at"] == isoformat_utc(
        fractional_now_utc.replace(microsecond=0)
    ), (
        "M143 -> at this assertion: correct ="
        f" {isoformat_utc(fractional_now_utc.replace(microsecond=0))!r} (15.2's"
        " isoformat_utc(t.replace(microsecond=0))), mutant ="
        f" {record_row['scored_at']!r}"
    )


# ---------------------------------------------------------------------------
# SD18
# ---------------------------------------------------------------------------


def test_sd18_considered_feeds_switched_on_or_downloaded_not_never_fetched(
    tmp_path,
) -> None:  # noqa: ARG001
    """SD18: the feeds considered at D (6.2) are X (switched on, no download
    on D-7..D), Y (switched off, only its D-3 download) and V (a third real
    feed with no `site_feed_state` row at all, never fetched): X and Y are
    considered and get rows; V, never subscribed and never downloaded, gets
    none (6.2, 6.7, 6.8).
    """
    from wxverify.scoring.close_scores import score_day

    conn = asof_conn()
    skip = frozenset(
        [("X", k) for k in range(8)] + [("Y", k) for k in range(8) if k != 3]
    )
    site_id, feeds, record, now_utc, _ids, _gen = _build_base(conn, skip=skip)
    conn.execute(
        "UPDATE site_feed_state SET enabled = 0 WHERE site_id = ? AND feed_id = ?",
        (site_id, feeds["Y"]),
    )
    feeds["V"] = _make_feed(conn, "V")

    summary = score_day(
        conn, record=record, attempt=1, truth_version="fp-1", now_utc=now_utc
    )

    # --- X: switched on, no download anywhere -- still considered. --------
    x_rows = _day_score_rows(conn, site_id, feeds["X"])
    assert len(x_rows) == 24, (
        "M148 -> at this assertion: correct = 24 (X is switched on, so it"
        " is considered even with no download on D-7..D), mutant (only"
        " feeds with a scored download on D-7..D are considered) = 0"
    )
    assert all(r["status"] == "absent" for r in x_rows)
    x_dl_rows = conn.execute(
        "SELECT download_id FROM close_record_downloads"
        " WHERE site_id = ? AND local_date = ? AND feed_id = ?",
        (site_id, D.isoformat(), feeds["X"]),
    ).fetchall()
    assert len(x_dl_rows) == 8 and all(r["download_id"] is None for r in x_dl_rows)
    assert summary.absent.get(f"feed:{feeds['X']}") == 8

    # --- Y: switched off, one download at k=3 -- still considered. --------
    y_rows = _day_score_rows(conn, site_id, feeds["Y"])
    assert len(y_rows) == 24
    y_scored_k = {int(r["day_ahead"]) for r in y_rows if r["status"] == "scored"}
    assert y_scored_k == {3}, (
        "M149 -> at this assertion: correct = {3} (Y has a scored download"
        " only at k=3; it is considered despite being switched off, by its"
        " download history), mutant (only feeds switched on when the"
        " attempt runs are considered) = set() -- Y would not be"
        f" considered at all, got {y_scored_k!r}"
    )
    y_absent_k = {int(r["day_ahead"]) for r in y_rows if r["status"] == "absent"}
    assert y_absent_k == {0, 1, 2, 4, 5, 6, 7}
    assert summary.scored.get(f"feed:{feeds['Y']}") == 1
    assert summary.absent.get(f"feed:{feeds['Y']}") == 7

    # --- V: no site_feed_state row, never downloaded -- not considered. ---
    v_rows = _day_score_rows(conn, site_id, feeds["V"])
    assert v_rows == [], (
        "M147 -> at this assertion: correct = [] (V is switched off --"
        " default_subscribed 0, no site_feed_state row -- and never"
        " downloaded, so it is not a considered feed at all), mutant"
        " (every catalog real feed but the package row is considered,"
        " switched off and never downloaded included) = day_scores rows"
        " for V"
    )
    assert _forecast_history_rows(conn, site_id, feeds["V"]) == []
    v_dl_rows = conn.execute(
        "SELECT 1 FROM close_record_downloads"
        " WHERE site_id = ? AND local_date = ? AND feed_id = ?",
        (site_id, D.isoformat(), feeds["V"]),
    ).fetchall()
    assert v_dl_rows == []
    assert f"feed:{feeds['V']}" not in summary.scored
    assert f"feed:{feeds['V']}" not in summary.absent
