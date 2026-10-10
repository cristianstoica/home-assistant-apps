"""§15.3 the run-level wiring: ``twin_missing`` and the compose_iw_bc-only wrapper.

Drives the real pipeline (``run`` -> ``_run_temperature`` ->
``_temperature_heldout`` -> ``candidate_outcome``) end to end over a tiny,
production-written fixture, to prove by construction that ``twin_missing``
is literally the twin's own ``missing`` count, not merely documented as
such, and that a corrected candidate whose own composer gives no value is
caught by its own count rather than by its twin's.

Also covers §15.4's M22 ("warm-spell threshold frozen on the first
window"): a two-case, DB-backed fixture with disjoint training windows,
needed because the bug this mutant posits is only visible across more
than one case sharing a run.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast import daily_method
from wxverify.settings.keys import set_setting
from wxverify.verification import daily_shadow
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc

_DAY = date(2035, 6, 15)


def _t() -> datetime:
    return resolve_snapshot_utc("UTC", _DAY, "07:00")


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


def _make_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """One scored lead-0 case at ``_DAY``, built entirely by production code.

    An empty ``stations``/``station_observations`` gives ``readings == 0``
    (Gate A), so Arm B is always selected and no station fixture is
    needed; Arm B's training truth reads ``daily_truth`` directly. With no
    training pairs seeded at all, no feed reaches ``TEMP_MIN_DAYS``, so
    every composer's regime is ``"no_eligible"`` and falls back to the
    control value -- a single scored case is enough, with no multi-week
    training history.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "heldout-wiring-site")
    ensure_published_generation(conn, site_id)
    set_setting(conn, "forecast_blend_depth", "1")
    feed_a = asof_make_real_feed(conn, "model-a")
    day_before = _DAY - timedelta(days=1)
    issued = f"{day_before.isoformat()}T06:00:00Z"
    fetched = f"{day_before.isoformat()}T06:05:00Z"
    _insert_day_samples(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        local_date=_DAY,
        issued_at=issued,
        fetched_at=fetched,
        value=10.0,
    )
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=_t())
    site = daily_shadow.load_site(conn, site_id=site_id)
    for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
        _insert_daily_truth(
            conn,
            site_id=site_id,
            tz_generation_id=site.generation_id,
            local_date=_DAY,
            quantity=quantity,
            value=10.0,
        )
    conn.commit()
    return conn, site


_DAY_B = _DAY + timedelta(days=180)


def _make_two_window_fixture() -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """Two lead-0 cases 180 days apart, each with its own disjoint 35-day
    seed window of ``temperature_high`` truth, built for §15.4's M22
    ("warm-spell threshold frozen on the first window").

    Day A's window (``_DAY`` and the 34 days before it) is seeded at a
    uniform 5.0C; day B's window (``_DAY_B`` and the 34 days before it,
    which never overlaps day A's) is seeded at a uniform 30.0C. Both
    days' own target truth is 20.0C. A correct per-case
    ``warm_spell_threshold`` computation gives day A's threshold near
    5.0C (so 20.0 is a warm spell) and day B's threshold near 30.0C (so
    20.0 is not) -- if the threshold were frozen on the first window
    computed, day B would wrongly inherit day A's ~5.0C threshold and
    also come out warm.

    The 35-day seed (one more than ``TEMP_WINDOW_DAYS`` = 30) is a
    deliberate margin over the training window's own day count, to
    absorb any lag ``latest_knowable_target`` may apply when mapping a
    snapshot to its training K day -- the test checks the *computed*
    threshold value, not the window's exact boundary dates.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "heldout-wiring-two-window-site")
    ensure_published_generation(conn, site_id)
    set_setting(conn, "forecast_blend_depth", "1")
    feed_a = asof_make_real_feed(conn, "model-a")
    site = daily_shadow.load_site(conn, site_id=site_id)

    for target_day, seed_value in ((_DAY, 5.0), (_DAY_B, 30.0)):
        day_before = target_day - timedelta(days=1)
        issued = f"{day_before.isoformat()}T06:00:00Z"
        fetched = f"{day_before.isoformat()}T06:05:00Z"
        _insert_day_samples(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            local_date=target_day,
            issued_at=issued,
            fetched_at=fetched,
            value=10.0,
        )
        now = resolve_snapshot_utc("UTC", target_day, "07:00")
        build_forecast_record(conn, site_id, target_day.isoformat(), now=now)
        for offset in range(35):
            seed_day = target_day - timedelta(days=offset)
            _insert_daily_truth(
                conn,
                site_id=site_id,
                tz_generation_id=site.generation_id,
                local_date=seed_day,
                quantity="temperature_high",
                value=seed_value if seed_day != target_day else 20.0,
            )
        _insert_daily_truth(
            conn,
            site_id=site_id,
            tz_generation_id=site.generation_id,
            local_date=target_day,
            quantity="temperature_low",
            value=10.0,
        )
    conn.commit()
    return conn, site


def _note(site: daily_shadow.Site) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=site.site_id,
        first_h_date=_DAY,
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _run_heldout(conn: sqlite3.Connection, site: daily_shadow.Site) -> dict[str, bytes]:
    note = _note(site)
    return daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=_DAY,
        last=_DAY,
        h_first=_DAY,
        last_complete=_DAY,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )


def _record_outcome_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    real = daily_shadow.candidate_outcome

    def recorder(name: str, **kwargs: Any) -> daily_shadow.CandidateOutcome:
        calls.append({"name": name, **kwargs})
        return real(name, **kwargs)

    monkeypatch.setattr(daily_shadow, "candidate_outcome", recorder)
    return calls


def _force_none_at_lead_zero(monkeypatch: pytest.MonkeyPatch, attr: str) -> None:
    real = getattr(daily_method, attr)

    def forced(feeds: Any, **kwargs: Any) -> Any:
        result = real(feeds, **kwargs)
        if kwargs["lead"] == 0:
            return dataclasses.replace(result, value=None)
        return result

    monkeypatch.setattr(daily_method, attr, forced)


def test_twin_missing_equals_the_twins_own_missing_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forcing IW's (and TD-K's) composer to None at lead 0 raises IW's own
    ``missing`` (against control) and IW-BC's ``twin_missing`` (against IW)
    by the identical count, on the same scored lines -- proof that
    ``twin_missing`` is literally the twin's own missing count.
    """
    conn, site = _make_fixture()
    calls = _record_outcome_calls(monkeypatch)
    _force_none_at_lead_zero(monkeypatch, "compose_iw")
    _force_none_at_lead_zero(monkeypatch, "compose_td_k")

    _run_heldout(conn, site)

    by_name = {call["name"]: call for call in calls}
    assert set(by_name) == {"IW", "TD-K", "IW-BC", "TD-K-BC"}

    assert by_name["IW"]["missing"] > 0
    assert by_name["IW"]["twin"] is None
    assert by_name["IW"]["twin_missing"] == 0

    assert by_name["IW-BC"]["missing"] == 0
    assert by_name["IW-BC"]["twin_missing"] == by_name["IW"]["missing"]

    assert by_name["TD-K"]["missing"] > 0
    assert by_name["TD-K"]["twin_missing"] == 0

    assert by_name["TD-K-BC"]["missing"] == 0
    assert by_name["TD-K-BC"]["twin_missing"] == by_name["TD-K"]["missing"]


def test_a_corrected_candidates_own_composer_is_caught_by_its_own_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forcing IW-BC's OWN composer to None at lead 0 raises IW-BC's own
    ``missing`` (against control) -- unlike the previous test, where only
    IW-BC's ``twin_missing`` moved while its own ``missing`` stayed 0, here
    BOTH move (``twin_missing`` is ``case_deltas(lines, "IW-BC", "IW")``,
    which also reads IW-BC's own value, so it is not twin-only sensitive).
    IW itself, whose composer was never touched, stays unaffected either
    way: IW's own ``missing`` is 0 in both tests. (Whether the pipeline's
    own precedence rule then reports the own-count reason over the twin
    one, when both are nonzero, is pinned directly against
    ``candidate_outcome`` by Twin rule (iv) in
    ``test_daily_shadow_twin_rule.py`` -- this test's job is only to prove
    the counts the wiring hands that function, not to re-derive its
    verdict.)
    """
    conn, site = _make_fixture()
    calls = _record_outcome_calls(monkeypatch)
    _force_none_at_lead_zero(monkeypatch, "compose_iw_bc")

    _run_heldout(conn, site)

    by_name = {call["name"]: call for call in calls}
    assert by_name["IW"]["missing"] == 0
    assert by_name["IW-BC"]["missing"] > 0
    assert by_name["IW-BC"]["twin_missing"] > 0


def _warm_spell_by_target(files: dict[str, bytes]) -> dict[date, bool | None]:
    lines = [json.loads(line) for line in files["lines.jsonl"].decode().splitlines()]
    return {
        date.fromisoformat(entry["target"]): entry["warm_spell"]
        for entry in lines
        if entry["quantity"] == "temperature_high" and entry["lead"] == 0
    }


def test_m22_warm_spell_threshold_is_not_frozen_on_the_first_window() -> None:
    """M22: warm-spell threshold frozen on the first window -> at each
    day's own ``warm_spell`` flag in ``lines.jsonl``: correct = day A
    True, day B False (each case's own ~5C/~30C window gives its own
    threshold), mutant = day A True, day B ALSO True (day B wrongly
    inherits day A's ~5C threshold from "the first window").
    """
    conn, site = _make_two_window_fixture()
    note = daily_shadow.FreezeNote(
        site_id=site.site_id,
        first_h_date=_DAY,
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest={},
        constants={},
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )
    files = daily_shadow.run(
        conn,
        site=site,
        variable="temperature",
        mode="heldout",
        first=_DAY,
        last=_DAY_B,
        h_first=_DAY,
        last_complete=_DAY_B,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )
    by_target = _warm_spell_by_target(files)
    assert by_target[_DAY] is True
    assert by_target[_DAY_B] is False
