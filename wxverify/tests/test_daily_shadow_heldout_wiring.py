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

Also covers the ``--heldout`` half of §15.3's "What stage one lacks is
named" (plan :1355) -- the ``--freeze-prepare`` half lives in
``test_daily_shadow_not_built.py``.

Also covers the header's per-candidate discarded_resamples (M11) and the
tile and twin level at the call site (M10, call-site form).
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

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

# Plan :1353's own case: a September target day, then a December target day
# in the same autumn-to-winter run. 2035-09-15 to 2035-12-15 is 91 days
# apart (15 remaining days of September + 31 of October + 30 of November +
# 15 of December = 91), far more than the 35-day seed margin each side
# needs: September's seed window (its 34 days of lookback) spans 2035-08-12
# to 2035-09-15; December's spans 2035-11-11 to 2035-12-15. The gap between
# those two spans is 2035-12-15 minus 34 days (2035-11-11) minus 2035-09-15
# = 57 days, so the two seed windows -- and therefore the real 30-day
# training windows nested inside them -- cannot overlap.
_DAY_SEPTEMBER = date(2035, 9, 15)
_DAY_DECEMBER = date(2035, 12, 15)


def _make_two_window_fixture(
    *, day_a: date, day_b: date, seed_a: float, seed_b: float
) -> tuple[sqlite3.Connection, daily_shadow.Site]:
    """Two lead-0 cases, each with its own disjoint 35-day seed window of
    ``temperature_high`` truth, built for §15.4's M22 ("warm-spell
    threshold frozen on the first window").

    Day A's window (``day_a`` and the 34 days before it) is seeded at a
    uniform ``seed_a``; day B's window (``day_b`` and the 34 days before
    it, which must never overlap day A's) is seeded at a uniform
    ``seed_b``. Both days' own target truth is 20.0C. A correct per-case
    ``warm_spell_threshold`` computation gives day A's threshold near
    ``seed_a`` and day B's threshold near ``seed_b`` -- if the threshold
    were frozen on the first window computed, day B would wrongly
    inherit day A's threshold instead of its own.

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

    for target_day, seed_value in ((day_a, seed_a), (day_b, seed_b)):
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


def test_m11_header_reports_discarded_resamples_per_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M11's header line: the ``discarded_resamples`` entry built inside
    ``_temperature_heldout`` (daily_shadow.py:6270-6290) from each
    candidate's own ``o.tile`` -- not a full-run read of the final
    ``outcome``, which only needs a fixture where the discarded count is
    above zero. The fixture has one scored lead-0 case on one day, so ten
    of the twelve (quantity, lead) groups (leads 1-5, both quantities)
    have no scored case. No resample is complete, so all 10,000 are
    discarded. The test reads the header written by the real heldout run
    and compares it with each candidate's recorded tile.

    This also complements, not replaces,
    ``test_m11_draw_resamples_discard_over_1_percent_through_daily_bounds``
    in ``test_daily_shadow_batch1_mutants.py``: that test pins one lead
    missing on 1 of 30 H dates (a partial-discard population) directly
    against ``draw_resamples``/``daily_bounds``; this one drives the real
    pipeline end to end and checks the header line that the other test's
    direct call never reaches.

    M10, call-site form (daily_shadow.py `level=LEVEL_TILE` in `tile()`):
    ``call["tile"].level`` pins the call-site ``level=LEVEL_TILE``
    argument (daily_shadow.py:6182) as the literal ``0.975``, not the
    module constant -- a mutant that changed the call site's level
    argument, rather than the ``LEVEL_TILE`` constant itself, would flip
    this literal. The twin-bearing candidates (IW-BC, TD-K-BC, per
    ``daily_shadow.TWINS``) get the same pin on their own recorded twin
    bounds, which are never ``None`` in this fixture because their twin
    name is always looked up and tiled.
    """
    conn, site = _make_fixture()
    calls = _record_outcome_calls(monkeypatch)
    files = _run_heldout(conn, site)
    reported = json.loads(files["header.json"])["discarded_resamples"]
    by_name = {call["name"]: call for call in calls}

    # Non-vacuity: the recorder actually captured a call per candidate, and
    # the header actually reports one, so neither side can pass empty.
    assert by_name
    assert set(reported) == set(by_name) == set(daily_shadow.TEMPERATURE_CANDIDATES)

    for name, call in by_name.items():
        tile = call["tile"]
        assert tile.level == 0.975
        assert tile.kept == 0
        assert tile.discarded == 10_000  # literal, not RESAMPLES
        assert tile.kept + tile.discarded == 10_000
        assert reported[name] == {
            "kept": tile.kept,
            "discarded": tile.discarded,
            "discard_ok": False,
        }
        twin = call["twin"]
        if name in daily_shadow.TWINS:
            assert twin is not None
            assert twin.level == 0.975
        else:
            assert twin is None


def _warm_spell_by_target(files: dict[str, bytes]) -> dict[date, bool | None]:
    lines = [json.loads(line) for line in files["lines.jsonl"].decode().splitlines()]
    return {
        date.fromisoformat(entry["target"]): entry["warm_spell"]
        for entry in lines
        if entry["quantity"] == "temperature_high" and entry["lead"] == 0
    }


def test_heldout_not_built_names_a11_and_d2_option_c() -> None:
    """§15.3 "What stage one lacks is named" (plan :1355), the
    ``--heldout`` half -- the ``--freeze-prepare`` half is pinned in
    ``test_daily_shadow_not_built.py``.

    Stage one only: temperature's ``--heldout`` header names A11's
    temporary-file size and D2 option C in its top-level ``not_built``,
    and names D2 option C again in ``sensitivities["low=C"]``. Each
    assertion fails if that entry is dropped -- there is no way for this
    test to pass with A11's temporary-file-size entry or D2 option C
    missing from either place.
    """
    conn, site = _make_two_window_fixture(
        day_a=_DAY, day_b=_DAY_B, seed_a=5.0, seed_b=30.0
    )
    note = _note(site)
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
    header_raw = json.loads(files["header.json"])
    assert isinstance(header_raw, dict)
    header = cast("dict[str, object]", header_raw)

    not_built_raw = header["not_built"]
    assert isinstance(not_built_raw, list)
    not_built = cast("list[object]", not_built_raw)
    joined = "|".join(str(entry) for entry in not_built)
    a11_entries = [entry for entry in not_built if "A11" in str(entry)]
    assert a11_entries, not_built
    assert "SQLITE_TMPDIR" in str(a11_entries[0])
    assert "D2 option C" in joined

    sensitivities_raw = header["sensitivities"]
    assert isinstance(sensitivities_raw, dict)
    sensitivities = cast("dict[str, object]", sensitivities_raw)
    low_c_raw = sensitivities["low=C"]
    assert isinstance(low_c_raw, dict)
    low_c = cast("dict[str, object]", low_c_raw)
    assert "D2 option C" in str(low_c["not_built"])


@pytest.mark.parametrize(
    ("day_a", "day_b", "seed_a", "seed_b", "warm_a", "warm_b"),
    [
        pytest.param(_DAY, _DAY_B, 5.0, 30.0, True, False, id="cold-then-warm"),
        pytest.param(
            _DAY_SEPTEMBER,
            _DAY_DECEMBER,
            30.0,
            5.0,
            False,
            True,
            id="plan-1353-autumn-to-winter",
        ),
    ],
)
def test_m22_warm_spell_threshold_is_not_frozen_on_the_first_window(
    day_a: date, day_b: date, seed_a: float, seed_b: float, warm_a: bool, warm_b: bool
) -> None:
    """M22: warm-spell threshold frozen on the first window -> at each
    day's own ``warm_spell`` flag in ``lines.jsonl``: correct = day A
    ``warm_a``, day B ``warm_b`` (each case's own window gives its own
    threshold); mutant = day B wrongly inherits day A's threshold instead
    of computing its own, from "the first window".

    The ``cold-then-warm`` case is the original direction (cold ``_DAY``,
    then warm ``_DAY_B``), unchanged in meaning. The
    ``plan-1353-autumn-to-winter`` case is plan :1353's own case: an
    autumn-to-winter run. Here day A (September) has a warm
    reference window and day B (December) a cold one, and
    both targets are 20.0C. The rolling threshold classifies
    September's target as not warm (False) and December's as
    warm (True). With September's threshold reused in
    December, the December target is wrongly classified as not
    warm. This pins the plan's case; mutant-kill verification
    happens later, at checkpoint C.
    """
    conn, site = _make_two_window_fixture(
        day_a=day_a, day_b=day_b, seed_a=seed_a, seed_b=seed_b
    )
    note = daily_shadow.FreezeNote(
        site_id=site.site_id,
        first_h_date=day_a,
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
        first=day_a,
        last=day_b,
        h_first=day_a,
        last_complete=day_b,
        excluded_h_dates=(),
        t2_last=None,
        note=note,
        house_station=note.house_station,
        incumbent_from_record=False,
        failed_dirs=(),
        header_base={"copy": {}, "manifest": {}, "commit_sha": "a" * 40},
    )
    by_target = _warm_spell_by_target(files)
    assert by_target[day_a] is warm_a
    assert by_target[day_b] is warm_b
