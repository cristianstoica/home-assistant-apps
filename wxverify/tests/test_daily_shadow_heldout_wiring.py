"""§15.3 the run-level wiring: ``twin_missing`` and the compose_iw_bc-only wrapper.

Drives the real pipeline (``run`` -> ``_run_temperature`` ->
``_temperature_heldout`` -> ``candidate_outcome``) end to end over a tiny,
production-written fixture, to prove by construction that ``twin_missing``
is literally the twin's own ``missing`` count, not merely documented as
such, and that a corrected candidate whose own composer gives no value is
caught by its own count rather than by its twin's.
"""

from __future__ import annotations

import dataclasses
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
