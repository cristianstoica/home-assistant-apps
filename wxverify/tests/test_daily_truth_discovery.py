"""daily_truth day-discovery oracles O1-O18 (0.13.1 plan, §8).

Covers the new ``discover`` chain phase (``wxverify.worker.verification_run
.advance_verification``) and the creative materialization it drives
(``wxverify.verification.truth.materialize_missing_truth_days`` /
``missing_truth_days`` / ``observation_day_extent`` / ``settled_ceiling_local
_date``). Site names, timezones and coordinates are synthetic throughout
(``site-alpha``/``site-beta``, ``America/Denver``/``Etc/GMT+7``/``UTC``,
``40.0``/``-105.0``) -- never a real place.

Two harness tiers, chosen per oracle:

* Most oracles call ``materialize_missing_truth_days``/``missing_truth_days``
  directly against a bare in-memory connection, or drive
  ``advance_verification`` in a loop against that same bare connection. Both
  are safe here because nothing in those oracles depends on whole-chunk
  rollback-on-exception semantics.
* O14/O15/O18 assert exactly that rollback behavior (a mid-chunk exception
  must undo every write the chunk made), which a bare ``sqlite3`` connection
  under legacy isolation cannot reproduce faithfully -- an uncommitted write
  from the same connection stays visible to that same connection regardless
  of a later rollback. Those three drive through a real file-backed
  ``Database`` and its ``write_sync`` (``BEGIN IMMEDIATE`` / commit /
  rollback-on-exception), per ``wxverify/db/connection.py``'s
  ``_run_immediate``.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import wxverify.verification.truth as truth
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_pointer_key,
)
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.runs import settled_through
from wxverify.verification.truth import (
    materialize_daily_truth,
    materialize_missing_truth_days,
    settled_ceiling_local_date,
)
from wxverify.worker.processor import _fail_job  # noqa: SLF001
from wxverify.worker.verification_run import (
    _load_state,  # noqa: SLF001
    _save_state,  # noqa: SLF001
    advance_verification,
    verification_job_key,
    verification_state_key,
)

TZ_DENVER = "America/Denver"
TZ_GMT7 = "Etc/GMT+7"
CONSENSUS_LAG_HOURS = 3  # wxverify.verification.methodology.CONSENSUS_LAG_HOURS


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    """Fresh fully-migrated in-memory database (real datastore, no mocks)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(
    conn: sqlite3.Connection,
    *,
    timezone: str = TZ_DENVER,
    name: str = "site-alpha",
    rain_threshold_mm: float = 0.2,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                            timezone, rain_threshold_mm)
        VALUES (?, 40.0, -105.0, 900.0, ?, ?)
        """,
        (name, timezone, rain_threshold_mm),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_obs_day(
    conn: sqlite3.Connection,
    site_id: int,
    day: str,
    *,
    hour: int = 12,
    variable: str = "temperature",
    value: float = 10.0,
) -> None:
    """One consensus observation inside local day ``day``'s UTC window.

    Hour 12 UTC sits inside every zone this suite uses (``America/Denver``,
    ``Etc/GMT+7``, ``UTC``) regardless of which local day is targeted.
    """
    valid_at = f"{day}T{hour:02d}:00:00+00:00"
    conn.execute(
        """
        INSERT INTO observations (site_id, variable, valid_at, value,
                                   n_stations, computed_at)
        VALUES (?, ?, ?, ?, 3, ?)
        """,
        (site_id, variable, valid_at, value, valid_at),
    )


def _seed_obs_days(conn: sqlite3.Connection, site_id: int, days: list[str]) -> None:
    for day in days:
        _seed_obs_day(conn, site_id, day)


def _truth_local_dates(
    conn: sqlite3.Connection, site_id: int, tz_generation_id: int
) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT local_date FROM daily_truth "
        "WHERE site_id = ? AND tz_generation_id = ?",
        (site_id, tz_generation_id),
    ).fetchall()
    return {str(row["local_date"]) for row in rows}


def _decisions(conn: sqlite3.Connection, site_id: int) -> list[str]:
    rows = conn.execute(
        "SELECT decision FROM verification_trigger_decisions "
        "WHERE site_id = ? ORDER BY id",
        (site_id,),
    ).fetchall()
    return [str(row["decision"]) for row in rows]


def _drive_to_regen(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    max_steps: int = 50,
) -> None:
    """Call ``advance_verification`` until the ``discover`` phase ends."""
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        phase = "discover" if blob is None else str(blob.get("phase"))
        if phase != "discover":
            return
        advance_verification(conn, site_id, payload)
    raise AssertionError("discover phase did not terminate within max_steps")


def _drive_until_decided(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    max_steps: int = 100,
) -> None:
    """Drive the chain until a trigger decision has been recorded.

    Stops the moment ``decide`` records ``run_started`` and hands off to
    ``start`` (blob phase == "start"), or the moment the chain halts on a
    blocking decision (``advance_verification`` returns False).
    """
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        phase = "discover" if blob is None else str(blob.get("phase"))
        if phase == "start":
            return
        cont = advance_verification(conn, site_id, payload)
        if not cont:
            return
    raise AssertionError("chain did not reach a decision within max_steps")


def _file_db(tmp_path: Path, name: str = "discovery.db") -> Database:
    """Real file-backed database for the oracles that need genuine
    BEGIN IMMEDIATE / commit / rollback-on-exception semantics.
    """
    return Database(str(tmp_path / name))


# ---------------------------------------------------------------------------
# O4 / O12 -- pure `settled_ceiling_local_date` (plan §8, step-1 gate)
# ---------------------------------------------------------------------------


def test_o4_ceiling_boundary_second_standard_time() -> None:
    """The settled ceiling steps exactly on the lag boundary, in a
    non-DST zone, and agrees with the real ``settled_through`` SQL.

    Kills: an off-by-one in the boundary comparison, and ignoring
    CONSENSUS_LAG_HOURS (either would put the flip on the wrong second or
    the wrong day).
    """
    just_before = datetime(2026, 6, 11, 2, 59, 59, tzinfo=UTC)
    at_boundary = datetime(2026, 6, 11, 3, 0, 0, tzinfo=UTC)
    ceiling_before = settled_ceiling_local_date("UTC", just_before)
    ceiling_at = settled_ceiling_local_date("UTC", at_boundary)
    assert ceiling_before == date(2026, 6, 9)
    assert ceiling_at == date(2026, 6, 10)

    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    generation_id = ensure_published_generation(conn, site_id)
    _seed_obs_day(conn, site_id, "2026-06-10")
    materialize_daily_truth(
        conn, site_id=site_id, local_date="2026-06-10", tz_generation_id=generation_id
    )
    assert (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=just_before
        )
        is None
    )
    assert (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=at_boundary
        )
        == "2026-06-10"
    )


@pytest.mark.parametrize(
    ("label", "local_date_str", "timezone"),
    [
        ("spring_forward", "2026-03-08", TZ_DENVER),  # 23-hour local day
        ("fall_back", "2026-11-01", TZ_DENVER),  # 25-hour local day
        # The two EVES: their boundary instant (end_utc + LAG) falls INSIDE
        # the 60-minute band right after the transition, where a
        # convert-then-subtract (correct) and a subtract-then-convert
        # (wall-clock, mutant) implementation actually diverge -- the
        # transition-day rows above land a full day past that band and
        # cannot distinguish the two forms (see F3 in the 0.13.1 close-out).
        ("spring_forward_eve", "2026-03-07", TZ_DENVER),
        ("fall_back_eve", "2026-10-31", TZ_DENVER),
    ],
)
def test_o4_ceiling_boundary_second_dst(
    label: str, local_date_str: str, timezone: str
) -> None:
    """The lag-boundary flip holds across a DST transition day, derived
    from absolute UTC instants (never wall-clock arithmetic), and agrees
    with ``settled_through`` SQL bound to a real inserted row.

    Kills: wall-clock ``timedelta`` arithmetic on an aware non-UTC datetime
    in place of UTC-normalized arithmetic (would misplace the boundary by
    the DST offset delta on exactly these two days). The eve rows are the
    ones that actually kill it: their boundary instant sits inside the
    60-minute band immediately after the transition, where convert-then-
    subtract and subtract-then-convert disagree; the transition-day rows
    land a full day past that band and agree with the mutant too.
    """
    day = date.fromisoformat(local_date_str)
    bounds = local_day_bounds(day, timezone)
    boundary = bounds.end_utc + timedelta(hours=CONSENSUS_LAG_HOURS)
    just_before = boundary - timedelta(seconds=1)

    assert settled_ceiling_local_date(timezone, just_before) < day
    assert settled_ceiling_local_date(timezone, boundary) == day

    conn = _conn()
    site_id = _make_site(conn, timezone=timezone)
    generation_id = ensure_published_generation(conn, site_id)
    _seed_obs_day(conn, site_id, local_date_str)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=local_date_str, tz_generation_id=generation_id
    )
    assert (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=just_before
        )
        is None
    )
    assert (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=boundary
        )
        == local_date_str
    )


def test_o12_naive_now_agrees_with_aware_on_nonutc_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A naive ``now`` and its explicit ``tzinfo=UTC`` twin must agree,
    even when the OS timezone is a non-UTC, non-DST zone.

    Kills: reading a naive ``now`` through ``.astimezone()`` (which treats
    a naive datetime as SYSTEM-LOCAL time) instead of ``.replace(tzinfo=
    UTC)``. Under ``TZ=Etc/GMT+7`` (fixed UTC-7, no DST) that bug shifts
    the instant by exactly 7 hours before the lag is even applied, which
    for this ``now`` value crosses a local-date boundary in
    ``America/Denver`` and flips the returned ceiling by a day.
    """
    monkeypatch.setenv("TZ", "Etc/GMT+7")
    time.tzset()
    try:
        naive = datetime(2026, 6, 15, 3, 0, 0)
        aware = naive.replace(tzinfo=UTC)
        naive_ceiling = settled_ceiling_local_date(TZ_DENVER, naive)
        aware_ceiling = settled_ceiling_local_date(TZ_DENVER, aware)
        assert naive_ceiling == aware_ceiling == date(2026, 6, 13)
    finally:
        monkeypatch.delenv("TZ", raising=False)
        time.tzset()


# ---------------------------------------------------------------------------
# O2, O3, O5, O6, O7, O10 -- direct calls to the truth.py entry points
# (plan §8, step-2 gate)
# ---------------------------------------------------------------------------


def test_o2_forward_gap_past_existing_rows() -> None:
    """Truth exists for D..D+2 only; observations run through D+6, and the
    ceiling clears D+6. Discovery must fill the WHOLE forward gap, not just
    resume from ``MAX(local_date)``.

    Kills: "seed only if daily_truth is empty" (a mutant that short-circuits
    once any row exists). Does NOT kill an implementation anchored on
    ``MIN(daily_truth.local_date)`` instead of the observation extent's lower
    bound -- every day this oracle discovers is already forward of every
    existing truth row, so such an anchor and the observation extent agree
    here. See :func:`test_o2b_older_observations_are_discoverable_after_the_
    fact` for that direction.
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    days = [f"2026-06-{n:02d}" for n in range(1, 8)]  # D..D+6, 7 days
    _seed_obs_days(conn, site_id, days)
    for day in days[:3]:  # D..D+2 already materialized
        materialize_daily_truth(
            conn, site_id=site_id, local_date=day, tz_generation_id=generation_id
        )
    now = datetime(2026, 6, 20, tzinfo=UTC)  # well past D+6's settlement
    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert attempted == days[3:]
    assert _truth_local_dates(conn, site_id, generation_id) == set(days)
    assert (
        settled_through(conn, site_id=site_id, tz_generation_id=generation_id, now=now)
        == days[-1]
    )


def test_o2b_older_observations_are_discoverable_after_the_fact() -> None:
    """A late import of observations OLDER than every existing truth row must
    still be discovered on the next chunk -- the lower bound anchors on the
    observation extent, never on ``MIN(daily_truth.local_date)``.

    Plan D5, "Why the lower bound is the first observed day and not
    ``MIN(daily_truth.local_date)``" (``wxverify/verification/truth.py:382
    -384``). This is the backward-in-time companion to O2 (which only proves
    the forward direction, since every day it discovers sorts after every
    existing truth row).

    Kills: ``lower = MIN(daily_truth.local_date)`` once the site has any
    truth under the generation (falling back to the observation extent's
    first day only when it has none) -- a mutant that byte-copies the
    shipped body everywhere else. Under that mutant, once truth exists for
    06-10, the lower bound is frozen at 06-10 forever; a later import of
    strictly older observations for 06-05..06-07 is permanently
    unreachable, because ``regenerate_marked_truth_chunk`` only rebuilds
    days that already HAVE rows and nothing else creates truth rows outside
    a retrospective timezone correction.
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    _seed_obs_days(conn, site_id, ["2026-06-10", "2026-06-11", "2026-06-12"])
    now = datetime(2026, 6, 20, tzinfo=UTC)

    first = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=50)
    assert first == ["2026-06-10", "2026-06-11", "2026-06-12"]
    before = {
        str(row["local_date"]): str(row["generated_at"])
        for row in conn.execute(
            "SELECT local_date, generated_at FROM daily_truth WHERE site_id=?",
            (site_id,),
        ).fetchall()
    }

    # Late-import OLDER observations, strictly before every existing truth row.
    _seed_obs_days(conn, site_id, ["2026-06-05", "2026-06-06", "2026-06-07"])

    second = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=50)
    assert second == [
        "2026-06-05",
        "2026-06-06",
        "2026-06-07",
        "2026-06-08",
        "2026-06-09",
    ]
    assert _truth_local_dates(conn, site_id, generation_id) == {
        "2026-06-05",
        "2026-06-06",
        "2026-06-07",
        "2026-06-08",
        "2026-06-09",
        "2026-06-10",
        "2026-06-11",
        "2026-06-12",
    }
    after = {
        str(row["local_date"]): str(row["generated_at"])
        for row in conn.execute(
            "SELECT local_date, generated_at FROM daily_truth WHERE site_id=?",
            (site_id,),
        ).fetchall()
    }
    for day in ("2026-06-10", "2026-06-11", "2026-06-12"):
        assert after[day] == before[day]


def test_o3_interior_hole_is_filled_without_rewriting_neighbors() -> None:
    """An interior missing day (D+1, with D and D+2 already materialized)
    is filled without touching the neighbors' rows.

    Kills: a forward-only-from-MAX implementation (would never look back at
    an interior hole once D+2 exists), and a rebuild-whole-range
    implementation (would rewrite D and D+2's ``generated_at``, discarding
    provenance that never needed to change).
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    days = ["2026-06-01", "2026-06-02", "2026-06-03"]
    _seed_obs_days(conn, site_id, days)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=days[0], tz_generation_id=generation_id
    )
    materialize_daily_truth(
        conn, site_id=site_id, local_date=days[2], tz_generation_id=generation_id
    )
    before = {
        str(row["local_date"]): str(row["generated_at"])
        for row in conn.execute(
            "SELECT local_date, generated_at FROM daily_truth WHERE site_id=?",
            (site_id,),
        ).fetchall()
    }
    now = datetime(2026, 6, 20, tzinfo=UTC)
    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert attempted == [days[1]]
    assert _truth_local_dates(conn, site_id, generation_id) == set(days)
    after = {
        str(row["local_date"]): str(row["generated_at"])
        for row in conn.execute(
            "SELECT local_date, generated_at FROM daily_truth WHERE site_id=?",
            (site_id,),
        ).fetchall()
    }
    assert after[days[0]] == before[days[0]]
    assert after[days[2]] == before[days[2]]
    assert days[1] in after


def test_o5_ineligible_day_is_not_a_perpetual_hole() -> None:
    """A day with insufficient coverage still materializes (five rows, all
    ``eligible=0``), and is never re-selected as missing again -- yet still
    counts toward ``settled_through``.

    Kills: a presence query filtered on ``eligible = 1`` (or on
    ``stale = 0``) -- either would keep re-selecting this day as missing
    forever.
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    day = "2026-06-01"
    _seed_obs_day(conn, site_id, day, hour=12)  # a single hour: never eligible
    now = datetime(2026, 6, 10, tzinfo=UTC)
    first = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert first == [day]
    rows = conn.execute(
        "SELECT eligible, exclusion_reason FROM daily_truth "
        "WHERE site_id=? AND local_date=?",
        (site_id, day),
    ).fetchall()
    assert len(rows) == 5
    assert all(int(row["eligible"]) == 0 for row in rows)
    assert all(row["exclusion_reason"] is not None for row in rows)

    second = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert second == []
    assert (
        settled_through(conn, site_id=site_id, tz_generation_id=generation_id, now=now)
        == day
    )

    # The presence query must also ignore `stale`: a day marked stale by an
    # unrelated consensus mutation is still PRESENT (`regenerate_marked_
    # truth_chunk`'s job to rebuild, never discovery's).
    conn.execute(
        "UPDATE daily_truth SET stale = 1 WHERE site_id=? AND local_date=?",
        (site_id, day),
    )
    third = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert third == []


def test_o6_idempotent_on_a_full_pass() -> None:
    """Re-running discovery over an already-complete window changes
    nothing: zero attempts, zero rewritten rows.

    Kills: a delete-and-recreate-every-chunk implementation, or any
    "re-materialize whenever seen" implementation that rewrites rows the
    presence check should have skipped.
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    days = ["2026-06-01", "2026-06-02", "2026-06-03"]
    _seed_obs_days(conn, site_id, days)
    now = datetime(2026, 6, 20, tzinfo=UTC)
    first = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert first == days
    select_sql = (
        "SELECT local_date, quantity, generated_at FROM daily_truth WHERE site_id=?"
    )
    before = {
        (str(row["local_date"]), str(row["quantity"])): str(row["generated_at"])
        for row in conn.execute(select_sql, (site_id,)).fetchall()
    }
    second = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert second == []
    after = {
        (str(row["local_date"]), str(row["quantity"])): str(row["generated_at"])
        for row in conn.execute(select_sql, (site_id,)).fetchall()
    }
    assert after == before
    del generation_id  # only used to keep parity with the other oracles


def test_o7_materializes_under_published_generation_with_its_own_zone() -> None:
    """Discovery materializes ONLY under the site's published generation,
    tagging rows with THAT generation's timezone -- never
    ``sites.timezone`` and never a co-existing building generation.

    Kills: a presence query missing the ``tz_generation_id`` predicate (a
    row already existing under the building generation would then read as
    "already present" for the published generation too, and the published
    generation would end up with zero rows for that day); and ceiling/
    bounds computed from ``sites.timezone`` instead of the resolved
    generation's zone (``UTC`` vs ``Etc/GMT+7`` disagree by 7 hours on the
    UTC day bounds). The second half needs an observation whose UTC hour
    lands on DIFFERENT local dates under the two zones -- an observation at
    hour 12 UTC (this suite's usual anchor) sits inside every zone's local
    day regardless, which is why the discovery WINDOW itself must be probed
    with a second, boundary-straddling observation below.
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    published_cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state, published_at)
        VALUES (?, ?, 'initial', 'published', ?)
        """,
        (site_id, TZ_GMT7, isoformat_utc()),
    )
    g1 = int(published_cur.lastrowid or 0)
    set_runtime_state(conn, published_pointer_key(site_id), str(g1))
    building_cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, ?, 'retrospective_correction', 'building')
        """,
        (site_id, TZ_DENVER),
    )
    g2 = int(building_cur.lastrowid or 0)

    target = "2026-06-10"
    _seed_obs_day(conn, site_id, target)
    # A second observation whose UTC hour straddles the two zones' local
    # dates: 2026-06-12T03:00Z is still 2026-06-11 local under the
    # generation's Etc/GMT+7 (UTC-7) but is already 2026-06-12 local under
    # `sites.timezone` (UTC). Only the resolved generation's zone is
    # correct, so the discovery WINDOW -- not just the row tagging -- must
    # be computed from it: under `sites.timezone` the window would
    # (wrongly) extend a day further.
    _seed_obs_day(conn, site_id, "2026-06-12", hour=3)
    # A decoy row already exists for the SAME (site, local_date) under the
    # building generation, before discovery ever runs against g1.
    materialize_daily_truth(
        conn, site_id=site_id, local_date=target, tz_generation_id=g2
    )

    now = datetime(2026, 6, 20, tzinfo=UTC)
    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert attempted == [target, "2026-06-11"]

    g1_rows = conn.execute(
        "SELECT timezone, day_start_utc, day_end_utc FROM daily_truth "
        "WHERE site_id=? AND tz_generation_id=? AND local_date=?",
        (site_id, g1, target),
    ).fetchall()
    assert len(g1_rows) == 5
    assert {str(row["timezone"]) for row in g1_rows} == {TZ_GMT7}
    expected_bounds = local_day_bounds(date.fromisoformat(target), TZ_GMT7)
    assert {str(row["day_start_utc"]) for row in g1_rows} == {
        isoformat_utc(expected_bounds.start_utc)
    }
    assert {str(row["day_end_utc"]) for row in g1_rows} == {
        isoformat_utc(expected_bounds.end_utc)
    }
    # The building generation is untouched: still exactly the one decoy set.
    g2_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=? AND tz_generation_id=?",
        (site_id, g2),
    ).fetchone()
    assert int(g2_rows["n"]) == 5


def test_o10_outage_clamps_to_last_observed_day() -> None:
    """Observations stop 5+ days before the settled ceiling: discovery must
    clamp at the last OBSERVED day, never march forward to the ceiling on
    zero-coverage days.

    Kills: clamping the window's upper bound by the ceiling alone (without
    also clamping by the last observed day) -- ``materialize_daily_truth``
    happily materializes a day with zero observations (all quantities
    ineligible), so an unclamped implementation would create rows for every
    day between the outage and the ceiling instead of stopping.
    """
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    days = ["2026-06-01", "2026-06-02", "2026-06-03"]
    _seed_obs_days(conn, site_id, days)
    now = datetime(2026, 6, 20, tzinfo=UTC)  # ceiling is far past day[-1] + 5
    ceiling = settled_ceiling_local_date(TZ_DENVER, now)
    assert ceiling > date.fromisoformat(days[-1]) + timedelta(days=5)

    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=20)
    assert attempted == days
    assert _truth_local_dates(conn, site_id, generation_id) == set(days)
    beyond = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=? AND local_date > ?",
        (site_id, days[-1]),
    ).fetchone()
    assert int(beyond["n"]) == 0


# ---------------------------------------------------------------------------
# O1, O8, O9, O11, O13 -- chain-integration oracles (bare connection is
# safe: none of these depend on rollback-on-exception semantics)
# ---------------------------------------------------------------------------


def test_o1_bootstraps_from_empty_daily_truth() -> None:
    """A fresh chain with observations but no ``daily_truth`` rows at all
    discovers, decides, and records ``run_started`` -- not the "nothing to
    do" skip a do-nothing ``discover`` phase would produce.

    Kills: the pre-0.13.1 baseline where the nightly trigger only ever
    regenerated STALE rows and never created new ones -- an empty
    ``daily_truth`` table would leave ``settled_through`` returning None
    forever, and the decide phase would record ``skipped`` instead of
    ``run_started``.
    """
    conn = _conn()
    site_id = _make_site(conn)
    days = [f"2026-06-{n:02d}" for n in range(1, 6)]
    _seed_obs_days(conn, site_id, days)
    payload: dict[str, object] = {"trigger_date": "2026-06-10"}
    _drive_until_decided(conn, site_id, payload)

    generation_id = ensure_published_generation(conn, site_id)
    rows = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=?", (site_id,)
    ).fetchone()
    assert int(rows["n"]) > 0
    from wxverify.core.timeutil import utc_now

    assert (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=utc_now()
        )
        is not None
    )
    assert _decisions(conn, site_id) == ["run_started"]


def test_o8_chunking_resumability_and_blob_hygiene() -> None:
    """Five missing days, a chunk size of 2: three chunks (2, 2, 1), a
    strictly-increasing cursor, and an EXACT final blob with the cursor
    popped.

    Kills: an unbounded chunk (ignoring the payload override -- would
    finish in one call instead of three); a non-advancing cursor (would
    repeat the same two days every chunk); and a leaked ``truth_cursor``
    key surviving the transition to ``regen``.
    """
    conn = _conn()
    site_id = _make_site(conn)
    days = [f"2026-06-{n:02d}" for n in range(1, 6)]
    _seed_obs_days(conn, site_id, days)
    payload: dict[str, object] = {"truth_discovery_days": 2}

    advance_verification(conn, site_id, payload)
    blob1 = _load_state(conn, site_id)
    assert blob1 == {"phase": "discover", "truth_cursor": days[1]}

    advance_verification(conn, site_id, payload)
    blob2 = _load_state(conn, site_id)
    assert blob2 == {"phase": "discover", "truth_cursor": days[3]}
    assert str(blob1["truth_cursor"]) < str(blob2["truth_cursor"])

    advance_verification(conn, site_id, payload)
    blob3 = _load_state(conn, site_id)
    assert blob3 == {"phase": "regen"}

    generation_id = ensure_published_generation(conn, site_id)
    assert _truth_local_dates(conn, site_id, generation_id) == set(days)


def test_o9_structural_termination_independent_of_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``discover`` reaches ``regen`` in a bounded number of chunks even
    when ``materialize_daily_truth`` never actually creates a row -- the
    chunk cursor's forward progress terminates the phase, not the missing
    set shrinking.

    Kills: a "materialized day leaves the missing set" termination
    formulation. If ``discover`` re-derived its next window purely from
    which days STILL lack truth (instead of advancing an independent
    cursor), this no-op would leave the missing set unchanged forever and
    the phase would never reach ``regen`` -- the step cap below turns that
    hang into a deterministic failure instead of an actual hang.
    """
    conn = _conn()
    site_id = _make_site(conn)
    days = [f"2026-06-{n:02d}" for n in range(1, 6)]  # 5 days
    _seed_obs_days(conn, site_id, days)
    payload: dict[str, object] = {"truth_discovery_days": 2}

    def _noop_materialize(
        conn: sqlite3.Connection,
        *,
        site_id: int,
        local_date: str,
        tz_generation_id: int | None = None,
    ) -> dict[str, object]:
        return {}

    monkeypatch.setattr(truth, "materialize_daily_truth", _noop_materialize)

    max_steps = -(-len(days) // 2) + 1  # ceil(5/2) + 1 = 4
    steps = 0
    while True:
        blob = _load_state(conn, site_id)
        phase = "discover" if blob is None else str(blob.get("phase"))
        if phase != "discover":
            break
        steps += 1
        assert steps <= max_steps, "discover did not terminate within bounded steps"
        advance_verification(conn, site_id, payload)

    rows = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=?", (site_id,)
    ).fetchone()
    assert int(rows["n"]) == 0  # the no-op never actually wrote anything


def test_o11_discover_runs_before_any_decision_is_recorded() -> None:
    """The very FIRST ``advance_verification`` call already materializes
    truth (proving ``discover`` runs first in phase order), and driving to
    the decision never records ``skipped`` along the way.

    Kills: placing ``discover`` after ``decide`` (or dropping it in favor
    of a separately scheduled job) -- either would leave ``daily_truth``
    empty on the first call, and the decide phase would then find no
    settled truth and record ``skipped`` as its first (and only) decision.
    """
    conn = _conn()
    site_id = _make_site(conn)
    days = [f"2026-06-{n:02d}" for n in range(1, 6)]
    _seed_obs_days(conn, site_id, days)
    payload: dict[str, object] = {"trigger_date": "2026-06-10"}

    cont = advance_verification(conn, site_id, payload)
    assert cont is True
    rows = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=?", (site_id,)
    ).fetchone()
    assert int(rows["n"]) > 0

    _drive_until_decided(conn, site_id, payload)
    assert _decisions(conn, site_id) == ["run_started"]
    assert "skipped" not in _decisions(conn, site_id)


def test_o13_window_recomputed_live_across_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observations arriving mid-chain, after chunk 1 already ran, are
    still picked up by a later chunk -- the discovery window is
    recomputed every chunk, never frozen into the chain-state blob.

    Kills: freezing ``observation_day_extent``/``settled_ceiling_local_
    date`` into the blob at chunk 1 and reusing it on resume (the
    tz_correction.py:222 anti-precedent) -- late days D6/D7 would never be
    picked up under that formulation, no matter how many further chunks
    run, since the frozen window would never include them.
    """
    conn = _conn()
    site_id = _make_site(conn)
    early = [f"2026-06-{n:02d}" for n in range(1, 6)]  # D1..D5
    _seed_obs_days(conn, site_id, early)
    payload: dict[str, object] = {"truth_discovery_days": 2}

    fixed_now_1 = datetime(2026, 6, 20, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.verification_run.utc_now", lambda: fixed_now_1)
    advance_verification(conn, site_id, payload)  # chunk 1: D1, D2

    generation_id = ensure_published_generation(conn, site_id)
    assert _truth_local_dates(conn, site_id, generation_id) == {early[0], early[1]}

    late = ["2026-06-06", "2026-06-07"]  # D6, D7 -- added AFTER chunk 1
    _seed_obs_days(conn, site_id, late)
    fixed_now_2 = datetime(2026, 6, 25, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.verification_run.utc_now", lambda: fixed_now_2)
    _drive_to_regen(conn, site_id, payload)

    assert _truth_local_dates(conn, site_id, generation_id) == set(early + late)


# ---------------------------------------------------------------------------
# O14-O18 -- D13 failure-boundary oracles, driven through a real
# file-backed Database (db.write_sync / BEGIN IMMEDIATE / rollback).
# ---------------------------------------------------------------------------


def test_o14_chain_level_fault_propagates_never_silently_cancelled(
    tmp_path: Path,
) -> None:
    """A dangling published-generation pointer is a chain-level fault: it
    must raise a raw ``ValueError`` (never a swallowed ``JobCancelled``),
    leave the chain-state blob intact through the raise, and only clear
    that blob once the job goes terminally failed.

    Kills: a blanket ``except ValueError: raise JobCancelled()`` wrapping
    the discover phase (would misclassify a real infra/data-integrity
    fault as an ordinary chain cancellation, silently dropping it from the
    retry ladder's loud-failure path).
    """
    db = _file_db(tmp_path)
    try:

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn)
            generation_id = ensure_published_generation(conn, site_id)
            _seed_obs_day(conn, site_id, "2026-06-01")
            # FK forces daily_truth to be empty under this generation for
            # the DELETE below to succeed -- true here since nothing has
            # materialized yet.
            conn.execute(
                "DELETE FROM timezone_generations WHERE id = ?", (generation_id,)
            )
            _save_state(conn, site_id, {"phase": "discover"})
            return site_id

        site_id = db.write_sync(_seed)

        payload: dict[str, object] = {"trigger_date": "2026-06-10"}
        with pytest.raises(ValueError):
            db.write_sync(lambda conn: advance_verification(conn, site_id, payload))

        blob_after_raise = db.read_sync(
            lambda conn: get_runtime_state(conn, verification_state_key(site_id))
        )
        assert blob_after_raise == '{"phase":"discover"}'

        def _force_terminal_failure(conn: sqlite3.Connection) -> None:
            enqueue_if_absent(
                conn, "verification_run", site_id, verification_job_key(site_id), {}
            )
            job = claim_next_job(conn)
            assert job is not None
            conn.execute("UPDATE jobs SET max_retries = 0 WHERE id = ?", (job.id,))
            disposition = _fail_job(conn, job, "forced")
            assert disposition is not None
            assert disposition.terminal

        db.write_sync(_force_terminal_failure)

        blob_after_fail = db.read_sync(
            lambda conn: get_runtime_state(conn, verification_state_key(site_id))
        )
        assert blob_after_fail is None
        run_state = db.read_sync(
            lambda conn: conn.execute(
                "SELECT status FROM jobs WHERE site_id=? AND type='verification_run'",
                (site_id,),
            ).fetchone()
        )
        assert str(run_state["status"]) == "failed"
    finally:
        db.close()


def test_o15_one_unbuildable_day_is_contained_chunk_commits(
    tmp_path: Path,
) -> None:
    """One corrupt day inside a limit-full chunk is contained: the rest of
    the chunk commits, the failed day's savepoint fully rolls back (proven
    via a sentinel written and then undone), exactly one ERROR is logged,
    and the cursor sits on the failed day so the SUCCESS path (not just a
    later retry) is what advances past it.

    Kills FOUR mutants from one construction (limit-full chunk with the
    failure on the LAST day of that chunk is the only shape where a
    success-only-return mutant is distinguishable from correct code -- with
    a larger limit both end states are byte-identical):
    uncontained propagation (chunk would raise instead of committing D1);
    a missing/dropped SAVEPOINT (the sentinel would survive, or an orphan
    ``ROLLBACK TO`` would raise ``OperationalError`` instead of a contained
    ``ValueError``); swallowing the fault without the ERROR log; and a
    success-only-return that omits the failed day from ``attempted``
    (cursor would then point at D1, not D2, at chunk boundary).
    """
    db = _file_db(tmp_path)
    try:

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn)
            days = [f"2026-06-{n:02d}" for n in range(1, 6)]
            _seed_obs_days(conn, site_id, days)
            return site_id

        site_id = db.write_sync(_seed)
        d1, d2, d3, d4, d5 = (f"2026-06-{n:02d}" for n in range(1, 6))

        def _corrupt_d2(conn: sqlite3.Connection) -> str:
            conn.execute(
                "UPDATE observations SET value = 'corrupt' "
                "WHERE site_id = ? AND valid_at = ?",
                (site_id, f"{d2}T12:00:00+00:00"),
            )
            row = conn.execute(
                "SELECT typeof(value) AS t FROM observations "
                "WHERE site_id = ? AND valid_at = ?",
                (site_id, f"{d2}T12:00:00+00:00"),
            ).fetchone()
            return str(row["t"])

        assert db.write_sync(_corrupt_d2) == "text"

        sentinel_key = "test:o15:sentinel"
        real_materialize = truth.materialize_daily_truth

        def _wrapper(
            conn: sqlite3.Connection,
            *,
            site_id: int,
            local_date: str,
            tz_generation_id: int | None = None,
        ) -> dict[str, object]:
            if local_date == d2:
                set_runtime_state(conn, sentinel_key, "1")
            return real_materialize(
                conn,
                site_id=site_id,
                local_date=local_date,
                tz_generation_id=tz_generation_id,
            )

        import pytest as _pytest  # local alias to keep monkeypatch scoped here

        mp = _pytest.MonkeyPatch()
        mp.setattr(truth, "materialize_daily_truth", _wrapper)
        try:
            payload: dict[str, object] = {"truth_discovery_days": 2}
            caplog = _CapLogAdapter()
            with caplog.capture(logging.ERROR, "wxverify.verification.truth"):
                db.write_sync(lambda conn: advance_verification(conn, site_id, payload))

            blob = db.read_sync(lambda conn: _load_state(conn, site_id))
            assert blob == {"phase": "discover", "truth_cursor": d2}

            d1_rows = db.read_sync(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) AS n FROM daily_truth "
                    "WHERE site_id=? AND local_date=?",
                    (site_id, d1),
                ).fetchone()
            )
            assert int(d1_rows["n"]) == 5
            d2_rows = db.read_sync(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) AS n FROM daily_truth "
                    "WHERE site_id=? AND local_date=?",
                    (site_id, d2),
                ).fetchone()
            )
            assert int(d2_rows["n"]) == 0
            sentinel = db.read_sync(lambda conn: get_runtime_state(conn, sentinel_key))
            assert sentinel is None

            error_records = [
                r
                for r in caplog.records
                if r.levelno == logging.ERROR
                and "daily_truth discovery: day data failed" in r.getMessage()
            ]
            assert len(error_records) == 1
            assert f"site={site_id}" in error_records[0].getMessage()
            assert f"local_date={d2}" in error_records[0].getMessage()

            _drive_to_regen_db(db, site_id, payload)

            generation_id = db.read_sync(
                lambda conn: ensure_published_generation(conn, site_id)
            )
            present = db.read_sync(
                lambda conn: _truth_local_dates(conn, site_id, generation_id)
            )
            assert present == {d1, d3, d4, d5}
            final_blob = db.read_sync(lambda conn: _load_state(conn, site_id))
            assert final_blob == {"phase": "regen"}
        finally:
            mp.undo()
    finally:
        db.close()


def test_o16_missing_site_cancels_the_chain(tmp_path: Path) -> None:
    """A site deleted out from under a queued chain is the ONE condition
    ``discover`` cancels for -- a precondition probe, not a day fault.

    Kills: dropping the site-existence precheck (the discover phase would
    instead crash trying to compute the ceiling/window against a site that
    no longer exists, or silently no-op).
    """
    db = _file_db(tmp_path)
    try:

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn)
            _seed_obs_days(conn, site_id, ["2026-06-01", "2026-06-02"])
            return site_id

        site_id = db.write_sync(_seed)
        db.write_sync(
            lambda conn: conn.execute("DELETE FROM sites WHERE id=?", (site_id,))
        )

        from wxverify.worker.control import JobCancelled

        payload: dict[str, object] = {"trigger_date": "2026-06-10"}
        with pytest.raises(JobCancelled):
            db.write_sync(lambda conn: advance_verification(conn, site_id, payload))
    finally:
        db.close()


def test_o17_site_wide_precondition_fault_is_a_preamble_fault(
    tmp_path: Path,
) -> None:
    """A corrupt site-wide config value (``rain_threshold_mm``) is a
    PREAMBLE fault -- raised once, before any per-day work -- never N
    contained per-day faults.

    Kills: leaving the ``float()`` coercion only inside
    ``materialize_daily_truth``'s per-day body (would commit every day in
    the window with a per-day ERROR logged for each, materializing rows
    under a misclassification of a site-wide fault as N day faults).
    """
    db = _file_db(tmp_path)
    try:

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn)
            _seed_obs_days(conn, site_id, ["2026-06-01", "2026-06-02"])
            return site_id

        site_id = db.write_sync(_seed)

        def _corrupt_threshold(conn: sqlite3.Connection) -> str:
            conn.execute(
                "UPDATE sites SET rain_threshold_mm = 'not-a-number' WHERE id=?",
                (site_id,),
            )
            row = conn.execute(
                "SELECT typeof(rain_threshold_mm) AS t FROM sites WHERE id=?",
                (site_id,),
            ).fetchone()
            return str(row["t"])

        assert db.write_sync(_corrupt_threshold) == "text"

        payload: dict[str, object] = {"trigger_date": "2026-06-10"}
        caplog = _CapLogAdapter()
        with (
            caplog.capture(logging.ERROR, "wxverify.worker.verification_run"),
            caplog.capture(logging.ERROR, "wxverify.verification.truth"),
            pytest.raises(ValueError),
        ):
            db.write_sync(lambda conn: advance_verification(conn, site_id, payload))

        day_fault_records = [
            r
            for r in caplog.records
            if "daily_truth discovery: day data failed" in r.getMessage()
        ]
        assert day_fault_records == []
        rows = db.read_sync(
            lambda conn: conn.execute(
                "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=?", (site_id,)
            ).fetchone()
        )
        assert int(rows["n"]) == 0
    finally:
        db.close()


def test_o18_infra_fault_inside_per_day_body_propagates_whole_chunk(
    tmp_path: Path,
) -> None:
    """An infra fault (``sqlite3.OperationalError``) raised from inside the
    per-day body is NOT day data: it propagates, and the WHOLE chunk rolls
    back -- including the earlier, successfully-materialized day in the
    same transaction. This assertion requires the real ``BEGIN IMMEDIATE``
    path: a bare connection's legacy isolation would keep D1's uncommitted
    rows visible to the SAME connection's own SELECT even though they never
    actually commit.

    Kills: ``except Exception`` in place of ``except ValueError`` in
    ``materialize_missing_truth_days``'s per-day containment (would catch
    and contain the OperationalError as if it were ordinary day data,
    leaving D1 committed and D2 silently logged instead of the whole chunk
    rolling back).
    """
    db = _file_db(tmp_path)
    try:

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn)
            _seed_obs_days(conn, site_id, ["2026-06-01", "2026-06-02"])
            _save_state(conn, site_id, {"phase": "discover"})
            return site_id

        site_id = db.write_sync(_seed)
        d1, d2 = "2026-06-01", "2026-06-02"
        real_materialize = truth.materialize_daily_truth

        def _fake(
            conn: sqlite3.Connection,
            *,
            site_id: int,
            local_date: str,
            tz_generation_id: int | None = None,
        ) -> dict[str, object]:
            if local_date == d2:
                raise sqlite3.OperationalError("disk I/O error")
            return real_materialize(
                conn,
                site_id=site_id,
                local_date=local_date,
                tz_generation_id=tz_generation_id,
            )

        import pytest as _pytest

        mp = _pytest.MonkeyPatch()
        mp.setattr(truth, "materialize_daily_truth", _fake)
        try:
            payload: dict[str, object] = {"truth_discovery_days": 5}
            caplog = _CapLogAdapter()
            with (
                caplog.capture(logging.ERROR, "wxverify.verification.truth"),
                pytest.raises(sqlite3.OperationalError),
            ):
                db.write_sync(lambda conn: advance_verification(conn, site_id, payload))

            d1_rows = db.read_sync(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) AS n FROM daily_truth "
                    "WHERE site_id=? AND local_date=?",
                    (site_id, d1),
                ).fetchone()
            )
            assert int(d1_rows["n"]) == 0  # whole chunk rolled back

            blob = db.read_sync(lambda conn: _load_state(conn, site_id))
            assert blob == {"phase": "discover"}

            day_fault_records = [
                r
                for r in caplog.records
                if "daily_truth discovery: day data failed" in r.getMessage()
            ]
            assert day_fault_records == []
        finally:
            mp.undo()
    finally:
        db.close()


def _drive_to_regen_db(
    db: Database,
    site_id: int,
    payload: dict[str, object],
    *,
    max_steps: int = 50,
) -> None:
    for _ in range(max_steps):
        blob = db.read_sync(lambda conn: _load_state(conn, site_id))
        phase = "discover" if blob is None else str(blob.get("phase"))
        if phase != "discover":
            return
        db.write_sync(lambda conn: advance_verification(conn, site_id, payload))
    raise AssertionError("discover phase did not terminate within max_steps")


class _CapLogAdapter:
    """Minimal log-record capture, scoped to a specific logger name.

    ``pytest``'s own ``caplog`` fixture is awkward to nest inside a
    ``with``-managed context alongside ``pytest.raises`` here because the
    exception path needs the records gathered up to and including the
    raise; a small dedicated handler keeps the two oracles' assertions
    readable without fighting fixture ordering.
    """

    def __init__(self) -> None:
        self.records: list[logging.LogRecord] = []

    def capture(self, level: int, logger_name: str) -> _CaptureContext:
        return _CaptureContext(self, level, logger_name)


class _CaptureContext:
    def __init__(self, adapter: _CapLogAdapter, level: int, logger_name: str) -> None:
        self._adapter = adapter
        self._level = level
        self._logger = logging.getLogger(logger_name)
        self._handler = _ListHandler(adapter.records)

    def __enter__(self) -> None:
        self._prev_level = self._logger.level
        self._logger.setLevel(min(self._level, self._logger.level or self._level))
        self._logger.addHandler(self._handler)

    def __exit__(self, *exc_info: object) -> None:
        self._logger.removeHandler(self._handler)
        self._logger.setLevel(self._prev_level)


class _ListHandler(logging.Handler):
    def __init__(self, sink: list[logging.LogRecord]) -> None:
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        self._sink.append(record)
