"""§18.5 forecast-of-record integrity oracle suite (phase 5 QA).

Complements ``tests/test_forecast_record.py`` (the implementer suite) with
the adversarial oracles: first-write-cutoff bit-identity between an at-T
build and a late-but-in-window build over a post-T-poisoned database,
retry-confirms-never-replaces on the full row dump, generation binding
resolved at T (not the published pointer), the single-writer-of-``missed``
handoff between the daily job and the gap scan, the inclusive late-write
window boundary on both write paths, gap-scan reconstruction equality with
an at-T build, and the empty-grid latitude pin. All fixture values are
synthetic (fake site names, ``example-src`` feeds).

Every test names the production mutation it kills; the mutation loop ran
each mutation against production code, observed the named oracle red, and
restored the file byte-identical (sha256-verified, stale-``__pycache__``
purged per run).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_pair,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.core.units import ms_to_kmh
from wxverify.db.tz_generations import (
    apply_prospective_change,
    ensure_published_generation,
    published_generation_id,
)
from wxverify.forecast.service import build_forecast
from wxverify.settings.keys import set_setting
from wxverify.verification.methodology import LATE_WRITE_WINDOW_HOURS
from wxverify.verification.record import (
    MISSED_WINDOW_CLOSED,
    build_forecast_record,
    resolve_snapshot_utc,
    run_record_gap_scan,
)
from wxverify.worker.control import JobCancelled

_DAY = date(2035, 6, 15)

#: Columns legitimately different between two builds of the SAME product:
#: surrogate key, wall-clock insert stamp, execution-time latency, and the
#: observed (diagnostic, §7-stratifiable) production cache condition.
_NON_PRODUCT_COLUMNS = frozenset(
    {"id", "created_at", "write_latency_seconds", "leaderboard_status"}
)


def _t(local_date: date = _DAY) -> datetime:
    return resolve_snapshot_utc("UTC", local_date, "07:00")


def _insert_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    value: float,
    variable: str = "temperature",
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
            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


#: (variable, feed-A value, feed-B value) for the shared fixture. All three
#: variables are seeded so the written grid covers every identity the fixture's
#: 48 h lead cap can reach -- a one-variable fixture would make the row-count
#: non-vacuity checks below trivially true.
_FIXTURE_VARIABLES = (
    ("temperature", 10.0, 12.0),
    ("wind", 5.0, 6.0),
    ("precip", 0.0, 1.0),
)

#: The identities `_seed_base_fixture` makes knowable at T: 3 variables x the
#: 2 target days reachable within 48 h of lead.
_FIXTURE_GRID_SIZE = len(_FIXTURE_VARIABLES) * 2


def _seed_full_grid(
    conn: sqlite3.Connection, site_id: int, day: date, *, tag: str = ""
) -> int:
    """Make every identity of snapshot ``day`` knowable at its T.

    A record day now carries only the identities that had samples at T, so any
    oracle asserting on the complete 3 x 8 grid has to seed one. The feed is
    created here rather than through ``asof_make_real_feed`` because that
    helper caps leads at 48 h, which cannot reach display day 7.
    """
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 192)
        """,
        (f"grid-{day.isoformat()}{tag}",),
    )
    assert cur.lastrowid is not None
    feed_id = int(cur.lastrowid)
    issued = datetime.combine(day, datetime.min.time(), UTC)
    for offset in range(8):
        for variable, value, _ in _FIXTURE_VARIABLES:
            _insert_day_samples(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                local_date=day + timedelta(days=offset),
                issued_at=isoformat_utc(issued),
                fetched_at=isoformat_utc(issued + timedelta(minutes=5)),
                value=value,
                variable=variable,
            )
    return feed_id


def _seed_base_fixture(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Deterministic pre-T world shared by both sides of every bit-identity
    comparison: two feeds with knowable day-0/day-1 samples, feed A ahead of
    feed B on knowable pair count (the depth-1 scored-rung winner at T).
    """
    site_id = asof_make_site(conn, "record-oracle-site")
    ensure_published_generation(conn, site_id)
    feed_a = asof_make_real_feed(conn, "model-a")
    feed_b = asof_make_real_feed(conn, "model-b")
    # Feed C exists on BOTH sides of every comparison and stays sample-less
    # here; only the poisoned side gives it post-T samples. The candidate
    # universe is write-time feed state (§13), so creating the row only on the
    # poisoned side would differ the dumps for a fixture reason and mask the
    # leak the oracle is actually hunting.
    asof_make_real_feed(conn, "model-c")
    set_setting(conn, "forecast_blend_depth", "1")
    for local_date in (_DAY, _DAY + timedelta(days=1)):
        day_before = local_date - timedelta(days=1)
        issued = f"{day_before.isoformat()}T06:00:00Z"
        fetched = f"{day_before.isoformat()}T06:05:00Z"
        for variable, value_a, value_b in _FIXTURE_VARIABLES:
            _insert_day_samples(
                conn,
                site_id=site_id,
                feed_id=feed_a,
                local_date=local_date,
                issued_at=issued,
                fetched_at=fetched,
                value=value_a,
                variable=variable,
            )
            _insert_day_samples(
                conn,
                site_id=site_id,
                feed_id=feed_b,
                local_date=local_date,
                issued_at=issued,
                fetched_at=fetched,
                value=value_b,
                variable=variable,
            )
    # Knowable-at-T pairs: A has 2, B has 1 -> scored rung ranks A first.
    for hour in (10, 11):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            valid_at=f"2035-06-12T{hour:02d}:00:00Z",
            issued_at="2035-06-11T10:00:00Z",
            forecast=10.0,
            observed=10.5,
            first_known_at="2035-06-12T12:00:00Z",
        )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_b,
        valid_at="2035-06-12T10:00:00Z",
        issued_at="2035-06-11T10:00:00Z",
        forecast=12.0,
        observed=10.5,
        first_known_at="2035-06-12T12:00:00Z",
    )
    return site_id, feed_a, feed_b


def _poison_post_t(
    conn: sqlite3.Connection, *, site_id: int, feed_b: int, snapshot_utc: datetime
) -> int:
    """Inject data knowable only AFTER T (the §18.5 adversary): a brand-new
    feed fetched post-T, five post-T pairs flipping the pair-count ranking
    to feed B, and a ``score_cache`` row that would make B the confident
    live-path winner. A leak through ANY of the three as-of seams changes
    the record's candidates, ranking, or selection.
    """
    row = conn.execute("SELECT id FROM feeds WHERE model = 'model-c'").fetchone()
    assert row is not None
    feed_c = int(row["id"])
    post = isoformat_utc(snapshot_utc + timedelta(hours=1))
    for local_date in (_DAY, _DAY + timedelta(days=2)):
        _insert_day_samples(
            conn,
            site_id=site_id,
            feed_id=feed_c,
            local_date=local_date,
            issued_at=post,
            fetched_at=post,
            value=50.0,
        )
    for hour in range(5):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            valid_at=f"2035-06-13T{10 + hour:02d}:00:00Z",
            issued_at="2035-06-12T10:00:00Z",
            forecast=12.0,
            observed=12.1,
            first_known_at=post,
        )
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, mae,
             skill_score, computed_at)
        VALUES (?, ?, 'temperature', 1, 'w:30', 50, 0.1, 0.9, ?)
        """,
        (site_id, feed_b, post),
    )
    return feed_c


def _comparable_dump(
    conn: sqlite3.Connection,
    site_id: int,
    snapshot_local_date: str,
    *,
    also_exclude: Iterable[str] = (),
) -> list[tuple[object, ...]]:
    excluded = _NON_PRODUCT_COLUMNS | set(also_exclude)
    cols = [
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(forecast_of_record)")
        if str(row["name"]) not in excluded
    ]
    rows = conn.execute(
        "SELECT "
        + ", ".join(cols)
        + " FROM forecast_of_record WHERE site_id = ? AND snapshot_local_date = ?"
        " ORDER BY variable, target_local_date",
        (site_id, snapshot_local_date),
    ).fetchall()
    return [tuple(row) for row in rows]


def _cell(
    conn: sqlite3.Connection,
    site_id: int,
    snapshot_local_date: str,
    variable: str,
    display_lead: int,
) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = ?
          AND variable = ? AND display_lead = ?
        """,
        (site_id, snapshot_local_date, variable, display_lead),
    ).fetchone()
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# Oracle 1 — first-write cutoff: a record built late (post-T data present in
# the DB) is bit-identical to the record an at-T build produces.
# ---------------------------------------------------------------------------


def test_first_write_cutoff_late_build_bit_identical_to_at_t() -> None:
    """§18.5 first-write cutoff / §7 Construction (as-of parameterization).

    Kills, one at a time:
    - dropping ``as_of=as_of`` from the ``load_future_samples`` call in
      ``build_forecast_record`` (post-T feed C leaks into candidates);
    - ``as_of = isoformat_utc(at)`` instead of ``snapshot_utc`` (post-T
      pairs/samples become "knowable" on the late build);
    - dropping ``as_of=as_of`` from the ``forecast_ranking`` call (the
      ranking falls back to the live cache-backed path; the incomplete
      poisoned ``score_cache`` snapshot reads ``rebuilding`` -> EMPTY rows,
      so every candidate's pair evidence collapses to zero — caught by the
      absolute hand-derived pair-count assertions below, NOT by the dump
      comparison, which a both-sides-equally-degraded mutant slips past).
    """
    t = _t()
    conn_at_t = asof_conn()
    site_at_t, feed_a, _ = _seed_base_fixture(conn_at_t)
    build_forecast_record(conn_at_t, site_at_t, _DAY.isoformat(), now=t)
    # ``write_path`` records WHICH path wrote the row, not the product it
    # wrote; it is derived from the build clock and so differs by construction
    # between an at-T and an in-window late build.
    at_t_dump = _comparable_dump(
        conn_at_t, site_at_t, _DAY.isoformat(), also_exclude=("write_path",)
    )

    conn_late = asof_conn()
    site_late, feed_a_late, feed_b_late = _seed_base_fixture(conn_late)
    assert feed_a_late == feed_a  # identical construction order by design
    _poison_post_t(conn_late, site_id=site_late, feed_b=feed_b_late, snapshot_utc=t)
    build_forecast_record(
        conn_late, site_late, _DAY.isoformat(), now=t + timedelta(hours=6)
    )
    late_dump = _comparable_dump(
        conn_late, site_late, _DAY.isoformat(), also_exclude=("write_path",)
    )

    # Non-vacuity: every identity the fixture makes knowable, on both sides.
    assert len(at_t_dump) == _FIXTURE_GRID_SIZE
    assert late_dump == at_t_dump

    # Independent reconstruction from the fixture (not the code under test):
    # at T, feed A (2 knowable pairs > B's 1) wins the depth-1 scored rung,
    # and the day-0 blend is A's constant 10.0 series.
    day0 = _cell(conn_late, site_late, _DAY.isoformat(), "temperature", 0)
    assert json.loads(str(day0["selected_feed_ids"])) == [feed_a]
    hourly = json.loads(str(day0["hourly_values"]))
    assert len(hourly) == 24
    assert all(value == 10.0 for _, value in hourly)
    # Absolute pair evidence, reconstructed by hand from the fixture: at T,
    # feed A has exactly 2 knowable pairs and feed B exactly 1 (its 5 later
    # pairs have first_known_at > T). Any ranking-path leak or collapse
    # (live-cache fallback, execution-time as_of) breaks these numbers even
    # when it degrades BOTH builds identically.
    candidates = {int(c["feed_id"]): c for c in json.loads(str(day0["candidates"]))}
    assert candidates[feed_a]["pair_n"] == 2
    assert candidates[feed_b_late]["pair_n"] == 1


# ---------------------------------------------------------------------------
# Oracle 2 — retries confirm, never replace: full-dump immutability across a
# post-T retry and a same-day gap scan.
# ---------------------------------------------------------------------------


def test_retry_after_post_t_data_confirms_never_replaces() -> None:
    """§7 Identity/Immutability: after post-T samples, pairs, and cache rows
    land, a retry of the daily job AND a gap scan leave the already-written
    rows byte-identical on EVERY column (id and created_at included).

    Kills: replacing the record insert's ``ON CONFLICT ... DO NOTHING`` with
    ``DO UPDATE SET hourly_values = excluded.hourly_values,
    selected_feed_ids = excluded.selected_feed_ids`` (the classic
    retry-overwrites-with-fresher-data bug).
    """
    t = _t()
    conn = asof_conn()
    site_id, _, feed_b = _seed_base_fixture(conn)
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(minutes=5))
    before = [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM forecast_of_record WHERE site_id = ? ORDER BY id",
            (site_id,),
        )
    ]
    assert len(before) == _FIXTURE_GRID_SIZE

    _poison_post_t(conn, site_id=site_id, feed_b=feed_b, snapshot_utc=t)
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(hours=6))
    assert run_record_gap_scan(conn, site_id, {}, now=t + timedelta(hours=6)) is None
    after = [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM forecast_of_record WHERE site_id = ? ORDER BY id",
            (site_id,),
        )
    ]
    assert after == before


# ---------------------------------------------------------------------------
# Oracle 3 — generation binding: the record binds the generation resolved AT
# T, not the published pointer (§18.7-style, via a prospective change).
# ---------------------------------------------------------------------------


def test_generation_binding_resolves_at_t_not_pointer() -> None:
    """§14: derivation-time resolution is the published generation whose
    effective interval contains T. After a prospective change the pointer
    moves to the new generation immediately, but a record for a day whose T
    precedes the effective boundary must bind the OLD generation.

    Kills: ``_current_generation`` returning the pointer unconditionally
    (``return pointer`` — dropping ``resolve_generation_for_instant``).
    """
    conn = asof_conn()
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('record-genbind-site', 47.0, 25.0, 900.0, 'Etc/GMT-3')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    gen_old = ensure_published_generation(conn, site_id)
    gen_new = apply_prospective_change(
        conn, site_id, "Etc/GMT-2", "2035-06-20T00:00:00Z"
    )
    assert published_generation_id(conn, site_id) == gen_new
    # Only sampled identities are written now, so each probed day needs data.
    _seed_full_grid(conn, site_id, date(2035, 6, 15))
    _seed_full_grid(conn, site_id, date(2035, 6, 25))

    # Day before the boundary: T resolves inside gen_old's interval.
    build_forecast_record(
        conn,
        site_id,
        "2035-06-15",
        now=datetime(2035, 6, 15, 6, 0, tzinfo=UTC),
    )
    pre_rows = conn.execute(
        """
        SELECT DISTINCT tz_generation_id FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = '2035-06-15'
        """,
        (site_id,),
    ).fetchall()
    assert [int(r["tz_generation_id"]) for r in pre_rows] == [gen_old]

    # Paired positive: a day past the boundary binds the new generation.
    build_forecast_record(
        conn,
        site_id,
        "2035-06-25",
        now=datetime(2035, 6, 25, 6, 0, tzinfo=UTC),
    )
    post_rows = conn.execute(
        """
        SELECT DISTINCT tz_generation_id FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = '2035-06-25'
        """,
        (site_id,),
    ).fetchall()
    assert [int(r["tz_generation_id"]) for r in post_rows] == [gen_new]


# ---------------------------------------------------------------------------
# Oracle 4 — single writer of `missed`: the daily job refuses a closed
# window and writes NOTHING; the gap scan then writes the full missed grid,
# exactly once, and a forced revisit leaves it untouched.
# ---------------------------------------------------------------------------


def test_daily_job_never_writes_missed_gap_scan_writes_full_grid_once() -> None:
    """§7 Missed snapshots + §14 single-writer rule.

    Kills, one at a time:
    - in ``build_forecast_record``, replacing the beyond-window
      ``raise JobCancelled()`` with a ``_write_missed_rows(...)`` call +
      ``return`` (the "helpful" daily job writing missed itself);
    - in ``_write_missed_rows``, ``range(RECORD_DAY_COUNT)`` ->
      ``range(1, RECORD_DAY_COUNT)`` (an incomplete missed grid).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "record-missed-site")
    ensure_published_generation(conn, site_id)
    # Seed the log start: a complete on-time record for _DAY, plus the data
    # that makes every identity of the following day knowable at ITS T -- the
    # missed grid's reasons come from that as-of-T assessment.
    _seed_full_grid(conn, site_id, _DAY)
    _seed_full_grid(conn, site_id, _DAY + timedelta(days=1))
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=_t() + timedelta(minutes=5)
    )

    target = _DAY + timedelta(days=1)
    late = _t(target) + timedelta(hours=LATE_WRITE_WINDOW_HOURS, minutes=60)
    with pytest.raises(JobCancelled):
        build_forecast_record(conn, site_id, target.isoformat(), now=late)
    none_yet = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_of_record"
        " WHERE site_id = ? AND snapshot_local_date = ?",
        (site_id, target.isoformat()),
    ).fetchone()
    assert int(none_yet["n"]) == 0  # the failing attempt wrote NOTHING

    assert run_record_gap_scan(conn, site_id, {}, now=late) is None
    missed = conn.execute(
        """
        SELECT status, missed_reason, write_path, variable, display_lead
        FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = ?
        """,
        (site_id, target.isoformat()),
    ).fetchall()
    assert len(missed) == 24  # 3 variables x 8 target days, no shrunk grid
    assert all(str(r["status"]) == "missed" for r in missed)
    assert all(str(r["missed_reason"]) == MISSED_WINDOW_CLOSED for r in missed)
    assert all(r["write_path"] is None for r in missed)
    assert {int(r["display_lead"]) for r in missed} == set(range(8))

    # Forced revisit (explicit cursor before the missed day): still 24 rows,
    # byte-identical -- exactly-once semantics under a rescan.
    before = [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM forecast_of_record"
            " WHERE site_id = ? AND snapshot_local_date = ? ORDER BY id",
            (site_id, target.isoformat()),
        )
    ]
    run_record_gap_scan(conn, site_id, {"after_date": _DAY.isoformat()}, now=late)
    after = [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM forecast_of_record"
            " WHERE site_id = ? AND snapshot_local_date = ? ORDER BY id",
            (site_id, target.isoformat()),
        )
    ]
    assert after == before


# ---------------------------------------------------------------------------
# Oracle 5 — the late-write window boundary is inclusive on BOTH paths.
# ---------------------------------------------------------------------------


def test_late_write_window_boundary_is_inclusive_on_both_paths() -> None:
    """§7/§14: at exactly T + LATE_WRITE_WINDOW_HOURS a reconstruction is
    still permitted -- the daily job writes, and the gap scan
    late-reconstructs rather than writing ``missed``.

    Both paths now stamp ``late_reconstruction``: ``write_path`` is derived
    from how far past T the build ran (§4), not from which caller ran it, so
    the boundary build is late whichever path reaches it. The old latitude pin
    (daily job always ``on_time``) made the column describe the caller.

    Kills, one at a time:
    - in ``build_forecast_record``, ``at > snapshot_utc + window`` -> ``>=``
      (daily build at the exact boundary cancels);
    - in ``run_record_gap_scan``, ``at <= snapshot_utc + window`` -> ``<``
      (the scan marks the exact-boundary day missed);
    - the 0.11.0 implementation itself, ``write_path`` chosen by caller
      identity (the daily job stamps ``on_time``, the gap scan
      ``late_reconstruction``): a boundary build reached by the daily job
      would report ``on_time`` here, hiding a 48-hour-late write behind the
      column that is supposed to expose it.
    """
    boundary = timedelta(hours=LATE_WRITE_WINDOW_HOURS)

    conn_daily = asof_conn()
    site_daily = asof_make_site(conn_daily, "record-boundary-daily")
    ensure_published_generation(conn_daily, site_daily)
    _seed_full_grid(conn_daily, site_daily, _DAY)
    build_forecast_record(conn_daily, site_daily, _DAY.isoformat(), now=_t() + boundary)
    rows = conn_daily.execute(
        "SELECT DISTINCT status, write_path FROM forecast_of_record WHERE site_id = ?",
        (site_daily,),
    ).fetchall()
    assert [(str(r["status"]), str(r["write_path"])) for r in rows] == [
        ("recorded", "late_reconstruction")
    ]

    conn_scan = asof_conn()
    site_scan = asof_make_site(conn_scan, "record-boundary-scan")
    ensure_published_generation(conn_scan, site_scan)
    _seed_full_grid(conn_scan, site_scan, _DAY)
    _seed_full_grid(conn_scan, site_scan, _DAY + timedelta(days=1))
    build_forecast_record(
        conn_scan, site_scan, _DAY.isoformat(), now=_t() + timedelta(minutes=5)
    )
    target = _DAY + timedelta(days=1)
    assert (
        run_record_gap_scan(conn_scan, site_scan, {}, now=_t(target) + boundary) is None
    )
    scan_rows = conn_scan.execute(
        """
        SELECT DISTINCT status, write_path FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = ?
        """,
        (site_scan, target.isoformat()),
    ).fetchall()
    assert [(str(r["status"]), str(r["write_path"])) for r in scan_rows] == [
        ("recorded", "late_reconstruction")
    ]


# ---------------------------------------------------------------------------
# Oracle 6 — gap-scan late reconstruction equals the at-T build.
# ---------------------------------------------------------------------------


def test_gap_scan_reconstruction_equals_at_t_build() -> None:
    """§7 Construction: the gap scan's in-window reconstruction runs the
    SAME as-of construction as an at-T build -- on a post-T-poisoned
    database the reconstructed rows equal the at-T rows on every product
    column (write path/latency metadata aside).

    Kills: dropping ``now=at`` from the gap scan's
    ``build_forecast_record(...)`` call (the reconstruction re-anchors on
    wall-clock ``utc_now``, and the 2035 fixture day surfaces it loudly).
    """
    target = _DAY + timedelta(days=1)
    t_target = _t(target)

    conn_at_t = asof_conn()
    site_at_t, feed_a, _ = _seed_base_fixture(conn_at_t)
    build_forecast_record(conn_at_t, site_at_t, target.isoformat(), now=t_target)
    at_t_dump = _comparable_dump(
        conn_at_t, site_at_t, target.isoformat(), also_exclude=("write_path",)
    )

    conn_scan = asof_conn()
    site_scan, _, feed_b = _seed_base_fixture(conn_scan)
    build_forecast_record(
        conn_scan, site_scan, _DAY.isoformat(), now=_t() + timedelta(minutes=5)
    )
    _poison_post_t(conn_scan, site_id=site_scan, feed_b=feed_b, snapshot_utc=t_target)
    assert (
        run_record_gap_scan(
            conn_scan, site_scan, {}, now=t_target + timedelta(hours=23)
        )
        is None
    )
    scan_dump = _comparable_dump(
        conn_scan, site_scan, target.isoformat(), also_exclude=("write_path",)
    )

    # Snapshot day _DAY+1 is the fixture's last seeded target date, so only its
    # display-lead-0 identities are knowable -- one per variable.
    assert len(at_t_dump) == len(_FIXTURE_VARIABLES)
    assert scan_dump == at_t_dump
    day0 = _cell(conn_scan, site_scan, target.isoformat(), "temperature", 0)
    assert json.loads(str(day0["selected_feed_ids"])) == [feed_a]
    assert str(day0["write_path"]) == "late_reconstruction"


# ---------------------------------------------------------------------------
# Oracle 7 — a candidate-less cell is ABSENT, not an empty `recorded` row.
# ---------------------------------------------------------------------------


def test_zero_sample_day_writes_no_rows() -> None:
    """§4: an identity with nothing knowable at T gets no row at all.

    This inverts the pre-0.11.1 latitude pin, which asserted a full 24-row
    grid of empty ``recorded`` rows for a day with no samples. That shape was
    the defect: an empty ``recorded`` row asserts "this is what was forecast"
    for a cell where nothing was, and it is indistinguishable from a genuine
    all-feeds-silent product. Absence is now the honest encoding, and the gap
    scan remains the single writer that turns a closed window into ``missed``.

    Kills, one at a time:
    - restoring the unconditional per-cell INSERT (candidate-less cells come
      back as empty ``recorded`` rows);
    - an ``if not samples: raise JobCancelled()`` guard (the daily job must
      still complete cleanly on a partial day, writing what it knows).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "record-empty-site")
    ensure_published_generation(conn, site_id)
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=_t() + timedelta(minutes=5)
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_of_record WHERE site_id = ?",
            (site_id,),
        ).fetchone()["n"]
        == 0
    )

    # Paired positive, so the assertion above is not vacuously satisfied by a
    # builder that writes nothing at all: the same day WITH samples is written.
    other = asof_make_site(conn, "record-partial-site")
    ensure_published_generation(conn, other)
    _seed_full_grid(conn, other, _DAY, tag="-partial")
    build_forecast_record(
        conn, other, _DAY.isoformat(), now=_t() + timedelta(minutes=5)
    )
    rows = conn.execute(
        "SELECT status, selected_feed_ids FROM forecast_of_record WHERE site_id = ?",
        (other,),
    ).fetchall()
    assert len(rows) == 24
    assert all(str(r["status"]) == "recorded" for r in rows)
    assert all(json.loads(str(r["selected_feed_ids"])) != [] for r in rows)


# ---------------------------------------------------------------------------
# Oracle 8 — displayed-dailies parity (§18.6 record ≡ live page at T=now):
# the record's stored displayed daily quantities equal the Forecast tile's,
# under feeds with different peak hours and different coverage.
# ---------------------------------------------------------------------------


def _insert_var_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    local_date: date,
    hours: range,
    value_fn: Callable[[int], float],
) -> None:
    issued = f"{(local_date - timedelta(days=1)).isoformat()}T06:00:00Z"
    fetched = f"{(local_date - timedelta(days=1)).isoformat()}T06:05:00Z"
    for hour in hours:
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, 24, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued,
                isoformat_utc(valid),
                value_fn(hour),
                fetched,
            ),
        )


def test_record_displayed_dailies_match_live_page_at_t() -> None:
    """§18.6/§6/§7: the record's stored DISPLAYED dailies are the production
    aggregate-per-feed-then-blend values over the >= 18h clearing subset —
    byte-equal to the live Forecast tile built at the same instant T.

    Fixture is adversarial on both axes the shared math must respect:
    feed A covers all 24 hours (clears coverage) with its peak at hour 15;
    feed B covers only 14 hours (spread-adequate, selected, but NOT
    clearing) with a much higher peak at hour 5. Kills, one at a time:
    - blend-order mutant: the record aggregating over the BLENDED hourly
      series (mean-of-hours first) instead of per feed;
    - clearing-subset mutant: the record skipping the ``clears_coverage``
      rule and aggregating over all selected feeds.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "record-parity-site")
    ensure_published_generation(conn, site_id)
    feed_a = asof_make_real_feed(conn, "model-a")
    feed_b = asof_make_real_feed(conn, "model-b")
    # Feed C exists on BOTH sides of every comparison and stays sample-less
    # here; only the poisoned side gives it post-T samples. The candidate
    # universe is write-time feed state (§13), so creating the row only on the
    # poisoned side would differ the dumps for a fixture reason and mask the
    # leak the oracle is actually hunting.
    asof_make_real_feed(conn, "model-c")
    site = conn.execute(
        "SELECT timezone, rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    assert site is not None
    threshold = float(site["rain_threshold_mm"])
    t = _t()

    seeds: list[tuple[str, Callable[[int], float], Callable[[int], float]]] = [
        (
            "temperature",
            lambda h: 20.0 if h == 15 else 10.0,
            lambda h: 50.0 if h == 5 else 8.0,
        ),
        ("wind", lambda h: 8.0 if h == 15 else 3.0, lambda h: 20.0),
        (
            "precip",
            lambda h: threshold if 12 <= h <= 17 else 0.0,
            lambda h: threshold + 1.0,
        ),
    ]
    for variable, fn_a, fn_b in seeds:
        _insert_var_day(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            variable=variable,
            local_date=_DAY,
            hours=range(24),
            value_fn=fn_a,
        )
        _insert_var_day(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            variable=variable,
            local_date=_DAY,
            hours=range(14),
            value_fn=fn_b,
        )

    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t)
    view = build_forecast(
        conn,
        site_id=site_id,
        timezone=str(site["timezone"]),
        rain_threshold_mm=threshold,
        now=t,
    )
    tile = view.tiles[0]

    def displayed(variable: str) -> dict[str, object]:
        row = _cell(conn, site_id, _DAY.isoformat(), variable, 0)
        # Both feeds selected (fixture validity); only A clears coverage.
        assert json.loads(str(row["selected_feed_ids"])) == [feed_a, feed_b]
        quantities = json.loads(str(row["daily_quantities"]))
        return dict(quantities["displayed"])

    temp = displayed("temperature")
    assert temp["high_c"] == tile.temp.high_c
    assert temp["low_c"] == tile.temp.low_c
    # Hand anchor (not derived from either pipeline): the clearing subset is
    # feed A alone, so high is A's own peak — never B's higher-but-partial
    # peak (35.0 without the subset rule, 30.0 blended at hour 5).
    assert temp["high_c"] == 20.0
    assert temp["low_c"] == 10.0
    assert temp["partial"] is False
    assert temp["low_confidence"] is True  # unscored ladder rung

    wind = displayed("wind")
    max_ms = wind["max_ms"]
    assert isinstance(max_ms, float)
    assert tile.wind.max_kmh == ms_to_kmh(max_ms)
    assert max_ms == 8.0  # A's peak; 14.0 without the subset, 11.5 blended

    precip = displayed("precip")
    assert precip["total_mm"] == tile.precip.total_mm
    chance = precip["chance"]
    assert isinstance(chance, float)
    assert tile.precip.chance_pct == round(chance * 100)
    assert chance == 0.25  # 6 wet hours of A's 24; B's all-wet share excluded
    assert precip["total_mm"] == pytest.approx(6 * threshold)


def test_record_outcomes_score_the_clearing_subset_and_keep_the_full_hourly() -> None:
    """§17 family 4 (W3), record side: the stored OUTCOMES describe the
    clearing subset — the same feed set the displayed value uses — while
    ``hourly_values`` deliberately stays the FULL-selection blend that
    reproduces the Forecast page's hourly drill-down.

    Fixture: feed A covers all 24 hours dry (0.0 mm, clears coverage);
    feed B covers hours 0-13 only (14 hours, selected but NOT clearing) at
    ``threshold + 1.0`` mm. Hand-computed from the code path:

    * outcomes blend = feed A alone -> 24 covered hours, zero wet slots,
      near-complete -> ``precip_occurrence`` value 0.0 (DRY, eligible) and
      ``precip_total`` 0.0;
    * ``hourly_values`` = both feeds -> 24 entries, hours 0-13 at
      ``(threshold + 1.0) / 2`` (wet) and hours 14-23 at 0.0.

    Kills the 0.11.0 implementation, which evaluated the outcomes over the
    full-selection blend and so reported ``precip_occurrence`` 1.0 (WET,
    14 wet slots) beside a displayed total of 0.0. Kills the opposite
    over-correction too — unifying ``hourly_values`` onto ``agg_ids``
    drops every wet hour from the drill-down series.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "record-w3-site")
    ensure_published_generation(conn, site_id)
    feed_a = asof_make_real_feed(conn, "model-a")
    feed_b = asof_make_real_feed(conn, "model-b")
    site = conn.execute(
        "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    assert site is not None
    threshold = float(site["rain_threshold_mm"])
    wet_value = threshold + 1.0

    _insert_var_day(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        variable="precip",
        local_date=_DAY,
        hours=range(24),
        value_fn=lambda _h: 0.0,
    )
    _insert_var_day(
        conn,
        site_id=site_id,
        feed_id=feed_b,
        variable="precip",
        local_date=_DAY,
        hours=range(14),
        value_fn=lambda _h: wet_value,
    )

    build_forecast_record(conn, site_id, _DAY.isoformat(), now=_t())
    row = _cell(conn, site_id, _DAY.isoformat(), "precip", 0)
    # Fixture validity: both feeds are selected, only A clears coverage.
    assert json.loads(str(row["selected_feed_ids"])) == [feed_a, feed_b]

    quantities = json.loads(str(row["daily_quantities"]))
    outcomes = {str(o["quantity"]): o for o in quantities["outcomes"]}
    occurrence = outcomes["precip_occurrence"]
    assert occurrence["value"] == 0.0
    assert occurrence["eligible"] is True
    assert occurrence["wet_hours"] == 0
    assert occurrence["covered_hours"] == 24
    total = outcomes["precip_total"]
    assert total["value"] == pytest.approx(0.0)
    assert total["eligible"] is True

    hourly = [(str(at), float(v)) for at, v in json.loads(str(row["hourly_values"]))]
    assert len(hourly) == 24
    # The drill-down series keeps feed B: hours 0-13 blend to the mean of
    # the two feeds, which is wet — exactly what the outcomes must ignore.
    assert [v for _at, v in hourly[:14]] == [pytest.approx(wet_value / 2)] * 14
    assert [v for _at, v in hourly[14:]] == [0.0] * 10
    assert wet_value / 2 >= threshold
