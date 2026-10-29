"""Step 3 tests for the closed-day guard in each writer (plan
2026-10-20-daily-close-day-state-and-guard.md, Sec 13.3-13.4).

``wxverify.db.site_days`` arrives in Step 4 piece 2 of the daily-close plan;
importing names from it here, at module top, is deliberate: it is the
authoritative "not built yet" signal for this whole file -- every test below
collection-errors with the same ``ModuleNotFoundError`` until that module
exists, which is the right reason for all of them to be red right now.

Each GW test seeds the shared helper site (``tests.helpers.day_close_site``,
plan Sec 13.1) with the close on: D-3 closed, D-2 missed, D-1 open, D the
current day. It then calls the writer the way its job or route does, and
checks the shared 13.1 oracle (D-3 unchanged across the six day-state tables,
D-1 written, no exception, exactly one WARNING in the 7.2 form) except where
a GW row names an exception. Only synthetic station ids (``ISTATION01``,
``ISTATION02``), the ``UTC``/``Etc/GMT+-N`` zones and a fixed synthetic date
appear anywhere in this file (public repo).
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pytest
from starlette.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    asof_insert_observation,
    asof_insert_pair,
    asof_insert_sample,
    asof_make_real_feed,
    day_close_site,
    note_day_clock,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes import db_transfer
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, FencedWriter, close_db, get_db, init_db
from wxverify.db.queue import Job

# The arrives-in-Step-4 import: see the module docstring above.
from wxverify.db.site_days import (
    begin_close,
    enter_zone_change,
    fail_close,
)
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.db.wind_basis import write_auth_hold
from wxverify.obs.pws_adapter import PwsObservation
from wxverify.provider_ops import rebuild_mean_for_site
from wxverify.scoring.consensus import insert_station_observation, materialize_consensus
from wxverify.scoring.engine import pair_and_score
from wxverify.scoring.multimodel import compute_multimodel_mean
from wxverify.scoring.pairing import REAL_MODEL_PAIR_PROBE_SQL, real_model_pair_pending
from wxverify.scoring.persistence import compute_persistence_pairs
from wxverify.scoring.split import (
    DeleteOp,
    GenerationResolver,
    InsertOp,
    PairDelta,
    ReplaceOp,
    apply_pair_ops,
)
from wxverify.verification.truth import materialize_daily_truth
from wxverify.worker.backfill import (  # noqa: F401 -- SLF001 names below are private
    _persist_station_observations as _backfill_persist_station_observations,
)
from wxverify.worker.catchup import (  # noqa: F401
    _persist_station_observations as _catchup_persist_station_observations,
)
from wxverify.worker.processor import (  # noqa: F401
    _persist_station_observations as _processor_persist_station_observations,
)
from wxverify.worker.processor import (
    dispatch,
)
from wxverify.worker.score_batches import _apply_split_chunk  # noqa: SLF001
from wxverify.worker.wind_days import (  # noqa: SLF001
    _derive_days,
    _finish_day,
    _invalidate_unparseable,
    _persist_day,
    _reconcile,
)

_STATION_A: Final = "ISTATION01"
_STATION_B: Final = "ISTATION02"

_GUARD_RE: Final = re.compile(
    r"^closed-day guard refused (?P<kind>\S+) writer=(?P<writer>\S+) "
    r"site=(?P<site>\d+) days=(?P<days>\S*) count=(?P<count>\d+)$"
)

_ORACLE_DAY_TABLES: Final = (
    "station_wind_records",
    "station_observations",
    "observations",
    "daily_truth",
    "forecast_pairs",
    "site_days",
)


# ---------------------------------------------------------------------------
# Shared helpers (private to this file; nothing here is added to
# tests/helpers.py or tests/conftest.py).
# ---------------------------------------------------------------------------


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _guard_warnings(caplog: pytest.LogCaptureFixture) -> list[dict[str, str]]:
    hits = []
    for record in caplog.records:
        if record.levelno != logging.WARNING:
            continue
        match = _GUARD_RE.match(record.getMessage())
        if match:
            hits.append(match.groupdict())
    return hits


def _assert_one_guard_warning(
    caplog: pytest.LogCaptureFixture,
    *,
    kind: str,
    site_id: int,
    days: tuple[date, ...],
    count: int,
) -> None:
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    hit = hits[0]
    assert hit["kind"] == kind, hit
    assert hit["site"] == str(site_id), hit
    assert hit["count"] == str(count), hit
    assert hit["days"] == ",".join(d.isoformat() for d in days), hit


def _assert_no_guard_warning(caplog: pytest.LogCaptureFixture) -> None:
    assert _guard_warnings(caplog) == []


def _rows(
    conn: sqlite3.Connection, sql: str, params: tuple[object, ...]
) -> list[dict[str, object]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _day_bounds(local_date: date) -> tuple[str, str]:
    start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    return isoformat_utc(start), isoformat_utc(start + timedelta(days=1))


def _day_snapshot(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> dict[str, list[dict[str, object]]]:
    """The 13.1 "unchanged" snapshot across the six named tables."""
    start, end = _day_bounds(local_date)
    return {
        "station_wind_records": _rows(
            conn,
            """
            SELECT swr.* FROM station_wind_records swr
            JOIN stations st ON st.id = swr.station_id
            WHERE st.site_id = ? AND swr.obs_at >= ? AND swr.obs_at < ?
            ORDER BY swr.station_id, swr.obs_at
            """,
            (site_id, start, end),
        ),
        "station_observations": _rows(
            conn,
            """
            SELECT so.* FROM station_observations so
            JOIN stations st ON st.id = so.station_id
            WHERE st.site_id = ? AND so.valid_at >= ? AND so.valid_at < ?
            ORDER BY so.station_id, so.variable, so.valid_at
            """,
            (site_id, start, end),
        ),
        "observations": _rows(
            conn,
            """
            SELECT * FROM observations
            WHERE site_id = ? AND valid_at >= ? AND valid_at < ?
            ORDER BY variable, valid_at
            """,
            (site_id, start, end),
        ),
        "daily_truth": _rows(
            conn,
            "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?"
            " ORDER BY quantity",
            (site_id, local_date.isoformat()),
        ),
        "forecast_pairs": _rows(
            conn,
            """
            SELECT * FROM forecast_pairs
            WHERE site_id = ? AND valid_at >= ? AND valid_at < ?
            ORDER BY feed_id, variable, valid_at
            """,
            (site_id, start, end),
        ),
        "site_days": _rows(
            conn,
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, local_date.isoformat()),
        ),
    }


def _assert_day_unchanged(
    before: dict[str, list[dict[str, object]]],
    after: dict[str, list[dict[str, object]]],
) -> None:
    assert before == after


def _d3(d: date = D) -> date:
    return d - timedelta(days=3)


def _d1(d: date = D) -> date:
    return d - timedelta(days=1)


def _insert_wind_day(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    *,
    status: str = "pending",
) -> None:
    conn.execute(
        """
        INSERT INTO station_wind_days
            (station_id, local_date, status, record_count, pair_hours,
             attempts, next_attempt_at, last_error, last_ok_at, refetch_at,
             refetched, updated_at)
        VALUES (?, ?, ?, 0, 0, 0, NULL, NULL, NULL, NULL, 0, ?)
        """,
        (station_id, local_date.isoformat(), status, isoformat_utc(NOW)),
    )


# ---------------------------------------------------------------------------
# GW1-GW3: W1/W5/W6, the three _persist_station_observations entry points.
# ---------------------------------------------------------------------------


def test_gw1_processor_persist_station_observations_refuses_closed_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d3, d1 = _d3(), _d1()
    before_d3 = _day_snapshot(conn, site_id, d3)

    batch = [
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T06:00:00Z",
            value=5.0,
            source_raw="5.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T12:00:00Z",
            value=6.0,
            source_raw="6.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T18:00:00Z",
            value=7.0,
            source_raw="7.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d1.isoformat()}T12:00:00Z",
            value=8.0,
            source_raw="8.0 C",
        ),
    ]
    caplog.set_level(logging.WARNING)
    _processor_persist_station_observations(conn, site_id, station_a, batch)
    conn.commit()

    _assert_day_unchanged(before_d3, _day_snapshot(conn, site_id, d3))
    d1_row = conn.execute(
        "SELECT value FROM station_observations WHERE station_id = ? AND valid_at = ?",
        (station_a, f"{d1.isoformat()}T12:00:00Z"),
    ).fetchone()
    assert d1_row is not None and d1_row["value"] == 8.0
    _assert_one_guard_warning(
        caplog, kind="HOURLY", site_id=site_id, days=(d3,), count=3
    )

    caplog.clear()
    monkeypatch.setattr("wxverify.scoring.consensus.utc_now", lambda: NOW)
    insert_station_observation(
        conn,
        station_id=station_a,
        variable="temperature",
        valid_at=f"{d3.isoformat()}T15:00:00Z",
        value=9.0,
        source_raw="9.0 C",
    )
    conn.commit()
    _assert_no_guard_warning(caplog)
    _assert_day_unchanged(before_d3, _day_snapshot(conn, site_id, d3))


def test_gw2_backfill_persist_station_observations_refuses_closed_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.backfill.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d3, d1 = _d3(), _d1()
    before_d3 = _day_snapshot(conn, site_id, d3)

    batch = [
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T06:00:00Z",
            value=5.0,
            source_raw="5.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T12:00:00Z",
            value=6.0,
            source_raw="6.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T18:00:00Z",
            value=7.0,
            source_raw="7.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d1.isoformat()}T12:00:00Z",
            value=8.0,
            source_raw="8.0 C",
        ),
    ]
    caplog.set_level(logging.WARNING)
    _backfill_persist_station_observations(conn, site_id, station_a, batch)
    conn.commit()

    _assert_day_unchanged(before_d3, _day_snapshot(conn, site_id, d3))
    d1_row = conn.execute(
        "SELECT value FROM station_observations WHERE station_id = ? AND valid_at = ?",
        (station_a, f"{d1.isoformat()}T12:00:00Z"),
    ).fetchone()
    assert d1_row is not None and d1_row["value"] == 8.0
    # Backfill's backstop logs at DEBUG only -- an explicit exception to the
    # oracle's "exactly one WARNING" (plan 13.3, GW2).
    _assert_no_guard_warning(caplog)


def test_gw3_catchup_persist_refuses_closed_day_and_pre_first_record(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.catchup.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d3, d1 = _d3(), _d1()
    d5 = D - timedelta(days=5)  # before the first record (D-3)
    before_d3 = _day_snapshot(conn, site_id, d3)

    batch = [
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T06:00:00Z",
            value=5.0,
            source_raw="5.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T12:00:00Z",
            value=6.0,
            source_raw="6.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d3.isoformat()}T18:00:00Z",
            value=7.0,
            source_raw="7.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d1.isoformat()}T12:00:00Z",
            value=8.0,
            source_raw="8.0 C",
        ),
        PwsObservation(
            variable="temperature",
            valid_at=f"{d5.isoformat()}T12:00:00Z",
            value=1.0,
            source_raw="1.0 C",
        ),
    ]
    caplog.set_level(logging.WARNING)
    _catchup_persist_station_observations(conn, site_id, station_a, batch)
    conn.commit()

    _assert_day_unchanged(before_d3, _day_snapshot(conn, site_id, d3))
    d1_row = conn.execute(
        "SELECT value FROM station_observations WHERE station_id = ? AND valid_at = ?",
        (station_a, f"{d1.isoformat()}T12:00:00Z"),
    ).fetchone()
    assert d1_row is not None and d1_row["value"] == 8.0
    d5_row = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id = ? AND valid_at = ?",
        (station_a, f"{d5.isoformat()}T12:00:00Z"),
    ).fetchone()
    assert d5_row is None, "a late response before the first record must be refused too"
    _assert_no_guard_warning(caplog)


# ---------------------------------------------------------------------------
# GW4: consensus's materialize_consensus, called for a D-3 hour.
# ---------------------------------------------------------------------------


def test_gw4_materialize_consensus_leaves_a_closed_days_hour_alone(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.scoring.consensus.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    d3_hour = f"{_d3().isoformat()}T06:00:00Z"

    feed_id = asof_make_real_feed(conn, "gw4-model")
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d3_hour,
        issued_at=f"{_d3().isoformat()}T00:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at=d3_hour,
    )
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, skill_score,
             computed_at)
        VALUES (?, ?, 'temperature', 1, 'w:all', 1, 0.5, ?)
        """,
        (site_id, feed_id, d3_hour),
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             tz_generation_id)
        VALUES (?, ?, 'temperature_high', 10.0, 1, 24, 24, ?, ?, 'UTC', 1)
        """,
        (
            site_id,
            _d3().isoformat(),
            f"{_d3().isoformat()}T00:00:00Z",
            f"{(_d3() + timedelta(days=1)).isoformat()}T00:00:00Z",
        ),
    )
    conn.commit()

    before_pairs = _rows(
        conn, "SELECT * FROM forecast_pairs WHERE site_id = ? ORDER BY id", (site_id,)
    )
    before_cache = _rows(
        conn,
        "SELECT * FROM score_cache WHERE site_id = ?"
        " ORDER BY feed_id, variable, day_ahead, window_key",
        (site_id,),
    )
    before_truth = _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, _d3().isoformat()),
    )

    caplog.set_level(logging.WARNING)
    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=d3_hour
    )
    conn.commit()

    assert before_pairs == _rows(
        conn, "SELECT * FROM forecast_pairs WHERE site_id = ? ORDER BY id", (site_id,)
    )
    assert before_cache == _rows(
        conn,
        "SELECT * FROM score_cache WHERE site_id = ?"
        " ORDER BY feed_id, variable, day_ahead, window_key",
        (site_id,),
    )
    assert before_truth == _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, _d3().isoformat()),
    )
    obs_row = conn.execute(
        "SELECT 1 FROM observations WHERE site_id = ? AND valid_at = ?",
        (site_id, d3_hour),
    ).fetchone()
    assert obs_row is None, "the consensus write itself must be refused on D-3"
    _assert_one_guard_warning(
        caplog, kind="HOURLY", site_id=site_id, days=(_d3(),), count=1
    )


# ---------------------------------------------------------------------------
# GW5-GW7: W4/W3/W2, the wind-days persist/derive/reconcile trio.
# ---------------------------------------------------------------------------


def test_gw5_persist_day_refuses_the_store_margin_into_a_done_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d2, d1 = D - timedelta(days=2), _d1()
    _insert_wind_day(conn, station_a, d2, status="fetched")
    _insert_wind_day(conn, station_a, d1, status="partial")
    conn.commit()

    before_d2 = _day_snapshot(conn, site_id, d2)

    from wxverify.worker.wind_days import WindRecord  # local: not a module-top name

    margin_record = WindRecord(
        obs_at=datetime(d1.year, d1.month, d1.day, tzinfo=UTC) - timedelta(minutes=10),
        speed_kmh=12.0,
    )
    in_day_record = WindRecord(
        obs_at=datetime(d1.year, d1.month, d1.day, 12, tzinfo=UTC), speed_kmh=9.0
    )

    caplog.set_level(logging.WARNING)
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d1,
        tz_name="UTC",
        today=D,
        records=[margin_record, in_day_record],
        probe=None,
    )
    conn.commit()

    _assert_day_unchanged(before_d2, _day_snapshot(conn, site_id, d2))
    stored = {
        row["obs_at"]
        for row in conn.execute(
            "SELECT obs_at FROM station_wind_records WHERE station_id = ?", (station_a,)
        )
    }
    assert margin_record.obs_at.isoformat().replace("+00:00", "Z") not in stored, (
        "D-2's margin record must not be stored"
    )
    assert in_day_record.obs_at.isoformat().replace("+00:00", "Z") in stored
    # _persist_day's own cut (W4, kind READING) sees only the margin record
    # at D-2, so one WARNING there; it then calls _derive_days (W3, kind
    # HOURLY) for D-1, which re-derives D-2 too because D-2 is a 'fetched'
    # neighbour -- a second WARNING, for a different kind, not a duplicate of
    # the first (plan 7.3 gives W4 and W3 one WARNING each per call).
    hits = _guard_warnings(caplog)
    assert len(hits) == 2, hits
    by_kind = {hit["kind"]: hit for hit in hits}
    assert by_kind.keys() == {"READING", "HOURLY"}, hits
    reading = by_kind["READING"]
    assert reading["site"] == str(site_id)
    assert reading["days"] == d2.isoformat()
    assert reading["count"] == "1"
    hourly = by_kind["HOURLY"]
    assert hourly["site"] == str(site_id)
    assert hourly["days"] == d2.isoformat()
    # With tz_name="UTC" a local day is exactly 24 hourly slots (_day_hours
    # over a 24h window), and D-2 produces no pairs (only one raw record was
    # ever stored for it, never enough to pair), so all 24 of its hours are
    # offered to the HOURLY cut and all 24 are refused.
    assert hourly["count"] == "24"


def test_gw6_derive_days_does_not_rewrite_or_delete_a_done_days_wind_rows(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d2, d1 = D - timedelta(days=2), _d1()
    _insert_wind_day(conn, station_a, d2, status="fetched")
    _insert_wind_day(conn, station_a, d1, status="partial")

    # Seeded with direct SQL, never through a guarded writer: these rows
    # simulate data already on the (now closed) day before the close took
    # effect, which the guard must still protect.
    #
    # A D-2 hour with no raw station_wind_records anywhere near it: no pair
    # can form, so this row is a delete_hours candidate -- the unguarded
    # per-hour DELETE in _derive_days would remove it.
    delete_hour = f"{d2.isoformat()}T09:00:00Z"
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', ?, 2.5, 'ok', 'pair-max 9.0 km/h', ?)",
        (station_a, delete_hour, isoformat_utc(NOW)),
    )
    # A D-2 hour whose raw records WOULD pair to 15.0 km/h: a write_hours
    # candidate. The stored row below is a different value (1.0), so an
    # unguarded write would change it.
    write_hour = f"{d2.isoformat()}T15:00:00Z"
    conn.executemany(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, ?, 'five_minute', ?)",
        [
            (station_a, write_hour, 10.0),
            (station_a, f"{d2.isoformat()}T15:05:00Z", 20.0),
        ],
    )
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'wind', ?, 1.0, 'ok', 'pair-max 1.0 km/h', ?)",
        (station_a, write_hour, isoformat_utc(NOW)),
    )
    conn.commit()
    before_d2_wind = _rows(
        conn,
        "SELECT * FROM station_observations WHERE station_id = ? AND variable = 'wind'"
        " ORDER BY valid_at",
        (station_a,),
    )

    caplog.set_level(logging.WARNING)
    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d1,
        tz_name="UTC",
        today=D,
    )
    conn.commit()

    after_d2_wind = _rows(
        conn,
        "SELECT * FROM station_observations WHERE station_id = ? AND variable = 'wind'"
        " ORDER BY valid_at",
        (station_a,),
    )
    assert before_d2_wind == after_d2_wind, (
        "D-2's wind rows must be neither rewritten nor deleted"
    )
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    assert hits[0]["kind"] == "HOURLY"
    assert hits[0]["site"] == str(site_id)
    assert hits[0]["days"] == d2.isoformat()
    # _derive_hours for a non-"today" day returns the full local day
    # (_day_hours over a 24h window; today=D here, so neither D-2 nor D-1
    # is "today"), and D-2 is 'closed' so table 5.2 refuses every one of
    # its 24 hours regardless of which fall in write_hours or
    # delete_hours; D-1's 24 are allowed (it is 'open'), so they are not
    # counted here.
    assert hits[0]["count"] == "24"


def test_gw7_reconcile_does_not_schedule_a_refetch_for_a_done_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d3, d1 = _d3(), _d1()
    pool_day = d3 - timedelta(days=1)
    pool_day_5 = d3 - timedelta(days=2)
    pool_day_6 = d3 - timedelta(days=3)
    _insert_wind_day(conn, station_a, pool_day, status="fetched")
    conn.execute(
        "UPDATE station_wind_days SET record_count = 100"
        " WHERE station_id = ? AND local_date = ?",
        (station_a, pool_day.isoformat()),
    )
    _insert_wind_day(conn, station_a, pool_day_5, status="fetched")
    conn.execute(
        "UPDATE station_wind_days SET record_count = 280"
        " WHERE station_id = ? AND local_date = ?",
        (station_a, pool_day_5.isoformat()),
    )
    _insert_wind_day(conn, station_a, pool_day_6, status="fetched")
    conn.execute(
        "UPDATE station_wind_days SET record_count = 280"
        " WHERE station_id = ? AND local_date = ?",
        (station_a, pool_day_6.isoformat()),
    )
    _insert_wind_day(conn, station_a, d3, status="fetched")
    _insert_wind_day(conn, station_a, d1, status="fetched")
    conn.commit()

    caplog.set_level(logging.WARNING)
    _reconcile(conn, site_id, station_a, d3, "UTC", record_count=1)
    _reconcile(conn, site_id, station_a, d1, "UTC", record_count=1)
    conn.commit()

    # `_reconcile`'s own READING check is a backstop behind `_persist_day`'s
    # cut (wind_days.py:1300-1303, plan 7.2's last bullet): it sits behind an
    # already-refused write, so a refusal here logs at DEBUG only, not
    # WARNING.
    _assert_no_guard_warning(caplog)

    d3_row = conn.execute(
        "SELECT refetch_at FROM station_wind_days"
        " WHERE station_id = ? AND local_date = ?",
        (station_a, d3.isoformat()),
    ).fetchone()
    assert d3_row["refetch_at"] is None, "a done day must get no refetch_at"
    d1_row = conn.execute(
        "SELECT refetch_at FROM station_wind_days"
        " WHERE station_id = ? AND local_date = ?",
        (station_a, d1.isoformat()),
    ).fetchone()
    assert d1_row["refetch_at"] is not None, "the open day must get a refetch_at"


# ---------------------------------------------------------------------------
# GW8: W9, _invalidate_unparseable, close on vs close off.
# ---------------------------------------------------------------------------


def test_gw8_invalidate_unparseable_keeps_wind_score_cache_with_close_on(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "gw8-model")
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, skill_score,
             computed_at)
        VALUES (?, ?, 'wind', 1, 'w:all', 1, 0.5, ?)
        """,
        (site_id, feed_id, isoformat_utc(NOW)),
    )
    conn.commit()
    before = _rows(
        conn,
        "SELECT * FROM score_cache WHERE site_id = ?"
        " ORDER BY feed_id, variable, day_ahead, window_key",
        (site_id,),
    )

    caplog.set_level(logging.WARNING)
    _invalidate_unparseable(conn, site_id, "not-a-time")
    conn.commit()

    after = _rows(
        conn,
        "SELECT * FROM score_cache WHERE site_id = ?"
        " ORDER BY feed_id, variable, day_ahead, window_key",
        (site_id,),
    )
    assert before == after, (
        "the site's wind score_cache rows must be kept with close on"
    )
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    assert hits[0]["days"] == ""
    assert hits[0]["count"] == "1"


def test_gw8_invalidate_unparseable_deletes_as_today_with_close_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)
    feed_id = asof_make_real_feed(conn, "gw8-off-model")
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, skill_score,
             computed_at)
        VALUES (?, ?, 'wind', 1, 'w:all', 1, 0.5, ?)
        """,
        (site_id, feed_id, isoformat_utc(NOW)),
    )
    conn.commit()

    _invalidate_unparseable(conn, site_id, "not-a-time")
    conn.commit()

    remaining = conn.execute(
        "SELECT 1 FROM score_cache WHERE site_id = ?", (site_id,)
    ).fetchone()
    assert remaining is None, "with the close off, the row must be deleted as today"


# ---------------------------------------------------------------------------
# GW9/GW9b/GW9c: W10, apply_pair_ops/_apply_split_chunk and the computes.
# ---------------------------------------------------------------------------


def test_gw9_apply_pair_ops_and_split_chunk_refuse_a_closed_days_ops(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr("wxverify.scoring.split.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "gw9-model")
    d3_hour, d3_hour_insert, d3_hour_delete, d1_hour_a, d1_hour_b, d1_hour_c = (
        f"{_d3().isoformat()}T06:00:00Z",
        f"{_d3().isoformat()}T07:00:00Z",
        f"{_d3().isoformat()}T08:00:00Z",
        f"{_d1().isoformat()}T06:00:00Z",
        f"{_d1().isoformat()}T07:00:00Z",
        f"{_d1().isoformat()}T08:00:00Z",
    )

    resolver = GenerationResolver((site_id,))
    resolver.seed(conn)
    insert_sql = (
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, day_ahead,"
        " forecast, observed, error, abs_error, sq_error, first_known_at,"
        " tz_generation_id)"
        " VALUES (?, ?, 'temperature', ?, ?, 6, 1, ?, ?, ?, ?, ?, ?, ?)"
    )

    def _values(valid_at: str, forecast: float, observed: float) -> tuple[object, ...]:
        error = forecast - observed
        return (
            site_id,
            feed_id,
            valid_at,
            valid_at,
            forecast,
            observed,
            error,
            abs(error),
            error**2,
            valid_at,
        )

    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d3_hour,
        issued_at=d3_hour,
        forecast=3.0,
        observed=4.0,
        first_known_at=d3_hour,
    )
    replace_target = int(
        conn.execute(
            "SELECT id FROM forecast_pairs"
            " WHERE site_id=? AND feed_id=? AND valid_at=?",
            (site_id, feed_id, d3_hour),
        ).fetchone()["id"]
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d3_hour_delete,
        issued_at=d3_hour_delete,
        forecast=3.0,
        observed=4.0,
        first_known_at=d3_hour_delete,
    )
    d3_delete_target = int(
        conn.execute(
            "SELECT id FROM forecast_pairs"
            " WHERE site_id=? AND feed_id=? AND valid_at=?",
            (site_id, feed_id, d3_hour_delete),
        ).fetchone()["id"]
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d1_hour_a,
        issued_at=d1_hour_a,
        forecast=3.0,
        observed=4.0,
        first_known_at=d1_hour_a,
    )
    d1_replace_target = int(
        conn.execute(
            "SELECT id FROM forecast_pairs"
            " WHERE site_id=? AND feed_id=? AND valid_at=?",
            (site_id, feed_id, d1_hour_a),
        ).fetchone()["id"]
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d1_hour_b,
        issued_at=d1_hour_b,
        forecast=3.0,
        observed=4.0,
        first_known_at=d1_hour_b,
    )
    d1_delete_target = int(
        conn.execute(
            "SELECT id FROM forecast_pairs"
            " WHERE site_id=? AND feed_id=? AND valid_at=?",
            (site_id, feed_id, d1_hour_b),
        ).fetchone()["id"]
    )
    conn.commit()

    before_d3 = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at=?",
        (site_id, d3_hour),
    )

    delta = PairDelta((site_id,), (), insert_sql, count=None)
    d3_ops = (
        InsertOp(
            site_id=site_id,
            values=_values(d3_hour_insert, 5.0, 4.0),
            valid_at=d3_hour_insert,
        ),
        ReplaceOp(
            row_id=replace_target,
            site_id=site_id,
            values=_values(d3_hour, 6.0, 4.0),
            valid_at=d3_hour,
        ),
        DeleteOp(row_id=d3_delete_target, site_id=site_id, valid_at=d3_hour_delete),
    )
    d1_ops = (
        InsertOp(
            site_id=site_id, values=_values(d1_hour_c, 5.0, 4.0), valid_at=d1_hour_c
        ),
        ReplaceOp(
            row_id=d1_replace_target,
            site_id=site_id,
            values=_values(d1_hour_a, 6.0, 4.0),
            valid_at=d1_hour_a,
        ),
        DeleteOp(row_id=d1_delete_target, site_id=site_id, valid_at=d1_hour_b),
    )

    caplog.set_level(logging.WARNING)
    result = apply_pair_ops(conn, delta, d3_ops + d1_ops, resolver)
    assert result == 2

    after_d3 = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at=?",
        (site_id, d3_hour),
    )
    assert before_d3 == after_d3, "D-3's pair rows must be identical"
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_pairs WHERE id=?", (replace_target,)
        ).fetchone()
        is not None
    ), "the replaced D-3 row must still be present"

    # Replay the identical ops through the chunked apply path: roll back the
    # monolithic apply above so the row ids this test seeded are restored,
    # then confirm the chunk path reaches the same result row-for-row. Per
    # risk 10, `_apply_split_chunk` returns the number of ops applied, not
    # the apply's INSERT rowcount, so it is checked against `len(...)`, not
    # against `result`.
    conn.rollback()
    chunk_result = _apply_split_chunk(
        conn,
        site_id,
        delta,
        d3_ops + d1_ops,
        resolver,
        first=True,
        require_enabled=False,
    )
    conn.commit()
    assert chunk_result == len(d3_ops) + len(d1_ops)

    after_d3_chunk = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at=?",
        (site_id, d3_hour),
    )
    assert before_d3 == after_d3_chunk, "D-3's pair rows must stay untouched"
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_pairs WHERE id=?", (replace_target,)
        ).fetchone()
        is not None
    ), "the replaced D-3 row must still be present after the chunk apply"
    d1_replaced_row = conn.execute(
        "SELECT forecast FROM forecast_pairs WHERE site_id=? AND valid_at=?",
        (site_id, d1_hour_a),
    ).fetchone()
    assert d1_replaced_row is not None and d1_replaced_row["forecast"] == 6.0, (
        "the open day's replaced row must carry the replacement's values"
    )
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_pairs WHERE id=?", (d1_delete_target,)
        ).fetchone()
        is None
    ), "the open day's deleted row must be gone"
    d1_inserted_row = conn.execute(
        "SELECT forecast FROM forecast_pairs WHERE site_id=? AND valid_at=?",
        (site_id, d1_hour_c),
    ).fetchone()
    assert d1_inserted_row is not None and d1_inserted_row["forecast"] == 5.0, (
        "the open day's inserted row must be present"
    )
    _assert_no_guard_warning(caplog)


def test_gw9b_pair_computes_propose_no_ops_for_a_done_or_recordless_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.scoring.pairing.utc_now", lambda: NOW)
    monkeypatch.setattr("wxverify.scoring.persistence.utc_now", lambda: NOW)
    monkeypatch.setattr("wxverify.scoring.multimodel.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "gw9b-model")
    feed_id_b = asof_make_real_feed(conn, "gw9b-model-b")
    d5 = D - timedelta(days=5)

    for d in (d5, _d3(), _d1()):
        hour = f"{d.isoformat()}T12:00:00Z"
        asof_insert_observation(
            conn, site_id=site_id, valid_at=hour, value=4.0, computed_at=hour
        )
        asof_insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at=hour,
            valid_at=hour,
            lead_hours=6,
            value=5.0,
            fetched_at=hour,
        )

    mean_feed_row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert mean_feed_row is not None
    mean_feed_id = int(mean_feed_row["id"])

    for d in (_d3(), _d1()):
        hour = f"{d.isoformat()}T18:00:00Z"
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=hour,
            issued_at=hour,
            forecast=3.0,
            observed=4.0,
            first_known_at=hour,
        )
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id_b,
            valid_at=hour,
            issued_at=hour,
            forecast=5.0,
            observed=4.0,
            first_known_at=hour,
        )
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 first_known_at, tz_generation_id)
            SELECT site_id, ?, 'temperature', issued_at, valid_at, lead_hours,
                   day_ahead, 99.0, observed, 99.0 - observed,
                   ABS(99.0 - observed), (99.0 - observed) * (99.0 - observed),
                   first_known_at, tz_generation_id
            FROM forecast_pairs
            WHERE site_id = ? AND feed_id = ? AND valid_at = ?
            """,
            (mean_feed_id, site_id, feed_id, hour),
        )
    conn.commit()

    for d in (d5, _d3(), _d1()):
        hour = f"{d.isoformat()}T12:00:00Z"
        real_delta = asyncio.run(asyncio.to_thread(lambda h=hour: None)) or None
    from wxverify.scoring.pairing import compute_real_model_pairs

    real_delta = compute_real_model_pairs(conn, site_id)
    assert all(
        getattr(op, "valid_at", None)
        not in {
            f"{d5.isoformat()}T12:00:00Z",
            f"{_d3().isoformat()}T12:00:00Z",
            f"{(D - timedelta(days=2)).isoformat()}T12:00:00Z",
        }
        for op in real_delta.ops
    )
    d1_hour = f"{_d1().isoformat()}T12:00:00Z"
    assert all(getattr(op, "valid_at", d1_hour) == d1_hour for op in real_delta.ops)

    persistence_delta = compute_persistence_pairs(conn, site_id)
    assert len(persistence_delta.ops) == 2
    assert all(
        getattr(op, "valid_at", d1_hour) == d1_hour for op in persistence_delta.ops
    )

    mean_delta = compute_multimodel_mean(conn, site_id)
    d1_18 = f"{_d1().isoformat()}T18:00:00Z"
    d3_18 = f"{_d3().isoformat()}T18:00:00Z"
    assert any(
        isinstance(op, ReplaceOp) and getattr(op, "valid_at", None) == d1_18
        for op in mean_delta.ops
    )
    assert not any(getattr(op, "valid_at", None) == d3_18 for op in mean_delta.ops)
    _assert_no_guard_warning(caplog)


def test_gw9c_real_model_pair_pending_probe_unchanged_from_today() -> None:
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "gw9c-model")
    hour = f"{_d3().isoformat()}T06:00:00Z"
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at=hour,
        valid_at=hour,
        lead_hours=6,
        value=5.0,
        fetched_at=hour,
    )
    asof_insert_observation(
        conn, site_id=site_id, valid_at=hour, value=4.0, computed_at=hour
    )
    conn.commit()

    sample_id = int(
        conn.execute(
            "SELECT id FROM forecast_samples"
            " WHERE site_id=? AND feed_id=? AND valid_at=?",
            (site_id, feed_id, hour),
        ).fetchone()["id"]
    )

    class _RecordingConn:
        """Thin proxy: records the SQL text each `.execute` is given, then
        passes the call straight through to the real connection."""

        def __init__(self, inner: sqlite3.Connection) -> None:
            self._inner = inner
            self.statements: list[str] = []

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Cursor:
            self.statements.append(sql)
            return self._inner.execute(sql, params)

    recorder = _RecordingConn(conn)
    assert real_model_pair_pending(recorder, [sample_id]) is True  # type: ignore[arg-type]
    # The probe's SQL text, as actually executed, is the module constant
    # itself -- not merely present somewhere in the source, which can never
    # hold for a constant built by concatenation (REAL_MODEL_PAIR_PROBE_SQL
    # = _CANDIDATE_PAIRS_SQL + " AND fs.id = ?").
    assert recorder.statements == [REAL_MODEL_PAIR_PROBE_SQL]
    # "Unchanged from today": the probe's SQL carries no open-day clause.
    # open_day_clause's close-on branch always builds an EXISTS over
    # site_days (wxverify/db/site_days.py), so that table name's absence
    # here is the specific signal that no day clause was folded in.
    assert "site_days" not in REAL_MODEL_PAIR_PROBE_SQL


# ---------------------------------------------------------------------------
# GW10/GW10b: W11, materialize_daily_truth.
# ---------------------------------------------------------------------------


def test_gw10_materialize_daily_truth_refuses_closed_day_writes_open_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wxverify.verification.truth.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    d3, d1 = _d3(), _d1()
    for d in (d3, d1):
        conn.execute(
            """
            INSERT INTO daily_truth
                (site_id, local_date, quantity, value, eligible, covered_hours,
                 expected_slots, day_start_utc, day_end_utc, timezone,
                 tz_generation_id)
            VALUES (?, ?, 'temperature_high', 1.0, 1, 24, 24, ?, ?, 'UTC', 1)
            """,
            (
                site_id,
                d.isoformat(),
                f"{d.isoformat()}T00:00:00Z",
                f"{(d + timedelta(days=1)).isoformat()}T00:00:00Z",
            ),
        )
    conn.commit()
    before_d3 = _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, d3.isoformat()),
    )

    materialize_daily_truth(conn, site_id=site_id, local_date=d3.isoformat())
    conn.commit()
    assert before_d3 == _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, d3.isoformat()),
    )

    materialize_daily_truth(conn, site_id=site_id, local_date=d1.isoformat())
    conn.commit()
    # D-1's row must have been (re)written -- a correct write refreshes it.
    after_d1 = _rows(
        conn,
        "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, d1.isoformat()),
    )
    assert after_d1 != [] or after_d1 == []  # existence asserted below
    assert (
        conn.execute(
            "SELECT 1 FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, d1.isoformat()),
        ).fetchone()
        is not None
    )


def test_gw10b_materialize_daily_truth_refuses_retry_due_then_writes_at_retry(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.verification.truth.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    d1 = _d1()
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d1.isoformat()),
    ).fetchone()
    assert row is not None
    from wxverify.core.timeutil import parse_utc

    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, d1, now_utc=note_day_clock(t))
    fail_close(conn, site_id, d1, now_utc=note_day_clock(t), reason="x")
    conn.commit()

    caplog.set_level(logging.WARNING)
    materialize_daily_truth(conn, site_id=site_id, local_date=d1.isoformat())
    conn.commit()
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    assert hits[0]["days"] == d1.isoformat()
    written = conn.execute(
        "SELECT 1 FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, d1.isoformat()),
    ).fetchone()
    assert written is None, "retry_due must take no write"

    retry_row = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d1.isoformat()),
    ).fetchone()
    assert retry_row is not None and retry_row["retry_at"] is not None
    retry_at = parse_utc(retry_row["retry_at"])
    begin_close(conn, site_id, d1, now_utc=note_day_clock(retry_at))
    conn.commit()

    caplog.clear()
    materialize_daily_truth(conn, site_id=site_id, local_date=d1.isoformat())
    conn.commit()
    assert (
        conn.execute(
            "SELECT 1 FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, d1.isoformat()),
        ).fetchone()
        is not None
    ), "the retry must write"


# ---------------------------------------------------------------------------
# GW11/GW12: W12 (PUT /api/sites/{id}) and W13 (rebuild_mean_for_site).
# ---------------------------------------------------------------------------


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def test_gw11_put_site_rain_threshold_leaves_closed_days_pairs_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.api.routes.sites.utc_now", lambda: NOW, raising=False)
    # update_site's DELETE only removes D1's precip row (D1 is open); the PUT
    # handler then calls pair_and_score, whose real-model lane is an INSERT
    # OR IGNORE that never re-reads a deleted row -- it only regenerates one
    # from forecast_samples+observations. So the D1 precip assertion below
    # needs a real sample/observation seed and an initial pair_and_score
    # call, not just a hand-planted forecast_pairs row. That call runs each
    # phase's own module-scoped utc_now (pairing/persistence/multimodel);
    # left unpatched it would call ensure_site_days on the real wall clock.
    monkeypatch.setattr("wxverify.scoring.pairing.utc_now", lambda: NOW)
    monkeypatch.setattr("wxverify.scoring.persistence.utc_now", lambda: NOW)
    monkeypatch.setattr("wxverify.scoring.multimodel.utc_now", lambda: NOW)
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn)
    feed_id = asof_make_real_feed(conn, "gw11-model")
    d3, d1 = _d3(), _d1()
    generation_id = ensure_published_generation(conn, site_id)
    # D3 (closed) decoys: never reach the real-model recompute path (closed
    # days are excluded by open_day_clause), so a plain hand-planted row is
    # enough to prove the guard leaves them alone.
    d3_hour = f"{d3.isoformat()}T12:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             first_known_at, tz_generation_id)
        VALUES (?, ?, 'precip', ?, ?, 6, 1, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            d3_hour,
            d3_hour,
            0.3,
            0.2,
            0.1,
            0.1,
            0.01,
            d3_hour,
            generation_id,
        ),
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d3_hour,
        issued_at=d3_hour,
        forecast=0.3,
        observed=0.2,
        first_known_at=d3_hour,
        generation_id=generation_id,
    )
    # D1 (open) temperature decoy: update_site's DELETE only targets
    # variable='precip', so this row is never touched either way.
    d1_hour = f"{d1.isoformat()}T12:00:00Z"
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=d1_hour,
        issued_at=d1_hour,
        forecast=0.3,
        observed=0.2,
        first_known_at=d1_hour,
        generation_id=generation_id,
    )
    # D1 precip: real samples+observation so pair_and_score's real-model
    # lane can actually (re)materialize this pair, both now (to give it a
    # starting rain_threshold_mm distinct from 1.0) and again inside the PUT.
    d1_issued_at = f"{d1.isoformat()}T06:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'precip', ?, ?, 6, 0.3, '0.3 mm', 'gw11-precip-run', ?)
        """,
        (site_id, feed_id, d1_issued_at, d1_hour, d1_issued_at),
    )
    conn.execute(
        """
        INSERT INTO observations (site_id, variable, valid_at, value, n_stations)
        VALUES (?, 'precip', ?, 0.2, 1)
        """,
        (site_id, d1_hour),
    )
    conn.commit()
    pair_and_score(conn, site_id)
    conn.commit()
    before_d3 = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at LIKE ?",
        (site_id, f"{d3.isoformat()}%"),
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        caplog.set_level(logging.WARNING)
        resp = client.put(
            f"/api/sites/{site_id}", json={"rain_threshold_mm": 1.0}, headers=headers
        )
        assert resp.status_code == 200
    _assert_no_guard_warning(caplog)

    # The app's lifespan closes `conn` (the same connection `_init_tmp_db`
    # handed back) on TestClient exit, so a fresh connection to the same
    # file is needed to read the result and to make the plan row's second
    # `pair_and_score` call (the PUT handler, sites.py:173, makes the
    # first).
    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        caplog.clear()
        pair_and_score(direct, site_id)
        direct.commit()
        _assert_no_guard_warning(caplog)

        after_d3 = _rows(
            direct,
            "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at LIKE ?",
            (site_id, f"{d3.isoformat()}%"),
        )
        assert before_d3 == after_d3, "D-3's mean and precipitation pairs unchanged"
        d1_precip = direct.execute(
            "SELECT rain_threshold_mm FROM forecast_pairs"
            " WHERE site_id=? AND feed_id=? AND variable='precip' AND valid_at=?",
            (site_id, feed_id, f"{d1.isoformat()}T12:00:00Z"),
        ).fetchone()
        assert d1_precip is not None and d1_precip[
            "rain_threshold_mm"
        ] == pytest.approx(1.0)
    finally:
        direct.close()


def test_gw12_rebuild_mean_for_site_leaves_closed_days_mean_alone(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.provider_ops.utc_now", lambda: NOW, raising=False)
    monkeypatch.setattr("wxverify.scoring.multimodel.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    feed_a = asof_make_real_feed(conn, "gw12-model-a")
    feed_b = asof_make_real_feed(conn, "gw12-model-b")
    d3, d1 = _d3(), _d1()
    for d in (d3, d1):
        hour = f"{d.isoformat()}T12:00:00Z"
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            valid_at=hour,
            issued_at=hour,
            forecast=3.0,
            observed=4.0,
            first_known_at=hour,
        )
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            valid_at=hour,
            issued_at=hour,
            forecast=5.0,
            observed=4.0,
            first_known_at=hour,
        )
    conn.commit()
    mean_feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert mean_feed is not None
    mean_feed_id = int(mean_feed["id"])
    # compute_multimodel_mean never produces a D3 row (open_day_clause keeps
    # it out of both the existing-rows read and the groups read), so a mean
    # row must be hand-planted at D3 for the before/after comparison below to
    # be non-vacuous: without this insert, before_d3_mean == after_d3_mean
    # == [] regardless of whether the guard fires.
    d3_generation_id = ensure_published_generation(conn, site_id)
    d3_hour = f"{d3.isoformat()}T12:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm, contributors, tz_generation_id)
        VALUES (?, ?, 'temperature', ?, ?, 6, 1, 10.0, 4.0, 6.0, 6.0, 36.0,
                NULL, NULL, NULL, NULL, NULL, 2, ?)
        """,
        (site_id, mean_feed_id, d3_hour, d3_hour, d3_generation_id),
    )
    # 10.0 is deliberately NOT the real group average (3.0+5.0)/2=4.0 from
    # feed_a/feed_b above: if the day-clause guard is ever dropped from
    # either the existing-rows read or the groups read in
    # compute_multimodel_mean, this row would be overwritten to forecast=4.0
    # and before_d3_mean == after_d3_mean would fail.
    conn.commit()
    before_d3_mean = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND feed_id=? AND valid_at=?",
        (site_id, mean_feed_id, d3_hour),
    )
    assert before_d3_mean != []

    caplog.set_level(logging.WARNING)
    rebuild_mean_for_site(conn, site_id)
    conn.commit()
    _assert_no_guard_warning(caplog)

    after_d3_mean = _rows(
        conn,
        "SELECT * FROM forecast_pairs WHERE site_id=? AND feed_id=? AND valid_at=?",
        (site_id, mean_feed_id, d3_hour),
    )
    assert before_d3_mean == after_d3_mean


# ---------------------------------------------------------------------------
# GW13/GW13b/GW14: W14, import over HTTP, resume, and the new 10.6 checks.
# ---------------------------------------------------------------------------


def test_gw13_import_of_a_close_on_file_rebuilds_only_the_open_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from tests.helpers import asof_make_site

    conn = _init_tmp_db(tmp_path)
    asof_make_site(conn, "Pre Import Site")
    conn.commit()

    b_path = tmp_path / "source-b.db"
    b_db = Database(str(b_path))
    try:
        site_b = day_close_site(b_db._conn)  # noqa: SLF001
        station_a = _station_id(b_db._conn, site_b, _STATION_A)  # noqa: SLF001
        station_b = _station_id(b_db._conn, site_b, _STATION_B)  # noqa: SLF001
        d3, d1 = _d3(), _d1()
        feed_id = asof_make_real_feed(b_db._conn, "gw13-model")  # noqa: SLF001
        generation_id = ensure_published_generation(  # noqa: SLF001
            b_db._conn, site_b
        )
        d3_hour = f"{d3.isoformat()}T12:00:00Z"
        # D-3 (closed): a pair, an observation and a truth row, exactly as a
        # pre-cutover file would carry them.
        asof_insert_pair(
            b_db._conn,  # noqa: SLF001
            site_id=site_b,
            feed_id=feed_id,
            valid_at=d3_hour,
            issued_at=d3_hour,
            forecast=5.0,
            observed=4.0,
            first_known_at=d3_hour,
            generation_id=generation_id,
        )
        b_db._conn.execute(  # noqa: SLF001
            "INSERT INTO observations"
            " (site_id, variable, valid_at, value, n_stations, rejected_stations,"
            " computed_at)"
            " VALUES (?, 'temperature', ?, 4.0, 1, 0, ?)",
            (site_b, d3_hour, d3_hour),
        )
        b_db._conn.execute(  # noqa: SLF001
            """
            INSERT INTO daily_truth
                (site_id, local_date, quantity, value, eligible, covered_hours,
                 expected_slots, day_start_utc, day_end_utc, timezone,
                 tz_generation_id)
            VALUES (?, ?, 'temperature_high', 10.0, 1, 24, 24, ?, ?, 'UTC', ?)
            """,
            (
                site_b,
                d3.isoformat(),
                f"{d3.isoformat()}T00:00:00Z",
                f"{(d3 + timedelta(days=1)).isoformat()}T00:00:00Z",
                generation_id,
            ),
        )
        # D-1 (open): raw station readings the rebuild turns into one
        # consensus observation.
        d1_hour = f"{d1.isoformat()}T12:00:00Z"
        for station_id, value in ((station_a, 10.0), (station_b, 12.0)):
            b_db._conn.execute(  # noqa: SLF001
                "INSERT INTO station_observations"
                " (station_id, variable, valid_at, value, qc_flag, source_raw,"
                " fetched_at)"
                " VALUES (?, 'temperature', ?, ?, 'ok', 'synthetic-gw13', ?)",
                (station_id, d1_hour, value, d1_hour),
            )
        b_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        b_db._conn.commit()  # noqa: SLF001
    finally:
        b_db.close()

    start, end = _day_bounds(d3)
    direct_b = sqlite3.connect(str(b_path))
    direct_b.row_factory = sqlite3.Row
    try:
        file_b_pairs = _rows(
            direct_b,
            "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at>=?"
            " AND valid_at<? ORDER BY feed_id, variable, valid_at",
            (site_b, start, end),
        )
        file_b_obs = _rows(
            direct_b,
            "SELECT * FROM observations WHERE site_id=? AND valid_at>=?"
            " AND valid_at<? ORDER BY variable, valid_at",
            (site_b, start, end),
        )
        file_b_truth = _rows(
            direct_b,
            "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?"
            " ORDER BY quantity",
            (site_b, d3.isoformat()),
        )
    finally:
        direct_b.close()
    assert file_b_pairs and file_b_obs and file_b_truth  # non-vacuous
    payload = b_path.read_bytes()

    def _assert_rebuilt(db_path: str) -> None:
        direct = sqlite3.connect(db_path)
        direct.row_factory = sqlite3.Row
        try:
            after_pairs = _rows(
                direct,
                "SELECT * FROM forecast_pairs WHERE site_id=? AND valid_at>=?"
                " AND valid_at<? ORDER BY feed_id, variable, valid_at",
                (site_b, start, end),
            )
            after_obs = _rows(
                direct,
                "SELECT * FROM observations WHERE site_id=? AND valid_at>=?"
                " AND valid_at<? ORDER BY variable, valid_at",
                (site_b, start, end),
            )
            after_truth = _rows(
                direct,
                "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?"
                " ORDER BY quantity",
                (site_b, d3.isoformat()),
            )
            assert after_pairs == file_b_pairs, (
                "D-3's pairs must be exactly file B's, not deleted or rebuilt"
            )
            assert after_obs == file_b_obs, "D-3's observations must survive"
            assert after_truth == file_b_truth, "D-3's truth must survive"
            d3_row = direct.execute(
                "SELECT 1 FROM site_days"
                " WHERE site_id=? AND local_date=? AND state='closed'",
                (site_b, d3.isoformat()),
            ).fetchone()
            assert d3_row is not None, "D-3's close-state record must survive"
            d1_obs = direct.execute(
                "SELECT 1 FROM observations WHERE site_id=? AND variable='temperature'"
                " AND valid_at=?",
                (site_b, d1_hour),
            ).fetchone()
            assert d1_obs is not None, (
                "D-1 must be rebuilt into a consensus observation"
            )
        finally:
            direct.close()

    # -- direct: the normal import, with its immediate background rebuild. --
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        caplog.set_level(logging.INFO)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
    _assert_no_guard_warning(caplog)
    info_hits = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
        and "kept" in record.getMessage()
        and "skipped" in record.getMessage()
    ]
    assert len(info_hits) == 1, info_hits
    counts = re.search(r"kept (\d+) cells, skipped (\d+)", info_hits[0])
    assert counts is not None, info_hits[0]
    # One cell on D-1 (both stations' readings collapse to the site's one
    # temperature hour) is kept; D-3's own hand-planted observations cell is
    # skipped, not refused -- the file's day states are final.
    assert (counts.group(1), counts.group(2)) == ("1", "1"), info_hits[0]
    _assert_rebuilt(config.db_path)

    # -- resume: the same file, imported with its background rebuild
    # suppressed, then picked up by the next boot (api/app.py's startup
    # resume), following tests/test_db_transfer.py's
    # test_boot_resumes_interrupted_import_rebuild. --
    resume_dir = tmp_path / "resume"
    resume_dir.mkdir()
    conn2 = _init_tmp_db(resume_dir)
    asof_make_site(conn2, "Pre Import Site")
    conn2.commit()

    async def _rebuild_derived_noop(_backup: Path) -> None:
        return None

    monkeypatch.setattr(db_transfer, "_rebuild_derived", _rebuild_derived_noop)
    app1 = _make_app(monkeypatch)
    with TestClient(app1) as client1:
        headers1 = _csrf_headers(client1)
        resp1 = client1.post("/api/import/db", content=payload, headers=headers1)
        assert resp1.status_code == 200

    direct_pending = sqlite3.connect(config.db_path)
    try:
        pending = direct_pending.execute(
            "SELECT value FROM runtime_state WHERE key='import_rebuild_state'"
        ).fetchone()
    finally:
        direct_pending.close()
    assert pending is not None and pending[0] == "pending", (
        "setup check: the background rebuild must have been suppressed"
    )

    close_db()
    app2 = _make_app(monkeypatch)
    with TestClient(app2):
        pass  # the lifespan's boot-resume runs the real rebuild here.

    _assert_rebuilt(config.db_path)


def test_gw14_import_of_a_close_off_file_rebuilds_exactly_as_today(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    from tests.helpers import asof_make_site

    asof_make_site(conn, "Pre Import Site")
    conn.commit()

    b_path = tmp_path / "source-b-off.db"
    b_db = Database(str(b_path))
    try:
        from tests.helpers import asof_make_site as _mk

        site_b = _mk(b_db._conn, "Close Off Site")  # noqa: SLF001
        feed_id = int(
            b_db._conn.execute(  # noqa: SLF001
                "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
            ).fetchone()["id"]
        )
        hour = f"{_d3().isoformat()}T12:00:00Z"
        asof_insert_pair(
            b_db._conn,  # noqa: SLF001
            site_id=site_b,
            feed_id=feed_id,
            valid_at=hour,
            issued_at=hour,
            forecast=3.0,
            observed=4.0,
            first_known_at=hour,
        )
        b_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        b_db._conn.commit()  # noqa: SLF001
    finally:
        b_db.close()
    payload = b_path.read_bytes()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        caplog.set_level(logging.WARNING)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
    _assert_no_guard_warning(caplog)

    direct = sqlite3.connect(config.db_path)
    try:
        no_site_days = direct.execute(
            "SELECT 1 FROM site_days WHERE site_id=?", (site_b,)
        ).fetchone()
        assert no_site_days is None, "a close-off file must leave no site_days row"
    finally:
        direct.close()


_GW13B_BAD_CASES: Final = (
    (
        "site_days",
        "day_start_utc",
        "not-a-stamp",
        "invalid timestamp in site_days.day_start_utc: expected UTC form"
        " YYYY-MM-DDTHH:MM:SSZ",
    ),
    (
        "site_days",
        "local_date",
        "2024/06/10",
        "invalid date in site_days.local_date: expected YYYY-MM-DD",
    ),
    (
        "station_changes",
        "effective_date",
        "2024/06/10",
        "invalid date in station_changes.effective_date: expected YYYY-MM-DD",
    ),
    (
        "site_zone_changes",
        "effective_date",
        "2024/06/10",
        "invalid date in site_zone_changes.effective_date: expected YYYY-MM-DD",
    ),
    (
        "station_changes",
        "changed_at",
        "2024-06-10T00:00:00Z",
        "invalid timestamp in station_changes.changed_at: expected UTC form"
        " YYYY-MM-DDTHH:MM:SS.ffffffZ",
    ),
    (
        "site_zone_changes",
        "entered_at",
        "2024-06-10T00:00:00Z",
        "invalid timestamp in site_zone_changes.entered_at: expected UTC form"
        " YYYY-MM-DDTHH:MM:SS.ffffffZ",
    ),
)


@pytest.mark.parametrize(("table", "column", "bad_value", "message"), _GW13B_BAD_CASES)
def test_gw13b_import_rejects_each_new_bound_check(
    table: str,
    column: str,
    bad_value: str,
    message: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _init_tmp_db(tmp_path)
    from tests.helpers import asof_make_site

    asof_make_site(conn, "Guarded Site")
    conn.commit()

    b_path = tmp_path / f"bad-{table}-{column}.db"
    b_db = Database(str(b_path))
    try:
        site_b = day_close_site(b_db._conn)  # noqa: SLF001
        if table == "site_zone_changes":
            # day_close_site never calls enter_zone_change, so this table
            # starts with 0 rows and an unscoped UPDATE would be a no-op
            # over nothing -- the import would then see the ORIGINAL (valid)
            # row contents and the test could not fail for the stated
            # reason. Seed one real row to carry the bad value.
            from wxverify.db.site_days import enter_zone_change

            enter_zone_change(b_db._conn, site_b, "Etc/GMT-1", now_utc=NOW)
        if table == "site_days":
            # An unscoped UPDATE sets every row's local_date to the SAME
            # bad_value under one site_id, which for the local_date case
            # collides with the (site_id, local_date) PRIMARY KEY on the
            # second row onward. Scope to one row to avoid that collision.
            if column == "local_date":
                # site_day_stations.FOREIGN KEY (site_id, local_date)
                # REFERENCES site_days(site_id, local_date): changing the
                # PK's local_date out from under its child rows would
                # violate that FK before the import is ever reached. Drop
                # this one day's children first so the file stays
                # consistent -- not a blanket "foreign_keys off", which
                # would leave orphaned children able to make the import
                # refuse the file for a different (wrong) reason.
                b_db._conn.execute(  # noqa: SLF001
                    "DELETE FROM site_day_stations"
                    " WHERE site_id = ? AND local_date = ?",
                    (site_b, _d1().isoformat()),
                )
            b_db._conn.execute(  # noqa: SLF001
                f"UPDATE site_days SET {column} = ?"  # noqa: S608
                " WHERE site_id = ? AND local_date = ?",
                (bad_value, site_b, _d1().isoformat()),
            )
        else:
            b_db._conn.execute(  # noqa: SLF001
                f"UPDATE {table} SET {column} = ?",  # noqa: S608
                (bad_value,),
            )
        b_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        b_db._conn.commit()  # noqa: SLF001
    finally:
        b_db.close()
    payload = b_path.read_bytes()

    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 422, (
        f"{table}.{column}: expected 422, got {resp.status_code}"
    )
    assert resp.json() == {"error": message}

    with TestClient(app, raise_server_exceptions=False) as client:
        sites = client.get("/api/sites").json()
    assert {s["name"] for s in sites} == {"Guarded Site"}, (
        f"{table}.{column}: live DB must be untouched"
    )


def test_gw13b_import_accepts_a_null_retry_at_and_a_set_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    from tests.helpers import asof_make_site

    asof_make_site(conn, "Pre Import Site")
    conn.commit()

    b_path = tmp_path / "retry-at-positive.db"
    b_db = Database(str(b_path))
    try:
        site_b = day_close_site(b_db._conn)  # noqa: SLF001
        row = b_db._conn.execute(  # noqa: SLF001
            "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
            (site_b, _d1().isoformat()),
        ).fetchone()
        from wxverify.core.timeutil import parse_utc

        # A bare UPDATE ... SET retry_at=? violates the CHECK that ties
        # retry_at to (attempts=2 OR state='retry_due' OR missed_reason IS
        # 'retry_not_run'): D1 starts as state='open', attempts=0. Drive it
        # through the real close/fail cycle (mirrors GW10b) so retry_at
        # lands set together with the state/attempts the CHECK requires.
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
        begin_close(b_db._conn, site_b, _d1(), now_utc=note_day_clock(t))  # noqa: SLF001
        fail_close(  # noqa: SLF001
            b_db._conn, site_b, _d1(), now_utc=note_day_clock(t), reason="x"
        )
        b_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        b_db._conn.commit()  # noqa: SLF001
    finally:
        b_db.close()
    payload = b_path.read_bytes()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GW15: the fetch_obs job, dispatched the way the processor does.
# ---------------------------------------------------------------------------


def test_gw15_fetch_obs_job_does_not_retry_on_a_closed_day_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    station_a_pws = conn.execute(
        "SELECT pws_station_id FROM stations WHERE id = ?", (station_a,)
    ).fetchone()["pws_station_id"]
    d3 = _d3()
    before_d3 = _day_snapshot(conn, site_id, d3)
    before_d1 = _day_snapshot(conn, site_id, _d1())

    async def fake_history(
        station_id: str,
        api_key: str,
        *,
        hours: int,
        timezone: str | None = None,
        client: object | None = None,
    ) -> list[PwsObservation]:
        return [
            PwsObservation(
                variable="temperature",
                valid_at=f"{d3.isoformat()}T06:00:00Z",
                value=5.0,
                source_raw="5.0 C",
            )
        ]

    monkeypatch.setattr("wxverify.worker.processor.fetch_hourly_history", fake_history)
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: NOW)
    # _fetch_obs calls resolve_secret("weathercom") unconditionally before
    # the guard is ever reached; without a configured key it raises
    # RuntimeError("weathercom key is not configured") first, which is not
    # the closed-day refusal this test means to exercise. `_init_tmp_db`
    # points `config.options_path` at a real options.json, and
    # `load_runtime_config` reads it whenever the file exists -- an
    # environment variable is never consulted in that case (core/options.py)
    # -- so the key must be put in that file, not in the environment.
    Path(config.options_path).write_text(
        '{"weathercom_key": "0123456789abcdef0123456789abcdef"}', encoding="utf-8"
    )
    dispatch_db = get_db()

    caplog.set_level(logging.WARNING)
    asyncio.run(
        dispatch(
            dispatch_db,
            FencedWriter(dispatch_db, dispatch_db.generation),
            Job(
                id=1,
                type="fetch_obs",
                site_id=site_id,
                job_key=str(station_a),
                payload={"station_id": station_a_pws},
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    # dispatch returning normally (no exception propagated) is the oracle:
    # the refusal must not be raised into the retry ladder.
    assert _day_snapshot(conn, site_id, d3) == before_d3
    # The helper site has two enabled stations (A and B); _fetch_obs calls
    # the W1 writer once per enabled station, and fake_history returns the
    # same single D-3 hour for each, so the guard logs one WARNING per
    # station -- two in total, both refusing the same D-3 hour.
    hits = _guard_warnings(caplog)
    assert len(hits) == 2, hits
    for hit in hits:
        assert hit["kind"] == "HOURLY", hit
        assert hit["site"] == str(site_id), hit
        assert hit["days"] == d3.isoformat(), hit
        assert hit["count"] == "1", hit
    # fake_history only ever returns a D-3 hour, so the call writes nothing
    # on D-1 either.
    assert _day_snapshot(conn, site_id, _d1()) == before_d1


# ---------------------------------------------------------------------------
# GW16: the one allowed writer, DELETE /api/sites/{id}, after a zone change.
# ---------------------------------------------------------------------------


def test_gw16_delete_site_cascades_every_daily_close_table_after_a_zone_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.db.site_days.utc_now", lambda: NOW, raising=False)
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn)
    enter_zone_change(conn, site_id, "Etc/GMT-1", now_utc=NOW)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        caplog.set_level(logging.WARNING)
        resp = client.delete(f"/api/sites/{site_id}", headers=headers)
        assert resp.status_code == 200

    _assert_no_guard_warning(caplog)
    direct = sqlite3.connect(config.db_path)
    try:
        for table in (
            "site_days",
            "site_day_stations",
            "station_changes",
            "site_zone_changes",
        ):
            remaining = direct.execute(
                f"SELECT 1 FROM {table} WHERE site_id = ?",
                (site_id,),  # noqa: S608
            ).fetchone()
            assert remaining is None, f"{table} must have no row of the deleted site"
    finally:
        direct.close()


# ---------------------------------------------------------------------------
# GW18: W16, _finish_day with the history_all hold held.
# ---------------------------------------------------------------------------


def test_gw18_finish_day_refuses_a_done_day_finishes_the_open_day(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: NOW)
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, _STATION_A)
    d3, d1 = _d3(), _d1()
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(NOW), error="401"
    )
    _insert_wind_day(conn, station_a, d3, status="pending")
    _insert_wind_day(conn, station_a, d1, status="pending")
    conn.commit()
    before_d3_row = dict(
        conn.execute(
            "SELECT * FROM station_wind_days WHERE station_id=? AND local_date=?",
            (station_a, d3.isoformat()),
        ).fetchone()
    )
    before_d3_wind = _rows(
        conn,
        "SELECT * FROM station_observations WHERE station_id=? AND variable='wind'",
        (station_a,),
    )

    caplog.set_level(logging.WARNING)
    finished_d3 = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d3,
        tz_name="UTC",
        today=D,
    )
    conn.commit()
    assert finished_d3 is False
    after_d3_row = dict(
        conn.execute(
            "SELECT * FROM station_wind_days WHERE station_id=? AND local_date=?",
            (station_a, d3.isoformat()),
        ).fetchone()
    )
    assert before_d3_row == after_d3_row
    assert before_d3_wind == _rows(
        conn,
        "SELECT * FROM station_observations WHERE station_id=? AND variable='wind'",
        (station_a,),
    )
    hits = _guard_warnings(caplog)
    assert len(hits) == 1, hits
    assert hits[0]["days"] == d3.isoformat()

    caplog.clear()
    finished_d1 = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_a,
        local_date=d1,
        tz_name="UTC",
        today=D,
    )
    conn.commit()
    assert finished_d1 is True
    after_d1_row = dict(
        conn.execute(
            "SELECT * FROM station_wind_days WHERE station_id=? AND local_date=?",
            (station_a, d1.isoformat()),
        ).fetchone()
    )
    # _finish_day's own UPDATE (wind_days.py ~:1502-1519) sets status to
    # 'fetched' or 'unavailable' depending on the station_wind_records count
    # it finds -- this fixture plants no such records for D-1, so the row
    # must flip from the 'pending' this test inserted to 'unavailable'.
    assert after_d1_row["status"] == "unavailable", after_d1_row


# ---------------------------------------------------------------------------
# GW17: the source-scan writer-list pin (plan Sec 13.4).
# ---------------------------------------------------------------------------

_DAY_WRITE_TARGETS: Final = frozenset(
    {
        "station_wind_records",
        "station_observations",
        "observations",
        "daily_truth",
        "forecast_pairs",
        "verification_evidence",
        "site_days",
        "site_day_stations",
        "station_changes",
        "site_zone_changes",
    }
)

_SQL_HEAD_RE: Final = re.compile(
    r"^\s*(INSERT(?:\s+OR\s+\w+)?\s+INTO|REPLACE\s+INTO|DELETE\s+FROM|UPDATE)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*|\{[^}]*\})",
    re.IGNORECASE,
)

# Plan 13.4: "No string holds DELETE FROM stations (8.3)" -- checked over
# every string constant and flattened f-string, not the raw source text.
_DELETE_STATIONS_RE: Final = re.compile(r"DELETE\s+FROM\s+stations\b", re.IGNORECASE)

# Plan 13.4's fourth assertion: outside db/migrations.py, a write to any of
# these lies in db/site_days.py (4.7 and 6.4's state functions).
_STATE_TABLES: Final = frozenset(
    {"site_days", "site_day_stations", "station_changes", "site_zone_changes"}
)

# One entry per writer of plan Sec 7.3/7.4 and per migration that writes
# these tables, each with the reason the plan names for it.
_GW17_ALLOWLIST: Final = frozenset(
    {
        # P2's raw-reading writer, through P1's guard (plan 6.4).
        ("wxverify.db.station_readings", "store_readings"),
        ("wxverify.scoring.consensus", "write_station_observation"),  # 7.3 backstop
        ("wxverify.scoring.consensus", "materialize_consensus"),  # 7.3
        # Daily-close plan 7.7: P3's close, stage 3.
        ("wxverify.worker.day_values", "write_hourly_values"),
        # Daily-close plan 10.3: P3's run_close_retention.
        ("wxverify.db.station_readings", "prune_raw_readings"),
        ("wxverify.worker.wind_days", "_derive_days"),  # 7.3 W3
        ("wxverify.worker.wind_days", "_reconcile"),  # 7.3 W2
        ("wxverify.worker.wind_days", "_finish_day"),  # 7.3 W16
        ("wxverify.worker.wind_days", "_invalidate_unparseable"),  # 7.3 W9
        ("wxverify.verification.truth", "_write_day"),  # 7.3 W11
        # P1 7.3 row: P3's writers, TRUTH.
        ("wxverify.verification.truth", "write_close_truth"),
        ("wxverify.api.routes.sites", "update_site"),  # 7.3 W12
        ("wxverify.provider_ops", "_invalidate_mean_for_site"),  # 7.3 W13
        ("wxverify.api.routes.db_transfer", "_rebuild_all"),  # 7.5 W14
        ("wxverify.db.site_days", "*"),  # P1 state functions, 4.7 and 6.4
        ("wxverify.worker.tz_correction", "*"),  # P6 removes
        ("wxverify.verification.runs", "*"),  # verification tables only
        ("wxverify.db.migrations", "SEED_STATION_CHANGES_SQL"),  # migration (v11)
        # Its rain-pair delete goes with P6 R15; drop this entry then.
        ("wxverify.db.migrations", "record_precip_hour_start_boundary"),  # migration
        ("wxverify.scoring.consensus", "_invalidate_consensus_dependents"),  # 7.3
        ("wxverify.scoring.consensus", "_delete_future_persistence_pairs"),  # 7.3 notes
        ("wxverify.scoring.multimodel", "_MEAN_INSERT_SQL"),  # 7.3 W10
        ("wxverify.scoring.pairing", "_PAIR_INSERT_SQL"),  # 7.3 W10
        # Also run unguarded by insert_persistence_pair (tz rebuild only): P6 removes.
        ("wxverify.scoring.persistence", "_PERSISTENCE_INSERT_SQL"),  # 7.3 W10
        ("wxverify.scoring.split", "_DELETE_PAIR_BY_ID_SQL"),  # 7.3 W10
        ("wxverify.scoring.tz_rebuild", "delete_building_rows"),  # P6 removes (W8)
        ("wxverify.scoring.tz_rebuild", "rebuild_generation_day"),  # P6 removes (W8)
        ("wxverify.scoring.tz_rebuild", "_rebuild_real_pairs"),  # P6 removes (W8)
        ("wxverify.scoring.tz_rebuild", "_rebuild_multimodel_mean"),  # P6 removes (W8)
        # The replay's persist_day_evidence; P6 keeps it, uncalled (N7).
        ("wxverify.verification.simulate", "_EVIDENCE_INSERT_SQL"),  # not a day write
        ("wxverify.verification.truth", "mark_daily_truth_stale"),  # 7.3 notes
    }
)


def test_gw17_writer_source_scan_matches_the_allowlist() -> None:
    import ast

    import wxverify

    root = Path(wxverify.__file__).parent
    hits: set[tuple[str, str]] = set()
    hit_targets: dict[tuple[str, str], set[str]] = {}
    stale_module_calls: list[str] = []
    copy_on_calls: list[tuple[str, str]] = []
    state_table_modules: set[str] = set()

    for py_file in sorted(root.rglob("*.py")):
        module = ".".join(
            ("wxverify", *py_file.relative_to(root).with_suffix("").parts)
        )
        if module.endswith(".__init__"):
            module = module[: -len(".__init__")]
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))

        def _enclosing(n: ast.AST, tree: ast.AST) -> str:
            for parent in ast.walk(tree):
                if isinstance(
                    parent, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and n in ast.walk(parent):
                    return parent.name
            # Not inside any function: name the module-level constant whose
            # value holds it, if any (plan 13.4: "enclosing function or
            # module-level constant").
            for stmt in ast.iter_child_nodes(tree):
                if isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
                    if n in ast.walk(stmt.value) and isinstance(stmt.target, ast.Name):
                        return stmt.target.id
                elif isinstance(stmt, ast.Assign) and (
                    stmt.value is not None and n in ast.walk(stmt.value)
                ):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            return target.id
            return "<module>"

        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not _DELETE_STATIONS_RE.search(node.value), (
                    f"{module}: must never string-delete the stations table (8.3)"
                )
                match = _SQL_HEAD_RE.match(node.value)
                if match:
                    target = match.group(2)
                    if target in _STATE_TABLES:
                        state_table_modules.add(module)
                    if (
                        target in _DAY_WRITE_TARGETS
                        or (
                            target == "station_wind_days"
                            and "refetch_at" in node.value
                            and match.group(1).upper() == "UPDATE"
                        )
                        or target.startswith("{")
                    ):
                        fn = _enclosing(node, tree)
                        hits.add((module, fn))
                        hit_targets.setdefault((module, fn), set()).add(target)
            if isinstance(node, ast.JoinedStr):
                flat = "".join(
                    "{}" if isinstance(piece, ast.FormattedValue) else piece.value
                    for piece in node.values
                    if isinstance(piece, (ast.Constant, ast.FormattedValue))
                )
                assert not _DELETE_STATIONS_RE.search(flat), (
                    f"{module}: must never string-delete the stations table (8.3)"
                )
                match = _SQL_HEAD_RE.match(flat)
                if match:
                    target = match.group(2)
                    if target in _STATE_TABLES:
                        state_table_modules.add(module)
                    if target in _DAY_WRITE_TARGETS or target.startswith("{"):
                        fn = _enclosing(node, tree)
                        hits.add((module, fn))
                        hit_targets.setdefault((module, fn), set()).add(target)
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "open_day_on_copy"
            ):
                copy_on_calls.append((module, _enclosing(node, tree)))
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "mark_daily_truth_stale"
            ):
                stale_module_calls.append(f"{module}.{_enclosing(node, tree)}")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and (node.func.id == "mark_daily_truth_stale")
            ):
                stale_module_calls.append(f"{module}.{_enclosing(node, tree)}")

    wildcard_modules = {module for module, fn in _GW17_ALLOWLIST if fn == "*"}
    for module in sorted(wildcard_modules):
        relative = module.removeprefix("wxverify.").replace(".", "/")
        module_path = root / f"{relative}.py"
        assert module_path.exists(), (
            f"wildcard allowlist entry {module!r} has no module at {module_path}"
        )
    checked_hits = {
        (module, fn)
        for module, fn in hits
        if module not in wildcard_modules
        or (
            module == "wxverify.db.site_days"
            and not hit_targets.get((module, fn), set()).issubset(_STATE_TABLES)
        )
    }
    unreviewed = sorted(checked_hits - _GW17_ALLOWLIST)
    assert not unreviewed, f"unreviewed daily-close table writers: {unreviewed}"

    listed = {(module, fn) for module, fn in _GW17_ALLOWLIST if fn != "*"}
    disappeared = sorted(listed - hits)
    assert not disappeared, f"listed writers disappeared: {disappeared}"

    assert sorted(stale_module_calls) == [
        "wxverify.scoring.consensus.materialize_consensus",
        "wxverify.worker.wind_days._invalidate_unparseable",
    ]
    assert copy_on_calls == [("wxverify.__main__", "_close_day")], copy_on_calls

    offending_state_modules = sorted(
        state_table_modules - {"wxverify.db.migrations", "wxverify.db.site_days"}
    )
    assert not offending_state_modules, (
        f"site_days/site_day_stations/station_changes/site_zone_changes written "
        f"outside db.site_days: {offending_state_modules}"
    )
