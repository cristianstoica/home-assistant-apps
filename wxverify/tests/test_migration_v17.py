"""W7, the v17 storage-release migration (plan `2026-11-02-storage-release.md`
section 4.1, 5.3; work item W7). R2, R7, W7's own checklist (lines 314-326).

Written before ``migrate_v17_storage``, ``V17_DROP_ORDER`` and
``TARGET_USER_VERSION == 17`` exist, and fails first for that reason: this
module imports only ``wxverify.db.migrations`` as a module (never the new
names directly), so it collects today; each test below reaches
``migrations.migrate_v17_storage(...)`` / ``migrations.V17_DROP_ORDER`` /
``migrations.TARGET_USER_VERSION`` only inside its own body, where it fails
on its own -- an ``AttributeError`` for the first two (today's
``db/migrations.py`` has neither name), a plain assertion failure for the
third (``TARGET_USER_VERSION`` is 16 today, not 17).

Fixture: ``tests/fixtures/v16_schema.sql`` is the exact ``sqlite_master``
schema of a fresh 0.16.15 database (schema only, no data);
``tests/fixtures/v16_seed.sql`` is that same release's boot seed (its
``sources``, ``feeds``, ``settings`` and ``runtime_state`` rows, dumped with
``iterdump``). Every test below loads both into a fresh ``tmp_path`` file,
then calls ``migrations.create_tables`` (idempotent, ``IF NOT EXISTS``) so
``forecast_history`` -- new in version 17, absent from the v16 schema --
exists for ``migrate_v17_storage`` to write into, the same order
``run_migrations`` itself uses. ``PRAGMA user_version`` is then set to 16,
and each test inserts only the further rows it needs -- synthetic ids,
station/site names and values throughout (public repo; no real station,
device or place identifier).

Isolation: every connection is a real file-backed SQLite database under
``tmp_path``; no mocked persistence. ``isolation_level=None`` (autocommit)
on every raw connection built here, the same convention
``tests/test_migration_runner_pre_v4.py`` uses for driving ``run_migrations``
directly, so an explicit ``SAVEPOINT``/``RELEASE`` inside the function under
test behaves exactly as it will on the real writer connection
(``wxverify.db.connection.Database`` also opens with
``isolation_level=None``).
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db import migrations
from wxverify.db.connection import close_db, init_db
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.db.switch import ensure_site_generation, site_generation_id

_FIXTURE_SQL = (Path(__file__).parent / "fixtures" / "v16_schema.sql").read_text(
    encoding="utf-8"
)
_SEED_SQL = (Path(__file__).parent / "fixtures" / "v16_seed.sql").read_text(
    encoding="utf-8"
)

_VERIFICATION_TABLES: tuple[str, ...] = (
    "verification_evidence",
    "verification_day_context",
    "verification_results",
    "verification_verdicts",
    "verification_run_inputs",
    "verification_trigger_decisions",
    "verification_runs",
)
_DROPPED_TABLES: tuple[str, ...] = (
    *_VERIFICATION_TABLES,
    "close_pairs",
    "forecast_pairs",
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _v16_connect(db_path: Path) -> sqlite3.Connection:
    """A real, file-backed v16 database at ``db_path``: the 0.16.15 schema
    plus its boot seed (sources/feeds/settings/runtime_state), with
    ``forecast_history`` added via ``create_tables`` (the same ``IF NOT
    EXISTS`` call `run_migrations` makes before the version 17 upgrade --
    without it, calling `migrations.migrate_v17_storage` directly on this
    fixture fails with "no such table: forecast_history"), foreign keys on,
    autocommit (the `run_migrations`-direct convention)."""
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(_FIXTURE_SQL)
    conn.executescript(_SEED_SQL)
    migrations.create_tables(conn)
    conn.execute("PRAGMA user_version = 16")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _mark_close_on(conn: sqlite3.Connection, *, when: datetime | None = None) -> None:
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(when or utc_now()))


def _seed_site(
    conn: sqlite3.Connection, *, timezone: str = "UTC", name: str = "wx-v17-site-a"
) -> int:
    """A synthetic site with its published time zone generation and the real
    seeded feeds (``migrations.seed_default_feeds``, kept by W7 -- it is the
    run-order step 4 seeding, not removed DDL)."""
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 12.5, -34.0, 50.0, ?)",
        (name, timezone),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    migrations.seed_default_feeds(conn)
    ensure_site_generation(conn, int(site_id))
    return int(site_id)


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None, f"seeded feed {source}/{model} missing"
    return int(row["id"])


def _generation_id(conn: sqlite3.Connection, site_id: int) -> int:
    gen = site_generation_id(conn, site_id)
    assert gen is not None
    return gen


def _insert_forecast_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    tz_generation_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    day_ahead: int,
    forecast: float,
    variable: str = "temperature",
    contributors: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, day_ahead, forecast, observed, tz_generation_id,"
        " contributors) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            forecast - 0.3,  # a plausible 'observed' value, never read by the migration
            tz_generation_id,
            contributors,
        ),
    )


def _insert_close_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    feed_id: int,
    day_ahead: int,
    valid_at: str,
    lead_hours: int,
    forecast: float,
    variable: str = "temperature",
    issued_at: str | None = "2035-01-01T00:00:00Z",
    contributors: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO close_pairs (site_id, local_date, feed_id, variable,"
        " day_ahead, valid_at, lead_hours, forecast, observed, issued_at,"
        " contributors) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            valid_at,
            lead_hours,
            forecast,
            forecast - 0.3,
            issued_at,
            contributors,
        ),
    )


def _insert_site_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    day_start_utc: str,
    day_end_utc: str,
    timezone: str,
    state: str = "closed",
) -> None:
    now = isoformat_utc(utc_now())
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, attempts, created_at, state_changed_at, done_at)"
        " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            state,
            day_start_utc,
            day_end_utc,
            timezone,
            now,
            now,
            now,
        ),
    )


def _run_migrate_v17(conn: sqlite3.Connection) -> None:
    """Call the function under test the way ``run_migrations`` will: on an
    autocommit connection, no surrounding ``BEGIN`` (the function opens its
    own ``SAVEPOINT``)."""
    migrations.migrate_v17_storage(conn)


def _forecast_history_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT site_id, feed_id, variable, day_ahead, valid_at, local_date,"
            " value, lead_hours, issued_at, contributors FROM forecast_history"
            " ORDER BY site_id, feed_id, valid_at"
        )
    )


def _insert_verification_chain(conn: sqlite3.Connection, *, site_id: int) -> int:
    """One ``verification_runs`` row with one row in every dependent table,
    including a ``verification_trigger_decisions`` row pointing at it with a
    PLAIN (non-cascading) ``REFERENCES`` (R2's own failure case). Returns the
    run id."""
    gen_id = _generation_id(conn, site_id)
    cur = conn.execute(
        "INSERT INTO verification_runs (site_id, tz_generation_id,"
        " methodology_version, app_version, state, attempt, config_snapshot,"
        " bootstrap_seed, bootstrap_resamples, input_fingerprint)"
        " VALUES (?, ?, 1, '0.16.15', 'published', 1, '{}', 1, 1, 'fp-v17-r2')",
        (site_id, gen_id),
    )
    run_id = cur.lastrowid
    assert run_id is not None
    conn.execute(
        "INSERT INTO verification_day_context (run_id, snapshot_local_date,"
        " snapshot_utc, knowability_exclusions, null_availability_samples)"
        " VALUES (?, '2035-01-02', '2035-01-02T07:00:00Z', '[]', 0)",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_evidence (run_id, snapshot_local_date,"
        " target_local_date, lead, variable, quantity, entity_type, entity_key,"
        " forecast_eligible, truth_eligible) VALUES (?, '2035-01-02',"
        " '2035-01-03', 1, 'temperature', 'temperature_high', 'feed', 'x', 1, 1)",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_results (run_id, variable, lead, quantity,"
        " entity_type, entity_key, headline, common_days) VALUES"
        " (?, 'temperature', 1, 'temperature_high', 'feed', 'x', 1, 3)",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_verdicts (run_id, variable, outcome,"
        " incumbent_depth, tested_family) VALUES"
        " (?, 'temperature', 'retain_incumbent', 1, 'depth')",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_run_inputs (run_id, component, value, scope)"
        " VALUES (?, 'config', '{}', 'global')",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO verification_trigger_decisions (site_id, trigger_date,"
        " decided_at, decision, run_id) VALUES"
        " (?, '2035-01-02', '2035-01-02T07:00:00Z', 'run_started', ?)",
        (site_id, run_id),
    )
    return run_id


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# W7 item 1: the earliest instant per key wins, with a tie on two text forms
# of one instant (R7) decided by TEXT order after the julianday tie, not by
# insertion id.
# ---------------------------------------------------------------------------


def test_conversion_keeps_the_earliest_instant_per_key_with_an_r7_text_tie(
    tmp_path: Path,
) -> None:
    conn = _v16_connect(tmp_path / "earliest.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gen_id = _generation_id(conn, site_id)

    # Key K1: three issued_at instants, strictly ordered -- the earliest
    # (smallest julianday) must win, not the latest and not insertion order.
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-05-31T18:00:00Z",  # later: must lose
        valid_at="2035-06-01T06:00:00Z",
        lead_hours=12,
        day_ahead=1,
        forecast=6.0,
    )
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-05-30T12:00:00Z",  # earliest: must win
        valid_at="2035-06-01T06:00:00Z",
        lead_hours=42,
        day_ahead=1,
        forecast=5.0,
    )

    # Key K2: an R7 tie -- two TEXT forms of the identical instant
    # (00:00:00Z == 00:00:00+00:00), equal julianday. The "Z" form is
    # inserted FIRST (lower id); the "+00:00" form sorts before it in plain
    # TEXT order ('+' < 'Z'), so if the migration's ORDER BY ever dropped the
    # ``issued_at`` text tiebreak and fell back to insertion id, it would
    # pick the "Z" row (forecast=9.0) instead of the "+00:00" row
    # (forecast=8.0) this test pins as the winner.
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-06-01T00:00:00Z",
        valid_at="2035-06-02T12:00:00Z",
        lead_hours=36,
        day_ahead=1,
        forecast=9.0,
    )
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-06-01T00:00:00+00:00",
        valid_at="2035-06-02T12:00:00Z",
        lead_hours=36,
        day_ahead=1,
        forecast=8.0,
    )
    conn.commit()

    _run_migrate_v17(conn)
    rows = _forecast_history_rows(conn)
    by_valid_at = {row["valid_at"]: row for row in rows}

    assert by_valid_at["2035-06-01T06:00:00Z"]["value"] == pytest.approx(5.0), (
        "K1 must keep the EARLIEST issued_at's forecast (5.0), not the later one (6.0)"
    )
    assert by_valid_at["2035-06-02T12:00:00Z"]["value"] == pytest.approx(8.0), (
        "R7: a julianday tie must break on issued_at TEXT order ('+00:00' <"
        " 'Z'), keeping the '+00:00' row's forecast (8.0), not the 'Z' row"
        " inserted first (9.0)"
    )


# ---------------------------------------------------------------------------
# W7 item 2: persistence rows and rows at/after the first record are not
# converted; a site with no record converts every row; the mean keeps
# contributors.
# ---------------------------------------------------------------------------


def test_persistence_and_post_first_record_rows_are_excluded_mean_keeps_contributors(
    tmp_path: Path,
) -> None:
    conn = _v16_connect(tmp_path / "exclude.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-b")
    gen_id = _generation_id(conn, site_id)
    real_feed = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_feed = _feed_id(conn, "virtual", "_persistence")
    mean_feed = _feed_id(conn, "virtual", "_multimodel_mean")

    # The site's first record: valid_at before it converts, at/after it does
    # not.
    _insert_site_day(
        conn,
        site_id=site_id,
        local_date=date(2035, 6, 5),
        day_start_utc="2035-06-05T00:00:00Z",
        day_end_utc="2035-06-06T00:00:00Z",
        timezone="UTC",
    )

    _insert_forecast_pair(  # kept: before the first record, real feed
        conn,
        site_id=site_id,
        feed_id=real_feed,
        tz_generation_id=gen_id,
        issued_at="2035-06-03T00:00:00Z",
        valid_at="2035-06-04T12:00:00Z",
        lead_hours=36,
        day_ahead=1,
        forecast=11.0,
    )
    _insert_forecast_pair(  # dropped: persistence
        conn,
        site_id=site_id,
        feed_id=persistence_feed,
        tz_generation_id=gen_id,
        issued_at="2035-06-03T00:00:00Z",
        valid_at="2035-06-04T12:00:00Z",
        lead_hours=36,
        day_ahead=1,
        forecast=10.5,
    )
    _insert_forecast_pair(  # dropped: at the first record's day_start_utc
        conn,
        site_id=site_id,
        feed_id=real_feed,
        tz_generation_id=gen_id,
        issued_at="2035-06-04T00:00:00Z",
        valid_at="2035-06-05T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=12.0,
    )
    _insert_forecast_pair(  # kept: the mean, with contributors
        conn,
        site_id=site_id,
        feed_id=mean_feed,
        tz_generation_id=gen_id,
        issued_at="2035-06-03T06:00:00Z",
        valid_at="2035-06-04T18:00:00Z",
        lead_hours=36,
        day_ahead=1,
        forecast=11.5,
        contributors=4,
    )
    conn.commit()

    _run_migrate_v17(conn)
    rows = {
        (row["feed_id"], row["valid_at"]): row for row in _forecast_history_rows(conn)
    }

    assert (real_feed, "2035-06-04T12:00:00Z") in rows, (
        "pre-record real row must convert"
    )
    assert (mean_feed, "2035-06-04T18:00:00Z") in rows, (
        "pre-record mean row must convert"
    )
    assert rows[(mean_feed, "2035-06-04T18:00:00Z")]["contributors"] == 4, (
        "the mean's contributors must be kept across the conversion"
    )
    assert (persistence_feed, "2035-06-04T12:00:00Z") not in rows, (
        "a persistence row (virtual/_persistence) must never be converted"
    )
    assert (real_feed, "2035-06-05T00:00:00Z") not in rows, (
        "a row at the first record's day_start_utc (not strictly before it)"
        " must not be converted"
    )


def test_a_site_with_no_record_converts_every_row(tmp_path: Path) -> None:
    conn = _v16_connect(tmp_path / "no-record.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-c")
    gen_id = _generation_id(conn, site_id)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    # No site_days row at all for this site -- every forecast_pairs row
    # (however far in the future) must convert.
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-09-01T00:00:00Z",
        valid_at="2035-12-31T23:00:00Z",
        lead_hours=100,
        day_ahead=4,
        forecast=3.0,
    )
    conn.commit()

    _run_migrate_v17(conn)
    rows = _forecast_history_rows(conn)
    assert len(rows) == 1 and rows[0]["valid_at"] == "2035-12-31T23:00:00Z", (
        "a site with no site_days record must convert all of its rows,"
        " however far in the future"
    )


# ---------------------------------------------------------------------------
# W7 item 3: local_date is right across a DST change. Independent oracle:
# Python's own zoneinfo-derived local date, never a hand-computed offset.
# ---------------------------------------------------------------------------


def test_local_date_is_correct_across_a_dst_change(tmp_path: Path) -> None:
    conn = _v16_connect(tmp_path / "dst.db")
    _mark_close_on(conn)
    tz_name = "America/New_York"
    site_id = _seed_site(conn, timezone=tz_name, name="wx-v17-site-dst")
    gen_id = _generation_id(conn, site_id)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    # 2035-03-11 is the US spring-forward Sunday; these two instants straddle
    # it (one well before, one well after), in a zone where the naive-UTC
    # date and the local date can genuinely differ depending on the correct
    # standard/daylight offset for that date.
    valid_ats = ["2035-03-09T06:30:00Z", "2035-03-13T06:30:00Z"]
    for i, valid_at in enumerate(valid_ats):
        _insert_forecast_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            tz_generation_id=gen_id,
            issued_at="2035-03-01T00:00:00Z",
            valid_at=valid_at,
            lead_hours=12 + i,
            day_ahead=1,
            forecast=1.0 + i,
        )
    conn.commit()

    _run_migrate_v17(conn)
    rows = {row["valid_at"]: row for row in _forecast_history_rows(conn)}

    tz = ZoneInfo(tz_name)
    for valid_at in valid_ats:
        instant = datetime.fromisoformat(valid_at.replace("Z", "+00:00"))
        expected_local_date = instant.astimezone(tz).date().isoformat()
        assert rows[valid_at]["local_date"] == expected_local_date, (
            f"local_date for {valid_at} must be {expected_local_date} (computed"
            f" via zoneinfo for {tz_name}), got {rows[valid_at]['local_date']!r}"
        )


# ---------------------------------------------------------------------------
# W7 item 4: close_pairs rows copy as they are, persistence excluded.
# ---------------------------------------------------------------------------


def test_close_pairs_rows_are_copied_as_is_less_persistence(tmp_path: Path) -> None:
    conn = _v16_connect(tmp_path / "close-pairs.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-d")
    real_feed = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_feed = _feed_id(conn, "virtual", "_persistence")
    mean_feed = _feed_id(conn, "virtual", "_multimodel_mean")
    local_date = date(2035, 4, 1)
    _insert_site_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        day_start_utc="2035-04-01T00:00:00Z",
        day_end_utc="2035-04-02T00:00:00Z",
        timezone="UTC",
    )

    _insert_close_pair(
        conn,
        site_id=site_id,
        local_date=local_date,
        feed_id=real_feed,
        day_ahead=1,
        valid_at="2035-04-01T06:00:00Z",
        lead_hours=30,
        forecast=14.0,
        issued_at="2035-03-31T00:00:00Z",
    )
    _insert_close_pair(  # dropped: persistence
        conn,
        site_id=site_id,
        local_date=local_date,
        feed_id=persistence_feed,
        day_ahead=1,
        valid_at="2035-04-01T06:00:00Z",
        lead_hours=30,
        forecast=13.5,
        issued_at=None,
    )
    _insert_close_pair(  # kept: the closed-day mean, issued_at NULL
        conn,
        site_id=site_id,
        local_date=local_date,
        feed_id=mean_feed,
        day_ahead=1,
        valid_at="2035-04-01T06:00:00Z",
        lead_hours=30,
        forecast=14.2,
        issued_at=None,
        contributors=3,
    )
    conn.commit()

    _run_migrate_v17(conn)
    rows = {row["feed_id"]: row for row in _forecast_history_rows(conn)}

    assert real_feed in rows and rows[real_feed]["value"] == pytest.approx(14.0)
    assert rows[real_feed]["local_date"] == local_date.isoformat()
    assert rows[real_feed]["lead_hours"] == 30
    assert mean_feed in rows and rows[mean_feed]["contributors"] == 3
    assert rows[mean_feed]["issued_at"] is None, (
        "the closed-day mean's issued_at must stay NULL after the copy"
    )
    assert persistence_feed not in rows, "a close_pairs persistence row must not copy"


# ---------------------------------------------------------------------------
# W7 item 5 / R2: drop order.
# ---------------------------------------------------------------------------


def test_drop_order_leaves_every_verification_table_gone_with_no_fk_violation(
    tmp_path: Path,
) -> None:
    conn = _v16_connect(tmp_path / "drop-order.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-e")
    _insert_verification_chain(conn, site_id=site_id)
    conn.commit()

    _run_migrate_v17(conn)

    for table in _DROPPED_TABLES:
        assert not _table_exists(conn, table), f"{table} must be dropped"
    assert list(conn.execute("PRAGMA foreign_key_check")) == [], (
        "the drop order must leave no dangling foreign key"
    )


def test_a_reversed_drop_order_fails_and_leaves_v16_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R2's own negative: swap ``verification_runs`` and
    ``verification_trigger_decisions`` in ``V17_DROP_ORDER`` so the parent is
    dropped BEFORE its plain (non-cascading) child reference, while that
    child row still exists. This must fail -- proving the real order (which
    drops every child first) is load-bearing, not incidental."""
    conn = _v16_connect(tmp_path / "drop-order-reversed.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-f")
    _insert_verification_chain(conn, site_id=site_id)
    conn.commit()

    order = list(migrations.V17_DROP_ORDER)
    i, j = (
        order.index("verification_runs"),
        order.index("verification_trigger_decisions"),
    )
    order[i], order[j] = order[j], order[i]
    monkeypatch.setattr(migrations, "V17_DROP_ORDER", tuple(order))

    with pytest.raises(Exception):  # noqa: B017 - the exact DB error is production's
        _run_migrate_v17(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16, (
        "a failed drop must leave user_version at 16"
    )
    for table in _DROPPED_TABLES:
        assert _table_exists(conn, table), (
            f"a failed drop must leave {table} in place (rolled back to the savepoint)"
        )


# ---------------------------------------------------------------------------
# W7 item 6: the jobs allowlist (§5.3).
# ---------------------------------------------------------------------------


def _insert_job(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    job_type: str,
    job_key: str | None,
    status: str = "pending",
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES (?, ?, ?, ?)",
        (job_type, site_id, job_key, status),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_jobs_allowlist_keeps_exactly_the_listed_kinds(tmp_path: Path) -> None:
    conn = _v16_connect(tmp_path / "jobs.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-g")

    kept_ids = [
        _insert_job(conn, site_id=site_id, job_type="fetch_current_obs", job_key="k1"),
        _insert_job(conn, site_id=site_id, job_type="fetch_feed", job_key="k2"),
        _insert_job(
            conn, site_id=site_id, job_type="day_close", job_key="close:1:2035-01-01:2"
        ),
        _insert_job(
            conn,
            site_id=site_id,
            job_type="day_publish",
            job_key=f"publish:{site_id}:7",
        ),
        _insert_job(
            conn,
            site_id=site_id,
            job_type="fetch_obs",
            job_key="wind-days",
            status="failed",
        ),
    ]
    # card 29: forecast_record and record_gap_scan left _V17_KEPT_JOBS_SQL
    # along with the jobs themselves, so the 16->17 upgrade now deletes them
    # too, same as every other non-allowlisted type.
    removed_ids = [
        _insert_job(conn, site_id=site_id, job_type="forecast_record", job_key="k3"),
        _insert_job(conn, site_id=site_id, job_type="record_gap_scan", job_key="k4"),
        _insert_job(conn, site_id=site_id, job_type="pair_and_score", job_key="p1"),
        _insert_job(conn, site_id=None, job_type="catchup", job_key=None),
        _insert_job(conn, site_id=site_id, job_type="backfill_site", job_key="b1"),
        _insert_job(
            conn, site_id=site_id, job_type="timezone_correction", job_key="t1"
        ),
        _insert_job(conn, site_id=site_id, job_type="verification_run", job_key="v1"),
        _insert_job(conn, site_id=site_id, job_type="fetch_obs", job_key="old-key-1"),
        _insert_job(
            conn,
            site_id=site_id,
            job_type="day_publish",
            job_key=f"publish:{site_id}:8",
        ),
    ]
    conn.commit()

    _run_migrate_v17(conn)

    remaining = {int(row["id"]) for row in conn.execute("SELECT id FROM jobs")}
    for job_id in kept_ids:
        assert job_id in remaining, f"kept job {job_id} must survive the cleanup"
    for job_id in removed_ids:
        assert job_id not in remaining, f"removed-kind job {job_id} must be deleted"


# ---------------------------------------------------------------------------
# W7 item 7: verification_* runtime_state keys and the publish-hold setting
# go; import_rebuild_state goes; other keys stay.
# ---------------------------------------------------------------------------


def test_verification_keys_and_import_rebuild_state_go_other_keys_stay(
    tmp_path: Path,
) -> None:
    conn = _v16_connect(tmp_path / "keys.db")
    _mark_close_on(conn)
    _seed_site(conn, name="wx-v17-site-h")

    removed_runtime_keys = [
        "verification_last_run_at",
        "verification_publish_hold_until",
        "import_rebuild_state",
    ]
    kept_runtime_keys = ["worker_started_at", "offline_close_copy_made_at"]
    for key in removed_runtime_keys + kept_runtime_keys:
        if key == DAILY_CLOSE_ON_KEY:
            continue
        set_runtime_state(conn, key, "2035-01-01T00:00:00Z")

    conn.execute(
        "INSERT INTO settings (key, value) VALUES ('verification_publish_hold', '1')"
    )
    # 'rolling_window_days' is already a v16-seed settings row (fixtures/
    # v16_seed.sql); it stands in for "other keys stay" without a second,
    # colliding insert of the same key.
    conn.commit()

    _run_migrate_v17(conn)

    for key in removed_runtime_keys:
        assert get_runtime_state(conn, key) is None, f"{key} must be deleted"
    for key in kept_runtime_keys:
        assert get_runtime_state(conn, key) is not None, f"{key} must survive"
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is not None, (
        "daily_close_on_at itself must never be deleted"
    )

    settings_rows = {row["key"] for row in conn.execute("SELECT key FROM settings")}
    assert "verification_publish_hold" not in settings_rows
    assert "rolling_window_days" in settings_rows


# ---------------------------------------------------------------------------
# W7 item 8: refusals, raised by the real open.
# ---------------------------------------------------------------------------


def _open_and_expect_refusal(db_path: Path, *, match: str) -> None:
    close_db()
    config.db_path = str(db_path)
    config.options_path = str(db_path.with_name("options.json"))
    with pytest.raises(RuntimeError, match=re.escape(match)):
        init_db(str(db_path))


@pytest.mark.parametrize("old_version", list(range(1, 16)))
def test_versions_1_to_15_are_refused_by_the_open(
    tmp_path: Path, old_version: int
) -> None:
    db_path = tmp_path / f"v{old_version}.db"
    conn = _v16_connect(db_path)
    conn.execute(f"PRAGMA user_version = {old_version}")
    conn.close()

    _open_and_expect_refusal(
        db_path,
        match=(
            f"database schema {old_version} is too old for this release;"
            " install 0.16.15 first"
        ),
    )

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == old_version, (
            "a refused open must not touch user_version"
        )
    finally:
        check.close()


def test_v16_without_the_close_key_is_refused_by_the_open(tmp_path: Path) -> None:
    db_path = tmp_path / "v16-no-close.db"
    conn = _v16_connect(db_path)
    # Deliberately no daily_close_on_at written.
    conn.close()

    _open_and_expect_refusal(db_path, match="daily_close_on_at")

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16
        for table in _DROPPED_TABLES:
            row = check.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            assert row is not None, f"{table} must still be present after the refusal"
    finally:
        check.close()


def test_a_newer_user_version_is_refused_by_the_open(tmp_path: Path) -> None:
    db_path = tmp_path / "v18.db"
    conn = _v16_connect(db_path)
    conn.execute("PRAGMA user_version = 18")
    conn.close()

    _open_and_expect_refusal(db_path, match="is newer than this app")


def test_a_site_with_an_unloadable_published_zone_is_refused_by_the_open(
    tmp_path: Path,
) -> None:
    """A site whose ``published`` generation carries a time zone that cannot
    load (e.g. ``Not/A_Zone``) must make the upgrade RAISE, naming the site
    and 'zone', rather than silently skip the site's hours. Today's
    ``_v17_date_map`` only logs a WARNING and keeps going on a bad zone --
    this test pins the production change the plan calls for and fails until
    it lands."""
    db_path = tmp_path / "bad-zone.db"
    conn = _v16_connect(db_path)
    _mark_close_on(conn)
    site_id = _seed_site(conn, timezone="Not/A_Zone", name="wx-v17-site-badzone")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gen_id = _generation_id(conn, site_id)
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=5.0,
    )
    conn.commit()
    conn.close()

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(db_path.with_name("options.json"))
    with pytest.raises(RuntimeError) as exc_info:
        init_db(str(db_path))
    message = str(exc_info.value)
    assert str(site_id) in message and "zone" in message, (
        f"the refusal must name the site id and 'zone': {message!r}"
    )

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16, (
            "a refused upgrade must leave user_version at 16"
        )
        for table in ("forecast_pairs", *_VERIFICATION_TABLES):
            row = check.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            assert row is not None, f"{table} must still exist after the refusal"
        pair_count = check.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0]
        assert pair_count == 1, "the refusal must leave the site's forecast_pairs row"
        history_count = check.execute(
            "SELECT COUNT(*) FROM forecast_history"
        ).fetchone()[0]
        assert history_count == 0, (
            "the refusal must leave forecast_history without the site's rows"
        )
    finally:
        check.close()


def test_a_forecast_pair_with_an_unparseable_valid_at_is_refused_by_the_open(
    tmp_path: Path,
) -> None:
    """A ``forecast_pairs`` row below the site's bound whose ``valid_at``
    cannot parse must make the upgrade RAISE, naming the site and
    'valid_at', rather than silently drop that hour. Today's
    ``_v17_date_map`` only counts it as unparsed and logs a WARNING -- this
    test pins the production change the plan calls for and fails until it
    lands. ``"0000-00-00T00:00:00Z"`` sorts textually BELOW the real bound
    (``"2035-..."``, month/day digits included), so the migration's
    below-bound SQL filter (plain TEXT ``<``) selects it, and
    ``datetime.fromisoformat`` still rejects month/day 00 as invalid."""
    db_path = tmp_path / "bad-valid-at.db"
    conn = _v16_connect(db_path)
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-site-badvalidat")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gen_id = _generation_id(conn, site_id)
    _insert_site_day(
        conn,
        site_id=site_id,
        local_date=date(2035, 6, 5),
        day_start_utc="2035-06-05T00:00:00Z",
        day_end_utc="2035-06-06T00:00:00Z",
        timezone="UTC",
    )
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="0000-00-00T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=5.0,
    )
    conn.commit()
    conn.close()

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(db_path.with_name("options.json"))
    with pytest.raises(RuntimeError) as exc_info:
        init_db(str(db_path))
    message = str(exc_info.value)
    assert str(site_id) in message and "valid_at" in message, (
        f"the refusal must name the site id and 'valid_at': {message!r}"
    )

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16, (
            "a refused upgrade must leave user_version at 16"
        )
        for table in ("forecast_pairs", *_VERIFICATION_TABLES):
            row = check.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            assert row is not None, f"{table} must still exist after the refusal"
        pair_count = check.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0]
        assert pair_count == 1, "the refusal must leave the site's forecast_pairs row"
        history_count = check.execute(
            "SELECT COUNT(*) FROM forecast_history"
        ).fetchone()[0]
        assert history_count == 0, (
            "the refusal must leave forecast_history without the site's rows"
        )
    finally:
        check.close()


# ---------------------------------------------------------------------------
# QA addendum: a stop landing mid-``v17_copy_close_pairs``, via SQLite's
# progress handler, rolls back the whole upgrade; the next open resumes it.
# ---------------------------------------------------------------------------


def test_stop_during_v17_copy_close_pairs_leaves_v16_untouched_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stop arriving through SQLite's progress handler in the middle of
    ``migrations._v17_copy_close_pairs``'s single INSERT ... SELECT: an
    interrupted INSERT rolls back the WHOLE ``migrate_v17`` SAVEPOINT (not
    just this one step), leaving the file at ``user_version`` 16 with
    ``forecast_pairs``/``close_pairs`` intact; the next open finishes the
    upgrade.

    50,000 ``close_pairs`` rows (``stop._PROGRESS_OPS`` is 10,000 VM
    instructions between two checks within one statement -- the same
    construction as ``test_compaction.py``'s 50,000-row stop test) keep the
    copy running long enough for the stop to land mid-statement, rather
    than being caught only at the next ``begin_step`` after it has already
    finished.
    """
    from wxverify.core import stop

    db_path = tmp_path / "v17-stop.db"
    conn = _v16_connect(db_path)
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-stop-site")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    rows = [
        (
            site_id,
            "2024-01-01",
            feed_id,
            "temperature",
            0,
            f"2024-01-01T{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}Z",
            24,
            5.0,
            4.7,
            "2035-01-01T00:00:00Z",
            None,
        )
        for sec in range(50_000)
    ]
    conn.executemany(
        "INSERT INTO close_pairs (site_id, local_date, feed_id, variable,"
        " day_ahead, valid_at, lead_hours, forecast, observed, issued_at,"
        " contributors) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    real_copy = migrations._v17_copy_close_pairs
    copy_raised = False

    def _interrupting_copy(c: sqlite3.Connection) -> None:
        nonlocal copy_raised
        stop.request_stop()
        try:
            real_copy(c)
        except Exception:
            copy_raised = True
            raise

    monkeypatch.setattr(migrations, "_v17_copy_close_pairs", _interrupting_copy)

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(db_path.with_name("options.json"))
    with pytest.raises(Exception) as exc_info:  # noqa: PT011
        init_db(str(db_path))
    assert stop.stopped_by(exc_info.value), (
        "the exception raised out of init_db() must be the one the stop"
        " caused, not some other failure masquerading as one"
    )
    assert copy_raised, (
        "the real v17_copy_close_pairs must itself have been interrupted"
        " mid-copy; a copy that ran to completion and was only caught at"
        " the next begin_step must not pass this test"
    )

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16, (
            "an interrupted v17 upgrade must leave user_version at 16"
        )
        close_pairs = check.execute("SELECT COUNT(*) FROM close_pairs").fetchone()[0]
        assert close_pairs == 50_000, (
            "an interrupted copy must leave every seeded close_pairs row"
            f" intact (got {close_pairs})"
        )
    finally:
        check.close()

    # The next start reruns and finishes the upgrade, with nothing patched.
    monkeypatch.setattr(migrations, "_v17_copy_close_pairs", real_copy)
    stop.reset_stop()
    close_db()
    reopened = init_db(str(db_path))
    try:
        assert (
            reopened._conn.execute(  # noqa: SLF001
                "PRAGMA user_version"
            ).fetchone()[0]
            == 17
        ), "the next start must complete the v17 upgrade"
        history_count = reopened._conn.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM forecast_history"
        ).fetchone()[0]
        assert history_count == 50_000, (
            "the resumed upgrade must copy every close_pairs row into"
            f" forecast_history (got {history_count})"
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# W7 item 9: user_version 0 builds the same schema as an upgraded 16 file.
# ---------------------------------------------------------------------------


def _schema_signature(conn: sqlite3.Connection) -> dict[str, object]:
    tables = sorted(
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%'"
        )
    )
    columns = {
        table: sorted(
            (row["name"], row["type"], row["notnull"], row["dflt_value"], row["pk"])
            for row in conn.execute(f"PRAGMA table_info('{table}')")
        )
        for table in tables
    }
    foreign_keys = {
        table: sorted(
            (
                row["table"],
                row["from"],
                row["to"],
                row["on_update"],
                row["on_delete"],
                row["match"],
            )
            for row in conn.execute(f"PRAGMA foreign_key_list('{table}')")
        )
        for table in tables
    }
    indexes = {
        table: sorted(
            (
                idx["name"],
                tuple(
                    r["name"]
                    for r in conn.execute(f"PRAGMA index_info('{idx['name']}')")
                ),
            )
            for idx in conn.execute(f"PRAGMA index_list('{table}')")
        )
        for table in tables
    }
    triggers = sorted(
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
    )
    return {
        "tables": tables,
        "columns": columns,
        "foreign_keys": foreign_keys,
        "indexes": indexes,
        "triggers": triggers,
    }


def test_fresh_user_version_0_matches_an_upgraded_v16_schema(tmp_path: Path) -> None:
    fresh = sqlite3.connect(str(tmp_path / "fresh.db"), isolation_level=None)
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys = ON")
    fresh.execute("BEGIN IMMEDIATE")
    migrations.run_migrations(fresh)
    fresh.commit()
    assert fresh.execute("PRAGMA user_version").fetchone()[0] == 17

    upgraded = _v16_connect(tmp_path / "upgraded.db")
    _mark_close_on(upgraded)
    upgraded.execute("BEGIN IMMEDIATE")
    migrations.run_migrations(upgraded)
    upgraded.commit()
    assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 17

    assert _schema_signature(fresh) == _schema_signature(upgraded), (
        "a fresh user_version-0 open and an upgraded v16 file must produce the"
        " identical table/column/index/trigger shape (raw `sql` text is"
        " deliberately excluded -- ALTER-added columns make it differ even"
        " when the resulting shape is identical)"
    )


# ---------------------------------------------------------------------------
# W7 item 10: a fresh file opens with the close on; the dashboard, win rate
# and history chart APIs answer empty without error.
# ---------------------------------------------------------------------------


def test_fresh_file_opens_with_close_on_and_empty_readers_do_not_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "fresh-open.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None

    async def _idle_worker(_db: object) -> None:
        import asyncio

        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")

    with TestClient(app) as client:
        token = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": token}
        site_resp = client.post(
            "/api/sites",
            headers=headers,
            json={
                "name": "wx-v17-fresh-site",
                "forecast_lat": 1.0,
                "forecast_lon": 2.0,
                "elevation_m": 3.0,
                "timezone": "UTC",
            },
        )
        assert site_resp.status_code == 200, site_resp.text
        site_id = site_resp.json()["id"]

        feed_resp = client.get("/api/feeds")
        assert feed_resp.status_code == 200, feed_resp.text
        feed_id = next(f["id"] for f in feed_resp.json() if f["source"] == "open-meteo")

        dashboard_resp = client.get(f"/dashboard?site={site_id}")
        assert dashboard_resp.status_code == 200, dashboard_resp.text

        winrate_resp = client.get(f"/api/winrate?site={site_id}&variable=temperature")
        assert winrate_resp.status_code == 200, winrate_resp.text
        assert winrate_resp.json() == []

        history_resp = client.get(
            f"/api/timeseries?site={site_id}&feed_id={feed_id}&variable=temperature"
        )
        assert history_resp.status_code == 200, history_resp.text
        assert history_resp.json()["valid_at"] == []

    close_db()

    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row
    try:
        assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is not None, (
            "a fresh open must write daily_close_on_at"
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# W7 items 11-12: stop handling.
# ---------------------------------------------------------------------------


def _patch_begin_step_to_stop_at(
    monkeypatch: pytest.MonkeyPatch, target_step: str
) -> None:
    import wxverify.core.stop as stop_module

    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == target_step:
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)


def test_a_stop_inside_the_savepoint_leaves_v16_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "stop-inside.db"
    conn = _v16_connect(db_path)
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-stop-a")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=_generation_id(conn, site_id),
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=5.0,
    )
    _insert_verification_chain(conn, site_id=site_id)
    conn.commit()
    conn.close()

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")

    import wxverify.core.stop as stop_module

    _patch_begin_step_to_stop_at(monkeypatch, "v17_convert_pre_close")

    with pytest.raises(stop_module.StopRequested):
        init_db(str(db_path))

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16
        for table in _DROPPED_TABLES:
            row = check.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            assert row is not None, f"{table} must still exist after the stop"
        pair_count = check.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0]
        assert pair_count == 1, "the stop must leave the original forecast_pairs row"
        history_count = check.execute(
            "SELECT COUNT(*) FROM forecast_history"
        ).fetchone()[0]
        assert history_count == 0, (
            "a stop before the conversion commits must leave forecast_history empty"
        )
    finally:
        check.close()
    close_db()


def test_a_stop_between_release_and_version_write_converges_on_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "stop-after-release.db"
    conn = _v16_connect(db_path)
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-stop-b")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=_generation_id(conn, site_id),
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=7.0,
    )
    conn.commit()
    conn.close()

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")

    import wxverify.core.stop as stop_module

    _patch_begin_step_to_stop_at(monkeypatch, "create_indexes")

    with pytest.raises(stop_module.StopRequested):
        init_db(str(db_path))

    check = sqlite3.connect(str(db_path))
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 16, (
            "the version write must not have happened yet"
        )
        row = check.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='forecast_pairs'"
        ).fetchone()
        assert row is None, "the drop step must already be committed"
        history_count = check.execute(
            "SELECT COUNT(*) FROM forecast_history"
        ).fetchone()[0]
        assert history_count == 1, "the conversion must already be committed"
    finally:
        check.close()

    monkeypatch.undo()
    close_db()
    config.db_path = str(db_path)
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        init_db(str(db_path))
    check2 = sqlite3.connect(str(db_path))
    try:
        assert check2.execute("PRAGMA user_version").fetchone()[0] == 17
    finally:
        check2.close()
    close_db()
    assert any("already converted" in r.getMessage() for r in caplog.records), (
        "the re-entry guard (forecast_pairs absent) must log 'already converted'"
    )


# ---------------------------------------------------------------------------
# W7's own logging requirement: one INFO line per v17 step with its row
# count and milliseconds, and PRAGMA secure_delete logged.
# ---------------------------------------------------------------------------


_STEP_NAMES = (
    "v17_date_map",
    "v17_convert_pre_close",
    "v17_copy_close_pairs",
    "v17_delete_removed_jobs_keys",
    "v17_drop_tables",
)


def test_one_info_line_per_step_with_rows_and_ms_plus_secure_delete(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _v16_connect(tmp_path / "logging.db")
    _mark_close_on(conn)
    site_id = _seed_site(conn, name="wx-v17-logging")
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_forecast_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=_generation_id(conn, site_id),
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        lead_hours=24,
        day_ahead=1,
        forecast=7.0,
    )
    conn.commit()

    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        _run_migrate_v17(conn)

    messages = [r.getMessage() for r in caplog.records]
    for step in _STEP_NAMES:
        matching = [m for m in messages if step in m]
        assert matching, f"no INFO line names step {step!r}: {messages}"
        assert any(re.search(r"\d", m) for m in matching), (
            f"the line for {step!r} must carry a number (rows and/or ms): {matching}"
        )
    assert any("secure_delete" in m for m in messages), (
        "PRAGMA secure_delete must be logged"
    )
