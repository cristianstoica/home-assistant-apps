"""Retired checks and the keep list (daily-close plan
2026-10-22-daily-close-alerts-and-pages.md §4, §13.2; test IDs RT2, RT3,
RT5-RT9).

Written before ``wxverify.close_monitor`` exists: most rows import it (or a
name ``monitor.py`` has not yet added) inside the test body, so the rest of
this file keeps collecting -- each then fails at its own import or at an
assertion against today's unchanged ``build_verdict``, never at collection.

Real SQLite throughout (``tests.helpers.asof_conn``), real migrations;
nothing in the code under test is mocked. Synthetic data only: stations
``ISTATION01``/``ISTATION02`` (via ``day_close_site``), a synthetic feed
source ``synthetic-rt``, zone ``UTC``, dates built from the fixture's ``D``.
"""

from __future__ import annotations

from datetime import timedelta

from tests.helpers import NOW, D, asof_conn, day_close_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.monitor import build_verdict

_RETIRED_IDS = (
    "obs_stale",
    "fetch_obs_live",
    "obs_station_history_failing",
    "wind_history",
)
_KEPT_PIPELINE_IDS = (
    "feed_stale",
    "fetch_feed_live",
    "problem_jobs",
    "obs_collection_delayed",
    "main_worker_liveness",
)
_CLOSE_ALERT_IDS = (
    "close_day_missed",
    "close_day_thin",
    "station_nothing_usable",
    "publication_late",
    "close_stage_failed",
    "provider_down",
    "daytime_download_missed",
)


def _condition_ids(verdict: dict[str, object]) -> list[str]:
    return [str(c["id"]) for c in verdict["conditions"]]  # type: ignore[index]


def _add_eligible_feed(conn) -> None:
    """Make the site's feed group non-empty, synthetic-src only."""
    conn.execute(
        "INSERT INTO feeds (source, model, enabled, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('synthetic-rt', 'synthetic-rt-model', 1, 1, 360, 168, 0)"
    )


def _trip_retired_checks(conn, site_id: int) -> None:
    """Seed everything RT2 needs each retired check to trip, close on (5.1).

    ``sites.last_obs_at`` stays NULL (older than 12h by definition);
    a parked station (``history_error_count`` > 0); a wind station-day that
    failed for good (``status='failed'``, ``attempts>=3``) within the last 7
    days; no completed ``fetch_obs``/``pair_and_score`` job (none seeded).
    """
    _add_eligible_feed(conn)
    conn.execute(
        "UPDATE stations SET history_error_count = 3 WHERE site_id = ?", (site_id,)
    )
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1", (site_id,)
    ).fetchone()
    assert row is not None
    station_id = int(row[0])
    failed_date = (D - timedelta(days=1)).isoformat()
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, updated_at)"
        " VALUES (?, ?, 'failed', 3, ?)",
        (station_id, failed_date, "2024-06-09T00:00:00.000Z"),
    )


def test_rt2_close_on_no_retired_rows() -> None:
    """RT2: close on, every retired check seeded to trip (M3, M4)."""
    conn = asof_conn()
    site_id = day_close_site(conn)
    _trip_retired_checks(conn, site_id)

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=True,
        db_enabled=True,
        now=NOW,
        export_sweeper_dead=None,
    )
    ids = _condition_ids(verdict)
    for retired_id in _RETIRED_IDS:
        assert retired_id not in ids, (
            f"M3/M4 -> at this assertion: correct = {retired_id!r} absent from"
            f" the close-on verdict, mutant = present in {ids!r}"
        )


def test_rt3_close_on_pipeline_disabled_skipped_list() -> None:
    """RT3: close on; pipeline_enabled false (M5)."""
    conn = asof_conn()
    day_close_site(conn)

    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=True,
        db_enabled=True,
        now=NOW,
        export_sweeper_dead=None,
    )
    skipped = [
        str(c["id"])
        for c in verdict["conditions"]
        if c["group"] == "pipeline"  # type: ignore[index]
    ]
    expected = (
        list(_KEPT_PIPELINE_IDS) + ["station_silent_today"] + list(_CLOSE_ALERT_IDS)
    )
    assert skipped == expected, (
        f"M5 -> at this assertion: correct = {expected!r}, mutant ="
        f" {skipped!r} (the close-on skipped list keeps today's kept ids)"
    )
    for retired_id in _RETIRED_IDS:
        assert retired_id not in skipped, (
            f"M5 -> at this assertion: correct = {retired_id!r} absent,"
            f" mutant = present in {skipped!r}"
        )


def test_rt5_close_on_db_error_in_close_alerts_read() -> None:
    """RT5: close on; the test drops a table a close alert reads (M7)."""
    conn = asof_conn()
    day_close_site(conn)
    conn.execute("DROP TABLE close_publications")

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=True,
        db_enabled=True,
        now=NOW,
        export_sweeper_dead=None,
    )
    db_readable = next(c for c in verdict["conditions"] if c["id"] == "db_readable")  # type: ignore[index]
    assert db_readable["ok"] is False, (
        "M7 -> at this assertion: correct = db_readable ok=False (the"
        " dropped table's sqlite3.Error reported as any pipeline read),"
        f" mutant = {db_readable!r}"
    )


def test_rt6_close_alerts_unexpected_error_isolated() -> None:
    """RT6: close_alerts replaced by a stand-in that raises ValueError (M8).

    The one row that replaces code under test, since no seed reaches an
    unknown bug (plan 13.2).
    """
    import wxverify.monitor as monitor_module

    def _raise(conn, now):  # noqa: ANN001, ARG001
        raise ValueError("boom")

    conn = asof_conn()
    day_close_site(conn)

    original = getattr(monitor_module, "close_alerts", None)
    monitor_module.close_alerts = _raise  # type: ignore[attr-defined]
    try:
        verdict = build_verdict(
            conn,
            pipeline_enabled=True,
            budget_enabled=True,
            db_enabled=True,
            now=NOW,
            export_sweeper_dead=None,
        )
    finally:
        if original is None:
            del monitor_module.close_alerts  # type: ignore[attr-defined]
        else:
            monitor_module.close_alerts = original  # type: ignore[attr-defined]

    ids = _condition_ids(verdict)
    for kept_id in _KEPT_PIPELINE_IDS:
        assert kept_id in ids, (
            f"M8 -> at this assertion: correct = {kept_id!r} present despite"
            f" close_alerts raising, mutant = absent from {ids!r}"
        )
    unexpected = next(
        (c for c in verdict["conditions"] if c["id"] == "unexpected_error"),  # type: ignore[index]
        None,
    )
    assert unexpected is not None and str(unexpected["detail"]).startswith(
        "close alerts: ValueError"
    ), (
        "M8 -> at this assertion: correct = one unexpected_error row whose"
        " detail starts 'close alerts: ValueError', mutant ="
        f" {unexpected!r}"
    )
    assert not any(cid in _CLOSE_ALERT_IDS for cid in ids), (
        f"M8 -> at this assertion: correct = no close_alerts id, mutant ="
        f" one present in {ids!r}"
    )


def test_rt7_close_on_startup_grace() -> None:
    """RT7: close on; alert 1 seeded to trip; the worker started 5 minutes
    before `now` (M9)."""
    conn = asof_conn()
    day_close_site(conn)
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('worker_started_at', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (isoformat_utc(NOW - timedelta(minutes=5)),),
    )

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=True,
        db_enabled=True,
        now=NOW,
        export_sweeper_dead=None,
    )
    new_pipeline_ids = set(_CLOSE_ALERT_IDS) | {"station_silent_today"}
    ids = _condition_ids(verdict)
    for new_id in new_pipeline_ids:
        assert new_id in ids, (
            "M9 -> at this assertion: correct = every new pipeline row id"
            f" present, mutant = {new_id!r} absent from {ids!r}"
        )
    new_pipeline_rows = [
        c
        for c in verdict["conditions"]  # type: ignore[index]
        if c["id"] in new_pipeline_ids
    ]
    for cond in new_pipeline_rows:
        assert cond["ok"] is True and cond.get("detail") is None, (
            "M9 -> at this assertion: correct = ok=True, no detail (the"
            " 10-minute start-up grace), mutant ="
            f" {cond!r}"
        )


def test_rt8_unreadable_last_obstime() -> None:
    """RT8: one station's offline poll row with `last_obstime` corrupted
    (M10)."""
    conn = asof_conn()
    site_id = day_close_site(conn)
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1", (site_id,)
    ).fetchone()
    assert row is not None
    station_id = int(row[0])
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, last_obstime, last_poll_at, next_poll_at,"
        "  updated_at)"
        " VALUES (?, 'offline', ?, ?, ?, ?)",
        (
            station_id,
            "2024-06-10T06:00:00Z",
            "2024-06-10T06:00:00Z",
            "2024-06-11T06:00:00Z",
            "2024-06-10T06:00:00Z",
        ),
    )
    conn.execute(
        "UPDATE station_poll_state SET last_obstime = 'not-a-time'"
        " WHERE station_id = ?",
        (station_id,),
    )

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=True,
        db_enabled=True,
        now=NOW,
        export_sweeper_dead=None,
    )
    ids = _condition_ids(verdict)
    for kept_id in _KEPT_PIPELINE_IDS:
        assert kept_id in ids, (
            f"M10 -> at this assertion: correct = {kept_id!r} present as with"
            f" a valid value, mutant = absent from {ids!r}"
        )
    silent = next(c for c in verdict["conditions"] if c["id"] == "station_silent_today")  # type: ignore[index]
    assert silent.get("detail") == "station ISTATION01: unreadable last_obstime", (
        "M10 -> at this assertion: correct = 'station ISTATION01: unreadable"
        f" last_obstime', mutant = {silent.get('detail')!r} (the stamp helper"
        " raised instead of reporting unreadable)"
    )
    assert not any(c["id"] == "unexpected_error" for c in verdict["conditions"]), (  # type: ignore[index]
        "M10 -> at this assertion: correct = no unexpected_error row, mutant"
        " = one present (the stamp helper raised)"
    )


def test_rt9_runtime_state_dropped_every_group_off() -> None:
    """RT9: migrated in-memory db, `runtime_state` dropped, every group off.
    With every group disabled, the pipeline-off path reads nothing (the
    switch read it used to make is gone with the always-on close) and
    neither does the db-off path, so the dropped table causes no failure:
    the pipeline group's skipped list is still appended, `db_readable` is
    its own skipped row, and the verdict stays 'ok' -- a broken
    `runtime_state` table is reported only by a group that actually reads
    it, not pinned here as a regression."""
    conn = asof_conn()
    conn.execute("DROP TABLE runtime_state")

    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=NOW,
        export_sweeper_dead=None,
    )
    pipeline_rows = [c for c in verdict["conditions"] if c["group"] == "pipeline"]  # type: ignore[index]
    assert pipeline_rows and all(c["skipped"] for c in pipeline_rows), (  # type: ignore[index]
        "at this assertion: correct = every pipeline row present and"
        f" skipped, mutant = {pipeline_rows!r}"
    )
    db_readable = next(c for c in verdict["conditions"] if c["id"] == "db_readable")  # type: ignore[index]
    assert (
        db_readable["ok"] is True
        and db_readable["skipped"] is True
        and db_readable["severity"] == "critical"
    ), (
        "at this assertion: correct = db_readable skipped (nothing reads"
        f" the dropped table), mutant = {db_readable!r}"
    )
    sweeper = next(
        c
        for c in verdict["conditions"]
        if c["id"] == "export_sweeper_dead"  # type: ignore[index]
    )
    assert sweeper["ok"] is True, (
        f"at this assertion: correct = export_sweeper_dead present and ok,"
        f" mutant = {sweeper!r}"
    )
    assert verdict["overall"] == "ok", (
        "at this assertion: correct = overall 'ok' (every group disabled"
        " reads nothing, so the dropped table causes no failure), mutant"
        f" = {verdict['overall']!r}"
    )
