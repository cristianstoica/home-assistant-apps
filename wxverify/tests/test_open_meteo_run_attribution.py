"""Open-Meteo run attribution and per-model fetch cadence -- Item E oracles.

Covers ``wxverify.feeds.open_meteo._snap_run``/``_run_shape`` (per-model run
boundary + availability lag, no silent defaults), ``persist_fetch_result``'s
newest-write-wins behaviour on the one row per hour ``idx_samples_slot``
keeps (rain release, Part B), ``config.OPEN_METEO_FETCH_INTERVAL_MINUTES``
and ``db.migrations.correct_open_meteo_fetch_intervals`` (the fetch-interval
counterpart to the horizon correction in
``tests/test_open_meteo_horizon_correction.py``), and the scheduler's cadence
arithmetic for a 12-hourly model.

E-T9 (three-mapping key-set parity) is deliberately NOT duplicated here: it
extends C-T7 in ``tests/test_open_meteo_horizon_correction.py`` in place, per
the plan's explicit instruction that C-T7 is updated, not copied.

All fixtures synthetic: station ids ``ISTATION01`` onward, site timezone
``UTC``, coordinates ``0.0``/``0.0``, any host literal ``192.0.2.10``.
Databases are built with ``tests.helpers.asof_conn`` (a real fully-migrated
in-memory datastore, no mocks) except where a pre-migration state must be
hand-built, which mirrors ``tests/test_open_meteo_horizon_correction.py``'s
``_bare_db``/``_insert_feed`` pattern. ``_snap_run`` is driven through its
explicit ``fetch_time`` argument, never a patched clock, except E-T7, whose
whole point is to prove the ambient clock is irrelevant when ``fetch_time``
is supplied.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta

import pytest

from tests.helpers import asof_conn, asof_make_real_feed
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.core.timeutil import parse_utc
from wxverify.db.migrations import (
    OPEN_METEO_INTERVAL_CORRECTION_KEY,
    correct_open_meteo_fetch_intervals,
    create_schema,
    run_migrations,
)
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.feeds.open_meteo import (
    RUN_AVAILABILITY_LAG_MINUTES,
    RUN_CADENCE_HOURS,
    _run_shape,  # noqa: SLF001
    _samples_from_hourly,  # noqa: SLF001
    _snap_run,  # noqa: SLF001
)
from wxverify.feeds.seam import FetchResult, NormalizedSample
from wxverify.forecast.data import samples_fingerprint
from wxverify.worker.round import enqueue_due_daytime_downloads

_OPEN_METEO_MODELS = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "meteofrance_arpege_world",
    "jma_gsm",
    "ukmo_global_deterministic_10km",
)

_SIX_HOURLY_MODELS = tuple(m for m in _OPEN_METEO_MODELS if m != "gem_global")

_EXPECTED_INTERVALS: dict[str, int] = {
    "ecmwf_ifs": 360,
    "gfs_global": 360,
    "icon_global": 360,
    "gem_global": 720,
    "meteofrance_arpege_world": 360,
    "jma_gsm": 360,
    "ukmo_global_deterministic_10km": 360,
}


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 0.0, 0.0, 0.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# Bare-schema fixture helpers for pre-migration fetch-interval states,
# mirroring tests/test_open_meteo_horizon_correction.py's _bare_db pattern.
# ---------------------------------------------------------------------------


def _bare_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _insert_feed(
    conn: sqlite3.Connection,
    *,
    source: str,
    model: str,
    fetch_interval_minutes: int,
    max_lead_hours: int = 168,
    enabled: int = 1,
    disabled_reason: str | None = None,
    default_subscribed: int = 1,
    is_virtual: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO feeds
            (source, model, enabled, disabled_reason, default_subscribed,
             fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source,
            model,
            enabled,
            disabled_reason,
            default_subscribed,
            fetch_interval_minutes,
            max_lead_hours,
            is_virtual,
        ),
    )


def _seed_seven_at_360(conn: sqlite3.Connection) -> None:
    for model in _OPEN_METEO_MODELS:
        _insert_feed(conn, source="open-meteo", model=model, fetch_interval_minutes=360)


def _interval(conn: sqlite3.Connection, *, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT fetch_interval_minutes FROM feeds WHERE source = ? AND model = ?",
        (source, model),
    ).fetchone()
    assert row is not None, f"expected a ({source}, {model}) feed row"
    return int(row["fetch_interval_minutes"])


# ---------------------------------------------------------------------------
# E-T1 -- gem_global's 12-hour publication boundaries.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fetch_time,expected_issued_at",
    [
        # Exact 00Z boundary (lagged == 00:00:00 exactly).
        ("2026-06-02T01:30:00Z", "2026-06-02T00:00:00Z"),
        # Just before the 00Z boundary -- floors to the PRECEDING 12Z run.
        ("2026-06-02T01:29:59Z", "2026-06-01T12:00:00Z"),
        # Just before the 12Z boundary -- floors to 00Z, same day.
        ("2026-06-02T13:29:59Z", "2026-06-02T00:00:00Z"),
        # Exact 12Z boundary.
        ("2026-06-02T13:30:00Z", "2026-06-02T12:00:00Z"),
        # The defect-discriminating case (E.7): under the corrected 12-hour
        # cadence, a fetch at 07:30Z (lagged 06:00Z) floors DOWN to 00:00Z --
        # the old flat-6-hour cadence would instead floor this to 06:00Z,
        # which is exactly the duplicate-label defect Item E fixes.
        ("2026-06-02T07:30:00Z", "2026-06-02T00:00:00Z"),
    ],
)
def test_e_t1_gem_global_floors_to_the_12_hour_boundary(
    fetch_time: str, expected_issued_at: str
) -> None:
    assert _snap_run("gem_global", fetch_time) == expected_issued_at


# ---------------------------------------------------------------------------
# E-T2 -- the other six models still floor to their 6-hour boundary.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", _SIX_HOURLY_MODELS)
def test_e_t2_six_hourly_models_unchanged_by_the_cadence_correction(
    model: str,
) -> None:
    # Same fetch time as E-T1's discriminating case: lagged 06:00Z floors to
    # 06:00Z under a 6-hour cadence, vs. gem_global's 00:00Z under 12.
    assert _snap_run(model, "2026-06-02T07:30:00Z") == "2026-06-02T06:00:00Z"


# ---------------------------------------------------------------------------
# E-T3 -- a delayed response inside one 12-hour window is the same run.
# ---------------------------------------------------------------------------


def test_e_t3_delayed_gem_global_fetch_in_the_same_window_is_the_same_run() -> None:
    first = _snap_run("gem_global", "2026-06-02T01:30:00Z")
    second = _snap_run("gem_global", "2026-06-02T11:00:00Z")
    assert first == second == "2026-06-02T00:00:00Z"
    assert f"gem_global:{first}" == f"gem_global:{second}"


# ---------------------------------------------------------------------------
# E-T4 -- duplicate ingestion: the newest write wins (rain release, Part B:
# forecast_samples keeps the newest value of each hour).
# ---------------------------------------------------------------------------


def test_e_t4_duplicate_forward_fetch_keeps_the_newest_value() -> None:
    conn = asof_conn()
    site_id = _make_site(conn, "ISTATION01")
    feed_id = asof_make_real_feed(conn, "gem_global")
    conn.commit()

    key_issued_at = "2026-06-02T00:00:00Z"
    key_valid_at = "2026-06-02T06:00:00Z"
    first = NormalizedSample(
        model="gem_global",
        variable="temperature",
        issued_at=key_issued_at,
        valid_at=key_valid_at,
        lead_hours=6,
        value=10.0,
        source_raw="10.0",
        model_run_id=f"gem_global:{key_issued_at}",
    )
    outcome1 = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[first]),
        fetched_at="2026-06-02T07:30:00Z",
    )
    conn.commit()
    assert outcome1.inserted_count == 1
    assert outcome1.usable_sample_count == 1

    # A second poll inside the same 12-hour window: same run key, a
    # DIFFERENT value, which replaces the first (one row per hour).
    second = first.model_copy(update={"value": 99.9, "source_raw": "99.9"})
    outcome2 = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[second]),
        fetched_at="2026-06-02T11:00:00Z",
    )
    conn.commit()

    assert outcome2.inserted_count == 1
    assert outcome2.usable_sample_count == 1

    rows = conn.execute(
        """
        SELECT value, fetched_at FROM forecast_samples
        WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'
          AND valid_at = ?
        """,
        (site_id, feed_id, key_valid_at),
    ).fetchall()
    assert [(r["value"], r["fetched_at"]) for r in rows] == [
        (99.9, "2026-06-02T11:00:00Z")
    ]

    # An identical third poll writes nothing.
    outcome3 = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[second]),
        fetched_at="2026-06-02T11:30:00Z",
    )
    conn.commit()
    assert outcome3.inserted_count == 0
    assert outcome3.usable_sample_count == 1

    state = conn.execute(
        "SELECT last_run_at, last_error, error_count"
        " FROM site_feed_state WHERE site_id = ? AND feed_id = ?",
        (site_id, feed_id),
    ).fetchone()
    assert state is not None
    # The feed still records a successful run: usable > 0 even though
    # inserted == 0, so _clear_feed_state runs, not _stamp_no_op.
    assert state["last_run_at"] == "2026-06-02T11:30:00Z"
    assert state["last_error"] is None
    assert state["error_count"] == 0


# ---------------------------------------------------------------------------
# E-T5 -- a revised payload for the same run: the revision is stored.
# ---------------------------------------------------------------------------


def test_e_t5_revised_payload_for_the_same_run_is_stored() -> None:
    """A provider revision to an already-fetched run's values is no
    longer silently dropped: the revision replaces every non-key value
    the hour stored, and its ``fetched_at``.
    """
    conn = asof_conn()
    site_id = _make_site(conn, "ISTATION01")
    feed_id = asof_make_real_feed(conn, "gem_global")
    conn.commit()

    key_issued_at = "2026-06-02T00:00:00Z"
    key_valid_at = "2026-06-02T06:00:00Z"
    original = NormalizedSample(
        model="gem_global",
        variable="temperature",
        issued_at=key_issued_at,
        valid_at=key_valid_at,
        lead_hours=6,
        value=10.0,
        source_raw="10.0",
        model_run_id=f"gem_global:{key_issued_at}",
    )
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[original]),
        fetched_at="2026-06-02T07:30:00Z",
    )
    conn.commit()

    # A revision that changes every non-key value the payload carries.
    revised = original.model_copy(
        update={
            "value": -5.0,
            "source_raw": "-5.0 (revised)",
        }
    )
    outcome = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[revised]),
        fetched_at="2026-06-02T11:45:00Z",
    )
    conn.commit()

    assert outcome.inserted_count == 1
    row = conn.execute(
        "SELECT value, source_raw, fetched_at FROM forecast_samples"
        " WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'"
        " AND issued_at = ? AND valid_at = ?",
        (site_id, feed_id, key_issued_at, key_valid_at),
    ).fetchone()
    assert row is not None
    assert row["value"] == -5.0
    assert row["source_raw"] == "-5.0 (revised)"
    assert row["fetched_at"] == "2026-06-02T11:45:00Z"


# ---------------------------------------------------------------------------
# E-T7 -- issuance is provably clock-only: no other input matters.
# ---------------------------------------------------------------------------


def test_e_t7_snap_run_depends_only_on_its_explicit_fetch_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_fetch_time = "2026-06-02T07:30:00Z"

    import wxverify.feeds.open_meteo as open_meteo_module

    monkeypatch.setattr(
        open_meteo_module, "utc_now", lambda: parse_utc("1999-01-01T00:00:00Z")
    )
    first = _snap_run("gem_global", fixed_fetch_time)

    monkeypatch.setattr(
        open_meteo_module, "utc_now", lambda: parse_utc("2099-12-31T23:59:59Z")
    )
    second = _snap_run("gem_global", fixed_fetch_time)

    assert first == second == "2026-06-02T00:00:00Z"


# ---------------------------------------------------------------------------
# E-T8 -- an unkeyed model fails loudly, names itself, never floors to 6h.
# ---------------------------------------------------------------------------


def test_e_t8_unknown_model_raises_and_names_itself() -> None:
    with pytest.raises(ValueError, match="not_a_model"):
        _snap_run("not_a_model", "2026-06-02T07:30:00Z")


# E-T9 -- three-mapping key-set parity. NOT duplicated here: it extends C-T7
# in tests/test_open_meteo_horizon_correction.py (updated in this commit).


# ---------------------------------------------------------------------------
# E-T10 -- one poll per run for every model except a named, deliberate
# subsample, pinned as arithmetic on the mappings.
# ---------------------------------------------------------------------------

# Models deliberately polled at k times their run cadence (owner decision
# 2026-10-02). They sample roughly every k-th run, with no guarantee of which
# runs.
_SUBSAMPLED_RUNS_PER_POLL: dict[str, int] = {"icon_eu": 2}


def test_e_t10_fetch_interval_matches_cadence_except_subsampled() -> None:
    from wxverify import config

    assert set(_SUBSAMPLED_RUNS_PER_POLL) <= set(RUN_CADENCE_HOURS)
    assert all(k >= 2 for k in _SUBSAMPLED_RUNS_PER_POLL.values())
    for model in RUN_CADENCE_HOURS:
        k = _SUBSAMPLED_RUNS_PER_POLL.get(model, 1)
        assert (
            config.OPEN_METEO_FETCH_INTERVAL_MINUTES[model]
            == RUN_CADENCE_HOURS[model] * 60 * k
        ), model


# ---------------------------------------------------------------------------
# E-T11 -- interval seed, fresh database.
# ---------------------------------------------------------------------------


def test_e_t11_fresh_database_seeds_interval_per_model() -> None:
    conn = asof_conn()
    for model, minutes in _EXPECTED_INTERVALS.items():
        assert _interval(conn, source="open-meteo", model=model) == minutes, model
    assert _interval(conn, source="open-meteo", model="gem_global") == 720
    for model in _SIX_HOURLY_MODELS:
        assert _interval(conn, source="open-meteo", model=model) == 360, model


# ---------------------------------------------------------------------------
# E-T12 -- interval migration, upgraded database.
# ---------------------------------------------------------------------------


def test_e_t12_existing_database_all_at_360_corrected_by_migration() -> None:
    conn = _bare_db()
    _seed_seven_at_360(conn)
    run_migrations(conn)
    for model, minutes in _EXPECTED_INTERVALS.items():
        assert _interval(conn, source="open-meteo", model=model) == minutes, model

    before = {
        model: _interval(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }
    run_migrations(conn)
    after = {
        model: _interval(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }
    assert after == before


# ---------------------------------------------------------------------------
# E-T13 -- operator intent preserved.
# ---------------------------------------------------------------------------


def test_e_t13_operator_set_interval_survives_the_correction() -> None:
    conn = _bare_db()
    _seed_seven_at_360(conn)
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 500"
        " WHERE source = 'open-meteo' AND model = 'gem_global'"
    )

    correct_open_meteo_fetch_intervals(conn)

    assert _interval(conn, source="open-meteo", model="gem_global") == 500
    for model, minutes in _EXPECTED_INTERVALS.items():
        if model == "gem_global":
            continue
        assert _interval(conn, source="open-meteo", model=model) == minutes, model


def _operator_columns(
    conn: sqlite3.Connection, *, source: str, model: str
) -> tuple[int, str | None, int]:
    row = conn.execute(
        "SELECT enabled, disabled_reason, default_subscribed"
        " FROM feeds WHERE source = ? AND model = ?",
        (source, model),
    ).fetchone()
    assert row is not None, f"expected a ({source}, {model}) feed row"
    return (
        int(row["enabled"]),
        None if row["disabled_reason"] is None else str(row["disabled_reason"]),
        int(row["default_subscribed"]),
    )


def test_e_t13_migration_scope_foreign_source_same_model_name_untouched() -> None:
    """Migration scope, (b): the correction's ``WHERE`` clause is scoped by
    ``source = 'open-meteo'``, not by ``model`` alone. A feed from a
    DIFFERENT source that happens to share an Open-Meteo model name (e.g. an
    ``example-src`` feed also called ``gem_global``) must not be touched by
    a correction keyed on model name. Also confirms the correction leaves
    the operator-owned columns (``enabled``, ``disabled_reason``,
    ``default_subscribed``) unchanged, both on the real open-meteo rows and
    on the foreign ``example-src`` row.
    """
    conn = _bare_db()
    _seed_seven_at_360(conn)
    conn.execute(
        "UPDATE feeds SET enabled = 0, disabled_reason = 'operator paused',"
        " default_subscribed = 0 WHERE source = 'open-meteo'"
    )
    _insert_feed(
        conn,
        source="example-src",
        model="gem_global",
        fetch_interval_minutes=360,
        enabled=0,
        disabled_reason="operator paused",
        default_subscribed=0,
    )

    before = {
        model: _operator_columns(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }

    correct_open_meteo_fetch_intervals(conn)

    assert _interval(conn, source="example-src", model="gem_global") == 360
    for model, minutes in _EXPECTED_INTERVALS.items():
        assert _interval(conn, source="open-meteo", model=model) == minutes, model

    after = {
        model: _operator_columns(conn, source="open-meteo", model=model)
        for model in _OPEN_METEO_MODELS
    }
    assert after == before
    assert _operator_columns(conn, source="example-src", model="gem_global") == (
        0,
        "operator paused",
        0,
    )


def test_e_t13_migration_scope_alone_never_moves_user_version() -> None:
    """Migration scope, (c): mirrors
    ``tests/test_open_meteo_horizon_correction.py``'s
    ``test_correction_alone_never_moves_user_version``.
    ``correct_open_meteo_fetch_intervals`` is a data fix and must leave
    ``PRAGMA user_version`` exactly where it found it. Driven by a DIRECT
    call, not through ``run_migrations`` (which writes ``PRAGMA
    user_version`` unconditionally on its way out and would mask a version
    write added inside the correction). The sentinel 99 is deliberately not
    a real schema version.
    """
    conn = _bare_db()
    conn.execute("PRAGMA user_version = 99")
    _seed_seven_at_360(conn)

    correct_open_meteo_fetch_intervals(conn)

    row = conn.execute("PRAGMA user_version").fetchone()
    assert int(row[0]) == 99


# ---------------------------------------------------------------------------
# E-T14 -- marker is the gate; statement order is the crash guard.
# ---------------------------------------------------------------------------


def test_e_t14_marker_present_blocks_recorrection_of_a_reset_row() -> None:
    conn = _bare_db()
    _seed_seven_at_360(conn)
    set_runtime_state(conn, OPEN_METEO_INTERVAL_CORRECTION_KEY, "applied")

    correct_open_meteo_fetch_intervals(conn)

    for model in _OPEN_METEO_MODELS:
        assert _interval(conn, source="open-meteo", model=model) == 360, model


def test_e_t14_partial_prior_correction_converges_with_no_double_application() -> None:
    conn = _bare_db()
    _seed_seven_at_360(conn)
    # Simulates a crash after the gem_global UPDATE ran but before the
    # marker write.
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 720"
        " WHERE source = 'open-meteo' AND model = 'gem_global'"
        " AND fetch_interval_minutes = 360"
    )
    assert get_runtime_state(conn, OPEN_METEO_INTERVAL_CORRECTION_KEY) is None

    correct_open_meteo_fetch_intervals(conn)

    for model, minutes in _EXPECTED_INTERVALS.items():
        assert _interval(conn, source="open-meteo", model=model) == minutes, model
    assert get_runtime_state(conn, OPEN_METEO_INTERVAL_CORRECTION_KEY) == "applied"


# ---------------------------------------------------------------------------
# E-T15 -- correct lead calculation end to end.
# ---------------------------------------------------------------------------


def test_e_t15_lead_hours_end_to_end_against_the_12_hour_label() -> None:
    fetch_time = "2026-06-02T07:30:00Z"
    issued_at = _snap_run("gem_global", fetch_time)
    assert issued_at == "2026-06-02T00:00:00Z"

    hours = [f"2026-06-02T{h:02d}:00" for h in range(24)]
    values = [10.0 + i * 0.1 for i in range(24)]
    data = {"hourly": {"time": hours, "temperature_2m": values}}

    samples = _samples_from_hourly("gem_global", issued_at, data)

    issued_dt = parse_utc(issued_at)
    valid_ats = set()
    for sample in samples:
        valid_dt = parse_utc(sample.valid_at)
        expected_lead = int((valid_dt - issued_dt).total_seconds() // 3600)
        assert sample.lead_hours == expected_lead
        assert expected_lead >= 1
        valid_ats.add(sample.valid_at)

    # Hour 00 (lead 0, same as issued_at) is skipped by the lead < 1 guard.
    assert "2026-06-02T00:00:00Z" not in valid_ats
    assert len(samples) == 23


# ---------------------------------------------------------------------------
# E-T16 (plan C3-T18) -- the slot rule (plan §5.1): a 720-minute-cadence
# feed's only daytime slot is the round's local time (00:10 default) plus
# one interval, 12:10; before that instant the slot is not due, at or after
# it the slot is due exactly once. Catches the slot arithmetic being wired
# into catchup only and never reaching the scheduler.
# ---------------------------------------------------------------------------


def test_e_t16_scheduling_arithmetic_at_the_690_minute_lead_boundary() -> None:
    conn = asof_conn()
    _make_site(conn, "ISTATION01")
    cur = conn.execute(
        """
        INSERT INTO feeds
            (source, model, enabled, default_subscribed, fetch_interval_minutes,
             max_lead_hours, is_virtual)
        VALUES ('example-src', 'gem_global', 1, 1, 720, 168, 0)
        """
    )
    assert cur.lastrowid is not None
    feed_id = int(cur.lastrowid)
    conn.commit()

    slot_instant = parse_utc("2026-06-02T12:10:00Z")

    # Not yet due: one second before the 12:10 slot.
    enqueue_due_daytime_downloads(conn, now_utc=slot_instant - timedelta(seconds=1))
    conn.commit()
    # asof_conn() seeds the eight default open-meteo feeds too, each
    # never-run and therefore unconditionally due -- scope to THIS feed's
    # job_key ("fetch:<feed_id>") so those siblings' jobs cannot mask the
    # assertion.
    jobs_before = conn.execute(
        "SELECT * FROM jobs WHERE type = 'fetch_feed' AND job_key = ?",
        (f"fetch:{feed_id}",),
    ).fetchall()
    assert jobs_before == []

    # Due: exactly at the 12:10 slot instant.
    enqueue_due_daytime_downloads(conn, now_utc=slot_instant)
    conn.commit()
    jobs_due = conn.execute(
        "SELECT * FROM jobs WHERE type = 'fetch_feed' AND job_key = ?",
        (f"fetch:{feed_id}",),
    ).fetchall()
    assert len(jobs_due) == 1
    payload = json.loads(str(jobs_due[0]["payload"]))
    assert payload["feed_id"] == feed_id


# ---------------------------------------------------------------------------
# E-T18 -- the availability-lag policy, asserted on values, not keys.
# ---------------------------------------------------------------------------


def test_e_t18_availability_lag_is_a_flat_90_minutes_for_every_model() -> None:
    assert set(RUN_AVAILABILITY_LAG_MINUTES.values()) == {90}
    for model in RUN_CADENCE_HOURS:
        assert _run_shape(model) == (RUN_CADENCE_HOURS[model], 90), model


# ---------------------------------------------------------------------------
# E-T19 -- the write is per sample, not per response: a later same-label
# fetch still inserts previously-absent keys alongside an unchanged
# already-stored one, which it leaves as it is (rain release, Part B: an
# hour is written only when its value differs).
# ---------------------------------------------------------------------------


def test_e_t19_same_label_refetch_inserts_only_the_new_key() -> None:
    conn = asof_conn()
    site_id = _make_site(conn, "ISTATION01")
    feed_id = asof_make_real_feed(conn, "gem_global")
    conn.commit()

    issued_at = "2026-06-02T00:00:00Z"
    shared_valid_at = "2026-06-02T06:00:00Z"
    later_valid_at = "2026-06-02T07:00:00Z"

    seed = NormalizedSample(
        model="gem_global",
        variable="temperature",
        issued_at=issued_at,
        valid_at=shared_valid_at,
        lead_hours=6,
        value=10.0,
        source_raw="10.0",
        model_run_id=f"gem_global:{issued_at}",
    )
    outcome0 = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[seed]),
        fetched_at="2026-06-02T07:30:00Z",
    )
    conn.commit()
    assert outcome0.inserted_count == 1

    before = samples_fingerprint(conn, site_id=site_id)

    # (a) same (variable, issued_at, valid_at) key, same value -- an
    # unchanged hour, which writes nothing (E-T4's third poll).
    unchanged = seed.model_copy()
    # (b) one extra, later valid_at not yet stored under this run -- a
    # distinct key that must still be inserted, proving the write is
    # per sample and not a whole-response decision once any key collides.
    new_key = seed.model_copy(
        update={
            "valid_at": later_valid_at,
            "lead_hours": 7,
            "value": 11.0,
            "source_raw": "11.0",
        }
    )
    outcome1 = persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feed_id,
        result=FetchResult(samples=[unchanged, new_key]),
        fetched_at="2026-06-02T11:00:00Z",
    )
    conn.commit()

    assert outcome1.inserted_count == 1

    after = samples_fingerprint(conn, site_id=site_id)
    assert int(after) > int(before)

    row_a = conn.execute(
        """
        SELECT value FROM forecast_samples
        WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'
          AND issued_at = ? AND valid_at = ?
        """,
        (site_id, feed_id, issued_at, shared_valid_at),
    ).fetchone()
    assert row_a is not None
    assert row_a["value"] == 10.0

    row_b = conn.execute(
        """
        SELECT value FROM forecast_samples
        WHERE site_id = ? AND feed_id = ? AND variable = 'temperature'
          AND issued_at = ? AND valid_at = ?
        """,
        (site_id, feed_id, issued_at, later_valid_at),
    ).fetchone()
    assert row_b is not None
    assert row_b["value"] == 11.0
