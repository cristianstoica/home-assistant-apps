"""Part A of the switch-over migration (plan ``2026-10-22-daily-close-switch-
over.md`` §16, §17.5, MG1-MG8): ``migrate_v15_switch`` and its four parts --
``keep_read_generations`` (A1), ``settle_retired_jobs`` (A2),
``fail_incomplete_attempts`` (A3, an existing replay helper) and the retired
``runtime_state`` keys (A4).

``wxverify.db.switch`` does not exist yet, so every test that calls
``migrate_v15_switch`` or ``keep_read_generations``/``settle_retired_jobs``
imports it INSIDE the test body (plan §19 Step 3): until P6 lands, each such
test fails with ``ModuleNotFoundError`` -- a red caused by P6's own missing
behaviour, not a fixture bug. MG7 and MG8 need no such import: MG7 fails
today because ``run_migrations`` still raises on a user_version above its
(still-14) ``TARGET_USER_VERSION``; MG8 fails because
``record_precip_hour_start_boundary`` still runs its old rain-pair delete,
correction restart and score rescore, which §16.3 retires.

Synthetic data only: stations/sites are bare synthetic rows (no real
station ids, coordinates or site names); dates are far-future (2030) to
avoid any resemblance to real collection history.
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.db.migrations import TARGET_USER_VERSION, run_migrations
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
)
from wxverify.db.switch import ensure_site_generation
from wxverify.verification.runs import published_run_key

# The retired time zone pointer and correction-chain keys, in the same text
# ``wxverify.db.switch`` still retires by prefix (``_RETIRED_KEY_PREFIXES``,
# ``db/switch.py:40-47``) now that the old per-site/per-generation module
# that used to build them is gone. Local to this file: these tests seed the
# PRE-switch state Part A reads and deletes, so they still need the retired
# key text itself.


def _pointer_key(site_id: int) -> str:
    return f"tz_generation_published:{site_id}"


def _correction_state_key(generation_id: int) -> str:
    return f"tz_correction_state:{generation_id}"


def _correction_heartbeat_key(generation_id: int) -> str:
    return f"tz_correction_heartbeat:{generation_id}"


# ---------------------------------------------------------------------------
# Small fixture helpers (local to this file; tests/helpers.py untouched).
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str = "mg-site") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_feed(conn: sqlite3.Connection, model: str = "mg_feed") -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, fetch_interval_minutes)"
        " VALUES ('open-meteo', ?, 60)",
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_generation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    mode: str,
    state: str,
    effective_to: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state,"
        " effective_to) VALUES (?, 'UTC', ?, ?, ?)",
        (site_id, mode, state, effective_to),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    generation_id: int,
    valid_at: str,
    variable: str = "temperature",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 1, 0, 10.0, 10.0, ?)
        """,
        (site_id, feed_id, variable, valid_at, valid_at, generation_id),
    )


def _insert_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    local_date: str,
    quantity: str = "temperature_high",
) -> None:
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             tz_generation_id)
        VALUES (?, ?, ?, 10.0, 1, 24, 24, ?, ?, 'UTC', ?)
        """,
        (
            site_id,
            local_date,
            quantity,
            f"{local_date}T00:00:00Z",
            f"{local_date}T23:59:59Z",
            generation_id,
        ),
    )


def _insert_record(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    snapshot_local_date: str,
    target_local_date: str | None = None,
    variable: str = "temperature",
) -> int:
    target_local_date = target_local_date or snapshot_local_date
    cur = conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, missed_reason, methodology_version,
             app_version)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, ?, 0, 'missed', 'no_data',
                4, 'test')
        """,
        (
            site_id,
            generation_id,
            snapshot_local_date,
            f"{snapshot_local_date}T07:00:00Z",
            target_local_date,
            variable,
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_run(
    conn: sqlite3.Connection, *, site_id: int, generation_id: int, state: str
) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 4, 'test', ?, 1, '{}', 0, 0, 'fp')
        """,
        (site_id, generation_id, state),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_evidence(conn: sqlite3.Connection, *, run_id: int) -> None:
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, forecast_eligible,
             truth_eligible)
        VALUES (?, '2030-01-01', '2030-01-01', 0, 'temperature',
                'temperature_high', 'feed', 'k1', 0, 0)
        """,
        (run_id,),
    )


def _insert_day_context(conn: sqlite3.Connection, *, run_id: int) -> None:
    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc, knowability_exclusions,
             null_availability_samples)
        VALUES (?, '2030-01-01', '2030-01-01T07:00:00Z', '{}', 0)
        """,
        (run_id,),
    )


def _insert_result(conn: sqlite3.Connection, *, run_id: int) -> None:
    conn.execute(
        """
        INSERT INTO verification_results
            (run_id, variable, lead, quantity, entity_type, entity_key,
             headline, common_days)
        VALUES (?, 'temperature', 1, 'temperature_high', 'feed', 'k1', 1, 1)
        """,
        (run_id,),
    )


def _insert_verdict(conn: sqlite3.Connection, *, run_id: int) -> None:
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, incumbent_depth, tested_family)
        VALUES (?, 'temperature', 'skipped', 1, 'single')
        """,
        (run_id,),
    )


def _insert_job(
    conn: sqlite3.Connection,
    *,
    type_: str,
    site_id: int | None,
    status: str,
    job_key: str | None,
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES (?, ?, ?, ?)",
        (type_, site_id, job_key, status),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _generations(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM timezone_generations WHERE site_id=? ORDER BY id",
        (site_id,),
    ).fetchall()


def _pairs(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM forecast_pairs WHERE site_id=? ORDER BY valid_at",
        (site_id,),
    ).fetchall()


def _truth(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM daily_truth WHERE site_id=? ORDER BY local_date",
        (site_id,),
    ).fetchall()


def _jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()


# ---------------------------------------------------------------------------
# MG1 -- A1 keeps only the pointer generation's forecast_pairs/daily_truth
# rows; forecast_of_record and verification_runs stay whole; only the
# pointer generation ends up 'published'. M23, M24, M25.
# ---------------------------------------------------------------------------


def test_mg1_keep_read_generations_keeps_only_the_pointer_generation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    feed_id = _insert_feed(conn)

    g0 = _insert_generation(conn, site_id=site_id, mode="initial", state="retired")
    g1 = _insert_generation(
        conn,
        site_id=site_id,
        mode="prospective_change",
        state="published",
        effective_to="2030-01-01T00:00:00Z",
    )
    g2 = _insert_generation(
        conn, site_id=site_id, mode="prospective_change", state="published"
    )
    g3 = _insert_generation(
        conn, site_id=site_id, mode="retrospective_correction", state="building"
    )
    # A fifth, published, HIGHER-id generation that is deliberately NOT the
    # pointer: distinguishes M25 (picks the newest published row) from the
    # correct "pointer's id" resolution, which must pick g2 here even though
    # g4 is newer and also published.
    g4 = _insert_generation(
        conn, site_id=site_id, mode="prospective_change", state="published"
    )
    set_runtime_state(conn, _pointer_key(site_id), str(g2))

    for gen in (g0, g1, g2, g3, g4):
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            generation_id=gen,
            valid_at=f"2030-01-0{gen % 9 + 1}T00:00:00Z",
        )
        _insert_truth(
            conn,
            site_id=site_id,
            generation_id=gen,
            local_date=f"2030-01-0{gen % 9 + 1}",
        )
    record_id = _insert_record(
        conn, site_id=site_id, generation_id=g1, snapshot_local_date="2029-06-01"
    )
    run_id = _insert_run(conn, site_id=site_id, generation_id=g2, state="published")

    g2_pairs_before = {
        (r["valid_at"], r["tz_generation_id"])
        for r in _pairs(conn, site_id)
        if r["tz_generation_id"] == g2
    }
    g2_truth_before = {
        (r["local_date"], r["tz_generation_id"])
        for r in _truth(conn, site_id)
        if r["tz_generation_id"] == g2
    }

    with caplog.at_level(logging.INFO):
        from wxverify.db.switch import keep_read_generations

        keep_read_generations(conn)

    pairs_after = _pairs(conn, site_id)
    truth_after = _truth(conn, site_id)
    pair_gens_after = {r["tz_generation_id"] for r in pairs_after}
    truth_gens_after = {r["tz_generation_id"] for r in truth_after}
    assert pair_gens_after == {g2}, (
        f"M23/M25 -> at this assertion: correct = {g2!r} (only the pointer "
        "generation's forecast_pairs survive), "
        f"mutant = {pair_gens_after!r} (another published generation's rows "
        "also survive)"
    )
    assert truth_gens_after == {g2}, (
        f"M23 -> at this assertion: correct = {g2!r}, mutant = {truth_gens_after!r}"
    )
    g2_pairs_after = {(r["valid_at"], r["tz_generation_id"]) for r in pairs_after}
    assert g2_pairs_after == g2_pairs_before, (
        "M23 -> at this assertion: correct = "
        f"{g2_pairs_before!r} (g2's own rows unchanged), mutant = "
        f"{g2_pairs_after!r}"
    )
    g2_truth_after = {(r["local_date"], r["tz_generation_id"]) for r in truth_after}
    assert g2_truth_after == g2_truth_before

    # forecast_of_record and verification_runs are kept whole (M24).
    record_rows = conn.execute(
        "SELECT id, tz_generation_id FROM forecast_of_record WHERE site_id=?",
        (site_id,),
    ).fetchall()
    assert [r["id"] for r in record_rows] == [record_id], (
        "M24 -> at this assertion: correct = "
        f"{[record_id]!r} (g1's record row kept), mutant = "
        f"{[r['id'] for r in record_rows]!r}"
    )
    assert record_rows[0]["tz_generation_id"] == g1
    run_rows = conn.execute(
        "SELECT id FROM verification_runs WHERE site_id=?", (site_id,)
    ).fetchall()
    assert [r["id"] for r in run_rows] == [run_id]

    gens_after = {r["id"]: r["state"] for r in _generations(conn, site_id)}
    assert gens_after[g2] == "published", (
        "M25 -> at this assertion: correct = 'published' for g2 (the "
        f"pointer), mutant state = {gens_after[g2]!r}"
    )
    for gen in (g0, g1, g3, g4):
        assert gens_after[gen] == "retired", (
            "M25 -> at this assertion: correct = 'retired' for every "
            f"non-pointer generation, mutant state of {gen} = "
            f"{gens_after[gen]!r}"
        )

    assert any(rec.levelno == logging.INFO for rec in caplog.records), (
        "keep_read_generations must log one INFO line (plan 10.1, step 4)"
    )


# ---------------------------------------------------------------------------
# MG2 -- A2 deletes pending/running rows of retired kinds and fetch_obs
# keyed 'obs', keeps fetch_obs keyed 'wind-days', completed/failed rows and
# every kept kind. M26, M27, M28.
# ---------------------------------------------------------------------------


def test_mg2_settle_retired_jobs_deletes_only_pending_and_running_retired(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    site_id2 = _insert_site(conn, "mg-site-2")
    ensure_site_generation(conn, site_id)
    ensure_site_generation(conn, site_id2)

    retired_kinds = (
        "catchup",
        "backfill_site",
        "timezone_correction",
        "verification_run",
        "pair_and_score",
    )
    kept_ids: list[int] = []
    retired_pending_running_ids: list[int] = []
    for kind in retired_kinds:
        if kind == "catchup":
            # catchup has no site (it is global), so the active-row dedupe
            # index (type, COALESCE(site_id, -1), job_key) collapses both
            # rows onto site -1; give the pending and running row distinct
            # job_keys instead so both can be active at once.
            retired_pending_running_ids.append(
                _insert_job(
                    conn,
                    type_=kind,
                    site_id=None,
                    status="pending",
                    job_key="catchup-key-pending",
                )
            )
            retired_pending_running_ids.append(
                _insert_job(
                    conn,
                    type_=kind,
                    site_id=None,
                    status="running",
                    job_key="catchup-key-running",
                )
            )
            for status in ("completed", "failed"):
                kept_ids.append(
                    _insert_job(
                        conn,
                        type_=kind,
                        site_id=None,
                        status=status,
                        job_key="catchup-key",
                    )
                )
            continue
        key = f"{kind}-key"
        # Per-site kinds: put the pending and running row on two different
        # sites so the dedupe index's (type, site, job_key) triple differs
        # between them -- a pending AND a running row of the SAME type,
        # key and site cannot coexist under that index.
        retired_pending_running_ids.append(
            _insert_job(
                conn, type_=kind, site_id=site_id, status="pending", job_key=key
            )
        )
        retired_pending_running_ids.append(
            _insert_job(
                conn, type_=kind, site_id=site_id2, status="running", job_key=key
            )
        )
        for status in ("completed", "failed"):
            kept_ids.append(
                _insert_job(
                    conn, type_=kind, site_id=site_id, status=status, job_key=key
                )
            )

    obs_pending_id = _insert_job(
        conn, type_="fetch_obs", site_id=site_id, status="pending", job_key="obs"
    )
    retired_pending_running_ids.append(obs_pending_id)
    wind_days_pending_id = _insert_job(
        conn,
        type_="fetch_obs",
        site_id=site_id,
        status="pending",
        job_key="wind-days",
    )
    wind_days_running_id = _insert_job(
        conn,
        type_="fetch_obs",
        site_id=site_id2,
        status="running",
        job_key="wind-days",
    )
    kept_ids.extend([wind_days_pending_id, wind_days_running_id])

    for kind in (
        "fetch_feed",
        "fetch_current_obs",
        "forecast_record",
        "record_gap_scan",
        "day_close",
        "day_publish",
    ):
        kept_ids.append(
            _insert_job(
                conn, type_=kind, site_id=site_id, status="pending", job_key=None
            )
        )

    with caplog.at_level(logging.INFO):
        from wxverify.db.switch import settle_retired_jobs

        deleted = settle_retired_jobs(conn)

    assert deleted == len(retired_pending_running_ids), (
        "M26/M27 -> at this assertion: correct = "
        f"{len(retired_pending_running_ids)} rows deleted, mutant = "
        f"{deleted!r}"
    )
    remaining_ids = {r["id"] for r in _jobs(conn)}
    assert remaining_ids == set(kept_ids), (
        "M28 -> at this assertion: correct = the kept-ids set "
        f"{sorted(kept_ids)!r} (the 'obs' pending row gone, 'wind-days' "
        f"rows kept), mutant remaining ids = {sorted(remaining_ids)!r}"
    )
    assert obs_pending_id not in remaining_ids, (
        "M28 -> at this assertion: correct = the 'obs'-keyed fetch_obs row "
        "gone, mutant = it is still present"
    )
    assert {wind_days_pending_id, wind_days_running_id} <= remaining_ids, (
        "M26 -> at this assertion: correct = both 'wind-days'-keyed "
        "fetch_obs rows kept, mutant = one or both deleted"
    )
    assert any(rec.levelno == logging.INFO for rec in caplog.records)


# ---------------------------------------------------------------------------
# MG3 -- A3 (plan 8.3) is the step of Part A that calls
# fail_incomplete_attempts(conn, site_id, error=...) for EVERY site. This
# test exercises A3 through migrate_v15_switch (not by calling
# fail_incomplete_attempts directly, which is pre-existing code P6 does not
# add) so it actually pins the new behaviour: that Part A runs the replay
# cleanup for every site, not just one the caller names. Two sites, each
# with one published run and one running run, each carrying all four kinds
# of worksheet rows; each site's published-run pointer is also checked
# unchanged. M29.
#
# wxverify.db.switch does not exist yet, so this fails today with
# ModuleNotFoundError -- correct, since migrate_v15_switch (and therefore
# A3 run for every site) is P6's own missing behaviour.
# ---------------------------------------------------------------------------


def test_mg3_fail_incomplete_attempts_only_touches_unpublished_runs() -> None:
    conn = _conn()
    site_id = _insert_site(conn, "mg-site")
    site_id2 = _insert_site(conn, "mg-site-2")
    generation_id = ensure_site_generation(conn, site_id)
    generation_id2 = ensure_site_generation(conn, site_id2)

    def _seed_site_runs(sid: int, gen_id: int) -> tuple[int, int]:
        published_run = _insert_run(
            conn, site_id=sid, generation_id=gen_id, state="published"
        )
        _insert_evidence(conn, run_id=published_run)
        _insert_day_context(conn, run_id=published_run)
        _insert_result(conn, run_id=published_run)
        _insert_verdict(conn, run_id=published_run)

        running_run = _insert_run(
            conn, site_id=sid, generation_id=gen_id, state="running"
        )
        _insert_evidence(conn, run_id=running_run)
        _insert_day_context(conn, run_id=running_run)
        _insert_result(conn, run_id=running_run)
        _insert_verdict(conn, run_id=running_run)

        set_runtime_state(conn, published_run_key(sid), str(published_run))
        return published_run, running_run

    published_1, running_1 = _seed_site_runs(site_id, generation_id)
    published_2, running_2 = _seed_site_runs(site_id2, generation_id2)

    pointer_before = {
        site_id: get_runtime_state(conn, published_run_key(site_id)),
        site_id2: get_runtime_state(conn, published_run_key(site_id2)),
    }

    from wxverify.db.switch import migrate_v15_switch

    migrate_v15_switch(conn)

    error_text = "retired at the switch to the daily close"
    for site, published_run, running_run in (
        (site_id, published_1, running_1),
        (site_id2, published_2, running_2),
    ):
        rows = {
            r["id"]: (r["state"], r["error"])
            for r in conn.execute(
                "SELECT id, state, error FROM verification_runs WHERE site_id=?",
                (site,),
            ).fetchall()
        }
        assert rows[published_run][0] == "published", (
            "M29 -> at this assertion: correct = 'published' for the "
            f"already-published run of site {site}, mutant state = "
            f"{rows[published_run][0]!r}"
        )
        assert rows[running_run][0] == "failed", (
            "M29 -> at this assertion: correct = 'failed' for the "
            f"unfinished run of site {site}, mutant state = "
            f"{rows[running_run][0]!r}"
        )
        assert rows[running_run][1] == error_text, (
            "M29 -> at this assertion: correct = "
            f"{error_text!r} for the failed run's error (site {site}), "
            f"mutant = {rows[running_run][1]!r}"
        )

        for table in (
            "verification_evidence",
            "verification_day_context",
            "verification_results",
            "verification_verdicts",
        ):
            published_count = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE run_id=?",
                (published_run,),
            ).fetchone()["n"]
            running_count = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE run_id=?",
                (running_run,),
            ).fetchone()["n"]
            assert published_count == 1, (
                "M29 -> at this assertion: correct = 1 (the published "
                f"run's {table} row kept, site {site}), mutant = "
                f"{published_count!r}"
            )
            assert running_count == 0, (
                "M29 -> at this assertion: correct = 0 (the unfinished "
                f"run's {table} rows dropped, site {site}), mutant = "
                f"{running_count!r}"
            )

        assert (
            get_runtime_state(conn, published_run_key(site)) == pointer_before[site]
        ), (
            "M29 -> at this assertion: correct = "
            f"{pointer_before[site]!r} (the site's published-run pointer "
            f"unchanged), mutant = {get_runtime_state(conn, published_run_key(site))!r}"
        )


# ---------------------------------------------------------------------------
# MG4 -- A4 deletes only the retired runtime_state key prefixes. M30.
# ---------------------------------------------------------------------------


def test_mg4_retired_keys_removed_kept_keys_survive() -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    generation_id = ensure_site_generation(conn, site_id)

    retired_keys = [
        "wind_history_auth_hold:history_all",
        _pointer_key(site_id),
        _correction_state_key(generation_id),
        _correction_heartbeat_key(generation_id),
        "verification_state:1",
        "verification_heartbeat:1",
    ]
    for key in retired_keys:
        set_runtime_state(conn, key, "retired-value")

    kept_keys = {
        "weathercom_access_hold:five_minute": "hold-value",
        PRECIP_HOUR_START_BOUNDARY_KEY: "1970-01-01T00:00:00Z",
        "verification_published:1": "1",
    }
    for key, value in kept_keys.items():
        set_runtime_state(conn, key, value)

    from wxverify.db.switch import migrate_v15_switch

    migrate_v15_switch(conn)

    remaining = {
        row["key"] for row in conn.execute("SELECT key FROM runtime_state").fetchall()
    }
    for key in retired_keys:
        assert key not in remaining, (
            f"M30 -> at this assertion: correct = {key!r} absent from "
            f"runtime_state, mutant = it is still present"
        )
    for key in kept_keys:
        assert key in remaining, (
            f"M30 -> at this assertion: correct = {key!r} present in "
            "runtime_state, mutant = it was deleted too"
        )


# ---------------------------------------------------------------------------
# MG5 -- A1's K resolution: a dangling pointer (names a 'building'
# generation) falls back to the newest published row; a site with no
# published row at all gets a fresh one. Both log a WARNING. M31.
# ---------------------------------------------------------------------------


def test_mg5_dangling_and_missing_pointers_resolve_with_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    site2 = _insert_site(conn, "mg-site-2")
    building = _insert_generation(conn, site_id=site2, mode="initial", state="building")
    older_published = _insert_generation(
        conn, site_id=site2, mode="initial", state="published"
    )
    # The pointer names the BUILDING generation -- not a valid published K.
    set_runtime_state(conn, _pointer_key(site2), str(building))

    site3 = _insert_site(conn, "mg-site-3")
    # site3 has no timezone_generations row at all.

    with caplog.at_level(logging.WARNING):
        from wxverify.db.switch import keep_read_generations

        keep_read_generations(conn)

    site2_gens = {r["id"]: r["state"] for r in _generations(conn, site2)}
    assert site2_gens[older_published] == "published", (
        "M31 -> at this assertion: correct = 'published' for site 2's "
        "newest actually-published generation when the pointer dangles, "
        f"mutant state = {site2_gens[older_published]!r}"
    )
    assert site2_gens[building] == "retired"

    site3_gens = _generations(conn, site3)
    assert len(site3_gens) == 1, (
        "M31 -> at this assertion: correct = 1 new generation seeded for "
        f"site 3 (none existed), mutant count = {len(site3_gens)!r}"
    )
    assert site3_gens[0]["state"] == "published"

    assert any(rec.levelno == logging.WARNING for rec in caplog.records), (
        "both the dangling pointer and the missing-published-row cases "
        "must log a WARNING (plan 10.1, step 1)"
    )


# ---------------------------------------------------------------------------
# MG6 -- Part A is one SAVEPOINT: a fault during A2 rolls back A1's work
# too; with the fault removed, a repeat runs Part A whole. M32.
# ---------------------------------------------------------------------------


def test_mg6_a_fault_mid_part_a_rolls_back_the_whole_savepoint() -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    g1 = _insert_generation(
        conn,
        site_id=site_id,
        mode="initial",
        state="published",
        effective_to="2030-01-01T00:00:00Z",
    )
    g2 = _insert_generation(conn, site_id=site_id, mode="initial", state="published")
    set_runtime_state(conn, _pointer_key(site_id), str(g2))
    _insert_job(
        conn, type_="catchup", site_id=None, status="pending", job_key="catchup"
    )

    conn.execute(
        "CREATE TRIGGER mg6_fault BEFORE DELETE ON jobs "
        "BEGIN SELECT RAISE(ABORT, 'fault'); END"
    )

    from wxverify.db.switch import migrate_v15_switch

    with pytest.raises(sqlite3.DatabaseError):
        migrate_v15_switch(conn)

    gens_after_fault = {r["id"]: r["state"] for r in _generations(conn, site_id)}
    assert gens_after_fault[g1] == "published", (
        "M32 -> at this assertion: correct = 'published' (A1's retire of "
        "g1 rolled back along with A2's failure), mutant state = "
        f"{gens_after_fault[g1]!r}"
    )

    conn.execute("DROP TRIGGER mg6_fault")
    migrate_v15_switch(conn)

    gens_after_retry = {r["id"]: r["state"] for r in _generations(conn, site_id)}
    assert gens_after_retry[g1] == "retired", (
        "M32 -> at this assertion: correct = 'retired' once Part A runs "
        f"without the fault, mutant state = {gens_after_retry[g1]!r}"
    )
    assert gens_after_retry[g2] == "published"


# ---------------------------------------------------------------------------
# MG7 -- Part A is gated on current < 15: a file already at 15 is left
# alone on a later open. M33.
#
# Calls run_migrations directly; no wxverify.db.switch import is needed
# here, because the gate lives in run_migrations itself (plan 16.1, 16.2),
# which today fails any v15 file outright ("newer than this app") since
# TARGET_USER_VERSION is still 14 -- itself a legitimate red for "P6's
# behaviour is missing".
# ---------------------------------------------------------------------------


def test_mg7_part_a_is_gated_and_does_not_rerun_on_an_already_switched_file() -> None:
    conn = _conn()
    _insert_site(conn)
    job_id = _insert_job(
        conn, type_="catchup", site_id=None, status="pending", job_key="catchup"
    )
    conn.execute("PRAGMA user_version = 15")

    run_migrations(conn)

    remaining_ids = {r["id"] for r in _jobs(conn)}
    assert job_id in remaining_ids, (
        "M33 -> at this assertion: correct = the pending catchup row still "
        f"present (Part A gated on current < 15, {TARGET_USER_VERSION=} "
        "target 15 == current 15 skips it), mutant = the row was deleted "
        "by a rerun of A2"
    )


# ---------------------------------------------------------------------------
# MG8 -- the 0.16.12 boundary step keeps only its boundary-key write; its
# rain-pair delete, correction restart and rescore are retired (§16.3).
# M34.
#
# Calls run_migrations directly; no wxverify.db.switch import needed. Today
# (before §16.3's retirement lands) record_precip_hour_start_boundary still
# deletes the rain pairs and score_cache row and enqueues pair_and_score,
# so this is a genuine red against current, unfixed behaviour.
# ---------------------------------------------------------------------------


def test_mg8_boundary_step_keeps_only_its_key_write() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # One migrations pass to build the schema and seed default feeds, but
    # WITHOUT writing the boundary key yet: delete it straight after.
    run_migrations(conn)
    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)

    site_id = _insert_site(conn)
    generation_id = ensure_site_generation(conn, site_id)
    om_feed = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND is_virtual=0 LIMIT 1"
    ).fetchone()
    assert om_feed is not None
    feed_id = int(om_feed["id"])

    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'precip', '2030-01-01T00:00:00Z', '2030-01-01T01:00:00Z',
                1, 0.5, '{}', 'run1', '2030-01-01T00:05:00Z')
        """,
        (site_id, feed_id),
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=generation_id,
        valid_at="2030-01-01T01:00:00Z",
        variable="precip",
    )
    conn.execute(
        "INSERT INTO score_cache (site_id, feed_id, variable, day_ahead,"
        " window_key, n, computed_at) VALUES (?, ?, 'precip', 0, '7d', 1,"
        " '2030-01-01T00:00:00Z')",
        (site_id, feed_id),
    )
    jobs_before = {r["id"] for r in _jobs(conn)}

    run_migrations(conn)

    boundary = get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
    assert boundary is not None, (
        "M34 -> at this assertion: correct = the boundary key written, "
        "mutant = it is still absent"
    )

    rain_pairs = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE variable='precip'"
    ).fetchone()["n"]
    assert rain_pairs == 1, (
        "M34 -> at this assertion: correct = 1 (the rain pair kept, the "
        f"rain-pair delete retired), mutant = {rain_pairs!r} (deleted)"
    )
    score_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM score_cache WHERE variable='precip'"
    ).fetchone()["n"]
    assert score_rows == 1, (
        "M34 -> at this assertion: correct = 1 (the rain score_cache row "
        f"kept), mutant = {score_rows!r} (deleted)"
    )
    jobs_after = {r["id"] for r in _jobs(conn)}
    assert jobs_after == jobs_before, (
        "M34 -> at this assertion: correct = no new job row (no "
        "timezone_correction or pair_and_score enqueue), mutant = "
        f"{jobs_after - jobs_before!r} newly enqueued"
    )
