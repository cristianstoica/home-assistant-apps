"""Timezone-correction machinery and job-chain infrastructure (§13 / §14).

Covers: the retrospective correction chain end-to-end (build-alongside
rebuild, atomic flip, chunked cleanup), chunk-size equivalence (resume at
any boundary), mid-build published serving, the claim priority tier, the
chain-bound attempt cap, the prospective change path, the delete-scoping
obligation on live consensus mutations, and the daily_truth local_date
canonicalization. All fixture values are synthetic.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_observation,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify.db.queue import Job, claim_next_job, enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import (
    apply_prospective_change,
    correction_job_key,
    ensure_published_generation,
    published_generation_clause,
    published_generation_id,
    resolve_generation_for_instant,
    start_retrospective_correction,
)
from wxverify.scoring.consensus import materialize_consensus
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.verification.truth import (
    materialize_daily_truth,
    regenerate_marked_truth,
)
from wxverify.worker.control import JobCancelled
from wxverify.worker.tz_correction import (
    advance_correction,
    correction_state_key,
    mark_correction_failed,
)

# Target zone for corrections: fixed +03:00 offset, no DST, obviously
# synthetic relative to the fixtures' 'UTC' sites.
NEW_TZ = "Etc/GMT-3"


def _seed_station_observation(
    conn: sqlite3.Connection, site_id: int, valid_at: str, value: float
) -> None:
    """One enabled station reading feeding materialize_consensus."""
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ?", (site_id,)
    ).fetchone()
    if row is None:
        cur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
            VALUES (?, 'ISYNTH0001', 40.0, -105.0, 900.0, 1)
            """,
            (site_id,),
        )
        assert cur.lastrowid is not None
        station_id = int(cur.lastrowid)
    else:
        station_id = int(row["id"])
    conn.execute(
        """
        INSERT INTO station_observations
            (station_id, variable, valid_at, value, qc_flag, source_raw,
             fetched_at)
        VALUES (?, 'temperature', ?, ?, 'ok', '{}', ?)
        ON CONFLICT(station_id, variable, valid_at) DO UPDATE SET
            value = excluded.value
        """,
        (station_id, valid_at, value, valid_at),
    )


def _seed_history(conn: sqlite3.Connection) -> tuple[int, int]:
    """Synthetic two-day history: consensus observations + one real feed's
    samples, paired under the site's published (UTC) generation.
    """
    site_id = asof_make_site(conn, "corr-site")
    feed_id = asof_make_real_feed(conn, "model-a")
    hours = (
        "2026-06-10T18:00:00Z",
        "2026-06-10T22:00:00Z",
        "2026-06-10T23:00:00Z",
        "2026-06-11T00:00:00Z",
    )
    for index, hour in enumerate(hours):
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=hour,
            value=10.0 + index,
            computed_at="2026-06-11T01:00:00Z",
        )
    samples = (
        # (issued, valid, lead): UTC bucket 0 -> Etc/GMT-3 bucket 1 (changed)
        ("2026-06-10T00:00:00Z", "2026-06-10T22:00:00Z", 22),
        ("2026-06-10T00:00:00Z", "2026-06-10T23:00:00Z", 23),
        # UTC bucket 1 -> new bucket 2 (changed)
        ("2026-06-09T12:00:00Z", "2026-06-10T23:00:00Z", 35),
        # UTC bucket 0 -> new bucket 0 (unchanged: both local 2026-06-10)
        ("2026-06-10T12:00:00Z", "2026-06-10T18:00:00Z", 6),
    )
    for issued, valid, lead in samples:
        asof_insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at=issued,
            valid_at=valid,
            lead_hours=lead,
            value=11.5,
            fetched_at=issued,
        )
    ensure_published_generation(conn, site_id)
    pair_real_models(conn, site_id)
    materialize_persistence(conn, site_id)
    return site_id, feed_id


def _published_pairs(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        f"""
        SELECT fp.* FROM forecast_pairs fp
        WHERE fp.site_id = ? AND {published_generation_clause("fp")}
        ORDER BY fp.feed_id, fp.variable, fp.issued_at, fp.valid_at
        """,
        (site_id,),
    ).fetchall()


def _drive_chain(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    *,
    days_per_chunk: int = 1,
    cleanup_chunk_rows: int = 2,
    max_chunks: int = 400,
) -> int:
    """Run advance_correction to completion; returns the chunk count."""
    payload: dict[str, object] = {
        "generation_id": generation_id,
        "days_per_chunk": days_per_chunk,
        "cleanup_chunk_rows": cleanup_chunk_rows,
    }
    for chunk in range(1, max_chunks + 1):
        if not advance_correction(conn, site_id, payload):
            return chunk
    raise AssertionError("correction chain did not terminate")


class TestRetrospectiveChain:
    def test_end_to_end_flip_and_cleanup(self) -> None:
        conn = asof_conn()
        site_id, _feed_id = _seed_history(conn)
        old_generation = published_generation_id(conn, site_id)
        assert old_generation is not None
        published_before = _published_pairs(conn, site_id)
        assert published_before, "fixture must produce published pairs"
        # A cached score row and a truth row that must be invalidated/kept.
        conn.execute(
            """
            INSERT INTO score_cache
                (site_id, feed_id, variable, day_ahead, window_key, n,
                 computed_at)
            VALUES (?, 1, 'temperature', 0, 'all', 3, '2026-06-11T02:00:00Z')
            """,
            (site_id,),
        )
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)

        _drive_chain(conn, site_id, generation_id)

        # Flip landed: states, pointer, site timezone.
        assert published_generation_id(conn, site_id) == generation_id
        states = {
            int(row["id"]): str(row["state"])
            for row in conn.execute(
                "SELECT id, state FROM timezone_generations WHERE site_id = ?",
                (site_id,),
            )
        }
        assert states[generation_id] == "published"
        assert states[old_generation] == "retired"
        site = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        assert site is not None and str(site["timezone"]) == NEW_TZ
        # score_cache dropped, rescore enqueued.
        cache = conn.execute(
            "SELECT COUNT(*) AS n FROM score_cache WHERE site_id = ?",
            (site_id,),
        ).fetchone()
        assert cache is not None and int(cache["n"]) == 0
        rescore = conn.execute(
            """
            SELECT COUNT(*) AS n FROM jobs
            WHERE type = 'pair_and_score' AND site_id = ? AND status = 'pending'
            """,
            (site_id,),
        ).fetchone()
        assert rescore is not None and int(rescore["n"]) == 1
        # Cleanup dissolved the retired generation's rows entirely.
        for table in ("forecast_pairs", "daily_truth"):
            leftover = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE tz_generation_id = ?",
                (old_generation,),
            ).fetchone()
            assert leftover is not None and int(leftover["n"]) == 0, table
        # Chain state blob cleaned up.
        assert get_runtime_state(conn, correction_state_key(generation_id)) is None
        # Reconciliation counts: internal identity + full published coverage.
        counts = conn.execute(
            """
            SELECT examined_count, changed_count, unchanged_count,
                   excluded_count
            FROM timezone_generations WHERE id = ?
            """,
            (generation_id,),
        ).fetchone()
        assert counts is not None
        examined = int(counts["examined_count"])
        assert examined == len(published_before)
        assert examined == (
            int(counts["changed_count"])
            + int(counts["unchanged_count"])
            + int(counts["excluded_count"])
        )
        # The hand-computed real-pair buckets under Etc/GMT-3.
        buckets = {
            (str(row["issued_at"]), str(row["valid_at"])): int(row["day_ahead"])
            for row in _published_pairs(conn, site_id)
            if int(row["lead_hours"]) in (22, 23, 35, 6)
        }
        assert buckets[("2026-06-10T00:00:00Z", "2026-06-10T22:00:00Z")] == 1
        assert buckets[("2026-06-10T00:00:00Z", "2026-06-10T23:00:00Z")] == 1
        assert buckets[("2026-06-09T12:00:00Z", "2026-06-10T23:00:00Z")] == 2
        assert buckets[("2026-06-10T12:00:00Z", "2026-06-10T18:00:00Z")] == 0
        # daily_truth rows exist under the new generation only.
        truth = conn.execute(
            """
            SELECT COUNT(*) AS n FROM daily_truth
            WHERE site_id = ? AND tz_generation_id = ?
            """,
            (site_id, generation_id),
        ).fetchone()
        assert truth is not None and int(truth["n"]) > 0

    def test_chunk_size_equivalence(self) -> None:
        """Resume-at-any-boundary: 1-day chunks and one giant chunk converge
        to the identical final pair set (idempotent chunks by construction).
        """

        def run(days_per_chunk: int) -> list[tuple[object, ...]]:
            conn = asof_conn()
            site_id, _ = _seed_history(conn)
            generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
            _drive_chain(
                conn,
                site_id,
                generation_id,
                days_per_chunk=days_per_chunk,
                cleanup_chunk_rows=100000,
            )
            return sorted(
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT feed_id, variable, issued_at, valid_at, lead_hours,
                           day_ahead, forecast, observed
                    FROM forecast_pairs WHERE site_id = ?
                    """,
                    (site_id,),
                )
            )

        assert run(1) == run(1000)

    def test_mid_build_published_keeps_serving(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        old_generation = published_generation_id(conn, site_id)
        before = [tuple(row) for row in _published_pairs(conn, site_id)]
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1,
        }
        # Chain start + first day chunk only: generation still building.
        assert advance_correction(conn, site_id, payload)
        assert advance_correction(conn, site_id, payload)
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?",
            (generation_id,),
        ).fetchone()
        assert state is not None and str(state["state"]) == "building"
        # Published reads are byte-identical to before the build started,
        # and the pointer has not moved.
        assert [tuple(row) for row in _published_pairs(conn, site_id)] == before
        assert published_generation_id(conn, site_id) == old_generation
        # Building rows exist but are invisible through the shared clause.
        building = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE tz_generation_id = ?",
            (generation_id,),
        ).fetchone()
        assert building is not None and int(building["n"]) > 0

    def test_live_consensus_mutation_spares_building_rows(self) -> None:
        """Carried obligation 1: the consensus invalidation delete is scoped
        to the published generation, and the chain's rescan rebuilds the
        mutated day under the building generation.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }
        assert advance_correction(conn, site_id, payload)  # chain start
        assert advance_correction(conn, site_id, payload)  # all days rebuilt

        def building_rows_at(valid_at: str) -> int:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM forecast_pairs
                WHERE site_id = ? AND tz_generation_id = ? AND valid_at = ?
                """,
                (site_id, generation_id, valid_at),
            ).fetchone()
            assert row is not None
            return int(row["n"])

        mutated_hour = "2026-06-10T22:00:00Z"
        rows_before = building_rows_at(mutated_hour)
        assert rows_before > 0
        # Live mutation lands mid-build (funnels through
        # materialize_consensus, the sole observations writer).
        _seed_station_observation(conn, site_id, mutated_hour, 30.0)
        materialize_consensus(
            conn, site_id=site_id, variable="temperature", valid_at=mutated_hour
        )
        # Building rows at the mutated hour survived the invalidation
        # delete (published-scoped) ...
        assert building_rows_at(mutated_hour) == rows_before
        # ... and the rescan phase re-derives the day so the building
        # generation converges on the NEW observed value.
        _drive_chain(conn, site_id, generation_id, days_per_chunk=1000)
        observed = conn.execute(
            """
            SELECT DISTINCT observed FROM forecast_pairs
            WHERE site_id = ? AND tz_generation_id = ? AND valid_at = ?
            """,
            (site_id, generation_id, mutated_hour),
        ).fetchall()
        assert [float(row["observed"]) for row in observed] == [30.0]

    def test_third_chain_start_marks_failed(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {"generation_id": generation_id}

        def fail_run() -> None:
            conn.execute(
                "UPDATE timezone_generations SET state = 'failed' WHERE id = ?",
                (generation_id,),
            )

        assert advance_correction(conn, site_id, payload)  # start 1
        fail_run()
        assert advance_correction(conn, site_id, payload)  # start 2 (restart)
        fail_run()
        with pytest.raises(JobCancelled):  # start 3: cap
            advance_correction(conn, site_id, payload)
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?",
            (generation_id,),
        ).fetchone()
        assert state is not None and str(state["state"]) == "failed"
        # Not re-pended: a further claim keeps cancelling, never restarts.
        with pytest.raises(JobCancelled):
            advance_correction(conn, site_id, payload)

    def test_restart_wipes_prior_attempt_evidence(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1,
        }
        assert advance_correction(conn, site_id, payload)  # start (attempt 1)
        assert advance_correction(conn, site_id, payload)  # one day rebuilt
        conn.execute(
            "UPDATE timezone_generations SET state = 'failed' WHERE id = ?",
            (generation_id,),
        )
        assert advance_correction(conn, site_id, payload)  # start (attempt 2)
        # First action of the new attempt deleted the prior attempt's rows
        # and reset the reconciliation counts.
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE tz_generation_id = ?",
            (generation_id,),
        ).fetchone()
        assert rows is not None and int(rows["n"]) == 0
        counts = conn.execute(
            "SELECT examined_count FROM timezone_generations WHERE id = ?",
            (generation_id,),
        ).fetchone()
        assert counts is not None and int(counts["examined_count"]) == 0
        raw = get_runtime_state(conn, correction_state_key(generation_id))
        assert raw is not None
        assert json.loads(raw)["attempts"] == 2

    def test_terminal_job_failure_marks_generation_failed(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        job = Job(
            id=1,
            type="timezone_correction",
            site_id=site_id,
            job_key=correction_job_key(generation_id),
            payload={"generation_id": generation_id},
            status="running",
            retry_count=5,
            max_retries=5,
        )
        mark_correction_failed(conn, job)
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?",
            (generation_id,),
        ).fetchone()
        assert state is not None and str(state["state"]) == "failed"

    def test_second_concurrent_correction_refused(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        start_retrospective_correction(conn, site_id, NEW_TZ)
        with pytest.raises(ValueError, match="already has a correction"):
            start_retrospective_correction(conn, site_id, "Etc/GMT-5")


class TestClaimPriority:
    def test_records_claim_before_chain_chunks(self) -> None:
        """§14: forecast_record / record_gap_scan claim ahead of everything;
        chain chunks claim last, so FIFO cannot let a self-re-enqueueing
        chain starve record derivation.
        """
        conn = asof_conn()
        site_id = asof_make_site(conn, "prio-site")
        # Enqueued oldest-first in REVERSE priority order.
        enqueue_if_absent(
            conn, "timezone_correction", site_id, "tzcorr:1", {"generation_id": 1}
        )
        enqueue_if_absent(conn, "pair_and_score", site_id, "score", {})
        enqueue_if_absent(conn, "forecast_record", site_id, "record", {})
        claims = (claim_next_job(conn), claim_next_job(conn), claim_next_job(conn))
        claimed = [job.type for job in claims if job is not None]
        assert claimed == ["forecast_record", "pair_and_score", "timezone_correction"]

    def test_fifo_within_a_tier(self) -> None:
        conn = asof_conn()
        site_id = asof_make_site(conn, "fifo-site")
        enqueue_if_absent(conn, "pair_and_score", site_id, "score", {})
        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})
        first = claim_next_job(conn)
        assert first is not None and first.type == "pair_and_score"


class TestProspectiveChange:
    def test_history_preserved_and_pointer_flipped(self) -> None:
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        old_generation = published_generation_id(conn, site_id)
        assert old_generation is not None
        old_rows = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE tz_generation_id = ?",
            (old_generation,),
        ).fetchone()
        assert old_rows is not None and int(old_rows["n"]) > 0
        effective = "2026-07-01T00:00:00Z"
        new_generation = apply_prospective_change(conn, site_id, NEW_TZ, effective)
        # Pointer and site timezone moved; no rebuild happened.
        assert published_generation_id(conn, site_id) == new_generation
        site = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        assert site is not None and str(site["timezone"]) == NEW_TZ
        # Earlier history preserved under its former generation (rows
        # intact, generation still published-state with a closed interval).
        old = conn.execute(
            "SELECT state, effective_to FROM timezone_generations WHERE id = ?",
            (old_generation,),
        ).fetchone()
        assert old is not None
        assert str(old["state"]) == "published"
        assert str(old["effective_to"]) == effective
        kept = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE tz_generation_id = ?",
            (old_generation,),
        ).fetchone()
        assert kept is not None and int(kept["n"]) == int(old_rows["n"])
        # Derivation-time resolution honors the effective split (§14).
        assert (
            resolve_generation_for_instant(conn, site_id, "2026-06-15T00:00:00Z")
            == old_generation
        )
        assert (
            resolve_generation_for_instant(conn, site_id, "2026-07-02T00:00:00Z")
            == new_generation
        )

    def test_guards(self) -> None:
        conn = asof_conn()
        site_id = asof_make_site(conn, "guard-site")
        ensure_published_generation(conn, site_id)
        with pytest.raises(ValueError, match="already on timezone"):
            apply_prospective_change(conn, site_id, "UTC", "2026-07-01T00:00:00Z")
        new_generation = apply_prospective_change(
            conn, site_id, NEW_TZ, "2026-07-01T00:00:00Z"
        )
        assert new_generation != 0
        with pytest.raises(ValueError, match="must be after"):
            apply_prospective_change(conn, site_id, "Etc/GMT-5", "2026-06-01T00:00:00Z")
        with pytest.raises(ValueError, match="unknown IANA timezone"):
            apply_prospective_change(conn, site_id, "Not/AZone", "2026-08-01T00:00:00Z")


class TestDailyTruthObligations:
    def test_local_date_stored_canonical(self) -> None:
        """Carried obligation 2: a basic-format ISO date ('20260610') must
        not defeat the UNIQUE dedup — the stored local_date is canonical.
        """
        conn = asof_conn()
        site_id = asof_make_site(conn, "truth-site")
        ensure_published_generation(conn, site_id)
        materialize_daily_truth(conn, site_id=site_id, local_date="20260610")
        materialize_daily_truth(conn, site_id=site_id, local_date="2026-06-10")
        rows = conn.execute(
            "SELECT DISTINCT local_date FROM daily_truth WHERE site_id = ?",
            (site_id,),
        ).fetchall()
        assert [str(row["local_date"]) for row in rows] == ["2026-06-10"]
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ?",
            (site_id,),
        ).fetchone()
        assert count is not None and int(count["n"]) == 5

    def test_regenerate_skips_retired_generations(self) -> None:
        """Carried obligation 3 companion: stale-marked rows of a RETIRED
        generation await the chunked cleanup and are never regenerated.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        _drive_chain(conn, site_id, generation_id, cleanup_chunk_rows=100000)
        old_generation = conn.execute(
            """
            SELECT id FROM timezone_generations
            WHERE site_id = ? AND state = 'retired'
            """,
            (site_id,),
        ).fetchone()
        assert old_generation is not None
        # Plant a stale truth row tagged to the retired generation (as
        # mark_daily_truth_stale would have, pre-cleanup).
        conn.execute(
            """
            INSERT INTO daily_truth
                (site_id, local_date, quantity, eligible, covered_hours,
                 expected_slots, day_start_utc, day_end_utc, timezone,
                 stale, generated_at, tz_generation_id)
            VALUES (?, '2026-06-10', 'temperature_high', 0, 0, 24,
                    '2026-06-10T00:00:00Z', '2026-06-11T00:00:00Z', 'UTC',
                    1, '2026-06-11T02:00:00Z', ?)
            """,
            (site_id, int(old_generation["id"])),
        )
        assert regenerate_marked_truth(conn, site_id=site_id) == 0
        still_stale = conn.execute(
            """
            SELECT stale FROM daily_truth
            WHERE site_id = ? AND tz_generation_id = ?
            """,
            (site_id, int(old_generation["id"])),
        ).fetchone()
        assert still_stale is not None and int(still_stale["stale"]) == 1
