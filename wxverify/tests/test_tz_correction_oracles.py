"""§18.7 generation-atomicity + §14 chain-semantics oracle suite (phase 4 QA).

Complements ``tests/test_tz_correction.py`` (the implementer suite) with the
adversarial oracles: mid-build delete-scoping across all three live mutation
paths, mixed-generation read isolation, flip transaction atomicity under an
injected mid-flip crash, interrupt/resume equivalence at every chunk
boundary on a real file database, chain-attempt cap edges, terminal-failure
wiring through the processor, the complete-then-continue single-transaction
dedupe ordering, fresh-DB acceptance of every new job type (the migrate_v3
CHECK latent-defect regression), cleanup completeness (including
stale-marked retired daily_truth and the drained-key deletion), the claim
priority tier under a full three-tier queue, and prospective-change
boundary resolution. All fixture values are synthetic.

Every test names the production mutation it kills (see the QA report):
mutations were applied one at a time, the named killer test observed red,
and the production file restored byte-identical (sha256-verified).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_observation,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import (
    apply_prospective_change,
    ensure_published_generation,
    published_generation_clause,
    published_generation_id,
    resolve_generation_for_instant,
    start_retrospective_correction,
)
from wxverify.scoring.consensus import materialize_consensus
from wxverify.scoring.leaderboard import LeaderboardRow, leaderboard
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.settings.keys import set_setting
from wxverify.worker.control import JobCancelled, JobContinuation
from wxverify.worker.processor import _complete_and_continue, _fail_job
from wxverify.worker.tz_correction import (
    advance_correction,
    correction_heartbeat_key,
    correction_state_key,
)

# Correction target: fixed +03:00, no DST — obviously synthetic next to the
# fixtures' 'UTC' sites, and every recomputed local day is hand-derivable.
NEW_TZ = "Etc/GMT-3"


# ---------------------------------------------------------------------------
# Fixture builders (synthetic; same raw-input shape as the implementer suite
# so hand-computed bucket transitions stay comparable across both files).
# ---------------------------------------------------------------------------


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
            VALUES (?, 'IORACLE001', 40.0, -105.0, 900.0, 1)
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


_OBS_HOURS = (
    "2026-06-10T18:00:00Z",
    "2026-06-10T22:00:00Z",
    "2026-06-10T23:00:00Z",
    "2026-06-11T00:00:00Z",
)

# (issued, valid, lead): UTC-vs-Etc/GMT-3 bucket transitions, hand-computed.
_SAMPLES = (
    ("2026-06-10T00:00:00Z", "2026-06-10T22:00:00Z", 22),  # 0 -> 1 (changed)
    ("2026-06-10T00:00:00Z", "2026-06-10T23:00:00Z", 23),  # 0 -> 1 (changed)
    ("2026-06-09T12:00:00Z", "2026-06-10T23:00:00Z", 35),  # 1 -> 2 (changed)
    ("2026-06-10T12:00:00Z", "2026-06-10T18:00:00Z", 6),  # 0 -> 0 (unchanged)
)


def _seed_history(
    conn: sqlite3.Connection, *, second_feed: bool = False
) -> tuple[int, int]:
    """Two-local-day synthetic history paired under the published (UTC)
    generation; optionally a second real feed at the same identities so the
    multimodel mean materializes (needs >= 2 contributors).
    """
    site_id = asof_make_site(conn, "oracle-site")
    feed_id = asof_make_real_feed(conn, "model-x")
    for index, hour in enumerate(_OBS_HOURS):
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=hour,
            value=10.0 + index,
            computed_at="2026-06-11T01:00:00Z",
        )
    feed_ids = [feed_id]
    if second_feed:
        feed_ids.append(asof_make_real_feed(conn, "model-y"))
    for fid, value in zip(feed_ids, (11.5, 13.5), strict=False):
        for issued, valid, lead in _SAMPLES:
            asof_insert_sample(
                conn,
                site_id=site_id,
                feed_id=fid,
                issued_at=issued,
                valid_at=valid,
                lead_hours=lead,
                value=value,
                fetched_at=issued,
            )
    ensure_published_generation(conn, site_id)
    pair_real_models(conn, site_id)
    materialize_persistence(conn, site_id)
    if second_feed:
        materialize_multimodel_mean(conn, site_id)
    return site_id, feed_id


def _published_pairs(
    conn: sqlite3.Connection, site_id: int
) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            f"""
            SELECT fp.feed_id, fp.variable, fp.issued_at, fp.valid_at,
                   fp.lead_hours, fp.day_ahead, fp.forecast, fp.observed
            FROM forecast_pairs fp
            WHERE fp.site_id = ? AND {published_generation_clause("fp")}
            ORDER BY fp.feed_id, fp.variable, fp.issued_at, fp.valid_at,
                     fp.lead_hours
            """,
            (site_id,),
        )
    ]


def _gen_rows(conn: sqlite3.Connection, generation_id: int, table: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) AS n FROM {table} WHERE tz_generation_id = ?",
        (generation_id,),
    ).fetchone()
    assert row is not None
    return int(row["n"])


def _chain_phase(conn: sqlite3.Connection, generation_id: int) -> str | None:
    raw = get_runtime_state(conn, correction_state_key(generation_id))
    if raw is None:
        return None
    return str(json.loads(raw).get("phase"))


def _advance_to_phase(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    payload: dict[str, object],
    phase: str,
    *,
    commit: bool = False,
    max_chunks: int = 200,
) -> None:
    """Drive chunks until the persisted chain state reaches ``phase``."""
    for _ in range(max_chunks):
        if _chain_phase(conn, generation_id) == phase:
            return
        assert advance_correction(conn, site_id, payload), (
            "chain finished before reaching the requested phase"
        )
        if commit:
            conn.commit()
    raise AssertionError(f"never reached chain phase {phase!r}")


def _drive_chain(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    payload: dict[str, object],
    *,
    commit: bool = False,
    max_chunks: int = 400,
) -> int:
    for chunk in range(1, max_chunks + 1):
        more = advance_correction(conn, site_id, payload)
        if commit:
            conn.commit()
        if not more:
            return chunk
    raise AssertionError("correction chain did not terminate")


# ---------------------------------------------------------------------------
# Oracle 1 — no-mixed-generation: the three live delete paths are
# published-scoped and building rows survive a mid-build mutation.
# ---------------------------------------------------------------------------


class TestMidBuildDeleteScoping:
    def test_consensus_invalidation_spares_building_real_and_persistence(
        self,
    ) -> None:
        """Kills: published-clause drop on the consensus pair-invalidation
        DELETE (consensus.py:331) and on the future-persistence-source DELETE
        (consensus.py:369). Paired positive: the same mutation event MUST
        delete the published rows it targets, so a no-op delete is loud too.
        """
        conn = asof_conn()
        site_id, feed_id = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }
        assert advance_correction(conn, site_id, payload)  # chain start
        assert advance_correction(conn, site_id, payload)  # all days rebuilt

        hour = "2026-06-10T22:00:00Z"

        def _counts(gen: int) -> tuple[int, int]:
            real = conn.execute(
                """
                SELECT COUNT(*) AS n FROM forecast_pairs
                WHERE site_id = ? AND tz_generation_id = ?
                  AND feed_id = ? AND valid_at = ?
                """,
                (site_id, gen, feed_id, hour),
            ).fetchone()
            persistence = conn.execute(
                """
                SELECT COUNT(*) AS n FROM forecast_pairs fp
                JOIN feeds f ON f.id = fp.feed_id
                WHERE fp.site_id = ? AND fp.tz_generation_id = ?
                  AND f.model = '_persistence' AND fp.issued_at = ?
                """,
                (site_id, gen, hour),
            ).fetchone()
            assert real is not None and persistence is not None
            return int(real["n"]), int(persistence["n"])

        published_gen = conn.execute(
            "SELECT CAST(value AS INTEGER) AS v FROM runtime_state WHERE key = ?",
            (f"tz_generation_published:{site_id}",),
        ).fetchone()
        assert published_gen is not None
        old_gen = int(published_gen["v"])

        building_before = _counts(generation_id)
        published_before = _counts(old_gen)
        assert building_before[0] > 0 and building_before[1] > 0, (
            "fixture must give the mutated hour building real AND "
            "persistence-source rows, or the oracle is vacuous"
        )
        assert published_before[0] > 0 and published_before[1] > 0

        # Live consensus mutation lands mid-build.
        _seed_station_observation(conn, site_id, hour, 30.0)
        materialize_consensus(
            conn, site_id=site_id, variable="temperature", valid_at=hour
        )

        # Building rows spared (both the real-pair delete and the
        # persistence-source delete are published-scoped) ...
        assert _counts(generation_id) == building_before
        # ... and the paired positive: the published rows WERE invalidated.
        assert _counts(old_gen) == (0, 0)

    def test_multimodel_refresh_spares_building_mean_rows(self) -> None:
        """Kills: published-clause drop on the multimodel site-scoped
        delete-and-recreate (multimodel.py:33)."""
        conn = asof_conn()
        site_id, _ = _seed_history(conn, second_feed=True)
        mean_feed = conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
        ).fetchone()
        assert mean_feed is not None
        mean_feed_id = int(mean_feed["id"])
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }
        assert advance_correction(conn, site_id, payload)
        assert advance_correction(conn, site_id, payload)

        def _mean_rows(gen: int) -> list[tuple[object, ...]]:
            return [
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT issued_at, valid_at, forecast FROM forecast_pairs
                    WHERE site_id = ? AND feed_id = ? AND tz_generation_id = ?
                    ORDER BY issued_at, valid_at
                    """,
                    (site_id, mean_feed_id, gen),
                )
            ]

        old_gen = published_generation_id(conn, site_id)
        assert old_gen is not None
        building_before = _mean_rows(generation_id)
        assert building_before, "chain must have built multimodel mean rows"
        published_before = _mean_rows(old_gen)
        assert published_before, "published multimodel mean rows must exist"

        # Live refresh mid-build (the site-scoped delete-and-recreate lane).
        materialize_multimodel_mean(conn, site_id)

        assert _mean_rows(generation_id) == building_before
        # Paired positive: the published lane really was recreated (rows
        # still present under the published generation, same identities).
        assert _mean_rows(old_gen) == published_before

    def test_mid_build_leaderboard_serves_published_only(self) -> None:
        """§18.7 read direction: while the chain is mid-build the live
        leaderboard is bit-identical to before the build started — building
        rows (whose day_ahead buckets moved) never contaminate the aggregate.

        Kills: published-clause drop in ContinuousStrategy.aggregate
        (metrics.py:86) — building rows double the per-feed n and shift MAE.
        Paired positive: after the flip the same read DIFFERS, so the oracle
        cannot pass by the reader ignoring generations entirely.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        set_setting(conn, "min_n", "1")

        def _read() -> list[LeaderboardRow]:
            return leaderboard(
                conn,
                site_id=site_id,
                variable="temperature",
                day_ahead=1,
                window="9999d",
            )

        before = _read()
        assert before, "fixture must yield leaderboard rows (non-vacuity)"
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }
        assert advance_correction(conn, site_id, payload)
        assert advance_correction(conn, site_id, payload)
        assert _gen_rows(conn, generation_id, "forecast_pairs") > 0

        assert _read() == before

        _drive_chain(conn, site_id, generation_id, payload)
        after_flip = _read()
        assert after_flip != before, (
            "post-flip leaderboard must reflect the corrected buckets — if "
            "it does not, the mid-build equality above was vacuous"
        )


def test_rain_threshold_change_spares_building_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills: published-clause drop on the rain-threshold precip delete
    (api/routes/sites.py:127). Exercised through the real route (CSRF +
    inline pair_and_score), with a correction chain genuinely mid-build.
    """
    import asyncio as _asyncio

    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, get_db

    async def _idle_worker(_db: object) -> None:
        await _asyncio.Event().wait()

    close_db()
    config.db_path = str(tmp_path / "oracle-rain.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            site_id = asof_make_site(conn, "rain-oracle-site")
            feed_id = asof_make_real_feed(conn, "model-rain")
            conn.execute(
                """
                INSERT INTO observations
                    (site_id, variable, valid_at, value, n_stations)
                VALUES (?, 'precip', '2026-06-10T22:00:00Z', 0.4, 1)
                """,
                (site_id,),
            )
            asof_insert_sample(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                issued_at="2026-06-10T00:00:00Z",
                valid_at="2026-06-10T22:00:00Z",
                lead_hours=22,
                value=0.5,
                fetched_at="2026-06-10T00:00:00Z",
            )
            conn.execute(
                """
                UPDATE forecast_samples SET variable='precip'
                WHERE site_id = ?
                """,
                (site_id,),
            )
            ensure_published_generation(conn, site_id)
            pair_real_models(conn, site_id)
            generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
            payload: dict[str, object] = {
                "generation_id": generation_id,
                "days_per_chunk": 1000,
            }
            assert advance_correction(conn, site_id, payload)
            assert advance_correction(conn, site_id, payload)
            return site_id, generation_id

        site_id, generation_id = db.write_sync(_seed)

        def _building_precip(conn: sqlite3.Connection) -> int:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM forecast_pairs
                WHERE site_id = ? AND variable = 'precip'
                  AND tz_generation_id = ?
                """,
                (site_id, generation_id),
            ).fetchone()
            assert row is not None
            return int(row["n"])

        building_before = db.read_sync(_building_precip)
        assert building_before > 0, "chain must have built precip rows"

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        response = client.put(
            f"/api/sites/{site_id}",
            json={"rain_threshold_mm": 1.0},
            headers=headers,
        )
        assert response.status_code == 200

        assert db.read_sync(_building_precip) == building_before
        # Paired positive: the published precip pair was recreated on the
        # NEW threshold (delete really fired on the published lane).
        published_threshold = db.read_sync(
            lambda conn: conn.execute(
                f"""
                SELECT fp.rain_threshold_mm FROM forecast_pairs fp
                WHERE fp.site_id = ? AND fp.variable = 'precip'
                  AND {published_generation_clause("fp")}
                """,
                (site_id,),
            ).fetchone()
        )
        assert published_threshold is not None
        assert float(published_threshold["rain_threshold_mm"]) == 1.0
    close_db()


# ---------------------------------------------------------------------------
# Oracle 2 — flip atomicity: a crash INSIDE the flip rolls back the whole
# transition; no observer ever sees a half-flipped state.
# ---------------------------------------------------------------------------


class TestFlipAtomicity:
    def test_crash_inside_flip_leaves_no_torn_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Kills: splitting the flip into two transactions (a commit inserted
        after the pointer write, before the rescore enqueue) — the rollback
        below would then leave the retire/publish/pointer half durable.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        conn.execute(
            """
            INSERT INTO score_cache
                (site_id, feed_id, variable, day_ahead, window_key, n,
                 computed_at)
            VALUES (?, 1, 'temperature', 0, 'all', 3, '2026-06-11T02:00:00Z')
            """,
            (site_id,),
        )
        conn.commit()
        old_gen = published_generation_id(conn, site_id)
        assert old_gen is not None
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        conn.commit()
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1,
        }
        # Commit at every chunk boundary so the rollback below scopes to the
        # flip transaction alone, exactly like the worker's one-txn-per-chunk.
        _advance_to_phase(conn, site_id, generation_id, payload, "flip", commit=True)

        def _boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("injected crash inside flip")

        monkeypatch.setattr("wxverify.worker.tz_correction.enqueue_if_absent", _boom)
        with pytest.raises(RuntimeError, match="injected crash inside flip"):
            advance_correction(conn, site_id, payload)
        conn.rollback()
        monkeypatch.undo()

        # NOTHING of the flip is observable: pointer, states, score_cache,
        # site timezone, rescore queue, chain phase.
        assert published_generation_id(conn, site_id) == old_gen
        states = {
            int(row["id"]): str(row["state"])
            for row in conn.execute(
                "SELECT id, state FROM timezone_generations WHERE site_id = ?",
                (site_id,),
            )
        }
        assert states[generation_id] == "building"
        assert states[old_gen] == "published"
        cache = conn.execute(
            "SELECT COUNT(*) AS n FROM score_cache WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert cache is not None and int(cache["n"]) == 1
        site = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        assert site is not None and str(site["timezone"]) == "UTC"
        rescore = conn.execute(
            """
            SELECT COUNT(*) AS n FROM jobs
            WHERE type = 'pair_and_score' AND site_id = ? AND status = 'pending'
            """,
            (site_id,),
        ).fetchone()
        assert rescore is not None and int(rescore["n"]) == 0
        assert _chain_phase(conn, generation_id) == "flip"

        # Resume-at-the-flip-boundary: the very next chunk completes the
        # flip; the chain then drains cleanup and finishes normally.
        _drive_chain(conn, site_id, generation_id, payload, commit=True)
        assert published_generation_id(conn, site_id) == generation_id
        site = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        assert site is not None and str(site["timezone"]) == NEW_TZ

    def test_flip_refuses_unreconciled_counts(self) -> None:
        """§13: the pointer flips only once examined == changed + unchanged
        + excluded reconciles.

        Kills: removing the reconciliation-identity guard in _flip — the
        tampered chain below would then publish instead of erroring.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        conn.commit()
        old_gen = published_generation_id(conn, site_id)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        conn.commit()
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }
        _advance_to_phase(conn, site_id, generation_id, payload, "flip", commit=True)
        conn.execute(
            """
            UPDATE timezone_generations
            SET examined_count = examined_count + 1 WHERE id = ?
            """,
            (generation_id,),
        )
        conn.commit()
        with pytest.raises(RuntimeError, match="reconciliation mismatch"):
            advance_correction(conn, site_id, payload)
        conn.rollback()
        assert published_generation_id(conn, site_id) == old_gen
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?", (generation_id,)
        ).fetchone()
        assert state is not None and str(state["state"]) == "building"
        # Paired positive: restore the tally and the flip completes.
        conn.execute(
            """
            UPDATE timezone_generations
            SET examined_count = examined_count - 1 WHERE id = ?
            """,
            (generation_id,),
        )
        conn.commit()
        _drive_chain(conn, site_id, generation_id, payload, commit=True)
        assert published_generation_id(conn, site_id) == generation_id


# ---------------------------------------------------------------------------
# Oracle 3 — interrupt/resume at EVERY chunk boundary on a real file DB.
# ---------------------------------------------------------------------------


def _open_file_conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _final_state_dump(
    conn: sqlite3.Connection, site_id: int, generation_id: int
) -> tuple[object, ...]:
    pairs = sorted(
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
    truth = sorted(
        tuple(row)
        for row in conn.execute(
            """
            SELECT local_date, quantity, eligible, covered_hours,
                   expected_slots, timezone, stale
            FROM daily_truth WHERE site_id = ?
            """,
            (site_id,),
        )
    )
    counts = conn.execute(
        """
        SELECT examined_count, changed_count, unchanged_count, excluded_count,
               state, timezone
        FROM timezone_generations WHERE id = ?
        """,
        (generation_id,),
    ).fetchone()
    assert counts is not None
    return (pairs, truth, tuple(counts))


def test_interrupt_and_resume_at_every_boundary_matches_uninterrupted(
    tmp_path: Path,
) -> None:
    """§18.7 resumability: the chain is driven ONE chunk per process life —
    after every chunk the connection is committed and CLOSED, then a fresh
    connection resumes from the persisted state blob. The final state is
    identical to an uninterrupted in-memory run (pairs, truth, counts,
    states, pointer).

    Kills: dropping the cursor advance from the persisted days-chunk state
    (the chain then never progresses across a resume and the chunk budget
    trips).
    """
    # Reference: uninterrupted run on a single in-memory connection.
    ref_conn = asof_conn()
    ref_site, _ = _seed_history(ref_conn)
    ref_gen = start_retrospective_correction(ref_conn, ref_site, NEW_TZ)
    payload: dict[str, object] = {
        "generation_id": ref_gen,
        "days_per_chunk": 1,
        "cleanup_chunk_rows": 1,
    }
    _drive_chain(ref_conn, ref_site, ref_gen, payload)
    reference = _final_state_dump(ref_conn, ref_site, ref_gen)

    # Interrupted run: file DB, one chunk per connection.
    db_path = tmp_path / "resume-oracle.db"
    conn = _open_file_conn(db_path)
    run_migrations(conn)
    site_id, _ = _seed_history(conn)
    generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
    conn.commit()
    conn.close()
    payload = {
        "generation_id": generation_id,
        "days_per_chunk": 1,
        "cleanup_chunk_rows": 1,
    }
    phases_seen: list[str] = []
    for _ in range(200):
        conn = _open_file_conn(db_path)
        more = advance_correction(conn, site_id, payload)
        conn.commit()
        phase = _chain_phase(conn, generation_id)
        if phase is not None:
            phases_seen.append(phase)
        conn.close()
        if not more:
            break
    else:
        raise AssertionError("interrupted chain did not terminate")

    # Every boundary type was actually crossed (non-vacuity).
    assert {"days", "rescan", "flip", "cleanup"} <= set(phases_seen)

    conn = _open_file_conn(db_path)
    assert _final_state_dump(conn, site_id, generation_id) == reference
    assert published_generation_id(conn, site_id) == generation_id
    # Reconciliation identity held across every interruption.
    counts = conn.execute(
        """
        SELECT examined_count, changed_count, unchanged_count, excluded_count
        FROM timezone_generations WHERE id = ?
        """,
        (generation_id,),
    ).fetchone()
    assert counts is not None
    assert int(counts["examined_count"]) == (
        int(counts["changed_count"])
        + int(counts["unchanged_count"])
        + int(counts["excluded_count"])
    )
    conn.close()


def test_redelivery_after_completed_chain_is_refused() -> None:
    conn = asof_conn()
    site_id, _ = _seed_history(conn)
    generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
    payload: dict[str, object] = {"generation_id": generation_id}
    _drive_chain(conn, site_id, generation_id, payload)
    assert published_generation_id(conn, site_id) == generation_id
    rows_before = _gen_rows(conn, generation_id, "forecast_pairs")
    assert rows_before > 0
    # Stale re-delivery of the finished chain's job.
    with pytest.raises(JobCancelled):
        advance_correction(conn, site_id, payload)
    assert _gen_rows(conn, generation_id, "forecast_pairs") == rows_before
    state = conn.execute(
        "SELECT state FROM timezone_generations WHERE id = ?", (generation_id,)
    ).fetchone()
    assert state is not None and str(state["state"]) == "published"


def test_redelivery_after_generation_retired_is_refused() -> None:
    """A stale re-delivery for a generation that has since been RETIRED
    (a newer correction published over it) must likewise never restart a
    build: only 'building' and 'failed' may reach a chain start.

    Kills: dropping the retired guard (or widening the chain-start gate),
    which would resurrect a retired generation's build under a pointer
    that no longer serves it.
    """
    conn = asof_conn()
    site_id, _ = _seed_history(conn)
    generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
    payload: dict[str, object] = {"generation_id": generation_id}
    _drive_chain(conn, site_id, generation_id, payload)
    # A newer correction publishing flips this generation to 'retired'.
    conn.execute(
        "UPDATE timezone_generations SET state = 'retired' WHERE id = ?",
        (generation_id,),
    )
    rows_before = _gen_rows(conn, generation_id, "forecast_pairs")
    with pytest.raises(JobCancelled):
        advance_correction(conn, site_id, payload)
    assert _gen_rows(conn, generation_id, "forecast_pairs") == rows_before
    state = conn.execute(
        "SELECT state FROM timezone_generations WHERE id = ?", (generation_id,)
    ).fetchone()
    assert state is not None and str(state["state"]) == "retired"


# ---------------------------------------------------------------------------
# Oracle 4 — chain-bound attempts: cap edges, failed-generation isolation,
# fresh correction after failure, terminal-failure wiring.
# ---------------------------------------------------------------------------


class TestChainAttempts:
    def test_second_start_is_allowed_and_completes(self) -> None:
        """§14 allows exactly MAX_CHAIN_STARTS starts: a chain restarted once
        must still be able to COMPLETE on its second (last allowed) start.

        Kills: tightening the attempt cap from > to >= (the second start
        would then be refused).
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1,
        }
        assert advance_correction(conn, site_id, payload)  # start 1
        assert advance_correction(conn, site_id, payload)  # one day chunk
        conn.execute(
            "UPDATE timezone_generations SET state = 'failed' WHERE id = ?",
            (generation_id,),
        )
        _drive_chain(conn, site_id, generation_id, payload)  # start 2 + finish
        assert published_generation_id(conn, site_id) == generation_id
        raw = get_runtime_state(conn, correction_state_key(generation_id))
        assert raw is None, "completed chain must have dropped its state blob"

    def test_capped_failure_isolates_rows_and_allows_fresh_correction(self) -> None:
        """After the attempt cap: the failed generation's rows stay on disk
        but never reach published reads, further deliveries keep cancelling
        without restarting, and a brand-new correction (fresh generation id)
        runs to completion.

        Kills: removing the cap branch's JobCancelled raise (the third start
        would silently restart the build instead of failing the run).
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        published_before = _published_pairs(conn, site_id)
        assert published_before
        generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        payload: dict[str, object] = {
            "generation_id": generation_id,
            "days_per_chunk": 1000,
        }

        def _fail() -> None:
            conn.execute(
                "UPDATE timezone_generations SET state = 'failed' WHERE id = ?",
                (generation_id,),
            )

        assert advance_correction(conn, site_id, payload)  # start 1
        assert advance_correction(conn, site_id, payload)  # builds rows
        _fail()
        assert advance_correction(conn, site_id, payload)  # start 2
        assert advance_correction(conn, site_id, payload)  # builds rows again
        _fail()
        with pytest.raises(JobCancelled):
            advance_correction(conn, site_id, payload)  # start 3: cap
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?", (generation_id,)
        ).fetchone()
        assert state is not None and str(state["state"]) == "failed"
        # Rows of the failed attempt linger on disk (cleanup only runs
        # post-flip) but are invisible to published reads.
        assert _gen_rows(conn, generation_id, "forecast_pairs") > 0
        assert _published_pairs(conn, site_id) == published_before
        # Re-delivery keeps cancelling; it must NOT restart the build.
        rows_at_cap = _gen_rows(conn, generation_id, "forecast_pairs")
        with pytest.raises(JobCancelled):
            advance_correction(conn, site_id, payload)
        assert _gen_rows(conn, generation_id, "forecast_pairs") == rows_at_cap

        # A fresh correction starts a FRESH generation and completes.
        fresh_id = start_retrospective_correction(conn, site_id, NEW_TZ)
        assert fresh_id != generation_id
        fresh_payload: dict[str, object] = {"generation_id": fresh_id}
        _drive_chain(conn, site_id, fresh_id, fresh_payload)
        assert published_generation_id(conn, site_id) == fresh_id

    def test_terminal_job_failure_marks_generation_through_processor(self) -> None:
        """The processor's _fail_job wiring (not just mark_correction_failed
        in isolation): a TERMINAL disposition of a timezone_correction job
        marks the building generation failed in the same transaction; a
        non-terminal retry leaves it building.

        Kills: dropping the mark_correction_failed call from _fail_job.
        """
        conn = asof_conn()
        # Terminal case: max_retries forced to 0 so the first failure lands
        # the terminal disposition.
        site_a = asof_make_site(conn, "term-site")
        _seed_min_history(conn, site_a)
        gen_a = start_retrospective_correction(conn, site_a, NEW_TZ)
        conn.execute(
            """
            UPDATE jobs SET max_retries = 0
            WHERE type = 'timezone_correction' AND site_id = ?
            """,
            (site_a,),
        )
        job_a = claim_next_job(conn)
        assert job_a is not None and job_a.type == "timezone_correction"
        disposition = _fail_job(conn, job_a, "synthetic chunk failure")
        assert disposition is not None and disposition.terminal
        state = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?", (gen_a,)
        ).fetchone()
        assert state is not None and str(state["state"]) == "failed"

        # Paired negative: a retryable failure must NOT fail the generation.
        site_b = asof_make_site(conn, "retry-site")
        _seed_min_history(conn, site_b)
        gen_b = start_retrospective_correction(conn, site_b, NEW_TZ)
        job_b = claim_next_job(conn)
        assert job_b is not None and job_b.type == "timezone_correction"
        disposition_b = _fail_job(conn, job_b, "synthetic transient failure")
        assert disposition_b is not None and not disposition_b.terminal
        state_b = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?", (gen_b,)
        ).fetchone()
        assert state_b is not None and str(state_b["state"]) == "building"


def _seed_min_history(conn: sqlite3.Connection, site_id: int) -> None:
    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at="2026-06-10T22:00:00Z",
        value=10.0,
        computed_at="2026-06-11T01:00:00Z",
    )
    ensure_published_generation(conn, site_id)


# ---------------------------------------------------------------------------
# Oracle 5 — claim priority tier: records < legacy < chain chunks, FIFO
# within each tier even when the chain job is the OLDEST row overall.
# ---------------------------------------------------------------------------


def test_claim_priority_full_three_tier_matrix() -> None:
    """Kills: dropping the type-priority CASE from claim_next_job (order
    reverts to enqueue FIFO) and demoting record_gap_scan out of tier 0.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "prio-matrix-site")
    # Enqueued oldest-first in WORST-case order: the chain chunks are the
    # oldest rows, the record types the newest.
    enqueue_order = (
        ("verification_run", "vrun:1"),
        ("timezone_correction", "tzcorr:1"),
        ("pair_and_score", "score"),
        ("fetch_obs", "obs"),
        ("record_gap_scan", "gap"),
        ("forecast_record", "record"),
    )
    for job_type, key in enqueue_order:
        payload: dict[str, object] = (
            {"generation_id": 1} if job_type == "timezone_correction" else {}
        )
        result = enqueue_if_absent(conn, job_type, site_id, key, payload)
        assert result.created, job_type
    claimed = []
    for _ in range(len(enqueue_order)):
        job = claim_next_job(conn)
        assert job is not None
        claimed.append(job.type)
    assert claim_next_job(conn) is None, "queue must be drained"
    assert claimed == [
        "record_gap_scan",  # tier 0, FIFO within tier
        "forecast_record",
        "pair_and_score",  # tier 1, FIFO within tier
        "fetch_obs",
        "verification_run",  # tier 2, FIFO within tier
        "timezone_correction",
    ]


# ---------------------------------------------------------------------------
# Oracle 6 — fresh-DB job-type acceptance (the migrate_v3 CHECK latent
# defect): where the production bug was silent, this oracle is loud.
# ---------------------------------------------------------------------------


def test_fresh_db_accepts_and_claims_every_new_job_type() -> None:
    """On a FRESH database (create_schema + the full migration chain —
    migrate_v4's entry guard early-returns on a fresh DB, so migrate_v3's
    jobs rebuild is the CHECK that governs the live
    table), each new job type must enqueue with created=True, land as a
    real row, and be claimable — enqueue_if_absent swallows CHECK
    IntegrityErrors, so a narrowed CHECK loses jobs silently in production
    (the phase-1 latent defect, fixed in migrations.py).

    Kills: re-narrowing the jobs CHECK (all rebuild copies) to the
    pre-0.11.0 5-type list.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "fresh-jobs-site")
    new_types = (
        "forecast_record",
        "record_gap_scan",
        "verification_run",
        "timezone_correction",
    )
    for job_type in new_types:
        result = enqueue_if_absent(conn, job_type, site_id, f"key-{job_type}", {})
        assert result.created, f"{job_type} enqueue was silently swallowed"
        row = conn.execute(
            "SELECT status FROM jobs WHERE type = ? AND site_id = ?",
            (job_type, site_id),
        ).fetchone()
        assert row is not None and str(row["status"]) == "pending", job_type
    claimed = set()
    for _ in new_types:
        job = claim_next_job(conn)
        assert job is not None
        claimed.add(job.type)
    assert claimed == set(new_types)
    # Decoy: the CHECK itself still stands — an unknown type is rejected,
    # not accepted (guards the opposite mutation, dropping the CHECK).
    bogus = enqueue_if_absent(conn, "bogus_job_type", site_id, "key-bogus", {})
    assert not bogus.created
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type = 'bogus_job_type'").fetchone()
        is None
    )


# ---------------------------------------------------------------------------
# Oracle 7 — cleanup completeness: retired pairs AND daily_truth (including
# stale-marked retired rows), key deletion when drained, published rows
# untouched.
# ---------------------------------------------------------------------------


def test_cleanup_drains_retired_rows_including_stale_truth_and_keys() -> None:
    """Kills: dropping daily_truth from the cleanup table loop (retired
    truth rows would linger forever), dropping the drained-key deletion
    (state/heartbeat blobs would linger), and dropping the tg.state =
    'retired' filter (the freshly published generation's rows would be
    deleted by its own cleanup).
    """
    conn = asof_conn()
    site_id, _ = _seed_history(conn)
    old_gen = published_generation_id(conn, site_id)
    assert old_gen is not None
    generation_id = start_retrospective_correction(conn, site_id, NEW_TZ)
    payload: dict[str, object] = {
        "generation_id": generation_id,
        "days_per_chunk": 1000,
        "cleanup_chunk_rows": 1,
    }
    _advance_to_phase(conn, site_id, generation_id, payload, "cleanup")
    # Flip has landed; cleanup has NOT run yet. Plant a stale-marked truth
    # row on the retired generation (what mark_daily_truth_stale leaves
    # behind pre-cleanup — the phase-3 INFO obligation).
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
        (site_id, old_gen),
    )
    retired_pairs_before = _gen_rows(conn, old_gen, "forecast_pairs")
    retired_truth_before = _gen_rows(conn, old_gen, "daily_truth")
    assert retired_pairs_before > 0 and retired_truth_before > 0, (
        "cleanup oracle needs real retired rows in BOTH tables"
    )
    new_pairs_before = _gen_rows(conn, generation_id, "forecast_pairs")
    new_truth_before = _gen_rows(conn, generation_id, "daily_truth")
    assert new_pairs_before > 0 and new_truth_before > 0

    chunks = 0
    while advance_correction(conn, site_id, payload):
        chunks += 1
        assert chunks < 200, "cleanup did not terminate"
    # cleanup_chunk_rows=1 forces genuine chunking (non-vacuity for LIMIT).
    assert chunks >= retired_pairs_before

    assert _gen_rows(conn, old_gen, "forecast_pairs") == 0
    assert _gen_rows(conn, old_gen, "daily_truth") == 0
    # Published (new) generation rows survived their own cleanup verbatim.
    assert _gen_rows(conn, generation_id, "forecast_pairs") == new_pairs_before
    assert _gen_rows(conn, generation_id, "daily_truth") == new_truth_before
    # Drained: both runtime_state keys deleted.
    assert get_runtime_state(conn, correction_state_key(generation_id)) is None
    assert get_runtime_state(conn, correction_heartbeat_key(generation_id)) is None


# ---------------------------------------------------------------------------
# Oracle — complete + continuation in ONE transaction, in dedupe-safe order.
# ---------------------------------------------------------------------------


def test_complete_and_continue_is_not_swallowed_by_own_running_row() -> None:
    """processor._complete_and_continue completes the finished chunk BEFORE
    enqueueing its continuation, so the active-dedupe index (pending/running
    + same key) cannot swallow the chain's next link.

    Kills: swapping the order (enqueue before complete) — the continuation
    dedupes against the still-running chunk and the chain silently drops.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "chain-cont-site")
    result = enqueue_if_absent(
        conn, "timezone_correction", site_id, "tzcorr:7", {"generation_id": 7}
    )
    assert result.created
    job = claim_next_job(conn)
    assert job is not None and job.status == "running"
    continuation = JobContinuation(
        job_type="timezone_correction",
        site_id=site_id,
        job_key="tzcorr:7",
        payload={"generation_id": 7},
    )
    _complete_and_continue(conn, job.id, continuation)
    rows = conn.execute(
        """
        SELECT id, status FROM jobs
        WHERE type = 'timezone_correction' AND site_id = ? AND job_key = 'tzcorr:7'
        ORDER BY id
        """,
        (site_id,),
    ).fetchall()
    assert [(int(r["id"]) == job.id, str(r["status"])) for r in rows] == [
        (True, "completed"),
        (False, "pending"),
    ]
    # Paired negative: a terminal chunk (no continuation) enqueues nothing.
    last = claim_next_job(conn)
    assert last is not None
    _complete_and_continue(conn, last.id, None)
    remaining = conn.execute(
        """
        SELECT COUNT(*) AS n FROM jobs
        WHERE type = 'timezone_correction' AND site_id = ?
          AND status IN ('pending', 'running')
        """,
        (site_id,),
    ).fetchone()
    assert remaining is not None and int(remaining["n"]) == 0


# ---------------------------------------------------------------------------
# Oracle 8 — prospective change: effective-boundary resolution, no rebuild.
# ---------------------------------------------------------------------------


class TestProspectiveBoundary:
    def test_boundary_instant_resolves_to_new_generation(self) -> None:
        """The effective instant itself belongs to the NEW generation: the
        old interval is half-open ([..., effective)), the new one closed at
        the bottom ([effective, ...)).

        Kills: loosening the upper-bound exclusion in
        resolve_generation_for_instant from >= to > (the boundary instant
        would then resolve to the OLD generation).
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        old_gen = published_generation_id(conn, site_id)
        assert old_gen is not None
        effective = "2026-07-01T00:00:00Z"
        new_gen = apply_prospective_change(conn, site_id, NEW_TZ, effective)
        assert resolve_generation_for_instant(conn, site_id, effective) == new_gen
        # Paired probes on both sides of the boundary.
        assert (
            resolve_generation_for_instant(conn, site_id, "2026-06-30T23:59:59Z")
            == old_gen
        )
        assert (
            resolve_generation_for_instant(conn, site_id, "2026-07-01T00:00:01Z")
            == new_gen
        )

    def test_prospective_change_schedules_no_rebuild(self) -> None:
        """§13: prospective = forward-only. No correction chain job, no
        building generation, and the historical pairs are byte-identical.

        Kills: closing the old generation's effective_to being dropped (the
        boundary probe above would double-match; here the explicit
        effective_to pin goes red), and any drift that starts enqueueing a
        rebuild for the prospective path.
        """
        conn = asof_conn()
        site_id, _ = _seed_history(conn)
        old_gen = published_generation_id(conn, site_id)
        assert old_gen is not None
        pairs_before = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE site_id = ?",
            (site_id,),
        ).fetchone()
        assert pairs_before is not None
        effective = "2026-07-01T00:00:00Z"
        apply_prospective_change(conn, site_id, NEW_TZ, effective)
        jobs = conn.execute(
            """
            SELECT COUNT(*) AS n FROM jobs
            WHERE type = 'timezone_correction' AND site_id = ?
            """,
            (site_id,),
        ).fetchone()
        assert jobs is not None and int(jobs["n"]) == 0
        building = conn.execute(
            """
            SELECT COUNT(*) AS n FROM timezone_generations
            WHERE site_id = ? AND state = 'building'
            """,
            (site_id,),
        ).fetchone()
        assert building is not None and int(building["n"]) == 0
        old_row = conn.execute(
            "SELECT effective_to FROM timezone_generations WHERE id = ?",
            (old_gen,),
        ).fetchone()
        assert old_row is not None and str(old_row["effective_to"]) == effective
        pairs_after = conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs WHERE site_id = ?",
            (site_id,),
        ).fetchone()
        assert pairs_after is not None
        assert int(pairs_after["n"]) == int(pairs_before["n"])
