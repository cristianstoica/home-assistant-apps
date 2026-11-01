"""Readers of a generation after the switch (plan ``2026-10-22-daily-close-
switch-over.md`` §10.2, §17.6, TZ1-TZ8).

TZ1-TZ4 each seed site 1's four-generation shape (G0 retired, G1 published
with ``effective_to`` set -- a closed prospective change, G2 published and
the pointer, G3 building), open the file so Part A (``keep_read_generations``,
``wxverify.db.switch``, not yet written) runs, and call EVERY reader named
in the row's group (plan §17.6): TZ1 adds the mean's existing published
rows (G6) to pairing candidates (G1); TZ2 adds the leaderboard (G8), the
composite (G9) and the win rate (G7) to metrics aggregation (G3), the
leaderboard/composite calls going through the close-off live path (the key
is absent in this fixture) with a custom "Nd" window so they read
forecast_pairs directly; TZ3 adds the as-of exclusions (G11) to wind
training pairs (G10); TZ4 adds the feed-change scope (G13, called directly)
and ``update_site``'s rain-pair delete (G14, through its route, since the
delete lives inside the route's write closure) to the timeseries reader
(G12). ``wxverify.db.switch`` is imported at module level above. TZ5 adds
the gap scan's origin resolver (G18, ``_current_
generation``, the writer-path twin of ``current_generation_readonly`` it
also calls) to the record readers (G17); TZ7 imports new
``wxverify.db.switch`` names. TZ6 and TZ8 need no such import: both call
EXISTING functions (``write_close_truth``, ``current_result_basis_
fingerprint``) after directly deleting the site's ``tz_generation_published:``
pointer key -- exactly what A4 does (plan §8.4) -- which breaks both
functions today, since neither yet resolves the generation any other way.

Each TZ row's mutant (M35 through M38) is the same root defect -- A1
leaving another generation's rows in place, or a reader resolving the
wrong generation -- for every reader the row names. TZ6 and TZ8 still
exercise only their one named function each, since the plan lists exactly
one reader per row there.

Synthetic data only: stations/feeds are bare synthetic rows; dates are in
2029-2030.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import delete_runtime_state, set_runtime_state
from wxverify.db.site_days import begin_close, day_record
from wxverify.db.switch import ensure_site_generation, site_generation_id
from wxverify.forecast.wind_blend import MIN_RUN_HOURS, training_pairs_query
from wxverify.scoring.metrics import ContinuousStrategy
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.truth import evaluate_day_window, write_close_truth

# ---------------------------------------------------------------------------
# Shared fixture helpers.
# ---------------------------------------------------------------------------


def _pointer_key(site_id: int) -> str:
    """The retired time zone pointer's text (``db/switch.py:35``'s
    ``_POINTER_KEY_PREFIX``, private), local since the module that used to
    build it is gone. These tests seed/delete the PRE-switch pointer Part A
    reads and A4 deletes.
    """
    return f"tz_generation_published:{site_id}"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str = "tz-site") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_feed(conn: sqlite3.Connection, model: str = "tz_feed") -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, fetch_interval_minutes,"
        " max_lead_hours, default_subscribed) VALUES"
        " ('open-meteo', ?, 60, 168, 1)",
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_generation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    state: str,
    effective_to: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state,"
        " effective_to) VALUES (?, 'UTC', 'initial', ?, ?)",
        (site_id, state, effective_to),
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
    day_ahead: int = 0,
    forecast: float = 10.0,
    observed: float = 10.0,
) -> None:
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            valid_at,
            valid_at,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
            generation_id,
        ),
    )


def _build_four_generation_site(
    conn: sqlite3.Connection,
) -> tuple[int, int, dict[str, int]]:
    """Site 1's MG/TZ generation shape: G0 retired, G1 published+closed, G2
    published (the pointer, K), G3 building. Returns ``(site_id, feed_id,
    generations)``."""
    site_id = _insert_site(conn)
    feed_id = _insert_feed(conn)
    g0 = _insert_generation(conn, site_id=site_id, state="retired")
    g1 = _insert_generation(
        conn, site_id=site_id, state="published", effective_to="2030-01-01T00:00:00Z"
    )
    g2 = _insert_generation(conn, site_id=site_id, state="published")
    g3 = _insert_generation(conn, site_id=site_id, state="building")
    set_runtime_state(conn, _pointer_key(site_id), str(g2))
    return site_id, feed_id, {"g0": g0, "g1": g1, "g2": g2, "g3": g3}


def _remaining_generation_ids(
    conn: sqlite3.Connection, site_id: int, table: str
) -> set[int]:
    return {
        int(row["tz_generation_id"])
        for row in conn.execute(
            f"SELECT DISTINCT tz_generation_id FROM {table} WHERE site_id=?",
            (site_id,),
        ).fetchall()
    }


# ---------------------------------------------------------------------------
# TZ1 -- pairing candidates (G1) AND the mean's existing published rows
# (G6) see only K's rows once Part A has run; a stale duplicate pair/mean
# row left under another generation must not survive the open. M35.
# ---------------------------------------------------------------------------


def test_tz1_pairing_candidates_see_only_the_kept_generation() -> None:
    conn = _conn()
    site_id, feed_id, gens = _build_four_generation_site(conn)

    # A noise pair under g1 and g3 -- must be gone after Part A.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g1"],
        valid_at="2030-02-01T00:00:00Z",
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g3"],
        valid_at="2030-02-02T00:00:00Z",
    )
    # g2 (K)'s own real pair, which must survive untouched.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g2"],
        valid_at="2030-02-03T00:00:00Z",
    )

    # A genuine candidate sample: a matching observation, no pair anywhere
    # yet, with no day-ahead bucket issue (issued the same day it is valid).
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, 'temperature', '2030-02-04T00:00:00Z',
                '2030-02-04T01:00:00Z', 1, 12.0, '{}', 'run1')
        """,
        (site_id, feed_id),
    )
    conn.execute(
        """
        INSERT INTO observations (site_id, variable, valid_at, value, n_stations)
        VALUES (?, 'temperature', '2030-02-04T01:00:00Z', 11.5, 1)
        """,
        (site_id,),
    )

    # G6: the virtual mean feed's own published mean rows (forecast_pairs
    # rows under feed source='virtual' model='_multimodel_mean') -- a stale
    # row under g1 that must not survive, and g2's own row that must.
    mean_feed_rows = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchall()
    assert len(mean_feed_rows) == 1
    mean_feed_id = int(mean_feed_rows[0][0])
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=mean_feed_id,
        generation_id=gens["g1"],
        valid_at="2030-02-10T00:00:00Z",
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=mean_feed_id,
        generation_id=gens["g2"],
        valid_at="2030-02-11T00:00:00Z",
    )

    from wxverify.db.switch import keep_read_generations

    keep_read_generations(conn)

    remaining = _remaining_generation_ids(conn, site_id, "forecast_pairs")
    assert remaining == {gens["g2"]}, (
        "M35 -> at this assertion: correct = "
        f"{{gens['g2']}} (only the pointer generation's forecast_pairs "
        f"survive Part A), mutant = {remaining!r} (another generation's "
        "pair is still present)"
    )

    from wxverify.scoring.pairing import compute_real_model_pairs
    from wxverify.scoring.split import InsertOp

    delta = compute_real_model_pairs(conn, site_id)
    candidate_valid_ats = {op.valid_at for op in delta.ops if isinstance(op, InsertOp)}
    assert "2030-02-04T01:00:00Z" in candidate_valid_ats, (
        "the fresh sample must remain a pairing candidate after Part A"
    )

    from wxverify.scoring.multimodel import existing_mean_rows_sql

    mean_rows = conn.execute(
        existing_mean_rows_sql(site_scoped=True, day_clause="1"),
        (mean_feed_id, site_id),
    ).fetchall()
    mean_valid_ats = {r["valid_at"] for r in mean_rows}
    assert mean_valid_ats == {"2030-02-11T00:00:00Z"}, (
        "M35 -> at this assertion: correct = {'2030-02-11T00:00:00Z'} (only "
        "g2's published mean row survives Part A), mutant = "
        f"{mean_valid_ats!r} (g1's stale mean row is still readable)"
    )


# ---------------------------------------------------------------------------
# TZ2 -- metrics aggregation (G3), the leaderboard (G8), the composite (G9)
# and the win rate (G7) all read only K's pairs once Part A has run.
# Leaderboard/composite go through the close-off live path (the key is
# absent in this fixture), a custom "Nd" window, which reads forecast_pairs
# directly and so exercises A1's generation filter. M35.
# ---------------------------------------------------------------------------


def test_tz2_metrics_aggregation_reads_only_the_kept_generation() -> None:
    conn = _conn()
    site_id, feed_id, gens = _build_four_generation_site(conn)

    # The persistence baseline (scoring/metrics.py's _paired_skill always
    # joins against this feed, by name, regardless of the candidate) --
    # needed below to give g2's cell a skill score at all: with no
    # persistence pair at the same (valid_at, lead_hours, day_ahead),
    # skill_n is 0, skill_score is None, and composite()'s live path
    # (which skips a cell whose skill_score is None, scoring/composite.py
    # :404) drops g2's cell entirely.
    persistence_rows = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchall()
    assert len(persistence_rows) == 1
    persistence_feed_id = int(persistence_rows[0][0])

    # 3 noise pairs under g1/g3 at the SAME (feed, variable, day_ahead) as
    # the g2 pair below, so an unfixed A1 would double-count them into n.
    # Each also gets its own persistence reference pair, so an unfixed A1
    # leaving g1's/g3's rows in place would also leave their skill
    # references in place and still change the skill-based answers below,
    # not just the row count.
    noise = (("g1", "2030-03-01T00:00:00Z"), ("g3", "2030-03-02T00:00:00Z"))
    for gen_key, valid_at in noise:
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            generation_id=gens[gen_key],
            valid_at=valid_at,
        )
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=persistence_feed_id,
            generation_id=gens[gen_key],
            valid_at=valid_at,
            forecast=5.0,
            observed=10.0,
        )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g2"],
        valid_at="2030-03-03T00:00:00Z",
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence_feed_id,
        generation_id=gens["g2"],
        valid_at="2030-03-03T00:00:00Z",
        forecast=5.0,
        observed=10.0,
    )

    from wxverify.db.switch import keep_read_generations

    keep_read_generations(conn)

    remaining = _remaining_generation_ids(conn, site_id, "forecast_pairs")
    assert remaining == {gens["g2"]}, (
        "M35 -> at this assertion: correct = "
        f"{{gens['g2']}}, mutant = {remaining!r} (g1's or g3's pair is "
        "still readable)"
    )

    result = ContinuousStrategy().aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=0,
        window_cutoff=None,
        min_n=1,
    )
    assert result.n == 1, (
        "M35 -> at this assertion: correct = 1 (only g2's pair counted), "
        f"mutant = {result.n!r} (g1's/g3's leftover pair also counted)"
    )

    from wxverify.scoring.composite import composite
    from wxverify.scoring.leaderboard import leaderboard
    from wxverify.scoring.winrate import winrate
    from wxverify.settings.keys import set_setting

    # leaderboard() opens its own read_snapshot, which refuses to nest on a
    # connection already in a transaction (db/snapshot.py); commit the
    # seeding first, as other direct-conn callers of leaderboard() do.
    conn.commit()

    leaderboard_rows = leaderboard(
        conn, site_id=site_id, variable="temperature", day_ahead=0, window="2d"
    )
    # Two active feeds now have a g2 pair: the tz feed, and the
    # persistence baseline my own reference seeding above gives one too
    # (always an active competitor, db/migrations.py's FEED_SEEDS ->
    # is_virtual=1 -> scoring/effective.py's active_competitor_clause).
    # Ordered by (source, model): "open-meteo" sorts before "virtual".
    assert [(r.feed_id, r.n) for r in leaderboard_rows] == [
        (feed_id, 1),
        (persistence_feed_id, 1),
    ], (
        "M35 -> at this assertion: correct = "
        f"[({feed_id}, 1), ({persistence_feed_id}, 1)] (each active feed's "
        "g2 pair counted once), mutant = "
        f"{[(r.feed_id, r.n) for r in leaderboard_rows]!r}"
    )

    set_setting(conn, "min_n", "1")
    # composite() also opens a read_snapshot; commit set_setting's write
    # first for the same reason as above.
    conn.commit()
    composite_rows = composite(conn, site_id=site_id, window="2d")
    # Two composite rows now: the tz feed's (skill 1.0, since its own
    # error is 0 against the persistence reference's nonzero error) sorts
    # first by score (_sort_key is -score first), ahead of the
    # persistence baseline's own row (always skill 0.0 -- it is paired
    # against itself). [0] is still the tz feed's row.
    assert len(composite_rows) == 2 and composite_rows[0]["lead_counts"] == {
        "temperature": 1
    }, (
        "M35 -> at this assertion: correct = two composite rows, the tz "
        "feed's first with lead_counts {'temperature': 1} (only g2's "
        "pair), mutant = "
        f"{composite_rows!r}"
    )

    winrate_rows = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=0, window="2d"
    )
    # Two winrate rows now too, for the same reason as leaderboard above;
    # pick the tz feed's own row rather than assume its position (sorted
    # by feed_id, and the persistence baseline's id was seeded first).
    assert len(winrate_rows) == 2, (
        "M35 -> at this assertion: correct = 2 (the tz feed's row and the "
        "persistence baseline's), mutant = "
        f"{len(winrate_rows)!r}"
    )
    own_winrate_row = next(r for r in winrate_rows if r["feed_id"] == feed_id)
    assert own_winrate_row["covered"] == 1, (
        "M35 -> at this assertion: correct = one feed's row with covered=1 "
        "(only g2's pair seen by the canonical-cell query), mutant = "
        f"{own_winrate_row['covered']!r}"
    )


# ---------------------------------------------------------------------------
# TZ3 -- wind training pairs (G10, representative of G10/G11) read only K's
# pairs once Part A has run. M35.
# ---------------------------------------------------------------------------


def test_tz3_wind_training_pairs_read_only_the_kept_generation() -> None:
    conn = _conn()
    site_id, feed_id, gens = _build_four_generation_site(conn)

    for gen_key, valid_at in (
        ("g1", "2030-04-01T00:00:00Z"),
        ("g3", "2030-04-02T00:00:00Z"),
    ):
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            generation_id=gens[gen_key],
            valid_at=valid_at,
            variable="wind",
            day_ahead=1,
        )
    # training_pairs_query groups by (feed_id, issued_at, day_ahead) with a
    # HAVING COUNT(DISTINCT valid_at) >= MIN_RUN_HOURS floor; give the kept
    # row MIN_RUN_HOURS distinct valid_at hours under the SAME
    # issued_at/day_ahead so it clears that floor on its own.
    for hour in range(MIN_RUN_HOURS):
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, tz_generation_id)
            VALUES (?, ?, 'wind', '2030-04-03T00:00:00Z', ?, 1, 1, 5.0, 5.0, ?)
            """,
            (site_id, feed_id, f"2030-04-03T{hour:02d}:00:00Z", gens["g2"]),
        )

    from wxverify.db.switch import keep_read_generations

    keep_read_generations(conn)

    remaining = _remaining_generation_ids(conn, site_id, "forecast_pairs")
    assert remaining == {gens["g2"]}, (
        f"M35 -> at this assertion: correct = {{gens['g2']}}, mutant = {remaining!r}"
    )

    sql, params = training_pairs_query(
        site_id=site_id,
        lo="2030-04-01T00:00:00Z",
        hi="2030-04-05T00:00:00Z",
        as_of=None,
    )
    rows = conn.execute(sql, params).fetchall()
    total_n = sum(int(r["n"]) for r in rows)
    # Only g2's MIN_RUN_HOURS-hour run (04-03, hours 00 through MIN_RUN_HOURS
    # - 1) should count; a leftover g1/g3 row reaching this query would only
    # ever add single-hour, sub-floor runs, so it could never pass the
    # HAVING floor on its own -- the discriminator is therefore the
    # surviving ROW COUNT above, already asserted; this call pins the
    # reader still answers.
    assert total_n == MIN_RUN_HOURS, (
        f"M35 -> at this assertion: correct = {MIN_RUN_HOURS} "
        "(g2's MIN_RUN_HOURS-hour training run), "
        f"mutant = {total_n!r}"
    )

    # G11: the as-of exclusions over the same wind/day_ahead=1 rows -- every
    # surviving row has no first_known_at, so each is counted once under
    # null_first_known_at; a leftover g1/g3 row would inflate that count.
    from wxverify.verification.asof import pair_knowability_exclusions

    exclusions = pair_knowability_exclusions(
        conn,
        site_id=site_id,
        variable="wind",
        day_ahead=1,
        as_of="2030-04-10T00:00:00Z",
    )
    assert exclusions.null_first_known_at == MIN_RUN_HOURS, (
        f"M35 -> at this assertion: correct = {MIN_RUN_HOURS} (only g2's "
        f"{MIN_RUN_HOURS} wind rows counted), mutant = "
        f"{exclusions.null_first_known_at!r} (a g1/g3 leftover row also "
        "counted)"
    )


# ---------------------------------------------------------------------------
# TZ4 -- the timeseries stored-pairs reader (G12), the feed-change scope
# (G13) and update_site's rain-pair delete (G14) all read/act on only K's
# pairs once Part A has run. M35.
# ---------------------------------------------------------------------------


def test_tz4_timeseries_stored_pairs_reads_only_the_kept_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _conn()
    site_id, feed_id, gens = _build_four_generation_site(conn)

    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g1"],
        valid_at="2030-05-01T00:00:00Z",
        forecast=99.0,
        observed=99.0,
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=gens["g2"],
        valid_at="2030-05-02T00:00:00Z",
        forecast=20.0,
        observed=21.0,
    )

    from wxverify.db.switch import keep_read_generations

    keep_read_generations(conn)

    remaining = _remaining_generation_ids(conn, site_id, "forecast_pairs")
    assert remaining == {gens["g2"]}, (
        f"M35 -> at this assertion: correct = {{gens['g2']}}, mutant = {remaining!r}"
    )

    from wxverify.api.routes.timeseries import _stored_pairs

    rows = _stored_pairs(
        conn,
        site=site_id,
        variable="temperature",
        feed_id=feed_id,
        issued_at=None,
        from_=None,
        to=None,
        before=None,
        day_ahead=None,
    )
    assert rows == [("2030-05-02T00:00:00Z", 20.0, 21.0)], (
        "M35 -> at this assertion: correct = "
        "[('2030-05-02T00:00:00Z', 20.0, 21.0)] (only g2's row), mutant = "
        f"{rows!r} (g1's 99.0/99.0 row leaked through too)"
    )

    # G13: the feed-change scope, called directly (no route needed) -- it
    # still finds this site via g2's surviving pair.
    from wxverify.api.routes.feeds import _affected_sites_for_feed_change

    feed_row = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
    assert feed_row is not None
    affected = _affected_sites_for_feed_change(conn, feed_row, inherited_only=False)
    assert affected == {site_id}, (
        "M35 -> at this assertion: correct = "
        f"{{site_id}} (the site still found via g2's surviving pair), "
        f"mutant = {affected!r}"
    )

    # G14: update_site's rain-pair delete, through its route (the delete
    # lives inside the route's write closure) -- it must remove the SAME
    # rows a correctly-scoped reader sees: g2's precip pair, the only one
    # left standing after Part A.
    import asyncio

    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, get_db, init_db

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    init_db(str(db_path))

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        route_conn = get_db()._conn  # noqa: SLF001
        route_site_id, route_feed_id, route_gens = _build_four_generation_site(
            route_conn
        )
        _insert_pair(
            route_conn,
            site_id=route_site_id,
            feed_id=route_feed_id,
            generation_id=route_gens["g1"],
            valid_at="2030-05-10T00:00:00Z",
            variable="precip",
        )
        _insert_pair(
            route_conn,
            site_id=route_site_id,
            feed_id=route_feed_id,
            generation_id=route_gens["g2"],
            valid_at="2030-05-11T00:00:00Z",
            variable="precip",
        )

        from wxverify.db.switch import keep_read_generations as route_keep

        route_keep(route_conn)

        route_remaining = {
            r["tz_generation_id"]
            for r in route_conn.execute(
                "SELECT tz_generation_id FROM forecast_pairs"
                " WHERE site_id=? AND variable='precip'",
                (route_site_id,),
            ).fetchall()
        }
        assert route_remaining == {route_gens["g2"]}, (
            "M35 -> at this assertion: correct = "
            f"{{route_gens['g2']}} (only g2's precip pair survives Part A "
            f"before the rain-threshold edit), mutant = {route_remaining!r}"
        )

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        resp = client.put(
            f"/api/sites/{route_site_id}",
            headers=headers,
            json={"rain_threshold_mm": 0.75},
        )
        assert resp.status_code == 200

        remaining_after_edit = route_conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_pairs"
            " WHERE site_id=? AND variable='precip'",
            (route_site_id,),
        ).fetchone()["n"]
        assert remaining_after_edit == 0, (
            "G14 -> at this assertion: correct = 0 (update_site's rain "
            "delete removes the same (and only the) precip rows a "
            f"correctly-scoped reader sees), mutant = {remaining_after_edit!r}"
        )


# ---------------------------------------------------------------------------
# TZ5 -- the 07:00 record readers always resolve the site's one kept
# generation: a day recorded under a now-retired generation reads as
# having no row; a day under the kept generation reads complete. M36.
# ---------------------------------------------------------------------------


def test_tz5_record_readers_bind_the_kept_generation_not_the_instant() -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    g1 = _insert_generation(
        conn, site_id=site_id, state="published", effective_to="2029-06-01T00:00:00Z"
    )
    g2 = _insert_generation(conn, site_id=site_id, state="published")
    set_runtime_state(conn, _pointer_key(site_id), str(g2))

    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, missed_reason, methodology_version,
             app_version)
        VALUES (?, ?, 'UTC', '2029-05-01', '07:00:00',
                '2029-05-01T07:00:00Z', '2029-05-01', 'temperature', 0,
                'missed', 'no_data', 4, 'test')
        """,
        (site_id, g1),
    )
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, missed_reason, methodology_version,
             app_version)
        VALUES (?, ?, 'UTC', '2030-06-01', '07:00:00',
                '2030-06-01T07:00:00Z', '2030-06-01', 'temperature', 0,
                'missed', 'no_data', 4, 'test')
        """,
        (site_id, g2),
    )

    from wxverify.verification.record import (
        current_generation_readonly,
        record_day_has_any_row,
    )

    # Today's resolver still honours the OLD generation's effective interval
    # for an instant inside it, which finds the g1-dated row -- the exact
    # bug this row pins against.
    old_instant_generation = current_generation_readonly(
        conn, site_id, "2029-05-01T07:00:00Z"
    )
    has_old_row = record_day_has_any_row(
        conn, site_id, old_instant_generation, "2029-05-01"
    )
    assert has_old_row is False, (
        "M36 -> at this assertion: correct = False (the old-generation day "
        "reads as having no row once the resolver always binds the site's "
        f"ONE kept generation, g2={g2}), mutant/today's resolver = "
        f"{has_old_row!r} (resolves to g1={g1} via the per-instant interval "
        "and finds the row there)"
    )

    new_instant_generation = current_generation_readonly(
        conn, site_id, "2030-06-01T07:00:00Z"
    )
    has_new_row = record_day_has_any_row(
        conn, site_id, new_instant_generation, "2030-06-01"
    )
    assert has_new_row is True, (
        "M36 -> at this assertion: correct = True for the g2-dated day "
        f"(same answer with or without the fix), mutant = {has_new_row!r}"
    )

    # run_record_gap_scan's origin (plan G18) reads
    # "MIN(snapshot_local_date) FROM forecast_of_record WHERE
    # tz_generation_id = ?" with that generation_id resolved by
    # _current_generation (verification/record.py:263) -- the SAME
    # per-instant resolver used above, and the writer-path twin of
    # current_generation_readonly. Once it always binds the site's one
    # kept generation (G18's fix), the origin's generation is the same
    # whatever instant triggered the scan; today it still varies with the
    # instant via the old generation's effective interval, which is the
    # exact defect this pins.
    from wxverify.verification.record import _current_generation

    origin_gen_old_instant = _current_generation(conn, site_id, "2029-05-01T07:00:00Z")
    origin_gen_new_instant = _current_generation(conn, site_id, "2030-06-01T07:00:00Z")
    assert origin_gen_old_instant == origin_gen_new_instant == g2, (
        "M36 -> at this assertion: correct = "
        f"g2={g2} for BOTH instants (the gap scan's origin always binds "
        "the site's one kept generation, so its answer is unchanged "
        f"across the switch), mutant/today's resolver = old={origin_gen_old_instant!r} "
        f"new={origin_gen_new_instant!r} (the old instant still resolves to g1)"
    )


# ---------------------------------------------------------------------------
# TZ6 -- stage 4's truth write resolves the site's one kept generation even
# after its pointer is gone (A4 deletes it); a second write replaces the
# first's rows under the same generation. M37.
# ---------------------------------------------------------------------------


def test_tz6_write_close_truth_resolves_without_the_pointer() -> None:
    from tests.helpers import NOW, D, day_close_site

    conn = _conn()
    site_id = day_close_site(conn, timezone="UTC", close_on=True, with_records=True)
    generation_id = site_generation_id(conn, site_id)
    assert generation_id is not None

    # A non-published generation with a HIGHER id than the site's published
    # one -- M37's query (no ``state = 'published'`` filter, highest id
    # wins) would resolve to THIS row instead, so write_close_truth's rows
    # would land under the wrong generation. Without this second row the
    # site has only its one published generation, so M37's unfiltered
    # "highest id" query happens to agree with the correct "published"
    # query and the mutant survives.
    conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (?, 'UTC', 'initial', 'building')",
        (site_id,),
    )

    closing_date = D - timedelta(days=1)
    begin_close(conn, site_id, closing_date, now_utc=NOW)
    record = day_record(conn, site_id, closing_date)
    assert record is not None and record.state == "closing"

    # A4 deletes the site's pointer unconditionally (plan §8.4) -- simulated
    # directly since wxverify.db.switch does not exist yet; the GENERATION
    # row itself (kept whole by A1) is untouched.
    delete_runtime_state(conn, _pointer_key(site_id))

    bounds = local_day_bounds(closing_date, "UTC")
    evaluation = evaluate_day_window(
        conn, site_id=site_id, local_date=closing_date, timezone="UTC", bounds=bounds
    )

    try:
        write_close_truth(
            conn,
            evaluation,
            site_id=site_id,
            admission_basis="complete",
            now_utc=NOW,
        )
    except ValueError as exc:
        pytest.fail(
            "M37 -> at this assertion: correct = write_close_truth succeeds, "
            f"resolving the site's one kept generation ({generation_id}) "
            "without reading the (now-deleted) pointer; mutant/today's code "
            f"= raises {exc!r} because site_generation_id returns None"
        )

    rows = conn.execute(
        "SELECT tz_generation_id FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, closing_date.isoformat()),
    ).fetchall()
    assert rows, "write_close_truth must have written at least one row"
    tz_ids = {r["tz_generation_id"] for r in rows}
    assert tz_ids == {generation_id}, (
        "M37 -> at this assertion: correct = "
        f"{{{generation_id!r}}} (the write lands under the site's published "
        f"generation), mutant = {tz_ids!r} (lands under the highest-id "
        "generation regardless of state)"
    )

    # A second write replaces the first's rows under the same generation.
    write_close_truth(
        conn, evaluation, site_id=site_id, admission_basis="complete", now_utc=NOW
    )
    rows_after = conn.execute(
        "SELECT tz_generation_id FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, closing_date.isoformat()),
    ).fetchall()
    assert len(rows_after) == len(rows)
    tz_ids_after = {r["tz_generation_id"] for r in rows_after}
    assert tz_ids_after == {generation_id}, (
        "M37 -> at this assertion: correct = "
        f"{{{generation_id!r}}} (the replaced rows stay under the site's "
        f"published generation), mutant = {tz_ids_after!r} (lands under the "
        "highest-id generation regardless of state)"
    )


# ---------------------------------------------------------------------------
# TZ7 -- create_site makes exactly one generation, found the same way by
# both of db.switch's new accessors; delete_site removes it with the site.
# M38.
# ---------------------------------------------------------------------------


def test_tz7_create_site_makes_one_generation_delete_site_removes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    def _init_tmp_db(path: Path) -> sqlite3.Connection:
        close_db()
        db_path = path / "wxverify.db"
        config.db_path = str(db_path)
        options_path = path / "options.json"
        options_path.write_text("{}", encoding="utf-8")
        config.options_path = str(options_path)
        db = init_db(str(db_path))
        return db._conn  # noqa: SLF001

    _init_tmp_db(tmp_path)

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        conn = get_db()._conn  # noqa: SLF001
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        resp = client.post(
            "/api/sites",
            headers=headers,
            json={
                "name": "tz7-site",
                "forecast_lat": 10.0,
                "forecast_lon": 20.0,
                "elevation_m": 100.0,
                "timezone": "UTC",
                "rain_threshold_mm": 0.5,
            },
        )
        assert resp.status_code == 200
        site_id = int(resp.json()["id"])

        rows_before = conn.execute(
            "SELECT id, state FROM timezone_generations WHERE site_id=?",
            (site_id,),
        ).fetchall()
        assert len(rows_before) == 1, (
            "M38 -> at this assertion: correct = 1 generation row for the "
            f"new site, mutant = {len(rows_before)!r} (create_site made none)"
        )
        assert rows_before[0]["state"] == "published"

        from wxverify.db.switch import ensure_site_generation, site_generation_id

        read_id = site_generation_id(conn, site_id)
        ensured_id = ensure_site_generation(conn, site_id)
        assert read_id == ensured_id == int(rows_before[0]["id"]), (
            "M38 -> at this assertion: correct = both db.switch accessors "
            f"return {rows_before[0]['id']!r}, mutant/actual = "
            f"read={read_id!r} ensured={ensured_id!r}"
        )

        resp2 = client.delete(f"/api/sites/{site_id}", headers=headers)
        assert resp2.status_code == 200

        # The TestClient lifespan closes the Database on exit (init_db
        # re-opens it fresh), so this read must happen while the block is
        # still open -- the same connection taken above is dead once it
        # exits.
        rows_after = conn.execute(
            "SELECT id FROM timezone_generations WHERE site_id=?", (site_id,)
        ).fetchall()
        assert rows_after == [], (
            "the delete must remove the site's generation along with the "
            f"site (cascade); still present = {rows_after!r}"
        )


# ---------------------------------------------------------------------------
# TZ8 -- the run-freshness fingerprints resolve the kept generation even
# after the pointer is gone, so they are unchanged across the switch. M37.
# ---------------------------------------------------------------------------


def test_tz8_fingerprints_unchanged_once_the_pointer_is_gone() -> None:
    conn = _conn()
    site_id = _insert_site(conn)
    ensure_site_generation(conn, site_id)
    published_id = site_generation_id(conn, site_id)
    assert published_id is not None

    # A non-published generation with a HIGHER id than the site's published
    # one -- M37's query (no ``state = 'published'`` filter, highest id
    # wins) would resolve to THIS row instead. With only the one published
    # generation present, M37's unfiltered query happens to agree with the
    # correct "published" query (there is nothing else to pick), so the
    # mutant would otherwise survive.
    conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (?, 'UTC', 'initial', 'building')",
        (site_id,),
    )

    from wxverify.verification.runs import (
        _config_snapshot,
        current_result_basis_fingerprint,
        result_basis_fingerprint,
    )

    period_start = isoformat_utc(datetime(2030, 1, 1, tzinfo=UTC))
    period_end = isoformat_utc(datetime(2030, 1, 2, tzinfo=UTC))

    before = current_result_basis_fingerprint(
        conn, site_id, period_start=period_start, period_end=period_end
    )
    assert before is not None

    # The ground-truth oracle: the fingerprint computed directly under the
    # KNOWN published generation id. The before/after comparison below is
    # pointer-independent either way (site_generation_id never reads the
    # pointer), so it alone cannot distinguish M37 -- this comparison to an
    # externally-known-correct id is the one that can.
    expected_snapshot = _config_snapshot(conn, site_id, tz_generation_id=published_id)
    expected_fp = result_basis_fingerprint(
        conn,
        site_id,
        expected_snapshot,
        period_start=period_start,
        period_end=period_end,
    )
    assert before == expected_fp, (
        "M37 -> at this assertion: correct = "
        f"{expected_fp!r} (the fingerprint computed under the site's "
        f"published generation {published_id}), mutant = {before!r} "
        "(computed under the highest-id generation regardless of state)"
    )

    # A4 deletes the site's pointer unconditionally (plan §8.4); the
    # generation row itself (kept whole by A1) is untouched.
    delete_runtime_state(conn, _pointer_key(site_id))

    after = current_result_basis_fingerprint(
        conn, site_id, period_start=period_start, period_end=period_end
    )
    assert after == before, (
        "the fingerprint must not change once the pointer is gone, "
        f"since site_generation_id never read it; before = {before!r}, "
        f"after = {after!r}"
    )
