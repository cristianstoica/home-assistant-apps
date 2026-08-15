"""Unit tests for the shared published-generation accessor (db.tz_generations).

Pure-helper coverage only: seeding idempotency, pointer key shape, missing
site rejection, and the in-statement clause selecting exactly the published
generation's rows. Migration proofs and the DST oracle families live with
qa-engineer's suites. All fixture data is synthetic.
"""

from __future__ import annotations

import sqlite3

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_clause,
    published_generation_id,
    published_pointer_key,
)
from wxverify.scoring.leaderboard import leaderboard


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_pointer_key_shape() -> None:
    assert published_pointer_key(7) == "tz_generation_published:7"


def test_ensure_seeds_initial_published_generation_and_pointer() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Seed Site")
    assert published_generation_id(conn, site_id) is None

    generation_id = ensure_published_generation(conn, site_id)

    assert published_generation_id(conn, site_id) == generation_id
    row = conn.execute(
        "SELECT * FROM timezone_generations WHERE id=?", (generation_id,)
    ).fetchone()
    assert row["site_id"] == site_id
    assert row["timezone"] == "UTC"
    assert row["mode"] == "initial"
    assert row["state"] == "published"
    assert row["published_at"] is not None
    assert row["effective_from"] is None
    assert row["effective_to"] is None


def test_ensure_is_idempotent() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Idempotent Site")
    first = ensure_published_generation(conn, site_id)
    second = ensure_published_generation(conn, site_id)
    assert first == second
    count = conn.execute(
        "SELECT COUNT(*) AS n FROM timezone_generations WHERE site_id=?",
        (site_id,),
    ).fetchone()["n"]
    assert count == 1


def test_ensure_rejects_missing_site() -> None:
    conn = _conn()
    with pytest.raises(ValueError, match="site 999 does not exist"):
        ensure_published_generation(conn, 999)


def test_clause_selects_only_published_generation_rows() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Clause Site")
    feed_id = int(
        conn.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()["id"]
    )
    published = ensure_published_generation(conn, site_id)
    # A second, unpublished (building) generation alongside the published one.
    building = int(
        conn.execute(
            """
            INSERT INTO timezone_generations (site_id, timezone, mode, state)
            VALUES (?, 'UTC', 'retrospective_correction', 'building')
            """,
            (site_id,),
        ).lastrowid  # type: ignore[arg-type]
    )
    for generation_id, issued_at in (
        (published, "2035-01-01T00:00:00Z"),
        (building, "2035-01-01T01:00:00Z"),
    ):
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, tz_generation_id)
            VALUES (?, ?, 'temperature', ?, '2035-01-02T00:00:00Z', 24, 1,
                    11.0, 10.0, ?)
            """,
            (site_id, feed_id, issued_at, generation_id),
        )

    rows = conn.execute(
        f"""
        SELECT tz_generation_id FROM forecast_pairs
        WHERE site_id=? AND {published_generation_clause("forecast_pairs")}
        """,
        (site_id,),
    ).fetchall()
    assert [int(row["tz_generation_id"]) for row in rows] == [published]


# ---------------------------------------------------------------------------
# Published-binding isolation through the PRODUCTION readers (leaderboard +
# metrics path), not just the raw clause: with a second building generation
# holding pairs for the same identities, the live leaderboard sees only
# published-generation rows; flipping the pointer (simulating a publish flip)
# makes the same readers see the new generation.
#
# Value design (each mutation lands on a different asserted number):
#   published:  feed sq_error=1,  persistence sq_error=4   -> skill 0.75, mae 1.0
#   building:   feed sq_error=16, persistence sq_error=25  -> skill 0.36, mae 4.0
# A dropped generation binding in the aggregate mixes both (n=4, mae=2.5);
# a dropped binding on either side of the paired-skill join shifts skill off
# both 0.75 and 0.36.
# ---------------------------------------------------------------------------


def _make_subscribed_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes)
        VALUES ('example-src', ?, 1, 360)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_scored_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    error: float,
    generation_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 24, 1,
                ?, 10.0, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            valid_at,
            10.0 + error,
            error,
            abs(error),
            error * error,
            generation_id,
        ),
    )


def _isolation_fixture(
    conn: sqlite3.Connection,
) -> tuple[int, int, int, int, int]:
    """Site with published+building generations holding rival pair sets.

    Returns ``(site_id, feed_x, feed_y, published_id, building_id)``.
    """
    site_id = _make_site(conn, "Isolation Site")
    feed_x = _make_subscribed_feed(conn, "feed-x")
    feed_y = _make_subscribed_feed(conn, "feed-y")
    persistence_feed = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )
    published_id = ensure_published_generation(conn, site_id)
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    building_id = int(cur.lastrowid)

    for valid_at in ("2035-01-02T00:00:00Z", "2035-01-02T01:00:00Z"):
        # Published generation: feed error 1, persistence error 2.
        _insert_scored_pair(
            conn,
            site_id=site_id,
            feed_id=feed_x,
            valid_at=valid_at,
            error=1.0,
            generation_id=published_id,
        )
        _insert_scored_pair(
            conn,
            site_id=site_id,
            feed_id=persistence_feed,
            valid_at=valid_at,
            error=2.0,
            generation_id=published_id,
        )
        # Building generation, SAME identities: feed error 4, persistence 5.
        _insert_scored_pair(
            conn,
            site_id=site_id,
            feed_id=feed_x,
            valid_at=valid_at,
            error=4.0,
            generation_id=building_id,
        )
        _insert_scored_pair(
            conn,
            site_id=site_id,
            feed_id=persistence_feed,
            valid_at=valid_at,
            error=5.0,
            generation_id=building_id,
        )
    # feed_y exists ONLY in the building generation: any reader that leaks
    # unpublished rows surfaces it before the flip.
    _insert_scored_pair(
        conn,
        site_id=site_id,
        feed_id=feed_y,
        valid_at="2035-01-02T00:00:00Z",
        error=3.0,
        generation_id=building_id,
    )
    return site_id, feed_x, feed_y, published_id, building_id


def test_leaderboard_reads_only_the_published_generation() -> None:
    conn = _conn()
    site_id, feed_x, feed_y, _published_id, _building_id = _isolation_fixture(conn)

    rows = {
        row.feed_id: row
        for row in leaderboard(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="7d"
        )
    }

    assert feed_y not in rows, (
        "a feed whose pairs live only in a building generation must be "
        "invisible to the live leaderboard"
    )
    row = rows[feed_x]
    assert row.n == 2
    assert row.mae == pytest.approx(1.0)
    # Persistence-MSE skill from published rows only: 1 - 1/4.
    assert row.skill_score == pytest.approx(0.75)


def test_pointer_flip_switches_readers_to_the_new_generation() -> None:
    conn = _conn()
    site_id, feed_x, feed_y, _published_id, building_id = _isolation_fixture(conn)

    # Simulate the completing publish transaction's pointer flip.
    set_runtime_state(conn, published_pointer_key(site_id), str(building_id))

    rows = {
        row.feed_id: row
        for row in leaderboard(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="7d"
        )
    }

    row = rows[feed_x]
    assert row.n == 2
    assert row.mae == pytest.approx(4.0)
    # 1 - 16/25 from the building generation's rows only.
    assert row.skill_score == pytest.approx(0.36)
    # The building-only feed becomes visible exactly at the flip.
    assert feed_y in rows
    assert rows[feed_y].n == 1


def test_clause_resolves_pointer_per_row_in_cross_site_statements() -> None:
    """F2 cross-site binding oracle: the clause correlates the pointer probe
    on EACH row's own site_id (``'tz_generation_published:' || alias.site_id``).
    Two sites with DIFFERENT published pointer values, plus decoy rows tagged
    with the other site's generation, make a per-statement (single-pointer)
    resolution return the wrong row set in one bare cross-site SELECT.
    """
    conn = _conn()
    site_a = _make_site(conn, "Cross Site A")
    site_b = _make_site(conn, "Cross Site B")
    feed_id = int(
        conn.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()["id"]
    )

    gen_a = ensure_published_generation(conn, site_a)
    gen_b_initial = ensure_published_generation(conn, site_b)
    # A second published generation for site B, pointer flipped onto it: B's
    # live pointer value is now distinct from A's AND from B's retired seed,
    # so "resolve one pointer for the whole statement" and "match any of the
    # site's generations" both diverge from the correct per-row binding.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, 'UTC', 'retrospective_correction', 'published',
                '2035-01-01T00:00:00Z')
        """,
        (site_b,),
    )
    assert cur.lastrowid is not None
    gen_b = int(cur.lastrowid)
    set_runtime_state(conn, published_pointer_key(site_b), str(gen_b))
    assert len({gen_a, gen_b_initial, gen_b}) == 3

    def insert_pair(site_id: int, generation_id: int, forecast: float) -> None:
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, tz_generation_id)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                    '2035-01-02T00:00:00Z', 24, 1, ?, 10.0, ?)
            """,
            (site_id, feed_id, forecast, generation_id),
        )

    # Real rows: each site under its OWN published pointer's generation.
    insert_pair(site_a, gen_a, 1.0)
    insert_pair(site_b, gen_b, 2.0)
    # Decoys: each site under the OTHER site's pointer value — exactly the
    # rows a hardcoded/single-pointer resolution would admit — plus site B
    # under its own retired initial generation.
    insert_pair(site_a, gen_b, 91.0)
    insert_pair(site_b, gen_a, 92.0)
    insert_pair(site_b, gen_b_initial, 93.0)

    # ONE cross-site statement (no site_id filter) through the clause.
    rows = conn.execute(
        f"""
        SELECT fp.site_id AS site_id, fp.tz_generation_id AS gen_id,
               fp.forecast AS forecast
        FROM forecast_pairs fp
        WHERE {published_generation_clause("fp")}
        """
    ).fetchall()
    got = {
        (int(row["site_id"]), int(row["gen_id"]), float(row["forecast"]))
        for row in rows
    }
    assert got == {
        (site_a, gen_a, 1.0),
        (site_b, gen_b, 2.0),
    }, "each site must bind to its OWN pointer value, with no decoys admitted"


def test_clause_matches_nothing_when_pointer_absent() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Unseeded Site")
    rows = conn.execute(
        f"""
        SELECT 1 FROM forecast_pairs fp
        WHERE fp.site_id=? AND {published_generation_clause("fp")}
        """,
        (site_id,),
    ).fetchall()
    assert rows == []
