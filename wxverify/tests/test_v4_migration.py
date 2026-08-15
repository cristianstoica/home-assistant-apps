"""Schema v4 migration proof (§18.14): a genuine ``user_version = 3`` database
upgraded through the production chain.

Mirrors ``test_sm1_migration.py``'s fixture approach: the v3-era tables that
``migrate_v4`` rebuilds or reads are hand-written in their REAL v3 shape (old
jobs CHECK, ``forecast_pairs`` without ``first_known_at``/``tz_generation_id``
and with the five-column UNIQUE, the pre-v4 ``idx_pairs_winrate``), seeded
with representative synthetic data, then migrated via ``init_db`` — the
production boot path — never by hand-driving individual migration steps.

All fixture data is synthetic placeholder data (fake names/IDs only).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import TARGET_USER_VERSION
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.tz_generations import published_pointer_key

# ---------------------------------------------------------------------------
# v3-era DDL — the shapes migrate_v4 must find on disk.
# ---------------------------------------------------------------------------

_V3_SITES_DDL = """\
CREATE TABLE sites (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    forecast_lat REAL NOT NULL,
    forecast_lon REAL NOT NULL,
    elevation_m REAL NOT NULL,
    timezone TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    rain_threshold_mm REAL NOT NULL DEFAULT 0.2 CHECK(rain_threshold_mm >= 0),
    last_obs_at TEXT,
    backfill_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(backfill_status IN ('pending','in_progress','complete')),
    backfill_through TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

_V3_FEEDS_DDL = """\
CREATE TABLE feeds (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    model TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    disabled_reason TEXT,
    default_subscribed INTEGER NOT NULL DEFAULT 0
        CHECK(default_subscribed IN (0,1)),
    fetch_interval_minutes INTEGER NOT NULL,
    max_lead_hours INTEGER NOT NULL DEFAULT 168,
    is_virtual INTEGER NOT NULL DEFAULT 0 CHECK(is_virtual IN (0,1)),
    UNIQUE(source, model)
);
"""

# v3 forecast_pairs: no first_known_at, no tz_generation_id, five-column
# UNIQUE, and idx_pairs_winrate WITHOUT the trailing tz_generation_id column.
_V3_PAIRS_DDL = """\
CREATE TABLE forecast_pairs (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
    variable TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    valid_at TEXT NOT NULL,
    lead_hours INTEGER NOT NULL CHECK(lead_hours >= 1),
    day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
    forecast REAL NOT NULL,
    observed REAL NOT NULL,
    error REAL,
    abs_error REAL,
    sq_error REAL,
    cat_hit INTEGER,
    cat_false INTEGER,
    cat_miss INTEGER,
    cat_correct_neg INTEGER,
    rain_threshold_mm REAL,
    contributors INTEGER,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
);
CREATE INDEX idx_pairs_leaderboard
    ON forecast_pairs(site_id, variable, day_ahead, valid_at);
CREATE INDEX idx_pairs_cell
    ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);
CREATE INDEX idx_pairs_winrate
    ON forecast_pairs(site_id, variable, day_ahead, feed_id,
                      valid_at, issued_at DESC, abs_error);
"""

_V3_JOBS_DDL = """\
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY,
    type TEXT NOT NULL,
    site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
    job_key TEXT,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','running','completed','failed')),
    next_attempt_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 5,
    last_error TEXT,
    result TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK (
        (type = 'catchup' AND site_id IS NULL)
        OR (
            type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                     'pair_and_score','backfill_site')
            AND site_id IS NOT NULL
        )
    )
);
CREATE UNIQUE INDEX idx_jobs_active_dedupe
    ON jobs(type, COALESCE(site_id, -1), job_key)
    WHERE status IN ('pending','running') AND job_key IS NOT NULL;
CREATE INDEX idx_jobs_type_key_site
    ON jobs(type, job_key, site_id, id);
"""

_V3_SAMPLES_DDL = """\
CREATE TABLE forecast_samples (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
    variable TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    valid_at TEXT NOT NULL,
    lead_hours INTEGER NOT NULL CHECK(lead_hours >= 1),
    value REAL NOT NULL,
    source_raw TEXT NOT NULL,
    model_run_id TEXT NOT NULL,
    fetched_at TEXT,
    UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
);
"""

_V3_OBSERVATIONS_DDL = """\
CREATE TABLE observations (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    variable TEXT NOT NULL,
    valid_at TEXT NOT NULL,
    value REAL NOT NULL,
    n_stations INTEGER NOT NULL,
    rejected_stations INTEGER NOT NULL DEFAULT 0,
    computed_at TEXT,
    UNIQUE(site_id, variable, valid_at)
);
"""

_V3_SCORE_CACHE_DDL = """\
CREATE TABLE score_cache (
    site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
    variable TEXT NOT NULL,
    day_ahead INTEGER NOT NULL,
    window_key TEXT NOT NULL,
    n INTEGER NOT NULL,
    bias REAL,
    mae REAL,
    rmse REAL,
    pod REAL,
    far REAL,
    csi REAL,
    ets REAL,
    hss REAL,
    skill_score REAL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY(site_id, feed_id, variable, day_ahead, window_key)
);
"""

_V3_RUNTIME_STATE_DDL = """\
CREATE TABLE runtime_state (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

# Columns shared by the v3 and v4 forecast_pairs shapes — the content the
# rebuild must carry over byte-for-byte.
_CARRIED_PAIR_COLUMNS = (
    "id",
    "site_id",
    "feed_id",
    "variable",
    "issued_at",
    "valid_at",
    "lead_hours",
    "day_ahead",
    "forecast",
    "observed",
    "error",
    "abs_error",
    "sq_error",
    "cat_hit",
    "cat_false",
    "cat_miss",
    "cat_correct_neg",
    "rain_threshold_mm",
    "contributors",
    "created_at",
)

_NEW_JOB_TYPES = (
    "forecast_record",
    "record_gap_scan",
    "verification_run",
    "timezone_correction",
)


def _build_v3_db(db_path: Path) -> tuple[dict[int, str], list[dict[str, object]]]:
    """Write a representative v3 database to disk.

    Returns ``(site timezones by id, pre-migration forecast_pairs snapshot)``.
    """
    raw = sqlite3.connect(str(db_path), isolation_level=None)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA foreign_keys=ON")
    for ddl in (
        _V3_SITES_DDL,
        _V3_FEEDS_DDL,
        _V3_SAMPLES_DDL,
        _V3_OBSERVATIONS_DDL,
        _V3_PAIRS_DDL,
        _V3_JOBS_DDL,
        _V3_SCORE_CACHE_DDL,
        _V3_RUNTIME_STATE_DDL,
    ):
        raw.executescript(ddl)

    site_timezones: dict[int, str] = {}
    for name, timezone in (
        ("SITE-ALPHA", "Europe/Athens"),
        ("SITE-BETA", "UTC"),
    ):
        raw.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES (?, 40.0, -105.0, 900.0, ?)",
            (name, timezone),
        )
        site_id = int(raw.execute("SELECT last_insert_rowid()").fetchone()[0])
        site_timezones[site_id] = timezone
    site_a, site_b = sorted(site_timezones)

    raw.execute(
        "INSERT INTO feeds (id, source, model, default_subscribed,"
        " fetch_interval_minutes) VALUES (901, 'example-src', 'model-x', 1, 360)"
    )
    raw.execute(
        "INSERT INTO feeds (id, source, model, fetch_interval_minutes,"
        " max_lead_hours, is_virtual)"
        " VALUES (902, 'virtual', '_persistence', 360, 48, 1)"
    )

    # Representative surrounding data: samples + observations for site A.
    raw.execute(
        "INSERT INTO forecast_samples (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, 901, 'temperature', '2026-01-01T00:00:00Z',"
        " '2026-01-01T06:00:00Z', 6, 5.0, '{}', 'run-1',"
        " '2026-01-01T00:05:00Z')",
        (site_a,),
    )
    raw.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations, computed_at) VALUES (?, 'temperature',"
        " '2026-01-01T06:00:00Z', 4.0, 3, '2026-01-01T07:00:00Z')",
        (site_a,),
    )

    # forecast_pairs: real-model pairs for BOTH sites plus a persistence pair
    # (feed 902) — the backfill must hit every one of them.
    pair_seeds = (
        (site_a, 901, "temperature", "2026-01-01T00:00:00Z", "2026-01-01T06:00:00Z"),
        (site_a, 901, "temperature", "2026-01-01T00:00:00Z", "2026-01-01T07:00:00Z"),
        (site_a, 902, "temperature", "2026-01-01T05:00:00Z", "2026-01-01T06:00:00Z"),
        (site_b, 901, "precip", "2026-01-02T00:00:00Z", "2026-01-02T06:00:00Z"),
    )
    for pair_site, feed_id, variable, issued_at, valid_at in pair_seeds:
        raw.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 rain_threshold_mm)
            VALUES (?, ?, ?, ?, ?, 6, 0, 5.0, 4.0, 1.0, 1.0, 1.0, ?)
            """,
            (
                pair_site,
                feed_id,
                variable,
                issued_at,
                valid_at,
                0.2 if variable == "precip" else None,
            ),
        )

    raw.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('fetch_feed', ?, 'pre-v4-job', 'pending')",
        (site_a,),
    )
    raw.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('catchup', NULL, 'pre-v4-catchup', 'completed')"
    )
    raw.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('unrelated_key', 'keepme')"
    )
    raw.execute(
        "INSERT INTO score_cache (site_id, feed_id, variable, day_ahead,"
        " window_key, n, computed_at)"
        " VALUES (?, 901, 'temperature', 0, 'w:30', 2, '2026-01-01T08:00:00Z')",
        (site_a,),
    )

    snapshot = [
        {column: row[column] for column in _CARRIED_PAIR_COLUMNS}
        for row in raw.execute("SELECT * FROM forecast_pairs ORDER BY id").fetchall()
    ]
    assert len(snapshot) == len(pair_seeds)

    raw.execute("PRAGMA user_version = 3")
    raw.close()
    return site_timezones, snapshot


def _migrate(
    tmp_path: Path,
) -> tuple[sqlite3.Connection, dict[int, str], list[dict[str, object]]]:
    """Build the v3 fixture and run the production boot-path migration."""
    db_path = tmp_path / "wxverify-v3.db"
    site_timezones, snapshot = _build_v3_db(db_path)
    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn, site_timezones, snapshot  # noqa: SLF001


def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def test_v3_to_v4_seeds_one_published_initial_generation_per_site(
    tmp_path: Path,
) -> None:
    conn, site_timezones, _snapshot = _migrate(tmp_path)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION
    assert TARGET_USER_VERSION == 4

    for site_id, timezone in site_timezones.items():
        rows = conn.execute(
            "SELECT * FROM timezone_generations WHERE site_id=?", (site_id,)
        ).fetchall()
        assert len(rows) == 1, (
            f"site {site_id} must get exactly one seeded generation, got {len(rows)}"
        )
        generation = rows[0]
        assert generation["mode"] == "initial"
        assert generation["state"] == "published"
        assert generation["timezone"] == timezone, (
            "seeded generation must copy the SITE's timezone, not a fixed value"
        )
        # NULL-bounded interval: initial generations cover the whole history.
        assert generation["effective_from"] is None
        assert generation["effective_to"] is None
        assert generation["published_at"] is not None

        pointer = conn.execute(
            "SELECT value FROM runtime_state WHERE key=?",
            (published_pointer_key(site_id),),
        ).fetchone()
        assert pointer is not None, f"site {site_id} must get a published pointer"
        assert int(pointer["value"]) == int(generation["id"])

    # Pre-existing unrelated runtime_state row untouched by the pointer upsert.
    unrelated = conn.execute(
        "SELECT value FROM runtime_state WHERE key='unrelated_key'"
    ).fetchone()
    assert unrelated is not None
    assert unrelated["value"] == "keepme"


def test_v3_to_v4_backfills_every_pair_and_invents_no_availability(
    tmp_path: Path,
) -> None:
    conn, site_timezones, snapshot = _migrate(tmp_path)

    generation_by_site = {
        int(row["site_id"]): int(row["id"])
        for row in conn.execute(
            "SELECT id, site_id FROM timezone_generations"
        ).fetchall()
    }
    migrated = conn.execute("SELECT * FROM forecast_pairs ORDER BY id").fetchall()

    # None lost, none duplicated.
    assert len(migrated) == len(snapshot)

    for before, after in zip(snapshot, migrated, strict=True):
        # Full content spot-check on every carried column, including id.
        for column in _CARRIED_PAIR_COLUMNS:
            assert after[column] == before[column], (
                f"pair id={before['id']} column {column} changed in the rebuild"
            )
        # Backfilled to the row's OWN site's seeded initial generation —
        # two sites with distinct generations make a cross-site mixup fail.
        assert (
            int(after["tz_generation_id"]) == generation_by_site[int(after["site_id"])]
        )
        # No invented availability: migrated rows must stay NULL.
        assert after["first_known_at"] is None, (
            "migrate_v4 must never invent first_known_at for pre-existing pairs"
        )
    assert len({int(s["site_id"]) for s in snapshot}) == 2, (
        "fixture must cover pairs on more than one site or the per-site "
        "generation assertion above is vacuous"
    )


def test_v3_to_v4_unique_key_now_includes_generation(tmp_path: Path) -> None:
    conn, _site_timezones, snapshot = _migrate(tmp_path)
    first = snapshot[0]
    site_id = int(str(first["site_id"]))

    published_generation = int(
        conn.execute(
            "SELECT id FROM timezone_generations WHERE site_id=?", (site_id,)
        ).fetchone()["id"]
    )

    def insert_pair(generation_id: int) -> None:
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, 6, 0, 9.0, 8.0, ?)
            """,
            (
                site_id,
                first["feed_id"],
                first["variable"],
                first["issued_at"],
                first["valid_at"],
                generation_id,
            ),
        )

    # Same identity + same generation: rejected.
    with pytest.raises(sqlite3.IntegrityError):
        insert_pair(published_generation)

    # Same identity + a DIFFERENT generation: admitted. This is the widened
    # unique key doing real work — under the v3 five-column UNIQUE this
    # insert would raise exactly like the one above.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'Europe/Athens', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    insert_pair(int(cur.lastrowid))
    count = conn.execute(
        """
        SELECT COUNT(*) AS n FROM forecast_pairs
        WHERE site_id=? AND feed_id=? AND variable=? AND issued_at=? AND valid_at=?
        """,
        (
            site_id,
            first["feed_id"],
            first["variable"],
            first["issued_at"],
            first["valid_at"],
        ),
    ).fetchone()["n"]
    assert int(count) == 2


def test_v3_to_v4_jobs_check_admits_new_types_via_enqueue(tmp_path: Path) -> None:
    conn, site_timezones, _snapshot = _migrate(tmp_path)
    site_id = sorted(site_timezones)[0]

    # Each new per-site type enqueues through the production path.
    for job_type in _NEW_JOB_TYPES:
        result = enqueue_if_absent(
            conn, job_type, site_id, f"probe-{job_type}", {"probe": True}
        )
        assert result.created is True, (
            f"jobs CHECK must admit {job_type} after migrate_v4"
        )

    # catchup stays enqueueable site-NULL. The fixture's pre-existing catchup
    # row is 'completed', so dedupe does not mask this insert.
    catchup = enqueue_if_absent(conn, "catchup", None, "post-v4-catchup")
    assert catchup.created is True

    # An unknown type is still rejected by the CHECK: enqueue_if_absent
    # swallows the IntegrityError into created=False — assert both that and
    # the absence of any row, so the rejection is real, not a dedupe.
    bogus = enqueue_if_absent(conn, "bogus_type", site_id, "bogus-probe")
    assert bogus.created is False
    assert conn.execute("SELECT 1 FROM jobs WHERE type='bogus_type'").fetchone() is None

    # Pre-existing jobs survived the rebuild unchanged.
    survived = conn.execute(
        "SELECT type, status FROM jobs WHERE job_key='pre-v4-job'"
    ).fetchone()
    assert survived is not None
    assert survived["type"] == "fetch_feed"
    assert survived["status"] == "pending"
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE job_key='pre-v4-catchup'").fetchone()
        is not None
    )


def test_v3_to_v4_recreates_jobs_indexes_and_keeps_score_cache(
    tmp_path: Path,
) -> None:
    conn, site_timezones, _snapshot = _migrate(tmp_path)

    assert _index_exists(conn, "idx_jobs_active_dedupe")
    assert _index_exists(conn, "idx_jobs_type_key_site")
    # The three pairs indexes come back after the pairs rebuild too.
    assert _index_exists(conn, "idx_pairs_leaderboard")
    assert _index_exists(conn, "idx_pairs_cell")
    assert _index_exists(conn, "idx_pairs_winrate")

    # idx_jobs_active_dedupe is not just present but ENFORCING: a duplicate
    # active (type, site, key) insert must be rejected.
    site_id = sorted(site_timezones)[0]
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('forecast_record', ?, 'dedupe-probe', 'pending')",
        (site_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, status)"
            " VALUES ('forecast_record', ?, 'dedupe-probe', 'pending')",
            (site_id,),
        )

    # score_cache is deliberately NOT re-keyed by migrate_v4 — the fixture
    # row must survive untouched.
    cache = conn.execute(
        "SELECT n, window_key FROM score_cache WHERE feed_id=901"
    ).fetchone()
    assert cache is not None
    assert int(cache["n"]) == 2
    assert cache["window_key"] == "w:30"


def _dump_v4_state(
    conn: sqlite3.Connection,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Full-content dumps of forecast_pairs and timezone_generations."""
    pairs = [
        {column: row[column] for column in row.keys()}  # noqa: SIM118
        for row in conn.execute("SELECT * FROM forecast_pairs ORDER BY id")
    ]
    generations = [
        {column: row[column] for column in row.keys()}  # noqa: SIM118
        for row in conn.execute("SELECT * FROM timezone_generations ORDER BY id")
    ]
    return pairs, generations


def test_migrate_v4_rerun_after_crashed_version_write_is_a_no_op(
    tmp_path: Path,
) -> None:
    """F1 re-run regression: ``run_migrations`` writes ``PRAGMA user_version``
    only AFTER migrate_v4's RELEASE commits, so a crash between the two leaves
    the v4 body fully committed with user_version still 3. Re-entry must hit
    the column-probe guard and return — without it the seed step doubles the
    initial generations and the pairs rebuild crash-loops on UNIQUE(id).
    """
    db_path = tmp_path / "wxverify-v3.db"
    site_timezones, _snapshot = _build_v3_db(db_path)
    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))

    # Snapshot the fully migrated state: every pairs row (all v4 columns,
    # including tz_generation_id and first_known_at) and every generation.
    pairs_before, generations_before = _dump_v4_state(db._conn)  # noqa: SLF001
    close_db()

    # Simulate the crash window: the migration body is on disk, but the
    # user_version bump never landed.
    raw = sqlite3.connect(str(db_path), isolation_level=None)
    raw.execute("PRAGMA user_version = 3")
    raw.close()

    # Second boot through the production path — must not raise.
    conn = init_db(str(db_path))._conn  # noqa: SLF001

    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION

    # Still exactly ONE generation per site, byte-identical to the first boot.
    for site_id in site_timezones:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM timezone_generations WHERE site_id=?",
            (site_id,),
        ).fetchone()["n"]
        assert int(count) == 1, (
            f"site {site_id}: re-entry must not double-seed initial generations"
        )
    pairs_after, generations_after = _dump_v4_state(conn)
    assert generations_after == generations_before
    # Pairs count AND content unchanged — the rebuild must not have re-run.
    assert pairs_after == pairs_before
