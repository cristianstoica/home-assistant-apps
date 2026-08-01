"""Equivalence and regression tests for the feed-health bound-seek rewrite.

Covers three surfaces that answer "does this (site, feed) pair have any
forecast samples": ``load_feed_health`` (the ``/ops`` page, a boolean
question), ``health_feeds`` (``/api/health/feeds``, an exact count that is a
published JSON field), and ``render_backfill`` (the HTMX fragment behind the
"Catch up"/"Backfill" buttons, which must not run either of the other two).

Isolation: the SQL-level tests each open their own fresh
``sqlite3.connect(":memory:")`` and run ``run_migrations`` (mirrors
``tests/test_expected_universe_equivalence.py``); the HTTP-level tests use a
file-backed ``tmp_path`` database through the real app, mirroring the pattern
in ``tests/test_m1_m5.py``.

Synthetic data only (public repo): fake site names, the repo's existing
47/25 lat-lon convention, no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from pathlib import Path
from typing import NamedTuple

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes.health import HEALTH_FEEDS_SQL
from wxverify.db.connection import close_db, get_db
from wxverify.db.migrations import run_migrations
from wxverify.web.context import FEED_HEALTH_SQL, load_feed_health

# The exact statement `load_feed_health` and `health_feeds` computed before
# this change: a full-table GROUP BY materialised as a subquery, then LEFT
# JOINed against 13-or-so display rows (`SCAN fs` over every forecast sample
# to answer a handful of questions). Frozen here as the oracle for the
# rewrite: it is deliberately NOT the code under test, and NOT updated when
# the loaders change, so it cannot silently drift to agree with a future bug.
_LEGACY_SAMPLE_COUNT_SQL = """
    SELECT s.id AS site_id, s.name AS site_name, f.id AS feed_id,
           s.enabled AS site_enabled,
           f.source, f.model, f.enabled AS feed_enabled,
           f.default_subscribed, f.disabled_reason,
           sfs.enabled AS override_enabled, sfs.last_run_at, sfs.last_error,
           sfs.error_count,
           COALESCE(sample_counts.n, 0) AS sample_count
    FROM sites s
    JOIN feeds f
    LEFT JOIN site_feed_state sfs
      ON sfs.site_id = s.id AND sfs.feed_id = f.id
    LEFT JOIN (
        SELECT fs.site_id,
               CASE
                 WHEN sf.source='meteoblue' AND sf.model!='multimodel'
                 THEN pkg.id
                 ELSE sf.id
               END AS feed_id,
               COUNT(*) AS n
        FROM forecast_samples fs
        JOIN feeds sf ON sf.id = fs.feed_id
        LEFT JOIN feeds pkg
          ON pkg.source='meteoblue' AND pkg.model='multimodel'
        GROUP BY fs.site_id, CASE
                 WHEN sf.source='meteoblue' AND sf.model!='multimodel'
                 THEN pkg.id
                 ELSE sf.id
               END
    ) sample_counts
      ON sample_counts.site_id = s.id AND sample_counts.feed_id = f.id
    WHERE f.is_virtual = 0
      AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
    ORDER BY s.name COLLATE NOCASE, f.source, f.model
    """

# main's `/api/health/feeds` copy of this statement differed in exactly one
# way: `ORDER BY s.name` with NO `COLLATE NOCASE`. That divergence is
# pre-existing and deliberate -- it fixes the order of a published JSON array
# the HA integration consumes -- so the API surface must be compared against
# its own order, not the /ops one.
_LEGACY_HEALTH_FEEDS_SQL = _LEGACY_SAMPLE_COUNT_SQL.replace(
    "ORDER BY s.name COLLATE NOCASE", "ORDER BY s.name"
)


class _FeedHealthIds(NamedTuple):
    alpha: int
    bravo: int
    charlie: int
    delta: int
    model_a_with_samples: int
    model_b_zero_samples: int
    meteoblue_member_one: int
    meteoblue_member_two: int
    virtual_feed: int
    meteoblue_package: int | None


def _seed_into(conn: sqlite3.Connection, *, package_present: bool) -> _FeedHealthIds:
    """Build a synthetic feeds/sites/samples universe that forces every
    branch the feed-health rewrite can take, on a `feeds` table cleared of
    the migration's default seeds.

    Clearing first is mandatory, not tidy: `run_migrations` seeds 15 default
    feeds against zero sites, 13 of them non-virtual and non-member, so left
    in place they add 13 rows per site of pure noise and make
    `package_present=False` unconstructible outright -- `feeds` carries
    `UNIQUE(source, model)` and one of the seeded rows already is
    `meteoblue/multimodel`. The delete must run before any insert:
    `forecast_samples.feed_id` and `site_feed_state.feed_id` are
    `ON DELETE RESTRICT`, so it only succeeds while nothing yet references
    the seeded rows.
    """
    conn.execute("DELETE FROM feeds")

    def _site(name: str, *, enabled: int) -> int:
        return int(
            conn.execute(
                """
                INSERT INTO sites
                    (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
                VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
                """,
                (name, enabled),
            ).lastrowid
        )

    alpha = _site("Feed Health Alpha", enabled=1)
    bravo = _site("Feed Health Bravo", enabled=1)
    charlie = _site("Feed Health Charlie", enabled=1)
    delta = _site("Feed Health Delta", enabled=0)

    def _feed(
        source: str, model: str, *, is_virtual: int = 0, default_subscribed: int = 1
    ) -> int:
        return int(
            conn.execute(
                """
                INSERT INTO feeds
                    (source, model, enabled, disabled_reason, default_subscribed,
                     fetch_interval_minutes, max_lead_hours, is_virtual)
                VALUES (?, ?, 1, NULL, ?, 60, 168, ?)
                """,
                (source, model, default_subscribed, is_virtual),
            ).lastrowid
        )

    model_a = _feed("provider-one", "modelA")
    model_b = _feed("provider-two", "modelB")
    member_one = _feed("meteoblue", "MEMBERONE", default_subscribed=0)
    member_two = _feed("meteoblue", "MEMBERTWO", default_subscribed=0)
    virtual_feed = _feed("virtual", "synthetic", is_virtual=1, default_subscribed=0)
    package = _feed("meteoblue", "multimodel") if package_present else None

    def _sample(site_id: int, feed_id: int, valid_at: str) -> None:
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 1,
                    5.0, '5 C', 'test-run', '2035-01-01T00:05:00Z')
            """,
            (site_id, feed_id, valid_at),
        )

    # Alpha/modelA: the ordinary row with samples.
    _sample(alpha, model_a, "2035-01-01T01:00:00Z")
    _sample(alpha, model_a, "2035-01-01T02:00:00Z")
    # Alpha/modelB: no samples anywhere -- forces the zero branch.
    # Alpha's meteoblue package, fed ONLY via its two member feeds -- pins
    # the rollup (must report n=2, not 0).
    _sample(alpha, member_one, "2035-01-01T01:00:00Z")
    _sample(alpha, member_two, "2035-01-01T01:00:00Z")
    # Charlie's meteoblue package, carrying samples on the package row
    # itself -- the non-member path through the rollup CASE.
    if package is not None:
        _sample(charlie, package, "2035-01-01T01:00:00Z")
        _sample(charlie, package, "2035-01-01T02:00:00Z")
        _sample(charlie, package, "2035-01-01T03:00:00Z")
    # Bravo: no samples at all -- every row on Bravo is zero.
    # Delta: disabled site, one sample -- a disabled site still reports its
    # count.
    _sample(delta, model_a, "2035-01-01T01:00:00Z")
    # A virtual feed's sample must be hidden and must never leak into any
    # displayed row's count.
    _sample(alpha, virtual_feed, "2035-01-01T01:00:00Z")

    # The two site_feed_state rows that reach the has_samples branch
    # (context.py's status derivation). Column values are not free choices:
    # `enabled` must be 1 EXPLICITLY, because a NULL falls back to
    # `default_subscribed` and the row short-circuits on "not subscribed /
    # available" before it ever reaches the has_samples test; `last_run_at`
    # must be non-NULL or it stops one branch earlier on "never run / due";
    # `last_error` must be NULL or it stops on the error branches.
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, enabled, last_run_at, last_error, error_count)
        VALUES (?, ?, 1, '2035-01-01T00:00:00Z', NULL, 0),
               (?, ?, 1, '2035-01-01T00:00:00Z', NULL, 0)
        """,
        (alpha, model_a, alpha, model_b),
    )

    return _FeedHealthIds(
        alpha=alpha,
        bravo=bravo,
        charlie=charlie,
        delta=delta,
        model_a_with_samples=model_a,
        model_b_zero_samples=model_b,
        meteoblue_member_one=member_one,
        meteoblue_member_two=member_two,
        virtual_feed=virtual_feed,
        meteoblue_package=package,
    )


def _seed(*, package_present: bool) -> tuple[sqlite3.Connection, _FeedHealthIds]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    ids = _seed_into(conn, package_present=package_present)
    return conn, ids


# --- The equivalence oracle -------------------------------------------------


def test_feed_health_matches_legacy_zero_classification() -> None:
    conn, _ids = _seed(package_present=True)
    legacy = conn.execute(_LEGACY_SAMPLE_COUNT_SQL).fetchall()
    new_rows = load_feed_health(conn)
    # Symmetric sequence check: a per-key lookup alone would only ever visit
    # the legacy row set, so a row present ONLY in the new result -- what a
    # dropped or weakened WHERE clause would produce -- would never be
    # visited and would leak silently onto the Feed Status panel.
    legacy_keys = [(int(r["site_id"]), int(r["feed_id"])) for r in legacy]
    new_keys = [(r.site_id, r.feed_id) for r in new_rows]
    assert new_keys == legacy_keys
    legacy_zero = {
        (int(r["site_id"]), int(r["feed_id"])): int(r["sample_count"]) == 0
        for r in legacy
    }
    for row in new_rows:
        assert legacy_zero[(row.site_id, row.feed_id)] == (not row.has_samples)


def test_health_feeds_matches_legacy_exact_counts() -> None:
    assert _LEGACY_HEALTH_FEEDS_SQL != _LEGACY_SAMPLE_COUNT_SQL, (
        "COLLATE NOCASE not found in the frozen legacy statement; the /api "
        "order oracle silently fell back to the /ops collation"
    )
    conn, _ids = _seed(package_present=True)
    legacy = conn.execute(_LEGACY_HEALTH_FEEDS_SQL).fetchall()
    api_rows = conn.execute(HEALTH_FEEDS_SQL).fetchall()
    legacy_keys = [(int(r["site_id"]), int(r["feed_id"])) for r in legacy]
    api_keys = [(int(r["site_id"]), int(r["feed_id"])) for r in api_rows]
    assert api_keys == legacy_keys
    legacy_counts = {
        (int(r["site_id"]), int(r["feed_id"])): int(r["sample_count"]) for r in legacy
    }
    for row in api_rows:
        key = (int(row["site_id"]), int(row["feed_id"]))
        assert int(row["sample_count"]) == legacy_counts[key]


def test_the_two_surfaces_order_sites_by_different_collations() -> None:
    conn, _ids = _seed(package_present=True)
    for name in ("aardvark site", "Zulu site"):
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone, enabled) VALUES (?, 47.0, 25.0, 900.0, 'UTC', 1)",
            (name,),
        )
    ops_names = [r.site_name for r in load_feed_health(conn)]
    api_names = [str(r["site_name"]) for r in conn.execute(HEALTH_FEEDS_SQL)]
    assert ops_names != api_names, (
        "fixture no longer discriminates the two collations; the /api order "
        "oracle is vacuous"
    )
    assert ops_names == [
        str(r["site_name"]) for r in conn.execute(_LEGACY_SAMPLE_COUNT_SQL)
    ]
    assert api_names == [
        str(r["site_name"]) for r in conn.execute(_LEGACY_HEALTH_FEEDS_SQL)
    ]


def test_feed_health_status_pins_the_has_samples_branch() -> None:
    conn, ids = _seed(package_present=True)
    rows = {(r.site_id, r.feed_id): r for r in load_feed_health(conn)}
    assert rows[(ids.alpha, ids.model_a_with_samples)].status == "ok"
    assert rows[(ids.alpha, ids.model_b_zero_samples)].status == "ran / no usable data"
    statuses = {r.status for r in rows.values()}
    assert {"ok", "ran / no usable data"} <= statuses, (
        "fixture never reached the has_samples branch; the status oracle is vacuous"
    )


# --- The fixture's own vacuity gate -----------------------------------------


@pytest.mark.parametrize(
    ("package_present", "expected_rows", "expected_zero"),
    [(True, 12, 8), (False, 8, 6)],
)
def test_feed_health_fixture_forces_designed_row_and_zero_counts(
    package_present: bool, expected_rows: int, expected_zero: int
) -> None:
    conn, _ids = _seed(package_present=package_present)
    legacy = conn.execute(_LEGACY_SAMPLE_COUNT_SQL).fetchall()
    zero_rows = sum(1 for row in legacy if int(row["sample_count"]) == 0)
    # Exact cardinality, not merely `zero_rows > 0`: both numbers are
    # deterministic on this fixture, and the row-count assertion is what
    # catches a future edit that stops clearing the seeded feeds (which
    # yields 60/56 and 60/57 instead -- both would satisfy a bare `> 0`
    # gate).
    assert len(legacy) == expected_rows, (
        "fixture row set drifted; seeded feeds may not be cleared"
    )
    assert zero_rows == expected_zero, "fixture no longer forces the designed zero rows"


# --- The plan-shape regression guard ----------------------------------------

_FS_LINE = re.compile(r"\bfs\b")


def _binds_site_and_feed(conn: sqlite3.Connection, sql: str) -> bool:
    lines = [
        row[3]
        for row in conn.execute("EXPLAIN QUERY PLAN " + sql)
        if _FS_LINE.search(row[3])
    ]
    # No `fs` line at all is "can't tell", which must fail rather than pass.
    return bool(lines) and all(
        ln.startswith("SEARCH fs ") and "site_id=? AND feed_id=?" in ln for ln in lines
    )


def test_feed_health_probe_binds_site_and_feed() -> None:
    conn, _ids = _seed(package_present=True)
    assert _binds_site_and_feed(conn, FEED_HEALTH_SQL)
    degraded = FEED_HEALTH_SQL.replace(
        "CROSS JOIN forecast_samples", "JOIN forecast_samples"
    )
    assert degraded != FEED_HEALTH_SQL, (
        "CROSS JOIN keyword not found in FEED_HEALTH_SQL; negative control is vacuous"
    )
    assert not _binds_site_and_feed(conn, degraded)


def test_health_feeds_probe_binds_site_and_feed() -> None:
    conn, _ids = _seed(package_present=True)
    assert _binds_site_and_feed(conn, HEALTH_FEEDS_SQL)
    degraded = HEALTH_FEEDS_SQL.replace(
        "CROSS JOIN forecast_samples", "JOIN forecast_samples"
    )
    assert degraded != HEALTH_FEEDS_SQL, (
        "CROSS JOIN keyword not found in HEALTH_FEEDS_SQL; negative control is vacuous"
    )
    assert not _binds_site_and_feed(conn, degraded)


# --- The cross-surface drift guard ------------------------------------------


def test_ops_and_api_agree_on_which_feeds_have_samples() -> None:
    conn, _ids = _seed(package_present=True)
    ops_rows = load_feed_health(conn)
    api_rows = conn.execute(HEALTH_FEEDS_SQL).fetchall()
    ops_by_key = {(r.site_id, r.feed_id): r for r in ops_rows}
    api_by_key = {(int(r["site_id"]), int(r["feed_id"])): r for r in api_rows}
    # Set equality first: iterating only the matched keys would silently
    # skip any key one surface emits and the other does not, which is the
    # main way the two rollups could diverge -- a matched-keys-only loop
    # would report agreement in exactly the case it exists to catch.
    assert ops_by_key.keys() == api_by_key.keys()
    assert ops_by_key, "fixture produced no rows; the drift guard is vacuous"
    for key, ops_row in ops_by_key.items():
        assert ops_row.has_samples == (int(api_by_key[key]["sample_count"]) > 0)


_PUBLISHED_HEALTH_FEEDS_KEYS = frozenset(
    {
        "site_id",
        "site_name",
        "feed_id",
        "source",
        "model",
        "subscribed",
        "status",
        "disabled_reason",
        "last_run_at",
        "last_error",
        "error_count",
        "feed_enabled",
        "site_enabled",
        "sample_count",
    }
)


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def test_health_feeds_publishes_its_documented_key_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-feeds-keys.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    # The real worker is irrelevant to this endpoint-contract check, and left
    # running it would race real fetch jobs against a real network during the
    # TestClient's brief lifespan -- swap in an idle stand-in instead.
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        # Seed only AFTER the app's lifespan has run its own `init_db` (which
        # re-seeds the 15 default feeds via INSERT OR IGNORE): seeding before
        # would have our DELETE FROM feeds undone by that re-seed.
        db = get_db()
        db.write_sync(lambda conn: _seed_into(conn, package_present=True))
        response = client.get("/api/health/feeds")
    assert response.status_code == 200
    rows = response.json()
    # An empty list would satisfy `set(row.keys()) == ...` for every row
    # trivially (there are no rows to check) -- the same vacuity shape the
    # fixture cardinality gate and the drift guard above already close.
    assert rows, "fixture produced no rows; the contract check is vacuous"
    for row in rows:
        assert set(row.keys()) == _PUBLISHED_HEALTH_FEEDS_KEYS


# --- The HTMX backfill fragment: the only test that reaches it --------------


def test_backfill_htmx_fragment_does_not_load_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "backfill-catchup.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        called: list[str] = []
        real_read = db.read

        async def _tracking_read(fn: object) -> object:
            called.append(getattr(fn, "__name__", repr(fn)))
            return await real_read(fn)  # type: ignore[arg-type]

        monkeypatch.setattr(db, "read", _tracking_read)
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {
            "Origin": "http://testserver",
            "X-CSRF-Token": csrf,
            "HX-Request": "true",
        }
        resp = client.post("/api/catchup", headers=headers)
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        # The negative assertion is the load-bearing one: the positive
        # assertions below pass unchanged against the pre-Change-3 code, so
        # on their own they prove only that the route still works.
        assert "load_ops" not in called, (
            "HTMX backfill fragment still runs the full ops context"
        )
        assert "load_backfill" in called
        assert "Backfill Progress" in resp.text
        assert (
            db.read_sync(
                lambda conn: conn.execute(
                    "SELECT 1 FROM jobs WHERE type='catchup'"
                ).fetchone()
            )
            is not None
        )


def test_backfill_site_htmx_fragment_does_not_load_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "backfill-site.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id = int(
            db.write_sync(
                lambda conn: (
                    conn.execute(
                        """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Backfill Target', 47.0, 25.0, 900.0, 'UTC')
                    """
                    ).lastrowid
                )
            )
        )
        called: list[str] = []
        real_read = db.read

        async def _tracking_read(fn: object) -> object:
            called.append(getattr(fn, "__name__", repr(fn)))
            return await real_read(fn)  # type: ignore[arg-type]

        monkeypatch.setattr(db, "read", _tracking_read)
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {
            "Origin": "http://testserver",
            "X-CSRF-Token": csrf,
            "HX-Request": "true",
        }
        resp = client.post(f"/api/sites/{site_id}/backfill", headers=headers)
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        assert "load_ops" not in called, (
            "HTMX backfill fragment still runs the full ops context"
        )
        assert "load_backfill" in called
        assert "Backfill Target" in resp.text
        assert (
            db.read_sync(
                lambda conn: conn.execute(
                    "SELECT 1 FROM jobs WHERE type='backfill_site' AND site_id=?",
                    (site_id,),
                ).fetchone()
            )
            is not None
        )
