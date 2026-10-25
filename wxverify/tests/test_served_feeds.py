"""``served_feed_ids`` and the ``served_only`` loader flag (plan Steps 1-2).

All fixture names are synthetic.
"""

from __future__ import annotations

import inspect
import sqlite3

from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import load_future_samples, served_feed_ids
from wxverify.verification.runs import roster_feeds

_OPEN_METEO_MODELS = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "meteofrance_arpege_world",
    "jma_gsm",
    "ukmo_global_deterministic_10km",
    "icon_eu",
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str, timezone: str = "UTC") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, ?)
        """,
        (name, timezone),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _seed_feeds(conn: sqlite3.Connection) -> dict[str, int]:
    """Step-1 fixture: the seeded open-meteo feeds plus synthetic additions.

    Pauses ``gfs_global`` globally, ``icon_global`` at site 1 only, and
    ``gem_global`` at site 2 only; adds a non-default ``example-src``/
    ``model-on`` feed turned on at site 1; adds meteoblue members
    ``example_member_a`` (enabled, package subscribed at site 1) and
    ``example_member_b`` (disabled at the feed level, never served anywhere).
    """
    ids: dict[str, int] = {
        model: _feed_id(conn, "open-meteo", model) for model in _OPEN_METEO_MODELS
    }

    conn.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (ids["gfs_global"],))
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 0)",
        (ids["icon_global"],),
    )
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (2, ?, 0)",
        (ids["gem_global"],),
    )

    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', 'model-on', 0, 360, 168)
        """
    )
    assert cur.lastrowid is not None
    model_on_id = int(cur.lastrowid)
    ids["model-on"] = model_on_id
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 1)",
        (model_on_id,),
    )

    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, enabled, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('meteoblue', 'example_member_a', 1, 0, 360, 168)
        """
    )
    assert cur.lastrowid is not None
    member_a_id = int(cur.lastrowid)
    ids["example_member_a"] = member_a_id

    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, enabled, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('meteoblue', 'example_member_b', 0, 0, 360, 168)
        """
    )
    assert cur.lastrowid is not None
    member_b_id = int(cur.lastrowid)
    ids["example_member_b"] = member_b_id

    package_id = _feed_id(conn, "meteoblue", "multimodel")
    ids["multimodel"] = package_id
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 1)",
        (package_id,),
    )
    # member_a's own site-2 row must be ignored: the package branch never
    # reads the member's own site_feed_state.
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (2, ?, 1)",
        (member_a_id,),
    )
    return ids


def _source_model(conn: sqlite3.Connection, feed_id: int) -> tuple[str, str]:
    row = conn.execute(
        "SELECT source, model FROM feeds WHERE id = ?", (feed_id,)
    ).fetchone()
    assert row is not None
    return (str(row["source"]), str(row["model"]))


def test_served_feed_ids_matrix() -> None:
    """Served feeds per site match the pause matrix exactly.

    Catches: a ``served_feed_ids`` that forgets the ``is_virtual = 0``
    filter kept separate from ``active_competitor_clause`` (virtual feeds,
    seeded by ``run_migrations``, would then leak into every site's set);
    a predicate that reads the meteoblue member's own ``site_feed_state``
    row (site 2's set would then gain ``example_member_a``, since that
    feed has an enabled=1 row at site 2); a predicate that checks only
    ``feeds.enabled`` and never ``site_feed_state`` (both sites would then
    keep ``icon_global``/``gem_global`` and site 2 would still drop
    ``gfs_global`` only).
    """
    conn = _conn()
    _make_site(conn, "site-a")
    _make_site(conn, "site-b")
    _seed_feeds(conn)
    conn.commit()

    def _pairs(site_id: int) -> set[tuple[str, str]]:
        ids = served_feed_ids(conn, site_id=site_id)
        return {_source_model(conn, fid) for fid in ids}

    open_meteo = {("open-meteo", m) for m in _OPEN_METEO_MODELS}
    site1_expected = open_meteo - {
        ("open-meteo", "gfs_global"),
        ("open-meteo", "icon_global"),
    } | {("example-src", "model-on"), ("meteoblue", "example_member_a")}
    site2_expected = open_meteo - {
        ("open-meteo", "gfs_global"),
        ("open-meteo", "gem_global"),
    }

    assert _pairs(1) == site1_expected
    assert _pairs(2) == site2_expected


def test_served_feed_ids_matches_roster() -> None:
    """``served_feed_ids`` is exactly the roster's feed-id set at both sites.

    Catches: the two functions diverging on the package-membership branch
    (any mismatch between ``served_feed_ids`` and ``roster_feeds`` would
    surface as a set difference here, at either site).
    """
    conn = _conn()
    _make_site(conn, "site-a")
    _make_site(conn, "site-b")
    _seed_feeds(conn)
    conn.commit()

    for site_id in (1, 2):
        assert served_feed_ids(conn, site_id=site_id) == {
            feed.feed_id for feed in roster_feeds(conn, site_id)
        }


_SIX_FEED_NAMES = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "example_member_a",
    "example_member_b",
)


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str = "temperature",
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float = 10.0,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )


def _seed_future_samples(conn: sqlite3.Connection, ids: dict[str, int]) -> None:
    for name in _SIX_FEED_NAMES:
        _insert_sample(
            conn,
            feed_id=ids[name],
            issued_at="2026-06-30T00:00:00Z",
            valid_at="2026-07-01T00:00:00Z",
        )


def test_loader_served_only_true_keeps_served_feeds() -> None:
    """``served_only=True`` keeps only site-1-served feeds among the six.

    Catches: ``served_only=True`` doing nothing (all six feed ids, including
    the globally-disabled and site-1-paused ones, would still come back);
    a filter applied before the latest-run pick instead of after (would
    either raise or silently change which run wins for a kept feed -- not
    reachable here since every feed has exactly one run, but the id set
    alone already distinguishes "no filter" from "right filter").
    """
    conn = _conn()
    _make_site(conn, "site-a")
    _make_site(conn, "site-b")
    ids = _seed_feeds(conn)
    _seed_future_samples(conn, ids)
    conn.commit()

    rows = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=True
    )
    feed_ids = {row.feed_id for row in rows}
    assert feed_ids == {ids["ecmwf_ifs"], ids["gem_global"], ids["example_member_a"]}


def test_loader_served_only_false_keeps_every_feed() -> None:
    """``served_only=False`` keeps all six feeds, pauses included.

    Catches: ``served_only=False`` silently applying the served filter
    anyway (the paused feeds would vanish just as in the ``True`` case).
    """
    conn = _conn()
    _make_site(conn, "site-a")
    _make_site(conn, "site-b")
    ids = _seed_feeds(conn)
    _seed_future_samples(conn, ids)
    conn.commit()

    rows = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    feed_ids = {row.feed_id for row in rows}
    assert feed_ids == {ids[name] for name in _SIX_FEED_NAMES}


def test_loader_sample_statement_unchanged() -> None:
    """``served_only`` filters in Python after the fetch, not in the SQL.

    The traced statement touching ``forecast_samples`` is byte-identical
    whether ``served_only`` is ``True`` or ``False``, and neither mentions
    ``site_feed_state``.

    Catches: folding the served filter into the sample SQL itself (the two
    traced statements would then differ, and the one with the filter would
    mention ``site_feed_state``).
    """
    conn = _conn()
    _make_site(conn, "site-a")
    _make_site(conn, "site-b")
    ids = _seed_feeds(conn)
    _seed_future_samples(conn, ids)
    conn.commit()

    statements_true: list[str] = []
    conn.set_trace_callback(lambda s: statements_true.append(s))
    load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=True
    )
    conn.set_trace_callback(None)

    statements_false: list[str] = []
    conn.set_trace_callback(lambda s: statements_false.append(s))
    load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    conn.set_trace_callback(None)

    sample_true = [s for s in statements_true if "FROM forecast_samples fs" in s]
    sample_false = [s for s in statements_false if "FROM forecast_samples fs" in s]
    assert sample_true
    assert sample_true == sample_false
    assert not any("site_feed_state" in s for s in sample_true)
    assert not any("site_feed_state" in s for s in sample_false)


def test_served_only_has_no_default() -> None:
    """``served_only`` is keyword-only with no default: every caller must pick.

    Catches: giving ``served_only`` a default value of either polarity, or
    making it positional (either change would show up in the signature's
    ``kind``/``default`` here).
    """
    sig = inspect.signature(load_future_samples)
    param = sig.parameters["served_only"]
    assert param.kind == inspect.Parameter.KEYWORD_ONLY
    assert param.default is inspect.Parameter.empty
