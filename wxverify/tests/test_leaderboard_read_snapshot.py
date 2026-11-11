"""Oracle suite for the ``read_snapshot`` bracket on the leaderboard/composite
read facades (``wxverify/db/snapshot.py``).

O1 (the torn-read six-seams oracle, a threaded writer racing a parked
facade read) and O2 (the statement-shape matrix over score_cache's window
set) pinned the cache-backed read path and are retired with it (storage
release) -- see the comment where they used to live. O3-O7 below, the
read_snapshot bracket itself, use a single real WAL connection (a pooled
reader driven via ``get_db().read``/``write_sync``) plus one independent
``sqlite3`` side connection for O3's single same-row update -- never a
mock. "No WARNING" assertions are exact-message checks on a named logger
via ``caplog.at_level`` -- never a bare "caplog is empty" check, which
would also pass if the logger were mis-named.

Synthetic fixtures only: the site is seeded via the reused ``_make_site``
helper's synthetic placeholders -- no real station id, key, or coordinate
anywhere in this file, including comments.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import (
    assert_read_pool_at_rest,
    seed_active_publication,
    seed_publication_ranking,
)
from tests.test_leaderboard_cache_backed import (
    _init_tmp_db,
    _make_site,
    _open_meteo_feed_ids,
    _start_app,
)
from wxverify import config
from wxverify.db.connection import get_db
from wxverify.db.snapshot import SnapshotNestingError
from wxverify.scoring.composite import composite_with_status
from wxverify.scoring.leaderboard import (
    LeaderboardResult,
    leaderboard_with_status,
)
from wxverify.scoring.metrics import MetricResult
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _side_conn(path: str) -> sqlite3.Connection:
    """A second, independent WAL connection -- the concurrent writer."""
    conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def _run_read(db: Any, fn: Any) -> Any:
    import asyncio

    return asyncio.run(db.read(fn))


def _hit_fixture(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Seed one site with a publication-backed rolling-window hit on feed 1.

    ``leaderboard_with_status``/``composite_with_status`` both resolve the
    SAME active publication, so a single seed of feed 1's ranking at
    ``window_key="w:14"`` (the publication's own ``window_days``)
    satisfies both facades' hit fixture at once. Feed 2 is reserved,
    deliberately unseeded -- used only by O1's torn-read writers, which are
    retired with the cache-backed path; kept here only so this fixture's
    return shape (and feed-id allocation order) stays unchanged for O3/O4's
    callers.
    """
    set_setting(conn, "min_n", "1")
    site_id = _make_site(conn, "snapshot-site")
    feed1, feed2 = _open_meteo_feed_ids(conn, 2)
    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=date(2035, 1, 1),
        published_at=datetime(2035, 1, 1, tzinfo=UTC),
        window_days=14,
        min_n=1,
    )
    seed_publication_ranking(
        conn,
        publication_id=pub_id,
        feed_id=feed1,
        variable="temperature",
        day_ahead=1,
        window_key="w:14",
        result=MetricResult(n=2, skill_score=0.55, confident=True),
    )
    conn.commit()
    return site_id, feed1, feed2


# ---------------------------------------------------------------------------
# There is no `wxverify.scoring.cache` module and no cache-backed
# leaderboard/composite read path (score_cache rows, writers mutating them
# mid-read) for a torn-read oracle or a statement-shape matrix over
# score_cache's window set to probe. The read_snapshot bracket itself,
# pinned below, is unaffected and keeps its own publication-backed
# `_hit_fixture`.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# O3 -- nesting-refusal positive control
# ---------------------------------------------------------------------------


def test_o3_nesting_refused_outer_survives(tmp_path: Path) -> None:
    """A facade call attempted while a snapshot is already held on the same
    connection is refused before any SQL runs, and the outer snapshot -- and
    the pre-commit view it pins -- survives the refusal intact.

    Paired positive: the outer block, entered the same way, DOES complete and
    return the correct pre-commit verdict once the inner call is removed
    (every other O-series test here is that positive -- a bare facade call
    with no nesting attempt always succeeds). This test alone proves only
    the refusal half in isolation; the
    positive half is carried by the rest of the module rather than duplicated
    here, since this nesting-refusal oracle is specifically about the
    refusal not silently corrupting an outer transaction already in flight.
    """
    conn = _init_tmp_db(tmp_path)
    site_id, feed1, _feed2 = _hit_fixture(conn)

    from wxverify.db.snapshot import read_snapshot

    side = _side_conn(config.db_path)
    try:
        with read_snapshot(conn, label="outer"):
            # The outer bracket already holds this connection's transaction,
            # so the FIRST read here must also go through the unbracketed
            # inner variant -- a bracketed facade call at this point (even
            # before the deliberately-nested one below) would itself raise
            # SnapshotNestingError, which is not what this oracle is probing.
            before = leaderboard_with_status_in_transaction_wrapper(conn, site_id)
            with pytest.raises(SnapshotNestingError):
                leaderboard_with_status(
                    conn,
                    site_id=site_id,
                    variable="temperature",
                    day_ahead=1,
                    window="rolling",
                )
            with pytest.raises(SnapshotNestingError):
                composite_with_status(conn, site_id=site_id, window="rolling")
            side.execute("BEGIN IMMEDIATE")
            side.execute("UPDATE sites SET enabled=0 WHERE id=?", (site_id,))
            side.commit()
            after_refusal = leaderboard_with_status_in_transaction_wrapper(
                conn, site_id
            )
    finally:
        side.close()

    assert conn.in_transaction is False
    assert before.status == "hit"
    assert frozenset(r.feed_id for r in before.rows) == frozenset({feed1})
    # The outer snapshot is still pinned after the refused nested attempt:
    # a same-connection re-read (via the unbracketed inner) still sees the
    # pre-commit enabled site, not the side writer's post-commit disable.
    assert after_refusal.status == "hit"
    assert frozenset(r.feed_id for r in after_refusal.rows) == frozenset({feed1})


def leaderboard_with_status_in_transaction_wrapper(
    conn: sqlite3.Connection, site_id: int
) -> LeaderboardResult:
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    return leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )


# ---------------------------------------------------------------------------
# ``verification/record.py`` has no ``_leaderboard_status_cell`` write-path
# caller: with the daily close permanent, the close-on record always
# writes ``leaderboard_status`` NULL and never reads the live/as-of
# leaderboard status at all. Nothing calls
# ``leaderboard_with_status_in_transaction`` under a writer's own
# ``BEGIN IMMEDIATE`` any more, so there is no in-transaction write-path
# case left to pin.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# O5 -- source-walk caller pin
# ---------------------------------------------------------------------------


def test_o5_leaderboard_with_status_in_transaction_has_exactly_two_callers() -> None:
    """The unbracketed inner (``leaderboard_with_status_in_transaction``) is
    called from exactly two places: its own facade wrapper
    (``leaderboard_with_status``) and ``forecast_ranking_with_status`` in
    ``forecast/data.py``, which calls it only when ``conn.in_transaction`` (a
    forecast route's read snapshot is open). The former write-path caller,
    ``verification/record.py``'s ``_leaderboard_status_cell``, is gone with
    the daily close being permanent. Any third caller would bypass the
    bracket outside a caller-owned transaction.
    """
    import wxverify

    package_root = Path(wxverify.__file__).resolve().parent
    callers: set[Path] = set()
    for path in package_root.rglob("*.py"):
        if "leaderboard_with_status_in_transaction(" in path.read_text(
            encoding="utf-8"
        ):
            callers.add(path.relative_to(package_root))

    assert callers == {
        Path("forecast/data.py"),
        Path("scoring/leaderboard.py"),
    }


# ---------------------------------------------------------------------------
# O6 -- HTTP surfaces: pool-at-rest, no WARNING
# ---------------------------------------------------------------------------


def _no_warning(records: list[logging.LogRecord], message: str) -> bool:
    return not any(r.getMessage() == message for r in records)


_O6_SETTLE_WARNING = "reader returned mid-transaction; rolling back"


def test_o6_http_surfaces_leave_pool_at_rest_no_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Every HTTP surface that reaches ``leaderboard_with_status`` /
    ``composite_with_status`` -- ``/api/leaderboard``, ``/api/curve``,
    ``/api/composite`` (each also with a live ``window=7d``), and
    ``/dashboard`` -- settles its pooled reader cleanly: the read pool
    returns to ``_READ_POOL_SIZE`` distinct connections after every single
    request, and ``Database._settle_reader`` never logs its "reader returned
    mid-transaction; rolling back" WARNING -- the bracket's own
    ``finally: conn.rollback()`` always closes the transaction it opened
    before the read executor hands the connection back to the pool.
    """
    from fastapi.testclient import TestClient

    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id, _feed1, _feed2 = _hit_fixture(conn)
            return site_id

        site_id = db.write_sync(_seed)

        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")

        requests = [
            ("/api/leaderboard", {"site": site_id, "window": "rolling"}),
            ("/api/leaderboard", {"site": site_id, "window": "7d"}),
            ("/api/curve", {"site": site_id, "window": "rolling"}),
            ("/api/curve", {"site": site_id, "window": "7d"}),
            ("/api/composite", {"site": site_id, "window": "rolling"}),
            ("/api/composite", {"site": site_id, "window": "7d"}),
            ("/dashboard", {"site": site_id}),
        ]
        for path, params in requests:
            resp = client.get(path, params=params)
            assert resp.status_code == 200, (path, params, resp.text)
            records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
            assert _no_warning(records, _O6_SETTLE_WARNING), (
                path,
                [r.getMessage() for r in records],
            )
            assert_read_pool_at_rest(db)


# ---------------------------------------------------------------------------
# O7 -- DEBUG label check
# ---------------------------------------------------------------------------


def test_o7_snapshot_debug_label_identifies_the_facade(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """``read_snapshot``'s DEBUG-level held-duration log line carries the
    caller's own label -- "leaderboard" for the leaderboard facade,
    "composite" for the composite facade -- so an operator reading the log
    can tell which bracket a slow hold belongs to without inferring it from
    call-site line numbers.
    """
    conn = _init_tmp_db(tmp_path)
    site_id, _feed1, _feed2 = _hit_fixture(conn)

    caplog.set_level(logging.DEBUG, logger="wxverify.db.snapshot")

    leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    composite_with_status(conn, site_id=site_id, window="rolling")

    records = [r for r in caplog.records if r.name == "wxverify.db.snapshot"]
    messages = [r.getMessage() for r in records]
    # Order-sensitive: the leaderboard call runs (and its snapshot closes,
    # logging its held-duration line) strictly before the composite call
    # starts, so messages[0] must be the leaderboard facade's own line and
    # messages[1] the composite facade's -- not just "one of each present",
    # which a label swap between the two facades would still satisfy.
    assert len(messages) == 2, messages
    assert messages[0].startswith("db snapshot leaderboard held="), messages
    assert messages[1].startswith("db snapshot composite held="), messages
