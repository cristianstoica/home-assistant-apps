"""Oracle suite for the ``read_snapshot`` bracket on the leaderboard/composite
read facades (``wxverify/db/snapshot.py``, plan ``2026-09-20-leaderboard-
read-snapshot``).

Mirrors ``tests/test_freshness_read_snapshot.py``'s harness shape exactly:
two real WAL connections (a pooled reader driven via ``get_db().read``/
``write_sync``, and an independent writer thread on its own ``sqlite3``
connection) -- never a mock. Every monkeypatch seam names the module that
actually CALLS the target (Python binds names at call time into the calling
module's namespace), not the module that defines it. Every threaded park is
bounded (``timeout=`` on every ``Event.wait``/``Thread.join``) and the writer
thread is joined on every exit path before its connection closes, so a
seam that never fires cannot hang the suite. "No WARNING" assertions are
exact-message checks on a named logger via ``caplog.at_level`` -- never a
bare "caplog is empty" check, which would also pass if the logger were
mis-named.

Synthetic fixtures only: the site is seeded via the reused ``_make_site``
helper's synthetic placeholders -- no real station id, key, or coordinate
anywhere in this file, including comments.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import assert_read_pool_at_rest
from tests.test_leaderboard_cache_backed import (
    _add_temperature_cell,
    _init_tmp_db,
    _make_site,
    _open_meteo_feed_ids,
    _seed_score_cache,
    _start_app,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import get_db
from wxverify.db.snapshot import SnapshotNestingError
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.composite import CompositeResult, composite_with_status
from wxverify.scoring.leaderboard import (
    LeaderboardResult,
    leaderboard_with_status,
)
from wxverify.scoring.metrics import MetricResult
from wxverify.settings.keys import set_setting
from wxverify.verification.record import _leaderboard_status_cell

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
    """Seed one site with a cache-backed rolling-window hit on feed 1.

    ``leaderboard`` and ``composite`` share the same ``forecast_pairs`` /
    ``score_cache`` tables keyed on ``(site_id, feed_id,
    variable='temperature', day_ahead=1)``, so a single seed of feed 1
    satisfies both facades' hit fixture at once. Feed 2 is reserved,
    deliberately unseeded, for writers that activate it (W-A).
    """
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "snapshot-site")
    feed1, feed2 = _open_meteo_feed_ids(conn, 2)
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed1, valid_at=isoformat_utc()
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed1,
        window_key="w:14",
        computed_at=isoformat_utc(),
        n=2,
        skill_score=0.55,
    )
    conn.commit()
    return site_id, feed1, feed2


def _writer_w_a(
    side: sqlite3.Connection, *, site_id: int, feed1: int, feed2: int
) -> None:
    """W-A: activate feed 2 -- a new in-window cell plus a fresh w:14 cache row."""
    side.execute("BEGIN IMMEDIATE")
    generation_id = ensure_published_generation(side, site_id)
    side.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 24, 1,
                11.0, 10.0, 1.0, 1.0, 1.0, ?)
        """,
        (site_id, feed2, isoformat_utc(), generation_id),
    )
    upsert_score_cache(
        side,
        site_id=site_id,
        feed_id=feed2,
        variable="temperature",
        day_ahead=1,
        window_key="w:14",
        result=MetricResult(n=2, skill_score=0.4, confident=True),
        computed_at=isoformat_utc(),
    )
    side.commit()


def _writer_w_b(
    side: sqlite3.Connection, *, site_id: int, feed1: int, feed2: int
) -> None:
    """W-B: the window moves -- rolling_window_days 14 -> 7, cache follows."""
    side.execute("BEGIN IMMEDIATE")
    side.execute("UPDATE settings SET value='7' WHERE key='rolling_window_days'")
    side.execute(
        "DELETE FROM score_cache WHERE site_id = ? AND window_key = 'w:14'",
        (site_id,),
    )
    upsert_score_cache(
        side,
        site_id=site_id,
        feed_id=feed1,
        variable="temperature",
        day_ahead=1,
        window_key="w:7",
        result=MetricResult(n=2, skill_score=0.55, confident=True),
        computed_at=isoformat_utc(),
    )
    side.commit()


def _writer_w_c(
    side: sqlite3.Connection, *, site_id: int, feed1: int, feed2: int
) -> None:
    """W-C: the site is disabled and its score_cache rows are dropped."""
    side.execute("BEGIN IMMEDIATE")
    side.execute("UPDATE sites SET enabled=0 WHERE id=?", (site_id,))
    side.execute("DELETE FROM score_cache WHERE site_id=?", (site_id,))
    side.commit()


_WRITERS = {"W-A": _writer_w_a, "W-B": _writer_w_b, "W-C": _writer_w_c}


def _call_facade(conn: sqlite3.Connection, facade: str, site_id: int) -> Any:
    if facade == "leaderboard":
        return leaderboard_with_status(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
        )
    return composite_with_status(conn, site_id=site_id, window="rolling")


def _verdict(result: Any, facade: str) -> tuple[str, frozenset[int]]:
    if facade == "leaderboard":
        assert isinstance(result, LeaderboardResult)
        return result.status, frozenset(r.feed_id for r in result.rows)
    assert isinstance(result, CompositeResult)
    return result.status, frozenset(int(r["feed_id"]) for r in result.rows)


# ---------------------------------------------------------------------------
# O1 -- torn read, six seams
# ---------------------------------------------------------------------------

_O1_CASES = [
    pytest.param(
        "leaderboard",
        "wxverify.scoring.leaderboard.resolve_window",
        "W-B",
        id="P1-leaderboard-resolve_window-WB",
    ),
    pytest.param(
        "leaderboard",
        "wxverify.scoring.leaderboard._site_enabled",
        "W-C",
        id="P2-leaderboard-site_enabled-WC",
    ),
    pytest.param(
        "leaderboard",
        "wxverify.scoring.leaderboard._expected_active_feed_ids",
        "W-A",
        id="P3-leaderboard-expected_feed_ids-WA",
    ),
    pytest.param(
        "composite",
        "wxverify.scoring.composite.resolve_window",
        "W-B",
        id="C1-composite-resolve_window-WB",
    ),
    pytest.param(
        "composite",
        "wxverify.scoring.composite._expected_active_cells",
        "W-A",
        id="C2-composite-expected_cells-WA",
    ),
    pytest.param(
        "composite",
        "wxverify.scoring.composite._site_enabled",
        "W-C",
        id="C3-composite-site_enabled-WC",
    ),
]


@pytest.mark.parametrize("facade, seam_target, writer_kind", _O1_CASES)
def test_o1_torn_read_six_seams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    facade: str,
    seam_target: str,
    writer_kind: str,
) -> None:
    """One derivation seam parks mid-facade while a writer commits underneath.

    The wrapped seam calls the real function FIRST (so its own read actually
    executes against whatever snapshot is currently pinned), then parks --
    placing the writer's commit window strictly between this seam's read and
    the facade's SUBSEQUENT statement(s). A held ``read_snapshot`` makes every
    later statement in the block still observe the pre-commit state; a
    version that opened the bracket after this seam (or not at all) lets the
    later statement observe the writer's post-commit state instead, tearing
    the verdict.
    """
    conn = _init_tmp_db(tmp_path)
    site_id, feed1, feed2 = _hit_fixture(conn)

    module_path, attr_name = seam_target.rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    real_fn = getattr(module, attr_name)

    entered = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[BaseException] = []

    def _parked(*args: Any, **kwargs: Any) -> Any:
        result = real_fn(*args, **kwargs)
        entered.set()
        assert writer_done.wait(timeout=10.0), "writer never signalled completion"
        return result

    monkeypatch.setattr(seam_target, _parked)

    side = _side_conn(config.db_path)

    def _writer() -> None:
        try:
            assert entered.wait(timeout=5.0), "seam never entered"
            _WRITERS[writer_kind](side, site_id=site_id, feed1=feed1, feed2=feed2)
        except BaseException as exc:  # noqa: BLE001 - surfaced via writer_errors
            writer_errors.append(exc)
        finally:
            writer_done.set()

    thread = threading.Thread(target=_writer)
    db = get_db()
    try:
        thread.start()
        first = _run_read(db, lambda c: _call_facade(c, facade, site_id))
    finally:
        thread.join(timeout=6.0)
        side.close()

    assert not thread.is_alive(), "writer thread did not finish"
    assert writer_errors == [], writer_errors

    pre_by_writer = {
        "W-A": ("hit", frozenset({feed1})),
        "W-B": ("hit", frozenset({feed1})),
        "W-C": ("hit", frozenset({feed1})),
    }
    post_by_writer = {
        "W-A": ("hit", frozenset({feed1, feed2})),
        "W-B": ("hit", frozenset({feed1})),
        "W-C": ("empty", frozenset()),
    }

    assert _verdict(first, facade) == pre_by_writer[writer_kind], (
        f"{facade}/{seam_target} parked read observed a torn (post-commit) "
        "state instead of the pre-commit snapshot"
    )

    if facade == "leaderboard" and writer_kind == "W-B":
        assert isinstance(first, LeaderboardResult)
        assert first.window_key == "w:14", (
            f"{facade}/{seam_target} parked read resolved the writer's "
            "post-commit window ('w:7') instead of the pre-commit snapshot"
        )

    second = _run_read(db, lambda c: _call_facade(c, facade, site_id))
    assert _verdict(second, facade) == post_by_writer[writer_kind], (
        f"{facade}/{seam_target} fresh post-commit read did not observe the "
        "writer's committed state"
    )

    if facade == "leaderboard" and writer_kind == "W-B":
        assert isinstance(second, LeaderboardResult)
        assert second.window_key == "w:7", (
            f"{facade}/{seam_target} fresh post-commit read did not resolve "
            "the writer's committed window"
        )


# ---------------------------------------------------------------------------
# O2 -- statement shape, every path
# ---------------------------------------------------------------------------


def _sql_trace(conn: sqlite3.Connection, fn: Any) -> list[str]:
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        fn(conn)
    finally:
        conn.set_trace_callback(None)
    return statements


def _leaderboard_rolling_hit(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, _feed1, _feed2 = _hit_fixture(conn)
    return (site_id,)


def _leaderboard_all_hit(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, feed1, _feed2 = _hit_fixture(conn)
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed1,
        window_key="w:all",
        computed_at=isoformat_utc(),
        n=2,
        skill_score=0.55,
    )
    conn.commit()
    return (site_id,)


def _leaderboard_disabled_site(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, _feed1, _feed2 = _hit_fixture(conn)
    conn.execute("UPDATE sites SET enabled=0 WHERE id=?", (site_id,))
    conn.commit()
    return (site_id,)


def _leaderboard_no_cell(conn: sqlite3.Connection) -> tuple[int, ...]:
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "snapshot-site")
    conn.commit()
    return (site_id,)


_O2_LEADERBOARD_CASES = [
    pytest.param(
        "rolling", _leaderboard_rolling_hit, "rolling", 5, id="leaderboard-rolling-hit"
    ),
    pytest.param("all", _leaderboard_all_hit, "all", 4, id="leaderboard-all-hit"),
    pytest.param("7d", _leaderboard_rolling_hit, "7d", 3, id="leaderboard-7d-live"),
    pytest.param(
        "disabled",
        _leaderboard_disabled_site,
        "rolling",
        2,
        id="leaderboard-disabled-site-empty",
    ),
    pytest.param(
        "no-cell", _leaderboard_no_cell, "rolling", 3, id="leaderboard-no-cell-empty"
    ),
]


@pytest.mark.parametrize("_label, setup, window, expected_min", _O2_LEADERBOARD_CASES)
def test_o2_leaderboard_statement_shape(
    tmp_path: Path, _label: str, setup: Any, window: str, expected_min: int
) -> None:
    """The bracket adds exactly BEGIN DEFERRED/PRAGMA/...ROLLBACK around the
    unbracketed statement list, for every leaderboard status-matrix path."""
    conn = _init_tmp_db(tmp_path)
    (site_id,) = setup(conn)

    def _call(c: sqlite3.Connection) -> LeaderboardResult:
        return leaderboard_with_status(
            c, site_id=site_id, variable="temperature", day_ahead=1, window=window
        )

    real = _sql_trace(conn, _call)
    assert real[0] == "BEGIN DEFERRED"
    assert real[1] == "PRAGMA user_version"
    assert real[-1] == "ROLLBACK"
    unbracketed = real[2:-1]
    if window == "7d":
        assert len(unbracketed) >= expected_min, (real, unbracketed)
    else:
        assert len(unbracketed) == expected_min, (real, unbracketed)


def _composite_rolling_hit(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, _feed1, _feed2 = _hit_fixture(conn)
    return (site_id,)


def _composite_all_hit(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, feed1, _feed2 = _hit_fixture(conn)
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed1,
        window_key="w:all",
        computed_at=isoformat_utc(),
        n=2,
        skill_score=0.55,
    )
    conn.commit()
    return (site_id,)


def _composite_disabled_site(conn: sqlite3.Connection) -> tuple[int, ...]:
    site_id, _feed1, _feed2 = _hit_fixture(conn)
    conn.execute("UPDATE sites SET enabled=0 WHERE id=?", (site_id,))
    conn.commit()
    return (site_id,)


def _composite_no_cell(conn: sqlite3.Connection) -> tuple[int, ...]:
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "snapshot-site")
    conn.commit()
    return (site_id,)


_O2_COMPOSITE_CASES = [
    pytest.param(_composite_rolling_hit, "rolling", 5, id="composite-rolling-hit"),
    pytest.param(_composite_all_hit, "all", 4, id="composite-all-hit"),
    pytest.param(_composite_rolling_hit, "7d", 3, id="composite-7d-live"),
    pytest.param(
        _composite_disabled_site, "rolling", 3, id="composite-disabled-site-empty"
    ),
    pytest.param(_composite_no_cell, "rolling", 2, id="composite-no-cell-empty"),
]


@pytest.mark.parametrize("setup, window, expected_min", _O2_COMPOSITE_CASES)
def test_o2_composite_statement_shape(
    tmp_path: Path, setup: Any, window: str, expected_min: int
) -> None:
    """Composite's cell-set-empty short-circuit means its no-cell case has
    one statement fewer than its disabled-site case (opposite ordering from
    leaderboard, which always checks the site before the feed set)."""
    conn = _init_tmp_db(tmp_path)
    (site_id,) = setup(conn)

    def _call(c: sqlite3.Connection) -> CompositeResult:
        return composite_with_status(c, site_id=site_id, window=window)

    real = _sql_trace(conn, _call)
    assert real[0] == "BEGIN DEFERRED"
    assert real[1] == "PRAGMA user_version"
    assert real[-1] == "ROLLBACK"
    unbracketed = real[2:-1]
    if window == "7d":
        assert len(unbracketed) >= expected_min, (real, unbracketed)
    else:
        assert len(unbracketed) == expected_min, (real, unbracketed)


# ---------------------------------------------------------------------------
# O3 -- nesting-refusal positive control
# ---------------------------------------------------------------------------


def test_o3_nesting_refused_outer_survives(tmp_path: Path) -> None:
    """A facade call attempted while a snapshot is already held on the same
    connection is refused before any SQL runs, and the outer snapshot -- and
    the pre-commit view it pins -- survives the refusal intact.

    Paired positive: the outer block, entered the same way, DOES complete and
    return the correct pre-commit verdict once the inner call is removed
    (``test_o1_torn_read_six_seams`` and every other O-series test here are
    that positive -- a bare facade call with no nesting attempt always
    succeeds). This test alone proves only the refusal half in isolation; the
    positive half is carried by the rest of the module rather than duplicated
    here, since the plan's nesting-refusal oracle is specifically about the
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
# O4 -- write-path caller, under BEGIN IMMEDIATE
# ---------------------------------------------------------------------------


def test_o4_leaderboard_status_cell_runs_inside_writer_transaction(
    tmp_path: Path,
) -> None:
    """``_leaderboard_status_cell`` (the one write-path caller, inside the
    writer's own ``BEGIN IMMEDIATE``) must succeed via the unbracketed
    in-transaction variant -- calling the bracketed facade from here would
    raise ``SnapshotNestingError`` since ``write_sync`` already holds a
    transaction on this exact connection.
    """
    conn = _init_tmp_db(tmp_path)
    site_id, feed1, _feed2 = _hit_fixture(conn)
    db = get_db()

    def _run(c: sqlite3.Connection) -> dict[str, object]:
        assert c.in_transaction, "write_sync must already hold BEGIN IMMEDIATE"
        cell = _leaderboard_status_cell(
            c, site_id=site_id, variable="temperature", day_ahead=1
        )
        assert c.in_transaction, (
            "the in-transaction variant must not end the writer's transaction"
        )
        return cell

    result = db.write_sync(_run)
    assert result["status"] == "hit"
    assert result["window_key"] == "w:14"
    assert db._conn.in_transaction is False


# ---------------------------------------------------------------------------
# O5 -- source-walk caller pin
# ---------------------------------------------------------------------------


def test_o5_leaderboard_with_status_in_transaction_has_exactly_two_callers() -> None:
    """The unbracketed inner (``leaderboard_with_status_in_transaction``) is
    called from exactly two places: its own facade wrapper
    (``leaderboard_with_status``) and the one write-path caller
    (``_leaderboard_status_cell`` in ``verification/record.py``). Any third
    caller would bypass the bracket outside a caller-owned transaction.
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
        Path("scoring/leaderboard.py"),
        Path("verification/record.py"),
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
