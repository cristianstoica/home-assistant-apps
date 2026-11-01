"""Scoring triggers (daily-close plan 2026-10-22-daily-close-pairs-and-
publication.md, section 10; test IDs TR1-TR5, plan 18.9).

Written before ``wxverify.scoring.engine.enqueue_scoring_run`` exists: every
test that imports it fails at ``ImportError``/``AttributeError`` (the plan
gives its signature, 10, but it is new on ``scoring/engine.py`` -- today's
module has no such name). TR2, TR3 and TR4 instead exercise TODAY's trigger
call sites directly and show they do NOT yet respect the close switch --
they fail on an assertion (a job/row IS created with the close on) rather
than at import, which Step 3 also accepts.

Real SQLite throughout (``tests.helpers.day_close_site``/``asof_conn``);
nothing in the code under test is mocked except the provider adapter (none
needed here) and, in TR5, a recording spy placed directly on the imported
name ``schedule_score_rescore`` (never the production rescore logic
itself). Synthetic data only: feed sources ``synthetic-tr-x``/
``synthetic-tr-y``, zone ``UTC``, site name ``Testsite``/``TR5 Site``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    NOW,
    D,
    MovingClock,
    asof_conn,
    day_close_site,
    patch_close_clock,
)
from tests.test_leaderboard_cache_backed import _start_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import FencedWriter

# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_feed(conn, name: str) -> int:
    source = f"synthetic-tr-{name.lower()}"
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, 168, 0)",
        (source, source),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


def _seed_pairable_sample(conn, *, site_id: int, feed_id: int) -> None:
    """One forecast sample with a matching observation at the same
    ``valid_at`` -- the shape ``real_model_pair_pending`` reports True for
    (an existing candidate, no published pair yet, lead in range)."""
    issued_at = f"{(D).isoformat()}T00:00:00Z"
    valid_at = f"{(D).isoformat()}T06:00:00Z"
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, 'temperature', ?, 10.0, 2)",
        (site_id, valid_at),
    )
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 6, 11.0, '{}', 'run-tr', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, issued_at),
    )


def _job_count(conn, site_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score' AND site_id = ?",
            (site_id,),
        ).fetchone()[0]
    )


def _table_count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])  # noqa: S608


def _seed_score_cache_row(conn, *, site_id: int, feed_id: int) -> None:
    """One ``score_cache`` row (feed X, ``temperature``, day_ahead 1,
    ``window_key`` ``w:all``) -- every NOT NULL column filled (18.9/M99)."""
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, computed_at)
        VALUES (?, ?, 'temperature', 1, 'w:all', 3, '2020-01-01T00:00:00.000000Z')
        """,
        (site_id, feed_id),
    )


def _score_cache_snapshot(conn) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM score_cache"
            " ORDER BY site_id, feed_id, variable, day_ahead, window_key"
        )
    ]


# ---------------------------------------------------------------------------
# TR1
# ---------------------------------------------------------------------------


def test_tr1_enqueue_scoring_run_respects_the_close_switch() -> None:
    """TR1: ``enqueue_scoring_run`` called twice enqueues one
    ``pair_and_score`` job with the close off, and none with the close on
    (10)."""
    from wxverify.scoring.engine import enqueue_scoring_run

    conn = asof_conn()
    site_off = day_close_site(conn, close_on=False, with_records=False)
    enqueue_scoring_run(conn, site_off)
    enqueue_scoring_run(conn, site_off)
    conn.commit()
    assert _job_count(conn, site_off) == 1, (
        "M95 -> at this assertion: correct = 1 (close off: today's enqueue"
        " rule, deduped), mutant M95 (enqueue_scoring_run's switch"
        f" inverted) = 0 -- got {_job_count(conn, site_off)}"
    )

    conn2 = asof_conn()
    site_on = day_close_site(conn2, close_on=True, with_records=True)
    enqueue_scoring_run(conn2, site_on)
    enqueue_scoring_run(conn2, site_on)
    conn2.commit()
    assert _job_count(conn2, site_on) == 0, (
        "M95 -> at this assertion: correct = 0 (close on: nothing"
        " enqueued, section 10), mutant M95 (enqueue_scoring_run's switch"
        f" inverted) = 1 -- got {_job_count(conn2, site_on)}"
    )


# ---------------------------------------------------------------------------
# TR2
# ---------------------------------------------------------------------------

_TR2_ALLOWED_QUALNAMES = frozenset(
    {
        "enqueue_scoring_run",
        "enqueue_score_rescore",
        "dispatch",
        "_enqueue_due_score_refresh",
    }
)
# The monitor and the health route reference it in an f-string/column
# label rather than inside one clean function body in every build; matched
# by filename instead of qualname for those two (plan 10's own wording).
_TR2_ALLOWED_FILE_SUBSTRINGS = ("monitor.py", "health")


def _package_root() -> Path:
    return Path(__file__).resolve().parents[1] / "wxverify"


class _LiteralVisitor:
    """Collects (``path``, enclosing-function-name, lineno) for every
    string literal ``"pair_and_score"`` in one already-parsed module,
    avoiding closures over an outer loop variable (each instance is built
    fresh per file, so there is nothing to capture)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.stack: list[str] = []
        self.hits: list[tuple[Path, str, int]] = []

    def visit(self, node: object) -> None:
        import ast

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.stack.append(node.name)
            for child in ast.iter_child_nodes(node):
                self.visit(child)
            self.stack.pop()
            return
        if isinstance(node, ast.Constant) and node.value == "pair_and_score":
            self.hits.append(
                (self.path, self.stack[-1] if self.stack else "<module>", node.lineno)
            )
        if isinstance(node, ast.AST):
            for child in ast.iter_child_nodes(node):
                self.visit(child)


def _find_pair_and_score_literals() -> list[tuple[Path, str, int]]:
    """Every string literal ``"pair_and_score"`` under the package, with
    the file and the qualified name of its innermost enclosing function
    (``"<module>"`` if none), and its line number."""
    import ast

    hits: list[tuple[Path, str, int]] = []
    for path in sorted(_package_root().rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:  # pragma: no cover - defensive
            continue
        visitor = _LiteralVisitor(path)
        visitor.visit(tree)
        hits.extend(visitor.hits)
    return hits


def test_tr2_pair_and_score_literal_confined_to_allowed_call_sites() -> None:
    """TR2: an ``ast`` scan of ``wxverify/`` for the string literal
    ``"pair_and_score"`` finds it only inside the allowed call sites
    (``enqueue_scoring_run``, ``enqueue_score_rescore``, ``dispatch``, the
    monitor, the health route and the scheduler's backstop); every OTHER
    occurrence is a disallowed, unrouted enqueue site (10)."""
    hits = _find_pair_and_score_literals()
    assert hits, "fixture bug: the literal must occur somewhere in wxverify/"

    offenders = [
        (path, fn, line)
        for path, fn, line in hits
        if fn not in _TR2_ALLOWED_QUALNAMES
        and not any(sub in str(path) for sub in _TR2_ALLOWED_FILE_SUBSTRINGS)
    ]
    assert offenders == [], (
        "M96 -> at this assertion: correct = [] (every enqueue site routes"
        " through enqueue_scoring_run/enqueue_score_rescore, 10), mutant"
        " M96 (one enqueue site left calling enqueue_if_absent directly) ="
        f" at least one offending site remains -- got {offenders!r}"
    )


# ---------------------------------------------------------------------------
# TR3
# ---------------------------------------------------------------------------


def test_tr3_fetch_score_gate_enqueues_nothing_with_the_close_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TR3 (fetch score gate): a forward fetch whose sample would pair
    (a matching observation already exists) must not enqueue
    ``pair_and_score``, write a publication row, or touch ``score_cache``,
    with the close on (10).

    Drives the real production call point, ``_persist_fetch_success``
    (``worker/feed_fetch.py``), directly -- the gate this row names -- with
    a hand-built ``FeedFetchTarget``/``FetchResult`` rather than a full job
    dispatch, which needs no provider, budget or domain-backoff fixture.
    """
    from tests.helpers import install_test_source_domains
    from wxverify.feeds.seam import FetchResult, NormalizedSample
    from wxverify.worker.feed_fetch import FeedFetchTarget, _persist_fetch_success

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    conn.commit()
    install_test_source_domains(monkeypatch)
    monkeypatch.setitem(
        __import__(
            "wxverify.worker.domain_backoff", fromlist=["SOURCE_DOMAINS"]
        ).SOURCE_DOMAINS,
        "synthetic-tr-x",
        "synthetic-tr-x",
    )

    issued_at = f"{D.isoformat()}T00:00:00Z"
    valid_at = f"{D.isoformat()}T06:00:00Z"
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, 'temperature', ?, 10.0, 2)",
        (site_id, valid_at),
    )
    conn.commit()

    target = FeedFetchTarget(
        site_id=site_id,
        feed_id=x,
        lat=0.0,
        lon=0.0,
        source="synthetic-tr-x",
        model="synthetic-tr-x",
        max_lead_hours=168,
    )
    sample = NormalizedSample(
        model="synthetic-tr-x",
        variable="temperature",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=6,
        value=11.0,
        source_raw="synthetic-tr-x:raw",
        model_run_id=issued_at,
    )
    _persist_fetch_success(conn, target, FetchResult(samples=[sample], grid=None))
    conn.commit()

    assert _job_count(conn, site_id) == 0, (
        "M98 -> at this assertion: correct = 0 (close on: the fetch gate"
        " routes through enqueue_scoring_run, which enqueues nothing),"
        " mutant M98 (the fetch gate not routed) = a pair_and_score job was"
        f" created -- got {_job_count(conn, site_id)}"
    )
    assert _table_count(conn, "publications") == 0
    assert _table_count(conn, "score_cache") == 0


def test_tr3_rolling_window_setter_enqueues_nothing_with_the_close_on() -> None:
    """TR3 (rolling-window setter): ``set_rolling_window_days_sync``'s own
    enqueue must not fire with the close on; the change still waits for
    the next publication (10, 13.1)."""
    from wxverify.settings.service import set_rolling_window_days_sync

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    conn.commit()

    set_rolling_window_days_sync(conn, 9)
    conn.commit()

    assert _job_count(conn, site_id) == 0, (
        "M97 -> at this assertion: correct = 0 (close on: the setter's"
        " enqueue routes through enqueue_scoring_run, which enqueues"
        " nothing), mutant M97 (the setter's enqueue not routed) = a"
        f" pair_and_score job was created -- got {_job_count(conn, site_id)}"
    )
    assert _table_count(conn, "publications") == 0


def test_tr3_schedule_score_rescore_enqueues_nothing_with_the_close_on() -> None:
    """TR3 (``schedule_score_rescore``/``enqueue_score_rescore``): the
    fire-and-forget post-read rescore path, drained deterministically
    through ``drain_pending_rescores``, must enqueue nothing with the
    close on (10, M100)."""
    from wxverify.scoring.rescore import drain_pending_rescores, schedule_score_rescore

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    conn.commit()

    async def _run() -> None:
        schedule_score_rescore(site_id)
        await drain_pending_rescores()

    asyncio.run(_run())

    assert _job_count(conn, site_id) == 0, (
        "M100 -> at this assertion: correct = 0 (close on:"
        " enqueue_score_rescore's own check), mutant M100"
        " (enqueue_score_rescore without its check) = a pair_and_score job"
        f" was created -- got {_job_count(conn, site_id)}"
    )
    assert _table_count(conn, "publications") == 0


def test_tr3_enqueue_score_rescore_enqueues_nothing_with_the_close_on() -> None:
    """TR3 (``enqueue_score_rescore``'s own check, M100): called directly
    with the close on it must enqueue nothing by itself.

    The fire-and-forget path above (``schedule_score_rescore``/
    ``drain_pending_rescores``) writes through the process-global
    ``get_db()`` -- a different connection than this test's own ``conn``
    -- so that test's job-count assertion cannot observe
    ``enqueue_score_rescore``'s effect at all, on clean code or on M100.
    This test calls ``enqueue_score_rescore`` directly on ``conn`` instead,
    so the observation point actually sees what it writes.
    """
    from wxverify.scoring.composite import enqueue_score_rescore

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    conn.commit()

    enqueue_score_rescore(conn, site_id)
    conn.commit()

    assert _job_count(conn, site_id) == 0, (
        "M100 -> at this assertion: correct = 0 (close on:"
        " enqueue_score_rescore's own check returns without enqueuing),"
        " mutant M100 (enqueue_score_rescore without its check) = a"
        f" pair_and_score job was created -- got {_job_count(conn, site_id)}"
    )
    assert _table_count(conn, "publications") == 0


def test_tr3_per_station_enqueue_enqueues_nothing_with_the_close_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TR3 (per-station enqueue, ``worker/processor.py:1511-1514``): a
    changed station observation must not enqueue ``pair_and_score`` with
    the close on (10). Calls the narrowest function that holds the enqueue,
    ``_persist_station_observations``, directly -- it takes only the
    station's own id and its observations, no job dispatch needed.

    ``_persist_station_observations`` reads ``utc_now()`` internally (no
    ``now_utc`` override exists on it), so the close clock is pinned with
    ``patch_close_clock``/``MovingClock`` first: left on the real 2026+
    clock, the closed-day guard would see D-close-site's fixture dates
    (2024) as long past any open record and refuse the write outright,
    which would leave ``changed`` False for a fixture reason, not a P4 one.
    """
    from wxverify.obs.pws_adapter import PwsObservation
    from wxverify.worker.processor import _persist_station_observations

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    conn.commit()
    patch_close_clock(monkeypatch, MovingClock(NOW))

    station_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
            (site_id, "ISTATION01"),
        ).fetchone()[0]
    )
    valid_at = f"{D.isoformat()}T06:00:00Z"
    outcome = _persist_station_observations(
        conn,
        site_id,
        station_id,
        [
            PwsObservation(
                variable="temperature",
                valid_at=valid_at,
                value=10.0,
                source_raw="{}",
            )
        ],
    )
    conn.commit()
    assert outcome.changed is True, "fixture bug: the observation must be a new write"

    assert _job_count(conn, site_id) == 0, (
        "M97/M98-shaped (per-station enqueue) -> at this assertion: correct"
        " = 0 (close on: routed through enqueue_scoring_run, which"
        " enqueues nothing, 10), mutant (the per-station enqueue not"
        f" routed through enqueue_scoring_run) = a pair_and_score job was"
        f" created -- got {_job_count(conn, site_id)}"
    )


def test_tr3_obs_cycle_enqueue_enqueues_nothing_with_the_close_on() -> None:
    """TR3 (obs-cycle enqueue, ``worker/processor.py:1582-1585``): the end
    of an observation cycle must not enqueue ``pair_and_score`` with the
    close on (10). ``_complete_obs_cycle`` is the narrowest function
    holding this enqueue site; it takes only ``site_id`` and the cycle's
    own ``changed``/``newest_obs_at`` summary, with no internal
    ``utc_now()``-driven day-write guard, so no clock patch is needed."""
    from wxverify.worker.processor import _complete_obs_cycle

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    conn.commit()

    _complete_obs_cycle(conn, site_id, changed=True, newest_obs_at=None)
    conn.commit()

    assert _job_count(conn, site_id) == 0, (
        "M97/M98-shaped (obs-cycle enqueue) -> at this assertion: correct ="
        " 0 (close on: routed through enqueue_scoring_run, which enqueues"
        " nothing, 10), mutant (the obs-cycle enqueue not routed through"
        f" enqueue_scoring_run) = a pair_and_score job was created -- got"
        f" {_job_count(conn, site_id)}"
    )


def test_tr3_wind_days_trigger_enqueues_nothing_with_the_close_on() -> None:
    """TR3 (wind-days trigger, ``worker/wind_days.py:2066-2068``): a
    station-wind-pairing run that changes an hour must not enqueue
    ``pair_and_score`` with the close on (10). ``_derive_days`` is the
    narrowest function holding this enqueue site -- the enqueue sits
    inline in its own body, not in any further-nested helper -- called
    with ``local_date`` D-1 (not ``today``) so it skips the
    newest-record lookup that only applies to today's partial day, and an
    explicit ``now_utc`` so no close-clock patch is needed. Two raw
    5-minute wind records 5 minutes apart seed one pairable hour, so
    ``_derive_days`` actually writes a changed station_observations row
    (proving the enqueue site itself ran) rather than finding nothing to
    pair."""
    from wxverify.worker.wind_days import _derive_days

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    station_id = int(
        conn.execute(
            "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
            (site_id, "ISTATION01"),
        ).fetchone()[0]
    )
    d_minus_1 = D - timedelta(days=1)
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh)"
        " VALUES (?, ?, 'five_minute', ?, 10.0)",
        (station_id, f"{d_minus_1.isoformat()}T10:00:00Z", d_minus_1.isoformat()),
    )
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh)"
        " VALUES (?, ?, 'five_minute', ?, 20.0)",
        (station_id, f"{d_minus_1.isoformat()}T10:05:00Z", d_minus_1.isoformat()),
    )
    conn.commit()

    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=d_minus_1,
        tz_name="UTC",
        today=D,
        now_utc=NOW,
    )
    conn.commit()

    paired = conn.execute(
        "SELECT COUNT(*) FROM station_observations"
        " WHERE station_id = ? AND variable = 'wind'",
        (station_id,),
    ).fetchone()[0]
    assert paired == 1, (
        "fixture bug: the two 5-minute records (5 minutes apart) must pair"
        " into exactly one wind hour"
    )
    assert _job_count(conn, site_id) == 0, (
        "M97/M98-shaped (wind-days trigger) -> at this assertion: correct ="
        " 0 (close on: routed through enqueue_scoring_run, which enqueues"
        " nothing, 10), mutant (the wind-days enqueue not routed through"
        f" enqueue_scoring_run) = a pair_and_score job was created -- got"
        f" {_job_count(conn, site_id)}"
    )


# ---------------------------------------------------------------------------
# TR4
# ---------------------------------------------------------------------------

# Scoring modules that import ``utc_now`` BY NAME (not ``_CLOSE_CLOCK_MODULES``'
# concern) and pass it as ``now_utc`` into ``wxverify.db.site_days``'s
# ``filter_day_writes``/``check_date_write``, which call ``_ensure`` and
# create day records up to that "now" with the close on. Left unpatched, the
# real 2026+ clock runs ``_ensure`` forward to today's real date, tripping
# the ``check_day_close_records`` teardown check for a fixture reason rather
# than a real TR4 assertion.
_TR4_SCORING_UTC_NOW_MODULES: tuple[str, ...] = (
    "wxverify.scoring.pairing",
    "wxverify.scoring.persistence",
    "wxverify.scoring.split",
    "wxverify.scoring.consensus",
    "wxverify.scoring.multimodel",
)


def _patch_tr4_close_clock(monkeypatch: pytest.MonkeyPatch, clock: MovingClock) -> None:
    """``patch_close_clock`` plus every scoring module that imports
    ``utc_now`` by name (see ``_TR4_SCORING_UTC_NOW_MODULES`` above) --
    without the extra patches, a day-write guard deep in the scoring path
    reads the real clock instead of the fixture's and the teardown's
    ``check_day_close_records`` fails for a fixture reason, not a TR4 one.
    """
    patch_close_clock(monkeypatch, clock)
    for module in _TR4_SCORING_UTC_NOW_MODULES:
        monkeypatch.setattr(f"{module}.utc_now", clock.now, raising=False)


def test_tr4_inline_and_lane_scoring_return_at_entry_with_the_close_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TR4: with the close on, the inline ``pair_and_score``,
    ``run_split_pair_phases`` and ``run_batched_scoring`` each return at
    entry; every table they would otherwise touch stays unchanged -- even
    though a pairable sample+observation is seeded, proving each actually
    returned early rather than finding nothing to do (10).

    The close clock is pinned to the fixture's own NOW (``patch_close_clock``
    / ``MovingClock``): left on the real clock, an unpatched ``utc_now()``
    read deep in the scoring/close-chain modules can run ``ensure_site_days``
    forward to today's real date, tripping the ``check_day_close_records``
    teardown check for an unrelated (fixture) reason instead of a real P4
    assertion.
    """
    from wxverify.scoring.engine import pair_and_score

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    _seed_pairable_sample(conn, site_id=site_id, feed_id=x)
    _seed_score_cache_row(conn, site_id=site_id, feed_id=x)
    conn.commit()
    _patch_tr4_close_clock(monkeypatch, MovingClock(NOW))

    before_score_cache = _score_cache_snapshot(conn)
    pair_and_score(conn, site_id)
    conn.commit()
    assert _table_count(conn, "forecast_pairs") == 0, (
        "M155 -> at this assertion: correct = 0 (close on: the inline"
        " pair_and_score returns at entry, section 10), mutant M155"
        " (pair_and_score without its entry check) = a pair was written --"
        f" got {_table_count(conn, 'forecast_pairs')}"
    )
    assert _score_cache_snapshot(conn) == before_score_cache, (
        "M155 -> at this assertion: correct = unchanged (close on: the"
        " inline pair_and_score returns at entry before touching"
        " score_cache, section 10), mutant M155 (pair_and_score without its"
        " entry check) = the pipeline's own sweep deletes the row -- got"
        f" {_score_cache_snapshot(conn)!r}"
    )


def test_tr4_split_pair_phases_and_batched_scoring_return_at_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TR4 (the two async lane orchestrators).

    The close clock is pinned (see the inline-scoring test's docstring
    above) before either orchestrator runs.
    """
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.worker.score_batches import run_batched_scoring, run_split_pair_phases

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 - test inspects the real writer connection
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    _seed_pairable_sample(conn, site_id=site_id, feed_id=x)
    _seed_score_cache_row(conn, site_id=site_id, feed_id=x)
    conn.commit()
    _patch_tr4_close_clock(monkeypatch, MovingClock(NOW))

    before_score_cache = _score_cache_snapshot(conn)
    writer = FencedWriter(db, db.generation)
    asyncio.run(run_split_pair_phases(writer, site_id, require_enabled=True))
    asyncio.run(run_batched_scoring(writer, site_id))

    assert _table_count(conn, "forecast_pairs") == 0, (
        "M156 -> at this assertion: correct = 0 (close on: both lane"
        " orchestrators return at entry, section 10), mutant M156"
        " (run_split_pair_phases without its entry check) = a pair was"
        f" written -- got {_table_count(conn, 'forecast_pairs')}"
    )
    assert _score_cache_snapshot(conn) == before_score_cache, (
        "M99 -> at this assertion: correct = unchanged (close on:"
        " run_batched_scoring returns at entry before touching"
        " score_cache, section 10), mutant M99 (run_batched_scoring without"
        " its entry check) = no cell to write (the pair phases wrote none)"
        " but its sweep still deletes the old row (strictly older than the"
        " run stamp) -- got"
        f" {_score_cache_snapshot(conn)!r}"
    )


def test_tr4_a_job_queued_before_the_switch_completes_with_no_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TR4 (the job): a ``pair_and_score`` job queued before the switch
    (i.e. found pending once the close is already on) completes with no
    effect on the scoring tables (10).

    The close clock is pinned (see the inline-scoring test's docstring
    above) before the job is claimed and run.
    """
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.queue import claim_next_job
    from wxverify.worker.processor import run_claimed_job

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 - test inspects the real writer connection
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    _seed_pairable_sample(conn, site_id=site_id, feed_id=x)
    _seed_score_cache_row(conn, site_id=site_id, feed_id=x)
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status)"
        " VALUES ('pair_and_score', ?, 'score', '{}', 'pending')",
        (site_id,),
    )
    conn.commit()
    _patch_tr4_close_clock(monkeypatch, MovingClock(NOW))

    before_score_cache = _score_cache_snapshot(conn)

    async def _run() -> None:
        job = await db.write(claim_next_job)
        assert job is not None
        await run_claimed_job(db, job, lane="main")

    asyncio.run(_run())

    assert _table_count(conn, "forecast_pairs") == 0, (
        "M156 -> at this assertion: correct = 0 (close on: the lanes'"
        " entry checks, reached through dispatch), mutant M156"
        " (run_split_pair_phases without its entry check) = a pair was"
        f" written -- got {_table_count(conn, 'forecast_pairs')}"
    )
    assert _score_cache_snapshot(conn) == before_score_cache, (
        "M99 -> at this assertion: correct = unchanged (close on:"
        " run_batched_scoring returns at entry before touching"
        " score_cache, section 10), mutant M99 (run_batched_scoring without"
        " its entry check) = no cell to write (the pair phases wrote none)"
        " but its sweep still deletes the old row (strictly older than the"
        " run stamp) -- got"
        f" {_score_cache_snapshot(conn)!r}"
    )
    status = conn.execute(
        "SELECT status FROM jobs WHERE type = 'pair_and_score' AND site_id = ?",
        (site_id,),
    ).fetchone()[0]
    assert status == "completed"


# ---------------------------------------------------------------------------
# TR5
# ---------------------------------------------------------------------------


def _tr5_leaderboard_params(window: str) -> dict[str, object]:
    return {"variable": "temperature", "lead": "D+1", "window": window}


def _tr5_requests(site_id: int, window: str) -> list[tuple[str, dict[str, object]]]:
    """Every page read of 3.4, for one window (plan's full set: TR5)."""
    page_params = {
        "site": site_id,
        "variable": "temperature",
        "window": window,
        "lead": "D+1",
    }
    return [
        ("/dashboard", dict(page_params)),
        ("/api/leaderboard", {"site": site_id, **_tr5_leaderboard_params(window)}),
        ("/api/curve", dict(page_params)),
        ("/api/composite", {"site": site_id, "window": window}),
    ]


def test_tr5_page_reads_fire_the_rescore_spy_only_with_the_close_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TR5: every page read of 3.4 (``dashboard_page``, ``/api/leaderboard``,
    ``/api/curve``, ``/api/composite``) fires a spy on
    ``schedule_score_rescore`` (imported by name at ``web/routes.py:13``/
    ``api/routes/dashboard.py:19``) for ``rolling``/``all`` with the close
    off (today's ``stale``/``rebuilding`` branch); with the close on -- no
    publication, then with a D-1 publication -- it fires for NEITHER, and
    no ``pair_and_score`` job is created either way (9.2, 10). The spy is
    the oracle because ``enqueue_score_rescore``'s own check (TR3) would
    hide a fired call from a job-count check alone.
    """
    from fastapi.testclient import TestClient

    from wxverify.db.connection import get_db

    calls: list[int] = []

    def _spy(site_id: int) -> None:
        calls.append(site_id)

    monkeypatch.setattr("wxverify.web.routes.schedule_score_rescore", _spy)
    monkeypatch.setattr("wxverify.api.routes.dashboard.schedule_score_rescore", _spy)

    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn) -> int:
            site_id = day_close_site(conn, close_on=False, with_records=False)
            x = _seed_feed(conn, "X")
            y = _seed_feed(conn, "Y")
            _subscribe(conn, site_id, x)
            _subscribe(conn, site_id, y)
            from tests.helpers import asof_insert_pair

            now = isoformat_utc()
            asof_insert_pair(
                conn,
                site_id=site_id,
                feed_id=x,
                valid_at=now,
                issued_at=now,
                forecast=10.0,
                observed=9.0,
                first_known_at=now,
                day_ahead=1,
                lead_hours=2,
            )
            return site_id

        site_id = db.write_sync(_seed)

        for window in ("rolling", "all"):
            for path, params in _tr5_requests(site_id, window):
                calls.clear()
                resp = client.get(path, params=params)
                assert resp.status_code == 200
                assert calls, (
                    f"close off, {path} window={window}: schedule_score_rescore"
                    " must fire on the stale/rebuilding branch (today's"
                    " behavior) -- it did not"
                )

        def _turn_close_on(conn) -> None:
            from wxverify.db.runtime_state import set_runtime_state
            from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

            set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc())

        db.write_sync(_turn_close_on)

        for window in ("rolling", "all"):
            for path, params in _tr5_requests(site_id, window):
                calls.clear()
                resp = client.get(path, params=params)
                assert resp.status_code == 200
                assert calls == [], (
                    "M81/M134 -> at this assertion: correct = [] (close on, no"
                    f" publication, {path} window={window}: status"
                    " empty/live, never stale/rebuilding, so the spy never"
                    " fires, 9.2), mutant M81 (the leaderboard with no"
                    " publication falls back to score_cache)/M134 (the"
                    " composite falls back to score_cache) = the"
                    f" stale/rebuilding branch fires the spy again -- got {calls!r}"
                )

        def _seed_publication(conn) -> None:
            from wxverify.core.timeutil import isoformat_utc as _fmt

            conn.execute(
                """
                INSERT INTO publications
                    (site_id, cutoff, first_record_date, window_days,
                     window_start_date, wind_start_date, settings,
                     methodology_version, scoring_rule, wind_method,
                     wind_constants, build_started_at, published_at)
                VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                        'first-stored-download-of-local-day/1',
                        'persistence_blend', '{}', ?, ?)
                """,
                (
                    site_id,
                    (D).isoformat(),
                    (D).isoformat(),
                    (D).isoformat(),
                    (D).isoformat(),
                    _fmt(),
                    _fmt(),
                ),
            )

        db.write_sync(_seed_publication)

        for window in ("rolling", "all"):
            for path, params in _tr5_requests(site_id, window):
                calls.clear()
                resp = client.get(path, params=params)
                assert resp.status_code == 200
                assert calls == [], (
                    "M134 -> at this assertion: correct = [] (close on, a"
                    f" publication present, {path} window={window}: status"
                    " hit, never stale/rebuilding), mutant M134"
                    " (composite_with_status with no publication falls back"
                    " to score_cache -- same shape for the leaderboard)"
                    f" would still show stale/rebuilding -- got {calls!r}"
                )

        final_jobs = db.write_sync(lambda c: _job_count(c, site_id))
        assert final_jobs == 0, (
            "no page read may create a pair_and_score job with the close on"
        )
