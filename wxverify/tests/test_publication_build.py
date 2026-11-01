"""The publication (daily-close plan 2026-10-22-daily-close-pairs-and-
publication.md, sections 7 and 8; test IDs PB1-PB17, plan 18.7).

Written before ``wxverify.scoring.publication`` exists: every test below
collects, then fails at its own ``from wxverify.scoring.publication import
...`` with ``ModuleNotFoundError`` -- the failure Step 3 accepts. Once
piece 1's migration exists but piece 6 does not, the same import still
fails first, before any table is touched.

Real SQLite throughout, through ``init_db`` on a ``tmp_path`` file (the
``standard_night``/``day_close_site`` pattern); nothing in the code under
test is mocked. Synthetic data only: stations ``ISTATION01``/``ISTATION02``
(via ``day_close_site``), feed sources ``synthetic-pub-x``/``-y``, zone
``UTC``. ``publish``/``publish_first`` are async; every test wraps its body
in an inner ``async def _run()`` and drives it with ``asyncio.run(_run())``
(this codebase's own pattern, e.g. ``tests/test_close_publication.py`` --
there is no ``pytest-asyncio`` dependency here, so ``async def test_...``
would never run).

Piece rule (plan 20, Step 4's table): PB rows go green in piece 6, so every
seed here is a plain INSERT into ``day_scores``/``starting_scores``/
``starting_wind_days``/``publications`` and its child tables -- never
through ``score_day`` or ``publish`` itself, except where a row's own text
says otherwise (PB14, PB15 exercise ``seed_starting_history`` and
``publish_first`` directly, so those two seed ``forecast_pairs`` instead,
the table that function reads per plan 7.3).

Column shapes assumed for the five new tables this file seeds directly
(``day_scores``, ``starting_scores``, ``starting_wind_days``,
``publications`` and its three child tables) are the plainest reading of
plan sections 6.4, 7.1 and 7.3 -- the schema does not exist yet to check
against. See the test-authoring report for the exact column lists and the
two functions patched as deterministic injection points (PB5, PB6).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import NOW, D, asof_make_site, day_close_site, note_day_clock
from wxverify import config
from wxverify.core.timeutil import is_canonical_utc_stamp, isoformat_utc, parse_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.site_days import begin_close, ensure_site_days, start_site_days
from wxverify.db.switch import ensure_site_generation
from wxverify.settings.keys import set_setting

CUTOFF = D - timedelta(days=1)
FIRST_RECORD_DATE = D - timedelta(days=3)

# ---------------------------------------------------------------------------
# Shared fixtures and seed helpers (own-helpers rule: nothing here touches
# tests/helpers.py or another test file).
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> tuple[Database, FencedWriter, sqlite3.Connection]:
    """A fresh real SQLite file under ``tmp_path``, as ``standard_night``
    opens one, returned with its writer and its raw connection."""
    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests build the fixture on the real writer conn
    return db, FencedWriter(db, db.generation), conn


def _close_through(
    conn: sqlite3.Connection, site_id: int, *, first_date, last_closed_date
) -> None:
    """Start ``site_days`` at ``first_date`` and close every day through
    ``last_closed_date`` inclusive, leaving the day after it ``open``
    (``ensure_site_days`` fills forward to ``far_future`` up front, as
    ``tests/test_close_scores.py``'s ``_prepare_closing_day`` does, but this
    also ``finish_close``s the last date instead of leaving it ``closing``).
    """
    from wxverify.db.site_days import finish_close

    start_dt = datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC)
    start_site_days(conn, site_id, first_date, now_utc=start_dt)
    far_future = note_day_clock(
        datetime(
            last_closed_date.year,
            last_closed_date.month,
            last_closed_date.day,
            tzinfo=UTC,
        )
        + timedelta(days=3)
    )
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d <= last_closed_date:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        begin_close(conn, site_id, d, now_utc=t)
        finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)


def _make_feed(conn: sqlite3.Connection, name: str) -> int:
    source = f"synthetic-pub-{name.lower()}"
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, 168, 0)",
        (source, source),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_day_score(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    variable: str = "temperature",
    day_ahead: int = 0,
    status: str = "scored",
    n: int = 0,
    err_n: int = 0,
    err_sum: float = 0.0,
    abs_sum: float = 0.0,
    sq_sum: float = 0.0,
    hits: int = 0,
    false_alarms: int = 0,
    misses: int = 0,
    correct_negs: int = 0,
    skill_n: int = 0,
    skill_sq_sum: float = 0.0,
    skill_ref_sq_sum: float = 0.0,
    download_id: int | None = None,
    download_date: str | None = None,
    fetched_at: str | None = None,
    wind_fc_high: float | None = None,
    wind_fc_hours: int | None = None,
    wind_obs_high: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO day_scores"
        " (site_id, local_date, feed_id, variable, day_ahead, status,"
        "  download_id, download_date, fetched_at, n, err_n, err_sum, abs_sum,"
        "  sq_sum, hits, false_alarms, misses, correct_negs, skill_n,"
        "  skill_sq_sum, skill_ref_sq_sum, wind_fc_high, wind_fc_hours,"
        "  wind_obs_high)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            status,
            download_id,
            download_date,
            fetched_at,
            n,
            err_n,
            err_sum,
            abs_sum,
            sq_sum,
            hits,
            false_alarms,
            misses,
            correct_negs,
            skill_n,
            skill_sq_sum,
            skill_ref_sq_sum,
            wind_fc_high,
            wind_fc_hours,
            wind_obs_high,
        ),
    )


def _seed_starting_score(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    variable: str = "temperature",
    day_ahead: int = 0,
    n: int = 0,
    err_n: int = 0,
    err_sum: float = 0.0,
    abs_sum: float = 0.0,
    sq_sum: float = 0.0,
    hits: int = 0,
    false_alarms: int = 0,
    misses: int = 0,
    correct_negs: int = 0,
    skill_n: int = 0,
    skill_sq_sum: float = 0.0,
    skill_ref_sq_sum: float = 0.0,
) -> None:
    conn.execute(
        "INSERT INTO starting_scores"
        " (site_id, local_date, feed_id, variable, day_ahead, n, err_n,"
        "  err_sum, abs_sum, sq_sum, hits, false_alarms, misses, correct_negs,"
        "  skill_n, skill_sq_sum, skill_ref_sq_sum)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            n,
            err_n,
            err_sum,
            abs_sum,
            sq_sum,
            hits,
            false_alarms,
            misses,
            correct_negs,
            skill_n,
            skill_sq_sum,
            skill_ref_sq_sum,
        ),
    )


def _seed_starting_wind_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    day_ahead: int,
    forecast_high: float,
    observed_high: float,
) -> None:
    conn.execute(
        "INSERT INTO starting_wind_days"
        " (site_id, local_date, feed_id, day_ahead, forecast_high, observed_high)"
        " VALUES (?,?,?,?,?,?)",
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            day_ahead,
            forecast_high,
            observed_high,
        ),
    )


_PER_DATE = {
    "n": 2,
    "err_n": 2,
    "err_sum": 2.0,
    "abs_sum": 2.0,
    "sq_sum": 2.0,
    "skill_n": 2,
    "skill_sq_sum": 2.0,
    "skill_ref_sq_sum": 4.0,
}


def _seed_uniform_x_scores(conn, site_id, feed_id, dates, *, closed_dates) -> None:
    """Seed one (feed, variable=temperature, day_ahead=0) row per date in
    ``dates``, identical sums (:data:`_PER_DATE`) on every date, into
    ``day_scores`` for a date in ``closed_dates`` and ``starting_scores``
    otherwise. The uniform sums keep bias/MAE/RMSE/skill the same whatever
    the date count, so only ``n`` (and bounds) distinguish a window that
    omits dates from one that does not -- exactly PB1/PB2's own oracle.
    """
    for local_date in dates:
        if local_date in closed_dates:
            _seed_day_score(
                conn,
                site_id=site_id,
                local_date=local_date,
                feed_id=feed_id,
                status="scored",
                **_PER_DATE,
            )
        else:
            _seed_starting_score(
                conn,
                site_id=site_id,
                local_date=local_date,
                feed_id=feed_id,
                **_PER_DATE,
            )


def _build_base(tmp_path: Path):
    """PB1-PB9's shared site: zone UTC, first record D-3, closed D-3..D-1,
    D open; feed X with uniform scores on D-8..D-1 (D-8..D-4 starting,
    D-3..D-1 closed), ``rolling_window_days`` 5, ``min_n`` 9.
    """
    db, writer, conn = _make_db(tmp_path)
    site_id = day_close_site(conn, close_on=True, with_records=False)
    _close_through(conn, site_id, first_date=FIRST_RECORD_DATE, last_closed_date=CUTOFF)
    feed_x = _make_feed(conn, "X")
    set_setting(conn, "rolling_window_days", "5")
    set_setting(conn, "min_n", "9")
    dates = [D - timedelta(days=k) for k in range(8, 0, -1)]
    closed = {D - timedelta(days=k) for k in range(1, 4)}
    _seed_uniform_x_scores(conn, site_id, feed_x, dates, closed_dates=closed)
    conn.commit()
    return db, writer, conn, site_id, feed_x


def _fetch_publication(
    conn: sqlite3.Connection, site_id: int, cutoff
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM publications WHERE site_id = ? AND cutoff = ?",
        (site_id, cutoff.isoformat()),
    ).fetchone()


def _fetch_ranking(
    conn: sqlite3.Connection,
    publication_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_key: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM publication_rankings"
        " WHERE publication_id = ? AND feed_id = ? AND variable = ?"
        " AND day_ahead = ? AND window_key = ?",
        (publication_id, feed_id, variable, day_ahead, window_key),
    ).fetchone()


def _fetch_coverage(
    conn: sqlite3.Connection,
    publication_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_key: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM publication_coverage"
        " WHERE publication_id = ? AND feed_id = ? AND variable = ?"
        " AND day_ahead = ? AND window_key = ?",
        (publication_id, feed_id, variable, day_ahead, window_key),
    ).fetchone()


def _fetch_wind_weight(
    conn: sqlite3.Connection,
    publication_id: int,
    feed_id: int,
    day_ahead: int,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM publication_wind_weights"
        " WHERE publication_id = ? AND feed_id = ? AND day_ahead = ?",
        (publication_id, feed_id, day_ahead),
    ).fetchone()


def _publication_table_counts(conn: sqlite3.Connection, site_id: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    pub_ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT id FROM publications WHERE site_id = ?", (site_id,)
        ).fetchall()
    ]
    counts["publications"] = len(pub_ids)
    for table in (
        "publication_rankings",
        "publication_wind_weights",
        "publication_coverage",
    ):
        if not pub_ids:
            counts[table] = 0
            continue
        placeholders = ",".join("?" * len(pub_ids))
        counts[table] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE publication_id IN"
                f" ({placeholders})",
                pub_ids,
            ).fetchone()[0]
        )
    return counts


# ---------------------------------------------------------------------------
# PB1
# ---------------------------------------------------------------------------


def test_pb1_publish_builds_rankings_bounds_and_settings(tmp_path: Path) -> None:
    """PB1: one ``publish`` call for cutoff D-1 writes a ``publications``
    row with the right bounds and settings, and X's ``w:5``/``w:all``
    rankings sum the right dates (7.1-7.3, 8.2).
    """

    async def _run() -> None:
        from wxverify.forecast.wind_blend import WIND_METHOD
        from wxverify.scoring.publication import publish
        from wxverify.verification.methodology import METHODOLOGY_VERSION

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)

        publication_id_text = await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None, "publish must insert a publications row"
        assert str(pub["id"]) == publication_id_text
        assert pub["window_start_date"] == (CUTOFF - timedelta(days=4)).isoformat()
        assert pub["wind_start_date"] == (CUTOFF - timedelta(days=29)).isoformat()
        assert pub["methodology_version"] == METHODOLOGY_VERSION
        assert pub["wind_method"] == WIND_METHOD
        assert pub["first_record_date"] == FIRST_RECORD_DATE.isoformat()

        rank5 = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert rank5 is not None
        assert rank5["n"] == 10
        assert rank5["bias"] == pytest.approx(1.0)
        assert rank5["mae"] == pytest.approx(1.0)
        assert rank5["rmse"] == pytest.approx(1.0)
        assert rank5["skill_score"] == pytest.approx(0.5)

        rank_all = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:all")
        assert rank_all is not None
        assert rank_all["n"] == 16, (
            "M64 -> at this assertion: correct = 16 (w:all sums the 5"
            " starting dates D-8..D-4 plus the 3 closed dates D-3..D-1, 8"
            " dates * n=2), mutant M64 (the starting history left out of"
            " w:all) = 6 (the 3 closed dates only)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB2
# ---------------------------------------------------------------------------


def test_pb2_a_deleted_date_shrinks_the_sum_not_the_bounds(tmp_path: Path) -> None:
    """PB2: D-2's ``day_scores`` rows deleted; the ``w:5`` window still
    starts at D-5 (7.2) and sums only the 4 remaining dates (7.2)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)
        conn.execute(
            "DELETE FROM day_scores"
            " WHERE site_id = ? AND local_date = ? AND feed_id = ?",
            (site_id, (D - timedelta(days=2)).isoformat(), feed_x),
        )
        conn.commit()

        await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        assert pub["window_start_date"] == (CUTOFF - timedelta(days=4)).isoformat(), (
            "M63 -> at this assertion: correct = D-5 (a window counts"
            " calendar dates, never days with data, 7.2), mutant M63"
            " (w:{N} counted in days with data) would need a 6th calendar"
            " date to reach 5 WITH data, moving window_start_date to D-6"
        )
        rank5 = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert rank5 is not None
        assert rank5["n"] == 8, (
            "M65 -> at this assertion: correct = 8 (the 4 remaining dates'"
            " sums only; a window never reaches back to replace a missing"
            " date, 7.2), mutant M65 (a window reaches back past a missing"
            " date) = 10 (pulls in a 6th date, D-6, to replace D-2)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB3
# ---------------------------------------------------------------------------


def test_pb3_repeat_publish_returns_same_id_and_rebuilds_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB3: a second ``publish`` for the same cutoff returns the first
    id and writes nothing new -- proven by emptying the inputs before the
    second call and showing the first ranking row survives untouched
    (8.2).

    The row-unchanged check alone cannot discriminate M66 (the repeat
    check at step 1 removed): even with it gone, ``_swap``'s OWN repeat
    check (8.3 step 1, ``scoring/publication.py``'s ``_swap``) still
    fires on the WRITE side and returns the existing id before any insert,
    so the stored row survives either way. What step 1's check actually
    buys is never reaching the BUILD (8.2 steps 2-5) at all on a repeat;
    with it removed, the second call wastefully rebuilds from the
    (emptied) inputs although the swap then discards the result. A spy on
    ``compute_wind_weights`` -- the one pure step 8.2 always reaches,
    patched the same way PB5/PB6/PB9 inject into it -- counts the builds.
    """

    async def _run() -> None:
        import wxverify.scoring.publication as pub_module
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)

        real_compute = pub_module.compute_wind_weights
        build_calls = {"n": 0}

        def _counting_compute(training):
            build_calls["n"] += 1
            return real_compute(training)

        monkeypatch.setattr(pub_module, "compute_wind_weights", _counting_compute)

        first_id = await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )
        assert build_calls["n"] == 1, "fixture bug: the first publish must build once"
        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        before = dict(_fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5"))

        conn.execute("DELETE FROM day_scores WHERE site_id = ?", (site_id,))
        conn.execute("DELETE FROM starting_scores WHERE site_id = ?", (site_id,))
        conn.commit()

        second_id = await publish(
            db,
            writer,
            site_id=site_id,
            cutoff=CUTOFF,
            attempt=2,
            now_utc=NOW + timedelta(minutes=5),
        )
        assert second_id == first_id
        assert build_calls["n"] == 1, (
            "M66 -> at this assertion: correct = 1 (the repeat check at"
            " 8.2 step 1 returns the existing id before the build is ever"
            " reached a second time; compute_wind_weights is the one pure"
            " step the build always calls, once per build, so this count"
            " is the build count), mutant M66 (the repeat check removed)"
            " = 2 (the second publish call reaches the build -- and so"
            " compute_wind_weights -- even though _swap's own, separate"
            " repeat check then discards the result and the row below is"
            " unaffected either way)"
        )

        after_row = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert after_row is not None and dict(after_row) == before, (
            "M66 -> at this assertion: correct = the original n=10/bias=1.0"
            " row unchanged (a repeat returns the existing id and runs"
            " nothing else), mutant M66 (the repeat check removed) = the"
            " row gone (its inputs were emptied before the second call) or"
            " an IntegrityError from a duplicate (site_id, cutoff) insert"
            " -- in fact neither: _swap's own inner repeat check (8.3 step"
            " 1) still catches it and this row stays unchanged either way;"
            " the build-count assertion above is M66's actual kill"
        )
        counts = _publication_table_counts(conn, site_id)
        assert counts["publications"] == 1

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB4
# ---------------------------------------------------------------------------


def test_pb4_an_open_or_recordless_cutoff_fails(tmp_path: Path) -> None:
    """PB4: an open cutoff (D) and one with no record (D-9) both fail with
    ``PublicationFailed("cutoff_not_closed")``; no rows (8.2)."""

    async def _run() -> None:
        from wxverify.scoring.publication import PublicationFailed, publish

        db, writer, conn, site_id, _feed_x = _build_base(tmp_path)

        with pytest.raises(PublicationFailed) as exc_info:
            await publish(db, writer, site_id=site_id, cutoff=D, attempt=1, now_utc=NOW)
        assert "cutoff_not_closed" in str(exc_info.value), (
            "M67 -> at this assertion: correct = PublicationFailed"
            " ('cutoff_not_closed') for an open cutoff (the cutoff's record"
            " state is checked), mutant M67 (the cutoff's state not"
            " checked) would build a publication for an open day instead"
            " of raising"
        )
        assert _fetch_publication(conn, site_id, D) is None

        with pytest.raises(PublicationFailed) as exc_info2:
            await publish(
                db,
                writer,
                site_id=site_id,
                cutoff=D - timedelta(days=9),
                attempt=1,
                now_utc=NOW,
            )
        assert "cutoff_not_closed" in str(exc_info2.value)
        assert _fetch_publication(conn, site_id, D - timedelta(days=9)) is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB5
# ---------------------------------------------------------------------------


def test_pb5_a_setting_saved_mid_build_waits_for_the_next_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB5: ``rolling_window_days`` changed from 5 to 7 between the build's
    settings snapshot and its wind-weight step (injected at the one call to
    ``compute_wind_weights``, the pure step 8.2 places after the ranking
    sums and the settings read): the publication built from THIS call
    still keeps window 5 (8.2, 13.1).

    Assumes ``scoring.publication`` imports ``compute_wind_weights`` by
    name from ``forecast.wind_blend`` (this file's own assumption, listed
    in the test-authoring report) -- a deterministic injection point, not
    a timing race: the real function still runs, after the side effect.
    """

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)
        db_path = str(config.db_path)

        import wxverify.scoring.publication as pub_module

        real_compute = pub_module.compute_wind_weights
        fired = {"done": False}

        def _bump_then_compute(training):
            if not fired["done"]:
                fired["done"] = True
                side = sqlite3.connect(db_path)
                try:
                    side.execute(
                        "UPDATE settings SET value = '7'"
                        " WHERE key = 'rolling_window_days'"
                    )
                    side.commit()
                finally:
                    side.close()
            return real_compute(training)

        monkeypatch.setattr(pub_module, "compute_wind_weights", _bump_then_compute)

        await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        assert pub["window_days"] == 5
        rank = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert rank is not None
        assert rank["n"] == 10, (
            "M68 -> at this assertion: correct = 10 (w:5 built from the"
            " rolling_window_days value read at the build's own snapshot,"
            " 5, before the mid-build change to 7), mutant M68 (settings"
            " read after the snapshot) = 14 (w:7 over D-7..D-1, picking up"
            " the change before computing the window)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB6
# ---------------------------------------------------------------------------


def test_pb6_the_site_deleted_mid_build_fails_the_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB6: the site deleted between the build's read phase and the swap
    (injected at the same ``compute_wind_weights`` call as PB5) fails the
    swap with ``PublicationFailed("no_site")`` rather than writing a
    publication for a gone site (8.3)."""

    async def _run() -> None:
        from wxverify.scoring.publication import PublicationFailed, publish

        db, writer, conn, site_id, _feed_x = _build_base(tmp_path)
        db_path = str(config.db_path)

        import wxverify.scoring.publication as pub_module

        real_compute = pub_module.compute_wind_weights
        fired = {"done": False}

        def _delete_site_then_compute(training):
            if not fired["done"]:
                fired["done"] = True
                side = sqlite3.connect(db_path)
                try:
                    side.execute("DELETE FROM sites WHERE id = ?", (site_id,))
                    side.commit()
                finally:
                    side.close()
            return real_compute(training)

        monkeypatch.setattr(
            pub_module, "compute_wind_weights", _delete_site_then_compute
        )

        with pytest.raises(PublicationFailed) as exc_info:
            await publish(
                db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
            )
        assert "no_site" in str(exc_info.value), (
            "M69 -> at this assertion: correct = PublicationFailed"
            " ('no_site') (the swap re-checks the site row), mutant M69"
            " (the site check removed from the swap) would insert a"
            " publications row for a site that no longer exists instead of"
            " raising"
        )
        assert fired["done"], "the site must have actually been deleted mid-build"
        assert _fetch_publication(conn, site_id, CUTOFF) is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB7
# ---------------------------------------------------------------------------


def test_pb7_a_poisoned_swap_insert_leaves_no_row_anywhere(tmp_path: Path) -> None:
    """PB7: a trigger that raises on INSERT into ``publication_coverage``
    fails the whole swap; since it is one ``writer.write``, no row lands
    in ``publications``, ``publication_rankings`` or
    ``publication_wind_weights`` either (8.3)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, _feed_x = _build_base(tmp_path)
        conn.execute(
            """
            CREATE TRIGGER pb7_poison_coverage
            BEFORE INSERT ON publication_coverage
            BEGIN
                SELECT RAISE(ABORT, 'pb7 poison');
            END
            """
        )
        conn.commit()

        with pytest.raises(Exception):  # noqa: B017, PT011 -- any DB error is the point
            await publish(
                db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
            )

        counts = _publication_table_counts(conn, site_id)
        assert counts == {
            "publications": 0,
            "publication_rankings": 0,
            "publication_wind_weights": 0,
            "publication_coverage": 0,
        }, (
            "M70 -> at this assertion: correct = every publication table at"
            " 0 rows for this site (one writer.write, so a poisoned INSERT"
            " rolls the whole swap back), mutant M70 (the swap split into"
            " two writes) would leave the publications/rankings/weights"
            " rows committed from an earlier write before the coverage"
            " insert fails"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB8
# ---------------------------------------------------------------------------


def _seed_publication_direct(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff,
    published_at: datetime,
) -> int:
    cur = conn.execute(
        "INSERT INTO publications"
        " (site_id, cutoff, first_record_date, window_days, window_start_date,"
        "  wind_start_date, settings, methodology_version, scoring_rule,"
        "  wind_method, wind_constants, build_started_at, published_at)"
        " VALUES (?,?,?,5,?,?,?,4,'first-stored-download-of-local-day/1',"
        "         'inverse_mae_daily_high_v1','{}',?,?)",
        (
            site_id,
            cutoff.isoformat(),
            FIRST_RECORD_DATE.isoformat(),
            (cutoff - timedelta(days=4)).isoformat(),
            (cutoff - timedelta(days=29)).isoformat(),
            '{"rolling_window_days": 5, "min_n": 9}',
            isoformat_utc(published_at),
            isoformat_utc(published_at),
        ),
    )
    assert cur.lastrowid is not None
    publication_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO publication_rankings"
        " (publication_id, variable, day_ahead, window_key, feed_id, n, bias,"
        "  mae, rmse, pod, far, csi, ets, hss, skill_score)"
        " VALUES (?,'temperature',0,'w:5',1,2,1.0,1.0,1.0,"
        "         NULL,NULL,NULL,NULL,NULL,0.5)",
        (publication_id,),
    )
    return publication_id


@pytest.mark.parametrize(
    ("delta", "expect_kept"),
    [
        (timedelta(hours=192) - timedelta(seconds=1), True),
        (timedelta(hours=192), True),
        (timedelta(hours=192) + timedelta(seconds=1), False),
    ],
)
def test_pb8_an_older_publication_is_kept_until_192h_past_its_successor(
    tmp_path: Path,
    delta: timedelta,
    expect_kept: bool,
) -> None:
    """PB8: D-5's publication, whose successor (D-3's) was published
    ``delta`` before the clock of a new build. Kept at 192h-1s and at
    exactly 192h; deleted with its children past 192h (7.5)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn = _make_db(tmp_path)
        site_id = day_close_site(conn, close_on=True, with_records=False)
        _close_through(
            conn, site_id, first_date=FIRST_RECORD_DATE, last_closed_date=CUTOFF
        )
        conn.commit()

        d3_published_at = NOW
        d5_published_at = NOW - timedelta(hours=1)
        d5_cutoff = D - timedelta(days=5)
        d3_cutoff = D - timedelta(days=3)
        _seed_publication_direct(
            conn, site_id=site_id, cutoff=d5_cutoff, published_at=d5_published_at
        )
        _seed_publication_direct(
            conn, site_id=site_id, cutoff=d3_cutoff, published_at=d3_published_at
        )
        conn.commit()

        await publish(
            db,
            writer,
            site_id=site_id,
            cutoff=CUTOFF,
            attempt=1,
            now_utc=d3_published_at + delta,
        )

        remaining = _fetch_publication(conn, site_id, d5_cutoff)
        if expect_kept:
            assert remaining is not None, (
                f"M71 -> at this assertion: correct = D-5's publication"
                f" kept at delta={delta} (the keep rule is a strict '>'"
                " over PUBLICATION_KEEP_HOURS, 192), mutant M71 (the keep"
                " rule with '>=') would delete it exactly at the 192h"
                " boundary"
            )
        else:
            assert remaining is None, (
                f"M128 -> at this assertion: correct = D-5's publication"
                f" deleted at delta={delta} (PUBLICATION_KEEP_HOURS is"
                " 192), mutant M128 (the keep rule at 24h instead of"
                " PUBLICATION_KEEP_HOURS) would ALSO have deleted it at"
                " 192h - the discriminating case is the 192h-1s one above,"
                " where the correct rule keeps it and the 24h rule would"
                " already have deleted it"
            )
            rank_count = conn.execute(
                "SELECT COUNT(*) FROM publication_rankings"
                " WHERE publication_id NOT IN (SELECT id FROM publications)",
            ).fetchone()[0]
            assert rank_count == 0, "children must cascade-delete with the publication"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB9
# ---------------------------------------------------------------------------


def test_pb9_a_second_connection_holding_begin_immediate_only_blocks_the_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB9: a second connection holds ``BEGIN IMMEDIATE`` through the
    build's read phase; the build completes and reaches its wind-weight
    step promptly (its reads run outside the write lock, 8.2); only the
    swap (8.3's one ``writer.write``) then waits for the lock (K18).

    A rendezvous, not a timing race: the wind-weight step (the same
    deterministic injection point PB5/PB6 use) signals a ``threading.
    Event`` the moment it is entered -- it runs on ``db.read``'s own
    worker thread, so a cross-thread event is the real synchronization
    primitive here, not a fixed sleep -- proving the build's reads were
    not blocked, then waits on a second ``Event`` the test controls before
    letting the pure compute (and the swap behind it) proceed. ``publish``
    itself runs as a task on this test's own event loop (never a second
    nested one), so waiting on the first event from the loop thread uses
    an executor hop, never blocking the loop.
    """
    import threading

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)
        db_path = str(config.db_path)

        import wxverify.scoring.publication as pub_module

        real_compute = pub_module.compute_wind_weights
        reached_wind_step = threading.Event()
        release_lock = threading.Event()

        def _signal_then_compute(training):
            reached_wind_step.set()
            # M72's own discriminator: if the build's sums were read
            # inside writer.write, this call never arrives at all while
            # the second connection holds BEGIN IMMEDIATE -- the whole
            # build blocks before getting here, and the wait right below
            # (on the loop thread) times out instead of being set.
            release_lock.wait(timeout=10.0)
            return real_compute(training)

        monkeypatch.setattr(pub_module, "compute_wind_weights", _signal_then_compute)

        # A real second connection to the same file, holding the write
        # lock directly -- never through the app's own FencedWriter.
        blocker = sqlite3.connect(db_path, timeout=0)
        blocker.execute("PRAGMA busy_timeout=30000")
        blocker.execute("BEGIN IMMEDIATE")

        loop = asyncio.get_running_loop()
        publish_task = asyncio.ensure_future(
            publish(db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW)
        )
        try:
            reached = await loop.run_in_executor(None, reached_wind_step.wait, 5.0)
            assert reached, (
                "M72 -> at this assertion: correct = the wind-weight step"
                " reached promptly (the build's reads run outside the"
                " write lock, 8.2), mutant M72 (the build's sums read"
                " inside writer.write) = this call never arrives while"
                " the second connection holds BEGIN IMMEDIATE -- the"
                " whole build blocks first"
            )

            # The build is now parked on release_lock.wait, not on the
            # swap. Release just enough for the pure compute to return,
            # then confirm the publish task is STILL pending a short beat
            # later -- on the swap's own BEGIN IMMEDIATE, which contends
            # with the still-held external lock.
            release_lock.set()
            done, pending = await asyncio.wait({publish_task}, timeout=1.0)
            assert publish_task in pending, (
                "M72 -> at this assertion: correct = the publish task"
                " still pending once its read phase completes (only the"
                " swap, 8.3's one writer.write, waits for the lock, 8.2),"
                " mutant M72 (the build itself holds or re-enters a write"
                " lock during its reads) would already have finished (or"
                " raised) here instead of still waiting on the external"
                " BEGIN IMMEDIATE"
            )
        finally:
            blocker.commit()
            blocker.close()

        publication_id_text = await asyncio.wait_for(publish_task, timeout=10.0)
        assert publication_id_text is not None

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        assert str(pub["id"]) == publication_id_text

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB10
# ---------------------------------------------------------------------------


def test_pb10_coverage_counts_scored_expected_and_starting_days_apart(
    tmp_path: Path,
) -> None:
    """PB10: of a 7-date window (2 starting, 5 closed), X is scored on 3
    closed dates, absent on 1, and the variable not_counted on the last ->
    scored 3, expected 4 (not_counted excluded), starting_days 2 (7.4)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn = _make_db(tmp_path)
        first_date = D - timedelta(days=5)
        cutoff = D - timedelta(days=1)
        site_id = day_close_site(conn, close_on=True, with_records=False)
        _close_through(conn, site_id, first_date=first_date, last_closed_date=cutoff)
        feed_x = _make_feed(conn, "X")
        set_setting(conn, "rolling_window_days", "7")
        set_setting(conn, "min_n", "1")

        starting_dates = [
            first_date - timedelta(days=2),
            first_date - timedelta(days=1),
        ]
        for sd in starting_dates:
            _seed_starting_score(
                conn, site_id=site_id, local_date=sd, feed_id=feed_x, **_PER_DATE
            )

        closed_dates = [first_date + timedelta(days=k) for k in range(5)]
        assert closed_dates[-1] == cutoff
        scored_dates = closed_dates[:3]
        absent_date = closed_dates[3]
        not_counted_date = closed_dates[4]
        for cd in scored_dates:
            _seed_day_score(
                conn,
                site_id=site_id,
                local_date=cd,
                feed_id=feed_x,
                status="scored",
                **_PER_DATE,
            )
        _seed_day_score(
            conn,
            site_id=site_id,
            local_date=absent_date,
            feed_id=feed_x,
            status="absent",
            n=0,
            err_n=0,
        )
        _seed_day_score(
            conn,
            site_id=site_id,
            local_date=not_counted_date,
            feed_id=feed_x,
            status="not_counted",
            n=0,
            err_n=0,
        )
        conn.commit()

        await publish(
            db, writer, site_id=site_id, cutoff=cutoff, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, cutoff)
        assert pub is not None
        coverage = _fetch_coverage(conn, pub["id"], feed_x, "temperature", 0, "w:7")
        assert coverage is not None
        assert (
            coverage["scored"],
            coverage["expected"],
            coverage["starting_days"],
        ) == (3, 4, 2), (
            "M73 -> at this assertion: correct = (3, 4, 2) (expected counts"
            " the 3 scored plus the 1 absent date, excluding the 1"
            " not_counted date), mutant M73 (not_counted dates counted as"
            " expected) = (3, 5, 2)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB11
# ---------------------------------------------------------------------------


def test_pb11_wind_weight_needs_min_training_days(tmp_path: Path) -> None:
    """PB11: X has 20 qualifying wind days in the 30-date window, Y 19 ->
    X gets a weight (mae_ms the mean absolute difference of its highs), Y
    none (MIN_TRAINING_DAYS, 8.2)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, feed_x = _build_base(tmp_path)
        feed_y = _make_feed(conn, "Y")

        # 17 starting dates before D-3 (first record), plus the 3 closed
        # dates D-3..D-1, give X 20 days; Y skips the earliest starting
        # date (19).
        starting_dates = [D - timedelta(days=k) for k in range(20, 3, -1)]
        closed_dates = [D - timedelta(days=k) for k in range(3, 0, -1)]
        assert len(starting_dates) == 17
        assert len(closed_dates) == 3

        for sd in starting_dates:
            _seed_starting_wind_day(
                conn,
                site_id=site_id,
                local_date=sd,
                feed_id=feed_x,
                day_ahead=1,
                forecast_high=12.0,
                observed_high=10.0,
            )
            if sd != starting_dates[0]:
                _seed_starting_wind_day(
                    conn,
                    site_id=site_id,
                    local_date=sd,
                    feed_id=feed_y,
                    day_ahead=1,
                    forecast_high=11.0,
                    observed_high=10.0,
                )
        for cd in closed_dates:
            _seed_day_score(
                conn,
                site_id=site_id,
                local_date=cd,
                feed_id=feed_x,
                variable="wind",
                day_ahead=1,
                status="scored",
                n=24,
                err_n=24,
                wind_fc_high=12.0,
                wind_fc_hours=24,
                wind_obs_high=10.0,
            )
            _seed_day_score(
                conn,
                site_id=site_id,
                local_date=cd,
                feed_id=feed_y,
                variable="wind",
                day_ahead=1,
                status="scored",
                n=24,
                err_n=24,
                wind_fc_high=11.0,
                wind_fc_hours=24,
                wind_obs_high=10.0,
            )
        conn.commit()

        await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        weight_x = _fetch_wind_weight(conn, pub["id"], feed_x, 1)
        assert weight_x is not None
        assert weight_x["days"] == 20
        assert weight_x["mae_ms"] == pytest.approx(2.0)

        weight_y = _fetch_wind_weight(conn, pub["id"], feed_y, 1)
        assert weight_y is None, (
            "M74 -> at this assertion: correct = no weight row for Y (only"
            " 19 qualifying days, below MIN_TRAINING_DAYS 20), mutant M74"
            " (MIN_TRAINING_DAYS not applied) = a weight row for Y too"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB14
# ---------------------------------------------------------------------------


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id,
    feed_id,
    variable,
    issued_at,
    valid_at,
    lead_hours,
    day_ahead,
    forecast,
    observed,
    generation_id,
) -> None:
    error = forecast - observed
    conn.execute(
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  day_ahead, forecast, observed, error, abs_error, sq_error,"
        "  tz_generation_id)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
            generation_id,
        ),
    )


def test_pb14_publish_first_builds_the_cutover_publication(tmp_path: Path) -> None:
    """PB14: ``publish_first`` for D-3 (no P1 record yet) builds cutoff D-4
    from starting rows only; a repeat returns the same id; D-2 gives a new
    publication at cutoff D-3; an empty site still gets a publication, with
    no ranking rows (16.3)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish_first, seed_starting_history

        db, writer, conn = _make_db(tmp_path)
        site_id = asof_make_site(conn, "pb14-site")
        generation_id = ensure_site_generation(conn, site_id)
        feed_x = _make_feed(conn, "X")
        starting_date = D - timedelta(days=4)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_x,
            variable="temperature",
            issued_at=f"{(starting_date - timedelta(days=1)).isoformat()}T00:00:00Z",
            valid_at=f"{starting_date.isoformat()}T12:00:00Z",
            lead_hours=36,
            day_ahead=1,
            forecast=12.0,
            observed=10.0,
            generation_id=generation_id,
        )
        conn.commit()

        seed_starting_history(
            conn,
            site_id=site_id,
            first_record_date=D - timedelta(days=3),
            generation_id=generation_id,
            timezone="UTC",
        )
        conn.commit()

        first_id = await publish_first(
            db,
            writer,
            site_id=site_id,
            first_record_date=D - timedelta(days=3),
            now_utc=NOW,
        )
        pub = _fetch_publication(conn, site_id, D - timedelta(days=4))
        assert pub is not None
        assert str(pub["id"]) == first_id
        assert pub["first_record_date"] == (D - timedelta(days=3)).isoformat()
        rank = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 1, "w:all")
        assert rank is not None and rank["n"] == 1

        repeat_id = await publish_first(
            db,
            writer,
            site_id=site_id,
            first_record_date=D - timedelta(days=3),
            now_utc=NOW + timedelta(minutes=1),
        )
        assert repeat_id == first_id, (
            "M77 -> at this assertion: correct = the same id on a repeat"
            " for the same first_record_date, mutant M77's own text"
            " (publish_first requires a closed cutoff) would instead raise"
            " PublicationFailed('cutoff_not_closed') here, since D-4 is"
            " never a P1 record"
        )

        second_id = await publish_first(
            db,
            writer,
            site_id=site_id,
            first_record_date=D - timedelta(days=2),
            now_utc=NOW + timedelta(minutes=2),
        )
        assert second_id != first_id
        pub2 = _fetch_publication(conn, site_id, D - timedelta(days=3))
        assert pub2 is not None
        assert str(pub2["id"]) == second_id

        empty_db, empty_writer, empty_conn = _make_db(tmp_path / "empty")
        empty_site_id = asof_make_site(empty_conn, "pb14-empty-site")
        empty_conn.commit()
        empty_id = await publish_first(
            empty_db,
            empty_writer,
            site_id=empty_site_id,
            first_record_date=D - timedelta(days=3),
            now_utc=NOW,
        )
        empty_pub = _fetch_publication(empty_conn, empty_site_id, D - timedelta(days=4))
        assert empty_pub is not None and str(empty_pub["id"]) == empty_id
        empty_rank_count = empty_conn.execute(
            "SELECT COUNT(*) FROM publication_rankings WHERE publication_id = ?",
            (empty_pub["id"],),
        ).fetchone()[0]
        assert empty_rank_count == 0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB15
# ---------------------------------------------------------------------------


def test_pb15_seed_starting_history_sums_only_the_passed_generation(
    tmp_path: Path,
) -> None:
    """PB15: two generations' ``forecast_pairs`` on the same date; the seed
    sums only the passed generation's rows, and a second call replaces,
    never adds (7.3). Synchronous: ``seed_starting_history`` is not
    marked async (16.3)."""
    from wxverify.scoring.publication import seed_starting_history

    _db, _writer, conn = _make_db(tmp_path)
    site_id = asof_make_site(conn, "pb15-site")
    g1 = ensure_site_generation(conn, site_id)
    cur = conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (?, 'UTC', 'initial', 'published')",
        (site_id,),
    )
    assert cur.lastrowid is not None
    g2 = int(cur.lastrowid)
    feed_x = _make_feed(conn, "X")
    local_date = D - timedelta(days=6)
    issued_at = f"{(local_date - timedelta(days=1)).isoformat()}T00:00:00Z"
    valid_at = f"{local_date.isoformat()}T12:00:00Z"

    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        variable="temperature",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=36,
        day_ahead=1,
        forecast=12.0,
        observed=10.0,
        generation_id=g1,
    )
    _seed_pair(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        variable="temperature",
        issued_at=issued_at,
        valid_at=f"{local_date.isoformat()}T13:00:00Z",
        lead_hours=37,
        day_ahead=1,
        forecast=100.0,
        observed=10.0,
        generation_id=g2,
    )
    conn.commit()

    seed_starting_history(
        conn,
        site_id=site_id,
        first_record_date=D - timedelta(days=3),
        generation_id=g1,
        timezone="UTC",
    )
    conn.commit()

    row = conn.execute(
        "SELECT n, err_sum FROM starting_scores"
        " WHERE site_id = ? AND local_date = ? AND feed_id = ? AND variable = ?"
        " AND day_ahead = 1",
        (site_id, local_date.isoformat(), feed_x, "temperature"),
    ).fetchone()
    assert row is not None
    assert (row["n"], row["err_sum"]) == (1, 2.0), (
        "M78 -> at this assertion: correct = (1, 2.0) (only g1's pair,"
        " forecast 12 - observed 10), mutant M78 (the seed ignores the"
        " generation it is passed) would also sum g2's pair, giving"
        " (2, 92.0)"
    )

    seed_starting_history(
        conn,
        site_id=site_id,
        first_record_date=D - timedelta(days=3),
        generation_id=g1,
        timezone="UTC",
    )
    conn.commit()
    count = conn.execute(
        "SELECT COUNT(*) FROM starting_scores"
        " WHERE site_id = ? AND local_date = ? AND feed_id = ? AND variable = ?"
        " AND day_ahead = 1",
        (site_id, local_date.isoformat(), feed_x, "temperature"),
    ).fetchone()[0]
    assert count == 1, "a second call must replace, not add, a duplicate row"


# ---------------------------------------------------------------------------
# PB16
# ---------------------------------------------------------------------------


def test_pb16_build_started_at_published_at_whole_second_microseconds_dropped(
    tmp_path: Path,
) -> None:
    """PB16: as PB1, with ``now_utc`` at an instant whose microsecond is
    250000: ``publications.build_started_at`` and ``published_at`` pass
    ``is_canonical_utc_stamp``, the check import applies to them (15.2,
    15.6)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn, site_id, _feed_x = _build_base(tmp_path)
        fractional_now_utc = NOW + timedelta(microseconds=250000)

        await publish(
            db,
            writer,
            site_id=site_id,
            cutoff=CUTOFF,
            attempt=1,
            now_utc=fractional_now_utc,
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None
        whole_second_stamp = isoformat_utc(fractional_now_utc.replace(microsecond=0))

        build_started_at_canonical = is_canonical_utc_stamp(pub["build_started_at"])
        assert build_started_at_canonical, (
            "M144 -> at this assertion: correct = True, mutant"
            " (build_started_at stored as isoformat_utc(t), microseconds"
            f" kept) = {build_started_at_canonical!r}"
        )
        assert pub["build_started_at"] == whole_second_stamp, (
            "M144 -> at this assertion: correct ="
            f" {whole_second_stamp!r} (15.2's"
            " isoformat_utc(t.replace(microsecond=0))), mutant ="
            f" {pub['build_started_at']!r}"
        )

        published_at_canonical = is_canonical_utc_stamp(pub["published_at"])
        assert published_at_canonical, (
            "M144 -> at this assertion: correct = True, mutant"
            " (published_at stored as isoformat_utc(t), microseconds kept)"
            f" = {published_at_canonical!r}"
        )
        assert pub["published_at"] == whole_second_stamp, (
            "M144 -> at this assertion: correct ="
            f" {whole_second_stamp!r} (15.2's"
            " isoformat_utc(t.replace(microsecond=0))), mutant ="
            f" {pub['published_at']!r}"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB17
# ---------------------------------------------------------------------------


def _close_through_with_missed(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    first_date,
    last_closed_date,
    missed_date,
) -> None:
    """Like ``_close_through``, but ``missed_date`` (between ``first_date``
    and ``last_closed_date`` inclusive) becomes ``missed`` via P1's own
    retry-exhaustion path (``begin_close`` then ``fail_close``, twice) in
    place of ``closed``; every other date in range still closes normally.
    Dates are processed in order, as P1 requires (``_has_earlier_undone``,
    ``db/site_days.py:757``): a ``missed`` date counts as done, so the date
    after it still closes.
    """
    from wxverify.db.site_days import fail_close, finish_close

    start_dt = datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC)
    start_site_days(conn, site_id, first_date, now_utc=start_dt)
    far_future = note_day_clock(
        datetime(
            last_closed_date.year,
            last_closed_date.month,
            last_closed_date.day,
            tzinfo=UTC,
        )
        + timedelta(days=3)
    )
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d <= last_closed_date:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        if d == missed_date:
            begin_close(conn, site_id, d, now_utc=t)
            fail_close(conn, site_id, d, now_utc=t, reason="pb17 synthetic retry 1")
            t2 = t + timedelta(hours=1, minutes=1)
            begin_close(conn, site_id, d, now_utc=t2)
            fail_close(conn, site_id, d, now_utc=t2, reason="pb17 synthetic retry 2")
        else:
            begin_close(conn, site_id, d, now_utc=t)
            finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)


def test_pb17_a_missed_dates_leftover_day_scores_rows_are_excluded(
    tmp_path: Path,
) -> None:
    """PB17: D-2's record is ``missed`` (P1's ``begin_close`` then
    ``fail_close``, twice), its ``day_scores`` rows left in place, scored
    with sums as on D-3 and D-1. X's ``w:5``/``w:all`` sums and w:5
    coverage leave D-2 out, as PB2 does by deleting its rows outright (7.2);
    here the rows stay and only the record's ``closed`` state excludes
    them, proving ``_DAY_SUMS_SQL`` joins on it (M150). Y's wind row on the
    missed D-2 would be its 20th qualifying day (``MIN_TRAINING_DAYS``) if
    counted; excluded, Y stays at 19 and gets no weight, proving
    ``_CLOSED_WIND_SQL`` joins on it too (M151)."""

    async def _run() -> None:
        from wxverify.scoring.publication import publish

        db, writer, conn = _make_db(tmp_path)
        site_id = day_close_site(conn, close_on=True, with_records=False)
        missed_date = D - timedelta(days=2)
        _close_through_with_missed(
            conn,
            site_id,
            first_date=FIRST_RECORD_DATE,
            last_closed_date=CUTOFF,
            missed_date=missed_date,
        )
        feed_x = _make_feed(conn, "X")
        feed_y = _make_feed(conn, "Y")
        set_setting(conn, "rolling_window_days", "5")
        set_setting(conn, "min_n", "1")

        dates = [D - timedelta(days=k) for k in range(8, 0, -1)]
        closed = {D - timedelta(days=k) for k in range(1, 4)}  # D-3, D-2, D-1
        _seed_uniform_x_scores(conn, site_id, feed_x, dates, closed_dates=closed)

        # Y's wind: 17 starting dates D-20..D-4, plus the closed D-3 and
        # D-1 -- 19 qualifying days. The same row on the missed D-2 would
        # be the 20th, reaching MIN_TRAINING_DAYS, only if the closed
        # record filter is dropped (M151).
        starting_wind_dates = [D - timedelta(days=k) for k in range(20, 3, -1)]
        assert len(starting_wind_dates) == 17
        for sd in starting_wind_dates:
            _seed_starting_wind_day(
                conn,
                site_id=site_id,
                local_date=sd,
                feed_id=feed_y,
                day_ahead=1,
                forecast_high=11.0,
                observed_high=10.0,
            )
        for cd in (D - timedelta(days=3), D - timedelta(days=1), missed_date):
            _seed_day_score(
                conn,
                site_id=site_id,
                local_date=cd,
                feed_id=feed_y,
                variable="wind",
                day_ahead=1,
                status="scored",
                n=24,
                err_n=24,
                wind_fc_high=11.0,
                wind_fc_hours=24,
                wind_obs_high=10.0,
            )
        conn.commit()

        await publish(
            db, writer, site_id=site_id, cutoff=CUTOFF, attempt=1, now_utc=NOW
        )

        pub = _fetch_publication(conn, site_id, CUTOFF)
        assert pub is not None

        rank5 = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert rank5 is not None
        assert rank5["n"] == 8, (
            "M150 -> at this assertion: correct = 8 (the starting D-5,D-4"
            " (2*2) plus the closed D-3,D-1 (2*2); D-2's record is missed,"
            " so its leftover day_scores row is left out, 7.2), mutant M150"
            " (_DAY_SUMS_SQL reads day_scores without the record's closed"
            " state) = 10 (D-2's leftover row, n=2, counted in)"
        )

        rank_all = _fetch_ranking(conn, pub["id"], feed_x, "temperature", 0, "w:all")
        assert rank_all is not None
        assert rank_all["n"] == 14, (
            "the starting D-8..D-4 (5*2=10) plus the closed D-3,D-1 (2*2=4);"
            " D-2 excluded as above"
        )

        coverage5 = _fetch_coverage(conn, pub["id"], feed_x, "temperature", 0, "w:5")
        assert coverage5 is not None
        assert (
            coverage5["scored"],
            coverage5["expected"],
            coverage5["starting_days"],
        ) == (2, 2, 2), (
            "scored/expected count only D-3 and D-1 (D-2's record is not"
            " closed); starting_days counts D-5 and D-4"
        )

        weight_y = _fetch_wind_weight(conn, pub["id"], feed_y, 1)
        assert weight_y is None, (
            "M151 -> at this assertion: correct = no weight row for Y (19"
            " qualifying days -- the 17 starting dates plus the closed D-3"
            " and D-1; D-2's record is missed, so its day_scores wind row"
            " is excluded, below MIN_TRAINING_DAYS 20), mutant M151"
            " (_CLOSED_WIND_SQL takes day_scores' wind columns without the"
            " record's closed state) = a weight row for Y too (D-2's row"
            " counted as the 20th qualifying day, reaching"
            " MIN_TRAINING_DAYS)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB18
# ---------------------------------------------------------------------------


def _plan_ops_for_alias(details: list[str], alias: str) -> list[str]:
    """The leading ``SCAN``/``SEARCH`` op of every plan line naming
    ``alias`` as its table reference (the plan detail's second word), or
    ``[]`` for a line with no such reference (e.g. ``USE TEMP B-TREE``)."""
    ops = []
    for d in details:
        parts = d.split()
        if len(parts) >= 2 and parts[1] == alias:
            ops.append(parts[0])
    return ops


def test_pb18_seed_unsettled_plan_searches_station_wind_days_by_station(
    tmp_path: Path,
) -> None:
    """PB18: ``_SEED_UNSETTLED_SQL``'s own query plan, pinned directly
    (data-layer fix), run exactly as the add-on runs it -- with bound
    parameters, never with the values written into the SQL text. Pins
    that ``station_wind_days`` (alias ``d``) is SOUGHT per station via
    a ``station_id=`` SEARCH and never walked in full, since a SCAN of
    that table grows with stations * days and nothing else prunes it;
    a scan of the small ``stations`` table is allowed (one row per
    enabled station). On this SQLite build, with no statistics
    gathered, the older inner-join form (``FROM station_wind_days d
    JOIN stations st ON st.id = d.station_id``) plans identically to
    the current CROSS JOIN form when run with bound parameters, so
    this test does not distinguish the two; the CROSS JOIN exists to
    keep the loop order fixed regardless of whatever statistics the
    planner later gathers. Matched on alias plus SCAN/SEARCH and the
    bound column name only, never the whole plan line (the shipping
    SQLite build differs from the dev one)."""
    from wxverify.scoring.publication import _SEED_UNSETTLED_SQL

    _db, _writer, conn = _make_db(tmp_path)
    site_id = asof_make_site(conn, "pb18-site")
    bound_date = (D - timedelta(days=3)).isoformat()

    plan_rows = conn.execute(
        f"EXPLAIN QUERY PLAN {_SEED_UNSETTLED_SQL}", (site_id, bound_date)
    ).fetchall()
    details = [str(row["detail"]) for row in plan_rows]

    d_ops = _plan_ops_for_alias(details, "d")
    assert "SCAN" not in d_ops, (
        "correct = no SCAN of station_wind_days (alias d; stations drives"
        " the loop and station_wind_days is sought per station), a broken"
        " form = SCAN d (e.g. the join's station_id condition dropped, or"
        " a planner choice under statistics that puts station_wind_days"
        f" outer, so the whole table is walked), got plan lines {details!r}"
    )

    search_d_lines = [d for d in details if d.split()[:2] == ["SEARCH", "d"]]
    assert search_d_lines and any("station_id=" in d for d in search_d_lines), (
        "correct = a SEARCH d line binding station_id= (station_wind_days"
        " sought per station, not walked), a broken form = no such SEARCH d"
        " line (station_wind_days walked instead, e.g. with the join's"
        " station_id condition dropped or under a planner choice that puts"
        f" station_wind_days outer), got plan lines {details!r}"
    )
