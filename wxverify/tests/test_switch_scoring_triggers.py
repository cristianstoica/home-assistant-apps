"""Old scoring triggers after the switch (daily-close switch-over plan §7.2
row R17; test ID SC1 of §17.13).

With the key set, P4's own switches (``enqueue_scoring_run``,
``enqueue_score_rescore``, ``pair_and_score``'s entry check -- all already
built, since code at this commit already carries P1 to P5) already make
every one of R17's triggers a no-op TODAY, before P6 removes anything or
rewrites ``scheduler_tick``. Every behavioural case below is therefore a
regression pin (so a future revert of P4's switches is still caught), apart
from the tick case: ``scheduler_tick``'s close-on branch
(``worker/scheduler.py:95-96``) already calls ``enqueue_due_close_work``
only, never the old six-hour score backstop, which is exactly what M70
(``scheduler_tick`` keeps the backstop's body under another name, called
after ``enqueue_due_close_work``) would reintroduce. That is the one
assertion below tagged with an "M70 ->" divergence claim.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    MovingClock,
    asof_conn,
    asof_make_real_feed,
    asof_make_site,
    day_close_site,
    patch_close_clock,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.runtime_state import delete_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.db.switch import ensure_site_generation
from wxverify.provider_ops import rebuild_mean_for_site, set_site_subscription
from wxverify.scoring.rescore import drain_pending_rescores, schedule_score_rescore
from wxverify.settings.service import set_rolling_window_days_sync
from wxverify.worker import domain_backoff
from wxverify.worker.scheduler import scheduler_tick


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _csrf_headers(client: TestClient) -> dict[str, str]:
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": csrf}


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _pending_pair_and_score_count(conn: sqlite3.Connection, site_id: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score' AND site_id = ?"
        " AND status IN ('pending', 'running')",
        (site_id,),
    ).fetchone()[0]


def _seed_forecast_pair(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, generation_id: int
) -> None:
    """One ``forecast_pairs`` row under the site's published generation, so
    ``update_feed``'s affected-site discovery (``api/routes/feeds.py``'s
    ``_affected_sites_for_feed_change``, a join on ``forecast_pairs``) finds
    this site."""
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, day_ahead, forecast, observed, tz_generation_id)"
        " VALUES (?, ?, 'temperature', '2024-06-08T00:00:00Z',"
        " '2024-06-09T00:00:00Z', 24, 1, 10.0, 11.0, ?)",
        (site_id, feed_id, generation_id),
    )
    conn.commit()


def test_sc1_every_old_scoring_trigger_queues_and_runs_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = day_close_site(conn, close_on=True)
    feed_id = asof_make_real_feed(conn, "sc1-feed")
    other_feed_id = asof_make_real_feed(conn, "sc1-other-feed")
    conn.commit()
    caplog.set_level(logging.DEBUG)

    def pending_count() -> int:
        return _pending_pair_and_score_count(conn, site_id)

    # Trigger: the rolling-window setter (R17, settings/service.py:47).
    set_rolling_window_days_sync(conn, 45)
    conn.commit()
    assert pending_count() == 0, (
        "the rolling-window setter must enqueue nothing with the close on"
        " (regression pin; already true today via P4's enqueue_scoring_run"
        " switch -- not this row's own kill)"
    )

    # Trigger: a dashboard view that calls schedule_score_rescore, with the
    # close key present (day_close_site's own precondition).
    async def _drive() -> None:
        schedule_score_rescore(site_id)
        await drain_pending_rescores()

    asyncio.run(_drive())
    assert pending_count() == 0, (
        "schedule_score_rescore must enqueue nothing with the close on"
        " (regression pin; enqueue_score_rescore returns first, already"
        " true today)"
    )

    # Paired positive, injected: with the close key explicitly removed, the
    # same schedule_score_rescore call must still queue a rescore -- proving
    # the close-on suppression just checked is a real switch, not an
    # ambient no-op (e.g. an always-empty jobs table).
    delete_runtime_state(conn, DAILY_CLOSE_ON_KEY)
    conn.commit()
    asyncio.run(_drive())
    assert pending_count() == 1, (
        "correct = 1 (the close key is absent, so schedule_score_rescore's"
        " enqueue_score_rescore call queues a rescore for the enabled site),"
        f" control/positive failure = {pending_count()}"
    )
    conn.execute(
        "DELETE FROM jobs WHERE type = 'pair_and_score' AND site_id = ?", (site_id,)
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    conn.commit()
    assert pending_count() == 0

    # Trigger: update_site's route changing the rain threshold
    # (api/routes/sites.py:186's inline pair_and_score call).
    app = _make_app(monkeypatch)
    caplog.clear()
    with TestClient(app) as client:
        # The lifespan startup above closed the connection ``conn`` pointed
        # at (``init_db`` replaces and the old ``Database`` is shut down);
        # every write/read below must go through the live one instead, the
        # way tests/test_close_pages.py:1282 does.
        conn = get_db()._conn  # noqa: SLF001
        response = client.put(
            f"/api/sites/{site_id}",
            json={"rain_threshold_mm": 0.6},
            headers=_csrf_headers(client),
        )
        assert response.status_code == 200
        assert pending_count() == 0
        assert any(
            f"pair_and_score skipped site={site_id}: daily close on" in record.message
            for record in caplog.records
        ), "update_site's inline pair_and_score call must log the close-on skip"
        assert not any(
            "scoring run complete" in record.message for record in caplog.records
        ), "update_site's inline pair_and_score call must not run a score"

        # Trigger: a subscription change through set_site_subscription, which
        # runs rebuild_mean_for_site.
        caplog.clear()
        set_site_subscription(conn, site_id, feed_id, enabled=True)
        conn.commit()
        assert pending_count() == 0
        assert any(
            f"pair_and_score skipped site={site_id}: daily close on" in record.message
            for record in caplog.records
        ), (
            "set_site_subscription's rebuild_mean_for_site call must log the"
            " close-on skip"
        )
        assert not any(
            "scoring run complete" in record.message for record in caplog.records
        ), "set_site_subscription's rebuild_mean_for_site call must not run a score"

        # Trigger: rebuild_mean_for_site called directly (the CLI `score` route
        # and provider_ops's own smoke path reach it the same way).
        caplog.clear()
        rebuild_mean_for_site(conn, site_id)
        conn.commit()
        assert pending_count() == 0, (
            "rebuild_mean_for_site must enqueue nothing with the close on"
            " (regression pin; true today for the same reason)"
        )
        assert any(
            f"pair_and_score skipped site={site_id}: daily close on" in record.message
            for record in caplog.records
        )
        assert not any(
            "scoring run complete" in record.message for record in caplog.records
        )

        # Trigger: update_feed's route (an `enabled` change) -- also runs
        # rebuild_mean_for_site for the affected site, through a fresh feed
        # with no prior subscription override.
        generation_id = ensure_site_generation(conn, site_id)
        _seed_forecast_pair(
            conn, site_id=site_id, feed_id=other_feed_id, generation_id=generation_id
        )
        caplog.clear()
        response = client.put(
            f"/api/feeds/{other_feed_id}",
            json={"enabled": False},
            headers=_csrf_headers(client),
        )
        assert response.status_code == 200
        assert pending_count() == 0
        assert any(
            f"pair_and_score skipped site={site_id}: daily close on" in record.message
            for record in caplog.records
        ), "update_feed's rebuild_mean_for_site call must log the close-on skip"
        assert not any(
            "scoring run complete" in record.message for record in caplog.records
        )

        # Trigger: a feed fetch storing a sample that pairs now. The fetch
        # score gate is deleted (rain release, Part B, B1): the fetch no
        # longer reaches enqueue_scoring_run at all, and queues no job.
        from wxverify.feeds.seam import FetchResult, NormalizedSample
        from wxverify.worker.feed_fetch import (
            FeedFetchTarget,
            _persist_fetch_success,
        )

        model_row = conn.execute(
            "SELECT model FROM feeds WHERE id = ?", (feed_id,)
        ).fetchone()
        assert model_row is not None
        model = str(model_row[0])
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value,"
            " n_stations) VALUES (?, 'temperature', '2024-06-09T00:30:00Z',"
            " 9.0, 1)",
            (site_id,),
        )
        conn.commit()
        target = FeedFetchTarget(
            site_id=site_id,
            feed_id=feed_id,
            lat=0.0,
            lon=0.0,
            source="example-src",
            model=model,
            max_lead_hours=48,
        )
        sample = NormalizedSample(
            model=model,
            variable="temperature",
            issued_at="2024-06-08T00:00:00Z",
            valid_at="2024-06-09T00:30:00Z",
            lead_hours=1,
            value=10.0,
            source_raw="{}",
            model_run_id="run-1",
        )
        result = FetchResult(samples=[sample], grid=None)

        monkeypatch.setitem(
            domain_backoff.SOURCE_DOMAINS, "example-src", "example.invalid"
        )

        caplog.clear()
        conn.commit()
        conn.execute("BEGIN")
        _persist_fetch_success(conn, target, result)
        conn.commit()

        assert pending_count() == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM forecast_samples WHERE feed_id = ?", (feed_id,)
            ).fetchone()[0]
            == 1
        ), "the fetch must have stored its sample"
        assert not any(
            "fetch score gate" in record.message for record in caplog.records
        ), "the fetch score gate is deleted"
        assert not any(
            "scoring enqueue skipped" in record.message for record in caplog.records
        ), "the fetch path must not reach enqueue_scoring_run"

    # Trigger: scheduler_tick's old six-hour score backstop, built as DO2
    # does (tests/test_close_dormant.py:178-193), on its own connection and
    # site -- this is M70's own divergence point.
    tick_conn = asof_conn()
    tick_site_id = day_close_site(tick_conn, close_on=True, with_records=False)
    t_tick = datetime(D.year, D.month, D.day, 5, 0, tzinfo=UTC)
    stale_score_at = isoformat_utc_micro(t_tick - timedelta(hours=6, minutes=1))
    tick_conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at,"
        " created_at) VALUES ('pair_and_score', ?, 'score', '{}', 'completed', ?, ?)",
        (tick_site_id, stale_score_at, stale_score_at),
    )
    tick_conn.commit()

    clock = MovingClock(t_tick)
    with pytest.MonkeyPatch.context() as mp:
        patch_close_clock(mp, clock)
        scheduler_tick(tick_conn)

    tick_pending = tick_conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score' AND site_id = ?"
        " AND status = 'pending'",
        (tick_site_id,),
    ).fetchone()[0]
    assert tick_pending == 0, (
        "M70 -> at this assertion: correct = 0 (scheduler_tick's close-on"
        " branch calls enqueue_due_close_work only, never the old six-hour"
        " score backstop), mutant M70 (_enqueue_due_score_refresh's body"
        " kept under another name and called after enqueue_due_close_work)"
        f" = {tick_pending} (the stale completed score job is re-queued)"
    )


def test_sc1_positive_the_gate_still_runs_with_the_close_off(
    tmp_path: Path,
) -> None:
    """Paired positive for the suppression above: with the close OFF, the
    same rolling-window-change trigger still queues a rescore -- proving the
    close-on suppression checked above is a real switch, not an ambient
    no-op (e.g. an empty jobs table that would pass regardless)."""
    conn = _init_tmp_db(tmp_path)
    site_id = asof_make_site(conn, "sc1-positive-site")
    asof_make_real_feed(conn, "sc1-positive-feed")
    conn.commit()

    set_rolling_window_days_sync(conn, 45)
    conn.commit()

    count = _pending_pair_and_score_count(conn, site_id)
    assert count == 1, (
        f"correct = 1 (the close is off, so the setter's change queues a"
        f" rescore for the enabled site), mutant/control failure = {count}"
    )
