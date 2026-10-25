"""Tests for the per-feed subscription toggle (changed/skip-rebuild).

`set_site_subscription` now reports whether its write actually changed the
stored value, and skips the mean rebuild when it did not. The per-site and
global routes, the Sites page fragment, its `app.js` handlers, and the CLI
all share one rebuild entry point (`rebuild_mean_for_site`, which looks
`pair_and_score` up from `wxverify.provider_ops` at call time), so a single
monkeypatched spy there proves the skip for every caller.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
import threading
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from wxverify import config
from wxverify.__main__ import main
from wxverify.api.app import create_app
from wxverify.api.routes.feeds import update_subscription
from wxverify.api.schemas import SubscriptionUpdate
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.provider_ops import SubscriptionResult, set_site_subscription
from wxverify.scoring.engine import pair_and_score as _real_pair_and_score
from wxverify.web.context import feed_label

ROOT = Path(__file__).resolve().parents[1]
APP_JS = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(encoding="utf-8")
BASE_HTML = (ROOT / "wxverify" / "web" / "templates" / "base.html").read_text(
    encoding="utf-8"
)
FEED_TOGGLES_HTML = (
    ROOT / "wxverify" / "web" / "templates" / "sites" / "_feed_toggles.html"
).read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 40, -105, 900, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


def _open_meteo_feed_ids(conn: sqlite3.Connection, count: int) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT ?",
        (count,),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def _add_pair(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, forecast: float
) -> None:
    observed = 10.0
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                '2035-01-02T00:00:00Z', 24, 1, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
            ensure_published_generation(conn, site_id),
        ),
    )


def _mean_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _mean_row_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE site_id=? AND feed_id=?",
        (site_id, _mean_feed_id(conn)),
    ).fetchone()
    return int(row["n"])


def _seed_site_with_two_feeds(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """One synthetic site subscribed to two open-meteo feeds, with a
    published pair for each and a materialized multimodel mean row -- the
    seed `tests/test_m1_m5.py`'s `test_subscription_rebuilds_multimodel_mean`
    uses to prove a rebuild adds and drops the mean row."""
    site_id = _insert_site(conn, "Example Site")
    feed_a, feed_b = _open_meteo_feed_ids(conn, 2)
    _add_pair(conn, site_id=site_id, feed_id=feed_a, forecast=8.0)
    _add_pair(conn, site_id=site_id, feed_id=feed_b, forecast=12.0)
    _real_pair_and_score(conn, site_id)
    return site_id, feed_a, feed_b


def _seed_two_sites_sharing_feed(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Two synthetic sites, each with a published pair for the same
    open-meteo feed, so the global route's affected-sites query finds
    both."""
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    site_a = _insert_site(conn, "Example Site")
    site_b = _insert_site(conn, "Example Site 2")
    _add_pair(conn, site_id=site_a, feed_id=feed_id, forecast=8.0)
    _add_pair(conn, site_id=site_b, feed_id=feed_id, forecast=12.0)
    return feed_id, site_a, site_b


def _install_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Replace the one name `rebuild_mean_for_site` looks up with a
    counting pass-through, so a test sees exactly how many times -- and for
    which site -- a rebuild actually ran."""
    calls: list[int] = []

    def spy(conn: sqlite3.Connection, site_id: int) -> None:
        calls.append(site_id)
        _real_pair_and_score(conn, site_id)

    monkeypatch.setattr("wxverify.provider_ops.pair_and_score", spy)
    return calls


def _csrf_headers(client: TestClient) -> dict[str, str]:
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": csrf}


def _feed_source_model(conn: sqlite3.Connection, feed_id: int) -> tuple[str, str]:
    row = conn.execute(
        "SELECT source, model FROM feeds WHERE id=?", (feed_id,)
    ).fetchone()
    assert row is not None
    return str(row["source"]), str(row["model"])


class _AttrParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, {key: value or "" for key, value in attrs}))


# --------------------------------------------------------------------------
# T1 - set_site_subscription's own skip/change decision
# --------------------------------------------------------------------------


def test_t1_set_subscription_skips_the_rebuild_only_on_a_true_repeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: a skip keyed on the request's `enabled` value alone (so it
    never rebuilds on a real repeat) or that never skips at all (so a true
    repeat still rebuilds); a missing stored row or a NULL (inherited) row
    treated as a match instead of a change."""
    conn = _init_tmp_db(tmp_path)
    site_id, _feed_a, feed_b = _seed_site_with_two_feeds(conn)
    db = get_db()
    calls = _install_spy(monkeypatch)

    def _set(enabled: bool) -> SubscriptionResult:
        return db.write_sync(
            lambda write_conn: set_site_subscription(
                write_conn, site_id, feed_b, enabled=enabled
            )
        )

    disable_1 = _set(False)
    assert disable_1.changed is True
    assert calls == [site_id]
    assert _mean_row_count(conn, site_id) == 0

    disable_2 = _set(False)
    assert disable_2.changed is False
    assert calls == [site_id]
    assert _mean_row_count(conn, site_id) == 0

    enable_1 = _set(True)
    assert enable_1.changed is True
    assert calls == [site_id, site_id]
    assert _mean_row_count(conn, site_id) == 1

    enable_2 = _set(True)
    assert enable_2.changed is False
    assert calls == [site_id, site_id]
    assert _mean_row_count(conn, site_id) == 1

    stored_default_subscribed = conn.execute(
        "SELECT default_subscribed FROM feeds WHERE id=?", (feed_b,)
    ).fetchone()["default_subscribed"]
    assert stored_default_subscribed == 1

    conn.execute(
        "DELETE FROM site_feed_state WHERE site_id=? AND feed_id=?", (site_id, feed_b)
    )
    missing_row = _set(True)
    assert missing_row.changed is True
    assert calls == [site_id, site_id, site_id]
    assert _mean_row_count(conn, site_id) == 1

    conn.execute(
        "UPDATE site_feed_state SET enabled=NULL WHERE site_id=? AND feed_id=?",
        (site_id, feed_b),
    )
    null_row = _set(True)
    assert null_row.changed is True
    assert calls == [site_id, site_id, site_id, site_id]
    assert _mean_row_count(conn, site_id) == 1


# --------------------------------------------------------------------------
# T2 - per-site route reports `changed` and keeps the fetch rule unchanged
# --------------------------------------------------------------------------


def test_t2_route_reports_changed_and_keeps_the_fetch_rule_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: `changed` missing from the JSON response; the skip logic
    wired into the CLI path only, leaving the route always rebuilding; the
    fetch-enqueue rule changed by accident alongside the skip."""
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    calls = _install_spy(monkeypatch)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, _feed_a, feed_b = db.write_sync(_seed_site_with_two_feeds)
        headers = _csrf_headers(client)

        def _put(enabled: bool) -> dict[str, Any]:
            response = client.put(
                f"/api/sites/{site_id}/feeds/{feed_b}",
                json={"enabled": enabled},
                headers=headers,
            )
            assert response.status_code == 200
            return dict(response.json())

        r1 = _put(False)
        r2 = _put(False)
        r3 = _put(True)
        count_after_enable_1 = db.read_sync(
            lambda conn: int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM jobs "
                    "WHERE type='fetch_feed' AND site_id=?",
                    (site_id,),
                ).fetchone()["n"]
            )
        )
        r4 = _put(True)
        count_after_enable_2 = db.read_sync(
            lambda conn: int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM jobs "
                    "WHERE type='fetch_feed' AND site_id=?",
                    (site_id,),
                ).fetchone()["n"]
            )
        )

        assert [r1["changed"], r2["changed"], r3["changed"], r4["changed"]] == [
            True,
            False,
            True,
            False,
        ]
        assert calls == [site_id, site_id]
        assert r3["fetch_enqueued"] is True
        assert r4["fetch_enqueued"] is True
        assert count_after_enable_2 == count_after_enable_1


# --------------------------------------------------------------------------
# T3 - HTML fragment notice reflects `changed` and the feed's label
# --------------------------------------------------------------------------


def test_t3_html_fragment_notice_reflects_changed_and_the_feed_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: a notice that ignores `changed` (always "Saved" or always
    "No change"); the notice dropped from the fragment entirely; the route
    calling `render_feed_toggles` without the keyword arguments its new
    signature requires."""
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    _install_spy(monkeypatch)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_id, _feed_a, feed_b = db.write_sync(_seed_site_with_two_feeds)
        source, model = db.read_sync(lambda conn: _feed_source_model(conn, feed_b))
        label = feed_label(source, model)
        headers = dict(_csrf_headers(client), **{"HX-Request": "true"})

        first = client.put(
            f"/api/sites/{site_id}/feeds/{feed_b}",
            json={"enabled": False},
            headers=headers,
        )
        assert first.status_code == 200
        assert f'id="feed-toggles-{site_id}"' in first.text
        assert f"Saved. {label} is now off for this site." in first.text

        second = client.put(
            f"/api/sites/{site_id}/feeds/{feed_b}",
            json={"enabled": False},
            headers=headers,
        )
        assert second.status_code == 200
        assert f"No change. {label} was already off for this site." in second.text


# --------------------------------------------------------------------------
# T4 - a failed rebuild rolls back the upsert with it
# --------------------------------------------------------------------------


def test_t4_a_failed_rebuild_rolls_back_so_the_retry_still_counts_as_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the upsert committed in a transaction separate from the
    rebuild, or committed before it; a stored value read outside the write
    transaction, which would let the retry see the failed attempt's value
    as already applied even though it never committed."""
    conn = _init_tmp_db(tmp_path)
    site_id, _feed_a, feed_b = _seed_site_with_two_feeds(conn)
    db = get_db()

    def _boom(write_conn: sqlite3.Connection, boom_site_id: int) -> None:
        raise RuntimeError("synthetic rebuild failure")

    monkeypatch.setattr("wxverify.provider_ops.pair_and_score", _boom)
    with pytest.raises(RuntimeError, match="synthetic rebuild failure"):
        db.write_sync(
            lambda write_conn: set_site_subscription(
                write_conn, site_id, feed_b, enabled=False
            )
        )
    stored = conn.execute(
        "SELECT enabled FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_b),
    ).fetchone()
    assert stored is None
    assert _mean_row_count(conn, site_id) == 1

    calls = _install_spy(monkeypatch)
    retried = db.write_sync(
        lambda write_conn: set_site_subscription(
            write_conn, site_id, feed_b, enabled=False
        )
    )
    assert retried.changed is True
    assert calls == [site_id]
    assert _mean_row_count(conn, site_id) == 0

    class _SyntheticAbort(BaseException):
        pass

    def _boom_base(write_conn: sqlite3.Connection, boom_site_id: int) -> None:
        raise _SyntheticAbort

    monkeypatch.setattr("wxverify.provider_ops.pair_and_score", _boom_base)
    with pytest.raises(_SyntheticAbort):
        db.write_sync(
            lambda write_conn: set_site_subscription(
                write_conn, site_id, feed_b, enabled=True
            )
        )
    stored_after_base = conn.execute(
        "SELECT enabled FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_b),
    ).fetchone()
    assert stored_after_base is not None
    assert bool(stored_after_base["enabled"]) is False
    assert _mean_row_count(conn, site_id) == 0


# --------------------------------------------------------------------------
# T5 - a cancelled write still commits so the repeat is a true no-op
# --------------------------------------------------------------------------


def test_t5_a_cancelled_write_still_commits_so_the_repeat_is_a_true_no_op(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: a cancelled write torn down before its commit (so a stored
    value never lands and the retry below is wrongly treated as the first
    real change); a write whose cancellation drops the write lock early,
    letting the retry proceed concurrently instead of serialized behind
    it."""
    conn = _init_tmp_db(tmp_path)
    site_id, _feed_a, feed_b = _seed_site_with_two_feeds(conn)
    db = get_db()
    entered = threading.Event()
    release = threading.Event()
    calls: list[int] = []

    def _blocking_spy(write_conn: sqlite3.Connection, blocked_site_id: int) -> None:
        calls.append(blocked_site_id)
        entered.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("release was never set")
        _real_pair_and_score(write_conn, blocked_site_id)

    monkeypatch.setattr("wxverify.provider_ops.pair_and_score", _blocking_spy)

    async def _run() -> None:
        task = asyncio.create_task(
            db.write(
                lambda write_conn: set_site_subscription(
                    write_conn, site_id, feed_b, enabled=False
                )
            )
        )
        try:
            entered_ok = await asyncio.to_thread(entered.wait, 5.0)
            assert entered_ok
            task.cancel()
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()

    asyncio.run(_run())

    assert calls == [site_id]
    stored = conn.execute(
        "SELECT enabled FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_b),
    ).fetchone()
    assert stored is not None
    assert bool(stored["enabled"]) is False
    assert _mean_row_count(conn, site_id) == 0

    calls_after = _install_spy(monkeypatch)
    repeat = db.write_sync(
        lambda write_conn: set_site_subscription(
            write_conn, site_id, feed_b, enabled=False
        )
    )
    assert repeat.changed is False
    assert calls_after == []


# --------------------------------------------------------------------------
# T6 - the global route's own skip matches the per-site skip, and a failed
# rebuild rolls back the feed row's flag flip with it
# --------------------------------------------------------------------------


def test_t6_global_route_skip_matches_the_per_site_route_and_rolls_back_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the global feed route rebuilding every affected site on a
    no-op PUT (enabled sent as its own current value), unlike the per-site
    route's skip; a failed rebuild committing the feed row's `enabled` flip
    anyway, leaving the column out of step with what was actually
    verified."""
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    calls = _install_spy(monkeypatch)
    app = create_app(root_path="")
    with TestClient(app, raise_server_exceptions=False) as client:
        db = get_db()
        feed_id, site_a, site_b = db.write_sync(_seed_two_sites_sharing_feed)
        headers = _csrf_headers(client)

        def _put(enabled: bool) -> None:
            response = client.put(
                f"/api/feeds/{feed_id}", json={"enabled": enabled}, headers=headers
            )
            assert response.status_code == 200

        _put(False)
        assert sorted(calls) == sorted([site_a, site_b])

        calls.clear()
        _put(False)
        assert calls == []

        calls.clear()
        _put(True)
        assert sorted(calls) == sorted([site_a, site_b])

        calls.clear()
        stored_default_subscribed = db.read_sync(
            lambda conn: bool(
                conn.execute(
                    "SELECT default_subscribed FROM feeds WHERE id=?", (feed_id,)
                ).fetchone()["default_subscribed"]
            )
        )
        same_default = client.put(
            f"/api/feeds/{feed_id}",
            json={"default_subscribed": stored_default_subscribed},
            headers=headers,
        )
        assert same_default.status_code == 200
        assert calls == []

        def _boom(write_conn: sqlite3.Connection, boom_site_id: int) -> None:
            raise RuntimeError("synthetic rebuild failure")

        monkeypatch.setattr("wxverify.provider_ops.pair_and_score", _boom)
        failed = client.put(
            f"/api/feeds/{feed_id}", json={"enabled": False}, headers=headers
        )
        assert failed.status_code == 500
        row = db.read_sync(
            lambda conn: conn.execute(
                "SELECT enabled FROM feeds WHERE id=?", (feed_id,)
            ).fetchone()
        )
        assert bool(row["enabled"]) is True

        calls_after = _install_spy(monkeypatch)
        _put(False)
        assert sorted(calls_after) == sorted([site_a, site_b])


# --------------------------------------------------------------------------
# T7 - a same-value write queued behind the first waits, then no-ops
# --------------------------------------------------------------------------


def test_t7_a_same_value_write_queued_behind_the_first_waits_then_no_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the second write computing its stored-value comparison
    before it queues for the write lock (so it sees the pre-write value and
    rebuilds anyway) instead of after it acquires the lock and sees the
    first write's committed value; and the route (or anything before
    `get_db().write`) deciding `changed` from a read taken outside the
    write lock, which would report the queued repeat as changed. Both
    disables go through `update_subscription` itself, not
    `set_site_subscription` directly, so a check the route body makes
    outside the lock cannot slip past this test."""
    conn = _init_tmp_db(tmp_path)
    site_id, _feed_a, feed_b = _seed_site_with_two_feeds(conn)
    entered = threading.Event()
    release = threading.Event()
    calls: list[int] = []

    def _blocking_spy(write_conn: sqlite3.Connection, blocked_site_id: int) -> None:
        calls.append(blocked_site_id)
        entered.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("release was never set")
        _real_pair_and_score(write_conn, blocked_site_id)

    monkeypatch.setattr("wxverify.provider_ops.pair_and_score", _blocking_spy)

    async def _disable_via_route() -> dict[str, object]:
        scope = {"type": "http", "method": "PUT", "path": "/", "headers": []}
        result = await update_subscription(
            Request(scope), site_id, feed_b, SubscriptionUpdate(enabled=False)
        )
        assert isinstance(result, dict)
        return result

    async def _run() -> tuple[dict[str, object], dict[str, object]]:
        first_task = asyncio.create_task(_disable_via_route())
        try:
            entered_ok = await asyncio.to_thread(entered.wait, 5.0)
            assert entered_ok

            second_task = asyncio.create_task(_disable_via_route())
            await asyncio.sleep(0.05)
            assert not second_task.done()
        finally:
            release.set()
        first = await first_task
        second = await second_task
        return first, second

    first, second = asyncio.run(_run())
    assert first["changed"] is True
    assert second["changed"] is False
    assert calls == [site_id]
    assert _mean_row_count(conn, site_id) == 0


# --------------------------------------------------------------------------
# T8 - the CLI reports `changed` and shares the per-site skip
# --------------------------------------------------------------------------


def test_t8_cli_providers_disable_reports_changed_and_skips_the_repeat_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: the CLI's `changed` key missing, or always true or always
    false; the CLI running its own rebuild instead of going through
    `set_site_subscription`'s shared skip, which would rebuild on the
    repeat the HTTP route would have skipped."""
    conn = _init_tmp_db(tmp_path)
    db_path = config.db_path
    site_id, _feed_a, feed_b = _seed_site_with_two_feeds(conn)
    calls = _install_spy(monkeypatch)

    argv = [
        "--db",
        db_path,
        "providers",
        "disable",
        "--site-id",
        str(site_id),
        "--feed-id",
        str(feed_b),
    ]

    code1 = main(argv)
    assert code1 == 0
    out1 = capsys.readouterr().out
    assert "changed=True" in out1
    assert calls == [site_id]

    code2 = main(argv)
    assert code2 == 0
    out2 = capsys.readouterr().out
    assert "changed=False" in out2
    assert calls == [site_id]
    mean_rows = get_db().read_sync(
        lambda read_conn: _mean_row_count(read_conn, site_id)
    )
    assert mean_rows == 0


# --------------------------------------------------------------------------
# T9 - app.js's subscription-toggle block: the three handlers, and what
# each one does and does not read
# --------------------------------------------------------------------------


_SUB_MARKER = "// Feed subscription toggles:"
_SUB_BLOCK_END_MARKER = '\n  // "Last fetched'


def _sub_block() -> str:
    assert _SUB_MARKER in APP_JS, "app.js lost its subscription toggle block"
    rest = APP_JS.split(_SUB_MARKER, 1)[1]
    assert _SUB_BLOCK_END_MARKER in rest, "could not find the block's own end marker"
    return rest.split(_SUB_BLOCK_END_MARKER, 1)[0]


def _sub_handler(event_name: str) -> str:
    block = _sub_block()
    marker = f'addEventListener("{event_name}", function (event) {{'
    assert marker in block, f"missing the {event_name} subscription handler"
    return block.split(marker, 1)[1].split("\n  });", 1)[0]


def test_t9_app_js_block_has_the_three_handlers_and_outcome_texts() -> None:
    """Catches: the subscription toggle block missing from app.js entirely,
    or missing one of its three listeners or outcome strings, which would
    leave the Sites page silent while a toggle is in flight or after it
    fails."""
    block = _sub_block()
    assert "[data-sub-toggle]" in block
    assert "isConnected" in block
    assert "SUB_BUSY" in block
    assert "SUB_UNKNOWN" in block
    assert "SUB_SESSION" in block
    assert (
        "Applying the change. This can take several minutes; the buttons "
        "stay locked until it finishes."
    ) in block
    assert (
        "No answer yet. The change may still be applying. Wait a few "
        "minutes, then reload the page to see the current state before "
        "clicking again."
    ) in block
    assert "Session expired or request blocked — reload the page to continue" in block
    assert 'addEventListener("htmx:beforeRequest"' in block
    assert 'addEventListener("htmx:beforeSwap"' in block
    assert 'addEventListener("htmx:afterRequest"' in block


def test_t9_before_swap_handler_reads_elt_only_through_requestconfig() -> None:
    """Catches: the beforeSwap handler reading `event.detail.elt` (the swap
    TARGET -- the section, not the button that sent the request) instead of
    `event.detail.requestConfig.elt`, which would misidentify which toggle a
    swap belongs to."""
    handler = _sub_handler("htmx:beforeSwap")
    assert "event.detail.requestConfig" in handler
    assert "event.detail.elt" not in handler
    assert "[data-sub-toggle]" in handler
    assert set(re.findall(r"(\w+)\.elt\b", handler)) == {"requestConfig"}


def test_t9_after_request_handler_leaves_an_unanswered_change_undecided() -> None:
    """Catches: a non-403, non-rejection outcome (no response, a 5xx, a
    swap htmx silently skipped) reported as settled -- changed or
    unchanged -- when the server's actual state cannot be known from the
    client alone."""
    handler = _sub_handler("htmx:afterRequest")
    assert "SUB_UNKNOWN" in handler
    assert "button.disabled = true" in handler
    assert "status === 403" in handler
    assert "400 || status === 404 || status === 422" in handler


# --------------------------------------------------------------------------
# T10 - base.html defers to the toggle's own handler; the fragment
# announces its own outcome
# --------------------------------------------------------------------------


def test_t10_response_error_handler_returns_early_for_subscription_toggles() -> None:
    """Catches: base.html's generic htmx:responseError handler (which shows
    the page-wide "Request rejected"/session-expired notice for every other
    mutation) also firing for a subscription toggle's error response, so its
    notice lands on top of what the toggle's own afterRequest handler
    already wrote into the status line."""
    marker = 'addEventListener("htmx:responseError", function (event) {'
    assert marker in BASE_HTML, "base.html lost its responseError"
    handler = BASE_HTML.split(marker, 1)[1].split("\n      });", 1)[0]
    guard_index = handler.find('elt.matches("[data-sub-toggle]")')
    notice_index = handler.find("mutation-notice")
    assert guard_index != -1
    assert notice_index != -1
    assert guard_index < notice_index


def test_t10_feed_toggles_fragment_has_an_announced_status_line() -> None:
    """Catches: the status line missing `data-sub-status` (app.js's
    `subLine` could never find it to update after a swap), or missing a
    live-region role, which would leave the toggle's outcome unannounced to
    assistive tech."""
    assert "data-sub-status" in FEED_TOGGLES_HTML
    assert 'role="status"' in FEED_TOGGLES_HTML
    assert 'aria-live="polite"' in FEED_TOGGLES_HTML


# --------------------------------------------------------------------------
# T11 - every subscription button's hx-disabled-elt is scoped to its own
# site's section, never narrower or broader
# --------------------------------------------------------------------------


_HX_PUT_SITE_RE = re.compile(r"/api/sites/(\d+)/feeds/\d+")


def test_t11_every_subscription_button_has_a_correctly_scoped_disabled_elt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: `hx-disabled-elt` scoped to `this` (only the clicked button
    locks, letting a second toggle in the same section fire while the first
    is still applying) instead of every button in that site's own
    feed-toggles section; or scoped broadly enough to lock an unrelated
    site's buttons too."""
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_a = db.write_sync(lambda conn: _insert_site(conn, "Example Site"))
        site_b = db.write_sync(lambda conn: _insert_site(conn, "Example Site 2"))
        page = client.get("/sites")
    assert page.status_code == 200
    page_text = page.text

    parser = _AttrParser()
    parser.feed(page_text)
    toggle_buttons = [
        attrs
        for tag, attrs in parser.tags
        if tag == "button" and "data-sub-toggle" in attrs
    ]
    buttons_by_site: dict[int, list[dict[str, str]]] = {site_a: [], site_b: []}
    for attrs in toggle_buttons:
        match = _HX_PUT_SITE_RE.search(attrs.get("hx-put", ""))
        assert match, f"button has no recognizable site in hx-put: {attrs}"
        site_id = int(match.group(1))
        assert site_id in buttons_by_site
        buttons_by_site[site_id].append(attrs)

        target = attrs.get("hx-target")
        assert target == f"#feed-toggles-{site_id}"
        assert attrs.get("hx-disabled-elt") == f"{target} button"
        assert attrs.get("hx-ext") == "json-enc"
        assert "X-CSRF-Token" in attrs.get("hx-headers", "")

    for site_id, buttons in buttons_by_site.items():
        assert buttons, f"site {site_id} has no feed toggle buttons"

    for site_id in (site_a, site_b):
        start = page_text.index(
            f'<section class="subsection" id="feed-toggles-{site_id}"'
        )
        end = page_text.index("</section>", start)
        section_text = page_text[start:end]
        assert section_text.count("data-sub-status") == 1
