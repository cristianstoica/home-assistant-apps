"""Readers of a generation after the switch (plan ``2026-10-22-daily-close-
switch-over.md`` §10.2, §17.6, TZ1-TZ7).

TZ1 and TZ3 seed site 1's four-generation shape (G0 retired, G1 published
with ``effective_to`` set -- a closed prospective change, G2 published and
the pointer, G3 building) and call every reader named in the row's group
(plan §17.6): TZ1 adds the mean's existing published rows (G6) to pairing
candidates (G1); TZ3 adds the as-of exclusions (G11) to wind training
pairs (G10). TZ2 and TZ4 are both retired: TZ2's live readers (metrics
aggregation, the leaderboard (G8), the composite (G9) and the win rate
(G7)) are gone with the daily close being permanent -- the live "Nd"
window reads per-day sums from day_scores through the active publication
(scoring/publication.py's live_cells -> live_ranking) instead -- and its
retention half (``forecast_pairs`` pruned by generation,
``keep_read_generations``) is gone too, both the table and the function
deleted (storage release). TZ4 (the timeseries stored-pairs reader, G12)
is retired the same way -- its one reader, ``_stored_pairs``, is gone.
``wxverify.db.switch`` is imported at module level above. TZ5 (the gap
scan's origin resolver and the 07:00 record readers) was removed with the
daily forecast record (card 29). TZ7 imports new ``wxverify.db.switch``
names. TZ6 needs no such import: it calls the
EXISTING function ``write_close_truth`` after directly deleting the site's
``tz_generation_published:`` pointer key -- exactly what A4 does (plan
§8.4) -- which breaks it today, since it does not yet resolve the
generation any other way.

Each TZ row's mutant (M35 through M38) is the same root defect -- A1
leaving another generation's rows in place, or a reader resolving the
wrong generation -- for every reader the row names. TZ6 still exercises
only its one named function, since the plan lists exactly one reader per
row there.

Synthetic data only: stations/feeds are bare synthetic rows; dates are in
2029-2030.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import delete_runtime_state
from wxverify.db.site_days import begin_close, day_record
from wxverify.db.switch import site_generation_id
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.truth import evaluate_day_window, write_close_truth

# ---------------------------------------------------------------------------
# Shared fixture helpers.
# ---------------------------------------------------------------------------


def _pointer_key(site_id: int) -> str:
    """The retired time zone pointer's text (``db/switch.py:35``'s
    ``_POINTER_KEY_PREFIX``, private), local since the module that used to
    build it is gone. These tests seed/delete the PRE-switch pointer Part A
    reads and A4 deletes.
    """
    return f"tz_generation_published:{site_id}"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str = "tz-site") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# TZ2 and TZ4 -- both retired. TZ2's live readers (metrics aggregation, the
# leaderboard, the composite, the win rate) are gone with the daily close
# being permanent (storage release); its retention half (``forecast_pairs``
# pruned by generation, ``keep_read_generations``) is gone too -- both the
# table and the function are deleted (storage release 4.1/table 1.B). TZ4's
# one reader, the timeseries stored-pairs reader (G12, ``_stored_pairs``),
# is gone the same way. Nothing from either row survives to test.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TZ6 -- stage 4's truth write resolves the site's one kept generation even
# after its pointer is gone (A4 deletes it); a second write replaces the
# first's rows under the same generation. M37.
# ---------------------------------------------------------------------------


def test_tz6_write_close_truth_resolves_without_the_pointer() -> None:
    from tests.helpers import NOW, D, day_close_site

    conn = _conn()
    site_id = day_close_site(conn, timezone="UTC", with_records=True)
    generation_id = site_generation_id(conn, site_id)
    assert generation_id is not None

    # A non-published generation with a HIGHER id than the site's published
    # one -- M37's query (no ``state = 'published'`` filter, highest id
    # wins) would resolve to THIS row instead, so write_close_truth's rows
    # would land under the wrong generation. Without this second row the
    # site has only its one published generation, so M37's unfiltered
    # "highest id" query happens to agree with the correct "published"
    # query and the mutant survives.
    conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (?, 'UTC', 'initial', 'building')",
        (site_id,),
    )

    closing_date = D - timedelta(days=1)
    begin_close(conn, site_id, closing_date, now_utc=NOW)
    record = day_record(conn, site_id, closing_date)
    assert record is not None and record.state == "closing"

    # A4 deletes the site's pointer unconditionally (plan §8.4) -- simulated
    # directly since wxverify.db.switch does not exist yet; the GENERATION
    # row itself (kept whole by A1) is untouched.
    delete_runtime_state(conn, _pointer_key(site_id))

    bounds = local_day_bounds(closing_date, "UTC")
    evaluation = evaluate_day_window(
        conn, site_id=site_id, local_date=closing_date, timezone="UTC", bounds=bounds
    )

    try:
        write_close_truth(
            conn,
            evaluation,
            site_id=site_id,
            admission_basis="complete",
            now_utc=NOW,
        )
    except ValueError as exc:
        pytest.fail(
            "M37 -> at this assertion: correct = write_close_truth succeeds, "
            f"resolving the site's one kept generation ({generation_id}) "
            "without reading the (now-deleted) pointer; mutant/today's code "
            f"= raises {exc!r} because site_generation_id returns None"
        )

    rows = conn.execute(
        "SELECT tz_generation_id FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, closing_date.isoformat()),
    ).fetchall()
    assert rows, "write_close_truth must have written at least one row"
    tz_ids = {r["tz_generation_id"] for r in rows}
    assert tz_ids == {generation_id}, (
        "M37 -> at this assertion: correct = "
        f"{{{generation_id!r}}} (the write lands under the site's published "
        f"generation), mutant = {tz_ids!r} (lands under the highest-id "
        "generation regardless of state)"
    )

    # A second write replaces the first's rows under the same generation.
    write_close_truth(
        conn, evaluation, site_id=site_id, admission_basis="complete", now_utc=NOW
    )
    rows_after = conn.execute(
        "SELECT tz_generation_id FROM daily_truth WHERE site_id=? AND local_date=?",
        (site_id, closing_date.isoformat()),
    ).fetchall()
    assert len(rows_after) == len(rows)
    tz_ids_after = {r["tz_generation_id"] for r in rows_after}
    assert tz_ids_after == {generation_id}, (
        "M37 -> at this assertion: correct = "
        f"{{{generation_id!r}}} (the replaced rows stay under the site's "
        f"published generation), mutant = {tz_ids_after!r} (lands under the "
        "highest-id generation regardless of state)"
    )


# ---------------------------------------------------------------------------
# TZ7 -- create_site makes exactly one generation, found the same way by
# both of db.switch's new accessors; delete_site removes it with the site.
# M38.
# ---------------------------------------------------------------------------


def test_tz7_create_site_makes_one_generation_delete_site_removes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    async def _idle_worker(_db: object) -> None:
        await asyncio.Event().wait()

    def _init_tmp_db(path: Path) -> sqlite3.Connection:
        close_db()
        db_path = path / "wxverify.db"
        config.db_path = str(db_path)
        options_path = path / "options.json"
        options_path.write_text("{}", encoding="utf-8")
        config.options_path = str(options_path)
        db = init_db(str(db_path))
        return db._conn  # noqa: SLF001

    _init_tmp_db(tmp_path)

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        conn = get_db()._conn  # noqa: SLF001
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        resp = client.post(
            "/api/sites",
            headers=headers,
            json={
                "name": "tz7-site",
                "forecast_lat": 10.0,
                "forecast_lon": 20.0,
                "elevation_m": 100.0,
                "timezone": "UTC",
                "rain_threshold_mm": 0.5,
            },
        )
        assert resp.status_code == 200
        site_id = int(resp.json()["id"])

        rows_before = conn.execute(
            "SELECT id, state FROM timezone_generations WHERE site_id=?",
            (site_id,),
        ).fetchall()
        assert len(rows_before) == 1, (
            "M38 -> at this assertion: correct = 1 generation row for the "
            f"new site, mutant = {len(rows_before)!r} (create_site made none)"
        )
        assert rows_before[0]["state"] == "published"

        from wxverify.db.switch import ensure_site_generation, site_generation_id

        read_id = site_generation_id(conn, site_id)
        ensured_id = ensure_site_generation(conn, site_id)
        assert read_id == ensured_id == int(rows_before[0]["id"]), (
            "M38 -> at this assertion: correct = both db.switch accessors "
            f"return {rows_before[0]['id']!r}, mutant/actual = "
            f"read={read_id!r} ensured={ensured_id!r}"
        )

        resp2 = client.delete(f"/api/sites/{site_id}", headers=headers)
        assert resp2.status_code == 200

        # The TestClient lifespan closes the Database on exit (init_db
        # re-opens it fresh), so this read must happen while the block is
        # still open -- the same connection taken above is dead once it
        # exits.
        rows_after = conn.execute(
            "SELECT id FROM timezone_generations WHERE site_id=?", (site_id,)
        ).fetchall()
        assert rows_after == [], (
            "the delete must remove the site's generation along with the "
            f"site (cascade); still present = {rows_after!r}"
        )
