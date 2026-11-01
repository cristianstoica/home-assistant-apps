"""Write-through-read guard for the composite close-off read path.

Covers ``composite_with_status``/``load_dashboard`` in
``wxverify/scoring/composite.py`` and ``wxverify/web/context.py``: the
write-through-read guard proving neither function attempts a write while
resolving a stale composite snapshot.

Isolation: builds its own tmp DB via ``_init_tmp_db`` (scoring-level tests),
mirroring ``tests/test_web_ui.py``'s harness -- including its real empty
``options.json`` (not a missing path), which routes runtime options through
the file loader instead of falling back to ambient ``WXV_*`` env vars that
would otherwise clobber a test's DB-seeded settings on startup.

Synthetic fixtures only -- fake site names and the repo's existing 40/-105
lat-lon convention (already used throughout this suite), no real keys or
station IDs.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.db.switch import ensure_site_generation
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.composite import composite_with_status
from wxverify.scoring.metrics import MetricResult
from wxverify.settings.keys import set_setting
from wxverify.web.context import load_dashboard

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py).
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    # Real (empty-object) options file, not a missing path -- see module
    # docstring; a missing path falls back to ambient WXV_* env vars and
    # clobbers DB-seeded settings on the next lifespan startup.
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _open_meteo_feed_ids(conn: sqlite3.Connection, count: int) -> list[int]:
    return [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT ?",
            (count,),
        )
    ]


def _add_temperature_cell(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, valid_at: str
) -> None:
    """Minimal single-row cell: enough to populate ``_expected_active_cells``.

    No matching persistence pair, so live aggregation of this exact cell would
    yield ``skill_score=None`` -- fine for this test, which never inspects the
    numeric skill.
    """
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 24, 1,
                11.0, 10.0, 1.0, 1.0, 1.0, ?)
        """,
        (site_id, feed_id, valid_at, ensure_site_generation(conn, site_id)),
    )


# ---------------------------------------------------------------------------
# Write-through-read guard: composite_with_status/load_dashboard never write
# on the read path.
# ---------------------------------------------------------------------------


def test_composite_read_functions_never_write_on_read_connection(
    tmp_path: Path,
) -> None:
    """Both ``composite_with_status`` (called directly) and ``load_dashboard``
    (which calls it internally, per ``context.py:294``'s own "Pure read ...
    never write here" contract) must never attempt a write while resolving a
    stale composite snapshot.

    Real enforcement, not a mock: after seeding a full-but-STALE snapshot on
    a genuine on-disk tmp DB, ``PRAGMA query_only=ON`` is set on that same
    connection -- SQLite itself then raises ``sqlite3.OperationalError`` on
    any attempted INSERT/UPDATE/DELETE. Both functions returning normally
    (and still reporting ``stale``, proving they did real work rather than
    short-circuiting) is the only way this test can pass if either one holds
    a hidden write.

    Close-off only: with the daily close on, ``composite_with_status`` takes
    the ``_published_composite`` branch (composite.py:117-118), which has no
    ``stale`` status at all (only ``empty``/``live``/``hit``) -- switching
    this fixture to close-on would need a different fixture and different
    assertions, not a small change, so it stays on the close-off path it was
    written for.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Write Guard")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_key="w:14",
        result=MetricResult(n=1, skill_score=0.4, confident=True),
        # Fixed PAST date -> stale (never w:all/2035 always-fresh), and stale
        # is the branch most likely to tempt an opportunistic write-back.
        computed_at="2020-01-01T00:00:00Z",
    )

    conn.execute("PRAGMA query_only=ON")

    composite_result = composite_with_status(conn, site_id=site_id, window="rolling")
    assert composite_result.status == "stale"

    context = load_dashboard(
        conn, site_id=site_id, variable="temperature", window="rolling", lead="D+1"
    )
    assert context["composite_status"] == "stale"
