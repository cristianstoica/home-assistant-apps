"""Google horizon correction oracles — 0.12.0 §15 family 7 (§4, W1).

Covers ``wxverify.db.migrations.correct_google_horizon``: the one-shot,
marker-gated data correction that raises the ``(source='google',
model='blend')`` feed row from 24 to 168 on an existing database, and the
fresh-install seed path that lands on 168 directly. Every oracle asserts
``PRAGMA user_version`` still equals ``TARGET_USER_VERSION`` — the
correction is a data fix, not a schema migration, and must never move the
version (§4 "Why this does not bump TARGET_USER_VERSION").

Every fixture is built with ``create_schema`` + a hand-set ``PRAGMA
user_version`` (mirrors ``tests/test_publish_hold_bootstrap.py``'s
``_emulated_0_11_0_db``), never through ``init_db``/a real file, because
these oracles only need a bare schema plus a handful of ``feeds`` rows.

Synthetic data only: ``site-beta-src`` / ``site-gamma-src`` feed rows,
obviously-fake lead-hour values (77, 99) standing in for "some other
value the correction must not touch".
"""

from __future__ import annotations

import sqlite3

import pytest

from wxverify.db import migrations
from wxverify.db.migrations import (
    GOOGLE_HORIZON_CORRECTION_KEY,
    TARGET_USER_VERSION,
    correct_google_horizon,
    create_schema,
    run_migrations,
)
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state


def _bare_db(*, user_version: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute(f"PRAGMA user_version = {user_version}")
    return conn


def _insert_feed(
    conn: sqlite3.Connection, *, source: str, model: str, max_lead_hours: int
) -> None:
    conn.execute(
        """
        INSERT INTO feeds (source, model, fetch_interval_minutes, max_lead_hours)
        VALUES (?, ?, 60, ?)
        """,
        (source, model, max_lead_hours),
    )


def _lead(conn: sqlite3.Connection, *, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT max_lead_hours FROM feeds WHERE source = ? AND model = ?",
        (source, model),
    ).fetchone()
    assert row is not None, f"expected a ({source}, {model}) feed row"
    return int(row["max_lead_hours"])


def _user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0])


def _build_v4_matrix_db(
    *, google_blend_lead: int, marker: str | None
) -> sqlite3.Connection:
    """A v4 database holding the three rows §4's fixture spec calls for:
    the target row, a differently-valued other-source row, and a
    google-non-blend row at a value that is neither 24 nor 168."""
    conn = _bare_db(user_version=4)
    _insert_feed(conn, source="google", model="blend", max_lead_hours=google_blend_lead)
    _insert_feed(conn, source="site-beta-src", model="model-x", max_lead_hours=99)
    _insert_feed(conn, source="google", model="current", max_lead_hours=50)
    if marker is not None:
        set_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY, marker)
    return conn


# ---------------------------------------------------------------------------
# §4 matrix — fresh install; existing db at 24; re-run; other-rows-untouched.
# ---------------------------------------------------------------------------


def test_fresh_database_seeds_google_blend_at_168() -> None:
    """Fresh install (no rows, no marker): the SEED lands the row at 168 --
    correct_google_horizon's UPDATE matches nothing on an empty table. This
    is the paired positive for the existing-db-at-24 correction below: it
    fails if only the correction half ships (§4's final "Separately" test).
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    assert _lead(conn, source="google", model="blend") == 168
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) == "applied"
    assert _user_version(conn) == TARGET_USER_VERSION


def test_existing_row_at_24_corrected_and_rerun_is_noop() -> None:
    """The core §4 matrix oracle: existing db at 24, no marker -> corrected,
    marker set, other rows untouched, user_version unmoved; then a SECOND
    run_migrations on the same connection changes nothing and does not
    raise (idempotent re-run)."""
    conn = _build_v4_matrix_db(google_blend_lead=24, marker=None)
    run_migrations(conn)

    assert _lead(conn, source="google", model="blend") == 168
    assert _lead(conn, source="site-beta-src", model="model-x") == 99
    assert _lead(conn, source="google", model="current") == 50
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) == "applied"
    assert _user_version(conn) == TARGET_USER_VERSION

    # Re-run on the SAME connection: nothing raises, nothing moves.
    run_migrations(conn)
    assert _lead(conn, source="google", model="blend") == 168
    assert _lead(conn, source="site-beta-src", model="model-x") == 99
    assert _lead(conn, source="google", model="current") == 50
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) == "applied"
    assert _user_version(conn) == TARGET_USER_VERSION


# ---------------------------------------------------------------------------
# §4's three design oracles.
# ---------------------------------------------------------------------------


def test_one_shot_marker_present_blocks_recorrection_of_a_reset_row() -> None:
    """One-shot: marker already present, row hand-set back to 24 ->
    run_migrations must leave it at 24. This is the assertion an
    unconditional ``UPDATE ... WHERE max_lead_hours = 24`` (no marker gate)
    would FAIL: without the marker check, the row would be corrected right
    back to 168 on this very run. The marker's presence is what makes the
    row's current value the one respected, not the WHERE clause's match.
    """
    conn = _build_v4_matrix_db(google_blend_lead=24, marker="applied")
    run_migrations(conn)
    assert _lead(conn, source="google", model="blend") == 24
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) == "applied"
    assert _user_version(conn) == TARGET_USER_VERSION


def test_crash_window_recovers_via_idempotent_rerun() -> None:
    """Crash window (§4 change item 4): a crash between the UPDATE and the
    marker write leaves the row at 168 with no marker. The guarantee the
    implementation actually makes -- UPDATE-then-marker ordering makes
    every interruption point already correct, not that the two writes are
    transactional together (``executescript`` inside ``create_schema`` has
    already committed by this point, and no SAVEPOINT wraps this function;
    see §4's "no SAVEPOINT is needed") -- is that RESUMING from exactly that
    state is idempotent: calling ``correct_google_horizon`` directly (not
    the full ``run_migrations``, to isolate the ordering property from
    seeding/bootstrap) re-runs an UPDATE that matches nothing and writes
    the marker, changing no row.
    """
    conn = _build_v4_matrix_db(google_blend_lead=168, marker=None)
    correct_google_horizon(conn)
    assert _lead(conn, source="google", model="blend") == 168
    assert _lead(conn, source="site-beta-src", model="model-x") == 99
    assert _lead(conn, source="google", model="current") == 50
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) == "applied"


def test_update_precedes_marker_so_a_marker_write_failure_leaves_the_row_corrected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ordering property the crash-window test above cannot pin (it
    enters at an already-correct state, so it passes under either
    statement order): a crash BETWEEN the UPDATE and the marker write must
    still leave the row corrected, because the UPDATE ran first. Simulated
    by making the marker write itself raise -- if the implementation wrote
    the marker before the UPDATE, the row would still be at 24 here.
    """
    conn = _build_v4_matrix_db(google_blend_lead=24, marker=None)

    def _boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated crash between the UPDATE and the marker")

    monkeypatch.setattr(migrations, "set_runtime_state", _boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        correct_google_horizon(conn)
    # UPDATE ran first: the row is already correct and the receipt is absent,
    # so the next boot re-runs the correction rather than skipping it.
    assert _lead(conn, source="google", model="blend") == 168
    assert get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) is None


def test_no_version_movement_across_run_migrations() -> None:
    """No version movement: TARGET_USER_VERSION == 4 (the literal the
    rollback/export guarantees in §4 are pinned against), and a database
    that starts at user_version = 4 still reads 4 after run_migrations."""
    assert TARGET_USER_VERSION == 4
    conn = _bare_db(user_version=4)
    run_migrations(conn)
    assert _user_version(conn) == 4
