"""Part B -- the migration frame for the rain-label boundary (§11, §19.6).

``record_precip_hour_start_boundary`` is a new migration step: it finds the
newest ``fetched_at`` among Open-Meteo/meteoblue rain samples (the newest row
written under the OLD, end-of-hour label) and stores it as the
``PRECIP_HOUR_START_BOUNDARY_KEY`` runtime-state key. Any Open-Meteo/
meteoblue rain sample fetched at or before that stamp -- including a NULL or
unparseable stamp -- is excluded from pairing, serving and replay by
``precip_label_eligible_sql``. The step also deletes stale precip pairs and
score-cache rows and restarts building timezone-correction generations.

Every new symbol is imported inside the test body that needs it, so a
missing or misnamed symbol fails only that test, never the whole module at
collection time.

Synthetic data only: no place name, coordinate, or non-UTC zone appears,
and no internal ticket number appears in a comment.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
    set_runtime_state_now,
)

# ---------------------------------------------------------------------------
# Shared DB harness
# ---------------------------------------------------------------------------

_BOUNDARY_KEY = "precip_hour_start_boundary"
_SENTINEL = "1970-01-01T00:00:00Z"
_K = "2026-01-01T12:00:00Z"


def _connect(path: str) -> sqlite3.Connection:
    # 0.16.12: autocommit mode, matching the app's own connections, so the
    # migration runner's own SAVEPOINT does not nest inside an implicit
    # legacy-mode BEGIN.
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _make_db() -> sqlite3.Connection:
    """In-memory connection, migrated schema, one enabled synthetic site."""
    # 0.16.12: autocommit mode, matching _connect above; existing .commit()
    # calls on this connection become harmless no-ops.
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    _make_site(conn)
    return conn


def _make_site(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    name: str = "ISTATIONX1",
    timezone: str = "UTC",
    enabled: int = 1,
) -> int:
    conn.execute(
        """
        INSERT INTO sites
            (id, name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, ?, 0.0, 0.0, 0.0, ?, ?)
        """,
        (site_id, name, timezone, enabled),
    )
    return site_id


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    """Explicitly enable a feed that is not subscribed by default."""
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET enabled = 1
        """,
        (site_id, feed_id),
    )


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float,
    fetched_at: str | None,
) -> int:
    """A copy of ``test_forecast_extrema_coverage._insert_sample`` that
    takes an explicit ``fetched_at`` instead of defaulting it to
    ``issued_at`` -- needed because every M-test below drives the
    eligibility gate through the stamp, not through the issue time."""
    cur = conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            value,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def _insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    day_ahead: int,
    forecast: float,
    observed: float = 10.0,
) -> int:
    """A copy of ``test_forecast_extrema_coverage._insert_pair``."""
    from wxverify.db.tz_generations import ensure_published_generation

    error = forecast - observed
    cur = conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
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
            ensure_published_generation(conn, site_id),
        ),
    )
    return int(cur.lastrowid)


def _insert_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    variable: str,
    valid_at: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO observations (site_id, variable, valid_at, value, n_stations)
        VALUES (?, ?, ?, ?, 1)
        """,
        (site_id, variable, valid_at, value),
    )


def _pending_job_count(
    conn: sqlite3.Connection, job_type: str, site_id: int | None = None
) -> int:
    if site_id is None:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type=? AND status='pending'",
            (job_type,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs"
            " WHERE type=? AND site_id=? AND status='pending'",
            (job_type, site_id),
        ).fetchone()
    return int(row["n"])


def _complete_jobs(conn: sqlite3.Connection, job_type: str) -> None:
    conn.execute("UPDATE jobs SET status='completed' WHERE type=?", (job_type,))


def _clear_boundary_and_seed(conn: sqlite3.Connection) -> None:
    """``run_migrations`` already called ``record_precip_hour_start_boundary``
    once (it runs unconditionally after migrations), writing the sentinel.
    Delete the key so the step under test runs again, against freshly seeded
    rows, as the setup paragraph (§19.6) directs."""
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)


# ---------------------------------------------------------------------------
# M1 -- the boundary value
# ---------------------------------------------------------------------------


def test_m1a_boundary_is_the_sentinel_with_no_affected_row() -> None:
    # With no Open-Meteo/meteoblue rain row at all, the boundary must be
    # the 1970 sentinel, not NULL or an error.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)

    record_precip_hour_start_boundary(conn)

    assert get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY) == _SENTINEL


def test_m1b_boundary_uses_julianday_order_not_text_order() -> None:
    # This is the test that tells a julianday-ordered MAX apart from a text
    # MAX: "Z" sorts after "." in plain text, so a text MAX would pick
    # 2026-01-01T10:00:00Z over 2026-01-01T10:00:00.500000Z, which is
    # chronologically later. A bug that orders the stamps as text instead
    # of by time reintroduces exactly that mistake.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T10:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T11:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00.500000Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T09:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T09:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T08:00:00Z",
        value=1.0,
        fetched_at=None,
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-02-01T00:00:00Z",
        valid_at="2026-02-01T01:00:00Z",
        value=1.0,
        fetched_at="2026-02-01T00:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-03-01T00:00:00Z",
        valid_at="2026-03-01T01:00:00Z",
        value=10.0,
        fetched_at="2026-03-01T00:00:00Z",
    )

    record_precip_hour_start_boundary(conn)

    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T10:00:00.500000Z"
    )


def test_m1c_only_null_stamped_rain_leaves_the_sentinel() -> None:
    # Pairs with M1(a) and M1(b): a present-but-all-NULL affected row set
    # must behave exactly like an absent one, never like a parseable stamp
    # of "now" or an error.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T08:00:00Z",
        value=1.0,
        fetched_at=None,
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T09:00:00Z",
        value=1.0,
        fetched_at=None,
    )

    record_precip_hour_start_boundary(conn)

    assert get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY) == _SENTINEL


# ---------------------------------------------------------------------------
# M2 -- no sample changes
# ---------------------------------------------------------------------------


def test_m2_boundary_step_changes_no_sample_row() -> None:
    # The step deletes pairs/score-cache rows and restarts generations but
    # must never touch forecast_samples -- the samples are evidence of what
    # was fetched, not of what is eligible.
    from wxverify.db.migrations import record_precip_hour_start_boundary

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend)".rstrip(")"))
    sample_ids = [
        _insert_sample(
            conn,
            feed_id=om,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=1.0,
            fetched_at="2026-01-01T11:00:00Z",
        ),
        _insert_sample(
            conn,
            feed_id=om,
            variable="temperature",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=10.0,
            fetched_at="2026-01-01T11:00:00Z",
        ),
        _insert_sample(
            conn,
            feed_id=google,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T13:00:00Z",
            value=0.5,
            fetched_at="2026-01-01T11:00:00Z",
        ),
    ]
    before = [
        tuple(
            conn.execute("SELECT * FROM forecast_samples WHERE id=?", (sid,)).fetchone()
        )
        for sid in sample_ids
    ]

    record_precip_hour_start_boundary(conn)

    after = [
        tuple(
            conn.execute("SELECT * FROM forecast_samples WHERE id=?", (sid,)).fetchone()
        )
        for sid in sample_ids
    ]
    assert before == after


# ---------------------------------------------------------------------------
# M3 -- the pairer and the probe read the same gate
# ---------------------------------------------------------------------------


def test_m3_pairer_and_probe_use_the_same_gate() -> None:
    # Pins the pairer and the probe to the same eligibility gate: the named
    # counts (4 eligible, exactly 3 excluded) must hold.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.scoring.pairing import (
        compute_real_model_pairs,
        real_model_pair_pending,
    )

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    meteoblue = _feed_id(conn, "meteoblue", "multimodel")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    valid_ats = [
        "2026-01-01T13:00:00Z",
        "2026-01-01T14:00:00Z",
        "2026-01-01T15:00:00Z",
        "2026-01-01T16:00:00Z",
        "2026-01-01T17:00:00Z",
        "2026-01-01T18:00:00Z",
        "2026-01-01T19:00:00Z",
    ]
    for valid_at in valid_ats:
        _insert_observation(conn, variable="precip", valid_at=valid_at, value=0.0)

    excluded_ids = [
        # Fetched exactly at K: strict > required, so this stays excluded.
        _insert_sample(
            conn,
            feed_id=om,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at=valid_ats[0],
            value=1.0,
            fetched_at=_K,
        ),
        # Fetched before K.
        _insert_sample(
            conn,
            feed_id=om,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at=valid_ats[1],
            value=1.0,
            fetched_at="2026-01-01T11:00:00Z",
        ),
        # NULL fetched_at: fails closed.
        _insert_sample(
            conn,
            feed_id=om,
            variable="precip",
            issued_at="2026-01-01T00:00:00Z",
            valid_at=valid_ats[2],
            value=1.0,
            fetched_at=None,
        ),
    ]
    eligible_om_id = _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[3],
        value=1.0,
        fetched_at="2026-01-01T12:00:01Z",
    )
    _insert_sample(
        conn,
        feed_id=meteoblue,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[4],
        value=1.0,
        fetched_at="2026-01-01T12:00:01Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[5],
        value=10.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[6],
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _insert_observation(conn, variable="temperature", valid_at=valid_ats[5], value=10.0)

    assert real_model_pair_pending(conn, excluded_ids) is False
    assert real_model_pair_pending(conn, [eligible_om_id]) is True

    delta = compute_real_model_pairs(conn)
    paired_valid_ats = {op.values[4] for op in delta.ops}
    assert paired_valid_ats == {valid_ats[3], valid_ats[4], valid_ats[5], valid_ats[6]}
    assert len(delta.ops) == 4

    from wxverify.scoring.split import apply_delta

    apply_delta(conn, delta)
    rows = conn.execute(
        "SELECT valid_at FROM forecast_pairs"
        " WHERE variable='precip' OR variable='temperature'"
    ).fetchall()
    assert {r["valid_at"] for r in rows} == {
        valid_ats[3],
        valid_ats[4],
        valid_ats[5],
        valid_ats[6],
    }


# ---------------------------------------------------------------------------
# M4 -- a second call changes nothing
# ---------------------------------------------------------------------------


def test_m4_second_call_changes_nothing() -> None:
    # A second call must leave the boundary and the existing pair alone and
    # enqueue no further rescore job.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.scoring.pairing import compute_real_model_pairs
    from wxverify.scoring.split import apply_delta

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00Z",
    )
    record_precip_hour_start_boundary(conn)
    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T10:00:00Z"
    )

    _complete_jobs(conn, "pair_and_score")

    # Seed an eligible row + its pair after the first call.
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-02T00:00:00Z",
        valid_at="2026-01-02T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-02T00:00:00Z",
    )
    _insert_observation(
        conn, variable="precip", valid_at="2026-01-02T13:00:00Z", value=0.0
    )
    delta = compute_real_model_pairs(conn)
    apply_delta(conn, delta)
    pair_count_before = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE variable='precip'"
    ).fetchone()["n"]
    assert pair_count_before == 1

    record_precip_hour_start_boundary(conn)

    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T10:00:00Z"
    )
    pair_count_after = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE variable='precip'"
    ).fetchone()["n"]
    assert pair_count_after == 1
    assert _pending_job_count(conn, "pair_and_score") == 0


# ---------------------------------------------------------------------------
# M5 -- fresh install
# ---------------------------------------------------------------------------


def test_m5_fresh_install_reads_the_sentinel_with_no_rescore_job(
    tmp_path: Path,
) -> None:
    # A brand-new database must read the sentinel boundary key with no
    # pending rescore job.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.connection import Database

    db = Database(str(tmp_path / "fresh.db"))
    try:
        assert (
            get_runtime_state(db._conn, PRECIP_HOUR_START_BOUNDARY_KEY)  # noqa: SLF001
            == _SENTINEL
        )
        assert _pending_job_count(db._conn, "pair_and_score") == 0  # noqa: SLF001
    finally:
        db.close()


# ---------------------------------------------------------------------------
# M6 -- import
# ---------------------------------------------------------------------------


def _build_backup_bytes(path: Path, *, boundary_key_value: str | None) -> bytes:
    """A standalone, fully migrated backup file: an old backup if
    ``boundary_key_value`` is None (the key absent, as a pre-this-release
    file would be), else a new one with the key already set."""
    conn = _connect(str(path))
    run_migrations(conn)
    site_id = _make_site(conn)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    if boundary_key_value is None:
        delete_runtime_state(conn, _BOUNDARY_KEY)
    else:
        set_runtime_state(conn, _BOUNDARY_KEY, boundary_key_value)
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00Z",
    )
    _insert_observation(
        conn,
        site_id=site_id,
        variable="precip",
        valid_at="2026-01-01T13:00:00Z",
        value=0.0,
    )
    from wxverify.db.tz_generations import ensure_published_generation

    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'precip', '2026-01-01T00:00:00Z', '2026-01-01T13:00:00Z',
                13, 0, 1.0, 0.0, 1.0, 1.0, 1.0, ?)
        """,
        (site_id, om, ensure_published_generation(conn, site_id)),
    )
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    return path.read_bytes()


async def _idle_worker(_db: object) -> None:
    import asyncio

    await asyncio.Event().wait()


def _csrf_headers(
    client: object, *, origin: str = "http://testserver"
) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]  # type: ignore[attr-defined]
    return {
        "Origin": origin,
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def test_m6_import_recomputes_boundary_for_an_old_backup_but_not_a_new_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An import of an old backup must recompute the boundary key from the
    # imported samples; an import of a new backup must keep its own key.
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.collection.precip_labels import precip_label_eligible_sql
    from wxverify.db.connection import close_db, init_db

    old_backup = _build_backup_bytes(tmp_path / "old.db", boundary_key_value=None)
    new_backup = _build_backup_bytes(
        tmp_path / "new.db", boundary_key_value="2026-01-01T09:00:00Z"
    )

    close_db()
    config.db_path = str(tmp_path / "live.db")
    config.options_path = str(tmp_path / "live.options.json")
    db = init_db(config.db_path)
    try:
        monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
        from wxverify.api.app import create_app

        app = create_app(root_path="")
        client = TestClient(app, raise_server_exceptions=False)

        eligible_sql = (
            "SELECT COUNT(*) FROM forecast_samples fs"
            " JOIN feeds f ON f.id = fs.feed_id"
            f" WHERE fs.id = ? AND {precip_label_eligible_sql('fs', 'f')}"
        )

        resp = client.post(
            "/api/import/db", content=old_backup, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200

        conn = db._conn  # noqa: SLF001
        # 0.16.12: an old backup carries no boundary key, so the import must
        # recompute it from the imported samples' own fetched_at (plan §19.6).
        assert get_runtime_state(conn, _BOUNDARY_KEY) == "2026-01-01T10:00:00Z"
        row = conn.execute(
            "SELECT id FROM forecast_samples WHERE variable='precip'"
        ).fetchone()
        eligible_count = conn.execute(eligible_sql, (int(row["id"]),)).fetchone()[0]
        assert eligible_count == 0

        resp2 = client.post(
            "/api/import/db", content=new_backup, headers=_csrf_headers(client)
        )
        assert resp2.status_code == 200
        conn2 = db._conn  # noqa: SLF001
        # 0.16.12: a new backup already carries its own boundary key, so the
        # import must keep it rather than recomputing from scratch.
        assert get_runtime_state(conn2, _BOUNDARY_KEY) == "2026-01-01T09:00:00Z"
        row2 = conn2.execute(
            "SELECT id FROM forecast_samples WHERE variable='precip'"
        ).fetchone()
        eligible_count2 = conn2.execute(eligible_sql, (int(row2["id"]),)).fetchone()[0]
        assert eligible_count2 == 1
    finally:
        close_db()


# ---------------------------------------------------------------------------
# M7 -- atomicity
# ---------------------------------------------------------------------------

_INJECT_TRIGGER_SQL = """
CREATE TEMP TRIGGER inject_failure BEFORE DELETE ON main.score_cache
WHEN OLD.variable = 'precip'
BEGIN
  SELECT RAISE(ABORT, 'injected');
END
"""


def _snapshot(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    tables = (
        "forecast_samples",
        "forecast_pairs",
        "score_cache",
        "jobs",
        "runtime_state",
        "timezone_generations",
    )
    snap = {
        t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY rowid")]
        for t in tables
    }
    snap["user_version"] = [conn.execute("PRAGMA user_version").fetchone()[0]]
    return snap


def test_m7_boundary_step_rolls_back_atomically_on_failure(tmp_path: Path) -> None:
    # The boundary step must roll back atomically on failure, leaving every
    # marker -- user_version, boundary key, pairs, cache -- unchanged.
    from wxverify.db.migrations import (  # noqa: F401
        TARGET_USER_VERSION,
        record_precip_hour_start_boundary,
    )
    from wxverify.scoring.cache import upsert_score_cache
    from wxverify.scoring.metrics import MetricResult

    path = tmp_path / "atomic.db"

    # Settle pass: open and close once so any write-on-every-open step runs
    # before the snapshot.
    settle = _connect(str(path))
    run_migrations(settle)
    settle.commit()
    settle.close()

    conn = _connect(str(path))
    site_id = _make_site(conn, enabled=1)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T10:00:00Z",
    )
    _insert_observation(
        conn,
        site_id=site_id,
        variable="precip",
        valid_at="2026-01-01T13:00:00Z",
        value=0.0,
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        lead_hours=13,
        day_ahead=0,
        forecast=1.0,
        observed=0.0,
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=om,
        variable="precip",
        day_ahead=0,
        window_key="w:7",
        result=MetricResult(n=1, confident=False),
        computed_at="2026-01-01T12:00:00Z",
    )
    from wxverify.db.tz_generations import start_retrospective_correction

    start_retrospective_correction(conn, site_id, "UTC")
    conn.commit()

    # (i) every marker present: a reopen changes nothing.
    before_i = _snapshot(conn)
    conn.close()
    reopened = _connect(str(path))
    run_migrations(reopened)
    reopened.commit()
    assert _snapshot(reopened) == before_i
    reopened.close()

    # (ii) delete the boundary key and roll user_version back to N.
    conn = _connect(str(path))
    delete_runtime_state(conn, _BOUNDARY_KEY)
    conn.execute(f"PRAGMA user_version = {TARGET_USER_VERSION - 1}")
    conn.commit()
    snapshot_before_failure = _snapshot(conn)

    # (iii) inject the failure and confirm the rollback.
    conn.execute(_INJECT_TRIGGER_SQL)
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        run_migrations(conn)
    assert conn.in_transaction is False
    assert _snapshot(conn) == snapshot_before_failure
    assert get_runtime_state(conn, _BOUNDARY_KEY) is None
    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION - 1

    # (iv) drop the trigger and run again: it finishes this time.
    conn.execute("DROP TRIGGER inject_failure")
    run_migrations(conn)
    conn.commit()
    pair_count = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE variable='precip'"
    ).fetchone()["n"]
    cache_count = conn.execute(
        "SELECT COUNT(*) AS n FROM score_cache WHERE variable='precip'"
    ).fetchone()["n"]
    assert pair_count == 0
    assert cache_count == 0
    assert get_runtime_state(conn, _BOUNDARY_KEY) is not None
    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION
    conn.close()


# ---------------------------------------------------------------------------
# M8 -- the loader, live and as-of
# ---------------------------------------------------------------------------


def test_m8_loader_gate_sits_before_the_row_number_pick() -> None:
    # The eligibility clause must sit before load_future_samples' row-number
    # pick, and count_null_availability_samples must apply it too.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.forecast.data import (
        count_null_availability_samples,
        load_future_samples,
    )

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    # Slot 1: two competing OM rain rows at the same valid_at.
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T06:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=2.0,
        fetched_at="2026-01-01T07:00:00Z",
    )
    # Slot 2: NULL-stamped OM rain, excluded.
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-03T00:00:00Z",
        value=5.0,
        fetched_at=None,
    )
    # OM temperature and Google rain fetched before K, both returned.
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        value=10.0,
        fetched_at="2026-01-01T07:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T01:00:00Z",
        value=0.5,
        fetched_at="2026-01-01T07:00:00Z",
    )
    # Slot 3: Google rain with NULL fetched_at.
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-04T00:00:00Z",
        value=0.5,
        fetched_at=None,
    )

    live = load_future_samples(
        conn,
        site_id=1,
        since_valid_at="2026-01-01T00:00:00Z",
        served_only=False,
    )
    live_om_rain = [
        s
        for s in live
        if s.feed_id == om
        and s.variable == "precip"
        and s.valid_at == "2026-01-02T00:00:00Z"
    ]
    assert len(live_om_rain) == 1
    assert live_om_rain[0].value == 1.0

    as_of = load_future_samples(
        conn,
        site_id=1,
        since_valid_at="2026-01-01T00:00:00Z",
        served_only=False,
        as_of="2026-01-01T14:00:00Z",
    )
    as_of_om_rain = [
        s
        for s in as_of
        if s.feed_id == om
        and s.variable == "precip"
        and s.valid_at == "2026-01-02T00:00:00Z"
    ]
    assert len(as_of_om_rain) == 1
    assert as_of_om_rain[0].value == 1.0

    assert not any(
        s.feed_id == om
        and s.variable == "precip"
        and s.valid_at == "2026-01-03T00:00:00Z"
        for s in live
    )
    assert any(s.feed_id == om and s.variable == "temperature" for s in live)
    assert any(s.feed_id == google and s.variable == "precip" for s in live)

    null_count = count_null_availability_samples(
        conn,
        site_id=1,
        since_valid_at="2026-01-01T00:00:00Z",
    )
    assert null_count == 1


# ---------------------------------------------------------------------------
# M9 -- the deletion scope
# ---------------------------------------------------------------------------


def test_m9_affected_pairs_deleted_in_every_generation_others_kept() -> None:
    # The assertions below require the Open-Meteo, meteoblue and
    # multimodel-mean precip pairs to be GONE across every generation, so a
    # no-op step fails them.
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.db.tz_generations import start_retrospective_correction

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    site_id = 1
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    meteoblue = _feed_id(conn, "meteoblue", "multimodel")
    mean = _feed_id(conn, "virtual", "_multimodel_mean")
    persistence = _feed_id(conn, "virtual", "_persistence")
    google = _feed_id(conn, "google", "blend")

    published_pair = _insert_pair(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        lead_hours=13,
        day_ahead=0,
        forecast=1.0,
    )
    meteoblue_pair = _insert_pair(
        conn,
        feed_id=meteoblue,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        lead_hours=14,
        day_ahead=0,
        forecast=1.0,
    )
    mean_pair = _insert_pair(
        conn,
        feed_id=mean,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T15:00:00Z",
        lead_hours=15,
        day_ahead=0,
        forecast=1.0,
    )
    persistence_pair = _insert_pair(
        conn,
        feed_id=persistence,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T16:00:00Z",
        lead_hours=16,
        day_ahead=0,
        forecast=1.0,
    )
    google_pair = _insert_pair(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T17:00:00Z",
        lead_hours=17,
        day_ahead=0,
        forecast=1.0,
    )
    temp_pair = _insert_pair(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T18:00:00Z",
        lead_hours=18,
        day_ahead=0,
        forecast=10.0,
    )

    generation_id = start_retrospective_correction(conn, site_id, "UTC")
    from wxverify.db.queue import enqueue_if_absent
    from wxverify.db.tz_generations import correction_job_key

    enqueue_if_absent(
        conn,
        "timezone_correction",
        site_id,
        correction_job_key(generation_id),
        {"generation_id": generation_id},
    )
    building_om_pair = conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, 'precip', '2026-01-01T00:00:00Z', '2026-01-01T19:00:00Z',
                19, 0, 1.0, 0.0, 1.0, 1.0, 1.0, ?)
        """,
        (site_id, om, generation_id),
    ).lastrowid

    record_precip_hour_start_boundary(conn)

    remaining = {
        int(r["id"]) for r in conn.execute("SELECT id FROM forecast_pairs").fetchall()
    }
    assert published_pair not in remaining
    assert meteoblue_pair not in remaining
    assert mean_pair not in remaining
    assert int(building_om_pair) not in remaining
    assert persistence_pair in remaining
    assert google_pair in remaining
    assert temp_pair in remaining


# ---------------------------------------------------------------------------
# M11 -- score cache + rescore jobs
# ---------------------------------------------------------------------------


def test_m11_precip_cache_cleared_and_one_rescore_job_per_enabled_site() -> None:
    # The step must clear only the precip score-cache rows and enqueue
    # exactly one rescore job per enabled site.
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.scoring.cache import upsert_score_cache
    from wxverify.scoring.metrics import MetricResult

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    enabled_site = 1
    disabled_site = _make_site(conn, site_id=2, name="ISTATIONY1", enabled=0)
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    upsert_score_cache(
        conn,
        site_id=enabled_site,
        feed_id=om,
        variable="precip",
        day_ahead=0,
        window_key="w:7",
        result=MetricResult(n=1, confident=False),
        computed_at="2026-01-01T12:00:00Z",
    )
    upsert_score_cache(
        conn,
        site_id=disabled_site,
        feed_id=om,
        variable="precip",
        day_ahead=0,
        window_key="w:7",
        result=MetricResult(n=1, confident=False),
        computed_at="2026-01-01T12:00:00Z",
    )
    upsert_score_cache(
        conn,
        site_id=enabled_site,
        feed_id=om,
        variable="temperature",
        day_ahead=0,
        window_key="w:7",
        result=MetricResult(n=1, confident=False),
        computed_at="2026-01-01T12:00:00Z",
    )

    record_precip_hour_start_boundary(conn)

    precip_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM score_cache WHERE variable='precip'"
    ).fetchone()["n"]
    assert precip_rows == 0
    other_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM score_cache WHERE variable='temperature'"
    ).fetchone()["n"]
    assert other_rows == 1
    assert _pending_job_count(conn, "pair_and_score", enabled_site) == 1
    assert _pending_job_count(conn, "pair_and_score", disabled_site) == 0


# ---------------------------------------------------------------------------
# M12 -- building generations restarted
# ---------------------------------------------------------------------------


def test_m12_building_generations_restarted() -> None:
    # A building timezone-correction generation must be restarted by the
    # boundary step.
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.db.tz_generations import (
        correction_heartbeat_key,
        correction_state_key,
        start_retrospective_correction,
    )
    from wxverify.worker.tz_correction import advance_correction

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    site_id = 1
    generation_id = start_retrospective_correction(conn, site_id, "UTC")
    set_runtime_state(
        conn,
        correction_state_key(generation_id),
        json.dumps({"phase": "days", "attempts": 2}),
    )
    set_runtime_state_now(conn, correction_heartbeat_key(generation_id))
    conn.commit()

    record_precip_hour_start_boundary(conn)

    assert get_runtime_state(conn, correction_state_key(generation_id)) is None
    assert get_runtime_state(conn, correction_heartbeat_key(generation_id)) is None
    assert _pending_job_count(conn, "timezone_correction") == 1

    conn.execute("BEGIN IMMEDIATE")
    advance_correction(conn, site_id, {"generation_id": generation_id})
    conn.execute("COMMIT")
    blob = json.loads(
        get_runtime_state(conn, correction_state_key(generation_id)) or "{}"
    )
    assert blob["attempts"] == 1

    # Case (b): complete the start call's own queued job, then run the step
    # again -- still exactly one pending job (the restart's own enqueue).
    # 0.16.12: the first call already wrote the boundary key, so without
    # clearing it a second call returns at the key gate before it ever
    # reaches the restart logic (plan §19.6).
    _complete_jobs(conn, "timezone_correction")
    _clear_boundary_and_seed(conn)
    record_precip_hour_start_boundary(conn)
    assert _pending_job_count(conn, "timezone_correction") == 1


# ---------------------------------------------------------------------------
# M13 -- the generation rebuild
# ---------------------------------------------------------------------------


def test_m13_generation_rebuild_writes_pairs_for_the_four_eligible_samples() -> None:
    # rebuild_generation_day's real-pair rebuild must apply the same
    # eligibility clause, so only the four eligible M3-style samples pair.
    from datetime import date

    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.tz_generations import start_retrospective_correction
    from wxverify.scoring.tz_rebuild import rebuild_generation_day

    conn = _make_db()
    site_id = 1
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    meteoblue = _feed_id(conn, "meteoblue", "multimodel")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)
    generation_id = start_retrospective_correction(conn, site_id, "UTC")

    valid_ats = [
        "2026-01-01T13:00:00Z",
        "2026-01-01T14:00:00Z",
        "2026-01-01T15:00:00Z",
        "2026-01-01T16:00:00Z",
        "2026-01-01T17:00:00Z",
        "2026-01-01T18:00:00Z",
        "2026-01-01T19:00:00Z",
    ]
    for valid_at in valid_ats:
        _insert_observation(conn, variable="precip", valid_at=valid_at, value=0.0)
    _insert_observation(conn, variable="temperature", valid_at=valid_ats[5], value=10.0)

    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[0],
        value=1.0,
        fetched_at=_K,
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[1],
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=valid_ats[2],
        value=1.0,
        fetched_at=None,
    )
    eligible_om_valid_at = valid_ats[3]
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=eligible_om_valid_at,
        value=1.0,
        fetched_at="2026-01-01T12:00:01Z",
    )
    eligible_meteoblue_valid_at = valid_ats[4]
    _insert_sample(
        conn,
        feed_id=meteoblue,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=eligible_meteoblue_valid_at,
        value=1.0,
        fetched_at="2026-01-01T12:00:01Z",
    )
    eligible_temp_valid_at = valid_ats[5]
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=eligible_temp_valid_at,
        value=10.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    eligible_google_valid_at = valid_ats[6]
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at=eligible_google_valid_at,
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )

    rebuild_generation_day(
        conn,
        site_id=site_id,
        generation_id=generation_id,
        timezone="UTC",
        day=date(2026, 1, 1),
        count=False,
    )

    # 0.16.12: a generation also carries the `_persistence` virtual feed's
    # own lagged pairs, so the real-feed rows must be isolated by excluding
    # virtual feeds rather than reading valid_at alone (plan §19.6).
    rows = conn.execute(
        """
        SELECT fp.feed_id, fp.variable, fp.valid_at
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        WHERE fp.tz_generation_id = ? AND f.is_virtual = 0
        """,
        (generation_id,),
    ).fetchall()
    assert {(r["feed_id"], r["variable"], r["valid_at"]) for r in rows} == {
        (om, "precip", eligible_om_valid_at),
        (meteoblue, "precip", eligible_meteoblue_valid_at),
        (om, "temperature", eligible_temp_valid_at),
        (google, "precip", eligible_google_valid_at),
    }


# ---------------------------------------------------------------------------
# M14 -- the version fence
# ---------------------------------------------------------------------------


def test_m14_boot_refuses_a_file_newer_than_the_running_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # n is deliberately lower than the real TARGET_USER_VERSION, so the
    # file on disk is stamped with a version newer than the (monkeypatched)
    # running app; boot must refuse it rather than silently proceed.
    import wxverify.db.migrations as migrations
    from wxverify.db.connection import Database

    n = 9
    path = tmp_path / "newer.db"
    db = Database(str(path))
    db.close()

    monkeypatch.setattr(migrations, "TARGET_USER_VERSION", n)
    with pytest.raises(RuntimeError, match="newer than this app"):
        Database(str(path))


def test_m14_import_rejects_a_backup_newer_than_the_running_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api import routes as _routes_pkg  # noqa: F401
    from wxverify.api.routes import db_transfer
    from wxverify.db.connection import close_db, init_db

    n = 9
    backup_path = tmp_path / "backup.db"
    backup_bytes = _build_backup_bytes(backup_path, boundary_key_value=None)

    close_db()
    config.db_path = str(tmp_path / "live.db")
    config.options_path = str(tmp_path / "live.options.json")
    init_db(config.db_path)
    try:
        monkeypatch.setattr(db_transfer, "TARGET_USER_VERSION", n)
        monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
        from wxverify.api.app import create_app

        app = create_app(root_path="")
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post(
            "/api/import/db",
            content=backup_bytes,
            headers=_csrf_headers(client),
        )
        assert resp.status_code == 422
        assert resp.json() == {"error": "exported by a newer wxverify"}
    finally:
        close_db()


def test_m14_literal_version_fence_check() -> None:
    from wxverify.db.migrations import TARGET_USER_VERSION

    n = 9
    # Pins the version bump itself: n must stay below TARGET_USER_VERSION.
    assert n < TARGET_USER_VERSION


# ---------------------------------------------------------------------------
# M15 -- a missing key fails closed
# ---------------------------------------------------------------------------


def test_m15_missing_key_excludes_affected_rows_and_reopen_recomputes_from_stored_rows() -> (  # noqa: E501
    None
):
    # A missing boundary key must exclude affected rows (fail closed), and
    # reopening must recompute the key from the stored rows' own
    # fetched_at.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.forecast.data import load_future_samples
    from wxverify.scoring.pairing import compute_real_model_pairs

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)
    _insert_sample(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    _insert_observation(
        conn, variable="precip", valid_at="2026-01-01T13:00:00Z", value=0.0
    )
    _insert_sample(
        conn,
        feed_id=om,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        value=10.0,
        fetched_at="2026-01-01T13:00:00Z",
    )
    # 0.16.12: without an observation, no temperature pair can form at all,
    # so the "temperature stays eligible" half of this test needs one.
    _insert_observation(
        conn, variable="temperature", valid_at="2026-01-01T13:00:00Z", value=10.0
    )
    _insert_sample(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        value=0.5,
        fetched_at="2026-01-01T13:00:00Z",
    )

    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)

    delta = compute_real_model_pairs(conn)
    assert not any(op.values[2] == "precip" and op.values[1] == om for op in delta.ops)
    assert any(op.values[2] == "temperature" for op in delta.ops)

    live = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    assert not any(s.feed_id == om and s.variable == "precip" for s in live)
    assert any(s.feed_id == om and s.variable == "temperature" for s in live)
    assert any(s.feed_id == google and s.variable == "precip" for s in live)

    # 0.16.12: a reopen recomputes the boundary from the stored rows' own
    # fetched_at rather than leaving the key missing forever (plan §19.6).
    record_precip_hour_start_boundary(conn)
    assert (
        get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
        == "2026-01-01T13:00:00Z"
    )
    delta_after_reopen = compute_real_model_pairs(conn)
    assert not any(
        op.values[2] == "precip" and op.values[1] == om for op in delta_after_reopen.ops
    )

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# M16 -- a refetch keeps the old row; a new run is eligible
# ---------------------------------------------------------------------------


def test_m16_refetch_keeps_the_old_row_and_a_new_run_is_eligible() -> None:
    # A refetch of the same model run keeps the old row's fetched_at and
    # excluded status; a genuinely new run is eligible.
    from wxverify.collection.forecast_fetcher import (
        persist_fetch_result,
    )
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
    from wxverify.feeds.seam import FetchResult, NormalizedSample
    from wxverify.forecast.data import load_future_samples
    from wxverify.scoring.pairing import (
        compute_real_model_pairs,
        real_model_pair_pending,
    )

    conn = _make_db()
    site_id = 1
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)
    _insert_observation(
        conn, variable="precip", valid_at="2026-01-01T13:00:00Z", value=0.0
    )

    def _sample(*, issued_at: str, lead_hours: int, value: float) -> NormalizedSample:
        return NormalizedSample(
            model="ecmwf_ifs",
            variable="precip",
            issued_at=issued_at,
            valid_at="2026-01-01T13:00:00Z",
            lead_hours=lead_hours,
            value=value,
            source_raw=str(value),
            model_run_id=f"ecmwf_ifs:{issued_at}",
        )

    outcome_a = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[
                _sample(issued_at="2026-01-01T00:00:00Z", lead_hours=13, value=1.0)
            ]
        ),
        fetched_at="2026-01-01T11:00:00Z",
    )
    assert outcome_a.inserted_count == 1

    outcome_b = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[
                _sample(issued_at="2026-01-01T00:00:00Z", lead_hours=13, value=2.0)
            ]
        ),
    )
    assert outcome_b.inserted_count == 0
    assert outcome_b.usable_sample_count == 1

    row = conn.execute(
        "SELECT fetched_at, value FROM forecast_samples WHERE variable='precip'"
    ).fetchone()
    assert row["fetched_at"] == "2026-01-01T11:00:00Z"
    assert row["value"] == 1.0

    id_row = conn.execute(
        "SELECT id FROM forecast_samples WHERE variable='precip'"
    ).fetchone()
    delta = compute_real_model_pairs(conn)
    assert not any(op.values[2] == "precip" for op in delta.ops)
    assert real_model_pair_pending(conn, [int(id_row["id"])]) is False
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    assert not any(s.feed_id == om and s.variable == "precip" for s in live)

    outcome_c = persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=om,
        result=FetchResult(
            samples=[_sample(issued_at="2026-01-01T06:00:00Z", lead_hours=7, value=3.0)]
        ),
    )
    assert outcome_c.inserted_count == 1

    new_row = conn.execute(
        "SELECT id, fetched_at FROM forecast_samples"
        " WHERE variable='precip' AND issued_at='2026-01-01T06:00:00Z'"
    ).fetchone()
    assert new_row is not None
    from wxverify.core.timeutil import parse_utc

    assert parse_utc(str(new_row["fetched_at"])) > parse_utc(_K)

    delta2 = compute_real_model_pairs(conn)
    assert any(
        op.values[2] == "precip" and op.values[3] == "2026-01-01T06:00:00Z"
        for op in delta2.ops
    )
    assert real_model_pair_pending(conn, [int(new_row["id"])]) is True
    live2 = load_future_samples(
        conn, site_id=site_id, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
    matches = [s for s in live2 if s.feed_id == om and s.variable == "precip"]
    assert len(matches) == 1
    assert matches[0].value == 3.0


# ---------------------------------------------------------------------------
# M17 -- missing rain stays missing, and the mean keeps no group of one
# ---------------------------------------------------------------------------


def _m17_insert_samples_every_hour(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    value: float,
    fetched_at: str | None,
    excluded_hours: set[str] | None = None,
    excluded_fetched_at: str | None = None,
) -> None:
    excluded_hours = excluded_hours or set()
    for h in range(24):
        valid_at = f"2026-01-02T{h:02d}:00:00Z"
        stamp = excluded_fetched_at if valid_at in excluded_hours else fetched_at
        lead_hours = 18 + h
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at="2026-01-01T06:00:00Z",
            valid_at=valid_at,
            lead_hours=lead_hours,
            value=value,
            fetched_at=stamp,
        )


def _m17_build_forecast_tile(
    conn: sqlite3.Connection, site_id: int = 1, depth: str = "2"
):
    from datetime import UTC, datetime

    from wxverify.forecast.service import build_forecast
    from wxverify.settings.keys import set_setting

    set_setting(conn, "forecast_blend_depth", depth)
    view = build_forecast(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        now=datetime(2026, 1, 2, 2, tzinfo=UTC),
    )
    tile = next(t for t in view.tiles if t.date_iso == "2026-01-02")
    return tile


def test_m17a_an_excluded_feed_is_dropped_from_the_blend() -> None:
    # The excluded feed's rain must be dropped from the blend entirely, not
    # just scored differently.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _subscribe(conn, 1, google)
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _m17_insert_samples_every_hour(
        conn,
        feed_id=google,
        variable="precip",
        value=0.5,
        fetched_at="2026-01-01T13:00:00Z",
    )

    tile = _m17_build_forecast_tile(conn)

    assert tile.precip.total_mm == 12.0
    assert tile.precip.wet_hours == 24
    assert [f.feed_id for f in tile.precip.meta.feeds] == [google]
    assert om not in tile.precip.meta.contributor_ids


def test_m17b_a_fully_excluded_rain_feed_is_not_available_not_zero() -> None:
    # A feed whose rain is entirely excluded must read not_available, with
    # total_mm and wet_hours both None -- never a zero total.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T11:00:00Z",
    )
    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="temperature",
        value=10.0,
        fetched_at="2026-01-01T13:00:00Z",
    )

    tile = _m17_build_forecast_tile(conn)

    assert tile.precip.meta.state == "not_available"
    assert tile.precip.total_mm is None
    assert tile.precip.wet_hours is None


def test_m17c_one_eligible_hour_never_substitutes_for_the_day() -> None:
    # A single eligible hour out of 24 must never substitute for the full
    # day: the outcome is extrema_unavailable, with total_mm, wet_hours and
    # feeds all empty/None.
    from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY

    conn = _make_db()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, _K)

    eligible_hour = {"2026-01-02T00:00:00Z"}
    excluded_hours = {f"2026-01-02T{h:02d}:00:00Z" for h in range(1, 24)}
    _m17_insert_samples_every_hour(
        conn,
        feed_id=om,
        variable="precip",
        value=1.0,
        fetched_at="2026-01-01T13:00:00Z",
        excluded_hours=excluded_hours,
        excluded_fetched_at="2026-01-01T11:00:00Z",
    )
    # (The "eligible_hour" set is implicit: every hour not in excluded_hours
    # keeps the eligible stamp -- only 00:00 keeps "2026-01-01T13:00:00Z".)
    assert eligible_hour  # documents intent; no behavior depends on this line

    tile = _m17_build_forecast_tile(conn)

    assert tile.precip.meta.extrema_unavailable is True
    assert tile.precip.total_mm is None
    assert tile.precip.wet_hours is None
    assert tile.precip.meta.feeds == []


def test_m17d_mean_pair_drops_the_excluded_contributor_and_keeps_no_group_of_one() -> (
    None
):
    # The step deletes every Open-Meteo rain pair, so at 13:00 only Google is
    # left and the mean writes no pair (never a group of one); at 14:00 Google
    # and Visual Crossing give the one remaining mean pair.
    from wxverify.db.migrations import record_precip_hour_start_boundary
    from wxverify.scoring.multimodel import materialize_multimodel_mean

    conn = _make_db()
    _clear_boundary_and_seed(conn)
    site_id = 1
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    vc = _feed_id(conn, "visualcrossing", "blend")
    _subscribe(conn, site_id, google)
    _subscribe(conn, site_id, vc)

    _insert_pair(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        lead_hours=13,
        day_ahead=0,
        forecast=3.0,
        observed=0.0,
    )
    _insert_pair(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T13:00:00Z",
        lead_hours=13,
        day_ahead=0,
        forecast=1.0,
        observed=0.0,
    )
    _insert_pair(
        conn,
        feed_id=om,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        lead_hours=14,
        day_ahead=0,
        forecast=3.0,
        observed=0.0,
    )
    _insert_pair(
        conn,
        feed_id=google,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        lead_hours=14,
        day_ahead=0,
        forecast=1.0,
        observed=0.0,
    )
    _insert_pair(
        conn,
        feed_id=vc,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T14:00:00Z",
        lead_hours=14,
        day_ahead=0,
        forecast=2.0,
        observed=0.0,
    )

    record_precip_hour_start_boundary(conn)
    materialize_multimodel_mean(conn, site_id)

    rows = conn.execute(
        """
        SELECT valid_at, forecast, contributors FROM forecast_pairs
        WHERE feed_id = (
            SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'
        )
          AND variable = 'precip'
        """
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["valid_at"] == "2026-01-01T14:00:00Z"
    assert rows[0]["forecast"] == 1.5
    assert rows[0]["contributors"] == 2


# ---------------------------------------------------------------------------
# M18 -- availability, rank eligibility and selection are separate
# ---------------------------------------------------------------------------


def _m18_setup() -> sqlite3.Connection:
    from wxverify.settings.keys import set_setting

    conn = _make_db()
    set_setting(conn, "min_n", "3")
    set_setting(conn, "forecast_blend_depth", "2")
    google = _feed_id(conn, "google", "blend")
    vc = _feed_id(conn, "visualcrossing", "blend")
    _subscribe(conn, 1, google)
    _subscribe(conn, 1, vc)
    return conn


def _m18_seed_feed_rain(conn: sqlite3.Connection, feed_id: int, value: float) -> None:
    for h in range(24):
        valid_at = f"2026-01-02T{h:02d}:00:00Z"
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="precip",
            issued_at="2026-01-01T06:00:00Z",
            valid_at=valid_at,
            lead_hours=18 + h,
            value=value,
            fetched_at="2026-01-01T13:00:00Z",
        )


def _m18_seed_rank(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    n: int,
    skill: float | None,
) -> None:
    from wxverify.scoring.cache import upsert_score_cache
    from wxverify.scoring.leaderboard import resolve_window
    from wxverify.scoring.metrics import MetricResult

    far_future_valid_ats = (
        "2035-07-01T00:00:00Z",
        "2035-07-01T01:00:00Z",
        "2035-07-01T02:00:00Z",
    )
    far_future_leads = (24, 25, 26)
    for valid_at, lead_hours in zip(
        far_future_valid_ats, far_future_leads, strict=True
    ):
        _insert_pair(
            conn,
            feed_id=feed_id,
            variable="precip",
            issued_at="2035-06-30T00:00:00Z",
            valid_at=valid_at,
            lead_hours=lead_hours,
            day_ahead=1,
            forecast=0.0,
            observed=0.0,
        )
    window_key = resolve_window(conn, "rolling").window_key
    confident = n >= 3 and skill is not None
    from wxverify.core.timeutil import isoformat_utc, utc_now

    upsert_score_cache(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="precip",
        day_ahead=1,
        window_key=window_key,
        result=MetricResult(n=n, ets=skill, skill_score=skill, confident=confident),
        computed_at=isoformat_utc(utc_now()),
    )


def test_m18a0_control_two_confident_feeds_both_selected() -> None:
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m18_seed_rank(conn, feed_id=om, n=3, skill=0.7)
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)

    tile = _m17_build_forecast_tile(conn)

    assert [f.feed_id for f in tile.precip.meta.feeds] == [om, google]
    assert tile.precip.total_mm == 18.0
    assert tile.precip.wet_hours == 24


@pytest.mark.parametrize(
    "om_n,om_skill",
    [
        pytest.param(0, None, id="a1-no-pairs-no-row"),
        pytest.param(3, None, id="a2-confident-skill-none"),
        pytest.param(2, 0.9, id="a3-below-min-n"),
    ],
)
def test_m18a_available_but_not_ranked_is_not_selected(
    om_n: int,
    om_skill: float | None,
) -> None:
    # Pins availability vs. selection: a feed present in the data but not
    # confidently ranked (no row, skill None, or below min_n) must still be
    # left out of the blend's selected feeds.
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)
    if om_n > 0:
        _m18_seed_rank(conn, feed_id=om, n=om_n, skill=om_skill)

    tile = _m17_build_forecast_tile(conn)

    assert [f.feed_id for f in tile.precip.meta.feeds] == [google]
    assert om not in tile.precip.meta.contributor_ids
    assert tile.precip.meta.state == "normal"
    assert tile.precip.total_mm == 12.0
    assert tile.precip.wet_hours == 24


def test_m18b_confident_but_outside_blend_depth_is_not_selected() -> None:
    conn = _m18_setup()
    om = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    google = _feed_id(conn, "google", "blend")
    vc = _feed_id(conn, "visualcrossing", "blend")
    _m18_seed_feed_rain(conn, om, 1.0)
    _m18_seed_feed_rain(conn, google, 0.5)
    _m18_seed_feed_rain(conn, vc, 0.25)
    _m18_seed_rank(conn, feed_id=google, n=3, skill=0.5)
    _m18_seed_rank(conn, feed_id=vc, n=3, skill=0.4)
    _m18_seed_rank(conn, feed_id=om, n=3, skill=0.3)

    tile_depth2 = _m17_build_forecast_tile(conn)
    assert [f.feed_id for f in tile_depth2.precip.meta.feeds] == [google, vc]
    assert om not in tile_depth2.precip.meta.contributor_ids
    assert tile_depth2.precip.meta.state == "normal"
    assert tile_depth2.precip.total_mm == 9.0
    assert tile_depth2.precip.wet_hours == 24

    tile_depth3 = _m17_build_forecast_tile(conn, depth="3")
    assert [f.feed_id for f in tile_depth3.precip.meta.feeds] == [google, vc, om]
    assert tile_depth3.precip.total_mm == 14.0
    assert tile_depth3.precip.wet_hours == 24
