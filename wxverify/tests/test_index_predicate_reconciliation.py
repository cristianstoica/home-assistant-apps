"""Tests for the partial-index reconciliation seam in ``create_schema``.

``_sync_invalid_sample_index`` drops and rebuilds ``idx_samples_invalid``
whenever the stored predicate no longer matches the current
``FORECAST_VALUE_RANGES``-derived predicate, so a schema built under an old
bound self-heals on the next ``create_schema`` call rather than leaving a
statement that names ``INDEXED BY idx_samples_invalid`` permanently broken.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify.collection.forecast_validation import (
    FORECAST_VALUE_RANGES,
    invalid_forecast_sample_sql,
)
from wxverify.db.migrations import _index_predicate, create_schema, run_migrations
from wxverify.provider_ops import bad_sample_count_sql


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('IndexReconciliation', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )


def _insert_sample(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, value: float
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, 'temperature', '2026-01-01T00:00:00Z',
                '2026-01-01T06:00:00Z', 24, ?, '{}', 'run-1')
        """,
        (site_id, feed_id, value),
    )


def test_stale_index_predicate_breaks_the_query_at_prepare_time_and_self_heals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Build the schema under a narrowed temperature bound, so the stored
    # partial index's predicate is tighter than the one the current
    # FORECAST_VALUE_RANGES would produce.
    monkeypatch.setitem(FORECAST_VALUE_RANGES, "temperature", (-90.0, 60.0))
    conn = sqlite3.connect(str(tmp_path / "reconciliation.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)

    site_id = _insert_site(conn)
    feed_row = conn.execute(
        "SELECT id FROM feeds WHERE is_virtual = 0 ORDER BY id LIMIT 1"
    ).fetchone()
    assert feed_row is not None
    feed_id = int(feed_row["id"])

    # Widen the bound back to its real value without touching the on-disk
    # index: the statement's INDEXED BY hint now names a partial index whose
    # stored predicate can no longer be proven to imply the WHERE clause.
    monkeypatch.setitem(FORECAST_VALUE_RANGES, "temperature", (-90.0, 70.0))
    sql = bad_sample_count_sql("?")
    with pytest.raises(sqlite3.OperationalError, match="no query solution"):
        # This is a prepare-time failure: it fires with zero rows in the
        # table, before any data is examined.
        conn.execute(sql, (site_id, feed_id)).fetchone()

    # create_schema() is the reconciliation seam: it detects the predicate
    # mismatch, drops the stale index, and rebuilds it under the current bound.
    create_schema(conn)

    # The rebuilt index's stored predicate must match what the current
    # generator produces -- not merely "some index exists under this name"
    # (a stale-but-different predicate could still let the query below run
    # by coincidence if the newly-inserted row happened to fall outside the
    # narrowed range too). This is the direct assertion for the reconciled
    # predicate; the working-query check that follows is not redundant with
    # it -- CREATE INDEX succeeding says nothing about whether INDEXED BY can
    # actually be satisfied afterward.
    stored_after = conn.execute(
        "SELECT sql FROM sqlite_master"
        " WHERE type='index' AND name='idx_samples_invalid'"
    ).fetchone()
    assert stored_after is not None
    current_ddl = (
        "CREATE INDEX IF NOT EXISTS idx_samples_invalid "
        "ON forecast_samples(site_id, feed_id) "
        f"WHERE {invalid_forecast_sample_sql('forecast_samples')}"
    )
    assert _index_predicate(stored_after["sql"]) == _index_predicate(current_ddl)

    _insert_sample(conn, site_id=site_id, feed_id=feed_id, value=999.0)
    row = conn.execute(sql, (site_id, feed_id)).fetchone()
    assert int(row[0]) == 1


class _FaultConn(sqlite3.Connection):
    def __init__(self, *args: object, fault_on: str, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]
        self._fault_on = fault_on.upper()
        self._faulted = False

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:  # type: ignore[override]
        if not self._faulted and self._fault_on in sql.upper():
            self._faulted = True
            raise sqlite3.OperationalError(
                f"_FaultConn: injected fault on statement matching {self._fault_on!r}"
            )
        return super().execute(sql, parameters)  # type: ignore[call-arg]


def _stored_index_sql(db_path: Path) -> str:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master"
            " WHERE type='index' AND name='idx_samples_invalid'"
        ).fetchone()
        return str(row[0])
    finally:
        conn.close()


def _open_fault_conn(db_path: Path, *, fault_on: str) -> _FaultConn:
    conn: _FaultConn = sqlite3.connect(  # type: ignore[call-overload]
        str(db_path),
        isolation_level=None,
        factory=_FaultConn,
        fault_on=fault_on,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def test_reconciliation_rolls_back_a_fault_between_drop_and_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "reconciliation-fault.db"

    monkeypatch.setitem(FORECAST_VALUE_RANGES, "temperature", (-90.0, 60.0))
    setup_conn = sqlite3.connect(str(db_path))
    setup_conn.row_factory = sqlite3.Row
    run_migrations(setup_conn)
    setup_conn.close()

    monkeypatch.setitem(FORECAST_VALUE_RANGES, "temperature", (-90.0, 70.0))

    stale_sql = _stored_index_sql(db_path)

    fault_conn = _open_fault_conn(
        db_path, fault_on="CREATE INDEX IF NOT EXISTS idx_samples_invalid"
    )
    with pytest.raises(sqlite3.OperationalError, match="injected fault"):
        create_schema(fault_conn)
    fault_conn.close()

    # The SAVEPOINT rollback must leave the stale (pre-reconciliation) index
    # intact rather than a half-dropped schema.
    assert _stored_index_sql(db_path) == stale_sql

    # A clean retry (no injected fault) rebuilds the index successfully.
    clean_conn = sqlite3.connect(str(db_path))
    clean_conn.row_factory = sqlite3.Row
    create_schema(clean_conn)
    clean_conn.close()
    assert _stored_index_sql(db_path) != stale_sql


def test_freshly_created_db_stores_the_current_generator_predicate(
    tmp_path: Path,
) -> None:
    # A brand-new database never goes through the stale-predicate-vs-current
    # comparison at all (there is no prior stored index to diff against) --
    # this is a sanity check that _sync_invalid_sample_index's first-run
    # CREATE branch stores the real generator output, not a hand-copied
    # literal that could drift from invalid_forecast_sample_sql() over time.
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.close()

    stored_sql = _stored_index_sql(db_path)
    current_ddl = (
        "CREATE INDEX IF NOT EXISTS idx_samples_invalid "
        "ON forecast_samples(site_id, feed_id) "
        f"WHERE {invalid_forecast_sample_sql('forecast_samples')}"
    )
    assert _index_predicate(stored_sql) == _index_predicate(current_ddl)
