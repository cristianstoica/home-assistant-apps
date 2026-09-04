"""§8 migration oracles (O14, O14b, O14c, O15, O15b) for step 1 of
DEF-11: the v6 ``daily_truth.admission_basis`` column.

Fixture rule for this whole group (plan §8 "Migration oracles"): the
current ``create_schema`` already contains ``admission_basis``, so
``migrate_v6_daily_truth_admission_basis``'s column-probe would no-op
against a database built by simply calling it -- both sides of every
comparison would be the same DDL and nothing would be detected. The
migrated side used here is therefore built explicitly as a genuinely-v5
database: a fresh schema with the column dropped by a real
``ALTER TABLE ... DROP COLUMN``, and ``user_version`` rolled back to 5.
Every oracle that then calls ``run_migrations`` first asserts the column
is genuinely absent -- the plan's mandatory anti-vacuity guard, without
which a future ``DROP COLUMN`` restriction would make all of these
oracles silently vacuous again.

Synthetic data only: invented site names, ``UTC``/``America/Denver``
fixture timezones, RFC-5737-style placeholders where a hostname-shaped
value is needed. No live site name, coordinate or timezone appears here.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    correct_google_horizon,
    create_schema,
    migrate_v6_daily_truth_admission_basis,
    run_migrations,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.completeness import ADMISSION_COMPLETE, ADMISSION_DEADLINE
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    published_run_key,
    result_basis_fingerprint,
)

# ---------------------------------------------------------------------------
# Shared fixture builders.
# ---------------------------------------------------------------------------


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _v5_db() -> sqlite3.Connection:
    """A genuinely-v5 database, built by dropping the column from a fresh
    schema rather than by skipping the DDL -- see module docstring.

    Pre-seeds ``sources``/``feeds``, default settings and the one-shot
    Google-horizon correction, matching a real v5 database that has
    already been booted at least once before this test's migration to v6
    -- ``run_migrations`` calls all four unconditionally on every boot
    (`migrations.py`), so a fixture that skips this step would see
    reconciled state (roster, or a settings default) change shape on the
    v6 call for a reason unrelated to ``admission_basis``, and O15/O15b
    would fail for the wrong reason. Written out explicitly rather than
    derived from ``run_migrations``'s own call list, so a future *fifth*
    unconditional reconciliation call breaks these oracles loudly (a
    missing pre-seed moving the config snapshot) instead of the fixture
    silently tracking production and hiding the gap.
    """
    conn = _fresh_db()
    conn.execute("ALTER TABLE daily_truth DROP COLUMN admission_basis")
    conn.execute("PRAGMA user_version = 5")
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
    correct_google_horizon(conn)
    return conn


def _assert_pregate_absent(conn: sqlite3.Connection) -> None:
    """Mandatory anti-vacuity guard: run immediately before
    ``run_migrations`` on every fixture in this module. If this ever
    fails, the fixture has stopped genuinely lacking the column and every
    oracle below would silently stop testing anything."""
    cols = [str(r["name"]) for r in conn.execute("PRAGMA table_info(daily_truth)")]
    assert "admission_basis" not in cols


def _column_list(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")]


def _seed_site(conn: sqlite3.Connection, name: str = "o14-site") -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )
    ensure_published_generation(conn, site_id)
    return site_id


def _insert_daily_truth_row(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    local_date: str,
    admission_basis: str | None,
    *,
    quantity: str = "temperature_high",
    value: float = 20.0,
) -> None:
    bounds = local_day_bounds(date.fromisoformat(local_date), "UTC")
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             rain_threshold_mm, stale, tz_generation_id, admission_basis)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, 'UTC', 0.2, 0, ?, ?)
        """,
        (
            site_id,
            local_date,
            quantity,
            value,
            bounds.expected_slots,
            bounds.expected_slots,
            bounds.start_utc.isoformat(),
            bounds.end_utc.isoformat(),
            generation_id,
            admission_basis,
        ),
    )


def _insert_pre_gate_truth_row(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    local_date: str,
    *,
    quantity: str = "temperature_high",
    value: float = 20.0,
) -> None:
    """Insert a ``daily_truth`` row with NO ``admission_basis`` column at
    all -- for seeding a genuinely-v5 database (§8 O15/O15b fixture): a
    pre-gate row is one whose creation predates the column's existence,
    not one written through a column that happens to hold NULL."""
    bounds = local_day_bounds(date.fromisoformat(local_date), "UTC")
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             rain_threshold_mm, stale, tz_generation_id)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, 'UTC', 0.2, 0, ?)
        """,
        (
            site_id,
            local_date,
            quantity,
            value,
            bounds.expected_slots,
            bounds.expected_slots,
            bounds.start_utc.isoformat(),
            bounds.end_utc.isoformat(),
            generation_id,
        ),
    )


def _publish_run_row(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    generation_id: int,
    fingerprint: str,
    basis: str,
    period_start: str,
    period_end: str,
) -> int:
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, result_basis_fingerprint, published_at)
            VALUES (?, ?, 1, '0.0.0-test', 'published', 1, '{}', ?, ?, ?, 1,
                    10, ?, ?, '2026-06-04T00:00:00Z')
            """,
            (
                site_id,
                generation_id,
                period_start,
                period_end,
                period_end,
                fingerprint,
                basis,
            ),
        ).lastrowid
    )
    set_runtime_state(conn, published_run_key(site_id), str(run_id))
    conn.commit()
    return run_id


def _normalize_whitespace(message: str) -> str:
    return " ".join(message.split())


# ---------------------------------------------------------------------------
# O14 -- migrated column order matches fresh column order (ordered, not set).
# ---------------------------------------------------------------------------


def test_o14_migrated_column_order_matches_fresh_as_ordered_sequence() -> None:
    """O14 -> at ``migrated_cols == fresh_cols``: correct = equal ordered
    sequences, mutant = unequal (or equal-as-sets-only) if
    ``admission_basis`` lands in a different position between the fresh
    ``create_schema`` DDL and the migrated-from-v5 database. Only able to
    detect this because the migrated side genuinely lacked the column
    beforehand (``_assert_pregate_absent``)."""
    fresh_cols = _column_list(_fresh_db(), "daily_truth")
    assert "admission_basis" in fresh_cols
    # Plan §5 D8: appended last among columns, before the trailing UNIQUE
    # table constraint.
    assert fresh_cols[-1] == "admission_basis"

    conn = _v5_db()
    _assert_pregate_absent(conn)
    run_migrations(conn)
    migrated_cols = _column_list(conn, "daily_truth")

    assert migrated_cols == fresh_cols


# ---------------------------------------------------------------------------
# O14b -- migration is idempotent from a genuinely-v5 database.
# ---------------------------------------------------------------------------


def test_o14b_migration_from_v5_is_idempotent() -> None:
    """O14b -> at the second ``migrate_v6_...`` call: correct = no-op, the
    column stays present exactly once and ``user_version`` is unmoved by
    the second call; mutant = ``sqlite3.OperationalError: duplicate column
    name`` if the column-probe guard is dropped and the ``ALTER TABLE`` is
    reissued unconditionally against an already-migrated database (the
    'crash between ALTER and the PRAGMA write, then reboot' scenario the
    guard exists for)."""
    conn = _v5_db()
    _assert_pregate_absent(conn)
    run_migrations(conn)
    assert TARGET_USER_VERSION == 6
    user_version_after_first = int(conn.execute("PRAGMA user_version").fetchone()[0])
    assert user_version_after_first == 6
    cols_after_first = _column_list(conn, "daily_truth")
    assert cols_after_first.count("admission_basis") == 1

    # The pre-`run_migrations` guard assertion above is what makes the
    # first call above a real ALTER (a bare second call against a fresh
    # v5 fixture with no first call would not exercise the no-op branch
    # at all).
    migrate_v6_daily_truth_admission_basis(conn)

    assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == (
        user_version_after_first
    )
    cols_after_second = _column_list(conn, "daily_truth")
    assert cols_after_second == cols_after_first
    assert cols_after_second.count("admission_basis") == 1


# ---------------------------------------------------------------------------
# O14c -- CHECK constraint enforced identically on fresh and migrated
# schemas, tolerant of the whitespace divergence between the two DDL
# sources for the SAME predicate.
# ---------------------------------------------------------------------------
#
# create_schema's DDL wraps the CHECK predicate across three source lines
# (an 88-character line-length rule); migrate_v6's ALTER TABLE writes the
# identical predicate as one concatenated line. SQLite preserves the
# literal whitespace of the predicate text in its constraint-violation
# message, so a fresh database and a migrated database raise the SAME
# exception type with DIFFERENT message strings for the same rejected
# value. This is not a bug and the two DDL sources must not be edited to
# make the diagnostic text byte-identical -- whitespace preservation in an
# exception message is not a schema-semantic difference. The assertions
# below normalize whitespace / check for stable substrings instead of
# pinning either literal message.


@pytest.mark.parametrize("db_kind", ["fresh", "migrated"])
def test_o14c_check_constraint_rejects_invalid_and_accepts_valid(
    db_kind: str,
) -> None:
    """O14c -> at the raised ``sqlite3.IntegrityError``'s (whitespace-
    normalized) message: correct = contains 'CHECK constraint failed' and
    'admission_basis', mutant = a different, unrelated message (or no
    exception at all) if the ``CHECK`` clause is dropped from the
    migration's ``ALTER TABLE`` -- the easiest thing to lose, since
    ``ALTER TABLE ... ADD COLUMN`` accepts a bare ``TEXT`` happily.
    Detectable only when the migrated side was built by a real ALTER, not
    a database that already had the constraint from ``create_schema``."""
    if db_kind == "fresh":
        conn = _fresh_db()
    else:
        conn = _v5_db()
        _assert_pregate_absent(conn)
        run_migrations(conn)

    site_id = _seed_site(conn)
    generation_id = ensure_published_generation(conn, site_id)

    with pytest.raises(sqlite3.IntegrityError) as excinfo:
        _insert_daily_truth_row(
            conn, site_id, generation_id, "2026-06-01", "junk-value"
        )
    message = _normalize_whitespace(str(excinfo.value))
    assert "CHECK constraint failed" in message
    assert "admission_basis" in message
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM daily_truth WHERE local_date = '2026-06-01'"
        ).fetchone()[0]
        == 0
    )

    for offset, value in enumerate((None, ADMISSION_COMPLETE, ADMISSION_DEADLINE)):
        _insert_daily_truth_row(
            conn, site_id, generation_id, f"2026-06-{2 + offset:02d}", value
        )
    rows = conn.execute(
        "SELECT admission_basis FROM daily_truth ORDER BY local_date"
    ).fetchall()
    assert [r["admission_basis"] for r in rows] == [
        None,
        ADMISSION_COMPLETE,
        ADMISSION_DEADLINE,
    ]


# ---------------------------------------------------------------------------
# O15 / O15b -- neither fingerprint moves across the migration, whether
# admission_basis stays NULL or is later populated.
# ---------------------------------------------------------------------------

_HORIZON_DATES = ["2026-06-01", "2026-06-02", "2026-06-03"]


def _seed_pre_gate_horizon(
    conn: sqlite3.Connection, site_id: int, generation_id: int
) -> None:
    for local_date in _HORIZON_DATES:
        _insert_pre_gate_truth_row(conn, site_id, generation_id, local_date)
    conn.commit()


def test_o15_fingerprints_byte_identical_across_migration() -> None:
    """O15 -> at ``(before_input, before_basis) == (after_input,
    after_basis)``: correct = byte-identical, mutant = a moved digest if
    either ``SELECT`` inside ``input_fingerprint`` /
    ``result_basis_fingerprint`` starts reading ``admission_basis`` after
    the v6 migration -- which would move every published run's digest and
    light the freshness warning across the whole history (D11, S6).

    The ``snapshot_after == snapshot`` assertion immediately before the
    digest comparison is a separator: it fails at the named
    reconciliation key (e.g. ``roster``) instead of at an opaque digest
    mismatch when the fixture's pre-seeded reconciliation calls
    (``_v5_db``) fall out of sync with what ``run_migrations`` actually
    does unconditionally on every boot -- and it is what turns a future
    fifth such call into a loud, attributable failure here instead of a
    silently-adapted fixture."""
    conn = _v5_db()
    site_id = _seed_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    _seed_pre_gate_horizon(conn, site_id, generation_id)

    snapshot = capture_config_snapshot(conn, site_id)
    before_input = input_fingerprint(conn, site_id, snapshot)
    before_basis = result_basis_fingerprint(
        conn,
        site_id,
        snapshot,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )
    _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        fingerprint=before_input,
        basis=before_basis,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )

    _assert_pregate_absent(conn)
    run_migrations(conn)

    snapshot_after = capture_config_snapshot(conn, site_id)
    after_input = input_fingerprint(conn, site_id, snapshot_after)
    after_basis = result_basis_fingerprint(
        conn,
        site_id,
        snapshot_after,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )

    assert snapshot_after == snapshot
    assert after_input == before_input
    assert after_basis == before_basis


def test_o15b_fingerprints_unchanged_when_admission_basis_populated() -> None:
    """O15b -> at ``(mid_input, mid_basis) == (after_input, after_basis)``
    once a row inside the horizon carries a non-NULL ``admission_basis``:
    correct = still byte-identical, mutant = a moved digest. Proves O15
    passes because the column is excluded from both ``SELECT``s, not
    merely because every row happened to read NULL."""
    conn = _v5_db()
    site_id = _seed_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    _seed_pre_gate_horizon(conn, site_id, generation_id)

    snapshot = capture_config_snapshot(conn, site_id)
    input_before_migration = input_fingerprint(conn, site_id, snapshot)
    basis_before_migration = result_basis_fingerprint(
        conn,
        site_id,
        snapshot,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )
    _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        fingerprint=input_before_migration,
        basis=basis_before_migration,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )

    _assert_pregate_absent(conn)
    run_migrations(conn)

    snapshot_mid = capture_config_snapshot(conn, site_id)
    mid_input = input_fingerprint(conn, site_id, snapshot_mid)
    mid_basis = result_basis_fingerprint(
        conn,
        site_id,
        snapshot_mid,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )

    conn.execute(
        "UPDATE daily_truth SET admission_basis = 'deadline' "
        "WHERE site_id = ? AND local_date = ?",
        (site_id, _HORIZON_DATES[1]),
    )
    conn.commit()

    snapshot_after = capture_config_snapshot(conn, site_id)
    after_input = input_fingerprint(conn, site_id, snapshot_after)
    after_basis = result_basis_fingerprint(
        conn,
        site_id,
        snapshot_after,
        period_start=_HORIZON_DATES[0],
        period_end=_HORIZON_DATES[-1],
    )

    assert after_input == mid_input
    assert after_basis == mid_basis
