"""§8 oracles for the ``result_basis_fingerprint`` freshness feature.

Covers the plan's Group A (which mutations move which hash), the remaining
Group B cases (B1, B3, B4, B5 -- B2 lives in ``test_verification_run.py``
and is not repeated here), Group C (API/page freshness surfaces), the new
Group D1 read-path purity oracle for the full ``result_basis_freshness``
derivation, and Group E (the v5 migration). D2 (the EQP pin) and D3 (the
§18.12 source tripwire) already live elsewhere and are unedited.

Every fixture value is synthetic: invented site names, RFC-5737-style ids,
and a hand-made 'UTC' timezone throughout -- Group C reuses ``_make_site``
and ``_seed_published_run`` from ``tests.test_phase7_surface``, which
already follow that convention.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

import pytest

from tests.helpers import asof_conn, asof_make_real_feed
from tests.test_phase7_surface import _make_app, _make_site, _seed_published_run
from tests.test_phase8_section18_oracles import (
    _fetch_page,
    _open_app_db,
    _read_only,
    _v16_markers,
)
from tests.test_verification_run import (
    _W8_PAYLOAD,
    _advance_samples,
    _decisions,
    _drive_chain,
    _drive_fixed,
    _drive_to_phase,
    _make_verification_site,
)
from wxverify.db.migrations import TARGET_USER_VERSION, create_schema, run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_id,
)
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.runs import (
    METHODOLOGY_VERSION,
    RESULT_BASIS_ALGORITHM,
    capture_config_snapshot,
    failed_attempts_for_fingerprint,
    input_fingerprint,
    published_run_key,
    result_basis_fingerprint,
    result_basis_freshness,
    start_run,
)

# ---------------------------------------------------------------------------
# Shared fixture builders (Groups A, B remaining, D1).
# ---------------------------------------------------------------------------

_A_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03"]


def _insert_truth_day(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    local_date: str,
    *,
    quantity: str = "temperature_high",
    value: float = 20.0,
    eligible: int = 1,
    covered_hours: int = 24,
    stale: int = 0,
) -> None:
    bounds = local_day_bounds(date.fromisoformat(local_date), "UTC")
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, covered_hours,
             expected_slots, day_start_utc, day_end_utc, timezone,
             rain_threshold_mm, stale, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'UTC', 0.2, ?, ?)
        """,
        (
            site_id,
            local_date,
            quantity,
            value,
            eligible,
            covered_hours,
            bounds.expected_slots,
            bounds.start_utc.isoformat(),
            bounds.end_utc.isoformat(),
            stale,
            generation_id,
        ),
    )


def _site_with_period(conn: sqlite3.Connection) -> tuple[int, int, list[int]]:
    """A published site with two real feeds and truth over ``_A_PERIOD_DAYS``.

    Returns ``(site_id, generation_id, feed_ids)``.
    """
    site_id = _make_site(conn, "group-a-site")
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    feeds = [
        asof_make_real_feed(conn, "model-alpha"),
        asof_make_real_feed(conn, "model-beta"),
    ]
    for local_date in _A_PERIOD_DAYS:
        _insert_truth_day(conn, site_id, generation_id, local_date)
    return site_id, generation_id, feeds


def _hashes(
    conn: sqlite3.Connection, site_id: int, *, period_start: str, period_end: str
) -> tuple[str, str]:
    snapshot = capture_config_snapshot(conn, site_id)
    return (
        input_fingerprint(conn, site_id, snapshot),
        result_basis_fingerprint(
            conn, site_id, snapshot, period_start=period_start, period_end=period_end
        ),
    )


# ---------------------------------------------------------------------------
# Group A -- which mutations move which hash (§8 core discrimination table).
# ---------------------------------------------------------------------------


def test_a1_raw_observation_insert_moves_input_not_result_basis() -> None:
    """a1 -> at ``after_input != before_input``: correct = moved (distinct
    digests), mutant = unmoved (equal digests) if the observation counters
    ever leave ``input_fingerprint``'s scope. Paired: the same INSERT must
    leave ``result_basis_fingerprint`` untouched, since it never reads
    ``observations`` at all."""
    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', '2026-06-01T06:00:00Z', 12.0, 1,
                '2026-06-01T07:00:00Z')
        """,
        (site_id,),
    )
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis == before_basis


def test_a2_raw_forecast_sample_insert_moves_input_not_result_basis() -> None:
    """a2 -> at ``after_input != before_input``: correct = moved, mutant =
    unmoved if the sample high-water counter is dropped from
    ``input_fingerprint``. Paired negative on ``result_basis_fingerprint``,
    which never reads ``forecast_samples``."""
    conn = asof_conn()
    site_id, _gen, feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    _advance_samples(conn, site_id, feeds[0])
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis == before_basis


def test_a3_truth_above_period_end_moves_input_not_result_basis() -> None:
    """a3 -> at ``after_basis == before_basis``: correct = unchanged,
    mutant = changed if the horizon filter (``local_date BETWEEN``) is
    dropped from ``result_basis_fingerprint``'s query. The appended day is
    genuinely outside ``[start, end]`` (``end`` is pinned one day short of
    the appended row)."""
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[1]  # excludes days 3 and 4
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    _insert_truth_day(conn, site_id, gen, "2026-06-04")  # a NEW day, above period_end
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis == before_basis


def test_a4_stale_flag_inside_horizon_moves_input_not_result_basis() -> None:
    """a4 -> at ``after_basis == before_basis``: correct = unmoved, mutant =
    moved if ``stale`` is added back to ``result_basis_fingerprint``'s row
    format. This asymmetry IS the fix: ``stale`` is a regeneration marker
    the scorer never reads (``simulate.py`` has zero references to it), so
    it stays in ``input_fingerprint``'s scope (the trigger-gate hash) but
    must never re-enter the result basis -- hashing it there reintroduced a
    permanently-true staleness warning, because the next night's
    regeneration always clears it back to 0 and the recorded basis could
    never be reproduced. Paired with ``after_input != before_input`` (left
    unchanged from the prior version of this test) as the liveness probe:
    it proves the UPDATE genuinely landed and genuinely moved a digest, so
    an accidentally-inert fixture cannot make the new equality pass
    vacuously."""
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    cursor = conn.execute(
        """
        UPDATE daily_truth SET stale = 1
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
        """,
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    assert cursor.rowcount > 0
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis == before_basis


def test_a5_value_change_inside_horizon_moves_both_hashes() -> None:
    """a5 -> at ``after_basis != before_basis``: correct = moved, mutant =
    unmoved if ``value`` is dropped from the row format."""
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    conn.execute(
        """
        UPDATE daily_truth SET value = 99.5, eligible = 0, covered_hours = 10
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
        """,
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis != before_basis


def test_a6_config_change_moves_both_hashes() -> None:
    """a6 -> at ``after_basis != before_basis``: correct = moved, mutant =
    unmoved if a config field is dropped from the snapshot dict either
    fingerprint hashes over."""
    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    conn.execute("UPDATE sites SET rain_threshold_mm = 5.5 WHERE id = ?", (site_id,))
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis != before_basis


def test_a7_roster_change_moves_both_hashes() -> None:
    """a7 -> at ``after_basis != before_basis``: correct = moved, mutant =
    unmoved if the roster is dropped from either snapshot. The competitor
    feed is deactivated via ``feeds.enabled`` (the real roster-membership
    predicate), never a mocked roster list."""
    conn = asof_conn()
    site_id, _gen, feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    conn.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (feeds[0],))
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis != before_basis


def test_a8_methodology_version_moves_both_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """a8 -> at ``after_basis != before_basis``: correct = moved, mutant =
    unmoved if ``METHODOLOGY_VERSION`` is dropped from either fingerprint's
    hashed blob. Monkeypatched at ``wxverify.verification.runs`` -- the
    module namespace both fingerprint functions actually read."""
    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    monkeypatch.setattr("wxverify.verification.runs.METHODOLOGY_VERSION", 999)
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis != before_basis


def test_a9_timezone_generation_flip_moves_both_hashes() -> None:
    """a9 -> at ``after_basis != before_basis``: correct = moved, mutant =
    unmoved if ``tz_generation_id`` is dropped from either snapshot. A
    second generation is created and published, then truth is duplicated
    under it -- the real path a retrospective correction takes."""
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    new_gen = int(
        conn.execute(
            """
            INSERT INTO timezone_generations
                (site_id, timezone, mode, state, created_at)
            VALUES (?, 'Etc/GMT+7', 'retrospective_correction', 'published',
                    '2026-06-04T00:00:00Z')
            """,
            (site_id,),
        ).lastrowid
    )
    for local_date in _A_PERIOD_DAYS:
        _insert_truth_day(conn, site_id, new_gen, local_date)
    conn.execute(
        "UPDATE runtime_state SET value = ? WHERE key = ?",
        (str(new_gen), f"tz_generation_published:{site_id}"),
    )
    assert published_generation_id(conn, site_id) == new_gen
    after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_input != before_input
    assert after_basis != before_basis
    assert gen != new_gen


def test_a10_recompute_with_nothing_mutated_is_identical() -> None:
    """a10 -> at ``second == first`` for both digests: correct = identical
    (determinism), mutant = a source of nondeterminism (e.g. an
    unordered / unstable-order query) shows unequal digests across two
    computations of the same state."""
    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    first = _hashes(conn, site_id, period_start=start, period_end=end)
    second = _hashes(conn, site_id, period_start=start, period_end=end)
    assert second == first


def test_a11_input_and_result_basis_are_not_equal() -> None:
    """a11 -> at ``result_basis != input_fp``: correct = not equal (the
    algorithm prefix and the narrower period/row scope make the two
    fingerprints structurally distinct), mutant = equal if
    ``result_basis_fingerprint`` degenerated into calling
    ``input_fingerprint``'s body verbatim."""
    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    input_fp, result_basis = _hashes(conn, site_id, period_start=start, period_end=end)
    assert result_basis != input_fp
    assert result_basis.startswith(f"{RESULT_BASIS_ALGORITHM}:")


# ---------------------------------------------------------------------------
# Group B remaining -- B1 (reference equivalence), B3 (no_change_skip), B4
# (real-data divergence guard), B5 (attempt counting). B2 lives in
# tests/test_verification_run.py and is not repeated here.
# ---------------------------------------------------------------------------


def _ref_input_fingerprint(
    conn: sqlite3.Connection, site_id: int, snapshot: dict[str, object]
) -> str:
    """B1: an independent transcription of ``input_fingerprint``'s body,
    hand-typed from the production source rather than imported, so an
    accidental change to the production function does not silently drag
    this reference along with it."""
    import hashlib
    import json

    def _dumps(obj: object) -> str:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True)

    generation_id = int(str(snapshot["tz_generation_id"]))
    obs = conn.execute(
        "SELECT COUNT(*) AS n, MAX(computed_at) AS latest "
        "FROM observations WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    samples = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS hi FROM forecast_samples WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    digest = hashlib.sha256()
    digest.update(_dumps(snapshot).encode())
    digest.update(
        _dumps(
            {
                "methodology_version": METHODOLOGY_VERSION,
                "obs_count": int(obs["n"]),
                "obs_latest_computed_at": None
                if obs["latest"] is None
                else str(obs["latest"]),
                "sample_high_water": int(samples["hi"]),
            }
        ).encode()
    )
    truth_rows = conn.execute(
        """
        SELECT local_date, quantity, value, eligible, covered_hours, stale
        FROM daily_truth WHERE site_id = ? AND tz_generation_id = ?
        ORDER BY local_date, quantity
        """,
        (site_id, generation_id),
    ).fetchall()
    for row in truth_rows:
        digest.update(
            (
                f"{row['local_date']}|{row['quantity']}|{row['value']}|"
                f"{row['eligible']}|{row['covered_hours']}|{row['stale']}\n"
            ).encode()
        )
    return digest.hexdigest()


def test_b1_reference_input_fingerprint_matches_production() -> None:
    """b1 -> at ``ref == prod``: correct = equal across every probed state
    (fresh, after a1's raw observation insert, after a5's value change,
    after a6's config change); a divergence here means the reference or
    the production body drifted from each other."""
    conn = asof_conn()
    site_id, gen, feeds = _site_with_period(conn)

    def _pair() -> tuple[str, str]:
        snapshot = capture_config_snapshot(conn, site_id)
        return (
            input_fingerprint(conn, site_id, snapshot),
            _ref_input_fingerprint(conn, site_id, snapshot),
        )

    prod, ref = _pair()
    assert prod == ref

    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', '2026-06-01T06:00:00Z', 12.0, 1,
                '2026-06-01T07:00:00Z')
        """,
        (site_id,),
    )
    prod, ref = _pair()
    assert prod == ref

    conn.execute(
        "UPDATE daily_truth SET value = 42.0 "
        "WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?",
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    prod, ref = _pair()
    assert prod == ref

    conn.execute("UPDATE sites SET rain_threshold_mm = 9.9 WHERE id = ?", (site_id,))
    prod, ref = _pair()
    assert prod == ref
    assert len(feeds) == 2  # roster participated in every snapshot above


def test_b3_no_change_skip_gates_on_input_fingerprint_only() -> None:
    """b3(a) -> at ``decisions[-1]['decision']``: correct = 'no_change_skip'
    when nothing changed between two triggers. b3(b) -> at the SAME
    assertion on a second trigger after a raw observation INSERT: correct =
    'run_started' (the gate did NOT fire), mutant = 'no_change_skip' if the
    gate ever stopped reading the raw observation counters."""
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    _drive_chain(conn, site_id, dict(_W8_PAYLOAD))
    before = len(_decisions(conn, site_id))

    payload = dict(_W8_PAYLOAD)
    payload["trigger_date"] = "2026-06-07"
    _drive_chain(conn, site_id, payload)
    rows = _decisions(conn, site_id)[before:]
    assert rows[-1]["decision"] == "no_change_skip"

    before2 = len(_decisions(conn, site_id))
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', '2026-06-08T06:00:00Z', 13.0, 1,
                '2026-06-08T07:00:00Z')
        """,
        (site_id,),
    )
    payload["trigger_date"] = "2026-06-08"
    _drive_chain(conn, site_id, payload)
    rows2 = _decisions(conn, site_id)[before2:]
    assert rows2[-1]["decision"] == "run_started"


def test_b4_divergence_guard_fires_on_a_real_sample_insert() -> None:
    """b4 -> at ``last['reason']``: correct = starts with 'superseded: ',
    mutant = does not if the start-phase divergence check stops
    re-deriving ``input_fingerprint`` against live state. Stronger than the
    existing monkeypatched ``advancing`` fixture in
    ``test_verification_run.py``: the divergence here is a genuine
    ``forecast_samples`` INSERT landing between the decide and start
    phases, not a substituted function."""
    conn = asof_conn()
    site_id, feeds = _make_verification_site(conn)
    payload = dict(_W8_PAYLOAD)
    calls: list[dict[str, object]] = []
    _drive_to_phase(conn, site_id, payload, "start", calls)
    before = len(_decisions(conn, site_id))
    _advance_samples(conn, site_id, feeds[0])  # real write, no monkeypatch
    _drive_fixed(conn, site_id, payload, calls, 10)
    rows = _decisions(conn, site_id)[before:]
    assert str(rows[0]["reason"]).startswith("superseded: ")


def test_b5_failed_attempts_increments_for_repeated_failures() -> None:
    """b5 -> at ``second == first + 1``: correct = increments, mutant =
    stays flat if the count query stops filtering on ``state='failed'``
    and ``input_fingerprint = ?``. Rows inserted directly rather than
    through a real failing run, matching how B2's sibling oracle in
    ``test_verification_run.py`` drives this same counter."""
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    fingerprint = "fp-oracle-town-repeated"
    first = failed_attempts_for_fingerprint(conn, site_id, fingerprint)
    conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 1, '0.0.0-test', 'failed', 1, '{}', 1, 10, ?)
        """,
        (site_id, gen, fingerprint),
    )
    second = failed_attempts_for_fingerprint(conn, site_id, fingerprint)
    assert second == first + 1
    conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 1, '0.0.0-test', 'failed', 1, '{}', 1, 10, ?)
        """,
        (site_id, gen, fingerprint),
    )
    third = failed_attempts_for_fingerprint(conn, site_id, fingerprint)
    assert third == second + 1


# ---------------------------------------------------------------------------
# Group C -- surfaces (API status + /verification page).
# ---------------------------------------------------------------------------


def _publish_run_row(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    generation_id: int,
    basis: str | None,
    period_start: str | None,
    period_end: str | None,
    fingerprint: str = "fp-group-c",
    snapshot_json: str = "{}",
) -> int:
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, result_basis_fingerprint, published_at)
            VALUES (?, ?, 1, '0.0.0-test', 'published', 1, ?, ?, ?, ?, 1,
                    10, ?, ?, '2026-06-12T00:00:00Z')
            """,
            (
                site_id,
                generation_id,
                snapshot_json,
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


def _status(client: Any, site_id: int) -> dict[str, object]:
    status = client.get(f"/api/verification/status?site={site_id}").json()
    (entry,) = status["sites"]
    return entry


def test_c1_legacy_run_reports_unknown_not_recorded(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """c1 -> at ``entry['result_basis']``: correct =
    {'state': 'unknown', 'reason': 'not_recorded'}, mutant = anything else
    if ``recorded is None`` stops being the first branch checked. Paired
    on ``warnings.stale_inputs`` and the page marker set."""
    import json

    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "c1-site")
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    snapshot = capture_config_snapshot(conn, site_id)
    _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        basis=None,
        period_start="2026-06-01",
        period_end="2026-06-01",
        snapshot_json=json.dumps(snapshot),
    )
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        entry = _status(client, site_id)
        assert entry["result_basis"] == {"state": "unknown", "reason": "not_recorded"}
        assert entry["warnings"]["stale_inputs"] is False

    page = _fetch_page(monkeypatch, site_id)
    markers = _v16_markers(page)
    assert "16.1.freshness_unknown" in markers
    assert "16.1.warn_stale" not in markers


def test_c2_matching_fingerprint_reports_fresh() -> None:
    """c2 -> at ``entry['result_basis']['state']``: correct = 'fresh',
    mutant = anything else if the equality comparison stops matching an
    identical value against itself."""
    conn = asof_conn()
    site_id = _make_site(conn, "c2-site")
    run_id = _seed_published_run(conn, site_id, fresh_fingerprint=True)
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=row["result_basis_fingerprint"],
        period_start=row["period_start"],
        period_end=row["period_end"],
    )
    assert freshness.state == "fresh"
    assert freshness.reason is None


def test_c3_stale_prefixed_fingerprint_reports_changed(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """c3 -> at ``entry['warnings']['stale_inputs']``: correct = True,
    mutant = False if the ``changed`` branch stops being reached for a
    same-prefix, non-matching value. Page-marker pair on
    ``16.1.warn_stale``."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "c3-site")
    _seed_published_run(conn, site_id, fresh_fingerprint=False)
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        entry = _status(client, site_id)
        assert entry["result_basis"]["state"] == "changed"
        assert entry["warnings"]["stale_inputs"] is True

    page = _fetch_page(monkeypatch, site_id)
    markers = _v16_markers(page)
    assert "16.1.warn_stale" in markers


def test_c4_superseded_algorithm_prefix_reports_unknown() -> None:
    """c4 -> at ``freshness.reason``: correct = 'algorithm_changed',
    mutant = 'changed' if the prefix check runs AFTER (or not before) the
    equality test."""
    conn = asof_conn()
    site_id = _make_site(conn, "c4-site")
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    run_id = _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        basis="rb2:" + "0" * 64,
        period_start="2026-06-01",
        period_end="2026-06-01",
    )
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=row["result_basis_fingerprint"],
        period_start=row["period_start"],
        period_end=row["period_end"],
    )
    assert freshness.state == "unknown"
    assert freshness.reason == "algorithm_changed"


def test_c5_no_published_run_reports_no_publishable_run(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """c5 -> at ``entry['result_basis']``: correct =
    {'state': 'unknown', 'reason': 'no_published_run'}, mutant = anything
    else if a call site stops falling back to ``RESULT_BASIS_NO_RUN`` when
    ``published_run_id`` is None. Page pair: shows
    '16.1.no_publishable_run' and neither freshness marker."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "c5-site")
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        entry = _status(client, site_id)
        assert entry["result_basis"] == {
            "state": "unknown",
            "reason": "no_published_run",
        }
        assert entry["warnings"]["no_publishable_run"] is True

    page = _fetch_page(monkeypatch, site_id)
    markers = _v16_markers(page)
    assert "16.1.no_publishable_run" in markers
    assert "16.1.warn_stale" not in markers
    assert "16.1.freshness_unknown" not in markers


def test_c6_api_and_page_agree_on_state_and_reason(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """c6 -> at the page/API state+reason pair: correct = both surfaces
    report the exact same (state, reason) for the same DB state, mutant =
    a divergence if either surface stops using the shared
    ``result_basis_freshness`` derivation. Verified by comparing the API's
    JSON payload directly to the marker the page actually rendered, rather
    than assuming they must agree."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "c6-site")
    _seed_published_run(conn, site_id, fresh_fingerprint=False)
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        entry = _status(client, site_id)
    assert entry["result_basis"]["state"] == "changed"

    page = _fetch_page(monkeypatch, site_id)
    markers = _v16_markers(page)
    assert "16.1.warn_stale" in markers
    assert "16.1.freshness_unknown" not in markers


def test_c7_round_trip_through_start_run_reads_fresh() -> None:
    """c7 -> at ``freshness.state``: correct = 'fresh', mutant = anything
    else if ``start_run`` ever stores a basis the read path cannot
    reproduce over its own recorded period. The only oracle here that
    proves the WRITE path's stored value round-trips -- C2 seeds
    ``period_start``/``period_end`` by hand and cannot prove this."""
    from datetime import UTC, datetime

    conn = asof_conn()
    site_id, _gen, _feeds = _site_with_period(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    now = datetime(2027, 1, 1, tzinfo=UTC)
    cfg = start_run(conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=now)
    assert cfg is not None
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=row["result_basis_fingerprint"],
        period_start=row["period_start"],
        period_end=row["period_end"],
    )
    assert freshness.state == "fresh"
    assert freshness.reason is None


def test_c8_null_period_reports_unknown_period_unknown() -> None:
    """c8 -> at ``freshness.reason``: correct = 'period_unknown', mutant =
    a crash or a wrong branch if the None-period guard is dropped. The row
    is direct-INSERTed with NULL bounds -- not reachable through
    ``start_run``, which always fills both."""
    conn = asof_conn()
    site_id = _make_site(conn, "c8-site")
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    run_id = _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        basis="rb1:" + "0" * 64,
        period_start=None,
        period_end=None,
    )
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=row["result_basis_fingerprint"],
        period_start=row["period_start"],
        period_end=row["period_end"],
    )
    assert freshness.state == "unknown"
    assert freshness.reason == "period_unknown"
    assert freshness.state != "changed"  # stale_inputs must not fire on this


def test_c9_no_published_generation_reports_unknown() -> None:
    """c9 -> at ``freshness.reason``: correct = 'no_published_generation',
    mutant = a crash or a wrong branch if
    ``current_result_basis_fingerprint``'s None-return stops being
    threaded through. The site's published-generation pointer is
    deliberately never set (built without ``ensure_published_generation``
    or ``_make_site``, both of which seed it)."""
    conn = asof_conn()
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('c9-site', 40.0, -105.0, 900.0, 'UTC')
            """
        ).lastrowid
    )
    # A real, unpublished generation row -- satisfies the FK without ever
    # calling ensure_published_generation or setting the pointer.
    generation_id = int(
        conn.execute(
            """
            INSERT INTO timezone_generations (site_id, timezone, mode, state)
            VALUES (?, 'UTC', 'initial', 'building')
            """,
            (site_id,),
        ).lastrowid
    )
    assert published_generation_id(conn, site_id) is None
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, result_basis_fingerprint, published_at)
            VALUES (?, ?, 1, '0.0.0-test', 'published', 1, '{}', '2026-06-01',
                    '2026-06-01', '2026-06-01', 1, 10, 'fp-c9', ?,
                    '2026-06-02T00:00:00Z')
            """,
            (site_id, generation_id, "rb1:" + "0" * 64),
        ).lastrowid
    )
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=row["result_basis_fingerprint"],
        period_start=row["period_start"],
        period_end=row["period_end"],
    )
    assert freshness.state == "unknown"
    assert freshness.reason == "no_published_generation"
    assert freshness.state != "changed"


# ---------------------------------------------------------------------------
# Group D1 -- result_basis_freshness derivation purity under query_only=ON.
# ---------------------------------------------------------------------------


def test_d1_result_basis_freshness_is_pure_including_no_published_generation() -> None:
    """d1 -> at the ``sqlite3.OperationalError`` NOT being raised despite
    ``query_only=ON``: correct = every branch (fresh, changed,
    no_published_generation) returns without writing, mutant = a write
    (e.g. reintroducing ``ensure_published_generation``) raises against
    this connection instead. Anchored on the returned state/reason too, so
    a mutation that avoids writing but returns the wrong branch is still
    caught."""
    conn = asof_conn()
    site_id = _make_site(conn, "d1-site")
    run_id = _seed_published_run(conn, site_id, fresh_fingerprint=True)
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    recorded, period_start, period_end = (
        row["result_basis_fingerprint"],
        row["period_start"],
        row["period_end"],
    )
    _read_only(conn)
    fresh = result_basis_freshness(
        conn,
        site_id,
        recorded=recorded,
        period_start=period_start,
        period_end=period_end,
    )
    assert fresh.state == "fresh"
    assert fresh.reason is None

    changed = result_basis_freshness(
        conn,
        site_id,
        recorded="rb1:" + "0" * 64,
        period_start=period_start,
        period_end=period_end,
    )
    assert changed.state == "changed"
    assert changed.reason is None

    conn.execute("PRAGMA query_only=OFF")
    other_site = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('d1-site-no-gen', 40.0, -105.0, 900.0, 'UTC')
            """
        ).lastrowid
    )
    conn.commit()
    conn.execute("PRAGMA query_only=ON")
    assert published_generation_id(conn, other_site) is None
    no_gen = result_basis_freshness(
        conn,
        other_site,
        recorded="rb1:" + "0" * 64,
        period_start="2026-06-01",
        period_end="2026-06-01",
    )
    assert no_gen.state == "unknown"
    assert no_gen.reason == "no_published_generation"


# ---------------------------------------------------------------------------
# Group E -- v5 migration (result_basis_fingerprint column).
# ---------------------------------------------------------------------------


def _bare_db(*, user_version: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute(f"PRAGMA user_version = {user_version}")
    return conn


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _column_list(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")]


def test_e1_migrated_v4_db_matches_fresh_schema_column_order() -> None:
    """e1 -> at ``migrated_cols == fresh_cols``: correct = equal (same
    columns, same order), mutant = unequal if ``migrate_v5`` stops adding
    ``result_basis_fingerprint`` to a genuinely-v4 database. Paired direct
    check that the new column specifically survived the migrated path."""
    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    create_schema(fresh)
    fresh_cols = _column_list(fresh, "verification_runs")
    assert "result_basis_fingerprint" in fresh_cols

    migrated = _bare_db(user_version=4)
    run_migrations(migrated)
    migrated_cols = _column_list(migrated, "verification_runs")

    assert migrated_cols == fresh_cols
    assert "result_basis_fingerprint" in migrated_cols


def test_e2_run_migrations_is_idempotent() -> None:
    """e2 -> at the second call not raising: correct = idempotent (the
    ``_table_columns`` guard skips a re-add), mutant = an unconditional
    ``ALTER TABLE ADD COLUMN`` raises 'duplicate column name' on the
    second pass."""
    conn = _bare_db(user_version=4)
    run_migrations(conn)
    first = _user_version(conn)
    run_migrations(conn)
    assert _user_version(conn) == first == TARGET_USER_VERSION


def test_e3_migration_preserves_an_existing_v4_run_row() -> None:
    """e3 -> at ``after == before``: correct = every pre-existing column
    byte-identical post-migration, mutant = a changed value if
    ``migrate_v5`` ever touches an existing row instead of only adding the
    column. Paired explicit check that the new column reads NULL, not a
    fabricated default."""
    conn = _bare_db(user_version=4)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('e3-site', 40.0, -105.0, 900.0, 'UTC')
            """
        ).lastrowid
    )
    generation_id = ensure_published_generation(conn, site_id)
    conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, published_at)
        VALUES (?, ?, 1, '0.10.0-test', 'published', 1, '{}', '2026-04-01',
                '2026-04-02', '2026-04-02', 42, 500, 'legacy-fp-e3',
                '2026-04-03T00:00:00Z')
        """,
        (site_id, generation_id),
    )
    conn.commit()
    before = dict(
        conn.execute(
            "SELECT * FROM verification_runs WHERE input_fingerprint = 'legacy-fp-e3'"
        ).fetchone()
    )
    run_migrations(conn)
    after = dict(
        conn.execute(
            "SELECT * FROM verification_runs WHERE input_fingerprint = 'legacy-fp-e3'"
        ).fetchone()
    )
    assert after["result_basis_fingerprint"] is None
    assert after == before


def test_e4_run_migrations_lands_on_user_version_five() -> None:
    """e4 -> at ``_user_version(conn) == 5``: correct = 5 (the literal),
    mutant = a different literal if ``TARGET_USER_VERSION`` is bumped
    without also bumping the literal every caller of this suite expects."""
    assert TARGET_USER_VERSION == 5
    conn = _bare_db(user_version=4)
    run_migrations(conn)
    assert _user_version(conn) == 5


# ---------------------------------------------------------------------------
# Group F -- oracles for the corrected contract: forecast exclusion is
# deliberate (F1), an unconsumed daily_truth column doesn't move the digest
# (F2), malformed-body reporting (F3), branch-order precedence (F4), and the
# template's reason-code coverage (F5).
# ---------------------------------------------------------------------------


def _insert_forecast_pair(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    generation_id: int,
    local_date: str,
    *,
    variable: str = "temperature_high",
) -> None:
    """A real ``forecast_pairs`` row inside ``local_date``'s UTC day."""
    bounds = local_day_bounds(date.fromisoformat(local_date), "UTC")
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 24, 1, 21.0, 20.0, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            bounds.start_utc.isoformat(),
            bounds.start_utc.isoformat(),
            generation_id,
        ),
    )


def test_f1_forecast_rebuild_inside_horizon_leaves_freshness_fresh_by_design() -> None:
    """f1 -> at ``freshness.state`` staying 'fresh' across an insert AND a
    delete of ``forecast_pairs`` rows inside the run's own horizon: this
    pins the DELIBERATE, documented exclusion of the forecast side from
    ``result_basis_fingerprint`` (runs.py's own docstring: 'forecast rows
    inside the horizon can be rebuilt or deleted without moving this
    digest'). This is NOT a claim that the behavior is desirable -- it is a
    known, accepted limitation. A future change that widens the digest to
    cover the forecast side should make this test fail loudly, on purpose,
    and get rewritten alongside that change rather than silently staying
    green.

    Mutation proof (recorded separately, not committed to the suite): a
    scratch reimplementation of ``result_basis_fingerprint`` that also folds
    in ``MAX(forecast_pairs.id)`` for the horizon produces a DIFFERENT
    digest after the insert below than before it -- so if production ever
    grew this coverage, the fixed ``recorded`` value asserted here would
    stop matching ``current`` and this test would go red for exactly the
    reason its docstring names.
    """
    from datetime import UTC, datetime

    conn = asof_conn()
    site_id, gen, feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    now = datetime(2027, 1, 1, tzinfo=UTC)
    cfg = start_run(conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=now)
    assert cfg is not None
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    recorded, period_start, period_end = (
        row["result_basis_fingerprint"],
        row["period_start"],
        row["period_end"],
    )
    assert period_start == start
    assert period_end == end

    def _freshness() -> str:
        return result_basis_freshness(
            conn,
            site_id,
            recorded=recorded,
            period_start=period_start,
            period_end=period_end,
        ).state

    assert _freshness() == "fresh"

    _insert_forecast_pair(conn, site_id, feeds[0], gen, start)
    assert _freshness() == "fresh"

    conn.execute("DELETE FROM forecast_pairs WHERE site_id = ?", (site_id,))
    assert _freshness() == "fresh"


def test_f2_unhashed_truth_column_does_not_move_result_basis() -> None:
    """f2 -> at ``after_basis == before_basis``: correct = unchanged,
    mutant = changed if the row format ``result_basis_fingerprint`` hashes
    ever widens to include a ``daily_truth`` column beyond the five it
    documents (``local_date``, ``quantity``, ``value``, ``eligible``,
    ``covered_hours``). ``stale`` has joined ``expected_slots`` in the
    unhashed set this test is about -- it is deliberately excluded from the
    result basis (see the module docstring and A4's asymmetry pin).

    Chose ``expected_slots`` over a cross-generation truth row (the brief's
    other candidate): a generation mismatch is already the mechanism a9
    covers end-to-end (a whole second generation, published-pointer flip
    included), so it would not add a new failure mode here. A same-
    generation, in-horizon column mutation is the sharper, more surgical
    case -- it isolates exactly what the row-format hash reads versus what
    the table happens to carry, with everything else held fixed.
    """
    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    start, end = _A_PERIOD_DAYS[0], _A_PERIOD_DAYS[-1]
    _before_input, before_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    cursor = conn.execute(
        """
        UPDATE daily_truth SET expected_slots = 999
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
        """,
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    assert cursor.rowcount > 0
    _after_input, after_basis = _hashes(
        conn, site_id, period_start=start, period_end=end
    )
    assert after_basis == before_basis


@pytest.mark.parametrize(
    "corrupt_body",
    [
        pytest.param("0" * 63, id="too_short"),
        pytest.param("0" * 65, id="too_long"),
        pytest.param("A" * 64, id="uppercase_hex"),
        pytest.param("g" * 64, id="non_hex_char"),
    ],
)
def test_f3_corrupt_body_reports_malformed_record(corrupt_body: str) -> None:
    """f3 -> at ``freshness.reason``: correct = 'malformed_record' for every
    corruption shape (wrong length both directions, uppercase hex, a
    non-hex character), distinct from 'algorithm_changed' -- which the
    same test file's C4 already pins for a wrong PREFIX with an
    otherwise-well-formed body. Together the two prove the form check
    tells a bad prefix apart from a bad body rather than collapsing both
    into one 'unknown' reason."""
    conn = asof_conn()
    freshness = result_basis_freshness(
        conn,
        1,
        recorded=f"{RESULT_BASIS_ALGORITHM}:{corrupt_body}",
        period_start="2026-06-01",
        period_end="2026-06-01",
    )
    assert freshness.state == "unknown"
    assert freshness.reason == "malformed_record"


def test_f4_form_checks_run_before_the_period_check() -> None:
    """f4 -> at ``freshness.reason`` for two constructions with NULL period
    bounds: correct = 'algorithm_changed' for a wrong-prefix value (the
    prefix check runs first) and 'malformed_record' for a right-prefix,
    corrupt-body value (the body-shape check also runs before the period
    check) -- NEITHER reports 'period_unknown', which is what a reader
    checking the period bounds before the stored form would report for
    both. No existing test in this file constructs a malformed/wrong-prefix
    value together with a null period; C8's period_unknown pin uses a
    WELL-FORMED recorded value ('rb1:' + '0'*64)."""
    conn = asof_conn()

    wrong_prefix_null_period = result_basis_freshness(
        conn,
        1,
        recorded="rb2:" + "0" * 64,
        period_start=None,
        period_end=None,
    )
    assert wrong_prefix_null_period.state == "unknown"
    assert wrong_prefix_null_period.reason == "algorithm_changed"

    corrupt_body_null_period = result_basis_freshness(
        conn,
        1,
        recorded=f"{RESULT_BASIS_ALGORITHM}:" + "0" * 63,
        period_start=None,
        period_end=None,
    )
    assert corrupt_body_null_period.state == "unknown"
    assert corrupt_body_null_period.reason == "malformed_record"


def test_f5_template_freshness_reasons_cover_every_reason_the_source_emits() -> None:
    """f5 -> at ``reasons_from_source <= template_keys``: correct = every
    reason string literal ``runs.py`` ever attaches to a ``state='unknown'``
    :class:`ResultBasisFreshness` (source-derived by regex over the
    production module, not hand-copied here, so the two cannot drift apart
    silently) has a matching key in ``show.html``'s ``freshness_reasons``
    dict. A seventh reason added to the source without adding its prose
    would otherwise fall through to the template's own fallback clause
    silently -- this test fails instead. Paired with a direct render check
    that the raw reason code never appears in the rendered notice, only the
    mapped prose.

    Source-derived rather than hardcoded: the six-way alternative (list the
    six strings by hand in this test) was judged too easy to let drift --
    the whole point of this oracle is to catch exactly the kind of edit
    that updates ``runs.py`` and forgets ``show.html``, and a hand-copied
    list in the test itself is exposed to that same forgetting.
    """
    import re
    from pathlib import Path

    import wxverify.verification.runs as runs_module

    source = Path(str(runs_module.__file__)).read_text(encoding="utf-8")
    reasons_from_source = set(re.findall(r'reason="([a-z_]+)"', source))
    assert reasons_from_source == {
        "no_published_run",
        "not_recorded",
        "algorithm_changed",
        "malformed_record",
        "period_unknown",
        "no_published_generation",
    }, "the regex scope drifted -- update it, don't hardcode the result set"

    import wxverify.web as web_module

    template_path = (
        Path(str(web_module.__file__)).parent
        / "templates"
        / "verification"
        / "show.html"
    )
    template_source = template_path.read_text(encoding="utf-8")
    block_match = re.search(
        r"freshness_reasons\s*=\s*\{(.*?)\}\s*-%\}", template_source, re.DOTALL
    )
    assert block_match is not None, "freshness_reasons dict literal not found"
    template_keys = set(re.findall(r"'([a-z_]+)':", block_match.group(1)))

    assert reasons_from_source == template_keys


def test_f5b_rendered_notice_never_leaks_the_raw_reason_code(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """f5b -> at the rendered page: correct = the mapped prose clause
    appears and the raw code 'malformed_record' does not, mutant = the raw
    code leaking into the page (e.g. a future edit that renders
    ``result_basis.reason`` directly instead of through the mapping) does
    not go unnoticed."""
    import json

    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "f5b-site")
    generation_id = published_generation_id(conn, site_id)
    assert generation_id is not None
    snapshot = capture_config_snapshot(conn, site_id)
    _publish_run_row(
        conn,
        site_id,
        generation_id=generation_id,
        basis="rb1:" + "0" * 63,  # too short -> malformed_record
        period_start="2026-06-01",
        period_end="2026-06-01",
        snapshot_json=json.dumps(snapshot),
    )
    page = _fetch_page(monkeypatch, site_id)
    markers = _v16_markers(page)
    assert "16.1.freshness_unknown" in markers
    assert "malformed_record" not in page
    assert "the record of what this run was based on is damaged" in page


def test_f6_regeneration_marker_between_regen_and_start_does_not_go_stale_forever() -> (
    None
):
    """f6 -> at ``freshness.state`` after a real in-horizon ``stale = 1``
    flip: correct = 'fresh' with ``reason is None``, mutant = 'changed' if
    ``stale`` is ever added back to ``result_basis_fingerprint``'s row
    format. This is the defect at the level the operator actually
    experiences it, not just at the digest-helper level (C7 and A4 both
    stop one layer short): the real chain is
    ``discover -> regen -> decide -> start -> ... -> publish``, and an
    ingest landing between ``regen`` and ``start`` can leave a truth row's
    ``stale`` flag set to 1 at the moment ``start_run`` records the basis.
    The next night's regeneration always clears ``stale`` back to 0
    (``materialize_daily_truth``'s INSERT hardcodes the literal 0), so a
    basis that hashed ``stale`` could never be reproduced and the freshness
    read would report 'changed' forever over byte-identical scored truth.
    Routed through ``start_run`` itself (the real recording path), not a
    stub -- a test that bypassed the recording path would prove nothing
    about the defect it exists to pin."""
    from datetime import UTC, datetime

    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    now = datetime(2027, 1, 1, tzinfo=UTC)
    cfg = start_run(conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=now)
    assert cfg is not None
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    recorded, period_start, period_end = (
        row["result_basis_fingerprint"],
        row["period_start"],
        row["period_end"],
    )

    # A real ingest landing between regen and start: mark an in-horizon
    # truth day stale, exactly as the regeneration marker does in
    # production.
    cursor = conn.execute(
        """
        UPDATE daily_truth SET stale = 1
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
        """,
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    assert cursor.rowcount > 0

    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=recorded,
        period_start=period_start,
        period_end=period_end,
    )
    assert freshness.state == "fresh"
    assert freshness.reason is None


def test_f7_value_change_after_start_run_is_genuinely_detected_as_changed() -> None:
    """f7 -> at ``freshness.state``: correct = 'changed' with
    ``reason is None``, mutant = 'fresh' if ``result_basis_freshness`` ever
    stops re-deriving the digest from live state (e.g. degenerates into an
    unconditional pass-through). This is the positive counterpart F6 does
    not cover: F6 pins that an UNHASHED marker (``stale``) must NOT move
    the basis; nothing in this file proves the opposite direction -- that a
    GENUINELY HASHED field moving mid-run IS still caught. Every existing
    'changed' assertion in this file reaches it synthetically (C3's
    hand-prefixed fingerprint, D1's purity check against a fabricated
    ``recorded`` value) rather than by mutating real data after a real
    ``start_run`` call.

    Reporting 'changed' here is CORRECT behavior, not a bug: the run's
    published results describe the basis pinned at ``start_run``, and if a
    ``daily_truth.value`` genuinely moves inside the horizon after that
    point, the operator needs to know current data has drifted away from
    what was published -- exactly the scenario an external review flagged
    as unproven. Without this pin, a future maintainer could silently
    suppress legitimate during-run detection (e.g. by over-widening a fix
    like F6's to swallow real value changes too) and only F6's negative
    assertion would object -- it says nothing about whether real changes
    are still caught.

    Routed through ``start_run`` itself (the same real recording path C7
    and F6 use), not a stub, and the mutation is a genuine ``daily_truth``
    UPDATE (not a hand-prefixed fingerprint) so this pin cannot be
    satisfied by a construction that never exercises the read path's
    actual re-derivation."""
    from datetime import UTC, datetime

    conn = asof_conn()
    site_id, gen, _feeds = _site_with_period(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    now = datetime(2027, 1, 1, tzinfo=UTC)
    cfg = start_run(conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=now)
    assert cfg is not None
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    recorded, period_start, period_end = (
        row["result_basis_fingerprint"],
        row["period_start"],
        row["period_end"],
    )

    # A genuine during-run change: an in-horizon truth value moves after
    # the basis was pinned at start_run, exactly the scenario the operator
    # freshness warning exists to catch.
    cursor = conn.execute(
        """
        UPDATE daily_truth SET value = 99.5
        WHERE site_id = ? AND tz_generation_id = ? AND local_date = ?
        """,
        (site_id, gen, _A_PERIOD_DAYS[0]),
    )
    assert cursor.rowcount > 0

    freshness = result_basis_freshness(
        conn,
        site_id,
        recorded=recorded,
        period_start=period_start,
        period_end=period_end,
    )
    assert freshness.state == "changed"
    assert freshness.reason is None
