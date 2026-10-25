"""Old-wind-method removal: fails-on-base pins (plan §10.4 D1-D16) plus the
source scan (plan §10.4's scan row).

Every test here imports only names that exist in BOTH the pre-removal tree
(0.16.10, commit c5f6734) and this tree, so the module collects cleanly on
base; each test fails there by assertion (or by an uncaught non-import
exception such as ``JobDeferred``), never by ``ImportError``. The six
deleted key-builder functions (``wind_basis_key`` and friends) are never
imported -- every ``runtime_state`` key they used to build is written here
as its literal string, e.g. ``f"wind_basis:{site_id}"``.

Expected values are read directly from this tree's current code (the
removal side) and from the base tree's current code (the pre-removal
side), not from the plan table's wording, per plan §10.4's instruction.

Isolation: most pins drive a plain ``sqlite3.connect(":memory:")`` +
``run_migrations`` connection directly against the function under test,
mirroring ``tests/test_wind_auth_hold.py``. The few that need the live app
or a real write path (D7, D9, D10, D12) use a ``tmp_path``-backed
``Database``/``FencedWriter``/``TestClient``, mirroring
``tests/test_scoring_split.py`` and ``tests/test_phase8_verification_v16.py``.

Synthetic data only (public repo): site name "Testsite", pws station ids
``WSTATION01``/``WSTATION02``, coordinates 0.0/0.0 or 40.0/-105.0 (matching
the reused helpers' own fixtures), timezone "UTC", dates in 2035/2026.
"""

from __future__ import annotations

import ast
import asyncio
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

import wxverify
from tests.test_forecast_record import (
    _DAY,
    _insert_full_grid,
    _snapshot_t,
)
from tests.test_forecast_record import (
    _conn as _record_conn,
)
from tests.test_forecast_record import (
    _make_feed as _record_make_feed,
)
from tests.test_forecast_record import (
    _make_site as _record_make_site,
)
from tests.test_forecast_service import (
    _feed_id,
    _hours,
    _seed_hourly,
    _seed_wind_training,
)
from tests.test_forecast_service import (
    _make_db as _fs_make_db,
)
from tests.test_phase8_verification_v16 import (
    _init_tmp_db as _v16_init_tmp_db,
)
from tests.test_phase8_verification_v16 import (
    _make_app as _v16_make_app,
)
from tests.test_phase8_verification_v16 import (
    _make_site as _v16_make_site,
)
from tests.test_phase8_verification_v16 import (
    _seed_rich_run,
)
from tests.test_scoring_split import _init_tmp_db
from tests.test_scoring_split import _make_site as _split_make_site
from tests.test_wind_auth_hold import (
    _insert_day as _wa_insert_day,
)
from tests.test_wind_auth_hold import (
    _make_db as _wa_make_db,
)
from tests.test_wind_auth_hold import (
    _make_site as _wa_make_site,
)
from tests.test_wind_auth_hold import (
    _make_station as _wa_make_station,
)
from tests.test_wind_basis import (
    _make_db as _wb_make_db,
)
from tests.test_wind_basis import (
    _make_feed as _wb_make_feed,
)
from tests.test_wind_basis import (
    _make_site as _wb_make_site,
)
from tests.test_wind_basis import (
    _seed_persistence_candidate as _wb_seed_persistence_candidate,
)
from tests.test_wind_basis import (
    _seed_published_wind_pair as _wb_seed_published_wind_pair,
)
from tests.test_wind_basis import (
    _seed_real_model_candidate as _wb_seed_real_model_candidate,
)
from wxverify.db.connection import FencedWriter, get_db
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import Job
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.wind_basis import write_auth_hold
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.forecast.wind_blend import WIND_METHOD
from wxverify.monitor import _wind_history_trips  # noqa: PLC2701
from wxverify.obs.pws_adapter import observations_from_payload
from wxverify.obs.wind_pairs import WindRecord
from wxverify.scoring.consensus import insert_station_observation, materialize_consensus
from wxverify.scoring.engine import _distinct_cells  # noqa: PLC2701
from wxverify.scoring.pairing import compute_real_model_pairs
from wxverify.scoring.persistence import compute_persistence_pairs
from wxverify.verification.record import build_forecast_record
from wxverify.verification.runs import capture_config_snapshot
from wxverify.web.context import (
    load_site,
    load_station_trust,
    load_wind_banner,
    load_wind_weights,
)
from wxverify.worker.processor import dispatch
from wxverify.worker.verification_run import _blocking_gate  # noqa: PLC2701
from wxverify.worker.wind_days import (  # noqa: PLC2701
    _persist_day,
    _start_job,
    wind_lane_due,
)

# ---------------------------------------------------------------------------
# D1 -- insert_station_observation's wind guard.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key_value", ["staging", "switching", "rescoring", "pair_max", "bogus", None]
)
def test_d1_wind_insert_is_declined_for_any_key(key_value: str | None) -> None:
    """The removal tree declines every wind write regardless of the key, per
    the unconditional guard at ``scoring/consensus.py``'s
    ``insert_station_observation``. Base writes a wind row only while the
    site's key reads "staging" (``db/wind_basis.py``'s ``wind_state_sql``
    defaults an absent key to "pair_max"), so only the "staging" id fails
    there -- the rest already decline on base too."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    station_id = _wa_make_station(conn, site_id, "WSTATION01")
    if key_value is not None:
        set_runtime_state(conn, f"wind_basis:{site_id}", key_value)
    conn.commit()

    wrote = insert_station_observation(
        conn,
        station_id=station_id,
        variable="wind",
        valid_at="2035-07-01T00:00:00Z",
        value=5.0,
        source_raw="test",
    )

    assert wrote is False
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert int(row["n"]) == 0


# ---------------------------------------------------------------------------
# D2 -- materialize_consensus's wind-row admission.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key_value", ["staging", "bogus"])
def test_d2_materialize_admits_the_pair_max_row_regardless_of_the_key(
    key_value: str,
) -> None:
    """Base's ``materialize_consensus`` filters station rows through
    ``wind_station_row_clause``, which admits a pair-max-prefixed row only
    while the key reads "switching"/"rescoring"/"pair_max" -- never
    "staging" or an unknown value, both of which admit nothing. The removal
    tree has no such filter: the one seeded reading always becomes the
    consensus observation."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    station_id = _wa_make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, f"wind_basis:{site_id}", key_value)
    valid_at = "2035-07-01T00:00:00Z"
    conn.execute(
        """
        INSERT INTO station_observations
            (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)
        VALUES (?, 'wind', ?, 6.0, 'ok', 'pair-max 21.6 km/h', ?)
        """,
        (station_id, valid_at, valid_at),
    )
    conn.commit()

    materialize_consensus(conn, site_id=site_id, variable="wind", valid_at=valid_at)

    row = conn.execute(
        "SELECT value FROM observations WHERE site_id = ? AND variable = 'wind'"
        " AND valid_at = ?",
        (site_id, valid_at),
    ).fetchone()
    assert row is not None
    assert float(row["value"]) == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# D3 -- the pairing, persistence and engine wind_open_clause readers.
# ---------------------------------------------------------------------------


def test_d3_pairing_persistence_and_engine_readers_admit_wind_under_switching() -> None:
    """Base's ``wind_open_clause`` (db/wind_basis.py), read by
    ``scoring/pairing.py``'s ``compute_real_model_pairs``,
    ``scoring/persistence.py``'s ``compute_persistence_pairs`` and
    ``scoring/engine.py``'s ``_distinct_cells``, excludes every wind row
    while the key reads "switching". The removal tree's three files read
    no such key -- all three admit the wind candidate alongside it."""
    conn = _wb_make_db()
    site_id = _wb_make_site(conn)
    feed_id = _wb_make_feed(conn, "real-model")
    set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
    conn.commit()

    _wb_seed_real_model_candidate(conn, site_id, feed_id, "wind")
    _wb_seed_real_model_candidate(conn, site_id, feed_id, "temperature")
    pair_delta = compute_real_model_pairs(conn, site_id)
    pair_variables = {str(op.values[2]) for op in pair_delta.ops}

    _wb_seed_persistence_candidate(conn, site_id)
    persistence_delta = compute_persistence_pairs(conn, site_id)
    persistence_variables = {str(op.values[2]) for op in persistence_delta.ops}

    _wb_seed_published_wind_pair(conn, site_id, feed_id)
    cells = _distinct_cells(conn, site_id)
    cell_variables = {cell.variable for cell in cells}

    assert "wind" in pair_variables
    assert "wind" in persistence_variables
    assert "wind" in cell_variables


# ---------------------------------------------------------------------------
# D4 -- forecast/hourly wind blend and the record policy's wind fields.
# ---------------------------------------------------------------------------


def test_d4_hourly_wind_blend_is_weighted_regardless_of_the_key() -> None:
    """Base's ``build_hourly`` reads the key to pick between the weighted
    blend ("pair_max") and the unweighted ``blend_series`` ("legacy") --
    under "staging" it serves the unweighted mean through the generic
    ``_select`` ladder, whose leaderboard read (no ``score_cache`` seeded
    here) is "rebuilding", so both trained feeds read unconfident and
    ``states["wind"]`` comes back "rebuilding". The removal tree's
    ``build_hourly`` has no such branch: the wind line is always the
    accuracy-weighted blend (forecast/wind_blend.py's ``WindServing``, which
    no longer carries a mode), ``choose_wind_feeds`` always sets
    ``low_confidence=False`` for an eligible selection, and the ranking
    rebuild is forced ``False`` for wind (``:476-480``) -- so
    ``states["wind"]`` is never "rebuilding"."""
    conn = _fs_make_db()
    set_runtime_state(conn, "wind_basis:1", "staging")
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)

    # Unweighted mean of 4.0/7.0 is 5.5 m/s = 19.8 km/h; the weighted value
    # (weights 2/3, 1/3) is 5.0 m/s = 18.0 km/h. Base, under "staging",
    # serves the unweighted figure; the removal tree always serves this one.
    assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert payload["weights"] == {
        "wind": {str(feed_a): pytest.approx(2 / 3), str(feed_b): pytest.approx(1 / 3)}
    }
    assert payload["notes"] == {"wind": None}
    # Both trained feeds have no score_cache row, so base's generic ladder
    # reads the leaderboard as "rebuilding" and low_confidence, which the
    # (low_confidence AND ranking_rebuilding) rule turns into "rebuilding".
    # The removal tree's weighted choice never sets low_confidence for an
    # eligible pair and forces ranking_rebuilding False for wind either way.
    assert payload["states"]["wind"] == "normal"


def test_d4_forecast_wind_daily_high_is_weighted_regardless_of_the_key() -> None:
    """Base's ``build_forecast`` only takes ``_select_wind``'s weighted
    daily high while ``serving.mode == "weighted"`` ("pair_max"); under
    "staging" (``serving.mode == "legacy"``) wind falls through to the
    generic ``_select``/``displayed_daily`` path and ``wind_weights`` stays
    ``None``. The removal tree's ``build_forecast`` always calls
    ``_select_wind`` for wind, so ``wind_weights`` is always
    ``choice.weights`` (a populated mapping) and the tile's wind daily high
    is always ``wind_blend.weighted_daily_high``'s figure."""
    conn = _fs_make_db()
    set_runtime_state(conn, "wind_basis:1", "staging")
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_training(conn, forecast_by_feed={feed_a: 5.0, feed_b: 6.0}, observed=4.0)
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    today = view.tiles[0]
    # Weighted (weights 2/3, 1/3): 5.0 m/s = 18.0 km/h. Base's unweighted
    # mean of the two feeds' daily highs is 5.5 m/s = 19.8 km/h.
    assert today.wind.max_kmh == pytest.approx(18.0)
    assert today.wind.note is None


def test_d4_record_policy_pins_pair_max_wind_fields_regardless_of_the_key() -> None:
    """``verification/record.py``'s ``compute_forecast_record`` writes
    ``policy["wind_method"]``/``policy["wind_basis"]`` from the kept
    ``WIND_METHOD`` constant and the literal "pair_max" unconditionally.
    Base instead writes ``_POLICY_WIND_METHOD[wind_serving.mode]`` and
    ``wind_serving.state``, which under the "staging" key are neither
    ``WIND_METHOD`` nor "pair_max"."""
    conn = _record_conn()
    site_id = _record_make_site(conn, "site-record")
    feed_id = _record_make_feed(conn, "model-a")
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    _insert_full_grid(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        start_date=_DAY,
        issued_at="2035-06-15T06:00:00Z",
        fetched_at="2035-06-15T06:05:00Z",
    )
    t = _snapshot_t()
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(minutes=5))

    row = conn.execute(
        "SELECT policy FROM forecast_of_record WHERE site_id = ? LIMIT 1",
        (site_id,),
    ).fetchone()
    policy = json.loads(str(row["policy"]))
    assert policy["wind_method"] == WIND_METHOD
    assert policy["wind_basis"] == "pair_max"


# ---------------------------------------------------------------------------
# D5 -- the dashboard wind weights read, independent of the key.
# ---------------------------------------------------------------------------


def test_d5_dashboard_wind_weights_ignore_the_key() -> None:
    """Base's ``web/context.py::load_wind_weights`` returns ``None`` unless
    ``wind_serving.mode == "weighted"`` (never true under "staging"). The
    removal tree's version has no mode to check -- it returns a (possibly
    empty) list every time."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.commit()
    site = load_site(conn, site_id)
    assert site is not None

    rows = load_wind_weights(conn, site, day_ahead=1)

    assert rows is not None
    assert rows == []


def test_d5_wind_banner_absent_and_station_trust_includes_wind() -> None:
    """Paired with the hold-present web test below: without any auth hold,
    ``load_wind_banner`` returns ``None`` regardless of the key (it is no
    longer gated on a "pair_max"/mode read at all). ``load_station_trust``
    is keyed on ``station_observations``/``observations`` rows, not on the
    wind key, so a wind row surfaces there under any key too."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    station_id = _wa_make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value,"
        " n_stations, rejected_stations, computed_at) VALUES"
        " (?, 'wind', '2035-07-01T00:00:00Z', 5.0, 1, 0, '2035-07-01T00:05:00Z')",
        (site_id,),
    )
    conn.execute(
        "INSERT INTO station_observations (station_id, variable, valid_at,"
        " value, qc_flag, source_raw, fetched_at) VALUES"
        " (?, 'wind', '2035-07-01T00:00:00Z', 8.0, 'ok', 'test',"
        " '2035-07-01T00:05:00Z')",
        (station_id,),
    )
    conn.commit()

    banner = load_wind_banner(conn, site_id)
    trust = load_station_trust(conn)

    assert banner is None
    assert any(row.variable == "wind" for row in trust)


def test_d5_ops_and_dashboard_render_the_history_hold_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §6.11 renames the Ops panel's "Rebuild calls today:" label to
    "History calls today:" and drops the dashboard banner's progress note,
    leaving only the auth-hold titles. Base's ``WIND_HOLD_TITLES`` for
    "history_all" is "Rebuild stalled: weather.com refused the key", so its
    dashboard banner both omits the removal tree's title text and still
    contains "Rebuild stalled"."""
    conn = _v16_init_tmp_db(tmp_path)
    _v16_make_site(conn)
    app = _v16_make_app(monkeypatch)
    with TestClient(app) as client:
        ops_resp = client.get("/ops")
        assert ops_resp.status_code == 200
        assert "History calls today" in ops_resp.text

        db = get_db()
        db.write_sync(
            lambda conn: write_auth_hold(
                conn,
                "history_all",
                status="held",
                since="2035-07-01T00:00:00Z",
                error="refused",
            )
        )

        dash_resp = client.get("/dashboard")

    assert dash_resp.status_code == 200
    assert 'id="wind-banner"' in dash_resp.text
    assert (
        "<strong>This weather.com key cannot download wind history for"
        " past days, a known limit of the key.</strong>" in dash_resp.text
    )
    assert (
        'Use "Try again" in the Wind History panel on Ops to check the key'
        " again." in dash_resp.text
    )
    assert "Rebuild stalled" not in dash_resp.text


# ---------------------------------------------------------------------------
# D6 -- the ops monitor's wind-rebuild-blocked trip.
# ---------------------------------------------------------------------------


def test_d6_monitor_today_checks_run_under_a_blocked_switching_key() -> None:
    """Base's checks (a)/(b) read the ``wind_basis:<site>`` and
    ``wind_basis_blocked:<site>`` keys: under "switching" with a blocked
    JSON object, (a) trips first with "wind rebuild blocked: <reason>",
    before the (e) today check is even reached (base's ``today_checks``
    also excludes "switching"/"rescoring"). The removal tree's
    ``_wind_history_trips`` reads neither key -- with a no-readings today
    row past the grace period, the first (and only exposed) reason is (e)'s
    "no wind readings today from any station", which never starts with
    "wind rebuild"."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    _wa_make_station(conn, site_id, "WSTATION01")
    now = datetime(2035, 7, 1, 12, tzinfo=UTC)
    # No station_wind_days row for today: the (e) no-readings-today check
    # is the one under test, past WIND_TODAY_GRACE_HOURS after midnight.
    set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
    set_runtime_state(
        conn, f"wind_basis_blocked:{site_id}", json.dumps({"reason": "stub reason"})
    )
    conn.commit()

    tripped, first = _wind_history_trips(conn, now)

    assert tripped == 1
    assert first == "no wind readings today from any station"
    assert first is not None
    assert not first.startswith("wind rebuild")


# ---------------------------------------------------------------------------
# D7 -- the verification-run blocking gate.
# ---------------------------------------------------------------------------


def test_d7_blocking_gate_no_longer_defers_a_run_for_a_switching_key() -> None:
    """Base's ``worker/verification_run.py::_blocking_gate`` returns
    ``("skipped", "wind history switch in progress")`` while the key reads
    "switching"/"rescoring". The removal tree's ``_blocking_gate`` has no
    such first check -- a fresh site with no published run and no failed
    attempts for the fingerprint falls through to ``None``."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
    conn.commit()

    decision = _blocking_gate(conn, site_id, "fp-test")

    assert decision is None


# ---------------------------------------------------------------------------
# D8 -- the run's captured config snapshot.
# ---------------------------------------------------------------------------


def test_d8_run_snapshot_pins_pair_max_wind_basis_regardless_of_the_key() -> None:
    """``verification/runs.py``'s ``_config_snapshot`` writes the literal
    "pair_max" for ``wind_basis`` unconditionally. Base instead writes
    ``wind_basis_state(conn, site_id)``, which under the "staging" key
    reads "staging", not "pair_max"."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.commit()

    snapshot = capture_config_snapshot(conn, site_id)

    assert snapshot["wind_basis"] == "pair_max"


# ---------------------------------------------------------------------------
# D9 -- the verification page's wind diagnostic notice.
# ---------------------------------------------------------------------------


def test_d9_verification_page_wind_diagnostic_is_unconditional(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``web/templates/verification/show.html``'s wind-diagnostic notice is
    now gated only on ``v.variable == 'wind'``. Base also requires the
    page's ``wind_pair_max`` context flag, which
    ``web/verification.py`` sets to
    ``wind_basis_state(conn, site.id) == "pair_max"`` -- false under the
    "staging" key, so the notice is absent there."""
    conn = _v16_init_tmp_db(tmp_path)
    site_id = _v16_make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.commit()
    _seed_rich_run(conn, site_id, methodology_version=1)
    app = _v16_make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200

    assert 'id="verification-wind-diagnostic"' in response.text


# ---------------------------------------------------------------------------
# D10 -- the timezone-correction job's wind-switch defer.
# ---------------------------------------------------------------------------


def test_d10_timezone_correction_is_not_deferred_for_a_switching_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Base's dispatch calls
    ``_advance_correction_outside_wind_switch``, which raises
    ``JobDeferred`` while the key reads "switching"/"rescoring" -- before
    ever calling ``advance_correction``. The removal tree's dispatch calls
    ``advance_correction`` unconditionally, so the call lands and the job
    completes."""

    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id = _split_make_site(conn, "t-d10-site")
        set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
        conn.commit()
        calls: list[int] = []

        def _stub_advance_correction(
            conn: sqlite3.Connection, site_id: int, payload: dict[str, object]
        ) -> bool:
            calls.append(site_id)
            return False

        monkeypatch.setattr(
            "wxverify.worker.processor.advance_correction", _stub_advance_correction
        )
        db = get_db()
        writer = FencedWriter(db, db.generation)
        job = Job(
            id=1,
            type="timezone_correction",
            site_id=site_id,
            job_key="tzc",
            payload={},
            status="running",
            retry_count=0,
            max_retries=3,
        )

        result = await dispatch(db, writer, job)

        assert calls == [site_id]
        assert result is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# D11 -- _persist_day's stamp of wind-switch progress.
# ---------------------------------------------------------------------------


def test_d11_persist_day_writes_no_wind_basis_progress_key() -> None:
    """Base's ``worker/wind_days.py::_persist_day`` calls
    ``stamp_wind_progress`` (``db/wind_basis.py``), which writes
    ``wind_basis_progress_at:<site>``, whenever a non-refetch day older
    than yesterday changes status. The removal tree's ``_persist_day`` has
    no such call at all -- no key is ever written, with no key present to
    begin with."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    station_id = _wa_make_station(conn, site_id, "WSTATION01")
    today = date(2035, 7, 10)
    local_date = date(2035, 7, 1)
    _wa_insert_day(conn, station_id, local_date, status="pending")
    conn.commit()

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name="UTC",
        today=today,
        records=[],
        probe=None,
    )

    assert get_runtime_state(conn, f"wind_basis_progress_at:{site_id}") is None


# ---------------------------------------------------------------------------
# D12 -- a second migrations run no longer reseeds the state.
# ---------------------------------------------------------------------------


def test_d12_reopening_the_db_does_not_reseed_a_wind_basis_key() -> None:
    """Base's ``run_migrations`` calls ``init_wind_basis(conn)`` on EVERY
    call, including a second one against an already-migrated db, which
    seeds a fresh ``wind_basis:<site>`` key for each site. The removal
    tree's v9 migration is one-time -- a later ``run_migrations`` call
    writes no such row."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC', 1)"
    )
    conn.commit()
    site_id = int(cur.lastrowid)

    run_migrations(conn)

    rows = conn.execute(
        "SELECT key FROM runtime_state WHERE key LIKE 'wind_basis%'"
    ).fetchall()
    assert rows == []
    assert get_runtime_state(conn, f"wind_basis:{site_id}") is None


# ---------------------------------------------------------------------------
# D13 -- the wind-days lane's due check.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key_value", ["switching", "bogus"])
def test_d13_wind_lane_due_ignores_the_key(key_value: str) -> None:
    """Base's ``wind_lane_due`` returns ``True`` immediately whenever the
    key is "switching"/"rescoring" or an unrecognized value -- before
    checking whether any station actually has wind days due. The removal
    tree reads no such key at all, so with nothing due it returns
    ``False``."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", key_value)
    conn.commit()

    now = datetime(2035, 7, 1, 12, tzinfo=UTC)
    assert wind_lane_due(conn, site_id, ZoneInfo("UTC"), now=now) is False


# ---------------------------------------------------------------------------
# D14 -- the PWS adapter's wind field in observations_from_payload.
# ---------------------------------------------------------------------------


def test_d14_observations_from_payload_yields_no_wind_observation() -> None:
    """Base's ``observations_from_payload`` (obs/pws_adapter.py) emits a
    ``PwsObservation(variable="wind", ...)`` whenever the row's metric
    carries a wind field (``windSpeed`` and friends). The removal tree's
    version has no such branch at all -- only temperature (and precip,
    absent here) are ever emitted."""
    payload: dict[str, object] = {
        "observations": [
            {
                "obsTimeUtc": "2035-07-01 00:00:00",
                "metric": {"temp": 15.0, "windSpeed": 10.0},
            }
        ]
    }

    observations = observations_from_payload(payload)

    variables = {observation.variable for observation in observations}
    assert "temperature" in variables
    assert "wind" not in variables


# ---------------------------------------------------------------------------
# D15 -- the wind-days job start no longer resets an unrecognized key.
# ---------------------------------------------------------------------------


def test_d15_wind_days_job_start_leaves_an_unrecognized_key_untouched() -> None:
    """Base's ``_start_job`` resets the key to "staging" whenever it holds
    a value outside the known state set. The removal tree's ``_start_job``
    never reads or writes the key, so an unrecognized value survives the
    call unchanged."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    _wa_make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, f"wind_basis:{site_id}", "bogus")
    conn.commit()

    _start_job(conn, site_id)

    assert get_runtime_state(conn, f"wind_basis:{site_id}") == "bogus"


# ---------------------------------------------------------------------------
# D16 -- _persist_day's pair-max station rows, independent of the key.
# ---------------------------------------------------------------------------


def test_d16_persist_day_writes_pair_max_rows_under_a_staging_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Base's ``_persist_day`` only derives/writes pair-max station rows
    when ``wind_basis_state(conn, site_id) == "pair_max"`` -- under the
    "staging" key it skips that write block entirely. The removal tree's
    ``_persist_day`` writes them unconditionally, whatever the key."""
    conn = _wa_make_db()
    site_id = _wa_make_site(conn)
    station_id = _wa_make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    today = date(2035, 7, 10)
    local_date = date(2035, 7, 5)
    frozen_now = datetime(2035, 7, 10, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: frozen_now)
    _wa_insert_day(conn, station_id, local_date, status="pending")
    conn.commit()
    records = [
        WindRecord(obs_at=datetime(2035, 7, 5, 10, 0, tzinfo=UTC), speed_kmh=5.0),
        WindRecord(obs_at=datetime(2035, 7, 5, 10, 5, tzinfo=UTC), speed_kmh=7.0),
    ]

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name="UTC",
        today=today,
        records=records,
        probe=None,
    )

    row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations"
        " WHERE station_id = ? AND variable = 'wind'",
        (station_id,),
    ).fetchone()
    assert int(row["n"]) > 0


# ---------------------------------------------------------------------------
# Source scan -- the six key-builders and their key prefixes are gone from
# every module except the v9 migration that deletes them.
# ---------------------------------------------------------------------------

_DELETED_KEY_BUILDERS = (
    "wind_basis_key",
    "wind_progress_key",
    "wind_cursor_key",
    "wind_blocked_key",
    "wind_report_key",
    "wind_done_at_key",
)

_DELETED_KEY_PREFIXES = (
    "wind_basis:",
    "wind_basis_progress_at:",
    "wind_basis_cursor:",
    "wind_basis_blocked:",
    "wind_rebuild_report:",
    "wind_rebuild_done_at:",
)


def _wxverify_py_files() -> list[Path]:
    root = Path(wxverify.__file__).parent
    return sorted(root.rglob("*.py"))


def test_source_scan_no_module_defines_or_calls_a_deleted_key_builder() -> None:
    """None of the six deleted ``runtime_state`` key-builder names may
    appear as an identifier anywhere under the ``wxverify`` package --
    not just unimported, genuinely gone."""
    offenders: list[str] = []
    for path in _wxverify_py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in _DELETED_KEY_BUILDERS:
                offenders.append(f"{path}:{node.lineno}:{node.id}")

    assert offenders == []


def test_source_scan_deleted_key_prefixes_survive_only_in_the_migration() -> None:
    """Each deleted key's literal prefix, immediately after a quote
    character, may appear only in ``db/migrations.py`` (the v9 migration
    that deletes the matching ``runtime_state`` rows) -- nowhere else in
    the package."""
    offenders: list[str] = []
    for path in _wxverify_py_files():
        if path.name == "migrations.py" and path.parent.name == "db":
            continue
        text = path.read_text(encoding="utf-8")
        for prefix in _DELETED_KEY_PREFIXES:
            for quote in ("'", '"'):
                if quote + prefix in text:
                    offenders.append(f"{path}:{prefix}")

    assert offenders == []
