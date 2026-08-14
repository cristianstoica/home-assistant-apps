"""§14a (W13): precip-total MAE restricted to observed-wet common days.

Every expected value below is hand-computed from the fixture's own rows, and
each fixture is built so the wet-restricted mean DIFFERS from the all-days
mean — an assertion that coincides with the unrestricted metric proves
nothing.

The three reporting states are covered separately: a value with
``low_sample = false``, a value with ``low_sample = true`` (never
suppressed), and the zero-sample ``value: null`` with the exact reason
string. The headline's identity gets its own cases — it is the run's
incumbent depth, never a pool over the cell's members, and it reports
``incumbent_not_simulated`` when that depth is outside ``SIM_DEPTHS`` — plus
a non-derivation pin on the ``leads`` grid. Two discipline pins follow: the
class-versus-millimetre reading of "wet", and a ``daily_rank_depth`` row
carrying a NULL ``abs_error`` on an observed-wet common-core date.

All fixture data is synthetic: a UTC site with invented coordinates, no
feeds, invented dates.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.diagnostics import (
    INCUMBENT_NOT_SIMULATED,
    NO_OBSERVED_WET_DAYS,
    observed_wet_precip_mae,
)
from wxverify.verification.engine import aggregate_run
from wxverify.verification.runs import RunConfig, capture_config_snapshot, publish_run
from wxverify.verification.simulate import SIM_DEPTHS, SIM_VARIABLES

_DEPTH = 2
_LEAD = 1
#: Wet-day truth in millimetres, and the dry-day truth.
_WET_TRUTH = 5.0
_DRY_TRUTH = 0.0

#: (date, observed-wet?, per-depth absolute error).
_Day = tuple[str, bool, float]


def _days(wet_errors: list[float], dry_errors: list[float]) -> list[_Day]:
    out: list[_Day] = []
    day = 1
    for error in wet_errors:
        out.append((f"2026-06-{day:02d}", True, error))
        day += 1
    for error in dry_errors:
        out.append((f"2026-06-{day:02d}", False, error))
        day += 1
    return out


#: Nine observed-wet days (errors 1,2,3 x3 -> mean 2.0) and three dry days at
#: error 10.0. All-days mean = (18 + 30) / 12 = 4.0, so a wet-restricted 2.0
#: cannot be produced by an unrestricted implementation.
_MIXED = _days([1.0, 2.0, 3.0] * 3, [10.0] * 3)

#: Three observed-wet days only (errors 1,2,3 -> mean 2.0); all-days mean is
#: (6 + 30) / 6 = 6.0. Below OCCURRENCE_MIN_WET_DAYS = 8.
_THIN = _days([1.0, 2.0, 3.0], [10.0] * 3)

#: No observed-wet day at all.
_DRY_ONLY = _days([], [10.0] * 4)


def _insert(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    entity: tuple[str, str],
    day: _Day,
    predicted: float | None,
    truth_value: float | None = None,
    wet_hours: int | None = None,
    lead: int = _LEAD,
) -> None:
    date, wet, _error = day
    truth = _WET_TRUTH if wet else _DRY_TRUTH
    if truth_value is not None:
        truth = truth_value
    hours = (2 if wet else 0) if wet_hours is None else wet_hours
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible,
             truth_wet_hours, truth_dry_hours, abs_error)
        VALUES (?, ?, ?, ?, 'precip', 'precip_total', ?, ?, ?, ?, 1, ?, 1, ?, ?, ?)
        """,
        (
            run_id,
            date,
            date,
            lead,
            entity[0],
            entity[1],
            predicted,
            1 if predicted is not None else 0,
            truth,
            hours,
            23 - hours,
            None if predicted is None else abs(predicted - truth),
        ),
    )


def _make_run(
    conn: sqlite3.Connection,
    days: list[_Day],
    *,
    leads: tuple[int, ...] = (_LEAD,),
    offsets: dict[int, float] | None = None,
    precip_depth: int = _DEPTH,
) -> tuple[int, int, RunConfig]:
    site_id = int(
        cast(
            int,
            conn.execute(
                """
                INSERT INTO sites
                    (name, forecast_lat, forecast_lon, elevation_m, timezone)
                VALUES ('site-alpha', 47.0, 25.0, 900.0, 'UTC')
                """
            ).lastrowid,
        )
    )
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    depths = {v: _DEPTH for v in SIM_VARIABLES}
    depths["precip"] = precip_depth
    snapshot["blend_depth"] = _DEPTH
    snapshot["blend_depths"] = dict(depths)
    period = [d for d, _w, _e in days] or ["2026-06-01"]
    run_id = int(
        cast(
            int,
            conn.execute(
                """
                INSERT INTO verification_runs
                    (site_id, tz_generation_id, methodology_version, app_version,
                     state, attempt, config_snapshot, period_start, period_end,
                     settled_through, bootstrap_seed, bootstrap_resamples,
                     input_fingerprint)
                VALUES (?, ?, 1, '0.11.1-test', 'running', 1, ?,
                        ?, ?, ?, 77, 200, 'fp-observed-wet-test')
                """,
                (
                    site_id,
                    generation_id,
                    json.dumps(snapshot, separators=(",", ":")),
                    period[0],
                    period[-1],
                    period[-1],
                ),
            ).lastrowid,
        )
    )
    for day in days:
        for depth in SIM_DEPTHS:
            error = day[2] + (offsets or {}).get(depth, 0.0)
            for lead in leads:
                _insert(
                    conn,
                    run_id,
                    entity=("depth", str(depth)),
                    day=day,
                    predicted=(_WET_TRUTH if day[1] else _DRY_TRUTH) + error,
                    lead=lead,
                )
    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=_DEPTH,
        blend_depths=dict(depths),
        min_n=1,
        window_days=30,
        tz_generation_id=generation_id,
        roster=(),
        period_start=period[0],
        period_end=period[-1],
        bootstrap_seed=77,
        bootstrap_resamples=200,
    )
    aggregate_run(conn, cfg)
    return site_id, run_id, cfg


def _lead_entry(payload: dict[str, object]) -> dict[str, object]:
    leads = payload["leads"]
    assert isinstance(leads, list)
    entries = cast(list[dict[str, object]], leads)
    assert len(entries) == 1
    return entries[0]


# ---------------------------------------------------------------------------
# The three reporting states
# ---------------------------------------------------------------------------


def test_the_wet_restriction_changes_the_reported_mean() -> None:
    """The headline is wet-restricted AND keyed to one entity.

    Kills, one at a time:
    - a mean over every strict-common-core date rather than the observed-wet
      subset: the fixture's twelve common days give 48 / 12 = 4.0 with
      ``sample_days`` 12, both visibly wrong here;
    - the earlier implementation of this diagnostic, whose headline was a
      mean POOLED over the cell's resolved members. The fixture's four depth
      entities carry identical per-day errors, so that implementation
      reports the same 2.0 — the value assertion alone cannot see it. It is
      the denominator that moves: nine wet days x four members = 36
      ``observations``, against nine for the run's incumbent depth alone.
      ``entity_type``/``entity_key`` pin the same fact from the other side:
      a pooled headline has no single subject to name.
    """
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _MIXED)

    payload = observed_wet_precip_mae(conn, run_id)

    # Wet-only: (1+2+3)*3 / 9 = 2.0. All twelve common days: 48 / 12 = 4.0.
    assert payload["value"] == pytest.approx(2.0)
    assert payload["sample_days"] == 9
    assert payload["low_sample"] is False
    assert payload["reason"] is None
    # The headline is the incumbent depth alone: nine wet days, one lead.
    assert payload["observations"] == 9
    assert payload["entity_type"] == "depth"
    assert payload["entity_key"] == str(_DEPTH)


def test_a_thin_sample_is_annotated_not_suppressed() -> None:
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _THIN)

    payload = observed_wet_precip_mae(conn, run_id)

    assert payload["value"] == pytest.approx(2.0)
    assert payload["sample_days"] == 3
    assert payload["low_sample"] is True
    assert payload["reason"] is None


def test_a_run_with_no_observed_wet_day_reports_the_reason() -> None:
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _DRY_ONLY)

    payload = observed_wet_precip_mae(conn, run_id)

    assert payload["value"] is None
    assert payload["sample_days"] == 0
    assert payload["reason"] == NO_OBSERVED_WET_DAYS
    assert payload["low_sample"] is True
    assert payload["leads"] == []
    # The subject is named even when the value is absent.
    assert payload["entity_key"] == str(_DEPTH)


# ---------------------------------------------------------------------------
# The headline is one named entity, and the grid is not it
# ---------------------------------------------------------------------------

#: Per-depth error offsets that make the incumbent's mean and the pooled
#: mean impossible to confuse: depth 2 keeps the day's own error, the other
#: three are 100 mm worse.
_OFFSETS = {1: 100.0, 3: 100.0, 4: 100.0}


def test_the_headline_is_the_incumbent_depth_not_a_pool() -> None:
    """A pooled implementation reports a different value AND a different
    denominator on this fixture, so neither assertion is vacuous."""
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _MIXED, leads=(0, _LEAD), offsets=_OFFSETS)

    payload = observed_wet_precip_mae(conn, run_id)

    # Incumbent depth 2 only: (1+2+3)*3 / 9 = 2.0 per lead, two leads.
    assert payload["value"] == pytest.approx(2.0)
    assert payload["entity_type"] == "depth"
    assert payload["entity_key"] == "2"
    # Nine distinct dates; eighteen (lead, date) cells pooled into the mean.
    assert payload["sample_days"] == 9
    assert payload["observations"] == 18
    # Pooled over all four depths it would be (2 + 102*3) / 4 = 77.0 over 72
    # cells — the two forms cannot coincide here.
    assert payload["value"] != pytest.approx(77.0)


def test_an_unsimulated_incumbent_depth_has_no_subject() -> None:
    """Depth 5 is a real configuration state, not a defect: no depth entity
    exists for it, so the metric reports its absence rather than
    substituting a simulated depth."""
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _MIXED, precip_depth=5)

    payload = observed_wet_precip_mae(conn, run_id)

    assert payload["value"] is None
    assert payload["reason"] == INCUMBENT_NOT_SIMULATED
    assert payload["sample_days"] == 0
    assert payload["observations"] == 0
    assert payload["entity_key"] == "5"
    # Observed-wet days DO exist, so a no_observed_wet_days answer is wrong.
    assert payload["leads"] != []


def test_the_grid_covers_every_wet_lead_and_does_not_derive_the_headline() -> None:
    """Lead 0 is a simulated cell with its own membership, and the grid is
    disclosure beside the headline, never its input."""
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _MIXED, leads=(0, _LEAD), offsets=_OFFSETS)

    payload = observed_wet_precip_mae(conn, run_id)

    entries = cast(list[dict[str, object]], payload["leads"])
    assert [e["lead"] for e in entries] == [0, _LEAD]
    for entry in entries:
        entities = cast(list[dict[str, object]], entry["entities"])
        assert [e["entity_key"] for e in entities] == [str(d) for d in SIM_DEPTHS]
    values = [
        cast(float, e["value"])
        for entry in entries
        for e in cast(list[dict[str, object]], entry["entities"])
    ]
    grid_mean = sum(values) / len(values)
    assert grid_mean == pytest.approx(77.0)
    assert cast(float, payload["value"]) != pytest.approx(grid_mean)


# ---------------------------------------------------------------------------
# Sample discipline
# ---------------------------------------------------------------------------


def test_wet_is_a_class_not_a_millimetre_threshold() -> None:
    """A 0.3 mm day with one wet hour is WET, though 0.3 < 0.5.

    ``_OCCURRENCE_THRESHOLD`` is an occurrence-scale constant on 0..1
    values; comparing it against the precip-total truth in millimetres
    would drop this day and change the denominator.
    """
    conn = asof_conn()
    day: _Day = ("2026-06-01", True, 4.0)
    dry: _Day = ("2026-06-02", False, 1.0)
    _site_id, run_id, _cfg = _make_run(conn, [dry])
    for depth in SIM_DEPTHS:
        _insert(
            conn,
            run_id,
            entity=("depth", str(depth)),
            day=day,
            predicted=0.3 + 4.0,
            truth_value=0.3,
            wet_hours=1,
        )
    # Re-resolve now that the wet day exists.
    aggregate_run(conn, _reaggregate(conn, run_id))

    payload = observed_wet_precip_mae(conn, run_id)

    assert payload["sample_days"] == 1
    assert payload["value"] == pytest.approx(4.0)


def _reaggregate(conn: sqlite3.Connection, run_id: int) -> RunConfig:
    row = conn.execute(
        "SELECT site_id, tz_generation_id FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    return RunConfig(
        site_id=int(row["site_id"]),
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=_DEPTH,
        blend_depths={v: _DEPTH for v in SIM_VARIABLES},
        min_n=1,
        window_days=30,
        tz_generation_id=int(row["tz_generation_id"]),
        roster=(),
        period_start="2026-06-01",
        period_end="2026-06-30",
        bootstrap_seed=77,
        bootstrap_resamples=200,
    )


def test_a_null_daily_rank_row_neither_breaks_nor_moves_the_mean() -> None:
    """The entity set is the cell's resolved members, so a NULL is unreachable."""
    conn = asof_conn()
    _site_id, run_id, _cfg = _make_run(conn, _MIXED)
    baseline = observed_wet_precip_mae(conn, run_id)

    _insert(
        conn,
        run_id,
        entity=("daily_rank_depth", "3"),
        day=_MIXED[0],
        predicted=None,
    )
    payload = observed_wet_precip_mae(conn, run_id)

    assert payload["value"] == pytest.approx(cast(float, baseline["value"]))
    assert payload["sample_days"] == baseline["sample_days"]
    assert payload["observations"] == baseline["observations"]
    entities = cast(list[dict[str, object]], _lead_entry(payload)["entities"])
    assert [e["entity_type"] for e in entities] == ["depth"] * len(SIM_DEPTHS)
    # The reported denominator equals the declared sample, per member.
    for entity in entities:
        assert entity["sample_days"] == payload["sample_days"]


# ---------------------------------------------------------------------------
# API-vs-page parity
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:  # pragma: no cover - never awaited
    import asyncio

    await asyncio.Event().wait()


def _published_app_run(
    tmp_path: Path,
    days: list[_Day],
    *,
    precip_depth: int = _DEPTH,
    leads: tuple[int, ...] = (_LEAD,),
    offsets: dict[int, float] | None = None,
) -> int:
    from wxverify import config
    from wxverify.db.connection import close_db, init_db

    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(config.db_path)
    conn = db._conn  # noqa: SLF001
    site_id, run_id, _cfg = _make_run(
        conn, days, precip_depth=precip_depth, leads=leads, offsets=offsets
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    return site_id


def test_the_page_and_the_api_report_the_same_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.api.app import create_app

    site_id = _published_app_run(tmp_path, _MIXED)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
        runs = client.get("/api/verification/runs")
        assert runs.status_code == 200
        run_id = int(runs.json()["runs"][0]["run_id"])
        payload = client.get(f"/api/verification/runs/{run_id}/diagnostics")
    assert page.status_code == 200
    assert payload.status_code == 200

    served = cast(dict[str, Any], payload.json()["observed_wet_precip_mae"])
    assert served["value"] == pytest.approx(2.0)
    assert served["sample_days"] == 9
    assert served["low_sample"] is False
    assert served["reason"] is None

    rendered = page.text.split('data-v16="16.4.observed_wet_precip_mae"', 1)[1]
    assert f'data-field="observed_wet_precip_mae">{served["value"]:.2f}' in rendered
    assert f'data-field="observed_wet_sample_days">{served["sample_days"]}' in rendered
    assert 'data-field="observed_wet_low_sample"' not in rendered
    # The page names the subject of the number, not just the number.
    assert f'data-entity-type="{served["entity_type"]}"' in rendered
    assert f'data-field="observed_wet_entity_key">{served["entity_key"]}' in rendered


def test_a_thin_sample_shows_its_caveat_on_the_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.api.app import create_app

    site_id = _published_app_run(tmp_path, _THIN)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
    rendered = page.text.split('data-v16="16.4.observed_wet_precip_mae"', 1)[1]
    assert 'data-field="observed_wet_low_sample"' in rendered
    assert 'data-field="observed_wet_precip_mae">2.00' in rendered
    assert f'data-field="observed_wet_entity_key">{_DEPTH}' in rendered


@pytest.mark.parametrize(
    ("days", "precip_depth", "expected_key", "expected_sentence", "forbidden_sentence"),
    [
        (
            _DRY_ONLY,
            _DEPTH,
            str(_DEPTH),
            "no observed-wet common days on this run",
            "did not simulate the configured precip depth",
        ),
        (
            _MIXED,
            5,
            "5",
            "did not simulate the configured precip depth",
            "no observed-wet common days on this run",
        ),
    ],
    ids=["no_observed_wet_days", "incumbent_not_simulated"],
)
def test_the_page_names_the_subject_even_when_the_value_is_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    days: list[_Day],
    precip_depth: int,
    expected_key: str,
    expected_sentence: str,
    forbidden_sentence: str,
) -> None:
    """``entity_key`` is populated in both null states, so a reader can tell
    what was attempted rather than seeing a bare 'not available'.

    Each null state must also carry its OWN explanation. The negative
    assertion is the load-bearing one: an ``incumbent_not_simulated`` page
    that says 'no observed-wet common days on this run' is factually wrong
    (the wet days exist; the configured depth was never simulated), so a
    template that branches on the truthiness of ``reason`` instead of its
    value fails here."""
    from wxverify.api.app import create_app

    site_id = _published_app_run(tmp_path, days, precip_depth=precip_depth)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
    assert page.status_code == 200
    rendered = page.text.split('data-v16="16.4.observed_wet_precip_mae"', 1)[1]
    # The absent-value branch is the one on screen ...
    assert 'data-field="observed_wet_precip_mae_reason"' in rendered
    # ... and the subject is still named.
    assert 'data-entity-type="depth"' in rendered
    assert f'data-field="observed_wet_entity_key">{expected_key}' in rendered
    # ... with the explanation that belongs to THIS state, and no other.
    assert expected_sentence in rendered
    assert forbidden_sentence not in rendered


# ---------------------------------------------------------------------------
# The `leads` disclosure grid is RENDERED (0.11.2 item 4)
# ---------------------------------------------------------------------------


def _wetmae_slice(text: str) -> str:
    """The subsection slice every render assertion below reads.

    Bounded at the end of the enclosing diagnostics ``<section>`` so the
    absence assertions cannot be satisfied by a slice that merely stops
    short of the markup they are looking for.
    """
    tail = text.split('data-v16="16.4.observed_wet_precip_mae"', 1)[1]
    slice_ = tail.split("</section>", 1)[0]
    assert len(slice_) < len(tail)
    return slice_


def _render(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> str:
    from wxverify.api.app import create_app

    site_id = _published_app_run(tmp_path, **kwargs)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
    assert page.status_code == 200
    return _wetmae_slice(page.text)


def test_the_leads_grid_renders_only_the_leads_the_run_scored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sparsity and per-entity values, hand-computed from the fixture rows.

    ``_MIXED`` on leads 0 and 3 only, with depth 4 offset by +2.0.
    ``diagnostics.py`` loops ``range(SIM_DAY_COUNT)`` and skips a lead with
    no observed-wet common date, so leads 1, 2 and 4-7 must not appear.
    Nine observed-wet days x four ``SIM_DEPTHS`` members x two leads = eight
    rendered rows: depths 1, 2 and 3 at (1+2+3)*3 / 9 = 2.00, depth 4 at
    (3+4+5)*3 / 9 = 4.00.

    Kills:
    - a fixed D0-D7 table, which emits eight phantom lead rows and blows
      both the absence assertions and the row count;
    - a template that repeats the headline value in every row: the headline
      is 2.00 and the depth-4 rows must read 4.00;
    - a template that renders ``entry.sample_days`` (9) where ``e.value``
      belongs: neither 2.00 nor 4.00 would appear in the MAE cell.
    """
    rendered = _render(
        tmp_path, monkeypatch, days=_MIXED, leads=(0, 3), offsets={4: 2.0}
    )
    assert 'data-field="observed_wet_leads"' in rendered
    assert 'data-field="observed_wet_leads_empty"' not in rendered
    # Sparsity: only the two scored leads, and nothing else in D0..D7.
    assert 'data-lead="0"' in rendered
    assert 'data-lead="3"' in rendered
    for absent in (1, 2, 4, 5, 6, 7):
        assert f'data-lead="{absent}"' not in rendered
    assert rendered.count("<tr data-lead=") == 8

    # Per-entity values, asserted as an unordered set: member order comes
    # from the persisted cell resolution and is not a contract.
    rows = {
        (lead, depth): f'<tr data-lead="{lead}" data-entity="depth:{depth}">'
        for lead in (0, 3)
        for depth in SIM_DEPTHS
    }
    assert set(rows) == {(lead, depth) for lead in (0, 3) for depth in SIM_DEPTHS}
    for (lead, depth), marker in rows.items():
        assert marker in rendered, (lead, depth)
        cells = rendered.split(marker, 1)[1].split("</tr>", 1)[0]
        expected = "4.00" if depth == 4 else "2.00"
        assert f'<td data-label="MAE (mm)">{expected}</td>' in cells, (lead, depth)
        assert '<td data-label="Days">9</td>' in cells, (lead, depth)
        assert f'<td data-label="Lead">D{lead}</td>' in cells, (lead, depth)


def test_the_leads_grid_renders_when_the_headline_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Placement pin: the grid sits OUTSIDE the `wetmae.value is none` pair.

    The ``incumbent_not_simulated`` state (``_MIXED`` with a precip depth
    outside ``SIM_DEPTHS``) has a null headline and a fully populated grid —
    ``diagnostics.py`` builds the grid from the cell's resolved members and
    never consults the incumbent depth. Hiding the disclosure exactly when
    the headline cannot be read inverts its purpose.

    Kills: nesting the grid inside the `{% else %}` value branch, and
    nesting it inside the `{% if %}` reason branch (the paired positive in
    the sibling test above covers the value state, so a grid that renders in
    only one of the two states fails one of the pair).
    """
    rendered = _render(tmp_path, monkeypatch, days=_MIXED, precip_depth=5, leads=(0, 3))
    assert 'data-field="observed_wet_precip_mae_reason"' in rendered
    assert 'data-field="observed_wet_precip_mae">' not in rendered
    assert 'data-field="observed_wet_leads"' in rendered
    assert 'data-lead="0"' in rendered
    assert 'data-lead="3"' in rendered


def test_the_leads_grid_shows_its_empty_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No lead cell has an observed-wet common day -> the named empty state.

    Kills an unconditional `<table>` that renders a header row over nothing:
    the table marker must be absent, not merely empty.
    """
    rendered = _render(tmp_path, monkeypatch, days=_DRY_ONLY)
    # Non-vacuity: the subsection really is on screen in this slice.
    assert 'data-field="observed_wet_precip_mae_reason"' in rendered
    assert 'data-field="observed_wet_leads_empty"' in rendered
    assert 'data-field="observed_wet_leads"' not in rendered
    assert "data-lead=" not in rendered
