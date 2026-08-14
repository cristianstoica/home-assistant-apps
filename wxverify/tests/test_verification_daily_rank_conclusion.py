"""§10 (W7) oracles: the daily-rank redesign conclusion.

Every fixture is hand-built evidence: four incumbent depth entities and up
to four ``daily_rank_depth`` entities over a fixed set of synthetic dates.
Constant per-entity errors keep the arithmetic checkable by hand.

The pair (`_ranked`, `_conclusions`) is deliberately thin so each oracle
states only what it pins.
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
from wxverify.verification.methodology import ADEQUATE_LEAD_MIN_DAYS
from wxverify.verification.ranking import (
    DAILY_RANK_ENTITY_TYPE,
    EXCLUSION_THIN_LEADS,
    daily_rank_conclusions,
)
from wxverify.verification.runs import capture_config_snapshot
from wxverify.verification.simulate import (
    EXCLUDE_INSUFFICIENT_RANK_HISTORY,
    SIM_DEPTHS,
)

_DATES = [f"2026-05-{d:02d}" for d in range(1, 29)]
_ADEQUATE = _DATES[:24]  # 24 >= ADEQUATE_LEAD_MIN_DAYS (20)
_TRUTH = 10.0
_LEAD = 1
_LEADS = (1,)

EntityId = tuple[str, str]


def _run(conn: sqlite3.Connection) -> int:
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m,
                           timezone)
        VALUES (1, 'site-alpha', 47.0, 25.0, 900.0, 'Etc/GMT+7')
        """
    )
    generation_id = ensure_published_generation(conn, 1)
    # The production write path pins a full snapshot; readers rehydrate a
    # RunConfig from it, so an empty object is not a state a run can be in.
    snapshot = json.dumps(capture_config_snapshot(conn, 1), separators=(",", ":"))
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version, state,
             attempt, config_snapshot, bootstrap_seed, bootstrap_resamples,
             input_fingerprint)
        VALUES (1, ?, 1, '0.11.1-test', 'published', 1, ?, 5, 100, 'fp-w7')
        """,
        (generation_id, snapshot),
    )
    return int(cast(int, cur.lastrowid))


def _row(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    variable: str,
    quantity: str,
    entity: EntityId,
    date: str,
    predicted: float | None,
    lead: int = _LEAD,
    outcome: str | None = None,
    truth_value: float = _TRUTH,
    exclusion: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             forecast_exclusion_reason, realized_contributors, truth_value,
             truth_eligible, abs_error, occurrence_outcome)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?,1,?,?)
        """,
        (
            run_id,
            date,
            date,
            lead,
            variable,
            quantity,
            entity[0],
            entity[1],
            predicted,
            1 if predicted is not None else 0,
            exclusion,
            truth_value,
            None if predicted is None else abs(predicted - truth_value),
            outcome,
        ),
    )


def _continuous_series(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    variable: str,
    quantity: str,
    entity: EntityId,
    error: float,
    dates: list[str] | None = None,
    lead: int = _LEAD,
    exclusion: str | None = None,
) -> None:
    """One entity's constant-error series over ``dates`` (default: adequate)."""
    for date in dates if dates is not None else _ADEQUATE:
        _row(
            conn,
            run_id,
            variable=variable,
            quantity=quantity,
            entity=entity,
            date=date,
            predicted=_TRUTH + error,
            lead=lead,
            exclusion=exclusion,
        )


def _wind_depths(conn: sqlite3.Connection, run_id: int, error: float = 4.0) -> None:
    for depth in SIM_DEPTHS:
        _continuous_series(
            conn,
            run_id,
            variable="wind",
            quantity="wind_max",
            entity=("depth", str(depth)),
            error=error,
        )


def _wind_rank(
    conn: sqlite3.Connection,
    run_id: int,
    key: int,
    error: float,
    *,
    dates: list[str] | None = None,
    exclusion: str | None = None,
) -> None:
    _continuous_series(
        conn,
        run_id,
        variable="wind",
        quantity="wind_max",
        entity=(DAILY_RANK_ENTITY_TYPE, str(key)),
        error=error,
        dates=dates,
        exclusion=exclusion,
    )


def _conclusions(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
    return cast(
        "dict[str, object]",
        daily_rank_conclusions(conn, run_id, leads=_LEADS)["wind"],
    )


def _value(conn: sqlite3.Connection, run_id: int) -> str:
    return str(_conclusions(conn, run_id)["value"])


def _entities(conn: sqlite3.Connection, run_id: int) -> dict[str, dict[str, object]]:
    listed = cast("list[dict[str, object]]", _conclusions(conn, run_id)["entities"])
    return {str(e["entity_key"]): e for e in listed}


# ---------------------------------------------------------------------------
# The five values
# ---------------------------------------------------------------------------


def test_one_dominant_entity_carries_the_conclusion() -> None:
    """Existential over the family: one dominant entity is enough."""
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    _wind_rank(conn, run_id, 1, 1.0)  # dominant: 75% better than every depth
    _wind_rank(conn, run_id, 2, 4.0)  # tied with the depths: beaten
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "indicated"
    assert conclusion["value"] != "indicated_all_depths"
    assert conclusion["selected_entity_key"] == "1"
    assert conclusion["entities_examined"] == 2
    assert _entities(conn, run_id)["1"]["better_than_depths"] == list(SIM_DEPTHS)


def test_no_entity_outperforms_is_not_indicated() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    for key in (1, 2):
        _wind_rank(conn, run_id, key, 4.0)
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "not_indicated"
    assert conclusion["selected_entity_key"] is None


def test_every_entity_dominant_is_a_distinct_stronger_value() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    for key in SIM_DEPTHS:
        _wind_rank(conn, run_id, key, 1.0)
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "indicated_all_depths"
    # Never collapsed into `indicated`.
    assert conclusion["value"] != "indicated"


def test_too_little_diagnostic_history_is_not_assessable() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    thin = _DATES[: ADEQUATE_LEAD_MIN_DAYS - 1]
    for key in (1, 2):
        _wind_rank(conn, run_id, key, 1.0, dates=thin)
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "not_assessable"
    # NOT collapsed into not_indicated: nothing was measured.
    assert conclusion["value"] != "not_indicated"
    assert {str(e["state"]) for e in _entities(conn, run_id).values()} == {
        "unassessable"
    }


def test_subset_dominance_is_reported_with_its_exclusions() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    for depth in SIM_DEPTHS:
        # Depth 4 is never buildable at this site, so no pair against it can
        # ever be adequate — the case the plan forbids collapsing.
        buildable = depth != 4
        _continuous_series(
            conn,
            run_id,
            variable="wind",
            quantity="wind_max",
            entity=("depth", str(depth)),
            error=4.0,
            dates=_ADEQUATE if buildable else [],
        )
        if not buildable:
            for date in _ADEQUATE:
                _row(
                    conn,
                    run_id,
                    variable="wind",
                    quantity="wind_max",
                    entity=("depth", "4"),
                    date=date,
                    predicted=None,
                    exclusion="no_samples",
                )
    _wind_rank(conn, run_id, 1, 1.0)
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "indicated_on_subset"
    assert conclusion["value"] not in {"indicated", "not_assessable"}
    excluded = cast(
        "list[dict[str, object]]", _entities(conn, run_id)["1"]["excluded_depths"]
    )
    assert [(e["depth"], e["reason"]) for e in excluded] == [(4, "no_samples")]


def test_insufficient_rank_history_is_named_as_the_exclusion() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    for date in _ADEQUATE:
        _row(
            conn,
            run_id,
            variable="wind",
            quantity="wind_max",
            entity=(DAILY_RANK_ENTITY_TYPE, "4"),
            date=date,
            predicted=None,
            exclusion=EXCLUDE_INSUFFICIENT_RANK_HISTORY,
        )
    excluded = cast(
        "list[dict[str, object]]", _entities(conn, run_id)["4"]["excluded_depths"]
    )
    assert {str(e["reason"]) for e in excluded} == {EXCLUDE_INSUFFICIENT_RANK_HISTORY}
    assert _value(conn, run_id) == "not_assessable"


# ---------------------------------------------------------------------------
# The precedence change, written both ways on ONE fixture shape
# ---------------------------------------------------------------------------


def _precedence_fixture(conn: sqlite3.Connection, *, with_dominant: bool) -> int:
    """Entity 1 is dominant_on_subset, entity 2 is beaten, entity 3 optional.

    Day windows, chosen so exactly one pair falls short of the 20-day floor:

    * depths 1-3 — days 1-24
    * depth 4 — days 9-28 (20 days)
    * entity 1 (error 1.0) — days 1-24, so its pair with depth 4 shares only
      days 9-24 (16) and is never adequately compared
    * entity 2 (error 4.0) — days 1-28, adequate against every depth, wins
      nothing
    * entity 3 (error 1.0, optional) — days 1-28, adequate against every
      depth including depth 4 (20 shared days), so it is fully dominant
    """
    run_id = _run(conn)
    for depth in (1, 2, 3):
        _continuous_series(
            conn,
            run_id,
            variable="wind",
            quantity="wind_max",
            entity=("depth", str(depth)),
            error=4.0,
        )
    _continuous_series(
        conn,
        run_id,
        variable="wind",
        quantity="wind_max",
        entity=("depth", "4"),
        error=4.0,
        dates=_DATES[8:],
    )
    _wind_rank(conn, run_id, 1, 1.0, dates=_ADEQUATE)
    _wind_rank(conn, run_id, 2, 4.0, dates=_DATES)
    if with_dominant:
        _wind_rank(conn, run_id, 3, 1.0, dates=_DATES)
    return run_id


def test_a_beaten_entity_does_not_veto_another_entitys_dominance() -> None:
    conn = asof_conn()
    run_id = _precedence_fixture(conn, with_dominant=True)
    states = {k: str(v["state"]) for k, v in _entities(conn, run_id).items()}
    assert states["2"] == "beaten"
    assert states["3"] == "dominant"
    # Under a pure conjunction, entity 2's loss would suppress this.
    assert _value(conn, run_id) == "indicated"


def test_a_beaten_entity_does_not_veto_a_subset_claim_either() -> None:
    conn = asof_conn()
    run_id = _precedence_fixture(conn, with_dominant=False)
    states = {k: str(v["state"]) for k, v in _entities(conn, run_id).items()}
    assert states["1"] == "dominant_on_subset"
    assert states["2"] == "beaten"
    excluded = cast(
        "list[dict[str, object]]", _entities(conn, run_id)["1"]["excluded_depths"]
    )
    # The shrinkage is REPORTED, not absorbed: which depth, and why.
    assert [(e["depth"], e["reason"]) for e in excluded] == [(4, EXCLUSION_THIN_LEADS)]
    value = _value(conn, run_id)
    assert value == "indicated_on_subset"
    assert value != "not_indicated"


# ---------------------------------------------------------------------------
# Disclosure of the post-hoc selection
# ---------------------------------------------------------------------------


def test_the_conclusion_discloses_what_was_selected_and_from_how_many() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    _wind_rank(conn, run_id, 1, 4.0)
    _wind_rank(conn, run_id, 2, 1.0)
    _wind_rank(conn, run_id, 3, 4.0)
    _wind_rank(conn, run_id, 4, 4.0)
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "indicated"
    assert conclusion["selected_entity_key"] == "2"
    assert conclusion["entities_examined"] == 4
    entities = _entities(conn, run_id)
    # Every examined entity carries its own result, not only the winner.
    assert set(entities) == {"1", "2", "3", "4"}
    assert {k: str(v["state"]) for k, v in entities.items()} == {
        "1": "beaten",
        "2": "dominant",
        "3": "beaten",
        "4": "beaten",
    }


# ---------------------------------------------------------------------------
# Per-variable metrics
# ---------------------------------------------------------------------------


def _temperature(conn: sqlite3.Connection, run_id: int, *, low_error: float) -> None:
    for quantity, rank_error in (
        ("temperature_high", 1.0),
        ("temperature_low", low_error),
    ):
        for depth in SIM_DEPTHS:
            _continuous_series(
                conn,
                run_id,
                variable="temperature",
                quantity=quantity,
                entity=("depth", str(depth)),
                error=4.0,
            )
        _continuous_series(
            conn,
            run_id,
            variable="temperature",
            quantity=quantity,
            entity=(DAILY_RANK_ENTITY_TYPE, "1"),
            error=rank_error,
        )


def _temperature_value(conn: sqlite3.Connection, run_id: int) -> str:
    return str(
        cast(
            "dict[str, object]",
            daily_rank_conclusions(conn, run_id, leads=_LEADS)["temperature"],
        )["value"]
    )


def test_temperature_needs_both_components() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _temperature(conn, run_id, low_error=1.0)
    assert _temperature_value(conn, run_id) == "indicated_all_depths"


def test_temperature_one_component_only_is_not_indicated() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    # High is decisively better; low ties the incumbent depths.
    _temperature(conn, run_id, low_error=4.0)
    assert _temperature_value(conn, run_id) == "not_indicated"


#: 24 days: 10 wet (index < 10), 14 dry — clears the 8/8 event minimums.
def _occurrence_outcome(*, predicted_wet: bool, observed_wet: bool) -> str:
    if predicted_wet:
        return "hit" if observed_wet else "false_alarm"
    return "miss" if observed_wet else "correct_negative"


def _precip(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    rank_total_error: float,
    rank_occurrence_skilful: bool,
) -> None:
    for depth in SIM_DEPTHS:
        _continuous_series(
            conn,
            run_id,
            variable="precip",
            quantity="precip_total",
            entity=("depth", str(depth)),
            error=4.0,
        )
    _continuous_series(
        conn,
        run_id,
        variable="precip",
        quantity="precip_total",
        entity=(DAILY_RANK_ENTITY_TYPE, "1"),
        error=rank_total_error,
    )
    for i, date in enumerate(_ADEQUATE):
        observed_wet = i < 10
        for depth in SIM_DEPTHS:
            # The incumbent depths call every day dry: 10 misses, 14 correct
            # negatives, ETS 0.
            _row(
                conn,
                run_id,
                variable="precip",
                quantity="precip_occurrence",
                entity=("depth", str(depth)),
                date=date,
                predicted=0.0,
                truth_value=1.0 if observed_wet else 0.0,
                outcome=_occurrence_outcome(
                    predicted_wet=False, observed_wet=observed_wet
                ),
            )
        predicted_wet = observed_wet if rank_occurrence_skilful else False
        _row(
            conn,
            run_id,
            variable="precip",
            quantity="precip_occurrence",
            entity=(DAILY_RANK_ENTITY_TYPE, "1"),
            date=date,
            predicted=1.0 if predicted_wet else 0.0,
            truth_value=1.0 if observed_wet else 0.0,
            outcome=_occurrence_outcome(
                predicted_wet=predicted_wet, observed_wet=observed_wet
            ),
        )


def _precip_value(conn: sqlite3.Connection, run_id: int) -> str:
    return str(
        cast(
            "dict[str, object]",
            daily_rank_conclusions(conn, run_id, leads=_LEADS)["precip"],
        )["value"]
    )


def test_precip_needs_both_endpoints() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    _precip(conn, run_id, rank_total_error=1.0, rank_occurrence_skilful=True)
    assert _precip_value(conn, run_id) == "indicated_all_depths"


def test_precip_split_endpoint_result_is_not_indicated() -> None:
    conn = asof_conn()
    run_id = _run(conn)
    # Wins the total endpoint, ties the occurrence endpoint (both all-dry).
    _precip(conn, run_id, rank_total_error=1.0, rank_occurrence_skilful=False)
    assert _precip_value(conn, run_id) == "not_indicated"


# ---------------------------------------------------------------------------
# Sample discipline — one pin per endpoint, each with its own falsifier
# ---------------------------------------------------------------------------


def test_continuous_pin_daily_rank_null_inside_the_strict_common_core() -> None:
    """The daily-rank entity is unbuilt on two strict-common-core dates.

    Every depth entity is eligible on all 24 dates, so the strict common
    core is all 24; the daily-rank entity carries ``predicted = None`` on the
    first two. Scoring it over that core is ``float(None)`` — a TypeError,
    not a stricter sample. The pairwise core is the remaining 22 days.
    """
    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    _wind_rank(conn, run_id, 1, 1.0, dates=_ADEQUATE[2:])
    for date in _ADEQUATE[:2]:
        _row(
            conn,
            run_id,
            variable="wind",
            quantity="wind_max",
            entity=(DAILY_RANK_ENTITY_TYPE, "1"),
            date=date,
            predicted=None,
            exclusion="no_samples",
        )
    conclusion = _conclusions(conn, run_id)
    assert conclusion["value"] == "indicated_all_depths"
    comparison = cast(
        "list[dict[str, object]]", _entities(conn, run_id)["1"]["comparisons"]
    )[0]
    per_lead = cast("dict[str, dict[str, object]]", comparison["per_lead"])["1"]
    metrics = cast("dict[str, object]", per_lead["wind_max"])
    assert metrics["dates_n"] == 22
    assert metrics["rank"] == pytest.approx(1.0)
    assert metrics["incumbent"] == pytest.approx(4.0)


def test_occurrence_pin_scores_the_pairwise_core_not_the_common_core() -> None:
    """A value pin: ``_contingency`` skips nulls, so nothing raises.

    Geometry. Depths 1-3 are eligible on days 1-28; depth 4 only on days
    1-24, so the STRICT COMMON CORE is days 1-24. The daily-rank entity is
    unbuilt (``occurrence_outcome IS NULL``) on days 1-2 — inside that core,
    where depth 1 is scored — and eligible on days 3-28, which includes four
    dates OUTSIDE the core.

    Hand-computed contingency for the (daily-rank, depth 1) pair:

    * Pairwise core, days 3-28, n = 26. Daily rank: 10 hits, 2 misses,
      2 false alarms, 12 correct negatives. Random hits 12*12/26 = 5.53846,
      denominator 14 - 5.53846 = 8.46154, **ETS = 0.527273**.
      Depth 1: 7/5/5/9 → random 12*12/26 = 5.53846, denominator
      17 - 5.53846 = 11.46154, **ETS = 0.127517**.
    * Common core minus nulls, days 3-24, n = 22. Daily rank: 10/2/2/8 →
      random 144/22 = 6.54545, denominator 7.45455, **ETS = 0.463415**.
      Depth 1: 7/5/5/5 → random 6.54545, denominator 10.45455,
      **ETS = 0.043478**.

    The two daily-rank values differ (0.527273 vs 0.463415), so asserting
    the pairwise one is not vacuous. Both samples clear the 8/8 event
    minimums and the 20-day floor, so the pin isolates the SAMPLE, not
    adequacy.
    """
    conn = asof_conn()
    run_id = _run(conn)
    rank: EntityId = (DAILY_RANK_ENTITY_TYPE, "1")
    depth1: EntityId = ("depth", "1")

    def outcome_for(index: int, entity: str) -> str:
        # days 3-24 (index 2..23): rank 10 hits / 2 misses / 2 FA / 8 CN,
        # depth 7 hits / 5 misses / 5 FA / 5 CN. Days 25-28: both 4 CN.
        if index >= 24:
            return "correct_negative"
        position = index - 2
        if entity == "rank":
            table = ["hit"] * 10 + ["miss"] * 2 + ["false_alarm"] * 2
        else:
            table = ["hit"] * 7 + ["miss"] * 5 + ["false_alarm"] * 5
        return table[position] if position < len(table) else "correct_negative"

    for index, date in enumerate(_DATES):
        for depth in SIM_DEPTHS:
            eligible = depth != 4 or index < 24
            _row(
                conn,
                run_id,
                variable="precip",
                quantity="precip_occurrence",
                entity=("depth", str(depth)),
                date=date,
                predicted=1.0 if eligible else None,
                truth_value=1.0,
                outcome=outcome_for(index, "depth") if eligible else None,
                exclusion=None if eligible else "no_samples",
            )
        rank_eligible = index >= 2
        _row(
            conn,
            run_id,
            variable="precip",
            quantity="precip_occurrence",
            entity=rank,
            date=date,
            predicted=1.0 if rank_eligible else None,
            truth_value=1.0,
            outcome=outcome_for(index, "rank") if rank_eligible else None,
            exclusion=None if rank_eligible else "no_samples",
        )
    # precip needs both endpoints; give the total endpoint a decisive win so
    # the occurrence numbers are what this pin reads.
    for entity, error in ((depth1, 4.0), (rank, 1.0)):
        _continuous_series(
            conn,
            run_id,
            variable="precip",
            quantity="precip_total",
            entity=entity,
            error=error,
            dates=_DATES[2:],
        )

    conclusion = cast(
        "dict[str, object]",
        daily_rank_conclusions(conn, run_id, leads=_LEADS)["precip"],
    )
    entity_row = cast("list[dict[str, object]]", conclusion["entities"])[0]
    against_depth1 = cast("list[dict[str, object]]", entity_row["comparisons"])[0]
    assert against_depth1["depth"] == 1
    metrics = cast(
        "dict[str, object]",
        cast("dict[str, dict[str, object]]", against_depth1["per_lead"])["1"][
            "precip_occurrence"
        ],
    )
    assert metrics["dates_n"] == 26
    assert metrics["rank"] == pytest.approx(0.527273, abs=1e-6)
    assert metrics["incumbent"] == pytest.approx(0.127517, abs=1e-6)
    # The common-core-minus-nulls values the wrong sample would produce.
    assert metrics["rank"] != pytest.approx(0.463415, abs=1e-6)
    assert metrics["incumbent"] != pytest.approx(0.043478, abs=1e-6)


# ---------------------------------------------------------------------------
# The conclusion reaches BOTH surfaces, and changes no recommendation
# ---------------------------------------------------------------------------


def test_the_conclusion_is_rendered_and_served_identically() -> None:
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.verification.runs import published_run_key
    from wxverify.web.verification import load_verification

    conn = asof_conn()
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    _wind_rank(conn, run_id, 1, 1.0)
    _wind_rank(conn, run_id, 2, 4.0)
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'retain_incumbent', NULL, 2, '{}')
        """,
        (run_id,),
    )
    conn.execute("UPDATE verification_runs SET published_at = '2026-05-29T07:00:00Z'")
    set_runtime_state(conn, published_run_key(1), str(run_id))
    context = load_verification(conn, 1)
    verdicts = cast("list[dict[str, object]]", context["verdicts"])
    page_value = cast("dict[str, object]", verdicts[0]["ranking_redesign_indicated"])
    assert page_value["value"] == "indicated"
    # The verdict itself is untouched by the diagnostic.
    assert verdicts[0]["outcome"] == "retain_incumbent"
    assert verdicts[0]["recommended_depth"] is None


async def _idle_worker(_db: object) -> None:  # pragma: no cover - never awaited
    import asyncio

    await asyncio.Event().wait()


def test_the_page_and_the_verdicts_api_state_the_same_conclusion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§10: a first-class conclusion line, and the API field beside it."""
    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.verification.runs import published_run_key

    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(config.db_path)
    conn = db._conn  # noqa: SLF001
    run_id = _run(conn)
    _wind_depths(conn, run_id)
    _wind_rank(conn, run_id, 1, 1.0)
    _wind_rank(conn, run_id, 2, 4.0)
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'retain_incumbent', NULL, 2, '{}')
        """,
        (run_id,),
    )
    conn.execute("UPDATE verification_runs SET published_at = '2026-05-29T07:00:00Z'")
    set_runtime_state(conn, published_run_key(1), str(run_id))
    conn.commit()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get("/verification?site=1")
        payload = client.get(f"/api/verification/runs/{run_id}/verdicts")
    assert page.status_code == 200
    assert payload.status_code == 200

    served = cast("list[dict[str, Any]]", payload.json()["verdicts"])[0]
    conclusion = cast("dict[str, Any]", served["ranking_redesign_indicated"])
    assert conclusion["value"] == "indicated"
    assert conclusion["selected_entity_key"] == "1"
    assert conclusion["entities_examined"] == 2
    # The page states it as a conclusion line, with the disclosure the
    # payload carries — not only in the diagnostics table.
    assert 'data-ranking="indicated"' in page.text
    line = page.text.split('data-ranking="indicated"', 1)[1].split("</p>", 1)[0]
    assert "2</span> daily-rank blends examined" in line
    assert "selected after the fact" in line
