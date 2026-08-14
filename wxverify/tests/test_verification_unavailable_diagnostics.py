"""§14/W11 change 3: the unavailable-diagnostics declaration reconciled.

Two kinds of "not shown" are kept apart, because conflating them is the
false declaration W11 exists to remove:

* a family methodology v1 does not define at all (``wet_hour_share``), and
* a family it does define but which THIS run has no data for.

A metric the specification calls always-displayed and the code merely does
not implement belongs in neither list — it is a gap to close, which is why
the observed-wet precip-total MAE (§14a/W13) must never appear here.
"""

from __future__ import annotations

from typing import Any

from wxverify.web.verification import (
    _DATA_UNAVAILABLE_DIAGNOSTICS,  # noqa: SLF001
    UNAVAILABLE_DIAGNOSTICS,
    _diagnostics,  # noqa: SLF001
)

_DAY_CONTEXT: dict[str, object] = {
    "snapshot_days": 3,
    "days_with_exclusions": 0,
    "null_availability_samples": 0,
}

_FAMILY_ROWS: dict[str, dict[str, Any]] = {
    "d0": {"entity_type": "depth", "lead": 0, "quantity": "temperature_high"},
    "bias_rmse": {
        "entity_type": "depth",
        "lead": 1,
        "quantity": "temperature_high",
        "bias": 0.4,
    },
    "contingency": {
        "entity_type": "depth",
        "lead": 1,
        "quantity": "precip_occurrence",
        "hits": 4,
    },
    "daily_rank": {
        "entity_type": "daily_rank_depth",
        "lead": 1,
        "quantity": "wind_max",
    },
    "feeds": {
        "entity_type": "feed",
        "lead": 1,
        "quantity": "wind_max",
        "availability_rate": 0.9,
        "headline": 1,
    },
}


def _row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "variable": "temperature",
        "entity_type": "depth",
        "entity_key": "2",
        "lead": 1,
        "quantity": "temperature_high",
        "bias": None,
        "hits": None,
        "availability_rate": None,
        "headline": 1,
        "common_days": 20,
        "detail": None,
    }
    base.update(overrides)
    return base


def _unavailable_keys(results: list[dict[str, object]]) -> set[str]:
    out = _diagnostics(results, _DAY_CONTEXT)
    items = out["unavailable"]
    assert isinstance(items, list)
    return {str(item["key"]) for item in items}


def test_a_run_with_no_results_declares_every_empty_family_with_a_reason() -> None:
    """Kills the shipped behaviour: five families could come up empty and
    the page declared none of them, so an empty section read as an
    unimplemented one."""
    out = _diagnostics([], _DAY_CONTEXT)
    items = out["unavailable"]
    assert isinstance(items, list)

    keys = {str(item["key"]) for item in items}
    assert keys == {"wet_hour_share", *_DATA_UNAVAILABLE_DIAGNOSTICS}
    for item in items:
        assert str(item["label"]).strip()
        assert str(item["reason"]).strip()


def test_a_populated_family_is_not_declared_unavailable() -> None:
    """Negative control: the declaration is data-driven, not unconditional."""
    for key, overrides in _FAMILY_ROWS.items():
        keys = _unavailable_keys([_row(**overrides)])
        assert key not in keys, f"{key} is populated but declared unavailable"
        assert keys == {"wet_hour_share", *_DATA_UNAVAILABLE_DIAGNOSTICS} - {key}


def test_every_produced_family_has_a_registered_data_reason() -> None:
    """The reconciliation itself: the declaration table and the families
    `_diagnostics` actually produces are the same set, so adding a family
    without a reason fails here rather than shipping an unexplained gap."""
    out = _diagnostics([], _DAY_CONTEXT)
    families = {
        key
        for key, value in out.items()
        if isinstance(value, list) and key != "unavailable"
    }
    assert families == set(_DATA_UNAVAILABLE_DIAGNOSTICS)


def test_the_observed_wet_precip_mae_is_never_declared_unavailable() -> None:
    """§14a is an implemented metric with its own empty-state rendering.
    Declaring it unavailable would be exactly the false declaration W11
    removes."""
    declared = {item["key"] for item in UNAVAILABLE_DIAGNOSTICS}
    declared |= set(_DATA_UNAVAILABLE_DIAGNOSTICS)
    assert "observed_wet_precip_mae" not in declared
    assert _unavailable_keys([]) == declared
