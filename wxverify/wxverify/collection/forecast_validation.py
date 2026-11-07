"""Shared forecast-sample validation predicates."""

from __future__ import annotations

from typing import Final

from wxverify.settings.depth import DEPTH_VARIABLES

#: The canonical roster (NB-4) — sample validation accepts exactly the
#: variables the rest of the pipeline scores.
FORECAST_VARIABLES: Final[tuple[str, ...]] = DEPTH_VARIABLES
FORECAST_VALUE_RANGES: Final[dict[str, tuple[float, float]]] = {
    "temperature": (-90.0, 70.0),
    "wind": (0.0, 150.0),
    "precip": (0.0, 500.0),
}
#: The display roster (card 7): stored in ``forecast_samples`` for the
#: Forecast page only, never scored. Cloud cover in %, gust in m/s, wind
#: direction in degrees the wind comes from.
DISPLAY_VARIABLES: Final[tuple[str, ...]] = (
    "cloud_cover",
    "wind_gust",
    "wind_direction",
)
DISPLAY_VALUE_RANGES: Final[dict[str, tuple[float, float]]] = {
    "cloud_cover": (0.0, 100.0),
    "wind_gust": (0.0, 150.0),
    "wind_direction": (0.0, 360.0),
}
#: Shape pre-filter for reads, not a validity proof: LIKE is ASCII
#: case-insensitive and ``_`` matches any character. Imports are checked by
#: value (``is_canonical_utc_stamp``). This text generates
#: ``idx_samples_invalid``; editing it rebuilds that index at boot.
FORECAST_TIMESTAMP_LIKE: Final = "____-__-__T__:__:__Z"


def invalid_forecast_sample_sql(alias: str = "forecast_samples") -> str:
    prefix = f"{alias}." if alias else ""
    variable_checks = " OR ".join(
        f"({prefix}variable='{variable}' AND "
        f"({prefix}value < {low:g} OR {prefix}value > {high:g}))"
        for variable, (low, high) in FORECAST_VALUE_RANGES.items()
    )
    variables = ", ".join(f"'{variable}'" for variable in FORECAST_VARIABLES)
    return f"""(
        {variable_checks}
        OR {prefix}variable NOT IN ({variables})
        OR {prefix}lead_hours < 1
        OR {prefix}issued_at NOT LIKE '{FORECAST_TIMESTAMP_LIKE}'
        OR {prefix}valid_at NOT LIKE '{FORECAST_TIMESTAMP_LIKE}'
    )"""


def invalid_stored_sample_sql(alias: str = "forecast_samples") -> str:
    """The invalid-row predicate over everything ``forecast_samples`` may hold.

    True for a row outside both rosters, a row of either roster out of its
    value range (``FORECAST_VALUE_RANGES``, ``DISPLAY_VALUE_RANGES``), a lead
    under 1 hour, or a malformed stamp. The scored readers keep
    :func:`invalid_forecast_sample_sql`, which rejects every display row;
    this one generates ``idx_samples_invalid`` and the Ops bad-sample count,
    so valid display rows count as good there. Its last three clauses are
    the scored predicate's, word for word.
    """
    prefix = f"{alias}." if alias else ""
    ranges = {**FORECAST_VALUE_RANGES, **DISPLAY_VALUE_RANGES}
    variable_checks = " OR ".join(
        f"({prefix}variable='{variable}' AND "
        f"({prefix}value < {low:g} OR {prefix}value > {high:g}))"
        for variable, (low, high) in ranges.items()
    )
    variables = ", ".join(
        f"'{variable}'" for variable in FORECAST_VARIABLES + DISPLAY_VARIABLES
    )
    return f"""(
        {variable_checks}
        OR {prefix}variable NOT IN ({variables})
        OR {prefix}lead_hours < 1
        OR {prefix}issued_at NOT LIKE '{FORECAST_TIMESTAMP_LIKE}'
        OR {prefix}valid_at NOT LIKE '{FORECAST_TIMESTAMP_LIKE}'
    )"""
