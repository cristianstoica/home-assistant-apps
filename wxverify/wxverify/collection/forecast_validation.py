"""Shared forecast-sample validation predicates."""

from __future__ import annotations

from typing import Final

FORECAST_VARIABLES: Final[tuple[str, ...]] = ("temperature", "wind", "precip")
FORECAST_VALUE_RANGES: Final[dict[str, tuple[float, float]]] = {
    "temperature": (-90.0, 70.0),
    "wind": (0.0, 150.0),
    "precip": (0.0, 500.0),
}
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
