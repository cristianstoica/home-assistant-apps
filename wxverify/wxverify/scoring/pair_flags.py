"""Shared pair-derived categorical flags."""

from __future__ import annotations


def precip_flags(
    variable: str, forecast: float, observed: float, threshold: float | None
) -> tuple[int | None, int | None, int | None, int | None]:
    if variable != "precip" or threshold is None:
        return None, None, None, None
    forecast_event = forecast >= threshold
    observed_event = observed >= threshold
    return (
        1 if forecast_event and observed_event else 0,
        1 if forecast_event and not observed_event else 0,
        1 if not forecast_event and observed_event else 0,
        1 if not forecast_event and not observed_event else 0,
    )
