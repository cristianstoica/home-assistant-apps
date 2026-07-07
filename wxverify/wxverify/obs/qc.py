"""Observation quality control."""

from __future__ import annotations

TARGET_VARIABLES = frozenset({"temperature", "wind", "precip"})


def qc_flag(variable: str, value: float, previous_value: float | None = None) -> str:
    if variable == "temperature" and not -90 <= value <= 60:
        return "range"
    if variable == "wind" and not 0 <= value <= 80:
        return "range"
    if variable == "precip" and not 0 <= value <= 500:
        return "range"
    if previous_value is not None and abs(value - previous_value) > _spike_limit(
        variable
    ):
        return "spike"
    return "ok"


def _spike_limit(variable: str) -> float:
    if variable == "temperature":
        return 15.0
    if variable == "wind":
        return 35.0
    return 100.0
