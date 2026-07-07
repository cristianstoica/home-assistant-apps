"""Unit conversion helpers."""

from __future__ import annotations


def k_to_c(value: float) -> float:
    return value - 273.15


def kmh_to_ms(value: float) -> float:
    return value / 3.6


def mm(value: float) -> float:
    return value
