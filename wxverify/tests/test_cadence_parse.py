"""Direct unit tests for ``worker.cadence.parse_fetch_interval_minutes``.

Every hostile carrier the module's docstring promises to fail closed on,
plus the integral-``float()``-reading contract: a REAL with a fractional
component must be rejected (not floored), while an exact-integral REAL is a
legitimate whole-number representation and must still be accepted. A decimal
spelling within half an ulp of an integer rounds onto it in the ``float()``
read and is therefore accepted -- pinned below as documented behaviour.

Synthetic values only -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import math

import pytest

from wxverify.worker.cadence import (
    MAX_FETCH_INTERVAL_MINUTES,
    parse_fetch_interval_minutes,
)


def test_accepts_plain_int() -> None:
    assert parse_fetch_interval_minutes(60, context="t") == 60


def test_accepts_integral_text() -> None:
    assert parse_fetch_interval_minutes("60", context="t") == 60


def test_accepts_integral_text_with_decimal_spelling() -> None:
    """A TEXT column can carry a decimal spelling of a whole number (e.g.
    an imported or hand-edited '360.0'); it denotes 360 exactly and must be
    accepted, not rejected for the carrier alone."""
    assert parse_fetch_interval_minutes("360.0", context="t") == 360


def test_accepts_integral_blob_with_decimal_spelling() -> None:
    """A BLOB spelling of a decimal whole number (e.g. an imported or
    hand-edited b'360.0') must be accepted on the same basis as its TEXT
    equivalent -- not rejected for the carrier alone."""
    assert parse_fetch_interval_minutes(b"360.0", context="t") == 360


def test_accepts_integral_text_in_exponent_notation() -> None:
    """An exponent spelling that denotes a whole number exactly (e.g.
    '1e3' == 1000) must be accepted."""
    assert parse_fetch_interval_minutes("1e3", context="t") == 1000


def test_accepts_exact_integral_real() -> None:
    """A REAL-typed column holding a whole number (e.g. 360.0) is a
    legitimate representation and must convert to 360, not be rejected."""
    assert parse_fetch_interval_minutes(360.0, context="t") == 360


def test_rejects_non_integral_real(caplog: pytest.LogCaptureFixture) -> None:
    """A fractional REAL (e.g. an imported or hand-edited 1.9) must be
    rejected outright, never silently floored to 1."""
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        result = parse_fetch_interval_minutes(1.9, context="t")
    assert result is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "unreadable fetch_interval_minutes" in warnings[0].getMessage()


def test_rejects_non_integral_real_large_but_in_range(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A large fractional value that would otherwise fall inside the
    range-check band must still be rejected for its fractional part, not
    floored into an accepted whole number."""
    value = MAX_FETCH_INTERVAL_MINUTES - 0.5
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        result = parse_fetch_interval_minutes(value, context="t")
    assert result is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "unreadable fetch_interval_minutes" in warnings[0].getMessage()


def test_rejects_non_integral_real_that_truncates_onto_the_ceiling(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sharpest instance of the defect: a fractional value whose truncation
    lands EXACTLY on ``MAX_FETCH_INTERVAL_MINUTES`` (the old ``int(value)``
    behavior silently accepted this as a legitimate at-ceiling cadence). It
    must still be rejected for its fractional part, not floored into the
    ceiling."""
    value = MAX_FETCH_INTERVAL_MINUTES + 0.9
    assert int(value) == MAX_FETCH_INTERVAL_MINUTES  # sanity: truncation lands on it
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        result = parse_fetch_interval_minutes(value, context="t")
    assert result is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "unreadable fetch_interval_minutes" in warnings[0].getMessage()


def test_rejects_non_integral_text() -> None:
    assert parse_fetch_interval_minutes("1.9", context="t") is None


def test_rejects_none() -> None:
    assert parse_fetch_interval_minutes(None, context="t") is None


def test_rejects_garbage_text() -> None:
    assert parse_fetch_interval_minutes("abc", context="t") is None


def test_rejects_blob() -> None:
    assert parse_fetch_interval_minutes(b"\x01\x02", context="t") is None


def test_rejects_infinity() -> None:
    assert parse_fetch_interval_minutes(math.inf, context="t") is None


def test_rejects_nan() -> None:
    assert parse_fetch_interval_minutes(math.nan, context="t") is None


def test_rejects_zero(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        result = parse_fetch_interval_minutes(0, context="t")
    assert result is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "out-of-range fetch_interval_minutes" in warnings[0].getMessage()


def test_rejects_negative() -> None:
    assert parse_fetch_interval_minutes(-1, context="t") is None


def test_rejects_above_ceiling() -> None:
    assert (
        parse_fetch_interval_minutes(MAX_FETCH_INTERVAL_MINUTES + 1, context="t")
        is None
    )


def test_accepts_at_ceiling() -> None:
    assert (
        parse_fetch_interval_minutes(MAX_FETCH_INTERVAL_MINUTES, context="t")
        == MAX_FETCH_INTERVAL_MINUTES
    )


def test_accepts_text_within_half_ulp_of_an_integer() -> None:
    """Documents the float-rounding boundary as accepted behaviour: the
    integrality test runs on the ``float()`` reading, so a decimal spelling
    the binary conversion snaps onto an integer is accepted at that integer.
    A future silent tightening of this boundary must fail here, visibly."""
    assert float("360.0000000000000001") == 360.0  # sanity: the snap itself
    assert parse_fetch_interval_minutes("360.0000000000000001", context="t") == 360
