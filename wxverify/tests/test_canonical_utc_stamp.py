"""Unit oracle for ``is_canonical_utc_stamp`` (O1).

The item-12 truth table (plan `2026-09-23-db-import-timestamp-validation.md`
Sec 4) as literal ``(value, expected)`` pairs, plus the non-``str`` guard
cases. Every value here is written by hand -- never built with
``isoformat_utc`` or any other production constant -- so the oracle cannot
share the formatter it is checking.
"""

from __future__ import annotations

import pytest

from wxverify.core.timeutil import is_canonical_utc_stamp

_CASES: tuple[tuple[str, object, bool], ...] = (
    # -- 1 / ok / False: parses, but not the canonical spelling ------------
    ("lower_t", "2026-01-01t06:00:00Z", False),
    # -- 1 / raises / False: passes the LIKE shape filter, but does not parse
    ("lower_z", "2026-01-01T06:00:00z", False),
    ("hour_24", "2026-01-01T24:00:00Z", False),
    ("second_60", "2026-01-01T06:00:60Z", False),
    ("feb_30", "2026-02-30T00:00:00Z", False),
    ("feb_29_non_leap", "2025-02-29T00:00:00Z", False),
    ("month_13", "2026-13-01T00:00:00Z", False),
    ("garbage", "abcd-ef-ghTij:kl:mnZ", False),
    # -- 0 / ok / False: parses to a real instant, wrong spelling -----------
    ("micro_half", "2026-01-01T06:00:00.500000Z", False),
    ("micro_zero", "2026-01-01T06:00:00.000000Z", False),
    ("micro_short", "2026-01-01T06:00:00.5Z", False),
    ("offset_zero", "2026-01-01T06:00:00+00:00", False),
    ("space_separator", "2026-01-01 06:00:00Z", False),
    ("naive", "2026-01-01T06:00:00", False),
    ("offset_nonzero", "2026-01-01T06:00:00+01:00", False),
    # -- 0 / raises / False --------------------------------------------------
    ("trailing_newline", "2026-01-01T06:00:00Z\n", False),
    # -- 1 / ok / True: canonical, including the boundaries -----------------
    ("half_hour", "2026-01-01T00:30:00Z", True),
    ("leap_day", "2024-02-29T00:00:00Z", True),
    ("year_min", "0001-01-01T00:00:00Z", True),
    ("year_max", "9999-12-31T23:59:59Z", True),
    # -- str / raises / False: an empty string still passes the isinstance
    # -- guard, and is rejected only because parse_utc raises on it ---------
    ("empty_string", "", False),
    # -- non-str: rejected solely by the isinstance guard --------------------
    ("none", None, False),
    ("int_zero", 0, False),
    ("bytes", b"2026-01-01T00:00:00Z", False),
)


@pytest.mark.parametrize(
    "value, expected", [(v, e) for _id, v, e in _CASES], ids=[c[0] for c in _CASES]
)
def test_is_canonical_utc_stamp(value: object, expected: bool) -> None:
    assert is_canonical_utc_stamp(value) is expected
