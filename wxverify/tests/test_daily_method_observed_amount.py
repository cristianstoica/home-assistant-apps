"""§15.2 rows 39-41: the owner's observed amount (§6.6), two-largest and one.

Expected values derived directly from `observed_amount`'s documented rule:
(h1+h2)/200 with two or more contributing stations, h1/100 with one, 0.0
for a dry day and None for unknown.
"""

from __future__ import annotations

import pytest

from wxverify.forecast.daily_method import StationDay, observed_amount


def _station(sid: str, mm: float, hours: int) -> StationDay:
    return StationDay(
        station_id=sid,
        hundredths=round(mm * 100),
        hours=hours,
        invalid=False,
        quarantined=False,
    )


def test_amount_two_largest() -> None:
    """Row 39: contributing totals 21.59, 0.5, 0.3mm and a fourth at 0.1mm.

    min_hours=1, wet_hundredths=20: the 0.1mm station (10 hundredths) does
    not clear the wet line, so only the first three contribute.
    (2159 + 50) / 200 = 11.045 mm, from the two largest.
    """
    stations = [
        _station("SYN-1", 21.59, 24),
        _station("SYN-2", 0.5, 24),
        _station("SYN-3", 0.3, 24),
        _station("SYN-4", 0.1, 24),
    ]
    amount = observed_amount(
        stations, outcome="rain_nearby", min_hours=1, wet_hundredths=20
    )
    assert amount.amount == pytest.approx(11.045)  # pyright: ignore[reportUnknownMemberType]
    assert [c[0] for c in amount.contributing] == ["SYN-1", "SYN-2", "SYN-3"]


def test_amount_one_contributing_station() -> None:
    """Row 40: SYN-1 0.5mm, SYN-2 0.1mm, SYN-3 0.0 -- one_at_0.5.

    wet_hundredths=50: only SYN-1 (50) clears it; SYN-2's 10 does not
    contribute (M49's bug would let it in and average to 0.3).
    """
    stations = [
        _station("SYN-1", 0.5, 24),
        _station("SYN-2", 0.1, 24),
        _station("SYN-3", 0.0, 24),
    ]
    amount = observed_amount(
        stations, outcome="rain_nearby", min_hours=1, wet_hundredths=50
    )
    assert amount.amount == pytest.approx(0.5)  # pyright: ignore[reportUnknownMemberType]
    assert len(amount.contributing) == 1


def test_amount_dry_and_unknown() -> None:
    """Row 41: the "one alone below 0.5mm" day, then row 17's second day.

    The dry day (row 14: SYN-1 0.25mm/24h, SYN-2/SYN-3 0.0mm/24h) gives
    amount 0.0, with SYN-1's 0.25 mm kept visible in ``contributing`` at a
    wet line it clears (20 hundredths) even though the amount itself is
    forced to 0.0 by the dry outcome. Row 17's second day (all three
    0.0mm/21h, min_hours=22, so none are adequate) is unknown -> None.
    """
    dry_stations = [
        _station("SYN-1", 0.25, 24),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
    ]
    dry_amount = observed_amount(
        dry_stations, outcome="dry", min_hours=1, wet_hundredths=20
    )
    assert dry_amount.amount == pytest.approx(0.0)  # pyright: ignore[reportUnknownMemberType]
    assert dry_amount.contributing == (("SYN-1", 25, 24),)

    unknown_stations = [
        _station("SYN-1", 0.0, 21),
        _station("SYN-2", 0.0, 21),
        _station("SYN-3", 0.0, 21),
    ]
    unknown_amount = observed_amount(
        unknown_stations, outcome="unknown", min_hours=22, wet_hundredths=50
    )
    assert unknown_amount.amount is None
