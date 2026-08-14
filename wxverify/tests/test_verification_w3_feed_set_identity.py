"""§17 family 4 (W3): one feed set describes a scored entity end to end.

0.11.0 computed the DISPLAYED daily value over the >= 18-hour clearing
subset but blended the hourly series, counted covered hours and reported
the contributor count over every feed that had samples. A day could then
be scored on a three-feed high while its occurrence class, coverage and
contributor count described a five-feed product.

These oracles drive ``_entities_for_selection`` directly — it is pure over
its arguments, so the whole discriminating fixture is five in-memory feed
sample lists and no database at all. Every expected value below is
hand-computed from the code path (clearing subset -> blend ->
``evaluate_variable``), never transcribed from a run.

All fixture identities are synthetic: feed ids 101-105, a fixed-offset
``Etc/GMT+7`` timezone (no DST fold to reason about), and a 2035 date.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from wxverify.forecast.data import FutureSampleRow
from wxverify.verification.coverage import (
    EXCLUDE_BELOW_NEAR_COMPLETE,
    QUANTITY_PRECIP_OCCURRENCE,
    QUANTITY_PRECIP_TOTAL,
    evaluate_variable,
)
from wxverify.verification.simulate import (
    _blend_hourly,  # pyright: ignore[reportPrivateUsage]
    _entities_for_selection,  # pyright: ignore[reportPrivateUsage]
    _Entity,  # pyright: ignore[reportPrivateUsage]
)

#: Fixed-offset zone: local midnight is always 07:00Z, the local day is
#: always 24 UTC instants, and the near-complete gate is always 23.
_TZ = "Etc/GMT+7"
_TARGET = date(2035, 6, 15)
_LOCAL_MIDNIGHT_UTC = datetime(2035, 6, 15, 7, tzinfo=UTC)
_THRESHOLD_MM = 0.2

_THICK_IDS = [101, 102, 103]
_THIN_IDS = [104, 105]


def _samples(feed_id: int, local_hours: range, value: float) -> list[FutureSampleRow]:
    """One feed's hourly samples at the given LOCAL hours of the target day."""
    return [
        FutureSampleRow(
            feed_id=feed_id,
            source="example-src",
            model="model-x",
            variable="precip",
            issued_at="2035-06-14T06:00:00Z",
            valid_at=(_LOCAL_MIDNIGHT_UTC + timedelta(hours=hour))
            .isoformat()
            .replace("+00:00", "Z"),
            value=value,
        )
        for hour in local_hours
    ]


def _build(
    feeds_samples: dict[int, list[FutureSampleRow]], feed_ids: list[int]
) -> dict[str, _Entity]:
    built = _entities_for_selection(
        entity_type="depth",
        entity_key="3",
        variable="precip",
        feed_ids=feed_ids,
        feeds_samples=feeds_samples,
        timezone=_TZ,
        target_date=_TARGET,
        rain_threshold_mm=_THRESHOLD_MM,
    )
    return dict(built)


def test_the_scored_entity_describes_the_clearing_subset_alone() -> None:
    """W3: coverage, occurrence and the contributor count come from the same
    feeds the displayed value does — the >= 18-hour clearing subset.

    Fixture (hand-built to flip the occurrence class): three feeds cover
    local hours 0-22 (23 hours each, clearing) with 0.0 mm everywhere; two
    feeds cover local hour 23 alone (1 hour each, NOT clearing) with
    5.0 mm. The clearing subset is the three thick feeds, so:

    * blended hourly series = 23 instants, all 0.0;
    * covered hours = 23, which is exactly the near-complete threshold
      (24 expected - 1), so the day is eligible;
    * zero wet slots and near-complete coverage => occurrence 0.0, DRY;
    * realized contributors = 3.

    Kills the 0.11.0 implementation, whose ``_blend_hourly(present, ...)``
    and ``realized_contributors=len(present)`` swept the two thin feeds in:
    24 covered hours, one wet slot at local hour 23 (mean(5.0, 5.0) = 5.0
    >= 0.2), occurrence 1.0 (WET) and 5 contributors — a wet, five-feed
    verdict attached to a dry three-feed value. Also kills a mutant that
    keeps the hourly blend on ``agg_ids`` but reverts either the covered
    hour count or the contributor count alone.
    """
    feeds_samples = {fid: _samples(fid, range(23), 0.0) for fid in _THICK_IDS}
    for fid in _THIN_IDS:
        feeds_samples[fid] = _samples(fid, range(23, 24), 5.0)
    all_ids = _THICK_IDS + _THIN_IDS

    built = _build(feeds_samples, all_ids)
    occurrence = built[QUANTITY_PRECIP_OCCURRENCE]
    total = built[QUANTITY_PRECIP_TOTAL]

    # The scored feed set is the clearing subset, on every axis at once.
    assert occurrence.realized_contributors == 3
    assert total.realized_contributors == 3
    assert occurrence.covered_hours == 23
    assert total.covered_hours == 23
    # Occurrence follows the three-feed blend: dry, and eligible because 23
    # covered hours meet the near-complete gate.
    assert occurrence.predicted == 0.0
    assert occurrence.forecast_eligible is True
    assert occurrence.forecast_exclusion_reason is None
    # The displayed total was already computed over the clearing subset in
    # 0.11.0 and is unchanged — which is the point: the value said dry while
    # the qualifiers said wet.
    assert total.predicted == 0.0
    assert total.forecast_eligible is True

    # Fixture validity: the excluded feeds really would flip the class, so
    # the assertions above are not green by coincidence.
    five_feed = _blend_hourly(all_ids, feeds_samples)
    assert len(five_feed) == 24
    assert five_feed[-1][1] == 5.0
    counterfactual = {
        outcome.quantity: outcome
        for outcome in evaluate_variable(
            "precip",
            five_feed,
            timezone=_TZ,
            local_date=_TARGET,
            rain_threshold_mm=_THRESHOLD_MM,
        )
    }
    assert counterfactual[QUANTITY_PRECIP_OCCURRENCE].value == 1.0
    assert counterfactual[QUANTITY_PRECIP_OCCURRENCE].covered_hours == 24


def test_no_feed_clearing_keeps_every_selected_feed_in_the_scored_set() -> None:
    """W3's paired positive: when NO feed clears the 18-hour guard the
    clearing subset falls back to the whole selection, and the scored
    entity then describes all five feeds — the fix narrows the feed set
    only where the display already narrowed it.

    Fixture: three feeds cover local hours 0-9 (10 hours, 0.0 mm) and two
    cover local hours 10-13 (4 hours, 5.0 mm). Nothing clears, so
    ``clearing_subset`` returns all five with ``partial=True`` and the
    blend spans 14 distinct instants: 10 dry, 4 at mean(5.0, 5.0) = 5.0.
    Hand-computed consequences: covered hours 14, contributors 5, four wet
    slots => occurrence 1.0 (one wet slot proves wet at any coverage),
    total 14 < 23 so ineligible with ``below_near_complete``, and the
    displayed total = blend_mean([0, 0, 0, 20, 20]) = 8.0.

    Kills a fix-overreach mutant that drops the ``partial`` fallback (e.g.
    scoring ``[fid for fid in present if clears_coverage(...)]`` directly):
    that yields an empty feed set, no blend, contributors 0.
    """
    feeds_samples = {fid: _samples(fid, range(10), 0.0) for fid in _THICK_IDS}
    for fid in _THIN_IDS:
        feeds_samples[fid] = _samples(fid, range(10, 14), 5.0)
    all_ids = _THICK_IDS + _THIN_IDS

    built = _build(feeds_samples, all_ids)
    occurrence = built[QUANTITY_PRECIP_OCCURRENCE]
    total = built[QUANTITY_PRECIP_TOTAL]

    assert occurrence.realized_contributors == 5
    assert total.realized_contributors == 5
    assert occurrence.covered_hours == 14
    assert total.covered_hours == 14
    assert occurrence.predicted == 1.0
    assert occurrence.forecast_eligible is True
    assert total.predicted == 8.0
    assert total.forecast_eligible is False
    assert total.forecast_exclusion_reason == EXCLUDE_BELOW_NEAR_COMPLETE
