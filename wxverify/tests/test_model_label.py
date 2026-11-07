"""Tests for ``wxverify.web.context.model_label`` (card 7, step 6
adjustment 4): the Forecast page's model-row label, "model name only, never
source / model". Pure -- no database.

The 13 known pairs' expected names are hardcoded here as literals, not read
from the private ``_MODEL_NAMES`` table: comparing the function's output to
the table it is built from would be tautological and could never catch an
edit to that table.
"""

from __future__ import annotations

import pytest

from wxverify.config import FEED_SEEDS
from wxverify.web.context import feed_label, model_label

# The 13 (source, model) pairs model_label names explicitly, and the exact
# text it must give -- independent literals, not _MODEL_NAMES's own values.
_KNOWN_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("open-meteo", "ecmwf_ifs", "ECMWF IFS"),
    ("open-meteo", "gfs_global", "GFS Global"),
    ("open-meteo", "icon_global", "ICON Global"),
    ("open-meteo", "gem_global", "GEM Global"),
    ("open-meteo", "meteofrance_arpege_world", "ARPEGE World"),
    ("open-meteo", "jma_gsm", "JMA GSM"),
    ("open-meteo", "ukmo_global_deterministic_10km", "UKMO Global 10 km"),
    ("open-meteo", "icon_eu", "ICON-EU"),
    ("visualcrossing", "blend", "Visual Crossing"),
    ("openweathermap", "blend", "OpenWeatherMap"),
    ("weatherapi", "blend", "WeatherAPI"),
    ("meteosource", "blend", "Meteosource"),
    ("google", "blend", "Google"),
)


@pytest.mark.parametrize(
    ("source", "model", "expected"),
    _KNOWN_PAIRS,
    ids=[f"{source}-{model}" for source, model, _expected in _KNOWN_PAIRS],
)
def test_known_pairs_give_their_model_name(
    source: str, model: str, expected: str
) -> None:
    """Each of the 13 pairs in _MODEL_NAMES gives its name, read through
    model_label itself."""
    assert model_label(source, model) == expected
    assert len(_KNOWN_PAIRS) == 13


def test_a_meteoblue_package_member_gives_its_own_model_name_unchanged() -> None:
    """A meteoblue member, such as ("meteoblue", "GFS05"), gives its stored
    model name unchanged -- it is not in _MODEL_NAMES, so model_label falls
    through to the raw model, that provider's own name."""
    assert model_label("meteoblue", "GFS05") == "GFS05"


@pytest.mark.parametrize(
    ("source", "model"),
    [
        ("meteoblue", "multimodel"),
        ("virtual", "_persistence"),
        ("virtual", "_multimodel_mean"),
    ],
)
def test_the_package_feed_and_virtual_feeds_give_feed_labels_text(
    source: str, model: str
) -> None:
    """("meteoblue", "multimodel") and the two virtual feeds give exactly
    feed_label's text."""
    assert model_label(source, model) == feed_label(source, model)


def test_every_feed_seed_label_never_contains_the_source_separator() -> None:
    """For every feed seeded by config.FEED_SEEDS, the label never contains
    " / " -- the one thing feed_label can produce that model_label must
    not."""
    assert len(FEED_SEEDS) > 0
    for seed in FEED_SEEDS:
        label = model_label(seed.source, seed.model)
        assert " / " not in label, (seed.source, seed.model, label)
