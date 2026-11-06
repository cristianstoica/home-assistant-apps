"""Drift guard for the read-path grid's variable axis.

The equivalence tests that used to live here (``_expected_active_cells``,
``_expected_active_feed_ids`` and ``_scoring_feeds`` each checked against an
independent DISTINCT-form reference query) all ran composite/leaderboard
helpers with the daily close off (no ``day_close_site`` call, and
``_scoring_feeds(close_on=False)`` explicitly) -- the add-on's only install
runs with the close on, where ``composite_with_status``/
``leaderboard_with_status`` take the published-only branch instead
(composite.py/leaderboard.py). Removed along with their three
DISTINCT-reference helpers and the stray/out-of-range score_cache guard
tests, which exercised the same close-off cache-backed path.

``test_cell_grid_lead_bound_matches_schema_check`` pinned
``_expected_active_cells``/``cell_grid_cte()``, the grid+EXISTS rewrite's own
candidate enumeration -- both removed with the old cache-backed read path
(storage-release plan 2026-11-02, table 1.B). Removed with them.

Synthetic data only (public repo): fake site name, the repo's existing
40/-105 lat-lon convention, no real station/device identifiers.
"""

from __future__ import annotations

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.forecast.service import VARIABLES as FORECAST_SERVICE_VARIABLES
from wxverify.obs.qc import TARGET_VARIABLES


def test_cell_grid_covers_every_variable_the_pairing_writers_emit() -> None:
    """``FORECAST_VARIABLES`` is the shared variable axis; every other
    module that materializes/consumes a (variable, ...) cell must agree, or
    a read path keyed on one of the others would silently exclude a
    variable a writer actually produces."""
    assert set(FORECAST_VARIABLES) == set(FORECAST_SERVICE_VARIABLES)
    assert set(FORECAST_VARIABLES) == set(TARGET_VARIABLES)
