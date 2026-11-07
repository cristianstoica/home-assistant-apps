"""Phase 8 Round C — the §18 named oracle families for 0.11.0.

One family lives here now, pinned against the mutation it exists to kill
(named in the test docstring, not just in a report).

* **NB-4** — the canonical depth/variable roster. ``DEPTH_VARIABLES`` and its
  three derived aliases, plus ``depth_option_values()``'s key set, pinned
  with tuple/set EQUALITY so an added, removed, renamed or REORDERED
  variable in one module without the others fails loudly.

Family 2 (NB-1, ``_contingency``/``_continuous_metrics`` fail-closed on a
partial common core) was removed: both were called only from
``verification.engine._write_result``, reached only from ``aggregate_run``,
the nightly replay function. Family 3 (NB-9, ``current_result_basis_
fingerprint``'s read-only purity) and Family 4/5 (the §16 element-presence
oracle over the ``/verification`` page) were removed with the
verification-run layer and its API/page, 0.16.14 Part B — the
``verification.runs``/``verification.simulate``/``verification.engine``
modules and the ``/api/verification/*``/``/verification`` routes they
backed no longer exist. ``depth_option_values`` (still read by
``settings/service.py`` on every settings save) is the only one of the
original three NB-4/NB-9 subjects that survives independent of the
verification-run layer, alongside NB-4's roster lockstep itself.

Every fixture value is synthetic: invented site names, RFC-5737-style fake
identifiers, ``UTC``/``Etc/GMT-3`` timezones, and hand-made feed ids.
"""

from __future__ import annotations

import pytest

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.core.options import RuntimeOptions, depth_option_values
from wxverify.forecast.service import VARIABLES as FORECAST_SERVICE_VARIABLES
from wxverify.settings.depth import DEPTH_VARIABLES

# ===========================================================================
# Family 1 — NB-4: the depth roster is ONE tuple, pinned by equality.
# ===========================================================================


class TestDepthRosterLockstep:
    """``settings/depth.py`` owns the canonical roster; three modules alias it
    and ``core/options.py`` writes the same names out explicitly.

    Equality pins, never membership or length: a rename ("precip" ->
    "precipitation") in one module, an addition, a removal, and a REORDER all
    have to fail here.
    """

    def test_canonical_roster_is_the_exact_declared_tuple(self) -> None:
        """Kills: any edit to ``DEPTH_VARIABLES`` itself — an added fourth
        variable, a dropped one, or a swapped order — that is not reflected
        in every derived site below.
        """
        assert DEPTH_VARIABLES == ("temperature", "wind", "precip")

    @pytest.mark.parametrize(
        ("module", "alias"),
        [
            ("wxverify.forecast.service:VARIABLES", FORECAST_SERVICE_VARIABLES),
            (
                "wxverify.collection.forecast_validation:FORECAST_VARIABLES",
                FORECAST_VARIABLES,
            ),
        ],
    )
    def test_every_derived_roster_equals_the_canonical_tuple(
        self, module: str, alias: tuple[str, ...]
    ) -> None:
        """Kills: re-spelling any of the three aliases as its own literal
        tuple (the drift NB-4 exists to prevent). Order is part of the pin —
        the forecast page, sample validation and the record builder iterate
        these tuples, so a reorder is a behavior change.
        """
        assert alias == DEPTH_VARIABLES, module

    def test_depth_option_values_keys_are_exactly_the_roster(self) -> None:
        """Kills: the real drift risk — ``depth_option_values`` writes its
        three keys out by hand, so adding a ``forecast_blend_depth_*`` field
        to ``RuntimeOptions`` without adding its key here (or vice versa)
        silently drops that variable's add-on option on the floor.

        Set equality plus a paired positive: the mapped values must be the
        options actually supplied, so a stub returning the right keys with
        constant ``None`` cannot pass.
        """
        blank = depth_option_values(RuntimeOptions())
        assert set(blank) == set(DEPTH_VARIABLES)
        assert blank == dict.fromkeys(DEPTH_VARIABLES, None)

        supplied = RuntimeOptions(
            forecast_blend_depth_temperature=4,
            forecast_blend_depth_wind=1,
            forecast_blend_depth_precip=6,
        )
        assert depth_option_values(supplied) == {
            "temperature": 4,
            "wind": 1,
            "precip": 6,
        }

    def test_runtime_options_declares_one_depth_field_per_roster_variable(
        self,
    ) -> None:
        """Kills: a per-variable depth field added to (or removed from)
        ``RuntimeOptions`` without a matching roster entry — the option would
        validate at the add-on boundary and then never reach a variable.
        """
        declared = {
            name.removeprefix("forecast_blend_depth_")
            for name in RuntimeOptions.model_fields
            if name.startswith("forecast_blend_depth_")
        }
        assert declared == set(DEPTH_VARIABLES)
