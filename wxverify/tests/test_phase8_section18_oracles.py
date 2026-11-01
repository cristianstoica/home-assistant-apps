"""Phase 8 Round C — the §18 named oracle families for 0.11.0.

Three families live here, each pinned against the mutation it exists to kill
(named in the test docstring, not just in a report).

* **NB-4** — the canonical depth/variable roster. ``DEPTH_VARIABLES`` and its
  four derived aliases, plus ``depth_option_values()``'s key set, pinned with
  tuple/set EQUALITY so an added, removed, renamed or REORDERED variable in
  one module without the others fails loudly.
* **NB-9** — ``current_result_basis_fingerprint`` is a pure read: proven
  against a connection with SQLite's ``query_only`` pragma engaged, where
  any write raises.
* **§18.11** — the §16 element-presence oracle. The required element list is
  transcribed from §16 of the specification (quoted per entry below), NOT
  read off the template — an oracle derived from the page it checks cannot
  catch an element the page dropped.

Family 2 (NB-1, ``_contingency``/``_continuous_metrics`` fail-closed on a
partial common core) was removed: both are called only from
``verification.engine._write_result``, reached only from ``aggregate_run``,
the nightly replay function that has no caller outside its own module (no
install runs the daily close off, and only the close-off lane runs the
replay). The other three families stay: ``depth_option_values`` is read by
``settings/service.py`` on every settings save, and
``current_result_basis_fingerprint`` is read by
``verification.runs.result_basis_freshness``, shared by
``/api/verification/status`` and the live ``/verification`` page — both run
regardless of the daily close.

Every fixture value is synthetic: invented site names, RFC-5737-style fake
identifiers, ``UTC``/``Etc/GMT-3`` timezones, and hand-made feed ids.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, asof_insert_observation, asof_make_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.core.options import RuntimeOptions, depth_option_values
from wxverify.db.connection import close_db, init_db
from wxverify.db.switch import ensure_site_generation, site_generation_id
from wxverify.forecast.service import VARIABLES as FORECAST_SERVICE_VARIABLES
from wxverify.settings.depth import DEPTH_VARIABLES
from wxverify.verification.record import RECORD_VARIABLES
from wxverify.verification.runs import (
    capture_config_snapshot,
    current_result_basis_fingerprint,
    input_fingerprint,
    publish_run,
    result_basis_fingerprint,
)
from wxverify.verification.simulate import SIM_VARIABLES

# ===========================================================================
# Family 1 — NB-4: the depth roster is ONE tuple, pinned by equality.
# ===========================================================================


class TestDepthRosterLockstep:
    """``settings/depth.py`` owns the canonical roster; four modules alias it
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
            ("wxverify.verification.simulate:SIM_VARIABLES", SIM_VARIABLES),
            ("wxverify.verification.record:RECORD_VARIABLES", RECORD_VARIABLES),
        ],
    )
    def test_every_derived_roster_equals_the_canonical_tuple(
        self, module: str, alias: tuple[str, ...]
    ) -> None:
        """Kills: re-spelling any of the four aliases as its own literal
        tuple (the drift NB-4 exists to prevent). Order is part of the pin —
        the forecast page, sample validation, the simulator and the record
        builder iterate these tuples, so a reorder is a behavior change.
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


# ===========================================================================
# Family 3 — NB-9: the read path never writes.
# ===========================================================================


def _read_only(conn: sqlite3.Connection) -> None:
    """Engage SQLite's own write guard on a real, already-populated DB.

    Not a mock or a spy: with ``query_only`` on, any INSERT/UPDATE the read
    path attempts raises ``sqlite3.OperationalError``, so the test fails on
    the write itself rather than on an assertion about writes.
    """
    conn.commit()
    conn.execute("PRAGMA query_only=ON")


class TestReadPathNeverWrites:
    def test_current_result_basis_fingerprint_is_pure_under_query_only(self) -> None:
        """Kills: reintroducing ``ensure_site_generation`` (or any other
        write) into ``current_result_basis_fingerprint`` — the seeding INSERT
        raises 'attempt to write a readonly database' against this
        connection.

        Anchored, not merely non-crashing: the value must equal the write
        path's fingerprint over the same state, so a mutation that returns a
        constant or a truncated digest is caught too.
        """
        conn = asof_conn()
        site_id = asof_make_site(conn, "nb9-site")
        generation_id = ensure_site_generation(conn, site_id)
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at="2026-05-01T06:00:00Z",
            value=11.0,
            computed_at="2026-05-01T07:00:00Z",
        )
        snapshot = capture_config_snapshot(conn, site_id)
        expected = result_basis_fingerprint(
            conn,
            site_id,
            snapshot,
            period_start="2026-05-01",
            period_end="2026-05-01",
        )
        _read_only(conn)

        actual = current_result_basis_fingerprint(
            conn, site_id, period_start="2026-05-01", period_end="2026-05-01"
        )
        assert actual == expected
        assert actual is not None and actual.startswith("rb1:")

        conn.execute("PRAGMA query_only=OFF")
        assert site_generation_id(conn, site_id) == generation_id

    def test_site_without_a_time_zone_generation_raises_and_seeds_nothing(
        self,
    ) -> None:
        """Paired negative with an INJECTED precondition: the site is created
        and its generation is asserted absent before the call, so the raise
        is never ambient coincidence.

        Kills: (a) resolving the generation with the seeding helper — that
        INSERT raises here; (b) returning a fingerprint instead of raising
        when the site has no time zone generation.
        """
        conn = asof_conn()
        site_id = asof_make_site(conn, "nb9-unseeded")
        # Injected, not inherited: prove the precondition holds.
        assert site_generation_id(conn, site_id) is None
        before = int(
            conn.execute("SELECT COUNT(*) AS n FROM timezone_generations").fetchone()[
                "n"
            ]
        )
        _read_only(conn)

        with pytest.raises(ValueError, match="has no time zone generation"):
            current_result_basis_fingerprint(
                conn, site_id, period_start="2026-05-01", period_end="2026-05-01"
            )

        conn.execute("PRAGMA query_only=OFF")
        after = int(
            conn.execute("SELECT COUNT(*) AS n FROM timezone_generations").fetchone()[
                "n"
            ]
        )
        assert after == before
        assert site_generation_id(conn, site_id) is None

    def test_query_only_really_blocks_the_write_path(self) -> None:
        """Negative control for the harness itself: the write-path sibling
        DOES raise on the same connection, so a green NB-9 test above means
        'no write attempted', not 'query_only was never engaged'.
        """
        conn = asof_conn()
        site_id = asof_make_site(conn, "nb9-control")
        assert site_generation_id(conn, site_id) is None
        _read_only(conn)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            capture_config_snapshot(conn, site_id)


# ===========================================================================
# Shared web harness (families 4 and 5).
# ===========================================================================


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _open_app_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    """A real file-backed database the TestClient app will open too."""
    close_db()
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(config, "db_path", str(tmp_path / "wxverify.db"))
    monkeypatch.setattr(config, "options_path", str(options_path))
    db = init_db(config.db_path)
    return db._conn  # noqa: SLF001


def _reopen_app_db() -> sqlite3.Connection:
    """Re-acquire the writer connection after a request cycle.

    The app's lifespan shutdown closes the process-wide database, so a test
    that fetches a page and then keeps mutating must reopen the same FILE
    rather than reuse a closed handle.
    """
    return init_db(config.db_path)._conn  # noqa: SLF001


def _fetch_page(monkeypatch: pytest.MonkeyPatch, site_id: int) -> str:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text


class _ElementText(HTMLParser):
    """Attributes plus flattened inner text for every element of one tag."""

    def __init__(self, tag: str) -> None:
        super().__init__()
        self._tag = tag
        self._depth = 0
        self._attrs: dict[str, str] = {}
        self._buf: list[str] = []
        self.elements: list[tuple[dict[str, str], str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._depth:
            if tag == self._tag:
                self._depth += 1
            return
        if tag == self._tag:
            self._depth = 1
            self._attrs = {key: value or "" for key, value in attrs}
            self._buf = []

    def handle_endtag(self, tag: str) -> None:
        if self._depth and tag == self._tag:
            self._depth -= 1
            if self._depth == 0:
                text = " ".join("".join(self._buf).split())
                self.elements.append((self._attrs, text))

    def handle_data(self, data: str) -> None:
        if self._depth:
            self._buf.append(data)


def _elements(html: str, tag: str) -> list[tuple[dict[str, str], str]]:
    parser = _ElementText(tag)
    parser.feed(html)
    return parser.elements


def _by_v16(html: str, tag: str, element: str) -> list[tuple[dict[str, str], str]]:
    return [item for item in _elements(html, tag) if item[0].get("data-v16") == element]


class _MarkerScan(HTMLParser):
    """Every ``data-v16`` value on the page, on ANY tag.

    Tag-agnostic on purpose: §16 constrains which facts are shown, not which
    element carries them, so a presentational change from ``<p>`` to ``<dd>``
    must not read as a dropped element.
    """

    def __init__(self) -> None:
        super().__init__()
        self.markers: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for key, value in attrs:
            if key == "data-v16" and value:
                self.markers.add(value)


def _v16_markers(html: str) -> set[str]:
    """Every ``data-v16`` value the rendered page actually carries."""
    scan = _MarkerScan()
    scan.feed(html)
    return scan.markers


#: One parsed table row's cells: ``data-v16`` -> (flattened text, nested tag
#: attributes). The nested attrs carry the gate badges' state.
_Cells = dict[str, tuple[str, list[dict[str, str]]]]


class _RowScan(HTMLParser):
    """``<tr>`` rows with their ``<td>`` cells: attrs, text, nested attrs.

    Parsing beats string slicing here: the headline table's rows differ only
    by attribute, and a ``split()`` on markup silently grabs whichever row
    happens to come first.
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[tuple[dict[str, str], _Cells]] = []
        self._row: dict[str, str] | None = None
        self._cells: _Cells = {}
        self._cell: dict[str, str] | None = None
        self._buf: list[str] = []
        self._nested: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        mapping = {key: value or "" for key, value in attrs}
        if tag == "tr":
            self._row = mapping
            self._cells = {}
        elif tag == "td" and self._row is not None:
            self._cell = mapping
            self._buf = []
            self._nested = []
        elif self._cell is not None:
            self._nested.append(mapping)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._cell is not None:
            key = self._cell.get("data-v16", self._cell.get("data-label", ""))
            self._cells[key] = (" ".join("".join(self._buf).split()), self._nested)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append((self._row, self._cells))
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._buf.append(data)


def _headline_row(html: str, **key: str) -> tuple[dict[str, str], _Cells]:
    """The single §16.3 row matching every supplied join-key attribute."""
    scan = _RowScan()
    scan.feed(html)
    matches = [
        row
        for row in scan.rows
        if row[0].get("data-v16") == "16.3.row"
        and all(row[0].get(f"data-{k}") == v for k, v in key.items())
    ]
    assert len(matches) == 1, (key, [row[0] for row in scan.rows])
    return matches[0]


# ===========================================================================
# Family 5 — §18.11: the §16 element-presence oracle.
#
# The expectation below is transcribed from §16 of the specification. Each
# entry carries the spec phrase it discharges. Deriving it from the template
# would make the oracle circular: it exists to catch an element §16 requires
# that the page dropped.
# ===========================================================================

#: §16.1 "Run-status banner: published run ID, evaluation period, data
#: cutoff, creation time, timezone generation, methodology version,
#: application version, and the run's pinned configuration/roster snapshot
#: ... results are labeled 'under the run's declared configuration'".
_V16_1_ALWAYS: dict[str, str] = {
    "16.1.run_id": "published run ID",
    "16.1.period": "evaluation period",
    "16.1.data_cutoff": "data cutoff",
    "16.1.created_at": "creation time",
    "16.1.tz_generation": "timezone generation",
    "16.1.methodology_version": "methodology version",
    "16.1.app_version": "application version",
    "16.1.config_snapshot": "the run's pinned configuration snapshot",
    "16.1.pinned_depths": "pinned configuration (per-variable depths, §15)",
    "16.1.roster_snapshot": "the run's pinned roster snapshot",
    "16.1.declared_configuration": "labeled 'under the run's declared configuration'",
}

#: §16.1 "explicit warnings for stale results, failed newer rebuild, or no
#: publishable run" — each renders only in its own state, so each is pinned
#: by a dedicated test below rather than on the populated run.
_V16_1_CONDITIONAL: dict[str, str] = {
    "16.1.warn_stale": "explicit warning for stale results",
    "16.1.warn_failed_newer": "explicit warning for a failed newer rebuild",
    "16.1.no_publishable_run": "explicit warning for no publishable run",
}

#: §16.2 "Per-variable verdict cards: effective incumbent depth, recommended
#: depth, and exactly one outcome ... plus primary effect, CI, adequate-lead
#: count, common-day range, practical-significance result, baseline gates,
#: completeness guards. 'Retain incumbent' is never labeled proof ..."
#: and §16.1 "Placeholder verdicts are never shown as successful evidence."
#: §15: the page shows whether the live effective depth is global or override.
_V16_2: dict[str, str] = {
    "16.2.card": "per-variable verdict card",
    "16.2.incumbent_depth": "effective incumbent depth",
    "16.2.recommended_depth": "recommended depth",
    "16.2.outcome": "exactly one outcome",
    "16.2.primary_effect": "primary effect",
    "16.2.primary_ci": "CI",
    "16.2.adequate_leads": "adequate-lead count",
    "16.2.common_day_range": "common-day range",
    "16.2.practical_significance": "practical-significance result",
    "16.2.baseline_gates": "baseline gates",
    "16.2.completeness_guards": "completeness guards",
    "16.2.caveat": "'Retain incumbent' is never labeled proof of optimality",
    "16.2.placeholder": "placeholder verdicts never shown as successful evidence",
    "16.2.skip_reason": "why a placeholder verdict was produced",
    "16.2.unresolved": "candidates statistically unresolved against the chosen depth",
    "16.2.live_depth": "§15 effective depth with its source (global vs override)",
}

#: §16.3 "Headline evidence table: rows by variable x lead x quantity x
#: candidate depth: primary metric, incumbent delta, CI, common-day count,
#: observed-event counts where applicable, realized contributor depth,
#: baseline comparisons, and every gate's pass/fail/insufficient state."
#: The three era-note markers (`16.3.two_samples` / `16.3.one_sample` /
#: `16.3.core_unavailable`, show.html:301-305) are deliberately NOT
#: registered here: `decision_core_era` picks exactly one of the three
#: arms per page, so this required-SUBSET presence check would fail on the
#: other two if they were added. Do not add them.
_V16_3: dict[str, str] = {
    "16.3.table": "headline evidence table",
    "16.3.row": "rows by variable x lead x quantity x candidate depth",
    "16.3.candidate_depth": "candidate depth",
    "16.3.primary_metric": "primary metric",
    "16.3.incumbent_delta": "incumbent delta",
    "16.3.ci": "CI",
    "16.3.common_days": "common-day count",
    "16.3.decision_days": "pairwise decision sample the verdict used for that depth",
    "16.3.observed_events": "observed-event counts where applicable",
    "16.3.realized_contributors": "realized contributor depth",
    "16.3.baseline_comparisons": "baseline comparisons",
    "16.3.gate_states": "every gate's pass/fail/insufficient state",
}

#: §16.4 "Diagnostics (visibly separate, labeled non-enactable where
#: applicable): D0; daily-quantity-ranking policies; pairwise-only and
#: below-floor feeds; wet-hour-share verification; bias/RMSE; contingency
#: counts; split-half results."
#:
#: AMENDED for this release by architecture ruling:
#:
#: * "split-half results" is STRUCK from §16.4 — it was an orphan
#:   requirement (no split rule, statistic, stability criterion, §17
#:   constant or table anywhere in the spec) and structurally vacuous at
#:   this release's history depth. It is deliberately NOT pinned here, and
#:   its absence is not a §16 conformance failure.
#: * "wet-hour-share verification" stays enumerated but is unspecified in
#:   the shipped methodology version. Only its PRESENCE and
#:   declared-unavailable state are pinned here; the reason wording is owned
#:   by ``UNAVAILABLE_DIAGNOSTICS`` and pinned once, at
#:   test_phase8_verification_v16.py:619.
_V16_4: dict[str, str] = {
    "16.4.non_enactable": "labeled non-enactable",
    "16.4.d0": "D0",
    "16.4.daily_rank": "daily-quantity-ranking policies",
    "16.4.feeds": "pairwise-only and below-floor feeds",
    "16.4.wet_hour_share": (
        "wet-hour-share verification (not specified in the shipped methodology version)"
    ),
    "16.4.bias_rmse": "bias/RMSE",
    "16.4.contingency": "contingency counts",
}

#: Struck from §16.4 by the same ruling; kept here only to document that the
#: omission above is deliberate rather than an oversight.
_V16_4_STRUCK: dict[str, str] = {
    "16.4.split_half": "split-half results (struck from §16.4)",
}

#: §16.5 "Methodology & provenance block: canonical snapshot time, timezone,
#: truth and eligibility rules, roster floor, candidates, baselines, metrics,
#: bootstrap method and seed, thresholds, ranking basis, code version, input
#: fingerprint, exclusion counts, the run's pinned configuration snapshot,
#: and each verdict's complete tested family (endpoints, comparisons,
#: per-comparison adjusted confidence levels, gate outcomes) — every
#: displayed result linked to its immutable run." Plus the §16 API paragraph:
#: "Response contract versioned with a dedicated verification_schema field;
#: units ... are machine-readable."
_V16_5: dict[str, str] = {
    "16.5.snapshot_time": "canonical snapshot time",
    "16.5.timezone": "timezone",
    "16.5.truth_rules": "truth rules",
    "16.5.eligibility_rules": "eligibility rules",
    "16.5.roster_floor": "roster floor",
    "16.5.candidates": "candidates",
    "16.5.baselines": "baselines",
    "16.5.metrics": "metrics",
    "16.5.units": "units",
    "16.5.bootstrap": "bootstrap method and seed",
    "16.5.thresholds": "thresholds",
    "16.5.ranking_basis": "ranking basis",
    "16.5.code_version": "code version",
    "16.5.input_fingerprint": "input fingerprint",
    "16.5.exclusion_counts": "exclusion counts",
    "16.5.config_snapshot": "the run's pinned configuration snapshot",
    "16.5.tested_family": "each verdict's complete tested family",
    "16.5.tested_family_variable": "the tested family, per verdict",
    "16.5.adjusted_confidence": "per-comparison adjusted confidence levels",
    "16.5.endpoint_window": "each endpoint's own comparison window",
    "16.5.gate_outcomes": "gate outcomes",
    "16.5.baseline_comparisons": "comparisons",
    "16.5.run_link": "every displayed result linked to its immutable run",
    "16.5.schema": "the dedicated verification_schema field",
}

_V16_POPULATED: dict[str, str] = {
    **_V16_1_ALWAYS,
    **_V16_2,
    **_V16_3,
    **_V16_4,
    **_V16_5,
}

#: §16 section anchors: the diagnostics must be "visibly separate".
_V16_SECTIONS = (
    "verification-run-banner",
    "verification-verdicts",
    "verification-headline",
    "verification-diagnostics",
    "verification-methodology",
)


# --- Fixture: a run exercising every §16 element at once. ------------------

_TEMPERATURE_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1, 2, 4, 5],
                "pooled_point": 0.137,
                "ci": [0.041, 0.233],
                "per_lead": {"1": 0.11, "2": 0.16},
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
                # Never evaluated: must read 'insufficient', not vanish.
                "components_non_inferior": None,
            },
            "baselines": {
                "baseline_persistence": {"passed": True, "ci": [0.21, 0.44]},
                "baseline_all_feed_mean": {"passed": False, "ci": [-0.11, 0.22]},
            },
            "components": {
                "temperature_high": {"pooled_point": 0.101},
                "temperature_low": {"pooled_point": 0.036, "degraded": True},
            },
        }
    },
    "statistically_unresolved": ["4"],
    "tie_break": {"chosen": "3", "reason": "nearest the incumbent"},
}

_WIND_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            # Nothing measurable: the wind candidate never cleared the
            # adequate-lead floor, so every headline number stays null. This
            # is the cell the never-numeric-zero invariant is checked on.
            "headline": {
                "adequate_leads": [],
                "pooled_point": None,
                "ci": None,
                "per_lead": {},
            },
            "conditions": {
                "ci_excludes_zero": False,
                "lead_stability": False,
                "practical_floor": False,
                "beats_baselines": False,
            },
            "baselines": {},
            "components": {},
        }
    },
    "statistically_unresolved": [],
}

_PRECIP_FAMILY: dict[str, object] = {
    "incumbent": 5,
    "reason": "incumbent depth 5 lies outside the simulated range 1-4",
    "simulated_depths": [1, 2, 3, 4],
}

_RESULT_COLUMNS = (
    "run_id, variable, lead, quantity, entity_type, entity_key, headline, "
    "common_days, mae, bias, rmse, hits, misses, false_alarms, "
    "correct_negatives, ets, availability_rate, delta_vs_incumbent"
)


def _result(
    run_id: int,
    variable: str,
    lead: int,
    quantity: str,
    entity_type: str,
    entity_key: str,
    headline: int,
    common_days: int,
    *,
    mae: float | None = None,
    bias: float | None = None,
    rmse: float | None = None,
    events: tuple[int, int, int, int] | None = None,
    ets: float | None = None,
    availability: float | None = None,
    delta: float | None = None,
) -> tuple[object, ...]:
    hits, misses, false_alarms, correct_negatives = events or (None,) * 4
    return (
        run_id,
        variable,
        lead,
        quantity,
        entity_type,
        entity_key,
        headline,
        common_days,
        mae,
        bias,
        rmse,
        hits,
        misses,
        false_alarms,
        correct_negatives,
        ets,
        availability,
        delta,
    )


def _seed_v16_site(conn: sqlite3.Connection) -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('Oracle Flats', 40.0, -105.0, 900.0, 'UTC', 1)
            """,
        ).lastrowid
    )
    ensure_site_generation(conn, site_id)
    return site_id


def _seed_v16_run(conn: sqlite3.Connection, site_id: int) -> int:
    generation_id = ensure_site_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    # Seeded to match at write time, so a genuine post-publish config change
    # is what moves the recomputed basis -- a NULL basis would read as
    # `unknown` and could never flip to `changed`.
    basis = result_basis_fingerprint(
        conn, site_id, snapshot, period_start="2026-04-02", period_end="2026-05-11"
    )
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, result_basis_fingerprint)
            VALUES (?, ?, 1, '0.11.0-oracle', 'running', 3, ?, '2026-04-02',
                    '2026-05-11', '2026-05-11', 909, 10000, ?, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint, basis),
        ).lastrowid
    )
    conn.executemany(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (run_id, "temperature", "recommend", 3, 2, json.dumps(_TEMPERATURE_FAMILY)),
            (run_id, "wind", "retain_incumbent", None, 2, json.dumps(_WIND_FAMILY)),
            (run_id, "precip", "skipped", None, 5, json.dumps(_PRECIP_FAMILY)),
        ],
    )
    rows: list[tuple[object, ...]] = [
        # §16.3 headline: a candidate depth and the incumbent it is measured
        # against. No metric here is exactly zero, so a stray "0.000" on the
        # page can only come from a null rendered as a number.
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "depth",
            "3",
            1,
            27,
            mae=0.812,
            bias=0.104,
            rmse=1.031,
            availability=0.96,
            delta=0.137,
        ),
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "depth",
            "2",
            1,
            27,
            mae=0.949,
            bias=0.152,
            rmse=1.148,
            availability=0.96,
        ),
        # The insufficient cell: every metric NULL. §16 forbids rendering
        # any of these as numeric zero.
        _result(run_id, "wind", 3, "wind_max", "depth", "3", 1, 4),
        # §16.4 D0 — diagnostic only, never pooled into the headline table.
        _result(
            run_id,
            "temperature",
            0,
            "temperature_high",
            "depth",
            "3",
            0,
            13,
            mae=0.443,
            bias=0.052,
            rmse=0.617,
            availability=0.91,
        ),
        # §16.3 baseline comparisons for the temperature D1 cell.
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "baseline_persistence",
            "-",
            0,
            27,
            mae=1.446,
        ),
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "baseline_all_feed_mean",
            "-",
            0,
            27,
            mae=0.987,
        ),
        # §16.3/§16.4 occurrence: contingency counts + ETS.
        _result(
            run_id,
            "precip",
            1,
            "precip_occurrence",
            "depth",
            "3",
            1,
            23,
            events=(9, 3, 4, 7),
            ets=0.317,
            availability=0.93,
            delta=0.041,
        ),
        # §16.4 daily-quantity-ranking diagnostic policy.
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "daily_rank_depth",
            "top2",
            0,
            21,
            mae=0.884,
            delta=-0.021,
        ),
        # §16.4 feeds: one below the availability floor, one pairwise-only.
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "feed",
            "9101",
            1,
            27,
            mae=1.052,
            availability=0.54,
        ),
        _result(
            run_id,
            "temperature",
            1,
            "temperature_high",
            "feed",
            "9102",
            0,
            27,
            mae=0.993,
            availability=0.89,
        ),
    ]
    conn.executemany(
        f"INSERT INTO verification_results ({_RESULT_COLUMNS})"
        f" VALUES ({', '.join('?' * 18)})",
        rows,
    )
    # §16.3 realized contributor depth comes from the run's own evidence.
    # The wind cell's only evidence row is INELIGIBLE, so its contributor
    # cell must render an em-dash rather than a count of zero.
    conn.executemany(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible, abs_error)
        VALUES (?, ?, ?, ?, ?, ?, 'depth', ?, ?, ?, ?, ?, 1, ?)
        """,
        [
            (
                run_id,
                "2026-04-02",
                "2026-04-03",
                1,
                "temperature",
                "temperature_high",
                "3",
                15.0,
                1,
                2,
                14.5,
                0.5,
            ),
            (
                run_id,
                "2026-04-03",
                "2026-04-04",
                1,
                "temperature",
                "temperature_high",
                "3",
                16.0,
                1,
                3,
                14.0,
                2.0,
            ),
            (
                run_id,
                "2026-04-02",
                "2026-04-05",
                3,
                "wind",
                "wind_max",
                "3",
                5.0,
                0,
                1,
                6.0,
                1.0,
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc,
             knowability_exclusions, null_availability_samples)
        VALUES (?, '2026-04-02', '2026-04-02T07:00:00Z', ?, 2)
        """,
        (run_id, json.dumps({"temperature_high": "truth_pending"})),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


@pytest.fixture
def v16_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _seed_v16_site(conn)
    _seed_v16_run(conn, site_id)
    page = _fetch_page(monkeypatch, site_id)
    close_db()
    return page


class TestSection16ElementPresence:
    def test_every_section16_element_renders_on_a_populated_run(
        self, v16_page: str
    ) -> None:
        """§18.11: the page carries every element §16 enumerates.

        The expectation is transcribed from the specification, so this fails
        when the page DROPS a required element — the failure mode an oracle
        derived from the template can never see. Kills: deleting any §16
        block or attribute from ``show.html``/``web/verification.py``, and
        any refactor that stops rendering a section on a populated run.
        """
        present = _v16_markers(v16_page)
        missing = sorted(
            f"{marker} ({requirement})"
            for marker, requirement in _V16_POPULATED.items()
            if marker not in present
        )
        assert missing == [], missing

    def test_the_presence_matcher_can_actually_fail(self, v16_page: str) -> None:
        """Anti-vacuity control for the oracle above: a marker no §16
        element declares must NOT be found. A matcher that reported every
        probe present would pass the presence test on an empty page.
        """
        markers = _v16_markers(v16_page)
        assert "16.9.not_a_section16_element" not in markers
        assert "16.3.primary_metric" in markers
        # Guards the expectation table itself: a struck §16.4 diagnostic must
        # never drift back into the required set, which would turn its
        # deliberate removal from the page into a spurious failure.
        assert _V16_4_STRUCK.keys().isdisjoint(_V16_POPULATED)

    def test_the_five_section16_blocks_are_visibly_separate(
        self, v16_page: str
    ) -> None:
        """§16.4 requires the diagnostics be 'visibly separate'; §16's five
        subsections each get their own anchor.

        Kills: collapsing a subsection into another (the diagnostics folded
        into the headline table is the specific regression §16.4 forbids).
        """
        for section in _V16_SECTIONS:
            assert f'id="{section}"' in v16_page, section

    def test_headline_rows_carry_the_full_join_key(self, v16_page: str) -> None:
        """§16.3 rows are keyed by variable x lead x quantity x candidate
        depth, and §16.4 keeps D0 out of the headline table.

        Kills: dropping any of the four join attributes (the API/UI
        consistency oracle in §18.11 joins on them), and pooling D0 into the
        decision leads.
        """
        rows = _by_v16(v16_page, "tr", "16.3.row")
        assert rows, "the populated fixture must render headline rows"
        keys: set[tuple[str, str, str, str]] = set()
        for attrs, _text in rows:
            for key in ("data-variable", "data-lead", "data-quantity", "data-depth"):
                assert attrs.get(key), (key, attrs)
            assert attrs["data-lead"] != "0", "D0 is diagnostic only (§16.4)"
            keys.add(
                (
                    attrs["data-variable"],
                    attrs["data-lead"],
                    attrs["data-quantity"],
                    attrs["data-depth"],
                )
            )
        assert len(keys) == len(rows), "the join key must identify a row uniquely"
        assert ("temperature", "1", "temperature_high", "3") in keys
        assert ("precip", "1", "precip_occurrence", "3") in keys

    def test_each_verdict_card_shows_exactly_one_outcome(self, v16_page: str) -> None:
        """§16.2: 'exactly one outcome' per variable card, one card per
        depth-configurable variable (§15 roster).

        Kills: rendering a second outcome line, or dropping a variable's card
        when its verdict is a placeholder.
        """
        cards = _by_v16(v16_page, "div", "16.2.card")
        assert [attrs["data-variable"] for attrs, _ in cards] == sorted(DEPTH_VARIABLES)
        outcomes = _by_v16(v16_page, "p", "16.2.outcome")
        assert len(outcomes) == len(cards)
        assert {attrs["data-outcome"] for attrs, _ in cards} == {
            "recommend",
            "retain_incumbent",
            "skipped",
        }

    def test_placeholder_verdict_is_never_shown_as_successful_evidence(
        self, v16_page: str
    ) -> None:
        """§16.1: 'Placeholder verdicts are never shown as successful
        evidence'; §16.2: 'Retain incumbent' is never labeled proof.

        Kills: dropping the placeholder badge or the retain-incumbent caveat
        — both make an absence of evidence read as evidence.
        """
        placeholders = _by_v16(v16_page, "span", "16.2.placeholder")
        assert len(placeholders) == 1
        assert "not evidence" in placeholders[0][1]
        caveats = {text for _attrs, text in _by_v16(v16_page, "p", "16.2.caveat")}
        assert any(
            "not proof that the incumbent depth is optimal" in text for text in caveats
        )
        assert any("Not evidence of improvement." in text for text in caveats)


class TestNullResultsNeverRenderAsZero:
    """§16: '"Insufficient", "not applicable", and "failed" are never encoded
    as numeric zero.' The page's encoding of a non-result is the literal
    em-dash (or 'n/a' where the comparison does not apply).
    """

    def test_the_all_null_headline_cell_renders_em_dashes_not_zeros(
        self, v16_page: str
    ) -> None:
        """The wind D3 cell has NULL mae/bias/rmse/ets, no CI, no observed
        events and no eligible evidence row.

        Kills: rendering a null metric through a numeric formatter (``%.3f``
        on a coerced ``0``), or defaulting a missing value to 0 anywhere in
        ``_headline_rows``. Each of those turns 'we could not measure this'
        into 'we measured zero error'.
        """
        _attrs, cells = _headline_row(
            v16_page, variable="wind", lead="3", quantity="wind_max", depth="3"
        )
        assert cells["16.3.primary_metric"][0] == "MAE —"
        assert cells["16.3.ci"][0] == "—"
        assert cells["16.3.incumbent_delta"][0] == "—"
        assert cells["16.3.observed_events"][0] == "n/a"
        assert cells["16.3.realized_contributors"][0] == "—"
        assert cells["16.3.baseline_comparisons"][0] == "—"
        for label, (text, _nested) in cells.items():
            assert "0.000" not in text, (label, text)
        # A never-evaluated gate reads 'insufficient', not a passing zero.
        states = {
            nested["data-gate"]: nested["data-gate-state"]
            for nested in cells["16.3.gate_states"][1]
            if "data-gate" in nested
        }
        assert states, cells["16.3.gate_states"]
        assert set(states.values()) <= {"fail", "insufficient"}

    def test_a_measured_cell_still_prints_its_number(self, v16_page: str) -> None:
        """Paired positive — without it, a page that em-dashed EVERY cell
        would pass the test above.

        Kills: a blanket null-rendering regression that suppresses real
        measurements along with the non-results.
        """
        _attrs, cells = _headline_row(
            v16_page,
            variable="temperature",
            lead="1",
            quantity="temperature_high",
            depth="3",
        )
        assert cells["16.3.primary_metric"][0] == "MAE 0.812"
        assert cells["16.3.ci"][0] == "[0.041, 0.233]"
        assert cells["16.3.common_days"][0] == "27"
        assert cells["16.3.realized_contributors"][0] != "—"

    def test_no_null_anywhere_on_the_page_became_a_numeric_zero(
        self, v16_page: str
    ) -> None:
        """Whole-page sweep. Every fixture metric is deliberately non-zero,
        so any '0.000' on the page can only be a null rendered numerically.

        Kills: a null-to-zero coercion in a subsection this file does not
        assert cell-by-cell (a diagnostic table, a verdict card fact).
        """
        assert "0.000" not in v16_page
        # ... and the em-dash encoding is actually in use, so the sweep above
        # is not passing merely because nothing null was rendered at all.
        assert "—" in v16_page


class TestSection16ConditionalWarnings:
    """§16.1's three explicit warnings, each with an injected precondition
    and a paired positive — an absence assertion on an ambient state proves
    nothing.
    """

    def test_no_publishable_run_is_stated_explicitly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Kills: rendering an empty page (or a zeroed banner) for a site
        with no published run instead of §16.1's explicit warning.
        """
        conn = _open_app_db(tmp_path, monkeypatch)
        site_id = _seed_v16_site(conn)
        conn.commit()
        page = _fetch_page(monkeypatch, site_id)
        close_db()
        markers = _v16_markers(page)
        assert "16.1.no_publishable_run" in markers
        # Paired positive: the run-scoped elements are absent, not zeroed.
        assert "16.3.row" not in markers
        assert "16.1.run_id" not in markers

    def test_a_failed_newer_attempt_is_warned_about(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """§16.1: 'explicit warnings for ... failed newer rebuild'.

        Injected precondition: a FAILED run row with an id above the
        published pointer. Paired positive: before it exists the warning is
        absent, so the assertion cannot pass on an ambient page.

        Kills: dropping the failed-newer probe, or comparing ``id >`` against
        0 instead of the published run (which would warn on every page).
        """
        conn = _open_app_db(tmp_path, monkeypatch)
        site_id = _seed_v16_site(conn)
        run_id = _seed_v16_run(conn, site_id)
        assert "16.1.warn_failed_newer" not in _v16_markers(
            _fetch_page(monkeypatch, site_id)
        )

        conn = _reopen_app_db()
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            SELECT site_id, tz_generation_id, methodology_version, app_version,
                   'failed', attempt + 1, config_snapshot, period_start,
                   period_end, settled_through, bootstrap_seed,
                   bootstrap_resamples, input_fingerprint
            FROM verification_runs WHERE id = ?
            """,
            (run_id,),
        )
        conn.commit()
        page = _fetch_page(monkeypatch, site_id)
        close_db()
        markers = _v16_markers(page)
        assert "16.1.warn_failed_newer" in markers
        # The published run is still served alongside the warning (§20).
        assert "16.3.row" in markers

    def test_stale_inputs_are_warned_about_after_a_configuration_change(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """§16.1: 'explicit warnings for stale results'.

        Injected precondition: a settings change AFTER publication moves the
        live result basis away from the run's pinned one. Paired positive:
        the same page before the change carries no warning.

        Kills: dropping the ``result_basis_freshness`` comparison, or
        comparing the run against itself (always fresh).
        """
        conn = _open_app_db(tmp_path, monkeypatch)
        site_id = _seed_v16_site(conn)
        _seed_v16_run(conn, site_id)
        assert "16.1.warn_stale" not in _v16_markers(_fetch_page(monkeypatch, site_id))

        conn = _reopen_app_db()
        conn.execute(
            "INSERT INTO settings (key, value) VALUES ('forecast_blend_depth', '4')"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.commit()
        page = _fetch_page(monkeypatch, site_id)
        close_db()
        markers = _v16_markers(page)
        assert "16.1.warn_stale" in markers
        # §16.2: the live depth moved, and the page must not let the pinned
        # incumbent read as the current policy.
        assert "16.2.depth_mismatch" in markers


def test_page_still_performs_no_simulation_or_bootstrap_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§19/§14.4 re-pinned against THIS file's richer fixture.

    The companion test in ``test_phase8_verification_v16.py`` covers the
    implementer's fixture; this one proves the additional rows seeded here
    (diagnostics, baselines, occurrence counts, contributor evidence) did not
    open a new request-path route into the simulator or the resampler.

    Kills: any lazy 'recompute it if the persisted row is missing' fallback
    added to the loader.
    """
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _seed_v16_site(conn)
    _seed_v16_run(conn, site_id)

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the /verification page must not simulate per request")

    monkeypatch.setattr("wxverify.verification.simulate.simulate_snapshot_day", _boom)
    monkeypatch.setattr("wxverify.verification.simulate.compute_snapshot_day", _boom)
    monkeypatch.setattr("wxverify.verification.simulate.compute_baseline_day", _boom)
    monkeypatch.setattr("wxverify.verification.engine.prepare_bootstrap_inputs", _boom)

    page = _fetch_page(monkeypatch, site_id)
    close_db()
    assert "16.3.row" in _v16_markers(page)
