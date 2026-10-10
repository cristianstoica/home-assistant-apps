"""§15.3 the rain control's version map (§13.1): T1-T5, X1-X4 + temperature.

Covers only the version-map change: :data:`RainRule`, :data:`V_B`,
:data:`LEGACY_GLYPH_MIN_WET_HOURS`, :data:`WET_HOURS_FIRST`,
:class:`RainRuleRead`, :data:`RAIN_RULE_READS`, :data:`RAIN_RULES`,
:class:`VersionUnmappedError`, the new :func:`control_value` signature,
:func:`rain_rules_record` and the :func:`rain_settings` additions. Does NOT
cover the span check or mutants M141-M144 -- those are covered by
``tests/test_daily_shadow_span_check.py`` instead.

Every rain fixture row sets the literal ``app_version`` ``'0.16.7'`` unless
the test names another version -- never ``wxverify.__version__``, and never
through the record writers (:929): the three JSON columns are written by
hand, with ``daily_quantities.displayed`` computed via a direct call to
:func:`wxverify.forecast.aggregate.displayed_daily` on the same hourly
values the ``forecast_samples`` fixture inserts, so F1 (the replay
re-validation) passes by construction rather than by coincidence.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import cast

import pytest

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import displayed_daily
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import (
    RAIN_RULE_READS,
    RAIN_RULES,
    WET_HOURS_FIRST,
    RecordRow,
    Unmatched,
    VersionUnmappedError,
    control_value,
    parse_version,
)
from wxverify.verification.record import resolve_snapshot_utc
from wxverify.verification.simulate import latest_knowable_target

COMMIT_SHA = "a" * 40
_DAY = date(2035, 6, 10)


# --- direct-call fixtures (no DB) ---------------------------------------------


def _row(
    *,
    app_version: str = "0.16.7",
    status: str = "recorded",
    policy: str | None = '{"rain_threshold_mm": 0.2}',
    daily_quantities: str | None = None,
) -> RecordRow:
    return RecordRow(
        snapshot_date=_DAY,
        target_date=_DAY,
        lead=0,
        snapshot_utc=isoformat_utc(datetime(2035, 6, 10, 7, tzinfo=UTC)),
        status=status,
        write_path="on_time",
        app_version=app_version,
        methodology_version=1,
        tz_generation_id=1,
        policy=policy,
        candidates=None,
        daily_quantities=daily_quantities,
    )


def _precip_daily_quantities(
    *, total_mm: float | None, wet_hours: float | None, extrema_feed_ids: object = None
) -> str:
    return json.dumps(
        {
            "displayed": {
                "total_mm": total_mm,
                "wet_hours": wet_hours,
                "extrema_feed_ids": extrema_feed_ids,
            }
        }
    )


def _temperature_daily_quantities(*, high_c: float, low_c: float) -> str:
    return json.dumps({"displayed": {"high_c": high_c, "low_c": low_c}})


# --- T1: control rule by version -----------------------------------------------


# F2: each case runs once against the tool's own map (RAIN_RULES) and once
# against a minimal map holding only the one version the row uses -- proof
# that control_value reads the *passed* rain_rules argument, not any
# particular map's other contents.
_T1_LEGACY_MAPS: dict[str, Mapping[tuple[int, ...], daily_shadow.RainRule]] = {
    "tool_map": RAIN_RULES,
    "minimal_map": MappingProxyType({(0, 16, 7): "legacy"}),
}


@pytest.mark.parametrize("map_name", sorted(_T1_LEGACY_MAPS))
@pytest.mark.parametrize(
    ("wet_hours", "expected_symbol"),
    [
        (5.5, True),  # round(5.5) == 6 >= 6
        (6.5, True),  # round(6.5) == 6 (banker's) >= 6
        (4.5, False),  # round(4.5) == 4 (banker's) < 6
    ],
)
def test_t1_control_rule_by_version_legacy_symbol(
    wet_hours: float, expected_symbol: bool, map_name: str
) -> None:
    """A mapped version (0.16.7) uses the legacy rule: round(wet_hours) >= 6.

    Runs against both the tool's own map and a minimal stand-in map that
    holds only 0.16.7, so the result is known to come from the lookup
    itself rather than from some other content of ``RAIN_RULES``.

    Kills M7 (legacy control uses ``wet_hours >= 6`` without ``round``): at
    wet_hours=5.5, M7's unrounded ``5.5 >= 6`` is False, but the correct
    ``round(5.5) >= 6`` is True -- the two literals diverge at this
    assertion.
    """
    row = _row(
        app_version="0.16.7",
        daily_quantities=_precip_daily_quantities(
            total_mm=3.0, wet_hours=wet_hours, extrema_feed_ids=[1]
        ),
    )
    rain_rules = _T1_LEGACY_MAPS[map_name]
    result = control_value(row, variable="precip", rain_rules=rain_rules)
    assert not isinstance(result, Unmatched)
    assert result.symbol is expected_symbol
    assert result.wet_hours == wet_hours
    assert result.amount == 3.0
    assert result.version == (0, 16, 7)


def test_f2_control_rule_reads_the_passed_map_not_the_module_constant() -> None:
    """A custom map holding 0.16.8 (absent from RAIN_RULES) is matched.

    Kills a mutant at control_value's map lookup that reads the module
    constant RAIN_RULES directly instead of the ``rain_rules`` parameter:
    with this custom map, the correct code finds 0.16.8 and returns a
    matched ControlValue; the mutant ignores the argument, falls back to
    RAIN_RULES (whose maximum key is 0.16.7), finds nothing for 0.16.8, and
    raises VersionUnmappedError instead -- the two diverge on whether an
    exception is raised at all.
    """
    custom_map: Mapping[tuple[int, ...], daily_shadow.RainRule] = MappingProxyType(
        {(0, 16, 8): "legacy"}
    )
    row = _row(
        app_version="0.16.8",
        daily_quantities=_precip_daily_quantities(
            total_mm=3.0, wet_hours=6.0, extrema_feed_ids=[1]
        ),
    )
    result = control_value(row, variable="precip", rain_rules=custom_map)
    assert not isinstance(result, Unmatched)
    assert result.symbol is True
    assert result.version == (0, 16, 8)


def test_t1_control_rule_by_version_each_mapped_version_uses_legacy() -> None:
    """Every mapped version (0.16.2-0.16.7) resolves through the legacy rule."""
    for read in RAIN_RULE_READS:
        version_text = ".".join(str(part) for part in read.version)
        row = _row(
            app_version=version_text,
            daily_quantities=_precip_daily_quantities(
                total_mm=1.0, wet_hours=6.0, extrema_feed_ids=[1]
            ),
        )
        result = control_value(row, variable="precip", rain_rules=RAIN_RULES)
        assert not isinstance(result, Unmatched), version_text
        assert result.symbol is True, version_text


@pytest.mark.parametrize("app_version", ["0.15.0", "0.11.0", "0.9.9"])
def test_t1_pre_wet_hours_versions_are_unmatched(app_version: str) -> None:
    """A version below WET_HOURS_FIRST (0.16.0) is unmatched ``pre_wet_hours``.

    The malformed ``policy``/``daily_quantities`` below prove the check runs
    before either is narrowed -- a positive the version-unmapped X1 tests
    mirror for the map-lookup branch.
    """
    row = _row(app_version=app_version, policy="not json", daily_quantities=None)
    result = control_value(row, variable="precip", rain_rules=RAIN_RULES)
    assert result == Unmatched("pre_wet_hours")


# --- F1 (blocker): the "0.16.0" boundary ----------------------------------------
#
# WET_HOURS_FIRST == (0, 16, 0), but RAIN_RULES' minimum key is (0, 16, 2) --
# so a version of exactly 0.16.0 is NOT below WET_HOURS_FIRST (fails the
# pre_wet_hours check) and is ALSO absent from RAIN_RULES: it must fall
# through to the map lookup and raise, never return Unmatched.


def test_f1_precip_row_at_wet_hours_first_itself_is_unmapped_not_pre_wet_hours() -> (
    None
):
    """0.16.0 (== WET_HOURS_FIRST, not < it) is absent from RAIN_RULES too.

    Kills two mutants at control_value's precip branch that each let the
    pre_wet_hours check swallow 0.16.0 instead of letting it fall through to
    the map lookup: ``version <= WET_HOURS_FIRST`` (0.16.0 <= 0.16.0 is True)
    and ``version < min(rain_rules)`` (0.16.0 < 0.16.2 is True) each return
    Unmatched("pre_wet_hours") here; the correct ``version < WET_HOURS_FIRST``
    is False for 0.16.0, so the only way through is the map lookup, which
    raises VersionUnmappedError naming "0.16.0" -- the two diverge on
    whether an exception is raised at all.
    """
    row = _row(
        app_version="0.16.0",
        daily_quantities=_precip_daily_quantities(
            total_mm=1.0, wet_hours=6.0, extrema_feed_ids=[1]
        ),
    )
    with pytest.raises(VersionUnmappedError, match=r"0\.16\.0"):
        control_value(row, variable="precip", rain_rules=RAIN_RULES)


def test_f1_temperature_row_at_wet_hours_first_itself_gives_its_control() -> None:
    """The same 0.16.0 app_version, as a temperature row: a normal control.

    The pre_wet_hours check (and the gap it would otherwise create at this
    exact version) is scoped to ``variable == "precip"`` -- a temperature
    row at 0.16.0 never touches it.
    """
    row = _row(
        app_version="0.16.0",
        daily_quantities=_temperature_daily_quantities(high_c=19.0, low_c=9.0),
    )
    result = control_value(row, variable="temperature", rain_rules=RAIN_RULES)
    assert not isinstance(result, Unmatched)
    assert result.high == 19.0
    assert result.low == 9.0


def test_f6_temperature_row_below_wet_hours_first_still_gives_its_control() -> None:
    """A temperature 0.15.0 row: still a normal control, not pre_wet_hours.

    Kills a mutant that hoists control_value's ``pre_wet_hours`` check out
    of ``if variable == "precip":`` so it runs for every variable: 0.15.0 is
    below WET_HOURS_FIRST, so the mutant returns Unmatched("pre_wet_hours")
    here; the correct code only runs that check for precip, so a
    temperature row gives its normal control instead -- the two diverge on
    whether the result is Unmatched at all.
    """
    row = _row(
        app_version="0.15.0",
        daily_quantities=_temperature_daily_quantities(high_c=18.0, low_c=8.0),
    )
    result = control_value(row, variable="temperature", rain_rules=RAIN_RULES)
    assert not isinstance(result, Unmatched)
    assert result.high == 18.0
    assert result.low == 8.0


# --- T2: the stop for a version the map does not hold --------------------------

_COMMIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


def test_f3_rain_rule_reads_commits_are_full_hex_shas_and_map_starts_at_0_16_2() -> (
    None
):
    """Every RAIN_RULE_READS commit field is a 40-hex-char sha; map floor is 0.16.2."""
    assert min(RAIN_RULES) >= (0, 16, 2)
    for read in RAIN_RULE_READS:
        assert _COMMIT_SHA_RE.fullmatch(read.release_commit), read
        assert _COMMIT_SHA_RE.fullmatch(read.read_to), read
        for other_head in read.other_heads:
            assert _COMMIT_SHA_RE.fullmatch(other_head), read


@pytest.mark.parametrize("app_version", ["0.16.8", "0.16.9", "0.16.10"])
def test_t2_version_unmapped_raises_versionunmappederror(app_version: str) -> None:
    """A version after the map's last read (0.16.7) raises VersionUnmappedError.

    Per plan :1300, :1302 and :1306 (F5), this unit-level stop test passes
    its own minimal map, ``{(0, 16, 7): "legacy"}``, rather than the tool's
    own ``RAIN_RULES`` -- so this pin survives stage two unchanged when the
    tool's own map grows to hold 0.16.8/0.16.10. Each of the three versions
    tested here (0.16.8, 0.16.9, 0.16.10) is absent from this map in both
    stages.

    Kills M8 (control always uses the legacy rule, without reading the map):
    at this call, the correct code raises VersionUnmappedError and never
    returns a value, while M8 (which skips the ``rain_rules.get`` lookup
    entirely) returns a ``ControlValue`` with ``symbol`` set -- the two
    diverge on whether an exception is raised at all.

    Also kills a message mutant that names an invented style-label
    ("Style B (V_B)") in the error: the real message never contains either
    substring, so a mutant that injects it diverges from the real text at
    these two ``not in`` assertions.
    """
    minimal_map: Mapping[tuple[int, ...], daily_shadow.RainRule] = MappingProxyType(
        {(0, 16, 7): "legacy"}
    )
    row = _row(
        app_version=app_version,
        daily_quantities=_precip_daily_quantities(
            total_mm=1.0, wet_hours=6.0, extrema_feed_ids=[1]
        ),
    )
    with pytest.raises(VersionUnmappedError, match=re.escape(app_version)) as excinfo:
        control_value(row, variable="precip", rain_rules=minimal_map)
    message = str(excinfo.value)
    assert "Style B" not in message
    assert "V_B" not in message
    assert "no verified control rule maps" in message


def test_t2_temperature_row_never_raises_for_the_same_unmapped_version() -> None:
    """The same 0.16.8 app_version, as a temperature row: no map is read."""
    row = _row(
        app_version="0.16.8",
        daily_quantities=_temperature_daily_quantities(high_c=20.0, low_c=10.0),
    )
    result = control_value(row, variable="temperature", rain_rules=RAIN_RULES)
    assert not isinstance(result, Unmatched)
    assert result.high == 20.0
    assert result.low == 10.0


# --- temperature-addition: the temperature path never reads the map -----------


def test_temperature_path_ignores_rain_rules_contents() -> None:
    """A temperature row gives the same ControlValue with an empty map.

    Direct proof the temperature path never reads ``rain_rules`` at all --
    not merely that it happens not to raise.
    """
    row = _row(
        app_version="0.16.7",
        daily_quantities=_temperature_daily_quantities(high_c=22.5, low_c=11.5),
    )
    with_real_map = control_value(row, variable="temperature", rain_rules=RAIN_RULES)
    with_empty_map = control_value(
        row, variable="temperature", rain_rules=MappingProxyType({})
    )
    assert not isinstance(with_real_map, Unmatched)
    assert with_real_map == with_empty_map
    assert with_real_map.high == 22.5
    assert with_real_map.low == 11.5


# --- X1: check order -- the map lookup runs before policy/quantity narrowing --


@pytest.mark.parametrize(
    ("policy", "daily_quantities"),
    [
        ("not json", None),
        (None, None),
        ('{"rain_threshold_mm": "nope"}', "also not json"),
    ],
)
def test_x1_version_unmapped_raises_before_policy_or_quantities_are_narrowed(
    policy: str | None, daily_quantities: str | None
) -> None:
    """VersionUnmappedError fires even when policy/daily_quantities are junk.

    Kills a mutant that moves the map lookup after policy/quantity
    narrowing: such a mutant would return ``Unmatched("policy_unreadable")``
    or ``Unmatched("daily_quantities_unreadable")`` here instead of raising
    -- the two diverge on whether an exception is raised at all.
    """
    row = _row(app_version="0.16.8", policy=policy, daily_quantities=daily_quantities)
    with pytest.raises(VersionUnmappedError):
        control_value(row, variable="precip", rain_rules=RAIN_RULES)


def test_x1_missed_row_never_reaches_the_map_lookup() -> None:
    """A ``missed`` row is Unmatched before the version is even parsed."""
    row = _row(app_version="not-a-version-at-all", status="missed")
    result = control_value(row, variable="precip", rain_rules=RAIN_RULES)
    assert result == Unmatched("missed")


def test_x1_version_unparseable_is_checked_before_the_map() -> None:
    """An unparseable app_version is Unmatched, not a map-lookup KeyError."""
    row = _row(app_version="0.16.9b1")
    result = control_value(row, variable="precip", rain_rules=RAIN_RULES)
    assert result == Unmatched("version_unparseable")
    assert parse_version("0.16.9b1") is None
    assert parse_version("0.16.10") == (0, 16, 10)


# --- X2: map consistency -------------------------------------------------------


def test_x2_rain_rules_matches_rain_rule_reads_exactly() -> None:
    """RAIN_RULES is exactly {read.version: read.rule for read in RAIN_RULE_READS}."""
    assert dict(RAIN_RULES) == {read.version: read.rule for read in RAIN_RULE_READS}
    assert len(RAIN_RULES) == len(RAIN_RULE_READS)


def test_x2_rain_rules_is_read_only() -> None:
    """RAIN_RULES is a MappingProxyType: mutation raises TypeError."""
    with pytest.raises(TypeError):
        RAIN_RULES[(9, 9, 9)] = "legacy"  # type: ignore[index]


def test_x2_rain_rules_record_lists_every_read_in_version_order() -> None:
    """rain_rules_record's version order matches RAIN_RULE_READS, sorted.

    The version-sortedness of the record's dotted text is pinned by
    ``test_f4_record_version_text_matches_the_sorted_tuples`` (which compares
    against text derived from an int-tuple sort, never a lexical one) --
    not repeated here, since a plain ``versions == sorted(versions)`` on the
    dotted strings would false-fail once a two-digit version segment (e.g.
    "0.16.10") is mapped, as "0.16.10" sorts lexically before "0.16.9".
    """
    record = daily_shadow.rain_rules_record(RAIN_RULE_READS)
    assert len(record) == len(RAIN_RULE_READS)
    for entry in record:
        assert set(entry) == {
            "version",
            "rule",
            "release_commit",
            "read_to",
            "other_heads",
        }


def test_f4_rain_rule_reads_versions_are_strictly_increasing() -> None:
    """RAIN_RULE_READS' own versions are strictly increasing, no repeats."""
    versions = [read.version for read in RAIN_RULE_READS]
    assert versions == sorted(versions)
    assert len(versions) == len(set(versions))


def test_f4_record_version_text_matches_the_sorted_tuples() -> None:
    """The record's version text equals the dotted text of the sorted tuples."""
    record = daily_shadow.rain_rules_record(RAIN_RULE_READS)
    expected = [
        ".".join(str(part) for part in version)
        for version in sorted(read.version for read in RAIN_RULE_READS)
    ]
    assert [entry["version"] for entry in record] == expected


def test_f4_record_sorts_a_reversed_input_and_sorts_other_heads() -> None:
    """A reversed, out-of-order input comes back version-sorted, heads sorted.

    The sort is by version as an int tuple, never as dotted text, and
    ``other_heads`` is sorted too -- the synthetic 0.16.9/0.16.10 pair is
    the trap: "0.16.10" < "0.16.9" as text, but (0, 16, 10) > (0, 16, 9) as
    the real ordering, so a string-sort mutant would place 0.16.10 first.

    Kills: the record built without ``sorted()`` (reversed input stays
    reversed), a mutant that sorts by the reversed constant instead of by
    version, and a mutant that drops ``sorted()`` from ``other_heads``.
    """
    head_a = "a" * 40
    head_b = "b" * 40
    head_c = "c" * 40
    reversed_reads = [
        daily_shadow.RainRuleRead(
            version=(0, 16, 10),
            rule="legacy",
            release_commit=head_c,
            read_to=head_c,
            other_heads=(head_b, head_a),
        ),
        daily_shadow.RainRuleRead(
            version=(0, 16, 9),
            rule="legacy",
            release_commit=head_b,
            read_to=head_b,
            other_heads=(),
        ),
    ]
    record = daily_shadow.rain_rules_record(reversed_reads)
    assert [entry["version"] for entry in record] == ["0.16.9", "0.16.10"]
    assert record[0]["other_heads"] == []
    assert record[1]["other_heads"] == [head_a, head_b]


def test_x2_rain_settings_rain_rules_is_byte_identical_across_two_calls() -> None:
    """Two independent rain_settings() calls write byte-identical rain_rules.

    A deterministic, pure-function stand-in for "two rain --freeze-prepare
    runs write byte-identical rain_rules" -- rain_settings() is exactly what
    the header's ``settings`` field holds, with no DB or pipeline involved.
    """
    first = json.dumps(daily_shadow.rain_settings()["rain_rules"], sort_keys=True)
    second = json.dumps(daily_shadow.rain_settings()["rain_rules"], sort_keys=True)
    assert first == second
    assert first.encode() == second.encode()


# --- X3: V_B is null; the map is kept out of frozen_constants ------------------


def test_x3_v_b_is_null_and_not_compared_anywhere() -> None:
    """V_B is None (stage one); rain_settings reports it as a null version text."""
    assert daily_shadow.V_B is None
    assert daily_shadow.rain_settings()["v_b"] is None


def test_x3_wet_hours_first_is_0_16_0() -> None:
    assert WET_HOURS_FIRST == (0, 16, 0)
    assert daily_shadow.rain_settings()["wet_hours_first"] == "0.16.0"


def test_x3_rain_rules_map_is_not_a_frozen_constant() -> None:
    """The map is not in frozen_constants(): it is not a freeze-note constant.

    ``V_B`` itself IS present (as None) -- the frozen-constants surface
    knows about Style B's release number, just not yet the rain map.
    """
    constants = daily_shadow.frozen_constants()
    assert constants["V_B"] is None
    for forbidden_key in ("rain_rules", "RAIN_RULES", "RAIN_RULE_READS", "rule_reads"):
        assert forbidden_key not in constants


# --- X4: dispatch ---------------------------------------------------------------
#
# Nothing to test at runtime in stage one: both call sites, temperature_lines
# and rain_controls, always pass the module-level RAIN_RULES constant (never
# V_B, never a dispatched map), so there is no branch to exercise here yet.
# The pin for this item is static: control_value's signature requires
# rain_rules as a keyword-only parameter, which pyright-strict checks at both
# call sites and Python itself enforces at runtime (a positional rain_rules
# argument raises TypeError); no behavioral test is written for X4.


def test_x4_rain_rules_is_keyword_only_on_control_value() -> None:
    """rain_rules is keyword-only, so no caller can pass it positionally."""
    import inspect

    signature = inspect.signature(control_value)
    parameter = signature.parameters["rain_rules"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


# --- T3/T4/T5/T5b: full runs via main() -----------------------------------------


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path) -> tuple[Path, daily_shadow.Site]:
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "rain-rules-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site


def _insert_precip_row(
    conn: sqlite3.Connection,
    *,
    site: daily_shadow.Site,
    snapshot_date: date,
    app_version: str,
    daily_quantities: str | None,
    policy: str = '{"rain_threshold_mm": 0.2}',
) -> None:
    snapshot_utc = isoformat_utc(resolve_snapshot_utc("UTC", snapshot_date, "07:00"))
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, write_path, policy, methodology_version,
             app_version, candidates, daily_quantities)
        VALUES (?, ?, 'UTC', ?, '07:00:00', ?, ?, 'precip', 0, 'recorded',
                'on_time', ?, 1, ?, NULL, ?)
        """,
        (
            site.site_id,
            site.generation_id,
            snapshot_date.isoformat(),
            snapshot_utc,
            snapshot_date.isoformat(),
            policy,
            app_version,
            daily_quantities,
        ),
    )


def _insert_precip_day_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str,
    values: list[float],
) -> None:
    assert len(values) == 24
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for hour, value in enumerate(values):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


def _run_main(
    *, copy: Path, out: Path, monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[int, str, str]:
    """Run main() in ``--freeze-prepare`` mode (per the plan's own T3-T5 text).

    The rain-rules stop fires in ``rain_controls()`` before the mode branch
    (:6310 vs :6362+), so T3's stop is identical under either mode; T4/T5
    and F5 need ``freeze-prepare`` specifically, since only that mode's
    header calls ``rain_settings()`` (:6409). ``--house-station`` is
    required by the CLI in this mode but the fixture has no matching
    ``stations`` row, so ``load_house_station`` returns its "not in the
    station set" default and never errors.
    """
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _set_deterministic_now(copy: Path, *, mtime: datetime) -> date:
    """Fix the copy's mtime so main()'s ``copied_at`` is deterministic.

    Returns the ``last_complete`` date main() will derive from it.
    """
    stamp = mtime.timestamp()
    os.utime(copy, (stamp, stamp))
    return latest_knowable_target("UTC", mtime)


def _result_payload(out_text: str) -> dict[str, object]:
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    return json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])


@pytest.mark.parametrize("app_version", ["0.16.0", "0.16.8"])
def test_t3_a_version_the_map_does_not_hold_stops_the_full_run(
    app_version: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A full --freeze-prepare run over a single unmapped row ends in ERROR.

    0.16.8 is after the map's last read; 0.16.0 is the F1 boundary -- equal
    to WET_HOURS_FIRST, so not caught by pre_wet_hours, and still absent
    from RAIN_RULES. The stop fires in rain_controls(), before any
    station/event/replay machinery runs, so this fixture needs no station
    or sample data at all -- only the one record row (which also satisfies
    rain_s0's S0 lookup). The run folder holds exactly ``error.json``.
    """
    copy, site = _make_copy(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=_DAY,
            app_version=app_version,
            daily_quantities=_precip_daily_quantities(
                total_mm=1.0, wet_hours=6.0, extrema_feed_ids=[1]
            ),
        )
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))

    out = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert exit_code == 1
    payload = _result_payload(out_text)
    assert payload["exit_code"] == 1
    run_dir = out / cast(str, payload["run_dir"])
    assert {entry.name for entry in run_dir.iterdir()} == {"error.json"}
    error_path = run_dir / "error.json"
    assert error_path.exists()
    error = json.loads(error_path.read_text())
    assert set(error) == {
        "stage",
        "error",
        "sqlite_errorcode",
        "copy_sha256",
        "manifest",
    }
    assert error["stage"] == "run"
    assert error["error"].startswith("VersionUnmappedError: ")
    assert app_version in error["error"]
    assert "no verified control rule maps" in error["error"]
    assert error["sqlite_errorcode"] is None
    assert f"ERROR at stage run: {error['error']}" in err_text


def _wet_dry_values(*, wet_count: int) -> list[float]:
    """24 hourly values, ``wet_count`` of them at/above the 0.2mm threshold."""
    return [0.5] * wet_count + [0.0] * (24 - wet_count)


def _build_matched_run(
    tmp_path: Path,
    *,
    app_version: str,
    wet_count: int,
    snapshot_date: date = _DAY,
) -> tuple[Path, daily_shadow.Site, int]:
    copy, site = _make_copy(tmp_path)
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, "model-a")
        day_before = snapshot_date - timedelta(days=1)
        issued_at = f"{day_before.isoformat()}T06:00:00Z"
        fetched_at = f"{day_before.isoformat()}T06:05:00Z"
        values = _wet_dry_values(wet_count=wet_count)
        _insert_precip_day_samples(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=snapshot_date,
            issued_at=issued_at,
            fetched_at=fetched_at,
            values=values,
        )
        displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
        assert displayed["total_mm"] is not None
        assert displayed["wet_hours"] is not None
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=snapshot_date,
            app_version=app_version,
            daily_quantities=_precip_daily_quantities(
                total_mm=displayed["total_mm"],
                wet_hours=displayed["wet_hours"],
                extrema_feed_ids=[feed_id],
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return copy, site, feed_id


def test_t4_a_mapped_version_run_finishes_with_matched_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single mapped-version (0.16.7) row finishes the run; unmatched.matched == 1.

    No error.json, and no ``*.failed`` sibling of the run directory under
    ``out``: the run completed, it did not stop.
    """
    copy, site, _feed_id = _build_matched_run(
        tmp_path, app_version="0.16.7", wet_count=6
    )
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))

    out = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert exit_code == 0, err_text
    payload = _result_payload(out_text)
    run_dir = out / cast(str, payload["run_dir"])
    header = json.loads((run_dir / "header.json").read_text())
    assert header["unmatched"]["rain"]["matched"]["0"] == 1
    assert not (run_dir / "error.json").exists()
    assert daily_shadow.list_failed_dirs(out) == []


def test_f5_freeze_prepare_header_settings_rain_rules_matches_the_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two separate --freeze-prepare runs produce equal rain_rules settings.

    Each run's ``header["settings"]["rain_rules"]`` is compared against
    ``rain_rules_record(RAIN_RULE_READS)`` as decoded JSON, and the two runs'
    values are compared to each other via ``json.dumps(..., sort_keys=True)``
    -- a key-order-independent equality check, not a byte-for-byte file
    comparison.

    Kills the mutant ``settings={}`` at the freeze-prepare header line: the
    correct header's ``settings.rain_rules`` equals
    ``rain_rules_record(RAIN_RULE_READS)``; the mutant's header has an empty
    ``settings`` dict, so ``header["settings"]["rain_rules"]`` raises
    KeyError instead of matching -- the two diverge on that lookup.
    """
    expected_record = daily_shadow.rain_rules_record(RAIN_RULE_READS)
    copy, site, _feed_id = _build_matched_run(
        tmp_path, app_version="0.16.7", wet_count=6
    )
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))

    serialized_runs: list[bytes] = []
    for run_index in range(2):
        out = tmp_path / f"out{run_index}"
        exit_code, out_text, err_text = _run_main(
            copy=copy, out=out, monkeypatch=monkeypatch, site_id=site.site_id
        )
        assert exit_code == 0, err_text
        payload = _result_payload(out_text)
        run_dir = out / cast(str, payload["run_dir"])
        header = json.loads((run_dir / "header.json").read_text())
        settings = cast("dict[str, object]", header["settings"])
        assert settings["rain_rules"] == expected_record
        serialized_runs.append(
            json.dumps(settings["rain_rules"], sort_keys=True).encode()
        )
    assert serialized_runs[0] == serialized_runs[1]


def _read_lines_jsonl(run_dir: Path) -> list[dict[str, object]]:
    text = (run_dir / "lines.jsonl").read_text()
    return [
        cast("dict[str, object]", json.loads(entry))
        for entry in text.splitlines()
        if entry
    ]


def _control_for_snapshot(
    lines: list[dict[str, object]], *, snapshot_date: date
) -> object:
    """The ``lead=0`` line's control for the given snapshot date.

    ``lines.jsonl`` has one entry per (snapshot_date, lead) pair -- every
    fixture row here is written at ``display_lead=0``, so that lead
    singles out the one case the fixture actually built for this date.
    """
    matches = [
        line
        for line in lines
        if line["snapshot_date"] == snapshot_date.isoformat() and line["lead"] == 0
    ]
    assert len(matches) == 1, matches
    return matches[0]["control"]


def test_t5_a_mixed_run_finishes_with_matched_and_pre_wet_hours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two record rows, one mapped (0.16.7) and one pre-wet-hours (0.15.0).

    The 0.15.0 row's samples match the 0.16.7 case's shape (24 hourly
    values, 6 wet), per the module's own replay-validation policy. Only the
    row's *control* value ignores them: version (0, 15, 0) is below
    WET_HOURS_FIRST, so control_value short-circuits before reading
    daily_quantities and the row is Unmatched ``pre_wet_hours`` rather than
    raising. The row's *candidate* amount in ``lines.jsonl`` is not gated
    the same way -- it is still read from those samples. The run finishes
    (exit 0), and both rows are reflected in ``unmatched``. Kills M145 (the
    pre_wet_hours check removed, so a pre-0.16.0 rain row reaches the map):
    with M145, the 0.15.0 row's version (0, 15, 0) is not in RAIN_RULES and
    the run would end in ERROR instead of finishing with both rows counted.

    The 0.16.7 case's control value is also checked against the single-case
    T4 run: cases are independent, so adding the unrelated 0.15.0 case must
    not change it.
    """
    matched_date = _DAY
    pre_wet_hours_date = _DAY - timedelta(days=1)
    copy, site, _feed_id = _build_matched_run(
        tmp_path, app_version="0.16.7", wet_count=6, snapshot_date=matched_date
    )
    conn = sqlite3.connect(copy)
    try:
        feed_id = asof_make_real_feed(conn, "model-b")
        issued_at = f"{(pre_wet_hours_date - timedelta(days=1)).isoformat()}T06:00:00Z"
        fetched_at = f"{(pre_wet_hours_date - timedelta(days=1)).isoformat()}T06:05:00Z"
        values = _wet_dry_values(wet_count=6)
        _insert_precip_day_samples(
            conn,
            site_id=site.site_id,
            feed_id=feed_id,
            local_date=pre_wet_hours_date,
            issued_at=issued_at,
            fetched_at=fetched_at,
            values=values,
        )
        displayed = displayed_daily("precip", [values], rain_threshold_mm=0.2)
        assert displayed["total_mm"] is not None
        assert displayed["wet_hours"] is not None
        _insert_precip_row(
            conn,
            site=site,
            snapshot_date=pre_wet_hours_date,
            app_version="0.15.0",
            daily_quantities=_precip_daily_quantities(
                total_mm=displayed["total_mm"],
                wet_hours=displayed["wet_hours"],
                extrema_feed_ids=[feed_id],
            ),
        )
        conn.commit()
    finally:
        conn.close()
    _set_deterministic_now(copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))

    out = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch, site_id=site.site_id
    )

    assert exit_code == 0, err_text
    payload = _result_payload(out_text)
    run_dir = out / cast(str, payload["run_dir"])
    header = json.loads((run_dir / "header.json").read_text())
    assert header["unmatched"]["rain"]["matched"]["0"] == 1
    assert header["unmatched"]["rain"]["pre_wet_hours"]["0"] == 1

    mixed_lines = _read_lines_jsonl(run_dir)
    mixed_control = _control_for_snapshot(mixed_lines, snapshot_date=matched_date)

    single_root = tmp_path / "single"
    single_root.mkdir()
    single_copy, single_site, _single_feed_id = _build_matched_run(
        single_root, app_version="0.16.7", wet_count=6
    )
    _set_deterministic_now(single_copy, mtime=datetime(2035, 6, 20, 12, tzinfo=UTC))
    single_out = tmp_path / "single_out"
    single_exit, single_out_text, single_err_text = _run_main(
        copy=single_copy,
        out=single_out,
        monkeypatch=monkeypatch,
        site_id=single_site.site_id,
    )
    assert single_exit == 0, single_err_text
    single_payload = _result_payload(single_out_text)
    single_run_dir = single_out / cast(str, single_payload["run_dir"])
    single_lines = _read_lines_jsonl(single_run_dir)
    single_control = _control_for_snapshot(single_lines, snapshot_date=matched_date)

    assert mixed_control == single_control


def test_t5b_a_real_shaped_0_15_0_row_with_absent_wet_hours_is_pre_wet_hours() -> None:
    """A real-shaped 0.15.0 row: wet_hours absent, chance/total_mm present.

    Covers the shape an actual pre-0.16.0 row has on disk -- ``wet_hours``
    was not yet a stored field, so ``daily_quantities.displayed`` omits it
    (rather than storing an explicit 0.0 as in T5). The pre_wet_hours check
    must still fire before daily_quantities is ever narrowed, so the
    missing key is never read.
    """
    row = RecordRow(
        snapshot_date=_DAY,
        target_date=_DAY,
        lead=0,
        snapshot_utc=isoformat_utc(datetime(2035, 6, 10, 7, tzinfo=UTC)),
        status="recorded",
        write_path="on_time",
        app_version="0.15.0",
        methodology_version=1,
        tz_generation_id=1,
        policy='{"rain_threshold_mm": 0.2}',
        candidates=None,
        daily_quantities=json.dumps({"displayed": {"chance": 0.1, "total_mm": 0.0}}),
    )
    result = control_value(row, variable="precip", rain_rules=RAIN_RULES)
    assert result == Unmatched("pre_wet_hours")


@pytest.mark.skip(
    reason="deferred to stage two (plan §13.1, §15.3): 0.16.10/0.16.11 style_b "
    "rows and the 0.16.9 stop"
)
def test_control_rule_by_version_style_b_deferred_to_stage_two() -> None: ...
