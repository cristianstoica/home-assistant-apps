"""Daily forecast-method trial: the read-only replay and verdict (Phase I).

Run as ``python -m wxverify.verification.daily_shadow`` against a COPY of the
add-on database, never the live file. The copy is opened read-only
(``mode=ro`` plus ``PRAGMA query_only``), its sha256 is taken before opening
and after closing, and every output is computed in memory and written at
once into a fresh directory. Nothing here writes to the database or goes
through the add-on's own entry point, so no migration path is reachable.

The pure rules live in :mod:`wxverify.forecast.daily_method`; this module
reads the copy, replays production's loader as of each decision time, builds
the cases and the truth, and writes the reports.

The rain control reads a map from ``app_version`` to rule
(:data:`RAIN_RULES`, §13.1), which holds only versions whose released code
was read. A rain record row from 0.16.0 on whose version the map does not
hold makes :func:`control_value` raise :class:`VersionUnmappedError`, and the
run ends in ERROR. Stage one's map holds the legacy rule only, so every row
from Style B's release (V_B) on stops this way until stage two.

A rain run's first step is the span check (§13.3): a copy that holds any
``station_precip_spans`` row raises :class:`SpanRowsPresentError` before any
other read of the copy, and the run ends in ERROR.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import sqlite3
import statistics
import subprocess
import sys
import time
from bisect import bisect_right
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from pathlib import Path
from stat import S_ISDIR, S_ISLNK, S_ISREG
from types import MappingProxyType
from typing import Literal, assert_never, cast
from zoneinfo import ZoneInfo

import wxverify
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.runtime_state import get_runtime_state_entry
from wxverify.db.tz_generations import (
    published_generation_clause,
    published_generation_id,
)
from wxverify.forecast import daily_method
from wxverify.forecast.aggregate import (
    covers_local_day,
    covers_local_day_exactly,
    display_day_index,
    displayed_daily,
)
from wxverify.forecast.data import load_future_samples
from wxverify.scoring.consensus import MAD_FLOORS, StationReading, compute_consensus
from wxverify.scoring.effective import active_competitor_clause
from wxverify.verification.coverage import (
    QuantityOutcome,
    evaluate_variable,
    local_day_bounds,
)
from wxverify.verification.simulate import latest_knowable_target
from wxverify.verification.stats import moving_block_indices, percentile_ci

Variable = Literal["temperature", "precip"]
Mode = Literal["counts-only", "freeze-prepare", "heldout"]
RainRule = Literal["legacy"]

# --- Constants (every one is recorded in the freeze note or the rain header) --

V_B: tuple[int, ...] | None = None
"""Style B's release number, null until stage two sets it (§13.1).

Stage two sets it from the checks on Style B's release. No code compares a
row's version with V_B.
"""

LEGACY_GLYPH_MIN_WET_HOURS = 6
"""The page's symbol under the legacy rule: ``round(wet_hours) >= 6``."""

WET_HOURS_FIRST: tuple[int, ...] = (0, 16, 0)
"""The first ``app_version`` whose record rows store ``wet_hours`` (§7.2).

Rain rows before it are unmatched (``pre_wet_hours``).
"""


@dataclass(frozen=True)
class RainRuleRead:
    """One version in the rain control's map, with the read that mapped it (§13.1)."""

    version: tuple[int, ...]
    rule: RainRule
    release_commit: str
    read_to: str
    other_heads: tuple[str, ...]


RAIN_RULE_READS: tuple[RainRuleRead, ...] = (
    RainRuleRead(
        version=(0, 16, 2),
        rule="legacy",
        release_commit="cde769b2db5ae54cd4cbb05d75c652014a995ef3",
        read_to="deb19f3216278cc58e7029bdb90b076373ade775",
        other_heads=(),
    ),
    RainRuleRead(
        version=(0, 16, 3),
        rule="legacy",
        release_commit="deb19f3216278cc58e7029bdb90b076373ade775",
        read_to="b21f342c0b6c3f6127749c462839fa07adbf1079",
        other_heads=(),
    ),
    RainRuleRead(
        version=(0, 16, 4),
        rule="legacy",
        release_commit="b21f342c0b6c3f6127749c462839fa07adbf1079",
        read_to="12a14bd7496deb83bcf5a8f45e624aac0b18c328",
        other_heads=(),
    ),
    RainRuleRead(
        version=(0, 16, 5),
        rule="legacy",
        release_commit="12a14bd7496deb83bcf5a8f45e624aac0b18c328",
        read_to="1d191d9c401579cc7379eb87270647369d0f66d0",
        other_heads=(),
    ),
    RainRuleRead(
        version=(0, 16, 6),
        rule="legacy",
        release_commit="1d191d9c401579cc7379eb87270647369d0f66d0",
        read_to="8cffc73cbf9bf6126c79f2da1b4ed228c0f8e6da",
        other_heads=(),
    ),
    RainRuleRead(
        version=(0, 16, 7),
        rule="legacy",
        release_commit="8cffc73cbf9bf6126c79f2da1b4ed228c0f8e6da",
        read_to="8cffc73cbf9bf6126c79f2da1b4ed228c0f8e6da",
        other_heads=(),
    ),
)
"""The rain control's map, one entry per version read, in version order (§13.1).

It holds only versions whose released code was read, each from its release
commit to ``read_to``; it is not a freeze-note constant, and no temperature
path reads it.
"""

RAIN_RULES: Mapping[tuple[int, ...], RainRule] = MappingProxyType(
    {read.version: read.rule for read in RAIN_RULE_READS}
)
"""The rain control's map from ``app_version`` to rule (§13.1)."""

CHOOSING_FIRST = date(2026, 7, 25)
CHOOSING_LAST = date(2026, 9, 7)
DEFAULT_H_FIRST = date(2026, 9, 8)
HARD_STOP = date(2027, 1, 31)

RECORD_LEADS: tuple[int, ...] = tuple(range(8))
SCORED_LEADS: tuple[int, ...] = tuple(range(6))
LEAD_WEIGHTS: Mapping[int, float] = {lead: 1.0 / 6.0 for lead in SCORED_LEADS}

SEED = 20260929
RESAMPLES = 10_000
BLOCK_LENGTH = 5
SENSITIVITY_BLOCK_LENGTHS: tuple[int, ...] = (3, 5, 7)
MAX_BLOCK_LAG = 15
MAX_DISCARD_SHARE_PERCENT = 1

LEVEL_TILE = 0.975
LEVEL_CHANCE = 0.90
LEVEL_SYMBOL = 0.90
LEVEL_AMOUNT = 0.95

F1_TOLERANCE = 1e-6
F1_MIN_PERCENT = 95
GB_MIN_PERCENT = 98
GB_TEMPERATURE_TOLERANCE_C = 0.05
GB_PRECIP_TOLERANCE_MM = 0.05

TEMP_WINDOW_DAYS = 30
TEMP_MIN_DAYS = 20
IW_FLOOR_C = 0.1
BC_SHRINK = 10.0
BC_SHRINK_SENSITIVITIES: tuple[float, ...] = (5.0, 20.0)
BC_LIMIT_C = 3.0
TD_K_CHOICES: tuple[int, ...] = (1, 2, 3, 5, 10)
TD_K_DEFAULT = 5
TD_K_MIN_DATES = 20
W_HIGH = 1.0
W_HIGH_SENSITIVITIES: tuple[float, ...] = (1.5, 2.0, 3.0)
G2_MARGIN = 1.02
G4_BAND_C = 2.0
G4_MARGIN_PP = 5
FROST_LINE_C = 2.0
FROST_MIN_LEAD0_DATES = 10
WARM_SPELL_MIN_LEAD0_DATES = 15
H_MIN_DATES_PER_LEAD = 60
SPLIT_MIN_DATES = 20
T1_APART_C = 1.0

RAIN_WINDOW_DAYS = 60
RAIN_ADEQUATE_HOURS = 22
RAIN_PAIR_HUNDREDTHS = 20
RAIN_SINGLE_HUNDREDTHS = 50
RAIN_MIN_DRY_STATIONS = 1
RAIN_MIN_DAYS = 20
RAIN_MISS_WEIGHT = 2.0
RAIN_SWITCH_MARGIN = 3
GROUP_FRACTION = Fraction(3, 4)
W_AMOUNT = 1.5
CLIM_MIN_DAYS = 20

FAULT_WET_LINE_HUNDREDTHS = 100
FAULT_DRY_LINE_HUNDREDTHS = 10
FAULT_MIN_PEERS = 3
SUSPECT_LOOKBACK_DAYS = 30
SUSPECT_MIN_DAYS = 5
SUSPECT_RATIO_BELOW = Fraction(1, 2)
QUARANTINE_LOOKBACK_DAYS = 60
QUARANTINE_MIN_DAYS = 8
QUARANTINE_MIN_EPISODES = 3
QUARANTINE_RATIO_AT_MOST = Fraction(1, 4)
QUARANTINE_DRY_SHARE = Fraction(1, 2)
RECOVERY_LOOKBACK_DAYS = 30
RECOVERY_MIN_DAYS = 5
RECOVERY_MIN_EPISODES = 2
RECOVERY_RATIO_AT_LEAST = Fraction(1, 2)
RECOVERY_MAX_DRY_DAYS = 1

LARGE_TOTAL_FACTOR = 2
READS_HIGH_RATIO = Fraction(3, 2)
READS_HIGH_BLOCK_DAYS = 30
GAP_HOURS = 2
LATE_FETCH_HOURS = 24

R1_LAMBDA = 1.0
R1_MAX_ITERATIONS = 25
R1_TOLERANCE = 1e-8
R1_MIN_DAYS = 20
R1_MIN_EACH = 3
M_CIW_MIN_DAYS = 8
M_CIW_FLOOR_MM = 0.1

COUNTS_ONLY_HEADER_KEYS: tuple[str, ...] = (
    "copy",
    "manifest",
    "commit_sha",
    "gates",
    "arm",
    "counts",
    "unmatched",
    "stale_truth_rows",
    "station_set",
    "station_set_changed",
    "db_import",
    "db_import_changed",
    "excluded_h_dates_count",
    "h_training_coverage",
    "rain_event_counts",
    "meteoblue",
    "coverage",
)
"""Every key a ``--counts-only`` header holds, and nothing else (§13.3).

The key set is the same for both variables: the key that does not apply to
the run's variable (``h_training_coverage`` for rain, ``rain_event_counts``
for temperature) is present and null. ``meteoblue`` (A8) and ``coverage``
(A10) apply to both, each for the run's variable (§11.5).
"""

ERROR_KEYS: tuple[str, ...] = (
    "stage",
    "error",
    "sqlite_errorcode",
    "copy_sha256",
    "manifest",
)

NOTE_FORMAT = "wxverify-daily-shadow-freeze-note/1"
HEADER_FORMAT = "wxverify-daily-shadow-header/1"

RESULT_PREFIX = "daily_shadow_result "
"""The result line's prefix (§13.3).

After a completed run (exit 0) and after an ERROR (exit 1), the last line on
standard output is this prefix followed by a JSON object with exactly the
keys ``exit_code`` and ``run_dir``. A refusal (exit 2) prints none.
"""

TEMPERATURE_QUANTITIES: tuple[str, ...] = ("temperature_high", "temperature_low")
RAIN_DECISIONS: tuple[str, ...] = ("symbol", "chance", "amount")
TEMPERATURE_CANDIDATES: tuple[str, ...] = ("IW", "TD-K", "IW-BC", "TD-K-BC")
TWINS: Mapping[str, str] = {"IW-BC": "IW", "TD-K-BC": "TD-K"}


# --- Exceptions ---------------------------------------------------------------


class VersionUnmappedError(RuntimeError):
    """A rain record row from 0.16.0 on whose app_version the map does not hold.

    Raised by :func:`control_value` and never caught, so the run ends in ERROR
    (§13.1).
    """


class SpanRowsPresentError(RuntimeError):
    """A rain run's copy holds ``station_precip_spans`` rows.

    Raised by :func:`main`'s span check. It propagates to the run-level
    error handler in :func:`main`, so the run ends in ERROR at stage
    ``span_check`` (§13.3).
    """


class RefusedError(Exception):
    """A refusal made before the copy is opened; no output is written."""


class CopyChangedError(RuntimeError):
    """The copy's sha256 after the run differs from the one before it."""


class CodeChangedError(RuntimeError):
    """The package's ``.py`` files after the run differ from those before it."""


class ReadOnlyCheckError(RuntimeError):
    """The connection did not confirm ``query_only``."""


# --- Record rows, versions and the control (§7.2) -----------------------------


@dataclass(frozen=True)
class RecordRow:
    """One ``forecast_of_record`` row of the published generation, as stored.

    The three JSON columns are kept as raw text; each is narrowed by its own
    function (:func:`narrow_policy`, :func:`narrow_daily_quantities`,
    :func:`narrow_candidates`).
    """

    snapshot_date: date
    target_date: date
    lead: int
    snapshot_utc: str
    status: str
    write_path: str | None
    app_version: str
    methodology_version: int
    tz_generation_id: int
    policy: str | None
    candidates: str | None
    daily_quantities: str | None


@dataclass(frozen=True)
class Unmatched:
    """Why a row or case has no control or no case: the reason, counted."""

    reason: str


@dataclass(frozen=True)
class PolicyView:
    """The parts of a row's ``policy`` the trial reads."""

    rain_threshold_mm: float
    segment: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DisplayedView:
    """The parts of a row's ``daily_quantities`` the trial reads."""

    values: Mapping[str, float | None]
    extrema_feed_ids: tuple[int, ...] | None
    displayed: object


@dataclass(frozen=True)
class CandidateEntry:
    """One entry of a row's ``candidates`` list."""

    feed_id: int
    participation: str


@dataclass(frozen=True)
class ControlValue:
    """The control for one record row: what the page showed (§7.2)."""

    variable: str
    high: float | None
    low: float | None
    symbol: bool | None
    amount: float | None
    wet_hours: float | None
    version: tuple[int, ...]
    policy: PolicyView
    displayed: DisplayedView


def parse_version(text: str) -> tuple[int, ...] | None:
    """``app_version`` as a tuple of integers, or None when it does not parse.

    Every dot-separated part must be ASCII digits only, so ``"0.16.9b1"`` does
    not parse and ``"0.16.10"`` parses to ``(0, 16, 10)``.
    """
    parts = text.split(".")
    if not parts or not all(p.isascii() and p.isdigit() for p in parts):
        return None
    return tuple(int(p) for p in parts)


def _load_json(raw: str | None) -> object:
    if raw is None:
        return None
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return None
    return parsed


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        number = float(value)
        return number if math.isfinite(number) else None
    return None


def narrow_policy(raw: str | None) -> PolicyView | Unmatched:
    """Narrow the ``policy`` column, or record the row unmatched."""
    parsed = _load_json(raw)
    if not isinstance(parsed, dict):
        return Unmatched("policy_unreadable")
    policy = cast("dict[str, object]", parsed)
    threshold = _as_float(policy.get("rain_threshold_mm"))
    if threshold is None:
        return Unmatched("policy_unreadable")
    depths_raw = policy.get("blend_depths")
    depths: dict[str, object] = (
        cast("dict[str, object]", depths_raw) if isinstance(depths_raw, dict) else {}
    )
    segment = (
        ("blend_depths.temperature", json.dumps(depths.get("temperature"))),
        ("blend_depths.precip", json.dumps(depths.get("precip"))),
        ("min_n", json.dumps(policy.get("min_n"))),
        ("window_days", json.dumps(policy.get("window_days"))),
        ("rain_threshold_mm", json.dumps(threshold)),
    )
    return PolicyView(rain_threshold_mm=threshold, segment=segment)


def narrow_daily_quantities(
    raw: str | None, *, variable: str
) -> DisplayedView | Unmatched:
    """Narrow the ``daily_quantities`` column, or record the row unmatched."""
    parsed = _load_json(raw)
    if not isinstance(parsed, dict):
        return Unmatched("daily_quantities_unreadable")
    shown = cast("dict[str, object]", parsed).get("displayed")
    if not isinstance(shown, dict):
        return Unmatched("daily_quantities_unreadable")
    displayed = cast("dict[str, object]", shown)
    keys = (
        ("high_c", "low_c") if variable == "temperature" else ("total_mm", "wet_hours")
    )
    values: dict[str, float | None] = {}
    for key in keys:
        if key not in displayed:
            return Unmatched("daily_quantities_unreadable")
        item = displayed[key]
        if item is None:
            values[key] = None
            continue
        number = _as_float(item)
        if number is None:
            return Unmatched("daily_quantities_unreadable")
        values[key] = number
    ids_raw = displayed.get("extrema_feed_ids")
    extrema: tuple[int, ...] | None = None
    if ids_raw is not None:
        if not isinstance(ids_raw, list):
            return Unmatched("daily_quantities_unreadable")
        ids: list[int] = []
        for item in cast("list[object]", ids_raw):
            if isinstance(item, bool) or not isinstance(item, int):
                return Unmatched("daily_quantities_unreadable")
            ids.append(item)
        extrema = tuple(ids)
    return DisplayedView(values=values, extrema_feed_ids=extrema, displayed=displayed)


def narrow_candidates(raw: str | None) -> tuple[CandidateEntry, ...] | Unmatched:
    """Narrow the ``candidates`` column, or record the row unmatched."""
    parsed = _load_json(raw)
    if not isinstance(parsed, list):
        return Unmatched("candidates_unreadable")
    out: list[CandidateEntry] = []
    for item in cast("list[object]", parsed):
        if not isinstance(item, dict):
            return Unmatched("candidates_unreadable")
        entry = cast("dict[str, object]", item)
        feed_id = entry.get("feed_id")
        participation = entry.get("participation")
        if isinstance(feed_id, bool) or not isinstance(feed_id, int):
            return Unmatched("candidates_unreadable")
        if not isinstance(participation, str):
            return Unmatched("candidates_unreadable")
        out.append(CandidateEntry(feed_id=feed_id, participation=participation))
    return tuple(out)


def _version_text(version: Sequence[int]) -> str:
    return ".".join(str(part) for part in version)


def control_value(
    row: RecordRow,
    *,
    variable: Variable,
    rain_rules: Mapping[tuple[int, ...], RainRule],
) -> ControlValue | Unmatched:
    """The control for one record row, or why the row is unmatched (§7.2).

    Order: a ``missed`` row is unmatched (``missed``); then ``app_version`` is
    parsed (``version_unparseable``); then, for rain only, a version below
    :data:`WET_HOURS_FIRST` is unmatched (``pre_wet_hours``), and a version
    ``rain_rules`` does not hold raises :class:`VersionUnmappedError` (never
    caught, never counted as unmatched, §13.1); then the JSON columns are
    narrowed and a null value is ``control_null``. Temperature never reads
    ``rain_rules`` and reads ``high_c`` and ``low_c``. Rain under the legacy
    rule: symbol = ``round(wet_hours) >= LEGACY_GLYPH_MIN_WET_HOURS``, amount =
    ``total_mm``.
    """
    if row.status != "recorded":
        return Unmatched("missed")
    version = parse_version(row.app_version)
    if version is None:
        return Unmatched("version_unparseable")
    rule: RainRule | None = None
    if variable == "precip":
        if version < WET_HOURS_FIRST:
            return Unmatched("pre_wet_hours")
        rule = rain_rules.get(version)
        if rule is None:
            raise VersionUnmappedError(
                f"rain record row with app_version {row.app_version!r}: no verified "
                "control rule maps this version, so its release mapping is unresolved"
            )
    policy = narrow_policy(row.policy)
    if isinstance(policy, Unmatched):
        return policy
    shown = narrow_daily_quantities(row.daily_quantities, variable=variable)
    if isinstance(shown, Unmatched):
        return shown
    if any(value is None for value in shown.values.values()):
        return Unmatched("control_null")
    if variable == "temperature":
        return ControlValue(
            variable=variable,
            high=shown.values["high_c"],
            low=shown.values["low_c"],
            symbol=None,
            amount=None,
            wet_hours=None,
            version=version,
            policy=policy,
            displayed=shown,
        )
    wet_hours = shown.values["wet_hours"]
    total = shown.values["total_mm"]
    assert wet_hours is not None and total is not None
    assert rule is not None
    match rule:
        case "legacy":
            symbol = round(wet_hours) >= LEGACY_GLYPH_MIN_WET_HOURS
        case _:
            assert_never(rule)
    return ControlValue(
        variable=variable,
        high=None,
        low=None,
        symbol=symbol,
        amount=total,
        wet_hours=wet_hours,
        version=version,
        policy=policy,
        displayed=shown,
    )


# --- The copy: refusals, opening, integrity (§13.3) ---------------------------


def refuse_unsafe_copy(copy: Path) -> None:
    """Refuse a copy before it is opened (§13.3).

    Refused: a path that does not exist; a path whose real path equals that
    of ``config.db_path`` (read through the module at call time, so a patched
    value is seen); a path that is not a regular file.
    """
    if not os.path.lexists(copy):
        raise RefusedError(f"the copy does not exist: {copy}")
    if os.path.realpath(copy) == os.path.realpath(config.db_path):
        raise RefusedError("the copy resolves to the live database path")
    if not copy.is_file():
        raise RefusedError(f"the copy is not a regular file: {copy}")


def open_copy_readonly(copy: Path) -> sqlite3.Connection:
    """Open the copy read-only and confirm it (§13.3).

    ``mode=ro`` through a URI, then ``PRAGMA query_only=ON`` with a read-back
    that must be 1, then ``SELECT COUNT(*) FROM sqlite_master``. The caller
    closes the connection. On any failure here the connection is closed
    before the exception propagates.
    """
    conn = sqlite3.connect(f"file:{copy}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        row = conn.execute("PRAGMA query_only").fetchone()
        if row is None or int(row[0]) != 1:
            raise ReadOnlyCheckError("PRAGMA query_only did not read back as 1")
        conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except BaseException:
        conn.close()
        raise
    return conn


def find_sqlite_errorcode(exc: BaseException) -> int | None:
    """The ``sqlite_errorcode`` of the first :class:`sqlite3.Error` in the chain.

    Walks from ``exc`` along ``__cause__``, else ``__context__``, visiting
    each exception once; None when no ``sqlite3.Error`` carries a code.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, sqlite3.Error):
            code: object = getattr(current, "sqlite_errorcode", None)
            if isinstance(code, int) and not isinstance(code, bool):
                return code
            return None
        current = (
            current.__cause__ if current.__cause__ is not None else current.__context__
        )
    return None


def sha256_file(path: Path) -> str:
    """The hex sha256 of a file's bytes, read in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def package_root() -> Path:
    """The directory of the ``wxverify`` package actually imported (§13.3).

    ``main()`` calls it once and passes that one value to :func:`check_commit`
    and to the end-of-run code check.
    """
    return Path(wxverify.__file__).parent


def _raise_walk_error(error: OSError) -> None:
    """``os.walk``'s ``onerror``: raise the error, so no directory is skipped."""
    raise error


def _check_entry_kind(root: Path, relative: Path, *, directory: bool) -> None:
    """Raise ``OSError`` unless ``root / relative`` is a directory or a regular file.

    ``directory`` says which the walk listed it as. ``os.lstat`` does not
    follow a link, so a symbolic link, to a file or to a directory, raises,
    as does an entry of any other kind (§13.3).
    """
    mode = os.lstat(root / relative).st_mode
    shown = json.dumps(relative.as_posix())
    if S_ISLNK(mode):
        raise OSError(f"{shown} is a symbolic link")
    if not (S_ISDIR(mode) if directory else S_ISREG(mode)):
        raise OSError(f"{shown} is neither a regular file nor a directory")


def package_snapshot(root: Path) -> tuple[dict[str, object], dict[str, str]]:
    """The package manifest of ``root`` and each file's git object id (§13.3).

    The files are found with ``os.walk``, whose ``onerror`` raises, so a
    directory that cannot be listed raises ``OSError`` instead of being
    skipped; ``__pycache__`` directories are not entered. Every other name
    the walk lists, whatever its suffix, is checked with ``os.lstat``: a
    listed subdirectory must be a directory and a listed file a regular
    file, so a symbolic link or an entry of another kind raises ``OSError``
    rather than being read through or passed over. Every file whose name
    ends ``.py`` is kept. Each file is read once, with one
    ``Path.read_bytes()``, in the manifest's order (the relative paths
    sorted as ``Path`` objects); from those bytes come both its sha256 for
    the manifest and its git blob id. Returns the manifest (``files``: the
    POSIX relative paths with their sha256; ``python``:
    ``sys.version_info[:2]``) and a mapping from each POSIX relative path to
    its object id. An unreadable file raises ``OSError`` too.
    """
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, onerror=_raise_walk_error):
        dirnames[:] = [name for name in dirnames if name != "__pycache__"]
        base = Path(dirpath).relative_to(root)
        for name in dirnames:
            _check_entry_kind(root, base / name, directory=True)
        for name in filenames:
            _check_entry_kind(root, base / name, directory=False)
        found.extend(base / name for name in filenames if name.endswith(".py"))
    files: list[list[str]] = []
    object_ids: dict[str, str] = {}
    for relative in sorted(found):
        data = (root / relative).read_bytes()
        path = relative.as_posix()
        files.append([path, hashlib.sha256(data).hexdigest()])
        object_ids[path] = hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
    return {"files": files, "python": list(sys.version_info[:2])}, object_ids


def package_manifest(root: Path) -> dict[str, object]:
    """Every ``*.py`` under ``root`` with its sha256, and the Python version.

    ``package_snapshot(root)[0]``, so the commit check and the end-of-run
    code check find and read the files the same way. Paths are relative to
    ``root``, POSIX-style, sorted; ``__pycache__`` is skipped. ``python`` is
    ``sys.version_info[:2]``. A directory that cannot be listed or a file
    that cannot be read raises ``OSError``.
    """
    return package_snapshot(root)[0]


def changed_paths(first: Mapping[str, str], second: Mapping[str, str]) -> str:
    """``changed [...]; added [...]; removed [...]`` between two path maps (§13.3).

    Each map goes from a relative path to a digest. ``changed``: the paths
    both have with different digests; ``added``: those only ``second`` has;
    ``removed``: those only ``first`` has. Each list is sorted and written
    with ``json.dumps``.
    """
    changed = sorted(
        path for path in first.keys() & second.keys() if first[path] != second[path]
    )
    added = sorted(second.keys() - first.keys())
    removed = sorted(first.keys() - second.keys())
    return (
        f"changed {json.dumps(changed)}; added {json.dumps(added)}; "
        f"removed {json.dumps(removed)}"
    )


def _manifest_digests(manifest: Mapping[str, object]) -> dict[str, str]:
    """A package manifest's ``files`` entries as a map from path to sha256."""
    entries = cast("Sequence[Sequence[str]]", manifest["files"])
    return {path: digest for path, digest in entries}


def _git_environment() -> dict[str, str]:
    """The environment the commit check gives git (§13.3).

    ``os.environ`` without any name that begins ``GIT_``, except
    ``GIT_CEILING_DIRECTORIES``, which is passed unchanged; plus
    ``GIT_NO_REPLACE_OBJECTS=1``, ``GIT_CONFIG_NOSYSTEM=1`` and
    ``GIT_CONFIG_GLOBAL`` set to ``os.devnull``. Inherited ``GIT_DIR``,
    ``GIT_WORK_TREE``, ``GIT_INDEX_FILE`` and the like would point git at
    another repository, index or object store, and ``-C`` does not undo
    them; the one name kept can only stop git's upward search, never move it.
    """
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("GIT_") or name == "GIT_CEILING_DIRECTORIES"
    }
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    return env


def _run_git(package_dir: Path, *args: str) -> bytes:
    """Run ``git -C package_dir *args``; its standard output, as bytes, on exit 0.

    Bytes mode, with no newline translation: text mode would turn a ``\\r``
    in a path into ``\\n``. git missing (``OSError``), a timeout and a
    non-zero exit each raise :class:`RefusedError` naming the command: a
    check that cannot be completed refuses. Standard error is decoded, with
    ``errors="replace"``, only for a non-zero exit's message. The environment
    is :func:`_git_environment`'s.
    """
    name = " ".join(("git", *args))
    try:
        result = subprocess.run(
            ["git", "-C", str(package_dir), *args],
            capture_output=True,
            check=False,
            timeout=30,
            env=_git_environment(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RefusedError(f"commit check: {name} could not run: {exc}") from exc
    if result.returncode != 0:
        stderr = result.stderr or b""
        message = stderr.decode("utf-8", errors="replace").strip().splitlines()
        reason = message[0][:200] if message else "no message"
        raise RefusedError(f"commit check: {name} exited {result.returncode}: {reason}")
    return result.stdout


_LS_TREE_KINDS: frozenset[tuple[str, str]] = frozenset(
    {
        ("100644", "blob"),
        ("100755", "blob"),
        ("120000", "blob"),
        ("160000", "commit"),
    }
)
"""The (mode, type) pairs ``git ls-tree -r`` may print: two regular-file
modes, a symbolic link and a submodule (§13.3)."""


def _unparseable_record(record: bytes) -> RefusedError:
    return RefusedError(
        f"commit check: git ls-tree printed a record it cannot parse: {record!r}"
    )


def _ls_tree_object_ids(listing: bytes, *, commit_sha: str) -> dict[str, str]:
    """The ``.py`` entries of ``git ls-tree -r -z`` output: path to object id.

    The output is split at ``b"\\0"``. The last piece must be empty and is
    the only one dropped; every other piece is one record, split at its
    first ``b"\\t"`` only. Before the tab, decoded as ASCII: exactly three
    fields separated by single spaces, ``<mode> <type> <object id>``, the
    (mode, type) pair one of :data:`_LS_TREE_KINDS` and the id 40 lowercase
    hex digits. After it, decoded with ``os.fsdecode`` as ``os.walk`` decodes
    names: a non-empty path that no other record has. Anything else, a part
    that cannot be decoded included, refuses with :class:`RefusedError`
    showing the record's bytes. Every record is checked first; then a
    symbolic link or a submodule outside ``__pycache__``, whatever its name,
    refuses. The entries kept are those whose path ends ``.py`` and has no
    ``__pycache__`` component, all of them regular files.
    """
    pieces = listing.split(b"\0")
    if pieces[-1]:
        raise _unparseable_record(pieces[-1])
    records: dict[str, tuple[str, str]] = {}
    for record in pieces[:-1]:
        meta_bytes, tab, path_bytes = record.partition(b"\t")
        try:
            meta = meta_bytes.decode("ascii")
            path = os.fsdecode(path_bytes)
        except UnicodeDecodeError:
            raise _unparseable_record(record) from None
        parts = meta.split(" ")
        if (
            not tab
            or not path
            or len(parts) != 3
            or (parts[0], parts[1]) not in _LS_TREE_KINDS
            or re.fullmatch(r"[0-9a-f]{40}", parts[2]) is None
            or path in records
        ):
            raise _unparseable_record(record)
        records[path] = (parts[0], parts[2])
    kept: dict[str, str] = {}
    for path, (mode, object_id) in records.items():
        if "__pycache__" in path.split("/"):
            continue
        if mode == "120000":
            raise RefusedError(
                f"commit check: {commit_sha} holds {json.dumps(path)} "
                "as a symbolic link"
            )
        if mode == "160000":
            raise RefusedError(
                f"commit check: {commit_sha} holds {json.dumps(path)} as a submodule"
            )
        if path.endswith(".py"):
            kept[path] = object_id
    return kept


def check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    """Refuse unless the package's ``.py`` files are ``commit_sha``'s (§13.3).

    Two git commands, in order, from ``package_dir`` with
    :func:`_git_environment`, both in bytes mode: ``rev-parse --verify
    HEAD^{commit}``, decoded as ASCII and stripped, must equal
    ``commit_sha``; and ``ls-tree -r -z --full-tree <sha>:wxverify/wxverify``
    lists the commit's package files, parsed by :func:`_ls_tree_object_ids`
    (a link or a submodule in the commit refuses). Then
    :func:`package_snapshot` reads the files once (a link on disk refuses),
    and the commit's and the files' (path, object id) pairs must be equal,
    in both directions, ignored files included. No command reads the index,
    so no index flag can hide a change. Every failure raises
    :class:`RefusedError` with a message that begins ``commit check:``.
    Returns the manifest of the bytes compared.
    """
    raw_head = _run_git(package_dir, "rev-parse", "--verify", "HEAD^{commit}")
    try:
        head = raw_head.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise RefusedError(
            "commit check: git rev-parse --verify HEAD^{commit} printed output "
            f"that is not ASCII: {raw_head[:100]!r}"
        ) from exc
    if head != commit_sha:
        raise RefusedError(
            f"commit check: the package's HEAD is {head[:64]!r}, "
            f"not --commit-sha {commit_sha}"
        )
    committed = _ls_tree_object_ids(
        _run_git(
            package_dir,
            "ls-tree",
            "-r",
            "-z",
            "--full-tree",
            f"{commit_sha}:wxverify/wxverify",
        ),
        commit_sha=commit_sha,
    )
    try:
        manifest, on_disk = package_snapshot(package_dir)
    except OSError as exc:
        raise RefusedError(
            f"commit check: the package's .py files could not be read: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if on_disk != committed:
        raise RefusedError(
            f"commit check: the package's .py files differ from {commit_sha}: "
            f"{changed_paths(committed, on_disk)}"
        )
    return manifest


def copy_facts(copy: Path, sha256: str) -> dict[str, object]:
    """The copy's path, size, mtime and sha256 for a header."""
    stat = copy.stat()
    return {
        "path": str(copy),
        "size": stat.st_size,
        "mtime": isoformat_utc(datetime.fromtimestamp(stat.st_mtime, tz=UTC)),
        "sha256": sha256,
    }


def error_after_open(
    copy: Path, *, before: str, stage: str, exc: BaseException
) -> tuple[str, str]:
    """An ERROR's ``stage`` and ``error`` text once the copy's open was tried (§13.3).

    The copy's sha256 is read again, whatever failed: a difference makes the
    stage ``copy_changed`` and names the original failure after it; a copy
    that cannot be read keeps the stage and says so, so "can't tell" never
    reads as "unchanged". A :class:`CopyChangedError` is already that finding
    and is returned as it is.
    """
    text = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, CopyChangedError):
        return stage, text
    try:
        after = sha256_file(copy)
    except OSError as read_exc:
        return (
            stage,
            f"{text}; copy sha256 after: unreadable "
            f"({type(read_exc).__name__}: {read_exc})",
        )
    if after != before:
        return (
            "copy_changed",
            f"CopyChangedError: the copy's sha256 changed during the run: "
            f"{before} -> {after}; while handling {text}",
        )
    return stage, text


# --- Writes (§13.3) ------------------------------------------------------------


def _write_file(path: Path, data: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def write_outputs_atomically(final_dir: Path, files: Mapping[str, bytes]) -> Path:
    """Write every file into ``<final_dir>.partial/``, then rename it once.

    ``final_dir`` must not exist. If any write or the rename fails, the
    partial directory is renamed ``<final_dir>.failed/`` and the error is
    raised again.
    """
    if final_dir.exists():
        raise FileExistsError(f"the output directory already exists: {final_dir}")
    partial = final_dir.with_name(final_dir.name + ".partial")
    partial.mkdir(parents=True)
    try:
        for name, data in files.items():
            _write_file(partial / name, data)
        os.replace(partial, final_dir)
    except BaseException:
        if partial.exists():
            os.replace(
                partial, _free_name(final_dir.with_name(final_dir.name + ".failed"))
            )
        raise
    return final_dir


def _free_name(path: Path) -> Path:
    candidate = path
    index = 2
    while candidate.exists():
        candidate = path.with_name(f"{path.name}-{index}")
        index += 1
    return candidate


def rename_stale_partials(out: Path) -> list[str]:
    """Rename every ``*.partial`` directory under ``out`` to ``*.failed``.

    Returns the renamed paths relative to ``out``, sorted. A name already
    taken gets a ``-N`` suffix. It begins with ``out.stat()``: only a
    ``FileNotFoundError`` returns ``[]`` (the run's write creates ``out``);
    any other error from it propagates. The walk's ``onerror`` raises, so an
    ``out`` that is not a directory raises ``NotADirectoryError`` and a
    directory under ``out`` that cannot be listed raises its ``OSError``,
    instead of being passed over (§13.3).
    """
    try:
        out.stat()
    except FileNotFoundError:
        return []
    found: list[Path] = []
    for dirpath, dirnames, _ in os.walk(out, onerror=_raise_walk_error):
        for name in dirnames:
            if name.endswith(".partial"):
                found.append(Path(dirpath) / name)
    renamed: list[str] = []
    for path in sorted(found, key=lambda p: len(p.parts), reverse=True):
        target = _free_name(path.with_name(path.name[: -len(".partial")] + ".failed"))
        os.replace(path, target)
        renamed.append(target.relative_to(out).as_posix())
    return sorted(renamed)


_FAILED_NAME = re.compile(r"\.failed(?:-\d+)?\Z")


def list_failed_dirs(out: Path) -> list[str]:
    """Every ``*.failed`` directory under ``out``, relative to it, sorted.

    A ``-N`` suffix (a name already taken, :func:`_free_name`) still counts.
    A failed directory's own contents are not searched. It begins with
    ``out.stat()``: only a ``FileNotFoundError`` returns ``[]``; any other
    error from it propagates. The walk's ``onerror`` raises, so an ``out``
    that is not a directory raises ``NotADirectoryError`` and a directory
    under ``out`` that cannot be listed raises its ``OSError``, instead of
    being passed over (§13.3).
    """
    try:
        out.stat()
    except FileNotFoundError:
        return []
    found: list[str] = []
    for dirpath, dirnames, _ in os.walk(out, onerror=_raise_walk_error):
        failed = [name for name in dirnames if _FAILED_NAME.search(name)]
        for name in failed:
            found.append((Path(dirpath) / name).relative_to(out).as_posix())
        dirnames[:] = [name for name in dirnames if name not in failed]
    return sorted(found)


def _jsonable(value: object) -> object:
    """A JSON-ready copy: dataclasses as objects, Fractions as strings.

    Dates and datetimes become ISO text; a non-finite float becomes null;
    a mapping's tuple key is joined with ``|``.
    """
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Fraction):
        return str(value)
    if isinstance(value, datetime):
        return isoformat_utc(value) if value.tzinfo is not None else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: _jsonable(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        mapping = cast("Mapping[object, object]", value)
        return {_json_key(k): _jsonable(v) for k, v in mapping.items()}
    if isinstance(value, list | tuple | set | frozenset):
        items = cast("Iterable[object]", value)
        if isinstance(value, set | frozenset):
            return sorted((_jsonable(v) for v in items), key=_sort_key)
        return [_jsonable(v) for v in items]
    return str(value)


def _sort_key(value: object) -> str:
    return json.dumps(value, sort_keys=True)


def _json_key(key: object) -> str:
    if isinstance(key, tuple):
        parts = cast("tuple[object, ...]", key)
        return "|".join(_json_key(part) for part in parts)
    if isinstance(key, date):
        return key.isoformat()
    return str(key)


def _dumps(value: object) -> bytes:
    return (json.dumps(_jsonable(value), indent=2, sort_keys=True) + "\n").encode()


def _dumps_lines(lines: Iterable[object]) -> bytes:
    return "".join(
        json.dumps(_jsonable(line), sort_keys=True) + "\n" for line in lines
    ).encode()


# --- The changed-header rule, the station set and the import record (§6.5) --

StationEntry = tuple[str, bool, str]


def changed_header_outcome(
    outcome: str,
    *,
    station_set_changed: Mapping[str, Sequence[str]] | None,
    db_import_changed: bool | None,
) -> str:
    """INCONCLUSIVE when the station set or the import record changed (§8.6).

    Any of the four lists in ``station_set_changed`` non-empty, or
    ``db_import_changed`` true, gives INCONCLUSIVE; otherwise ``outcome``.
    """
    if db_import_changed:
        return "INCONCLUSIVE"
    if station_set_changed is not None and any(
        station_set_changed.get(key)
        for key in ("added", "removed", "toggled", "recreated")
    ):
        return "INCONCLUSIVE"
    return outcome


def load_station_set(conn: sqlite3.Connection, *, site_id: int) -> list[StationEntry]:
    """The site's (pws_station_id, enabled, created_at), sorted."""
    rows = conn.execute(
        "SELECT pws_station_id, enabled, created_at FROM stations WHERE site_id = ?",
        (site_id,),
    ).fetchall()
    return sorted(
        (str(r["pws_station_id"]), bool(r["enabled"]), str(r["created_at"]))
        for r in rows
    )


def station_set_changes(
    current: Sequence[StationEntry], noted: Sequence[StationEntry]
) -> dict[str, list[str]]:
    """Added, removed, toggled and recreated ``pws_station_id`` against the note."""
    now = {sid: (enabled, created) for sid, enabled, created in current}
    then = {sid: (enabled, created) for sid, enabled, created in noted}
    return {
        "added": sorted(set(now) - set(then)),
        "removed": sorted(set(then) - set(now)),
        "toggled": sorted(s for s in set(now) & set(then) if now[s][0] != then[s][0]),
        "recreated": sorted(s for s in set(now) & set(then) if now[s][1] != then[s][1]),
    }


def load_db_import(conn: sqlite3.Connection) -> dict[str, str] | None:
    """The ``import_rebuild_state`` row's value and updated_at, or None."""
    entry = get_runtime_state_entry(conn, "import_rebuild_state")
    if entry is None:
        return None
    return {"value": entry.value, "updated_at": entry.updated_at}


# --- The freeze note (§10.2) -----------------------------------------------------


def frozen_constants() -> dict[str, object]:
    """Every constant the freeze note records and ``--heldout`` re-checks."""
    return cast(
        "dict[str, object]",
        _jsonable(
            {
                "V_B": None if V_B is None else list(V_B),
                "LEGACY_GLYPH_MIN_WET_HOURS": LEGACY_GLYPH_MIN_WET_HOURS,
                "CHOOSING_FIRST": CHOOSING_FIRST,
                "CHOOSING_LAST": CHOOSING_LAST,
                "HARD_STOP": HARD_STOP,
                "LEAD_WEIGHTS": dict(LEAD_WEIGHTS),
                "SEED": SEED,
                "RESAMPLES": RESAMPLES,
                "BLOCK_LENGTH": BLOCK_LENGTH,
                "LEVEL_TILE": LEVEL_TILE,
                "F1_TOLERANCE": F1_TOLERANCE,
                "F1_MIN_PERCENT": F1_MIN_PERCENT,
                "GB_MIN_PERCENT": GB_MIN_PERCENT,
                "GB_TEMPERATURE_TOLERANCE_C": GB_TEMPERATURE_TOLERANCE_C,
                "TEMP_WINDOW_DAYS": TEMP_WINDOW_DAYS,
                "TEMP_MIN_DAYS": TEMP_MIN_DAYS,
                "IW_FLOOR_C": IW_FLOOR_C,
                "BC_SHRINK": BC_SHRINK,
                "BC_LIMIT_C": BC_LIMIT_C,
                "TD_K_CHOICES": list(TD_K_CHOICES),
                "W_HIGH": W_HIGH,
                "G2_MARGIN": G2_MARGIN,
                "G4_BAND_C": G4_BAND_C,
                "G4_MARGIN_PP": G4_MARGIN_PP,
                "FROST_LINE_C": FROST_LINE_C,
                "FROST_MIN_LEAD0_DATES": FROST_MIN_LEAD0_DATES,
                "WARM_SPELL_MIN_LEAD0_DATES": WARM_SPELL_MIN_LEAD0_DATES,
                "H_MIN_DATES_PER_LEAD": H_MIN_DATES_PER_LEAD,
                "SPLIT_MIN_DATES": SPLIT_MIN_DATES,
                "MAX_DISCARD_SHARE_PERCENT": MAX_DISCARD_SHARE_PERCENT,
            }
        ),
    )


@dataclass(frozen=True)
class FreezeNote:
    """The parts of a freeze note the tool reads, narrowed (§10.2)."""

    site_id: int
    first_h_date: date
    excluded_h_dates: tuple[date, ...]
    station_set: tuple[StationEntry, ...]
    db_import: Mapping[str, str] | None
    manifest: Mapping[str, object]
    constants: Mapping[str, object]
    house_station: str | None
    td_k: int


def _note_date(value: object, what: str) -> date:
    if not isinstance(value, str):
        raise RefusedError(f"freeze note: {what} is not a date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RefusedError(f"freeze note: {what} is not a date") from exc


def load_freeze_note(path: Path) -> FreezeNote:
    """Read and narrow a freeze note; any malformed field is a refusal."""
    try:
        parsed: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RefusedError(f"freeze note unreadable: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RefusedError("freeze note is not a JSON object")
    note = cast("dict[str, object]", parsed)
    if note.get("format") != NOTE_FORMAT:
        raise RefusedError(f"freeze note format is not {NOTE_FORMAT}")
    site = note.get("site")
    site_id: object = (
        cast("dict[str, object]", site).get("id") if isinstance(site, dict) else None
    )
    if isinstance(site_id, bool) or not isinstance(site_id, int):
        raise RefusedError("freeze note: site.id is not an integer")
    excluded_raw = note.get("excluded_h_dates", [])
    if not isinstance(excluded_raw, list):
        raise RefusedError("freeze note: excluded_h_dates is not a list")
    excluded = tuple(
        _note_date(item, "excluded_h_dates")
        for item in cast("list[object]", excluded_raw)
    )
    stations_raw = note.get("station_set")
    if not isinstance(stations_raw, list):
        raise RefusedError("freeze note: station_set is not a list")
    stations: list[StationEntry] = []
    for item in cast("list[object]", stations_raw):
        if not isinstance(item, list) or len(cast("list[object]", item)) != 3:
            raise RefusedError("freeze note: a station_set entry is malformed")
        sid, enabled, created = cast("list[object]", item)
        if not (
            isinstance(sid, str)
            and isinstance(enabled, bool)
            and isinstance(created, str)
        ):
            raise RefusedError("freeze note: a station_set entry is malformed")
        stations.append((sid, enabled, created))
    import_raw = note.get("db_import")
    db_import: dict[str, str] | None = None
    if import_raw is not None:
        if not isinstance(import_raw, dict):
            raise RefusedError("freeze note: db_import is malformed")
        record = cast("dict[str, object]", import_raw)
        value, updated = record.get("value"), record.get("updated_at")
        if not (isinstance(value, str) and isinstance(updated, str)):
            raise RefusedError("freeze note: db_import is malformed")
        db_import = {"value": value, "updated_at": updated}
    manifest = note.get("manifest")
    constants = note.get("constants")
    if not isinstance(manifest, dict) or not isinstance(constants, dict):
        raise RefusedError("freeze note: manifest or constants missing")
    house = note.get("house_station")
    if house is not None and not isinstance(house, str):
        raise RefusedError("freeze note: house_station is not text")
    td_k = note.get("td_k")
    if isinstance(td_k, bool) or not isinstance(td_k, int) or td_k not in TD_K_CHOICES:
        raise RefusedError("freeze note: td_k is not one of the K choices")
    return FreezeNote(
        site_id=site_id,
        first_h_date=_note_date(note.get("first_h_date"), "first_h_date"),
        excluded_h_dates=excluded,
        station_set=tuple(sorted(stations)),
        db_import=db_import,
        manifest=cast("dict[str, object]", manifest),
        constants=cast("dict[str, object]", constants),
        house_station=house,
        td_k=td_k,
    )


def check_note_matches_package(
    note: FreezeNote, *, manifest: Mapping[str, object]
) -> None:
    """Refuse when the package, the Python version or a constant differs."""
    if note.manifest.get("python") != manifest.get("python"):
        raise RefusedError("the Python version differs from the freeze note's")
    if note.manifest.get("files") != manifest.get("files"):
        raise RefusedError("the package manifest differs from the freeze note's")
    if dict(note.constants) != frozen_constants():
        raise RefusedError("a constant differs from the freeze note's")


# --- The site, the record and the replay (§7.3) --------------------------------


@dataclass(frozen=True)
class Site:
    """The trial's site and its published generation."""

    site_id: int
    name: str
    enabled: bool
    elevation_m: float
    rain_threshold_mm: float
    generation_id: int
    timezone: str


def load_site(conn: sqlite3.Connection, *, site_id: int) -> Site:
    """The site row and its published generation's time zone."""
    row = conn.execute(
        "SELECT name, enabled, elevation_m, rain_threshold_mm FROM sites WHERE id = ?",
        (site_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"site {site_id} does not exist in the copy")
    generation = published_generation_id(conn, site_id)
    if generation is None:
        raise LookupError(f"site {site_id} has no published time-zone generation")
    tz_row = conn.execute(
        "SELECT timezone FROM timezone_generations WHERE id = ?", (generation,)
    ).fetchone()
    if tz_row is None:
        raise LookupError(f"generation {generation} does not exist")
    return Site(
        site_id=site_id,
        name=str(row["name"]),
        enabled=bool(row["enabled"]),
        elevation_m=float(row["elevation_m"]),
        rain_threshold_mm=float(row["rain_threshold_mm"]),
        generation_id=generation,
        timezone=str(tz_row["timezone"]),
    )


def load_record_rows(
    conn: sqlite3.Connection, *, site: Site, variable: str
) -> list[RecordRow]:
    """Every record row of the published generation for one variable."""
    rows = conn.execute(
        """
        SELECT snapshot_local_date, target_local_date, display_lead, snapshot_utc,
               status, write_path, app_version, methodology_version,
               tz_generation_id, policy, candidates, daily_quantities
        FROM forecast_of_record
        WHERE site_id = ? AND tz_generation_id = ? AND variable = ?
        ORDER BY snapshot_local_date, display_lead
        """,
        (site.site_id, site.generation_id, variable),
    ).fetchall()
    return [
        RecordRow(
            snapshot_date=date.fromisoformat(str(r["snapshot_local_date"])),
            target_date=date.fromisoformat(str(r["target_local_date"])),
            lead=int(r["display_lead"]),
            snapshot_utc=str(r["snapshot_utc"]),
            status=str(r["status"]),
            write_path=None if r["write_path"] is None else str(r["write_path"]),
            app_version=str(r["app_version"]),
            methodology_version=int(r["methodology_version"]),
            tz_generation_id=int(r["tz_generation_id"]),
            policy=None if r["policy"] is None else str(r["policy"]),
            candidates=None if r["candidates"] is None else str(r["candidates"]),
            daily_quantities=(
                None if r["daily_quantities"] is None else str(r["daily_quantities"])
            ),
        )
        for r in rows
    ]


@dataclass(frozen=True)
class FeedDayReplay:
    """One feed's samples for one display day of one snapshot's replay."""

    feed_id: int
    ref: daily_method.ModelRef
    valid_ats: tuple[str, ...]
    hours: tuple[datetime, ...]
    values: tuple[float, ...]
    issued_at: tuple[str, ...]
    covers: bool
    high: float | None
    low: float | None
    total: float | None


@dataclass(frozen=True)
class SnapshotReplay:
    """Production's as-of load for one snapshot, grouped by display day."""

    snapshot_date: date
    at: datetime
    days: Mapping[int, Mapping[int, FeedDayReplay]]
    seconds: float

    def feeds(self, lead: int) -> Mapping[int, FeedDayReplay]:
        """The feeds of display day ``lead`` (empty when none)."""
        return self.days.get(lead, {})

    def by_model(self, lead: int) -> dict[str, FeedDayReplay]:
        """Display day ``lead``'s feeds keyed by model; the first feed wins."""
        out: dict[str, FeedDayReplay] = {}
        for feed in self.feeds(lead).values():
            out.setdefault(feed.ref.key, feed)
        return out


class ReplayCache:
    """One as-of load per (snapshot date, decision time), kept for the run."""

    def __init__(self, conn: sqlite3.Connection, *, site: Site, variable: str) -> None:
        self._conn = conn
        self._site = site
        self._variable = variable
        self._cache: dict[tuple[date, str], SnapshotReplay] = {}

    def get(self, snapshot_date: date, snapshot_utc: str) -> SnapshotReplay:
        """The replay of ``snapshot_date`` as of ``snapshot_utc``."""
        key = (snapshot_date, snapshot_utc)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        replay = self._load(snapshot_date, snapshot_utc)
        self._cache[key] = replay
        return replay

    def seconds(self) -> list[float]:
        """Every replay's load time, in seconds by ``time.monotonic()``."""
        return [replay.seconds for replay in self._cache.values()]

    def _load(self, snapshot_date: date, snapshot_utc: str) -> SnapshotReplay:
        started = time.monotonic()
        tz = self._site.timezone
        at = parse_utc(snapshot_utc)
        since = isoformat_utc(local_day_bounds(snapshot_date, tz).start_utc)
        samples = load_future_samples(
            self._conn,
            site_id=self._site.site_id,
            since_valid_at=since,
            as_of=isoformat_utc(at),
        )
        grouped: dict[int, dict[int, list[tuple[str, float, str, str, str]]]] = {}
        for sample in samples:
            if sample.variable != self._variable:
                continue
            day = display_day_index(sample.valid_at, timezone=tz, now=at)
            if not 0 <= day < len(RECORD_LEADS):
                continue
            grouped.setdefault(day, {}).setdefault(sample.feed_id, []).append(
                (
                    sample.valid_at,
                    sample.value,
                    sample.issued_at,
                    sample.source,
                    sample.model,
                )
            )
        base = at.astimezone(ZoneInfo(tz)).date()
        days: dict[int, dict[int, FeedDayReplay]] = {}
        for day, per_feed in grouped.items():
            target = base + timedelta(days=day)
            out: dict[int, FeedDayReplay] = {}
            for feed_id, rows in per_feed.items():
                valid_ats = tuple(r[0] for r in rows)
                values = tuple(r[1] for r in rows)
                if self._variable == "precip":
                    covers = covers_local_day_exactly(
                        valid_ats, local_date=target, timezone=tz
                    )
                else:
                    covers = covers_local_day(valid_ats, local_date=target, timezone=tz)
                out[feed_id] = FeedDayReplay(
                    feed_id=feed_id,
                    ref=daily_method.ModelRef(source=rows[0][3], model=rows[0][4]),
                    valid_ats=valid_ats,
                    hours=tuple(parse_utc(v) for v in valid_ats),
                    values=values,
                    issued_at=tuple(sorted({r[2] for r in rows})),
                    covers=covers,
                    high=max(values) if values else None,
                    low=min(values) if values else None,
                    total=math.fsum(values) if values else None,
                )
            days[day] = out
        return SnapshotReplay(
            snapshot_date=snapshot_date,
            at=at,
            days=days,
            seconds=time.monotonic() - started,
        )


def f1_ok(row: RecordRow, control: ControlValue, replay: SnapshotReplay) -> bool:
    """F1: ``displayed_daily`` over the row's feeds equals the stored values.

    The feeds are ``displayed.extrema_feed_ids``, or for rows written before
    that field existed the ``candidates`` entries with participation
    ``contributed``. A listed feed missing from the replay is a mismatch.
    """
    ids = control.displayed.extrema_feed_ids
    if ids is None:
        entries = narrow_candidates(row.candidates)
        if isinstance(entries, Unmatched):
            return False
        ids = tuple(e.feed_id for e in entries if e.participation == "contributed")
    feeds = replay.feeds(row.lead)
    if any(feed_id not in feeds for feed_id in ids):
        return False
    rebuilt = displayed_daily(
        control.variable,
        [list(feeds[feed_id].values) for feed_id in ids],
        rain_threshold_mm=control.policy.rain_threshold_mm,
    )
    for key, stored in control.displayed.values.items():
        value = rebuilt.get(key)
        if stored is None or value is None:
            if stored is not value:
                return False
            continue
        if abs(value - stored) > F1_TOLERANCE:
            return False
    return True


# --- Station readings (§6.5, §6.6) ---------------------------------------------


@dataclass(frozen=True)
class StationRow:
    """One ``ok`` reading of an enabled station of the site."""

    pws_station_id: str
    station_id: int
    dem_elevation_m: float
    valid_at: str
    local_date: date
    value: float
    fetched_at: datetime | None


def load_station_rows(
    conn: sqlite3.Connection, *, site: Site, variable: str
) -> list[StationRow]:
    """Every ``ok`` reading of the site's enabled stations, read once per run.

    The ``fetched_at`` cutoff of Arm A is applied in Python (§13.3).
    """
    tz = ZoneInfo(site.timezone)
    rows = conn.execute(
        """
        SELECT s.pws_station_id, so.station_id, s.dem_elevation_m, so.valid_at,
               so.value, so.fetched_at
        FROM station_observations so JOIN stations s ON s.id = so.station_id
        WHERE s.site_id = ? AND s.enabled = 1 AND so.variable = ?
          AND so.qc_flag = 'ok'
        """,
        (site.site_id, variable),
    ).fetchall()
    out: list[StationRow] = []
    for r in rows:
        valid_at = str(r["valid_at"])
        fetched = r["fetched_at"]
        out.append(
            StationRow(
                pws_station_id=str(r["pws_station_id"]),
                station_id=int(r["station_id"]),
                dem_elevation_m=float(r["dem_elevation_m"]),
                valid_at=valid_at,
                local_date=parse_utc(valid_at).astimezone(tz).date(),
                value=float(r["value"]),
                fetched_at=None if fetched is None else parse_utc(str(fetched)),
            )
        )
    return out


def load_station_rain(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    first_day: date | None,
    last_day: date | None,
    known_by: datetime | None,
) -> dict[str, dict[date, list[daily_method.RainReading]]]:
    """The site's rain readings by station and local day (§6.6).

    Reads (hour, value, ``qc_flag``, ``source_raw``, ``fetched_at``) of the
    site's enabled stations' ``precip`` rows and keeps the ``ok`` ones. The
    local day uses the published generation's time zone. ``first_day`` and
    ``last_day`` bound the local days kept (None: unbounded). ``known_by``
    keeps only rows fetched by then, and a NULL ``fetched_at`` is never
    known; None keeps every row. Each day's readings are sorted by hour.
    """
    generation = published_generation_id(conn, site_id)
    if generation is None:
        raise LookupError(f"site {site_id} has no published time-zone generation")
    tz_row = conn.execute(
        "SELECT timezone FROM timezone_generations WHERE id = ?", (generation,)
    ).fetchone()
    if tz_row is None:
        raise LookupError(f"generation {generation} does not exist")
    tz = ZoneInfo(str(tz_row["timezone"]))
    rows = conn.execute(
        """
        SELECT s.pws_station_id, so.valid_at, so.value, so.qc_flag, so.source_raw,
               so.fetched_at
        FROM station_observations so JOIN stations s ON s.id = so.station_id
        WHERE s.site_id = ? AND s.enabled = 1 AND so.variable = 'precip'
        """,
        (site_id,),
    ).fetchall()
    out: dict[str, dict[date, list[daily_method.RainReading]]] = {}
    for r in rows:
        if str(r["qc_flag"]) != "ok":
            continue
        fetched_raw = r["fetched_at"]
        fetched = None if fetched_raw is None else parse_utc(str(fetched_raw))
        if known_by is not None and (fetched is None or fetched > known_by):
            continue
        hour = parse_utc(str(r["valid_at"]))
        day = hour.astimezone(tz).date()
        if first_day is not None and day < first_day:
            continue
        if last_day is not None and day > last_day:
            continue
        source_raw = r["source_raw"]
        out.setdefault(str(r["pws_station_id"]), {}).setdefault(day, []).append(
            daily_method.RainReading(
                hour=hour,
                value=float(r["value"]),
                source_raw=None if source_raw is None else str(source_raw),
                fetched_at=fetched,
            )
        )
    for days in out.values():
        for readings in days.values():
            readings.sort(key=lambda reading: reading.hour)
    return out


# --- Truth: the published rows, Arm A and the clean gate (§6.5) ---------------


@dataclass(frozen=True)
class PublishedTruth:
    """One published ``daily_truth`` row."""

    value: float | None
    eligible: bool
    stale: bool
    rain_threshold_mm: float | None


def load_published_truth(
    conn: sqlite3.Connection, *, site: Site, quantities: Sequence[str]
) -> dict[tuple[str, date], PublishedTruth]:
    """The published generation's truth rows for the given quantities."""
    marks = ",".join("?" for _ in quantities)
    rows = conn.execute(
        f"""
        SELECT quantity, local_date, value, eligible, stale, rain_threshold_mm
        FROM daily_truth
        WHERE site_id = ? AND tz_generation_id = ? AND quantity IN ({marks})
        """,
        (site.site_id, site.generation_id, *quantities),
    ).fetchall()
    return {
        (str(r["quantity"]), date.fromisoformat(str(r["local_date"]))): PublishedTruth(
            value=None if r["value"] is None else float(r["value"]),
            eligible=bool(r["eligible"]),
            stale=bool(r["stale"]),
            rain_threshold_mm=(
                None
                if r["rain_threshold_mm"] is None
                else float(r["rain_threshold_mm"])
            ),
        )
        for r in rows
    }


class ArmARebuilder:
    """Arm A: a day's truth rebuilt from the readings fetched by T (§6.5).

    Each day's readings are kept sorted by ``fetched_at`` (NULL last), so the
    readings known by T are a prefix; a rebuild is cached per (day, prefix
    length, threshold). ``known_by=None`` means no cutoff, over the readings
    with a ``fetched_at`` (a NULL ``fetched_at`` is never known).
    """

    def __init__(
        self, rows: Sequence[StationRow], *, site: Site, variable: str
    ) -> None:
        self._site = site
        self._variable = variable
        by_day: dict[date, list[StationRow]] = {}
        for row in rows:
            by_day.setdefault(row.local_date, []).append(row)
        self._rows: dict[date, list[StationRow]] = {}
        self._fetched: dict[date, list[datetime]] = {}
        for day, day_rows in by_day.items():
            known = sorted(
                (r for r in day_rows if r.fetched_at is not None),
                key=lambda r: cast("datetime", r.fetched_at),
            )
            unknown = [r for r in day_rows if r.fetched_at is None]
            self._rows[day] = known + unknown
            self._fetched[day] = [cast("datetime", r.fetched_at) for r in known]
        self._cache: dict[tuple[date, int, float], dict[str, QuantityOutcome]] = {}

    def readings(self, days: Iterable[date]) -> int:
        """The readings read on the given days, NULL ``fetched_at`` included (G-a)."""
        return sum(len(self._rows.get(day, ())) for day in days)

    def null_fetched(self, days: Iterable[date]) -> int:
        """Readings with a NULL ``fetched_at`` on the given days (G-a)."""
        return sum(
            len(self._rows.get(day, ())) - len(self._fetched.get(day, ()))
            for day in days
        )

    def rebuild(
        self, day: date, *, known_by: datetime | None, threshold: float
    ) -> dict[str, QuantityOutcome]:
        """The day's quantities from the readings fetched by ``known_by``."""
        fetched = self._fetched.get(day, [])
        if known_by is None:
            count = len(fetched)
        else:
            count = 0
            for at in fetched:
                if at > known_by:
                    break
                count += 1
        key = (day, count, threshold)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        by_hour: dict[str, list[StationReading]] = {}
        for row in self._rows.get(day, [])[:count]:
            by_hour.setdefault(row.valid_at, []).append(
                StationReading(
                    station_id=row.station_id,
                    dem_elevation_m=row.dem_elevation_m,
                    variable=self._variable,
                    valid_at=row.valid_at,
                    value=row.value,
                )
            )
        samples: list[tuple[str, float]] = []
        for valid_at in sorted(by_hour):
            result = compute_consensus(
                by_hour[valid_at],
                variable=self._variable,
                site_elevation_m=self._site.elevation_m,
                mad_floor=MAD_FLOORS[self._variable],
            )
            if result is not None:
                samples.append((valid_at, result.value))
        outcomes = {
            outcome.quantity: outcome
            for outcome in evaluate_variable(
                self._variable,
                samples,
                timezone=self._site.timezone,
                local_date=day,
                rain_threshold_mm=threshold,
            )
        }
        self._cache[key] = outcomes
        return outcomes


QUANTITY_TOLERANCES: Mapping[str, float] = {
    "temperature_high": GB_TEMPERATURE_TOLERANCE_C,
    "temperature_low": GB_TEMPERATURE_TOLERANCE_C,
    "precip_total": GB_PRECIP_TOLERANCE_MM,
    "precip_occurrence": 0.0,
}


def _same_value(a: float | None, b: float | None, tolerance: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tolerance + 1e-12


@dataclass(frozen=True)
class GateReport:
    """G-a, G-b and the arm they select (§6.5)."""

    in_scope_days: int
    ga_readings: int
    ga_null_fetched: int
    ga_pass: bool
    gb_matched: int
    gb_denominator: int
    gb_stale_days: int
    gb_no_truth_row_days: int
    gb_mismatched_days: tuple[date, ...]
    gb_pass: bool
    arm: Literal["A", "B"]
    forced: bool


def compute_gates(
    rebuilder: ArmARebuilder,
    published: Mapping[tuple[str, date], PublishedTruth],
    *,
    in_scope_days: Iterable[date],
    quantities: Sequence[str],
    site: Site,
    force_arm: Literal["A", "B"] | None,
) -> GateReport:
    """G-a and G-b over the in-scope days, and the arm they select.

    G-a passes when the in-scope days hold at least one reading and none
    has a NULL ``fetched_at``; zero readings (0/0) fails it. G-b's
    denominator is the in-scope days with a non-stale truth row for every
    quantity; a day with no row is reported apart, and a stale day is
    reported next to the denominator. 0/0 fails. ``force_arm`` (a testing
    and sensitivity hook) overrides the selection and is recorded.
    """
    days = sorted(set(in_scope_days))
    readings = rebuilder.readings(days)
    null_fetched = rebuilder.null_fetched(days)
    matched = denominator = stale = no_row = 0
    mismatched: list[date] = []
    for day in days:
        rows = [published.get((q, day)) for q in quantities]
        if any(r is None for r in rows):
            no_row += 1
            continue
        present = [r for r in rows if r is not None]
        if any(r.stale for r in present):
            stale += 1
            continue
        denominator += 1
        thresholds = [
            r.rain_threshold_mm for r in present if r.rain_threshold_mm is not None
        ]
        threshold = thresholds[0] if thresholds else site.rain_threshold_mm
        rebuilt = rebuilder.rebuild(day, known_by=None, threshold=threshold)
        same = True
        for quantity, stored in zip(quantities, present, strict=True):
            mine = rebuilt.get(quantity)
            if (
                mine is None
                or mine.eligible != stored.eligible
                or not _same_value(
                    mine.value, stored.value, QUANTITY_TOLERANCES[quantity]
                )
            ):
                same = False
        if same:
            matched += 1
        else:
            mismatched.append(day)
    ga_pass = readings > 0 and null_fetched == 0
    gb_pass = denominator > 0 and 100 * matched >= GB_MIN_PERCENT * denominator
    arm: Literal["A", "B"] = "A" if ga_pass and gb_pass else "B"
    if force_arm is not None:
        arm = force_arm
    return GateReport(
        in_scope_days=len(days),
        ga_readings=readings,
        ga_null_fetched=null_fetched,
        ga_pass=ga_pass,
        gb_matched=matched,
        gb_denominator=denominator,
        gb_stale_days=stale,
        gb_no_truth_row_days=no_row,
        gb_mismatched_days=tuple(mismatched),
        gb_pass=gb_pass,
        arm=arm,
        forced=force_arm is not None,
    )


def stale_truth_rows(
    published: Mapping[tuple[str, date], PublishedTruth],
    *,
    quantities: Sequence[str],
    date_sets: Mapping[str, Iterable[date]],
) -> dict[str, dict[str, int]]:
    """Per quantity, the stale published rows on each named set of dates."""
    sets = {name: set(days) for name, days in date_sets.items()}
    out: dict[str, dict[str, int]] = {}
    for quantity in quantities:
        counts = dict.fromkeys(sets, 0)
        for (q, day), row in published.items():
            if q != quantity or not row.stale:
                continue
            for name, days in sets.items():
                if day in days:
                    counts[name] += 1
        out[quantity] = counts
    return out


def temperature_window(k_day: date, arm: Literal["A", "B"]) -> tuple[date, date]:
    """A temperature case's training window: K-29..K, or K-30..K-1 (Arm B)."""
    if arm == "A":
        return k_day - timedelta(days=TEMP_WINDOW_DAYS - 1), k_day
    return k_day - timedelta(days=TEMP_WINDOW_DAYS), k_day - timedelta(days=1)


def _days(first: date, last: date) -> list[date]:
    out: list[date] = []
    day = first
    while day <= last:
        out.append(day)
        day += timedelta(days=1)
    return out


def training_truth(
    *,
    arm: Literal["A", "B"],
    quantity: str,
    window: tuple[date, date],
    rebuilder: ArmARebuilder,
    published: Mapping[tuple[str, date], PublishedTruth],
    known_by: datetime,
    threshold: float,
) -> tuple[dict[date, float], int]:
    """The window's eligible truth for one quantity, and its stale days.

    Arm A rebuilds each day from the readings fetched by ``known_by``. Arm B
    reads the published rows and skips (and counts) the stale ones.
    """
    out: dict[date, float] = {}
    stale = 0
    for day in _days(*window):
        if arm == "A":
            outcome = rebuilder.rebuild(
                day, known_by=known_by, threshold=threshold
            ).get(quantity)
            if outcome is not None and outcome.eligible and outcome.value is not None:
                out[day] = outcome.value
            continue
        row = published.get((quantity, day))
        if row is None:
            continue
        if row.stale:
            stale += 1
            continue
        if row.eligible and row.value is not None:
            out[day] = row.value
    return out, stale


# --- Cases (§7.1-§7.3) ------------------------------------------------------------


@dataclass(frozen=True)
class CaseKey:
    """One (s, L) and its target t = s + L."""

    snapshot_date: date
    lead: int
    target: date


def select_cases(
    rows: Mapping[tuple[date, int], RecordRow],
    *,
    first: date,
    last: date,
    excluded_h_dates: Sequence[date],
) -> tuple[list[tuple[CaseKey, RecordRow | None]], int]:
    """Every (s, L) whose target is in [first, last], and the excluded count.

    Snapshot dates are the dates with at least one record row; a lead with
    no row on such a date is returned with None (``no_row``). A target in
    ``excluded_h_dates`` is not a case; the count is of distinct target
    dates removed.
    """
    excluded = set(excluded_h_dates)
    removed: set[date] = set()
    snapshot_dates = sorted({s for s, _ in rows})
    out: list[tuple[CaseKey, RecordRow | None]] = []
    for snapshot in snapshot_dates:
        for lead in RECORD_LEADS:
            target = snapshot + timedelta(days=lead)
            if not first <= target <= last:
                continue
            if target in excluded:
                removed.add(target)
                continue
            out.append((CaseKey(snapshot, lead, target), rows.get((snapshot, lead))))
    return out, len(removed)


def recorded_map(rows: Iterable[RecordRow]) -> dict[tuple[date, date], datetime]:
    """(snapshot date, target date) -> T for every ``recorded`` row."""
    return {
        (row.snapshot_date, row.target_date): parse_utc(row.snapshot_utc)
        for row in rows
        if row.status == "recorded"
    }


def snapshot_utc_text(rows: Iterable[RecordRow]) -> dict[date, str]:
    """Each snapshot date's ``snapshot_utc`` (the first row's, by lead)."""
    out: dict[date, str] = {}
    for row in sorted(rows, key=lambda r: (r.snapshot_date, r.lead)):
        out.setdefault(row.snapshot_date, row.snapshot_utc)
    return out


class Tally:
    """Counts by (reason, lead), per quantity or decision."""

    def __init__(self) -> None:
        self.counts: dict[str, dict[str, dict[int, int]]] = {}

    def add(self, group: str, reason: str, lead: int) -> None:
        """Count one."""
        per_reason = self.counts.setdefault(group, {}).setdefault(reason, {})
        per_reason[lead] = per_reason.get(lead, 0) + 1

    def as_json(self) -> dict[str, dict[str, dict[str, int]]]:
        """Nested plain counts, leads as text keys."""
        return {
            group: {
                reason: {str(lead): n for lead, n in sorted(per_lead.items())}
                for reason, per_lead in sorted(reasons.items())
            }
            for group, reasons in sorted(self.counts.items())
        }


# --- Resampling and the bounds (§11.2), the block-length check (§10.3) --------


@dataclass(frozen=True)
class Bounds:
    """A statistic's point value and its percentile interval (§11.2)."""

    point: float | None
    lower: float | None
    upper: float | None
    kept: int
    discarded: int
    level: float


def draw_resamples(n: int, *, block_length: int = BLOCK_LENGTH) -> list[list[int]]:
    """The run's index lists, drawn once per variable over ``n`` dates.

    ``block_length`` other than BLOCK_LENGTH is the block-length sensitivity
    (§11.2), reported only.
    """
    rng = random.Random(SEED)
    return [moving_block_indices(rng, n, block_length) for _ in range(RESAMPLES)]


def daily_bounds(
    deltas: Mapping[tuple[str, date, int], float],
    index_lists: Sequence[Sequence[int]],
    *,
    dates: Sequence[date],
    quantity_weights: Mapping[str, float],
    lead_weights: Mapping[int, float],
    level: float,
) -> Bounds:
    """The bounds of sum_q sum_L w_q * W_L * mean delta over resampled cases.

    ``deltas`` maps (quantity, target date, lead) to the case's delta;
    ``index_lists`` index into ``dates`` (the sorted distinct target dates
    the lists were drawn over). A resample that leaves any (quantity, lead)
    of the weights without a case is discarded and counted. The point value
    is the same statistic over the full sample, None when a group is empty.
    """
    position = {day: i for i, day in enumerate(dates)}
    groups = [(q, lead) for q in quantity_weights for lead in lead_weights]
    group_index = {g: i for i, g in enumerate(groups)}
    per_group: list[dict[int, float]] = [{} for _ in groups]
    for (quantity, day, lead), value in deltas.items():
        g = group_index.get((quantity, lead))
        if g is None:
            continue
        i = position.get(day)
        if i is None:
            raise ValueError(f"delta for {day.isoformat()} is outside the drawn dates")
        per_group[g][i] = value
    weights = [quantity_weights[q] * lead_weights[lead] for q, lead in groups]
    point: float | None = None
    if all(per_group):
        point = math.fsum(
            w * math.fsum(d.values()) / len(d)
            for w, d in zip(weights, per_group, strict=True)
        )
    stats: list[float] = []
    discarded = 0
    for indices in index_lists:
        counts: dict[int, int] = {}
        for i in indices:
            counts[i] = counts.get(i, 0) + 1
        total = 0.0
        complete = True
        for w, d in zip(weights, per_group, strict=True):
            acc = 0.0
            n = 0
            for i, m in counts.items():
                value = d.get(i)
                if value is not None:
                    acc += m * value
                    n += m
            if n == 0:
                complete = False
                break
            total += w * acc / n
        if complete:
            stats.append(total)
        else:
            discarded += 1
    lower: float | None = None
    upper: float | None = None
    if stats:
        lower, upper = percentile_ci(stats, level)
    return Bounds(point, lower, upper, len(stats), discarded, level)


def discard_ok(bounds: Bounds) -> bool:
    """At most 1% of resamples discarded, in integers."""
    total = bounds.kept + bounds.discarded
    return 100 * bounds.discarded <= MAX_DISCARD_SHARE_PERCENT * total


def lead_weighted(per_lead: Mapping[int, Sequence[float]]) -> float | None:
    """sum_L W_L * mean over leads 0-5, W_L renormalised over leads present."""
    present = [lead for lead in SCORED_LEADS if per_lead.get(lead)]
    if not present:
        return None
    weight = math.fsum(LEAD_WEIGHTS[lead] for lead in present)
    return math.fsum(
        LEAD_WEIGHTS[lead] / weight * math.fsum(per_lead[lead]) / len(per_lead[lead])
        for lead in present
    )


def block_length_check(series: Mapping[date, float]) -> dict[str, object]:
    """A9: the series' length by the lag-correlation rule (§10.3).

    r_j is the Pearson correlation over pairs exactly j days apart. A lag
    whose correlation is not defined (fewer than two pairs, or a constant
    side) is reported as null and treated as unrelated.
    """
    lags: list[dict[str, object]] = []
    related: dict[int, bool] = {}
    for j in range(1, MAX_BLOCK_LAG + 1):
        xs: list[float] = []
        ys: list[float] = []
        for day, value in series.items():
            later = series.get(day + timedelta(days=j))
            if later is not None:
                xs.append(value)
                ys.append(later)
        r: float | None = None
        if len(xs) >= 2:
            try:
                r = statistics.correlation(xs, ys)
            except statistics.StatisticsError:
                r = None
        related[j] = r is not None and abs(r) >= 2 / math.sqrt(len(xs))
        lags.append({"lag": j, "r": r, "n": len(xs)})
    length = MAX_BLOCK_LAG + 1
    for k in range(1, MAX_BLOCK_LAG + 1):
        if not any(related[j] for j in range(k, MAX_BLOCK_LAG + 1)):
            length = k
            break
    return {"dates": len(series), "lags": lags, "length": length, "b": BLOCK_LENGTH}


# --- Temperature cases (§7, §8) ---------------------------------------------------


@dataclass(frozen=True)
class TempContext:
    """Everything one temperature run reads, loaded once."""

    site: Site
    rows: Mapping[tuple[date, int], RecordRow]
    recorded: Mapping[tuple[date, date], datetime]
    recorded_text: Mapping[tuple[date, date], str]
    replays: ReplayCache
    rebuilder: ArmARebuilder
    published: Mapping[tuple[str, date], PublishedTruth]
    arm: Literal["A", "B"]


@dataclass(frozen=True)
class TempParams:
    """The loss and candidate settings of one temperature pass (§8.1, §8.2).

    The defaults are the owner's choices (D1, D2 option A); other values are
    the sensitivities of §8.6, reported only.
    """

    w_high: float = W_HIGH
    low_under: float = 1.0
    low_over: float = 1.0
    low_tie: daily_method.TieBreak = "source_model"
    shrink: float = BC_SHRINK

    def weights(self, quantity: str) -> tuple[float, float]:
        """(under_weight, over_weight) of the loss for ``quantity``."""
        if quantity == "temperature_high":
            return self.w_high, 1.0
        return self.low_under, self.low_over

    def tie(self, quantity: str) -> daily_method.TieBreak:
        """TD-K's tie-break for ``quantity``."""
        return "under_forecast" if quantity == "temperature_high" else self.low_tie


OWNER_PARAMS = TempParams()


@dataclass(frozen=True)
class TempLine:
    """One (s, L, quantity): its control, truth, candidates and benchmarks."""

    key: CaseKey
    quantity: str
    row: RecordRow | None
    unmatched: str | None
    control: float | None = None
    truth: float | None = None
    f1: bool | None = None
    window: tuple[date, date] | None = None
    training_days: int | None = None
    regime: str | None = None
    warm_spell: bool | None = None
    frost: bool | None = None
    segment: tuple[object, ...] | None = None
    feeds: Mapping[str, float] = field(default_factory=lambda: {})
    candidates: Mapping[str, daily_method.CandidateValue] = field(
        default_factory=lambda: {}
    )
    benchmarks: Mapping[str, daily_method.BenchmarkValue] = field(
        default_factory=lambda: {}
    )

    @property
    def matched(self) -> bool:
        """A case: matched control, eligible truth and F1 passed."""
        return self.unmatched is None

    @property
    def scored(self) -> bool:
        """A matched case at a scored lead (0-5)."""
        return self.unmatched is None and self.key.lead in SCORED_LEADS

    def value(self, name: str) -> float | None:
        """A candidate's, a benchmark's or the control's value."""
        if name == "control":
            return self.control
        candidate = self.candidates.get(name)
        if candidate is not None:
            return candidate.value
        benchmark = self.benchmarks.get(name)
        return None if benchmark is None else benchmark.value


def temperature_loss(
    quantity: str, forecast: float, observed: float, params: TempParams = OWNER_PARAMS
) -> float:
    """§8.1: the high with w_high (1), the low symmetric (D2 option A)."""
    under, over = params.weights(quantity)
    return daily_method.asymmetric_loss(
        forecast, observed, under_weight=under, over_weight=over
    )


def _feed_value(feed: FeedDayReplay, quantity: str) -> float | None:
    if not feed.covers:
        return None
    return feed.high if quantity == "temperature_high" else feed.low


def _first_by_model(
    feeds: Mapping[int, FeedDayReplay],
) -> dict[daily_method.ModelRef, FeedDayReplay]:
    out: dict[daily_method.ModelRef, FeedDayReplay] = {}
    for feed in feeds.values():
        out.setdefault(feed.ref, feed)
    return out


TruthWindow = tuple[tuple[date, date], dict[date, float], int]
ModelPairs = dict[daily_method.ModelRef, list[tuple[float, float]]]


class TempTraining:
    """Per (s, T, quantity): the window's truth and each lead's pairs."""

    def __init__(self, ctx: TempContext) -> None:
        self._ctx = ctx
        self._truth: dict[tuple[date, str, str], TruthWindow] = {}
        self._pairs: dict[tuple[date, str, str, int], ModelPairs] = {}
        self._days: dict[tuple[date, str, str, int], int] = {}

    def truth(self, snapshot: date, snapshot_utc: str, quantity: str) -> TruthWindow:
        """The case window, its eligible truth by day, and its stale days."""
        key = (snapshot, snapshot_utc, quantity)
        cached = self._truth.get(key)
        if cached is not None:
            return cached
        ctx = self._ctx
        at = parse_utc(snapshot_utc)
        k_day = latest_knowable_target(ctx.site.timezone, at)
        window = temperature_window(k_day, ctx.arm)
        values, stale = training_truth(
            arm=ctx.arm,
            quantity=quantity,
            window=window,
            rebuilder=ctx.rebuilder,
            published=ctx.published,
            known_by=at,
            threshold=ctx.site.rain_threshold_mm,
        )
        result = (window, values, stale)
        self._truth[key] = result
        return result

    def pairs(
        self, snapshot: date, snapshot_utc: str, quantity: str, lead: int
    ) -> ModelPairs:
        """Each model's (forecast, observed) pairs at ``lead`` in the window."""
        key = (snapshot, snapshot_utc, quantity, lead)
        cached = self._pairs.get(key)
        if cached is not None:
            return cached
        ctx = self._ctx
        window, truth, _ = self.truth(snapshot, snapshot_utc, quantity)
        out: ModelPairs = {}
        days = daily_method.training_days(
            ctx.recorded, first=window[0], last=window[1], lead=lead
        )
        counted = 0
        for day, _at in days:
            observed = truth.get(day)
            if observed is None:
                continue
            counted += 1
            earlier = day - timedelta(days=lead)
            replay = ctx.replays.get(earlier, ctx.recorded_text[(earlier, day)])
            for ref, feed in _first_by_model(replay.feeds(lead)).items():
                value = _feed_value(feed, quantity)
                if value is not None:
                    out.setdefault(ref, []).append((value, observed))
        self._pairs[key] = out
        self._days[key] = counted
        return out

    def counted_days(
        self, snapshot: date, snapshot_utc: str, quantity: str, lead: int
    ) -> int:
        """Training days at ``lead`` with a recorded row and eligible truth."""
        self.pairs(snapshot, snapshot_utc, quantity, lead)
        return self._days[(snapshot, snapshot_utc, quantity, lead)]


def segment_key(row: RecordRow, control: ControlValue) -> tuple[object, ...]:
    """§11.4's split key: extrema ids present, policy keys, methodology, tz."""
    return (
        control.displayed.extrema_feed_ids is not None,
        control.policy.segment,
        row.methodology_version,
        row.tz_generation_id,
    )


def warm_spell_threshold(window_highs: Sequence[float]) -> float | None:
    """The 67th percentile of the window's observed highs (needs two)."""
    if len(window_highs) < 2:
        return None
    return statistics.quantiles(window_highs, n=3, method="inclusive")[1]


@dataclass
class F1Tally:
    """F1's rows and matches, over all rows and over ``on_time`` rows."""

    rows: int = 0
    matched: int = 0
    on_time_rows: int = 0
    on_time_matched: int = 0
    failed: list[tuple[date, int]] = field(default_factory=lambda: [])

    def add(self, row: RecordRow, ok: bool) -> None:
        """Count one row."""
        self.rows += 1
        self.matched += ok
        if row.write_path == "on_time":
            self.on_time_rows += 1
            self.on_time_matched += ok
        if not ok:
            self.failed.append((row.snapshot_date, row.lead))

    def passes(self) -> bool:
        """100 * matched >= 95 * rows, for all rows and for on_time rows."""
        return (
            100 * self.matched >= F1_MIN_PERCENT * self.rows
            and 100 * self.on_time_matched >= F1_MIN_PERCENT * self.on_time_rows
        )

    def as_json(self) -> dict[str, object]:
        """The tally, with the failing (s, L) listed."""
        return {
            "rows": self.rows,
            "matched": self.matched,
            "on_time_rows": self.on_time_rows,
            "on_time_matched": self.on_time_matched,
            "passes": self.passes(),
            "failed": [[s.isoformat(), lead] for s, lead in self.failed],
        }


def temperature_candidates(
    feeds: Sequence[daily_method.FeedCase],
    *,
    quantity: str,
    control: float,
    lead: int,
    td_k: int,
    params: TempParams = OWNER_PARAMS,
) -> dict[str, daily_method.CandidateValue]:
    """The four candidates of §8.2 for one case."""
    under, over = params.weights(quantity)
    tie = params.tie(quantity)
    return {
        "IW": daily_method.compose_iw(
            feeds,
            control=control,
            lead=lead,
            min_days=TEMP_MIN_DAYS,
            pooled_leads=SCORED_LEADS,
            floor=IW_FLOOR_C,
        ),
        "TD-K": daily_method.compose_td_k(
            feeds,
            control=control,
            lead=lead,
            min_days=TEMP_MIN_DAYS,
            pooled_leads=SCORED_LEADS,
            k=td_k,
            under_weight=under,
            over_weight=over,
            tie_break=tie,
        ),
        "IW-BC": daily_method.compose_iw_bc(
            feeds,
            control=control,
            lead=lead,
            min_days=TEMP_MIN_DAYS,
            pooled_leads=SCORED_LEADS,
            floor=IW_FLOOR_C,
            shrink=params.shrink,
            limit=BC_LIMIT_C,
        ),
        "TD-K-BC": daily_method.compose_td_k_bc(
            feeds,
            control=control,
            lead=lead,
            min_days=TEMP_MIN_DAYS,
            pooled_leads=SCORED_LEADS,
            k=td_k,
            under_weight=under,
            over_weight=over,
            tie_break=tie,
            shrink=params.shrink,
            limit=BC_LIMIT_C,
        ),
    }


def _truth_reason(published: PublishedTruth | None) -> str | None:
    if published is None:
        return "truth_absent"
    if published.stale:
        return "truth_stale"
    if not published.eligible or published.value is None:
        return "truth_not_eligible"
    return None


def temperature_lines(
    ctx: TempContext,
    training: TempTraining,
    cases: Sequence[tuple[CaseKey, RecordRow | None]],
    *,
    td_k: int | None,
    f1: F1Tally,
    params: TempParams = OWNER_PARAMS,
) -> list[TempLine]:
    """One line per (s, L, quantity); candidates only when ``td_k`` is given.

    Unmatched reasons, in order: ``no_row``, ``missed``,
    ``version_unparseable``, a column unreadable, ``control_null``,
    ``f1_mismatch``, then per quantity ``truth_absent``, ``truth_stale``
    and ``truth_not_eligible``.
    """
    out: list[TempLine] = []
    for key, row in cases:
        if row is None:
            out.extend(TempLine(key, q, None, "no_row") for q in TEMPERATURE_QUANTITIES)
            continue
        control = control_value(row, variable="temperature", rain_rules=RAIN_RULES)
        if isinstance(control, Unmatched):
            out.extend(
                TempLine(key, q, row, control.reason) for q in TEMPERATURE_QUANTITIES
            )
            continue
        replay = ctx.replays.get(row.snapshot_date, row.snapshot_utc)
        ok = f1_ok(row, control, replay)
        f1.add(row, ok)
        if not ok:
            out.extend(
                TempLine(key, q, row, "f1_mismatch", f1=False)
                for q in TEMPERATURE_QUANTITIES
            )
            continue
        segment = segment_key(row, control)
        by_model = _first_by_model(replay.feeds(key.lead))
        for quantity in TEMPERATURE_QUANTITIES:
            stored = control.high if quantity == "temperature_high" else control.low
            assert stored is not None
            published = ctx.published.get((quantity, key.target))
            reason = _truth_reason(published)
            if reason is not None or published is None or published.value is None:
                out.append(
                    TempLine(
                        key,
                        quantity,
                        row,
                        reason,
                        control=stored,
                        f1=True,
                        segment=segment,
                    )
                )
                continue
            observed = published.value
            window, window_truth, _ = training.truth(
                row.snapshot_date, row.snapshot_utc, quantity
            )
            counted = training.counted_days(
                row.snapshot_date, row.snapshot_utc, quantity, key.lead
            )
            warm: bool | None = None
            frost: bool | None = None
            if quantity == "temperature_high":
                threshold = warm_spell_threshold(list(window_truth.values()))
                warm = None if threshold is None else observed >= threshold
            else:
                frost = observed <= FROST_LINE_C
            values: dict[daily_method.ModelRef, float] = {}
            for ref, feed in by_model.items():
                value = _feed_value(feed, quantity)
                if value is not None:
                    values[ref] = value
            regime: str | None = None
            candidates: dict[str, daily_method.CandidateValue] = {}
            benchmarks: dict[str, daily_method.BenchmarkValue] = {}
            if td_k is not None:
                feeds = temperature_feed_cases(training, row, quantity, by_model)
                regime, _ = daily_method.training_regime(
                    feeds,
                    lead=key.lead,
                    min_days=TEMP_MIN_DAYS,
                    pooled_leads=SCORED_LEADS,
                )
                candidates = temperature_candidates(
                    feeds,
                    quantity=quantity,
                    control=stored,
                    lead=key.lead,
                    td_k=td_k,
                    params=params,
                )
                benchmarks = {
                    "EQ": daily_method.compose_eq(values),
                    "MB-mean": daily_method.compose_mb_mean(values),
                    "persistence": daily_method.compose_persistence(
                        window_truth, control=stored
                    ),
                }
            out.append(
                TempLine(
                    key,
                    quantity,
                    row,
                    None,
                    control=stored,
                    truth=observed,
                    f1=True,
                    window=window,
                    training_days=counted,
                    regime=regime,
                    warm_spell=warm,
                    frost=frost,
                    segment=segment,
                    feeds={ref.key: v for ref, v in sorted(values.items())},
                    candidates=candidates,
                    benchmarks=benchmarks,
                )
            )
    return out


# --- Temperature scoring and the verdict (§8.4-§8.6, §11.4) ---------------------

TILE_WEIGHTS: Mapping[str, float] = {q: 0.5 for q in TEMPERATURE_QUANTITIES}


def case_deltas(
    lines: Iterable[TempLine],
    name: str,
    against: str,
    *,
    params: TempParams = OWNER_PARAMS,
) -> tuple[dict[tuple[str, date, int], float], int]:
    """Delta = loss(name) - loss(against) per scored case, and the missing count.

    A case where either value is None is left out and counted.
    """
    out: dict[tuple[str, date, int], float] = {}
    missing = 0
    for line in lines:
        if not line.scored or line.truth is None:
            continue
        a = line.value(name)
        b = line.value(against)
        if a is None or b is None:
            missing += 1
            continue
        out[(line.quantity, line.key.target, line.key.lead)] = temperature_loss(
            line.quantity, a, line.truth, params
        ) - temperature_loss(line.quantity, b, line.truth, params)
    return out, missing


def _per_lead(
    lines: Iterable[TempLine], measure: Callable[[TempLine], float | None]
) -> dict[int, list[float]]:
    out: dict[int, list[float]] = {}
    for line in lines:
        value = measure(line)
        if value is not None:
            out.setdefault(line.key.lead, []).append(value)
    return out


def _loss_of(name: str, params: TempParams) -> Callable[[TempLine], float | None]:
    def measure(line: TempLine) -> float | None:
        value = line.value(name)
        if value is None or line.truth is None:
            return None
        return temperature_loss(line.quantity, value, line.truth, params)

    return measure


def _under_of(name: str) -> Callable[[TempLine], float | None]:
    def measure(line: TempLine) -> float | None:
        value = line.value(name)
        if value is None or line.truth is None:
            return None
        return max(line.truth - value, 0.0)

    return measure


def _over_of(name: str) -> Callable[[TempLine], float | None]:
    def measure(line: TempLine) -> float | None:
        value = line.value(name)
        if value is None or line.truth is None:
            return None
        return max(value - line.truth, 0.0)

    return measure


def _distinct_dates(lines: Iterable[TempLine], lead: int) -> int:
    return len({line.key.target for line in lines if line.key.lead == lead})


def guard_report(
    lines: Sequence[TempLine], name: str, *, params: TempParams = OWNER_PARAMS
) -> dict[str, object]:
    """G2, G3, G4 and the frost guard for one candidate against the control."""
    scored = [line for line in lines if line.scored and line.value(name) is not None]
    out: dict[str, object] = {}
    g2: dict[str, object] = {}
    g4: dict[str, object] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        mine = [line for line in scored if line.quantity == quantity]
        cand = lead_weighted(_per_lead(mine, _loss_of(name, params)))
        ctrl = lead_weighted(_per_lead(mine, _loss_of("control", params)))
        g2[quantity] = {
            "candidate": cand,
            "control": ctrl,
            "pass": cand is not None and ctrl is not None and cand <= G2_MARGIN * ctrl,
        }
        n = len(mine)
        warm_c = sum(
            1
            for line in mine
            if (v := line.value(name)) is not None
            and line.truth is not None
            and v - line.truth > G4_BAND_C
        )
        warm_k = sum(
            1
            for line in mine
            if line.control is not None
            and line.truth is not None
            and line.control - line.truth > G4_BAND_C
        )
        g4[quantity] = {
            "cases": n,
            "candidate_too_warm": warm_c,
            "control_too_warm": warm_k,
            "pass": 100 * warm_c <= 100 * warm_k + G4_MARGIN_PP * n,
        }
    out["G2"] = g2
    out["G4"] = g4
    warm = [
        line
        for line in scored
        if line.quantity == "temperature_high" and line.warm_spell
    ]
    g3_cand = lead_weighted(_per_lead(warm, _under_of(name)))
    g3_ctrl = lead_weighted(_per_lead(warm, _under_of("control")))
    out["G3"] = {
        "warm_spell_cases": len(warm),
        "candidate_under": g3_cand,
        "control_under": g3_ctrl,
        "pass": None if g3_cand is None or g3_ctrl is None else g3_cand <= g3_ctrl,
    }
    frost = [
        line for line in scored if line.quantity == "temperature_low" and line.frost
    ]
    lead0 = _distinct_dates(frost, 0)
    frost_cand = [m for line in frost if (m := _over_of(name)(line)) is not None]
    frost_ctrl = [m for line in frost if (m := _over_of("control")(line)) is not None]
    assessable = lead0 >= FROST_MIN_LEAD0_DATES and bool(frost_cand)
    out["frost"] = {
        "frost_cases": len(frost),
        "lead0_dates": lead0,
        "assessable": assessable,
        "candidate_too_warm": statistics.fmean(frost_cand) if frost_cand else None,
        "control_too_warm": statistics.fmean(frost_ctrl) if frost_ctrl else None,
        "pass": (
            None
            if not assessable
            else statistics.fmean(frost_cand) <= statistics.fmean(frost_ctrl)
        ),
    }
    return out


def guards_pass(guards: Mapping[str, object]) -> bool:
    """Every guard holds; G3 and frost count only when assessable."""
    g2 = cast("Mapping[str, Mapping[str, object]]", guards["G2"])
    g4 = cast("Mapping[str, Mapping[str, object]]", guards["G4"])
    g3 = cast("Mapping[str, object]", guards["G3"])
    frost = cast("Mapping[str, object]", guards["frost"])
    return (
        all(entry["pass"] is True for entry in g2.values())
        and all(entry["pass"] is True for entry in g4.values())
        and g3["pass"] is not False
        and frost["pass"] is not False
    )


def data_checks(lines: Sequence[TempLine], *, f1: F1Tally) -> dict[str, object]:
    """F1 and the minimums of §8.6 (the discard check is per statistic)."""
    scored = [line for line in lines if line.scored]
    per_lead: dict[str, dict[str, int]] = {}
    minimums = True
    for quantity in TEMPERATURE_QUANTITIES:
        mine = [line for line in scored if line.quantity == quantity]
        counts = {str(lead): _distinct_dates(mine, lead) for lead in SCORED_LEADS}
        per_lead[quantity] = counts
        minimums = minimums and all(n >= H_MIN_DATES_PER_LEAD for n in counts.values())
    warm0 = _distinct_dates(
        [
            line
            for line in scored
            if line.quantity == "temperature_high" and line.warm_spell
        ],
        0,
    )
    return {
        "f1": f1.as_json(),
        "matched_dates_per_lead": per_lead,
        "warm_spell_lead0_dates": warm0,
        "minimums_met": minimums and warm0 >= WARM_SPELL_MIN_LEAD0_DATES,
        "f1_pass": f1.passes(),
    }


def split_segments(lines: Sequence[TempLine]) -> list[list[TempLine]]:
    """Contiguous runs of the split key, in (snapshot date, lead) order."""
    keyed = sorted(
        (line for line in lines if line.segment is not None),
        key=lambda line: (line.key.snapshot_date, line.key.lead, line.quantity),
    )
    out: list[list[TempLine]] = []
    previous: tuple[object, ...] | None = None
    for line in keyed:
        if not out or line.segment != previous:
            out.append([])
            previous = line.segment
        out[-1].append(line)
    return out


def segment_point(
    lines: Sequence[TempLine], name: str, *, params: TempParams = OWNER_PARAMS
) -> float | None:
    """The tile's point Delta in one segment, W_L renormalised per quantity."""
    total = 0.0
    for quantity in TEMPERATURE_QUANTITIES:
        mine = [line for line in lines if line.quantity == quantity and line.scored]
        deltas: dict[int, list[float]] = {}
        for line in mine:
            a = line.value(name)
            if a is None or line.control is None or line.truth is None:
                continue
            deltas.setdefault(line.key.lead, []).append(
                temperature_loss(quantity, a, line.truth, params)
                - temperature_loss(quantity, line.control, line.truth, params)
            )
        point = lead_weighted(deltas)
        if point is None:
            return None
        total += TILE_WEIGHTS[quantity] * point
    return total


def segment_report(
    segments: Sequence[Sequence[TempLine]], names: Sequence[str]
) -> tuple[list[dict[str, object]], dict[str, bool]]:
    """Each segment's span, key and point Delta, and per name a sign flip.

    A flip is two segments, each with at least SPLIT_MIN_DATES dates at
    every lead 0-5 for both quantities, whose point Deltas have opposite
    signs (§11.4).
    """
    report: list[dict[str, object]] = []
    signs: dict[str, list[float]] = {name: [] for name in names}
    for segment in segments:
        scored = [line for line in segment if line.scored]
        dates_per_lead = {
            lead: min(
                _distinct_dates([line for line in scored if line.quantity == q], lead)
                for q in TEMPERATURE_QUANTITIES
            )
            for lead in SCORED_LEADS
        }
        large = all(n >= SPLIT_MIN_DATES for n in dates_per_lead.values())
        points = {name: segment_point(segment, name) for name in names}
        if large:
            for name, point in points.items():
                if point is not None:
                    signs[name].append(point)
        report.append(
            {
                "first_snapshot": segment[0].key.snapshot_date,
                "last_snapshot": segment[-1].key.snapshot_date,
                "key": segment[0].segment,
                "dates_per_lead": {str(k): v for k, v in dates_per_lead.items()},
                "large": large,
                "point_delta": points,
            }
        )
    flips = {
        name: any(p < 0 for p in values) and any(p > 0 for p in values)
        for name, values in signs.items()
    }
    return report, flips


@dataclass(frozen=True)
class CandidateOutcome:
    """One candidate's outcome, its tile bounds and why (§8.6)."""

    name: str
    outcome: str
    tile: Bounds
    reasons: tuple[str, ...]


def candidate_outcome(
    name: str,
    *,
    tile: Bounds,
    missing: int,
    guards: Mapping[str, object],
    twin: Bounds | None,
    twin_missing: int,
    checks: Mapping[str, object],
    flip: bool,
    station_set_changed: Mapping[str, Sequence[str]] | None,
    db_import_changed: bool | None,
) -> CandidateOutcome:
    """One temperature candidate's outcome and reasons (§8.5, §8.6, §13.3).

    The first that applies decides: INSUFFICIENT for failed data checks;
    NOT_ASSESSED, ``cases_without_value:<missing>``, when the candidate has
    no value at some scored case; NOT_ASSESSED,
    ``twin_values_missing:<twin_missing>``, when its twin has none at some;
    INSUFFICIENT, ``no_point_estimate``; FAIL, ``point_delta_not_negative``
    then ``guard_failed``; INCONCLUSIVE, ``upper_bound_not_negative``; with a
    twin, NOT_ASSESSED, ``twin_comparison_unavailable``, when the
    head-to-head's point or U is None, else INCONCLUSIVE,
    ``does_not_beat_its_twin``, when its U >= 0; then PASS. A PASS with a
    sign flip becomes INCONCLUSIVE, and the changed-header rule is applied
    last, to every outcome.
    """
    reasons: list[str] = []
    if not checks["f1_pass"]:
        reasons.append("f1_below_95")
    if not checks["minimums_met"]:
        reasons.append("minimums_not_met")
    if not discard_ok(tile):
        reasons.append("discarded_over_1_percent")
    outcome: str
    if reasons:
        outcome = "INSUFFICIENT"
    elif missing:
        outcome = "NOT_ASSESSED"
        reasons.append(f"cases_without_value:{missing}")
    elif twin_missing:
        outcome = "NOT_ASSESSED"
        reasons.append(f"twin_values_missing:{twin_missing}")
    elif tile.point is None or tile.upper is None:
        outcome = "INSUFFICIENT"
        reasons.append("no_point_estimate")
    elif tile.point >= 0:
        outcome = "FAIL"
        reasons.append("point_delta_not_negative")
    elif not guards_pass(guards):
        outcome = "FAIL"
        reasons.append("guard_failed")
    elif tile.upper >= 0:
        outcome = "INCONCLUSIVE"
        reasons.append("upper_bound_not_negative")
    elif twin is not None and (twin.point is None or twin.upper is None):
        outcome = "NOT_ASSESSED"
        reasons.append("twin_comparison_unavailable")
    elif twin is not None and twin.upper is not None and twin.upper >= 0:
        outcome = "INCONCLUSIVE"
        reasons.append("does_not_beat_its_twin")
    else:
        outcome = "PASS"
    if outcome == "PASS" and flip:
        outcome = "INCONCLUSIVE"
        reasons.append("segment_sign_flip")
    changed = changed_header_outcome(
        outcome,
        station_set_changed=station_set_changed,
        db_import_changed=db_import_changed,
    )
    if changed != outcome:
        reasons.append("station_set_or_import_changed")
    return CandidateOutcome(name, changed, tile, tuple(reasons))


def choose_winner(
    outcomes: Mapping[str, CandidateOutcome],
    head_to_head: Callable[[str, str], Bounds],
) -> tuple[str | None, list[dict[str, object]]]:
    """§8.6: the first PASS in order, displaced only by a head-to-head U < 0."""
    passing = [n for n in TEMPERATURE_CANDIDATES if outcomes[n].outcome == "PASS"]
    if not passing:
        return None, []
    first = passing[0]
    trail: list[dict[str, object]] = []
    displacers: list[str] = []
    for other in passing[1:]:
        bounds = head_to_head(other, first)
        trail.append({"challenger": other, "incumbent": first, "bounds": bounds})
        if bounds.upper is not None and bounds.upper < 0:
            displacers.append(other)
    if not displacers:
        return first, trail

    def point(name: str) -> float:
        value = outcomes[name].tile.point
        return math.inf if value is None else value

    return min(displacers, key=point), trail


def tile_verdict(outcomes: Mapping[str, CandidateOutcome], winner: str | None) -> str:
    """The run's one verdict (§11.3) from the candidates' outcomes.

    PASS with a winner; else INSUFFICIENT when the data checks failed;
    else INCONCLUSIVE when any candidate is INCONCLUSIVE or NOT_ASSESSED;
    else FAIL.
    """
    if winner is not None:
        return "PASS"
    values = {o.outcome for o in outcomes.values()}
    if values == {"INSUFFICIENT"}:
        return "INSUFFICIENT"
    if "INCONCLUSIVE" in values or "NOT_ASSESSED" in values:
        return "INCONCLUSIVE"
    if "INSUFFICIENT" in values:
        return "INSUFFICIENT"
    return "FAIL"


def error_table(
    lines: Sequence[TempLine],
    names: Sequence[str],
    *,
    params: TempParams = OWNER_PARAMS,
) -> dict[str, dict[str, dict[str, dict[str, object]]]]:
    """MAE, bias and loss per name, quantity and lead (reported only)."""
    out: dict[str, dict[str, dict[str, dict[str, object]]]] = {}
    for name in names:
        per_q: dict[str, dict[str, dict[str, object]]] = {}
        for quantity in TEMPERATURE_QUANTITIES:
            per_lead: dict[str, dict[str, object]] = {}
            for lead in RECORD_LEADS:
                errors: list[float] = []
                losses: list[float] = []
                for line in lines:
                    if (
                        not line.matched
                        or line.quantity != quantity
                        or line.key.lead != lead
                        or line.truth is None
                    ):
                        continue
                    value = line.value(name)
                    if value is None:
                        continue
                    errors.append(value - line.truth)
                    losses.append(temperature_loss(quantity, value, line.truth, params))
                per_lead[str(lead)] = {
                    "n": len(errors),
                    "mae": statistics.fmean(abs(e) for e in errors) if errors else None,
                    "bias": statistics.fmean(errors) if errors else None,
                    "loss": statistics.fmean(losses) if losses else None,
                }
            per_q[quantity] = per_lead
        out[name] = per_q
    return out


def quantile_table(lines: Sequence[TempLine]) -> dict[str, dict[str, dict[str, int]]]:
    """How often the observed value was above every feed or below every feed."""
    out: dict[str, dict[str, dict[str, int]]] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        per_lead: dict[str, dict[str, int]] = {}
        for lead in RECORD_LEADS:
            above = below = n = 0
            for line in lines:
                if (
                    not line.matched
                    or line.quantity != quantity
                    or line.key.lead != lead
                    or line.truth is None
                    or not line.feeds
                ):
                    continue
                n += 1
                values = list(line.feeds.values())
                above += line.truth > max(values)
                below += line.truth < min(values)
            per_lead[str(lead)] = {"cases": n, "above_all": above, "below_all": below}
        out[quantity] = per_lead
    return out


def regime_counts(lines: Sequence[TempLine]) -> dict[str, dict[str, dict[str, int]]]:
    """Per quantity and lead 0-5, matched dates in each training regime."""
    out: dict[str, dict[str, dict[str, int]]] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        per_lead: dict[str, dict[str, int]] = {}
        for lead in SCORED_LEADS:
            counts = {"per_lead": 0, "pooled": 0, "no_eligible": 0}
            for line in lines:
                if (
                    line.scored
                    and line.quantity == quantity
                    and line.key.lead == lead
                    and line.regime in counts
                ):
                    counts[line.regime] += 1
            per_lead[str(lead)] = counts
        out[quantity] = per_lead
    return out


def choose_td_k(
    lines: Sequence[TempLine],
    recompose: Callable[[TempLine, int], float | None],
) -> dict[str, object]:
    """§8.2: K from the per-lead-regime matched dates of the choosing set.

    If any quantity has fewer than TD_K_MIN_DATES such dates at a lead 0-5,
    K is TD_K_DEFAULT. Otherwise the K with the lowest tile loss over those
    cases (high and low weighted 1/2 each, W_L over leads 0-5); a tie goes
    to the smaller K.
    """
    per_lead_lines = [
        line for line in lines if line.scored and line.regime == "per_lead"
    ]
    short = any(
        _distinct_dates([line for line in per_lead_lines if line.quantity == q], lead)
        < TD_K_MIN_DATES
        for q in TEMPERATURE_QUANTITIES
        for lead in SCORED_LEADS
    )
    if short:
        return {"k": TD_K_DEFAULT, "rule": "per_lead_dates_below_minimum", "losses": {}}
    losses: dict[int, float] = {}
    for k in TD_K_CHOICES:
        total = 0.0
        for quantity in TEMPERATURE_QUANTITIES:
            per_lead: dict[int, list[float]] = {}
            for line in per_lead_lines:
                if line.quantity != quantity or line.truth is None:
                    continue
                value = recompose(line, k)
                if value is None:
                    continue
                per_lead.setdefault(line.key.lead, []).append(
                    temperature_loss(quantity, value, line.truth)
                )
            point = lead_weighted(per_lead)
            total += TILE_WEIGHTS[quantity] * (math.inf if point is None else point)
        losses[k] = total
    best = min(TD_K_CHOICES, key=lambda k: (losses[k], k))
    return {"k": best, "rule": "lowest_tile_loss", "losses": losses}


def h_training_coverage(
    lines: Sequence[TempLine],
) -> dict[str, dict[str, dict[str, int]]]:
    """§11.5: per quantity and lead 0-5, H cases by counted training days.

    The count is the case's training days at its lead with a recorded row
    and eligible truth, feed-independent: 30 (or more), 20-29, fewer than 20.
    """
    out: dict[str, dict[str, dict[str, int]]] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        per_lead: dict[str, dict[str, int]] = {}
        for lead in SCORED_LEADS:
            buckets = {"30": 0, "20-29": 0, "under_20": 0}
            for line in lines:
                if (
                    line.quantity != quantity
                    or line.key.lead != lead
                    or line.training_days is None
                ):
                    continue
                if line.training_days >= TEMP_WINDOW_DAYS:
                    buckets["30"] += 1
                elif line.training_days >= TEMP_MIN_DAYS:
                    buckets["20-29"] += 1
                else:
                    buckets["under_20"] += 1
            per_lead[str(lead)] = buckets
        out[quantity] = per_lead
    return out


def tile_error_series(lines: Sequence[TempLine]) -> dict[date, float]:
    """A9 for temperature: D_t = sum_L W_L * e(t, L), e = 1/2|dH| + 1/2|dL|.

    Only dates with every lead 0-5 present for both quantities.
    """
    errors: dict[date, dict[int, dict[str, float]]] = {}
    for line in lines:
        if not line.scored or line.control is None or line.truth is None:
            continue
        errors.setdefault(line.key.target, {}).setdefault(line.key.lead, {})[
            line.quantity
        ] = abs(line.control - line.truth)
    out: dict[date, float] = {}
    for day, per_lead in errors.items():
        if all(
            len(per_lead.get(lead, {})) == len(TEMPERATURE_QUANTITIES)
            for lead in SCORED_LEADS
        ):
            out[day] = math.fsum(
                LEAD_WEIGHTS[lead]
                * math.fsum(
                    TILE_WEIGHTS[q] * per_lead[lead][q] for q in TEMPERATURE_QUANTITIES
                )
                for lead in SCORED_LEADS
            )
    return out


def counts_by_reason(lines: Iterable[TempLine]) -> Tally:
    """Matched and unmatched lines by quantity, reason and lead."""
    tally = Tally()
    for line in lines:
        tally.add(line.quantity, line.unmatched or "matched", line.key.lead)
    return tally


def temperature_line_json(line: TempLine) -> dict[str, object]:
    """One output line (§13.3) for a temperature (s, L, quantity)."""
    loss: float | None = None
    if line.control is not None and line.truth is not None:
        loss = temperature_loss(line.quantity, line.control, line.truth)
    candidates: dict[str, object] = {}
    for name, cand in line.candidates.items():
        cand_loss = (
            None
            if cand.value is None or line.truth is None
            else temperature_loss(line.quantity, cand.value, line.truth)
        )
        candidates[name] = {"result": cand, "loss": cand_loss}
    return {
        "snapshot_date": line.key.snapshot_date,
        "lead": line.key.lead,
        "target": line.key.target,
        "quantity": line.quantity,
        "unmatched": line.unmatched,
        "control": line.control,
        "control_loss": loss,
        "truth": line.truth,
        "f1_ok": line.f1,
        "training_window": line.window,
        "training_days": line.training_days,
        "regime": line.regime,
        "warm_spell": line.warm_spell,
        "frost": line.frost,
        "feeds": line.feeds,
        "candidates": candidates,
        "benchmarks": line.benchmarks,
    }


# --- Rain: station days, fault states and events (§6.6, §6.7) -----------------


def rain_fault_states(
    history: Mapping[str, Mapping[date, daily_method.StationDayTotal]],
) -> daily_method.FaultStates:
    """§6.7's fault states over ``history``, with the trial's settings."""
    return daily_method.station_fault_states(
        history,
        adequate_hours=RAIN_ADEQUATE_HOURS,
        wet_line_hundredths=FAULT_WET_LINE_HUNDREDTHS,
        dry_line_hundredths=FAULT_DRY_LINE_HUNDREDTHS,
        min_peers=FAULT_MIN_PEERS,
        suspect_lookback_days=SUSPECT_LOOKBACK_DAYS,
        suspect_min_days=SUSPECT_MIN_DAYS,
        suspect_ratio_below=SUSPECT_RATIO_BELOW,
        quarantine_lookback_days=QUARANTINE_LOOKBACK_DAYS,
        quarantine_min_days=QUARANTINE_MIN_DAYS,
        quarantine_min_episodes=QUARANTINE_MIN_EPISODES,
        quarantine_ratio_at_most=QUARANTINE_RATIO_AT_MOST,
        quarantine_dry_share=QUARANTINE_DRY_SHARE,
        recovery_lookback_days=RECOVERY_LOOKBACK_DAYS,
        recovery_min_days=RECOVERY_MIN_DAYS,
        recovery_min_episodes=RECOVERY_MIN_EPISODES,
        recovery_ratio_at_least=RECOVERY_RATIO_AT_LEAST,
        recovery_max_dry_days=RECOVERY_MAX_DRY_DAYS,
    )


def _fetched(reading: daily_method.RainReading) -> datetime:
    assert reading.fetched_at is not None
    return reading.fetched_at


class StationDayStore:
    """Station-day totals from the run's one station-reading load (§6.6).

    :meth:`full` reads every ``ok`` reading; :meth:`as_of` only those fetched
    by T, and a NULL ``fetched_at`` is never known. The readings fetched by T
    are a prefix of the day's readings sorted by ``fetched_at``, so a total
    is cached per (station, day, prefix length).
    """

    def __init__(
        self,
        readings: Mapping[str, Mapping[date, Sequence[daily_method.RainReading]]],
        *,
        timezone: str,
    ) -> None:
        self._tz = timezone
        self._all: dict[tuple[str, date], tuple[daily_method.RainReading, ...]] = {}
        self._known: dict[tuple[str, date], tuple[daily_method.RainReading, ...]] = {}
        self._fetched: dict[tuple[str, date], list[datetime]] = {}
        for station, days in readings.items():
            for day, rows in days.items():
                key = (station, day)
                self._all[key] = tuple(rows)
                known = sorted(
                    (r for r in rows if r.fetched_at is not None), key=_fetched
                )
                self._known[key] = tuple(known)
                self._fetched[key] = [_fetched(r) for r in known]
        self.stations: tuple[str, ...] = tuple(sorted({s for s, _ in self._all}))
        self.days: tuple[date, ...] = tuple(sorted({d for _, d in self._all}))
        self._expected: dict[date, int] = {}
        self._full: dict[tuple[str, date], daily_method.StationDayTotal] = {}
        self._prefix: dict[tuple[str, date, int], daily_method.StationDayTotal] = {}

    def expected_hours(self, day: date) -> int:
        """The local day's expected hours (23, 24 or 25)."""
        hours = self._expected.get(day)
        if hours is None:
            hours = local_day_bounds(day, self._tz).expected_slots
            self._expected[day] = hours
        return hours

    def full(self, station: str, day: date) -> daily_method.StationDayTotal | None:
        """The day's total from every ``ok`` reading; None with no reading."""
        key = (station, day)
        cached = self._full.get(key)
        if cached is not None:
            return cached
        rows = self._all.get(key)
        if not rows:
            return None
        total = daily_method.station_day_total(
            rows, expected_hours=self.expected_hours(day), gap_hours=GAP_HOURS
        )
        self._full[key] = total
        return total

    def as_of(
        self, station: str, day: date, known_by: datetime
    ) -> daily_method.StationDayTotal | None:
        """The day's total from the readings fetched by ``known_by``."""
        fetched = self._fetched.get((station, day))
        if not fetched:
            return None
        count = bisect_right(fetched, known_by)
        if count == 0:
            return None
        key = (station, day, count)
        cached = self._prefix.get(key)
        if cached is not None:
            return cached
        total = daily_method.station_day_total(
            self._known[(station, day)][:count],
            expected_hours=self.expected_hours(day),
            gap_hours=GAP_HOURS,
        )
        self._prefix[key] = total
        return total

    def history(
        self, *, known_by: datetime | None, last_day: date | None
    ) -> dict[str, dict[date, daily_method.StationDayTotal]]:
        """Every station-day total, in full (``known_by`` None) or as of T."""
        out: dict[str, dict[date, daily_method.StationDayTotal]] = {}
        for station, day in sorted(self._all):
            if last_day is not None and day > last_day:
                continue
            total = (
                self.full(station, day)
                if known_by is None
                else self.as_of(station, day, known_by)
            )
            if total is not None:
                out.setdefault(station, {})[day] = total
        return out

    def days_of(self, station: str) -> list[date]:
        """The local days on which ``station`` has an ``ok`` reading."""
        return [d for s, d in sorted(self._all) if s == station]

    def late_fetches(self) -> dict[str, int]:
        """Per station, the readings fetched more than LATE_FETCH_HOURS late."""
        limit = timedelta(hours=LATE_FETCH_HOURS)
        out: dict[str, int] = dict.fromkeys(self.stations, 0)
        for (station, _), rows in self._all.items():
            out[station] += sum(
                1
                for r in rows
                if r.fetched_at is not None and r.fetched_at - r.hour > limit
            )
        return out


@dataclass(frozen=True)
class RainDay:
    """One local day's event, observed amount and high-reading flags."""

    day: date
    event: daily_method.RainEvent
    amount: daily_method.ObservedAmount
    flags: daily_method.HighReadingFlags
    stations: tuple[daily_method.StationDay, ...]


def build_rain_day(
    day: date,
    totals: Mapping[str, daily_method.StationDayTotal],
    states: Mapping[tuple[str, date], str],
) -> RainDay:
    """The day's event from its station totals and the fault states (§6.6)."""
    stations = tuple(
        daily_method.StationDay(
            station_id=station,
            hundredths=total.hundredths,
            hours=total.hours,
            invalid=total.invalid,
            quarantined=states.get((station, day)) == "quarantined",
        )
        for station, total in sorted(totals.items())
    )
    event = daily_method.rain_nearby_event(
        stations,
        min_hours=RAIN_ADEQUATE_HOURS,
        pair_hundredths=RAIN_PAIR_HUNDREDTHS,
        single_hundredths=RAIN_SINGLE_HUNDREDTHS,
        min_dry_stations=RAIN_MIN_DRY_STATIONS,
    )
    amount = daily_method.observed_amount(
        stations,
        outcome=event.outcome,
        min_hours=RAIN_ADEQUATE_HOURS,
        wet_hundredths=RAIN_PAIR_HUNDREDTHS,
    )
    flags = daily_method.high_reading_flags(
        stations,
        wet_hundredths=RAIN_PAIR_HUNDREDTHS,
        large_factor=LARGE_TOTAL_FACTOR,
        min_hours=RAIN_ADEQUATE_HOURS,
    )
    return RainDay(day=day, event=event, amount=amount, flags=flags, stations=stations)


class RainEvents:
    """The run's events: in full for scoring, and as of each T for training.

    The fault states are built once in full, and once per decision time T
    over the readings fetched by T, for the days up to K(T) (§6.6, §6.7).
    """

    def __init__(self, store: StationDayStore, *, timezone: str) -> None:
        self._store = store
        self._tz = timezone
        self.full_states = rain_fault_states(
            store.history(known_by=None, last_day=None)
        )
        self._full: dict[date, RainDay] = {}
        self._states: dict[datetime, daily_method.FaultStates] = {}
        self._as_of: dict[tuple[datetime, date], RainDay] = {}

    def full(self, day: date) -> RainDay:
        """The day's event built from the copy in full (scoring)."""
        cached = self._full.get(day)
        if cached is None:
            totals: dict[str, daily_method.StationDayTotal] = {}
            for station in self._store.stations:
                total = self._store.full(station, day)
                if total is not None:
                    totals[station] = total
            cached = build_rain_day(day, totals, self.full_states.states)
            self._full[day] = cached
        return cached

    def states_at(self, known_by: datetime) -> daily_method.FaultStates:
        """The fault states over the readings fetched by ``known_by``."""
        cached = self._states.get(known_by)
        if cached is None:
            last = latest_knowable_target(self._tz, known_by)
            cached = rain_fault_states(
                self._store.history(known_by=known_by, last_day=last)
            )
            self._states[known_by] = cached
        return cached

    def as_of(self, day: date, known_by: datetime) -> RainDay:
        """The day's event as of ``known_by`` (training)."""
        key = (known_by, day)
        cached = self._as_of.get(key)
        if cached is None:
            totals: dict[str, daily_method.StationDayTotal] = {}
            for station in self._store.stations:
                total = self._store.as_of(station, day, known_by)
                if total is not None:
                    totals[station] = total
            cached = build_rain_day(day, totals, self.states_at(known_by).states)
            self._as_of[key] = cached
        return cached


def local_day_hours(day: date, timezone: str) -> list[datetime]:
    """The local day's hours in UTC, from its start to before its end."""
    bounds = local_day_bounds(day, timezone)
    hours: list[datetime] = []
    at = bounds.start_utc
    while at < bounds.end_utc:
        hours.append(at)
        at += timedelta(hours=1)
    return hours


# --- Rain cases: the stop, the window, the selection and the benchmarks -------


def rain_controls(
    rows: Iterable[RecordRow],
) -> dict[tuple[date, int], ControlValue | Unmatched]:
    """Every rain row's control, in snapshot and lead order, before any other work.

    This is where a rain row of a version the map does not hold stops the run
    (§13.1): :func:`control_value` raises :class:`VersionUnmappedError`, which
    nothing catches, so the run ends in ERROR before any event, selection or
    score is computed. In stage one, this covers every row from V_B on.
    """
    out: dict[tuple[date, int], ControlValue | Unmatched] = {}
    for row in sorted(rows, key=lambda r: (r.snapshot_date, r.lead)):
        out[(row.snapshot_date, row.lead)] = control_value(
            row, variable="precip", rain_rules=RAIN_RULES
        )
    return out


def _displayed_object(row: RecordRow) -> object:
    parsed = _load_json(row.daily_quantities)
    if not isinstance(parsed, dict):
        return None
    return cast("dict[str, object]", parsed).get("displayed")


def _row_threshold(row: RecordRow, site: Site) -> float:
    policy = narrow_policy(row.policy)
    if isinstance(policy, Unmatched):
        return site.rain_threshold_mm
    return policy.rain_threshold_mm


def _extrema_ids(row: RecordRow, control: ControlValue | None) -> tuple[int, ...]:
    if control is not None and control.displayed.extrema_feed_ids is not None:
        return control.displayed.extrema_feed_ids
    entries = narrow_candidates(row.candidates)
    if isinstance(entries, Unmatched):
        return ()
    return tuple(e.feed_id for e in entries if e.participation == "contributed")


@dataclass(frozen=True)
class RainContext:
    """Everything one rain run reads, loaded once."""

    site: Site
    rows: Mapping[tuple[date, int], RecordRow]
    controls: Mapping[tuple[date, int], ControlValue | Unmatched]
    recorded: Mapping[tuple[date, date], datetime]
    recorded_text: Mapping[tuple[date, date], str]
    replays: ReplayCache
    store: StationDayStore
    events: RainEvents


@dataclass(frozen=True)
class RainLine:
    """One rain (s, L): the control, the event and the served rain (§13.3)."""

    key: CaseKey
    row: RecordRow | None
    unmatched: str | None
    control: ControlValue | None
    day: RainDay
    f1: bool | None
    threshold_mm: float | None
    decision_time: str | None
    window: tuple[date, date] | None
    training_days: int
    asof_differs: int
    incumbent: daily_method.RememberedIncumbent | None
    selection: daily_method.Selection | None
    served: daily_method.ServedRain | None
    clim: daily_method.ClimChance | None
    issued: Mapping[str, tuple[str, ...]]
    symbols: Mapping[str, daily_method.SymbolValue]
    amounts: Mapping[str, daily_method.BenchmarkValue]
    grid: tuple[daily_method.GridRanking, ...]
    own_days: daily_method.OwnDaysComparison | None

    @property
    def outcome(self) -> daily_method.EventOutcome:
        """The target day's event, built in full."""
        return self.day.event.outcome

    @property
    def scored(self) -> bool:
        """A matched control and a known event."""
        return self.control is not None and self.outcome != "unknown"

    @property
    def no_eligible(self) -> bool:
        """The selection ran and left no ranked model (J = 0)."""
        return self.served is not None and self.served.no_eligible


def _feed_days(
    feeds: Iterable[FeedDayReplay],
) -> list[daily_method.FeedDay]:
    return [
        daily_method.FeedDay(feed_id=f.feed_id, ref=f.ref, values=f.values)
        for f in sorted(feeds, key=lambda f: f.feed_id)
        if f.covers
    ]


def _unmatched_line(
    key: CaseKey,
    row: RecordRow | None,
    unmatched: str | None,
    control: ControlValue | None,
    day: RainDay,
) -> RainLine:
    return RainLine(
        key=key,
        row=row,
        unmatched=unmatched,
        control=control,
        day=day,
        f1=None,
        threshold_mm=None,
        decision_time=None,
        window=None,
        training_days=0,
        asof_differs=0,
        incumbent=None,
        selection=None,
        served=None,
        clim=None,
        issued={},
        symbols={},
        amounts={},
        grid=(),
        own_days=None,
    )


def rain_case(
    ctx: RainContext,
    key: CaseKey,
    row: RecordRow | None,
    decisions: Mapping[date, daily_method.RecordDecision],
    *,
    with_grid: bool,
) -> RainLine:
    """One (s, L): the window as of T(s), the selection, the served rain.

    The selection runs on every case with a ``recorded`` row, whatever its
    control; a missing or ``missed`` row has no decision time and no
    selection, and is a gap in the incumbent chain. A row whose replay fails
    F1 is unmatched (``f1_mismatch``) and its control is not scored (§7.3).
    """
    day = ctx.events.full(key.target)
    if row is None:
        return _unmatched_line(key, None, "no_row", None, day)
    found = ctx.controls[(key.snapshot_date, key.lead)]
    control = found if isinstance(found, ControlValue) else None
    unmatched = found.reason if isinstance(found, Unmatched) else None
    if row.status != "recorded":
        return _unmatched_line(key, row, unmatched, control, day)
    tz = ctx.site.timezone
    lead = key.lead
    at = parse_utc(row.snapshot_utc)
    replay = ctx.replays.get(key.snapshot_date, row.snapshot_utc)
    threshold = (
        control.policy.rain_threshold_mm
        if control is not None
        else _row_threshold(row, ctx.site)
    )
    f1 = None if control is None else f1_ok(row, control, replay)
    stored_control = control
    if f1 is False:
        control = None
        unmatched = "f1_mismatch"
    k_day = latest_knowable_target(tz, at)
    first = k_day - timedelta(days=RAIN_WINDOW_DAYS - 1)
    as_of = {d: ctx.events.as_of(d, at) for d in _days(first, k_day)}
    window_events = {
        d: rd.event.outcome == "rain_nearby"
        for d, rd in as_of.items()
        if rd.event.outcome != "unknown"
    }
    refs: dict[str, daily_method.ModelRef] = {}
    totals: dict[str, dict[date, float]] = {}
    vote_days: list[daily_method.VoteDay] = []
    ciw: dict[int, list[tuple[float, float]]] = {}
    training = daily_method.training_days(
        ctx.recorded, first=first, last=k_day, lead=lead
    )
    differs = 0
    for d, _ in training:
        past = ctx.replays.get(
            d - timedelta(days=lead), ctx.recorded_text[(d - timedelta(days=lead), d)]
        )
        for feed in past.feeds(lead).values():
            refs.setdefault(feed.ref.key, feed.ref)
        for model, feed in past.by_model(lead).items():
            if feed.covers and feed.total is not None:
                totals.setdefault(model, {})[d] = feed.total
        event = as_of[d]
        if event.event.outcome != ctx.events.full(d).event.outcome:
            differs += 1
        if event.event.outcome == "unknown":
            continue
        voters = _feed_days(past.feeds(lead).values())
        votes = daily_method.cast_votes(voters, threshold_mm=threshold)
        if votes is not None:
            vote_days.append(
                daily_method.VoteDay(
                    k=sum(1 for _, v in votes if v),
                    n=len(votes),
                    rain_nearby=event.event.outcome == "rain_nearby",
                )
            )
        observed = event.amount.amount
        if event.event.outcome == "rain_nearby" and observed is not None:
            for feed in voters:
                ciw.setdefault(feed.feed_id, []).append(
                    (
                        daily_method.qualifying_sum(
                            feed.values, threshold_mm=threshold
                        ),
                        observed,
                    )
                )
    case_models = replay.by_model(lead)
    for feed in replay.feeds(lead).values():
        refs.setdefault(feed.ref.key, feed.ref)
    covering = {
        m: f.total for m, f in case_models.items() if f.covers and f.total is not None
    }
    window = daily_method.RainWindow(
        events=window_events,
        models={
            m: daily_method.ModelWindow(
                source=ref.source,
                model=ref.model,
                totals=totals.get(m, {}),
                covers_target=m in covering,
            )
            for m, ref in sorted(refs.items())
        },
    )
    remembered = daily_method.remembered_incumbent(
        decisions, snapshot_date=key.snapshot_date
    )
    selection = daily_method.select_model(
        window,
        min_days=RAIN_MIN_DAYS,
        miss_weight=RAIN_MISS_WEIGHT,
        triggers_hundredths=(daily_method.RAIN_TRIGGER_HUNDREDTHS,),
        incumbent=remembered.model,
        switch_margin=RAIN_SWITCH_MARGIN,
    )
    ranked_totals = {r.key: covering[r.key] for r in selection.ranked}
    samples = {
        m: list(zip(f.hours, f.values, strict=True))
        for m, f in case_models.items()
        if m in covering
    }
    served = daily_method.served_rain(
        selection,
        ranked_totals,
        samples,
        hours=local_day_hours(key.target, tz),
        group_fraction=GROUP_FRACTION,
        trigger_hundredths=daily_method.RAIN_TRIGGER_HUNDREDTHS,
        miss_weight=RAIN_MISS_WEIGHT,
        threshold_mm=threshold,
    )
    issued: dict[str, tuple[str, ...]] = {}
    if served.group is not None:
        issued = {m: case_models[m].issued_at for m in served.group.members}
    feeds = _feed_days(replay.feeds(lead).values())
    r1 = daily_method.compose_r1(
        feeds,
        vote_days,
        threshold_mm=threshold,
        lam=R1_LAMBDA,
        max_iterations=R1_MAX_ITERATIONS,
        tolerance=R1_TOLERANCE,
        min_days=R1_MIN_DAYS,
        min_each=R1_MIN_EACH,
    )
    control_symbol = None if control is None else control.symbol
    control_amount = None if control is None else control.amount
    symbols = {
        "always": daily_method.compose_fixed_symbol(show=True),
        "never": daily_method.compose_fixed_symbol(show=False),
        "persistence": daily_method.compose_symbol_persistence(
            window_events, control=control_symbol
        ),
        "S-PICK": daily_method.compose_s_pick(selection, covering),
        "S-TOP2": daily_method.compose_s_top2(
            selection, covering, trigger_hundredths=daily_method.RAIN_TRIGGER_HUNDREDTHS
        ),
        "R0": daily_method.compose_r0(feeds, threshold_mm=threshold),
        "MB-R0": daily_method.compose_mb_r0(feeds, threshold_mm=threshold),
        "G-R0": daily_method.compose_g_r0(
            feeds, threshold_mm=threshold, miss_weight=RAIN_MISS_WEIGHT
        ),
        "G-ES": daily_method.compose_g_es(
            feeds,
            _extrema_ids(row, stored_control),
            threshold_mm=threshold,
            miss_weight=RAIN_MISS_WEIGHT,
        ),
        "R1": r1,
        "G-R1": daily_method.compose_g_r1(r1, miss_weight=RAIN_MISS_WEIGHT),
    }
    by_ref = {case_models[m].ref: total for m, total in covering.items()}
    window_amounts = {
        d: rd.amount.amount
        for d, rd in as_of.items()
        if rd.amount.amount is not None and rd.event.outcome != "unknown"
    }
    amounts: dict[str, daily_method.BenchmarkValue] = {
        "M-C": daily_method.compose_m_c(feeds, threshold_mm=threshold),
        "M-CIW": daily_method.compose_m_ciw(
            feeds,
            ciw,
            threshold_mm=threshold,
            min_days=M_CIW_MIN_DAYS,
            floor=M_CIW_FLOOR_MM,
        ),
        "EQ": daily_method.compose_eq(by_ref),
        "MB-mean": daily_method.compose_mb_mean(by_ref),
        "persistence": daily_method.compose_persistence(
            window_amounts, control=control_amount
        ),
    }
    if selection.ranked:
        group = daily_method.group_amount_benchmarks(
            [(r.key, ranked_totals[r.key]) for r in selection.ranked],
            group_fraction=GROUP_FRACTION,
        )
        amounts["group_median"] = daily_method.BenchmarkValue(
            "group_median", group.median, (), group.members
        )
        amounts["group_mean"] = daily_method.BenchmarkValue(
            "group_mean", group.mean, (), group.members
        )
    if selection.best is not None:
        amounts["best"] = daily_method.BenchmarkValue(
            "best", ranked_totals[selection.best], (), (selection.best,)
        )
    grid: tuple[daily_method.GridRanking, ...] = ()
    own: daily_method.OwnDaysComparison | None = None
    if with_grid:
        grid = daily_method.compose_trigger_grid(
            window,
            covering,
            min_days=RAIN_MIN_DAYS,
            miss_weight=RAIN_MISS_WEIGHT,
            grid_hundredths=daily_method.TRIGGER_GRID_HUNDREDTHS,
            incumbent=None,
            switch_margin=RAIN_SWITCH_MARGIN,
        )
        own = daily_method.compose_own_days(
            window,
            selection,
            min_days=RAIN_MIN_DAYS,
            miss_weight=RAIN_MISS_WEIGHT,
            trigger_hundredths=daily_method.RAIN_TRIGGER_HUNDREDTHS,
            group_fraction=GROUP_FRACTION,
        )
    return RainLine(
        key=key,
        row=row,
        unmatched=unmatched,
        control=control,
        day=day,
        f1=f1,
        threshold_mm=threshold,
        decision_time=row.snapshot_utc,
        window=(first, k_day),
        training_days=len(training),
        asof_differs=differs,
        incumbent=remembered,
        selection=selection,
        served=served,
        clim=daily_method.climatology_chance(window_events, min_days=CLIM_MIN_DAYS),
        issued=issued,
        symbols=symbols,
        amounts=amounts,
        grid=grid,
        own_days=own,
    )


def record_decisions(
    rows: Mapping[tuple[date, int], RecordRow],
) -> dict[int, dict[date, daily_method.RecordDecision]]:
    """``--incumbent-from-record``: each lead's decisions read from the rows.

    A ``missed`` row and a date with no row are gaps (absent).
    """
    out: dict[int, dict[date, daily_method.RecordDecision]] = {
        lead: {} for lead in RECORD_LEADS
    }
    for (snapshot, lead), row in rows.items():
        if row.status != "recorded" or lead not in out:
            continue
        out[lead][snapshot] = daily_method.record_decision(_displayed_object(row))
    return out


def rain_lines(
    ctx: RainContext,
    cases: Sequence[tuple[CaseKey, RecordRow | None]],
    *,
    incumbent_from_record: bool,
    with_grid: bool,
) -> list[RainLine]:
    """Every case in snapshot-date order, carrying the incumbent chain (§9.1).

    Without ``incumbent_from_record`` the chain starts empty at S0: each
    decided case records its selected model, or a clear under
    ``no_eligible``, and a date with no decision is a gap.
    """
    from_record = incumbent_from_record
    decisions: dict[int, dict[date, daily_method.RecordDecision]] = (
        record_decisions(ctx.rows)
        if from_record
        else {lead: {} for lead in RECORD_LEADS}
    )
    out: list[RainLine] = []
    for key, row in sorted(cases, key=lambda c: (c[0].snapshot_date, c[0].lead)):
        line = rain_case(ctx, key, row, decisions[key.lead], with_grid=with_grid)
        selection = line.selection
        if not from_record and selection is not None:
            decisions[key.lead][key.snapshot_date] = (
                daily_method.RecordDecision("clear")
                if selection.no_eligible or selection.selected is None
                else daily_method.RecordDecision("selected", selection.selected)
            )
        out.append(line)
    return out


# --- Rain scoring (§9.2, §9.3, §9.4) ---------------------------------------------


def symbol_cost(shown: bool, rain_nearby: bool, *, miss_weight: float) -> float:
    """w_r for a missed rain-nearby day, 1 for a false alarm, else 0."""
    if rain_nearby and not shown:
        return miss_weight
    if shown and not rain_nearby:
        return 1.0
    return 0.0


def amount_loss(forecast: float, observed: float, *, under_weight: float) -> float:
    """w_amount * max(y - F, 0) + max(F - y, 0)."""
    return under_weight * max(observed - forecast, 0.0) + max(forecast - observed, 0.0)


def symbol_values(line: RainLine) -> dict[str, bool | None]:
    """The control's, S-Q's and every symbol benchmark's value at one case."""
    out: dict[str, bool | None] = {
        "control": None if line.control is None else line.control.symbol,
        "S-Q": None if line.served is None else line.served.symbol,
    }
    for name, value in line.symbols.items():
        out[name] = value.symbol
    return out


def chance_values(line: RainLine) -> dict[str, float | None]:
    """CH-Q, R-clim, the vote shares and the control's symbol read as 0 or 1."""
    served = line.served
    out: dict[str, float | None] = {
        "CH-Q": None
        if served is None or served.chance is None
        else float(served.chance),
        "R-clim": None
        if line.clim is None or line.clim.chance is None
        else float(line.clim.chance),
        "control": None
        if line.control is None or line.control.symbol is None
        else float(line.control.symbol),
    }
    for name in ("R0", "MB-R0", "R1"):
        value = line.symbols.get(name)
        out[name] = None if value is None else value.chance
    return out


def amount_values(line: RainLine) -> dict[str, float | None]:
    """The control's, the served and every amount benchmark's value."""
    out: dict[str, float | None] = {
        "control": None if line.control is None else line.control.amount,
        "served": None if line.served is None else line.served.amount,
    }
    for name, value in line.amounts.items():
        out[name] = value.value
    return out


def _lead_point(per_lead: Mapping[int, Sequence[float]]) -> float | None:
    if any(not per_lead.get(lead) for lead in SCORED_LEADS):
        return None
    return math.fsum(
        LEAD_WEIGHTS[lead] * math.fsum(per_lead[lead]) / len(per_lead[lead])
        for lead in SCORED_LEADS
    )


def _mean(values: Sequence[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def _symbol_common(line: RainLine) -> bool:
    values = symbol_values(line)
    return line.scored and values["control"] is not None and values["S-Q"] is not None


def symbol_report(
    lines: Sequence[RainLine],
    index_lists: Sequence[Sequence[int]],
    dates: Sequence[date],
) -> dict[str, object]:
    """The symbol decision: S-Q against the control (§9.3), with reports."""
    common = [line for line in lines if _symbol_common(line)]
    deltas: dict[tuple[str, date, int], float] = {}
    for line in common:
        values = symbol_values(line)
        sq, ctl = values["S-Q"], values["control"]
        assert sq is not None and ctl is not None
        wet = line.outcome == "rain_nearby"
        deltas[("rain", line.key.target, line.key.lead)] = symbol_cost(
            sq, wet, miss_weight=RAIN_MISS_WEIGHT
        ) - symbol_cost(ctl, wet, miss_weight=RAIN_MISS_WEIGHT)
    bounds = daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights={"rain": 1.0},
        lead_weights=LEAD_WEIGHTS,
        level=LEVEL_SYMBOL,
    )
    sources: dict[str, dict[int, dict[str, object]]] = {}
    points: dict[str, dict[float, float | None]] = {}
    names = sorted({name for line in common for name in symbol_values(line)})
    for name in names:
        per_lead: dict[int, dict[str, object]] = {}
        costs: dict[float, dict[int, list[float]]] = {
            w: {} for w in (1.0, 1.5, RAIN_MISS_WEIGHT, 3.0)
        }
        for lead in RECORD_LEADS:
            at_lead = [line for line in common if line.key.lead == lead]
            misses = false_alarms = missing = shown = 0
            dry = 0
            for line in at_lead:
                value = symbol_values(line)[name]
                if value is None:
                    missing += 1
                    continue
                wet = line.outcome == "rain_nearby"
                misses += wet and not value
                false_alarms += value and not wet
                shown += value
                dry += not wet
                for w, per in costs.items():
                    per.setdefault(lead, []).append(
                        symbol_cost(value, wet, miss_weight=w)
                    )
            per_lead[lead] = {
                "cases": len(at_lead),
                "missing": missing,
                "misses": misses,
                "false_alarms": false_alarms,
                "dry_shown_share": None if dry == 0 else false_alarms / dry,
                "shown": shown,
                "cost_per_case": _mean(costs[RAIN_MISS_WEIGHT].get(lead, [])),
            }
        sources[name] = per_lead
        points[name] = {w: _lead_point(per) for w, per in costs.items()}
    sq_point = points.get("S-Q", {}).get(RAIN_MISS_WEIGHT)
    always = points.get("always", {}).get(RAIN_MISS_WEIGHT)
    never = points.get("never", {}).get(RAIN_MISS_WEIGHT)
    sanity = (
        None
        if sq_point is None or always is None or never is None
        else sq_point < always and sq_point < never
    )
    return {
        "level": LEVEL_SYMBOL,
        "cases": len(common),
        "delta": bounds,
        "discard_ok": discard_ok(bounds),
        "sources": sources,
        "lead_weighted_cost": {
            name: {str(w): p for w, p in per.items()} for name, per in points.items()
        },
        "sanity_below_always_and_never": sanity,
        "unknown_as_rain_nearby": _unknown_sensitivity(lines, wet=True),
        "unknown_as_dry": _unknown_sensitivity(lines, wet=False),
        "without_few_station_dry_days": _symbol_point_without(common, _few_station_dry),
        "without_flagged_days": _symbol_point_without(common, _flagged_day),
        "j_1_to_4": _small_group_counts(common),
        "days_per_month": _days_per_month(common),
    }


def _unknown_sensitivity(lines: Sequence[RainLine], *, wet: bool) -> float | None:
    per_lead: dict[int, list[float]] = {}
    for line in lines:
        if line.control is None or line.control.symbol is None or line.served is None:
            continue
        sq = line.served.symbol
        if sq is None:
            continue
        event = wet if line.outcome == "unknown" else line.outcome == "rain_nearby"
        per_lead.setdefault(line.key.lead, []).append(
            symbol_cost(sq, event, miss_weight=RAIN_MISS_WEIGHT)
            - symbol_cost(line.control.symbol, event, miss_weight=RAIN_MISS_WEIGHT)
        )
    return _lead_point(per_lead)


FEW_STATION_DRY_BELOW = 3
"""A dry day with fewer covered stations is left out of one sensitivity (§9.3)."""


def _few_station_dry(line: RainLine) -> bool:
    return (
        line.outcome == "dry" and len(line.day.event.adequate) < FEW_STATION_DRY_BELOW
    )


def _flagged_day(line: RainLine) -> bool:
    flags = line.day.flags
    return flags.large_total is not None or flags.lone_reading is not None


def _small_group_counts(common: Sequence[RainLine]) -> dict[str, dict[str, int]]:
    """Per lead: S-Q's cases, misses and false alarms when 1 <= J <= 4 (A15)."""
    out: dict[str, dict[str, int]] = {}
    for line in common:
        if line.selection is None or not 1 <= len(line.selection.ranked) <= 4:
            continue
        sq = symbol_values(line)["S-Q"]
        wet = line.outcome == "rain_nearby"
        counts = out.setdefault(
            str(line.key.lead), {"cases": 0, "misses": 0, "false_alarms": 0}
        )
        counts["cases"] += 1
        counts["misses"] += bool(wet and not sq)
        counts["false_alarms"] += bool(sq and not wet)
    return out


def _symbol_point_without(
    common: Sequence[RainLine], drop: Callable[[RainLine], bool]
) -> float | None:
    """The lead-weighted S-Q minus control cost with the ``drop`` days left out."""
    per_lead: dict[int, list[float]] = {}
    for line in common:
        if drop(line):
            continue
        values = symbol_values(line)
        sq, ctl = values["S-Q"], values["control"]
        assert sq is not None and ctl is not None
        wet = line.outcome == "rain_nearby"
        per_lead.setdefault(line.key.lead, []).append(
            symbol_cost(sq, wet, miss_weight=RAIN_MISS_WEIGHT)
            - symbol_cost(ctl, wet, miss_weight=RAIN_MISS_WEIGHT)
        )
    return _lead_point(per_lead)


def _days_per_month(common: Sequence[RainLine]) -> dict[str, dict[str, dict[str, int]]]:
    out: dict[str, dict[str, dict[str, int]]] = {}
    for name in ("control", "S-Q"):
        per: dict[str, dict[str, int]] = {}
        for line in common:
            value = symbol_values(line)[name]
            month = line.key.target.strftime("%Y-%m")
            counts = per.setdefault(str(line.key.lead), {})
            counts[month] = counts.get(month, 0) + bool(value)
        out[name] = per
    return out


def _brier(p: float, wet: bool) -> float:
    return (p - (1.0 if wet else 0.0)) ** 2


def chance_report(
    lines: Sequence[RainLine],
    index_lists: Sequence[Sequence[int]],
    dates: Sequence[date],
) -> dict[str, object]:
    """The chance decision: CH-Q against R-clim by the Brier score (§9.2)."""
    common: list[tuple[RainLine, float, float]] = []
    for line in lines:
        if not line.scored:
            continue
        values = chance_values(line)
        chq, clim = values["CH-Q"], values["R-clim"]
        if chq is not None and clim is not None:
            common.append((line, chq, clim))
    deltas = {
        ("rain", line.key.target, line.key.lead): _brier(
            chq, line.outcome == "rain_nearby"
        )
        - _brier(clim, line.outcome == "rain_nearby")
        for line, chq, clim in common
    }
    bounds = daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights={"rain": 1.0},
        lead_weights=LEAD_WEIGHTS,
        level=LEVEL_CHANCE,
    )
    briers: dict[str, dict[str, float | None]] = {}
    for name in ("CH-Q", "R-clim", "control", "R0", "MB-R0", "R1"):
        per_lead: dict[str, float | None] = {}
        for lead in RECORD_LEADS:
            scores = [
                _brier(value, line.outcome == "rain_nearby")
                for line, _, _ in common
                if line.key.lead == lead
                and (value := chance_values(line)[name]) is not None
            ]
            per_lead[str(lead)] = _mean(scores)
        briers[name] = per_lead
    pairs = [(chq, line.outcome == "rain_nearby") for line, chq, _ in common]
    clim_brier = _mean(
        [_brier(c, line.outcome == "rain_nearby") for line, _, c in common]
    )
    chq_brier = _mean([_brier(p, wet) for p, wet in pairs])
    skill = (
        None
        if clim_brier is None or chq_brier is None or clim_brier == 0
        else 1.0 - chq_brier / clim_brier
    )
    return {
        "level": LEVEL_CHANCE,
        "cases": len(common),
        "delta": bounds,
        "discard_ok": discard_ok(bounds),
        "passes": bounds.upper is not None and bounds.upper < 0 and discard_ok(bounds),
        "brier_per_lead": briers,
        "brier_skill_score": skill,
        "reliability": reliability_table(pairs),
    }


def reliability_table(pairs: Sequence[tuple[float, bool]]) -> dict[str, object]:
    """Ten equal-width bins of the chance, and the Brier decomposition."""
    bins: list[list[tuple[float, bool]]] = [[] for _ in range(10)]
    for p, wet in pairs:
        bins[min(int(p * 10), 9)].append((p, wet))
    n = len(pairs)
    base = None if n == 0 else sum(1 for _, wet in pairs if wet) / n
    rows: list[dict[str, object]] = []
    reliability = resolution = 0.0
    for index, members in enumerate(bins):
        count = len(members)
        mean_p = _mean([p for p, _ in members])
        observed = None if count == 0 else sum(1 for _, w in members if w) / count
        if mean_p is not None and observed is not None and base is not None:
            reliability += count * (mean_p - observed) ** 2
            resolution += count * (observed - base) ** 2
        rows.append(
            {
                "bin": [index / 10, (index + 1) / 10],
                "count": count,
                "mean_chance": mean_p,
                "observed_frequency": observed,
                "holds_one_third": index == 3,
            }
        )
    return {
        "bins": rows,
        "reliability": None if n == 0 else reliability / n,
        "resolution": None if n == 0 else resolution / n,
        "uncertainty": None if base is None else base * (1.0 - base),
    }


def _observed(line: RainLine) -> float | None:
    if line.outcome == "unknown":
        return None
    return line.day.amount.amount


def amount_report(
    lines: Sequence[RainLine],
    index_lists: Sequence[Sequence[int]],
    dates: Sequence[date],
) -> dict[str, object]:
    """The amount, scored and reported only (§9.4; D14 open)."""
    common: list[RainLine] = []
    for line in lines:
        values = amount_values(line)
        if (
            line.scored
            and _observed(line) is not None
            and values["control"] is not None
            and values["served"] is not None
        ):
            common.append(line)

    def paired(name: str, against: str) -> dict[tuple[str, date, int], float]:
        out: dict[tuple[str, date, int], float] = {}
        for line in common:
            values = amount_values(line)
            a, b, y = values[name], values[against], _observed(line)
            if a is None or b is None or y is None:
                continue
            out[("rain", line.key.target, line.key.lead)] = amount_loss(
                a, y, under_weight=W_AMOUNT
            ) - amount_loss(b, y, under_weight=W_AMOUNT)
        return out

    def bounds_of(deltas: Mapping[tuple[str, date, int], float]) -> Bounds:
        return daily_bounds(
            deltas,
            index_lists,
            dates=dates,
            quantity_weights={"rain": 1.0},
            lead_weights=LEAD_WEIGHTS,
            level=LEVEL_AMOUNT,
        )

    main = bounds_of(paired("served", "control"))
    benchmarks = {
        name: bounds_of(paired(name, "served"))
        for name in ("group_median", "group_mean", "best")
    }
    sources: dict[str, dict[str, object]] = {}
    names = sorted({name for line in common for name in amount_values(line)})
    for name in names:
        per_lead: dict[str, object] = {}
        for lead in RECORD_LEADS:
            pairs = [
                (value, y)
                for line in common
                if line.key.lead == lead
                and (value := amount_values(line)[name]) is not None
                and (y := _observed(line)) is not None
            ]
            per_lead[str(lead)] = {
                "cases": len(pairs),
                "loss": _mean(
                    [amount_loss(f, y, under_weight=W_AMOUNT) for f, y in pairs]
                ),
                "loss_by_weight": {
                    str(w): _mean([amount_loss(f, y, under_weight=w) for f, y in pairs])
                    for w in (1.0, 2.0, 3.0)
                },
                "mae": _mean([abs(f - y) for f, y in pairs]),
                "bias": _mean([f - y for f, y in pairs]),
            }
        sources[name] = per_lead
    tradeoffs: dict[str, dict[str, object]] = {}
    for lead in RECORD_LEADS:
        served: list[tuple[float, daily_method.EventOutcome]] = [
            (line.served.amount, line.outcome)
            for line in lines
            if line.key.lead == lead
            and line.served is not None
            and line.served.amount is not None
            and line.outcome != "unknown"
        ]
        counts = {"dry_shown": 0, "wet_short": 0}
        for amount, outcome in served:
            kind = daily_method.amount_tradeoff(amount, outcome=outcome)
            if kind is not None:
                counts[kind] += 1
        values = [a for a, _ in served]
        tradeoffs[str(lead)] = {
            **counts,
            "median": statistics.median(values) if values else None,
            "mean": _mean(values),
        }
    return {
        "level": LEVEL_AMOUNT,
        "w_amount": W_AMOUNT,
        "cases": len(common),
        "delta": main,
        "discard_ok": discard_ok(main),
        "benchmark_minus_served": benchmarks,
        "sources": sources,
        "tradeoffs": tradeoffs,
    }


def rain_case_counts(lines: Sequence[RainLine]) -> dict[str, dict[str, object]]:
    """Per lead: the selector's and the event's case counts (§9.1, §9.2, §7.3)."""
    out: dict[str, dict[str, object]] = {}
    for lead in RECORD_LEADS:
        at = [line for line in lines if line.key.lead == lead]
        decided = [line for line in at if line.selection is not None]
        j_spread: dict[str, int] = {}
        outside = j_small = full = provisional = no_eligible = covered = 0
        clim_short = unrecognised = 0
        outcomes: dict[str, int] = {}
        for line in decided:
            selection = line.selection
            served = line.served
            assert selection is not None and served is not None
            j = len(selection.ranked)
            j_spread[str(j)] = j_spread.get(str(j), 0) + 1
            j_small += 1 <= j <= 4
            if served.no_eligible:
                no_eligible += 1
                covered += line.control is not None and line.control.symbol is not None
            elif selection.provisional:
                provisional += 1
            else:
                full += 1
            if served.group is not None and served.selected not in served.group.members:
                outside += 1
            if line.clim is not None and line.clim.fallback_reason == "clim_short":
                clim_short += 1
            if (
                line.incumbent is not None
                and line.incumbent.unrecognised_on is not None
            ):
                unrecognised += 1
            kind = selection.incumbent_outcome or "no_incumbent"
            outcomes[kind] = outcomes.get(kind, 0) + 1
        out[str(lead)] = {
            "cases": len(at),
            "decided": len(decided),
            "event_unknown": sum(1 for line in at if line.outcome == "unknown"),
            "amount_not_available": sum(
                1 for line in at if line.day.amount.reason == "amount_not_available"
            ),
            "full_ranking": full,
            "provisional": provisional,
            "no_eligible": no_eligible,
            "no_eligible_control_covered": covered,
            "clim_short": clim_short,
            "j_spread": j_spread,
            "j_1_to_4": j_small,
            "selected_outside_group": outside,
            "incumbent_outcomes": outcomes,
            "incumbent_row_unrecognised": unrecognised,
            "training_days": sum(line.training_days for line in decided),
            "training_asof_differs": sum(line.asof_differs for line in decided),
        }
    return out


def rain_event_counts(lines: Sequence[RainLine]) -> dict[str, dict[str, object]]:
    """Per lead: rain-nearby, dry and unknown cases, with the trigger conditions."""
    out: dict[str, dict[str, object]] = {}
    for lead in RECORD_LEADS:
        counts = {"rain_nearby": 0, "dry": 0, "unknown": 0}
        triggers: dict[str, int] = {}
        for line in lines:
            if line.key.lead != lead:
                continue
            counts[line.outcome] += 1
            trigger = line.day.event.trigger
            if trigger is not None:
                triggers[trigger] = triggers.get(trigger, 0) + 1
        out[str(lead)] = {**counts, "triggers": triggers}
    return out


def rain_counts(lines: Iterable[RainLine]) -> Tally:
    """Matched and unmatched rain cases by reason and lead."""
    tally = Tally()
    for line in lines:
        tally.add("rain", line.unmatched or "matched", line.key.lead)
    return tally


def rain_block_series(lines: Sequence[RainLine]) -> dict[str, dict[date, float]]:
    """A9 for rain: the control's daily error series per decision (§10.3).

    Symbol: 1 for a miss or a false alarm; amount: |F - y|. Only dates with
    every lead 0-5 present, and only known days.
    """
    per: dict[str, dict[date, dict[int, float]]] = {"symbol": {}, "amount": {}}
    for line in lines:
        if not line.scored or line.control is None:
            continue
        wet = line.outcome == "rain_nearby"
        if line.control.symbol is not None:
            per["symbol"].setdefault(line.key.target, {})[line.key.lead] = (
                1.0 if line.control.symbol != wet else 0.0
            )
        y = _observed(line)
        if line.control.amount is not None and y is not None:
            per["amount"].setdefault(line.key.target, {})[line.key.lead] = abs(
                line.control.amount - y
            )
    out: dict[str, dict[date, float]] = {}
    for name, by_day in per.items():
        out[name] = {
            day: math.fsum(LEAD_WEIGHTS[lead] * leads[lead] for lead in SCORED_LEADS)
            for day, leads in by_day.items()
            if all(lead in leads for lead in SCORED_LEADS)
        }
    return out


def replay_time_stats(seconds: Sequence[float]) -> dict[str, object]:
    """A11: the dates replayed, and the median, p95 (nearest rank) and max."""
    ordered = sorted(seconds)
    if not ordered:
        return {"dates": 0, "median": None, "p95": None, "max": None}
    rank = math.ceil(0.95 * len(ordered))
    return {
        "dates": len(ordered),
        "median": statistics.median(ordered),
        "p95": ordered[rank - 1],
        "max": ordered[-1],
    }


def rain_line_json(line: RainLine) -> dict[str, object]:
    """One output line (§13.3) for a rain (s, L)."""
    y = _observed(line)
    wet = line.outcome == "rain_nearby"
    control_cost: float | None = None
    control_loss: float | None = None
    if line.control is not None and line.outcome != "unknown":
        if line.control.symbol is not None:
            control_cost = symbol_cost(
                line.control.symbol, wet, miss_weight=RAIN_MISS_WEIGHT
            )
        if line.control.amount is not None and y is not None:
            control_loss = amount_loss(line.control.amount, y, under_weight=W_AMOUNT)
    served_loss: float | None = None
    if line.served is not None and line.served.amount is not None and y is not None:
        served_loss = amount_loss(line.served.amount, y, under_weight=W_AMOUNT)
    selection = line.selection
    method = None
    if selection is not None:
        method = "none" if selection.no_eligible else "selector"
    fallbacks: list[str] = []
    if line.no_eligible:
        fallbacks.append("no_eligible")
    if line.clim is not None and line.clim.fallback_reason is not None:
        fallbacks.append(line.clim.fallback_reason)
    return {
        "snapshot_date": line.key.snapshot_date,
        "lead": line.key.lead,
        "target": line.key.target,
        "unmatched": line.unmatched,
        "control": line.control,
        "control_symbol_cost": control_cost,
        "control_amount_loss": control_loss,
        "f1_ok": line.f1,
        "event": line.day.event,
        "observed_amount": line.day.amount,
        "amount_method": list(daily_method.AMOUNT_METHOD),
        "station_days": line.day.stations,
        "high_reading_flags": line.day.flags,
        "method": method,
        "trigger_hundredths": daily_method.RAIN_TRIGGER_HUNDREDTHS,
        "decision_time": line.decision_time,
        "threshold_mm": line.threshold_mm,
        "training_window": line.window,
        "training_days": line.training_days,
        "training_asof_differs": line.asof_differs,
        "incumbent": line.incumbent,
        "incumbent_row_unrecognised": (
            line.incumbent is not None and line.incumbent.unrecognised_on is not None
        ),
        "selection": selection,
        "known_not_common": None if selection is None else selection.known_not_common,
        "served": line.served,
        "served_amount_loss": served_loss,
        "samples_issued_at": line.issued,
        "clim": line.clim,
        "symbols": line.symbols,
        "amounts": line.amounts,
        "fallback_reasons": fallbacks,
        "control_covered_no_eligible": (
            line.no_eligible
            and line.control is not None
            and line.control.symbol is not None
        ),
    }


def trigger_grid_json(lines: Sequence[RainLine]) -> list[dict[str, object]]:
    """A15's trigger grid and own-days ranking per case (§9.3)."""
    out: list[dict[str, object]] = []
    for line in lines:
        if line.selection is None:
            continue
        wet = line.outcome == "rain_nearby"
        grid: list[dict[str, object]] = []
        for ranking in line.grid:
            pick = ranking.s_pick.symbol
            cost = (
                None
                if pick is None or line.outcome == "unknown"
                else symbol_cost(pick, wet, miss_weight=RAIN_MISS_WEIGHT)
            )
            grid.append(
                {
                    "label": ranking.label,
                    "best": ranking.selection.best,
                    "ranked": [
                        [r.key, r.trigger_hundredths, r.misses, r.false_alarms]
                        for r in ranking.selection.ranked
                    ],
                    "s_pick": pick,
                    "s_pick_cost": cost,
                }
            )
        own = line.own_days
        out.append(
            {
                "snapshot_date": line.key.snapshot_date,
                "lead": line.key.lead,
                "target": line.key.target,
                "event": line.outcome,
                "grid": grid,
                "own_days": None
                if own is None
                else {
                    "ranking": own.ranking,
                    "best": own.best,
                    "same_best": own.same_best,
                    "group": own.group,
                    "group_size": len(own.group),
                    "served_group": own.served_group,
                    "served_group_size": len(own.served_group),
                    "shared_members": own.shared_members,
                },
            }
        )
    return out


def own_days_summary(lines: Sequence[RainLine]) -> dict[str, dict[str, int]]:
    """Per lead: how often the own-days and served best-score models agree."""
    out: dict[str, dict[str, int]] = {}
    for lead in RECORD_LEADS:
        same = differ = undefined = 0
        for line in lines:
            if line.key.lead != lead or line.own_days is None:
                continue
            match = line.own_days.same_best
            if match is None:
                undefined += 1
            elif match:
                same += 1
            else:
                differ += 1
        out[str(lead)] = {
            "same_best": same,
            "different_best": differ,
            "undefined": undefined,
        }
    return out


# --- Rain station-day outputs (§6.7, §13.3) ---------------------------------------


def station_days_lines(
    store: StationDayStore, events: RainEvents
) -> list[dict[str, object]]:
    """One line per (station, local day), built from the copy in full."""
    states = events.full_states
    out: list[dict[str, object]] = []
    for station in store.stations:
        for day in store.days_of(station):
            total = store.full(station, day)
            if total is None:
                continue
            flags = events.full(day).flags
            marks: list[str] = []
            if flags.large_total == station:
                marks.append("large_total")
            if flags.lone_reading == station:
                marks.append("lone_reading")
            qualifying = states.qualifying.get((station, day))
            out.append(
                {
                    "station": station,
                    "day": day,
                    "total": total.total,
                    "hundredths": total.hundredths,
                    "hours": total.hours,
                    "expected_hours": total.expected_hours,
                    "problems": total.problems,
                    "correction": total.correction,
                    "invalid": total.invalid,
                    "after_gap": total.after_gap,
                    "flags": marks,
                    "ratio": None if qualifying is None else qualifying.ratio,
                    "qualifying": qualifying,
                    "state": states.states.get((station, day), "ok"),
                }
            )
    return out


def fault_states_json(store: StationDayStore, events: RainEvents) -> dict[str, object]:
    """The fault-state timeline, with the rule and counts behind each change."""
    return {
        "changes": events.full_states.log,
        "late_fetches": store.late_fetches(),
        "stations": store.stations,
    }


# --- T2 (A4): the stations against the group truth (§6.3) ----------------------


@dataclass(frozen=True)
class TruthRainDay:
    """One eligible, non-stale published rain day, with its UTC window."""

    day: date
    occurrence: bool
    total: float | None
    threshold_mm: float
    start: datetime
    end: datetime


def load_truth_rain_days(
    conn: sqlite3.Connection, *, site: Site, last_day: date
) -> list[TruthRainDay]:
    """T2's days: eligible, non-stale published occurrence rows up to ``last_day``."""
    rows = conn.execute(
        """
        SELECT quantity, local_date, value, rain_threshold_mm, day_start_utc,
               day_end_utc
        FROM daily_truth
        WHERE site_id = ? AND tz_generation_id = ? AND eligible = 1 AND stale = 0
          AND quantity IN ('precip_occurrence', 'precip_total') AND local_date <= ?
        """,
        (site.site_id, site.generation_id, last_day.isoformat()),
    ).fetchall()
    totals: dict[date, float | None] = {}
    found: dict[date, sqlite3.Row] = {}
    for r in rows:
        day = date.fromisoformat(str(r["local_date"]))
        if str(r["quantity"]) == "precip_total":
            totals[day] = None if r["value"] is None else float(r["value"])
        else:
            found[day] = r
    out: list[TruthRainDay] = []
    for day in sorted(found):
        r = found[day]
        if r["value"] is None:
            continue
        bounds = local_day_bounds(day, site.timezone)
        start = bounds.start_utc
        end = bounds.end_utc
        if r["day_start_utc"] is not None and r["day_end_utc"] is not None:
            start = parse_utc(str(r["day_start_utc"]))
            end = parse_utc(str(r["day_end_utc"]))
        out.append(
            TruthRainDay(
                day=day,
                occurrence=float(r["value"]) >= 0.5,
                total=totals.get(day),
                threshold_mm=(
                    site.rain_threshold_mm
                    if r["rain_threshold_mm"] is None
                    else float(r["rain_threshold_mm"])
                ),
                start=start,
                end=end,
            )
        )
    return out


def t2_report(
    conn: sqlite3.Connection,
    *,
    site: Site,
    stations: Sequence[StationRow],
    last_day: date,
) -> dict[str, object]:
    """T2 (A4): per station and day, a wet hour and the total against the truth."""
    days = load_truth_rain_days(conn, site=site, last_day=last_day)
    by_day = {d.day: d for d in days}
    readings: dict[tuple[str, date], list[float]] = {}
    for row in stations:
        info = by_day.get(row.local_date)
        if info is None:
            continue
        hour = parse_utc(row.valid_at)
        if not info.start <= hour < info.end:
            continue
        readings.setdefault((row.pws_station_id, info.day), []).append(row.value)
    per_station: dict[str, dict[str, object]] = {}
    station_ids = sorted({s for s, _ in readings})
    local_only = local_only_half = 0
    for info in days:
        reporting = [s for s in station_ids if (s, info.day) in readings]
        wet = [
            s
            for s in reporting
            if any(v >= info.threshold_mm for v in readings[(s, info.day)])
        ]
        if not info.occurrence and wet:
            local_only += 1
            if 2 * len(wet) >= len(reporting):
                local_only_half += 1
    for station in station_ids:
        counts = {"days": 0, "both_wet": 0, "station_only_wet": 0, "truth_only_wet": 0}
        counts["both_dry"] = 0
        diffs: list[float] = []
        for info in days:
            values = readings.get((station, info.day))
            if values is None:
                continue
            counts["days"] += 1
            wet = any(v >= info.threshold_mm for v in values)
            if wet and info.occurrence:
                counts["both_wet"] += 1
            elif wet:
                counts["station_only_wet"] += 1
            elif info.occurrence:
                counts["truth_only_wet"] += 1
            else:
                counts["both_dry"] += 1
            if info.total is not None:
                diffs.append(math.fsum(values) - info.total)
        per_station[station] = {**counts, "mean_total_difference": _mean(diffs)}
    rejected_wet = rejected_dry = hours = 0
    for r in conn.execute(
        """
        SELECT valid_at, value, rejected_stations FROM observations
        WHERE site_id = ? AND variable = 'precip'
        """,
        (site.site_id,),
    ).fetchall():
        hour = parse_utc(str(r["valid_at"]))
        info = by_day.get(hour.astimezone(ZoneInfo(site.timezone)).date())
        if info is None or not info.start <= hour < info.end or r["value"] is None:
            continue
        hours += 1
        rejected = int(r["rejected_stations"] or 0)
        if float(r["value"]) >= info.threshold_mm:
            rejected_wet += rejected
        else:
            rejected_dry += rejected
    return {
        "last_day": last_day,
        "days": len(days),
        "truth_wet_days": sum(1 for d in days if d.occurrence),
        "truth_dry_days": sum(1 for d in days if not d.occurrence),
        "local_only_wet_days": local_only,
        "local_only_wet_days_half_or_more": local_only_half,
        "consensus_hours": hours,
        "rejected_on_wet_hours": rejected_wet,
        "rejected_on_dry_hours": rejected_dry,
        "stations": per_station,
    }


# --- The house station (§6.4) -------------------------------------------------------


@dataclass(frozen=True)
class HouseStation:
    """The named house station's ``ok`` readings, read in full."""

    pws_station_id: str
    in_station_set: bool
    enabled: bool | None
    station_id: int | None
    dem_elevation_m: float | None
    readings: tuple[tuple[str, float], ...]


def load_house_station(
    conn: sqlite3.Connection, *, site: Site, pws_station_id: str, variable: str
) -> HouseStation:
    """The house station's readings, whatever its ``enabled`` value."""
    row = conn.execute(
        "SELECT id, enabled, dem_elevation_m FROM stations "
        "WHERE site_id = ? AND pws_station_id = ?",
        (site.site_id, pws_station_id),
    ).fetchone()
    if row is None:
        return HouseStation(pws_station_id, False, None, None, None, ())
    readings = conn.execute(
        """
        SELECT valid_at, value FROM station_observations
        WHERE station_id = ? AND variable = ? AND qc_flag = 'ok'
        ORDER BY valid_at
        """,
        (int(row["id"]), variable),
    ).fetchall()
    return HouseStation(
        pws_station_id=pws_station_id,
        in_station_set=True,
        enabled=bool(row["enabled"]),
        station_id=int(row["id"]),
        dem_elevation_m=float(row["dem_elevation_m"]),
        readings=tuple((str(r["valid_at"]), float(r["value"])) for r in readings),
    )


class HouseDaily:
    """The house station's daily values, through production's gates (§6.4)."""

    def __init__(self, house: HouseStation, *, site: Site, variable: str) -> None:
        self._house = house
        self._site = site
        self._variable = variable
        tz = ZoneInfo(site.timezone)
        self._by_day: dict[date, list[tuple[str, float]]] = {}
        for valid_at, value in house.readings:
            day = parse_utc(valid_at).astimezone(tz).date()
            self._by_day.setdefault(day, []).append((valid_at, value))
        self._cache: dict[tuple[date, float], dict[str, QuantityOutcome]] = {}

    def readings_on(self, days: Iterable[date]) -> int:
        """The ``ok`` readings on the given local days."""
        return sum(len(self._by_day.get(day, ())) for day in set(days))

    def outcomes(self, day: date, *, threshold: float) -> dict[str, QuantityOutcome]:
        """The day's quantities, by ``evaluate_variable``."""
        key = (day, threshold)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        samples: list[tuple[str, float]] = []
        house = self._house
        for valid_at, value in sorted(self._by_day.get(day, [])):
            if self._variable == "temperature":
                assert (
                    house.station_id is not None and house.dem_elevation_m is not None
                )
                result = compute_consensus(
                    [
                        StationReading(
                            station_id=house.station_id,
                            dem_elevation_m=house.dem_elevation_m,
                            variable=self._variable,
                            valid_at=valid_at,
                            value=value,
                        )
                    ],
                    variable=self._variable,
                    site_elevation_m=self._site.elevation_m,
                    mad_floor=MAD_FLOORS[self._variable],
                )
                if result is None:
                    continue
                samples.append((valid_at, result.value))
            else:
                samples.append((valid_at, value))
        out = {
            o.quantity: o
            for o in evaluate_variable(
                self._variable,
                samples,
                timezone=self._site.timezone,
                local_date=day,
                rain_threshold_mm=threshold,
            )
        }
        self._cache[key] = out
        return out

    def value(self, day: date, quantity: str, *, threshold: float) -> float | None:
        """The day's eligible value of ``quantity``, else None."""
        outcome = self.outcomes(day, threshold=threshold).get(quantity)
        if outcome is None or not outcome.eligible:
            return None
        return outcome.value


def house_unusable(
    house: HouseStation, daily: HouseDaily, *, days: Sequence[date], eligible: int
) -> dict[str, object] | None:
    """The no-usable-readings reason (§6.4), or None when a value counts."""
    readings = daily.readings_on(days)
    if not house.in_station_set:
        reason = "not_in_station_set"
    elif readings == 0:
        reason = "no_ok_readings"
    elif eligible == 0:
        reason = "no_eligible_day"
    else:
        return None
    return {"reason": reason, "readings": readings, "days": len(set(days))}


def house_rain_report(
    house: HouseStation, lines: Sequence[RainLine], *, site: Site
) -> dict[str, object]:
    """``house_station.json`` for rain: point values only, never deciding."""
    daily = HouseDaily(house, site=site, variable="precip")
    per_lead: dict[str, dict[str, object]] = {}
    eligible_cases = 0
    for lead in SCORED_LEADS:
        costs: dict[str, list[float]] = {"control": [], "S-Q": []}
        briers: list[float] = []
        losses: dict[str, list[float]] = {"control": [], "served": []}
        for line in lines:
            if line.key.lead != lead or line.control is None:
                continue
            threshold = line.threshold_mm or site.rain_threshold_mm
            occurrence = daily.value(
                line.key.target, "precip_occurrence", threshold=threshold
            )
            if occurrence is None:
                continue
            eligible_cases += 1
            wet = occurrence >= 0.5
            for name, value in (
                ("control", line.control.symbol),
                ("S-Q", None if line.served is None else line.served.symbol),
            ):
                if value is not None:
                    costs[name].append(
                        symbol_cost(value, wet, miss_weight=RAIN_MISS_WEIGHT)
                    )
            if line.served is not None and line.served.chance is not None:
                briers.append(_brier(float(line.served.chance), wet))
            total = daily.value(line.key.target, "precip_total", threshold=threshold)
            if wet and total is not None:
                for name, value in (
                    ("control", line.control.amount),
                    ("served", None if line.served is None else line.served.amount),
                ):
                    if value is not None:
                        losses[name].append(
                            amount_loss(value, total, under_weight=W_AMOUNT)
                        )
        per_lead[str(lead)] = {
            "symbol_cost": {k: _mean(v) for k, v in costs.items()},
            "symbol_cases": {k: len(v) for k, v in costs.items()},
            "chq_brier": _mean(briers),
            "amount_loss": {k: _mean(v) for k, v in losses.items()},
            "amount_cases": {k: len(v) for k, v in losses.items()},
        }
    agreement = {"both_wet": 0, "both_dry": 0, "house_only_wet": 0, "group_only_wet": 0}
    diffs: list[float] = []
    seen: set[date] = set()
    for line in sorted(lines, key=lambda x: (x.key.target, x.key.lead)):
        target = line.key.target
        if target in seen or line.outcome == "unknown":
            continue
        threshold = line.threshold_mm or site.rain_threshold_mm
        occurrence = daily.value(target, "precip_occurrence", threshold=threshold)
        if occurrence is None:
            continue
        seen.add(target)
        house_wet = occurrence >= 0.5
        group_wet = line.outcome == "rain_nearby"
        if house_wet and group_wet:
            agreement["both_wet"] += 1
        elif house_wet:
            agreement["house_only_wet"] += 1
        elif group_wet:
            agreement["group_only_wet"] += 1
        else:
            agreement["both_dry"] += 1
        total = daily.value(target, "precip_total", threshold=threshold)
        observed = line.day.amount.amount
        if total is not None and observed is not None:
            diffs.append(total - observed)
    targets = [line.key.target for line in lines]
    return {
        "variable": "precip",
        "house_station": house.pws_station_id,
        "enabled": house.enabled,
        "unusable": house_unusable(house, daily, days=targets, eligible=eligible_cases),
        "per_lead": per_lead,
        "agreement": {**agreement, "mean_total_difference": _mean(diffs)},
        "point_values_only": True,
    }


# --- Rain's settings, as recorded in its test header (§10.2) ----------------------


def rain_rules_record(reads: Sequence[RainRuleRead]) -> list[dict[str, object]]:
    """The version map as rain's header records it (§10.2).

    Version order, and exactly the keys ``version`` (dotted), ``rule``,
    ``release_commit``, ``read_to`` and ``other_heads`` (sorted).
    """
    return [
        {
            "version": _version_text(read.version),
            "rule": read.rule,
            "release_commit": read.release_commit,
            "read_to": read.read_to,
            "other_heads": sorted(read.other_heads),
        }
        for read in sorted(reads, key=lambda r: r.version)
    ]


def rain_settings() -> dict[str, object]:
    """The rain settings the pre-deployment test runs with."""
    return {
        "event": {
            "pair_hundredths": RAIN_PAIR_HUNDREDTHS,
            "single_hundredths": RAIN_SINGLE_HUNDREDTHS,
            "adequate_hours": RAIN_ADEQUATE_HOURS,
            "min_dry_stations": RAIN_MIN_DRY_STATIONS,
        },
        "fault_check": {
            "wet_line_hundredths": FAULT_WET_LINE_HUNDREDTHS,
            "dry_line_hundredths": FAULT_DRY_LINE_HUNDREDTHS,
            "min_peers": FAULT_MIN_PEERS,
            "suspect": [SUSPECT_LOOKBACK_DAYS, SUSPECT_MIN_DAYS, SUSPECT_RATIO_BELOW],
            "quarantine": [
                QUARANTINE_LOOKBACK_DAYS,
                QUARANTINE_MIN_DAYS,
                QUARANTINE_MIN_EPISODES,
                QUARANTINE_RATIO_AT_MOST,
                QUARANTINE_DRY_SHARE,
            ],
            "recovery": [
                RECOVERY_LOOKBACK_DAYS,
                RECOVERY_MIN_DAYS,
                RECOVERY_MIN_EPISODES,
                RECOVERY_RATIO_AT_LEAST,
                RECOVERY_MAX_DRY_DAYS,
            ],
        },
        "flags": {
            "large_total_factor": LARGE_TOTAL_FACTOR,
            "reads_high_ratio": READS_HIGH_RATIO,
            "reads_high_block_days": READS_HIGH_BLOCK_DAYS,
            "gap_hours": GAP_HOURS,
            "late_fetch_hours": LATE_FETCH_HOURS,
        },
        "amount_method": list(daily_method.AMOUNT_METHOD),
        "window_days": RAIN_WINDOW_DAYS,
        "min_days": RAIN_MIN_DAYS,
        "miss_weight": RAIN_MISS_WEIGHT,
        "switch_margin": RAIN_SWITCH_MARGIN,
        "incumbent_lookback_dates": daily_method.INCUMBENT_LOOKBACK_DATES,
        "trigger_hundredths": daily_method.RAIN_TRIGGER_HUNDREDTHS,
        "trigger_grid_hundredths": list(daily_method.TRIGGER_GRID_HUNDREDTHS),
        "group_fraction": GROUP_FRACTION,
        "w_amount": W_AMOUNT,
        "clim_min_days": CLIM_MIN_DAYS,
        "v_b": None if V_B is None else _version_text(V_B),
        "wet_hours_first": _version_text(WET_HOURS_FIRST),
        "rain_rules": rain_rules_record(RAIN_RULE_READS),
        "levels": {
            "chance": LEVEL_CHANCE,
            "symbol": LEVEL_SYMBOL,
            "amount": LEVEL_AMOUNT,
        },
    }


OPEN = "open"
"""An owner answer not given yet (§22); never filled in by default."""


def rain_owner_answers() -> dict[str, object]:
    """The owner's answers that rain uses, copied from §22 (§10.2).

    Written as the owner gave them, not read from the settings, so the header
    shows any difference between the two. An answer not given yet (D7's
    chance part, D13, D14) is the string ``"open"``; no calculation reads it.
    """
    return {
        "D1": {"decided": "2026-09-29", "w_amount": 1.5},
        "D3": {"decided": "2026-09-29", "w_r": 2, "p_star": "1/3"},
        "D5": {
            "decided": "2026-09-29; the truth decided again for rain on 2026-10-06",
            "truth": "the rain-nearby event (§6.6); the target stays the house",
            "rule": (
                "A day is rain nearby when at least two validated stations each "
                "record at least 0.2 mm, or at least one records at least 0.5 mm. "
                "It is dry when that rule is not met and at least one validated "
                "station has at least 22 hourly readings that day. With no such "
                "station it is unknown, and an unknown day is never scored."
            ),
            "partial_coverage": (
                "A validated station with fewer than 22 hourly readings still "
                "counts toward rain nearby; it never makes a day dry, and it never "
                "enters the amount."
            ),
            "observed_amount": (
                "On a rain-nearby day, the mean of the two largest totals among "
                "the validated stations with adequate coverage and 0.2 mm or "
                "more, or the one total when only one station contributes; when "
                "every wet station has only partial coverage, amount_not_available. "
                "On a dry day 0 mm; on an unknown day none."
            ),
            "flagged_lone_readings": (
                "A high or lone reading that no other station confirms counts in "
                "full, in the event, the amount and the ranking, and is flagged "
                "large_total or lone_reading."
            ),
            "not_adopted": (
                "Holding out a high reading, or a station, because its neighbours "
                "disagree or stayed dry."
            ),
            "house_station": "c: a report-only comparison, for rain too (§6.4)",
            "change_after_freeze": (
                "b, with the interrupted run recorded INCONCLUSIVE; it applies to "
                "temperature's verdict: rain has no freeze and no verdict"
            ),
        },
        "D6": {
            "decided": "2026-10-06; the gap question on 2026-10-07",
            "option": "D, the selector",
            "method": (
                "The lead-matched window; the score, (2 · misses + false alarms) "
                "per common day; the group, the best ⌈0.75 · J⌉ models of the "
                "score order; the symbol, shown when at least 1/3 of the group "
                "votes rain; the chance, the vote share at 0.2 mm, shown only if "
                "it passes its test against R-clim; and the selected model's own "
                "amount, hours and hourly line. The ranking uses the same 0.2 mm "
                "trigger; the trigger grid is reported by A15 only."
            ),
            "window_days": 60,
            "window_retest": "after the winter (A17, report only)",
            "minimum": (
                "On when at least two models that cover the target have 20 or "
                "more counted days at the lead: a model is then eligible with at "
                "least 20 counted days, and N = 20. Otherwise every model with a "
                "counted day is eligible, N = min(20, the largest number of "
                "counted days), and the ranking is flagged provisional."
            ),
            "switch": (
                "The incumbent is replaced by the best-score model when incumbent "
                "total − best-score total ≥ 3 on the same common days, and kept "
                "when the difference is 2 or less; it is replaced at once if it has "
                "no exact-cover forecast for the target or is no longer eligible."
            ),
            "todays_method": (
                "Never served for rain; with no ranked model (J = 0) that "
                "day-ahead has no rain forecast, recorded no_eligible."
            ),
            "fresh_install": "availability comes from the data, not a timer",
            "gap": (
                "The model selected most recently at the lead, within the 60 "
                "previous snapshot dates, stays the incumbent across dates that "
                "decided nothing, while it still qualifies. A date recorded with "
                "no_eligible clears it, and so does a row written under the "
                "rollback (current). A record row of a shape the add-on does not "
                "recognise also ends it, and is flagged."
            ),
        },
        "D7": {
            "small_amounts": {
                "decided": "2026-10-06",
                "rule": (
                    "The tile shows the selected model's daily total exactly as "
                    "calculated, one decimal ('%.1f' mm). There is no cut-off, no "
                    "special label for small amounts and no zeroing. The stored "
                    "and scored value is the same unrounded total."
                ),
            },
            "chance": OPEN,
        },
        "D13": OPEN,
        "D14": OPEN,
    }


# --- A8 and A10: the meteoblue lists and the coverage counts (§10.4, §10.5) ------


def meteoblue_lists(
    conn: sqlite3.Connection, *, site: Site, variable: str
) -> dict[str, object]:
    """A8 for one variable at the site: names and counts only (§10.5, §12).

    ``members``: each meteoblue member model with samples, and its sample
    count. ``multimodel_samples``: the samples stored under the model
    ``multimodel``. ``multimodel_mean``: the published ``_multimodel_mean``
    rows, and the feeds its grouping averages. The grouping is
    ``scoring/multimodel.py``'s own (the same filters, GROUP BY and
    ``HAVING COUNT(*) >= 2``), run read-only with each group's feed ids
    collected; ``contributors`` counts the groups each feed is in.
    """
    members: dict[str, int] = {}
    multimodel_samples = 0
    for row in conn.execute(
        """
        SELECT f.model AS model, COUNT(*) AS samples
        FROM feeds f
        JOIN forecast_samples fs ON fs.feed_id = f.id
        WHERE f.source = 'meteoblue' AND fs.site_id = ? AND fs.variable = ?
        GROUP BY f.id, f.model
        """,
        (site.site_id, variable),
    ):
        model, samples = str(row["model"]), int(row["samples"])
        if model == "multimodel":
            multimodel_samples += samples
        else:
            members[model] = samples
    mean_row = conn.execute(
        f"""
        SELECT COUNT(*) FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        WHERE f.source = 'virtual' AND f.model = '_multimodel_mean'
          AND fp.site_id = ? AND fp.variable = ?
          AND {published_generation_clause("fp")}
        """,
        (site.site_id, variable),
    ).fetchone()
    names = {
        int(row["id"]): f"{row['source']}/{row['model']}"
        for row in conn.execute("SELECT id, source, model FROM feeds")
    }
    contributors: dict[str, int] = {}
    groups = 0
    for row in conn.execute(
        f"""
        SELECT GROUP_CONCAT(fp.feed_id) AS feed_ids
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        JOIN sites s ON s.id = fp.site_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE f.is_virtual = 0
          AND {active_competitor_clause(site_expr="fp.site_id")}
          AND {published_generation_clause("fp")}
          AND fp.site_id = ? AND fp.variable = ?
        GROUP BY fp.site_id, fp.variable, fp.issued_at, fp.valid_at, fp.lead_hours,
                 fp.day_ahead, fp.observed, s.rain_threshold_mm
        HAVING COUNT(*) >= 2
        """,
        (site.site_id, variable),
    ):
        groups += 1
        for feed_id in str(row["feed_ids"]).split(","):
            name = names.get(int(feed_id), f"feed {feed_id}")
            contributors[name] = contributors.get(name, 0) + 1
    return {
        variable: {
            "members": dict(sorted(members.items())),
            "multimodel_samples": multimodel_samples,
            "multimodel_mean": {
                "rows": 0 if mean_row is None else int(mean_row[0]),
                "groups": groups,
                "contributors": dict(sorted(contributors.items())),
            },
        }
    }


def coverage_counts(
    replays: ReplayCache, rows: Iterable[RecordRow]
) -> list[dict[str, object]]:
    """A10: per snapshot, the feeds whose day covers it, at every lead 0-7 (§10.4).

    One entry per distinct (snapshot date, decision time) among ``rows``,
    sorted. A feed counts when it passes the replay's own check:
    ``covers_local_day`` for temperature, ``covers_local_day_exactly`` for
    rain. Counts only.
    """
    snapshots = sorted({(row.snapshot_date, row.snapshot_utc) for row in rows})
    out: list[dict[str, object]] = []
    for snapshot_date, snapshot_utc in snapshots:
        replay = replays.get(snapshot_date, snapshot_utc)
        out.append(
            {
                "snapshot_date": snapshot_date,
                "snapshot_utc": snapshot_utc,
                "feeds_covering": [
                    sum(1 for feed in replay.feeds(lead).values() if feed.covers)
                    for lead in RECORD_LEADS
                ],
            }
        )
    return out


# --- Shared run helpers (§6.5, §7.4, §8.2) ------------------------------------

LOW_C_NOT_BUILT = (
    "D2 option C (lean cold) needs an over_forecast tie-break that daily_method "
    "does not have; not built"
)
G4_MARGIN_OPTIONS: tuple[float, ...] = (0.0, 5.0, 10.0)


def temperature_feed_cases(
    training: TempTraining,
    row: RecordRow,
    quantity: str,
    by_model: Mapping[daily_method.ModelRef, FeedDayReplay],
) -> list[daily_method.FeedCase]:
    """Each model's value and its training pairs at leads 0-5 and the row's lead."""
    leads = sorted({*SCORED_LEADS, row.lead})
    pairs = {
        lead: training.pairs(row.snapshot_date, row.snapshot_utc, quantity, lead)
        for lead in leads
    }
    return [
        daily_method.FeedCase(
            feed=ref,
            value=_feed_value(feed, quantity),
            training={lead: pairs[lead].get(ref, []) for lead in leads},
        )
        for ref, feed in by_model.items()
    ]


def td_k_recompose(
    ctx: TempContext, training: TempTraining
) -> Callable[[TempLine, int], float | None]:
    """TD-K's value for a line at another K, for :func:`choose_td_k`."""
    cache: dict[tuple[date, int, str], list[daily_method.FeedCase]] = {}

    def recompose(line: TempLine, k: int) -> float | None:
        row = line.row
        control = line.control
        if row is None or control is None:
            return None
        key = (row.snapshot_date, line.key.lead, line.quantity)
        feeds = cache.get(key)
        if feeds is None:
            replay = ctx.replays.get(row.snapshot_date, row.snapshot_utc)
            feeds = temperature_feed_cases(
                training,
                row,
                line.quantity,
                _first_by_model(replay.feeds(line.key.lead)),
            )
            cache[key] = feeds
        under, over = OWNER_PARAMS.weights(line.quantity)
        return daily_method.compose_td_k(
            feeds,
            control=control,
            lead=line.key.lead,
            min_days=TEMP_MIN_DAYS,
            pooled_leads=SCORED_LEADS,
            k=k,
            under_weight=under,
            over_weight=over,
            tie_break=OWNER_PARAMS.tie(line.quantity),
        ).value

    return recompose


def recorded_text_map(rows: Iterable[RecordRow]) -> dict[tuple[date, date], str]:
    """(snapshot date, target date) -> ``snapshot_utc`` for every recorded row."""
    return {
        (row.snapshot_date, row.target_date): row.snapshot_utc
        for row in rows
        if row.status == "recorded"
    }


def temperature_window_days(
    rows: Iterable[RecordRow],
    *,
    timezone: str,
    arms: Sequence[Literal["A", "B"]],
) -> set[date]:
    """Every day inside the training window of any recorded row, per arm."""
    out: set[date] = set()
    for snapshot_utc in {row.snapshot_utc for row in rows if row.status == "recorded"}:
        k_day = latest_knowable_target(timezone, parse_utc(snapshot_utc))
        for arm in arms:
            out.update(_days(*temperature_window(k_day, arm)))
    return out


def rain_in_scope_days(rows: Iterable[RecordRow], *, timezone: str) -> set[date]:
    """Every day inside the 60-day window of any recorded rain row."""
    out: set[date] = set()
    for snapshot_utc in {row.snapshot_utc for row in rows if row.status == "recorded"}:
        k_day = latest_knowable_target(timezone, parse_utc(snapshot_utc))
        out.update(_days(k_day - timedelta(days=RAIN_WINDOW_DAYS - 1), k_day))
    return out


def param_settings() -> list[tuple[str, TempParams]]:
    """The loss and candidate settings of §8.6's sensitivities (reported only).

    w_high at 1.5, 2 and 3; the low leaning warm (D2 option B, the
    under-forecast weight w and the under-forecast tie-break) at the same
    weights; the shrinkage at 5 and 20. D2 option C is not built.
    """
    out = [(f"w_high={w:g}", TempParams(w_high=w)) for w in W_HIGH_SENSITIVITIES]
    out += [
        (f"low=B,w={w:g}", TempParams(low_under=w, low_tie="under_forecast"))
        for w in W_HIGH_SENSITIVITIES
    ]
    out += [(f"shrink={s:g}", TempParams(shrink=s)) for s in BC_SHRINK_SENSITIVITIES]
    return out


@dataclass(frozen=True)
class HeaderFacts:
    """The station set and the import record, against the note's (§6.5)."""

    station_set: list[StationEntry]
    station_set_changed: dict[str, list[str]] | None
    db_import: dict[str, str] | None
    db_import_changed: bool | None

    def as_header(self) -> dict[str, object]:
        """The four header keys."""
        return {
            "station_set": self.station_set,
            "station_set_changed": self.station_set_changed,
            "db_import": self.db_import,
            "db_import_changed": self.db_import_changed,
        }


def header_facts(
    conn: sqlite3.Connection, *, site: Site, note: FreezeNote | None
) -> HeaderFacts:
    """The copy's station set and import record; the changes are null without a note."""
    station_set = load_station_set(conn, site_id=site.site_id)
    db_import = load_db_import(conn)
    if note is None:
        return HeaderFacts(station_set, None, db_import, None)
    noted_import = None if note.db_import is None else dict(note.db_import)
    return HeaderFacts(
        station_set,
        station_set_changes(station_set, note.station_set),
        db_import,
        db_import != noted_import,
    )


def _site_json(site: Site) -> dict[str, object]:
    return {"id": site.site_id, "name": site.name, "enabled": site.enabled}


def _error_summary(
    errors: Sequence[float], losses: Sequence[float]
) -> dict[str, object]:
    return {
        "n": len(errors),
        "mae": _mean([abs(e) for e in errors]),
        "bias": _mean(errors),
        "loss": _mean(losses),
    }


def house_temperature_report(
    house: HouseStation,
    lines: Sequence[TempLine],
    *,
    site: Site,
    names: Sequence[str],
) -> dict[str, object]:
    """``house_station.json`` for temperature: point values only, never deciding."""
    daily = HouseDaily(house, site=site, variable="temperature")
    threshold = site.rain_threshold_mm
    per_lead: dict[str, dict[str, object]] = {}
    agreement: dict[str, object] = {}
    unusable: dict[str, object] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        mine = [line for line in lines if line.matched and line.quantity == quantity]
        targets = sorted({line.key.target for line in mine})
        house_values: dict[date, float] = {}
        for target in targets:
            value = daily.value(target, quantity, threshold=threshold)
            if value is not None:
                house_values[target] = value
        by_lead: dict[str, object] = {}
        for lead in SCORED_LEADS:
            at_lead = [line for line in mine if line.key.lead == lead]
            entry: dict[str, object] = {}
            for name in ("control", *names):
                errors: list[float] = []
                losses: list[float] = []
                for line in at_lead:
                    observed = house_values.get(line.key.target)
                    value = line.value(name)
                    if observed is None or value is None:
                        continue
                    errors.append(value - observed)
                    losses.append(temperature_loss(quantity, value, observed))
                entry[name] = _error_summary(errors, losses)
            by_lead[str(lead)] = entry
        per_lead[quantity] = by_lead
        truth = {line.key.target: line.truth for line in mine if line.truth is not None}
        diffs = [
            house_values[day] - truth[day]
            for day in targets
            if day in house_values and day in truth
        ]
        agreement[quantity] = {
            "dates": len(diffs),
            "mean_difference": _mean(diffs),
            "days_apart": sum(1 for d in diffs if abs(d) > T1_APART_C),
        }
        unusable[quantity] = house_unusable(
            house, daily, days=targets, eligible=len(house_values)
        )
    return {
        "variable": "temperature",
        "house_station": house.pws_station_id,
        "enabled": house.enabled,
        "unusable": unusable,
        "per_lead": per_lead,
        "agreement": agreement,
        "point_values_only": True,
    }


# --- A6: the temperature trade-off tables (D1, D2, D4) -------------------------


def tradeoff_table(
    lines: Sequence[TempLine], names: Sequence[str], *, params: TempParams
) -> dict[str, object]:
    """Per name, quantity and lead 0-5: under, over, too warm and the loss.

    For a candidate, G4 is also given at each of D4's offered margins m.
    """
    out: dict[str, object] = {}
    for name in names:
        per_q: dict[str, object] = {}
        for quantity in TEMPERATURE_QUANTITIES:
            per_lead: dict[str, object] = {}
            for lead in SCORED_LEADS:
                under: list[float] = []
                over: list[float] = []
                losses: list[float] = []
                warm = 0
                for line in lines:
                    if (
                        not line.scored
                        or line.quantity != quantity
                        or line.key.lead != lead
                        or line.truth is None
                    ):
                        continue
                    value = line.value(name)
                    if value is None:
                        continue
                    under.append(max(line.truth - value, 0.0))
                    over.append(max(value - line.truth, 0.0))
                    losses.append(temperature_loss(quantity, value, line.truth, params))
                    if value - line.truth > G4_BAND_C:
                        warm += 1
                n = len(losses)
                per_lead[str(lead)] = {
                    "n": n,
                    "under": _mean(under),
                    "over": _mean(over),
                    "too_warm_percent": None if n == 0 else 100 * warm / n,
                    "loss": _mean(losses),
                }
            per_q[quantity] = per_lead
        entry: dict[str, object] = {"per_lead": per_q}
        if name in TEMPERATURE_CANDIDATES:
            g4 = cast(
                "Mapping[str, Mapping[str, int]]",
                guard_report(lines, name, params=params)["G4"],
            )
            entry["G4_pass_by_m"] = {
                f"{m:g}": {
                    q: 100 * g["candidate_too_warm"]
                    <= 100 * g["control_too_warm"] + m * g["cases"]
                    for q, g in g4.items()
                }
                for m in G4_MARGIN_OPTIONS
            }
        out[name] = entry
    return out


def temperature_tradeoffs(
    lines: Sequence[TempLine],
    *,
    names: Sequence[str],
    recompute: Callable[[TempParams], list[TempLine]],
) -> dict[str, object]:
    """A6 for temperature: the error spread and a table per offered setting."""
    spread: dict[str, object] = {}
    for quantity in TEMPERATURE_QUANTITIES:
        per_lead: dict[str, object] = {}
        for lead in RECORD_LEADS:
            errors = [
                line.control - line.truth
                for line in lines
                if line.matched
                and line.quantity == quantity
                and line.key.lead == lead
                and line.control is not None
                and line.truth is not None
            ]
            per_lead[str(lead)] = {
                "n": len(errors),
                "stdev": statistics.stdev(errors) if len(errors) >= 2 else None,
            }
        spread[quantity] = per_lead
    tables: dict[str, object] = {
        "owner": {
            "owner_choice": True,
            "settings": OWNER_PARAMS,
            "regime_counts": regime_counts(lines),
            "table": tradeoff_table(lines, names, params=OWNER_PARAMS),
        }
    }
    for label, params in param_settings():
        recomputed = recompute(params)
        tables[label] = {
            "owner_choice": False,
            "settings": params,
            "regime_counts": regime_counts(recomputed),
            "table": tradeoff_table(recomputed, names, params=params),
        }
    tables["low=C"] = {"owner_choice": False, "not_built": LOW_C_NOT_BUILT}
    return {
        "error_spread": spread,
        "owner_choices": {
            "D1_w_high": W_HIGH,
            "D2_low": "A",
            "D4_m": G4_MARGIN_PP,
        },
        "g4_margin_options": list(G4_MARGIN_OPTIONS),
        "tables": tables,
    }


# --- Temperature: the run (§8, §10.2, §11) ---------------------------------------

Cases = list[tuple[CaseKey, RecordRow | None]]

TEMPERATURE_NAMES: tuple[str, ...] = (
    *TEMPERATURE_CANDIDATES,
    "EQ",
    "MB-mean",
    "persistence",
)


def temperature_counts(
    lines: Sequence[TempLine], cases: Cases, f1: F1Tally
) -> dict[str, object]:
    """A1 for one range: record rows, matched dates, warm-spell and frost dates."""
    record_rows: dict[str, dict[str, int]] = {}
    for key, row in cases:
        status = "no_row" if row is None else row.status
        per = record_rows.setdefault(status, {})
        per[str(key.lead)] = per.get(str(key.lead), 0) + 1
    matched = [line for line in lines if line.matched]
    return {
        "record_rows": record_rows,
        "matched_dates_per_lead": {
            q: {
                str(lead): _distinct_dates(
                    [line for line in matched if line.quantity == q], lead
                )
                for lead in RECORD_LEADS
            }
            for q in TEMPERATURE_QUANTITIES
        },
        "warm_spell_dates_per_lead": {
            str(lead): _distinct_dates(
                [
                    line
                    for line in matched
                    if line.quantity == "temperature_high" and line.warm_spell
                ],
                lead,
            )
            for lead in RECORD_LEADS
        },
        "frost_dates_per_lead": {
            str(lead): _distinct_dates(
                [
                    line
                    for line in matched
                    if line.quantity == "temperature_low" and line.frost
                ],
                lead,
            )
            for lead in RECORD_LEADS
        },
        "f1": f1.as_json(),
    }


def _checked_counts_only(header: dict[str, object]) -> dict[str, object]:
    if set(header) != set(COUNTS_ONLY_HEADER_KEYS):
        raise RuntimeError(
            "counts-only header keys differ from the allowlist: "
            f"{sorted(set(header) ^ set(COUNTS_ONLY_HEADER_KEYS))}"
        )
    return header


def _temperature_line_json(line: TempLine, arm: str) -> dict[str, object]:
    return {**temperature_line_json(line), "training_truth_arm": arm}


def _run_temperature(
    conn: sqlite3.Connection,
    *,
    site: Site,
    mode: Mode,
    first: date,
    last: date,
    h_first: date,
    last_complete: date,
    excluded_h_dates: Sequence[date],
    note: FreezeNote | None,
    house_station: str | None,
    failed_dirs: Sequence[str],
    header_base: Mapping[str, object],
) -> dict[str, bytes]:
    rows = load_record_rows(conn, site=site, variable="temperature")
    by_key = {(row.snapshot_date, row.lead): row for row in rows}
    ranges: dict[str, tuple[Cases, int]] = {}
    if mode in ("counts-only", "freeze-prepare"):
        ranges["choosing_set"] = select_cases(
            by_key, first=first, last=last, excluded_h_dates=[]
        )
    if mode in ("counts-only", "heldout"):
        ranges["H"] = select_cases(
            by_key, first=h_first, last=last_complete, excluded_h_dates=excluded_h_dates
        )
    case_rows = [
        row for cases, _ in ranges.values() for _, row in cases if row is not None
    ]
    published = load_published_truth(conn, site=site, quantities=TEMPERATURE_QUANTITIES)
    rebuilder = ArmARebuilder(
        load_station_rows(conn, site=site, variable="temperature"),
        site=site,
        variable="temperature",
    )
    gates = compute_gates(
        rebuilder,
        published,
        in_scope_days=temperature_window_days(
            case_rows, timezone=site.timezone, arms=("A", "B")
        ),
        quantities=TEMPERATURE_QUANTITIES,
        site=site,
        force_arm=None,
    )
    replays = ReplayCache(conn, site=site, variable="temperature")
    ctx = TempContext(
        site=site,
        rows=by_key,
        recorded=recorded_map(rows),
        recorded_text=recorded_text_map(rows),
        replays=replays,
        rebuilder=rebuilder,
        published=published,
        arm=gates.arm,
    )
    training = TempTraining(ctx)
    facts = header_facts(conn, site=site, note=note)
    excluded = set(excluded_h_dates)
    stale = stale_truth_rows(
        published,
        quantities=TEMPERATURE_QUANTITIES,
        date_sets={
            "H_targets": [
                d for d in _days(h_first, last_complete) if d not in excluded
            ],
            "training_window_days": temperature_window_days(
                case_rows, timezone=site.timezone, arms=(gates.arm,)
            ),
            "choosing_set_targets": _days(first, last),
        },
    )
    header: dict[str, object] = {
        "copy": header_base["copy"],
        "manifest": header_base["manifest"],
        "commit_sha": header_base["commit_sha"],
        "gates": gates,
        "arm": gates.arm,
        "stale_truth_rows": stale,
        **facts.as_header(),
        "excluded_h_dates_count": (
            ranges["H"][1] if note is not None and "H" in ranges else None
        ),
    }
    if mode == "counts-only":
        counts: dict[str, object] = {}
        unmatched: dict[str, object] = {}
        coverage: object = None
        for name, (cases, _) in ranges.items():
            f1 = F1Tally()
            lines = temperature_lines(ctx, training, cases, td_k=None, f1=f1)
            counts[name] = temperature_counts(lines, cases, f1)
            unmatched[name] = counts_by_reason(lines).as_json()
            if name == "H":
                coverage = h_training_coverage(lines)
        header.update(
            counts=counts,
            unmatched=unmatched,
            h_training_coverage=coverage,
            rain_event_counts=None,
            meteoblue=meteoblue_lists(conn, site=site, variable="temperature"),
            coverage=coverage_counts(
                replays,
                (
                    row
                    for cases, _ in ranges.values()
                    for _, row in cases
                    if row is not None and row.status == "recorded"
                ),
            ),
        )
        return {"header.json": _dumps(_checked_counts_only(header))}
    header.update(
        format=HEADER_FORMAT, mode=mode, variable="temperature", site=_site_json(site)
    )
    if mode == "freeze-prepare":
        return _temperature_freeze_prepare(
            ctx,
            training,
            ranges["choosing_set"][0],
            header=header,
            facts=facts,
            house_station=house_station,
            failed_dirs=failed_dirs,
        )
    if note is None or note.house_station is None:
        raise ValueError("--heldout needs a freeze note that names the house station")
    return _temperature_heldout(
        conn,
        ctx,
        training,
        ranges["H"][0],
        header=header,
        facts=facts,
        note=note,
        house_station=note.house_station,
    )


def _temperature_freeze_prepare(
    ctx: TempContext,
    training: TempTraining,
    cases: Cases,
    *,
    header: dict[str, object],
    facts: HeaderFacts,
    house_station: str | None,
    failed_dirs: Sequence[str],
) -> dict[str, bytes]:
    f1 = F1Tally()
    lines = temperature_lines(ctx, training, cases, td_k=TD_K_DEFAULT, f1=f1)
    choice = choose_td_k(lines, td_k_recompose(ctx, training))
    k = choice["k"]
    if isinstance(k, bool) or not isinstance(k, int):
        raise TypeError("choose_td_k returned a K that is not an integer")
    if k != TD_K_DEFAULT:
        f1 = F1Tally()
        lines = temperature_lines(ctx, training, cases, td_k=k, f1=f1)
    names = (*TEMPERATURE_NAMES, "control")
    segments, flips = segment_report(split_segments(lines), TEMPERATURE_CANDIDATES)
    regimes = regime_counts(lines)
    block = block_length_check(tile_error_series(lines))

    def recompute(params: TempParams) -> list[TempLine]:
        return temperature_lines(
            ctx, training, cases, td_k=k, f1=F1Tally(), params=params
        )

    tradeoffs = temperature_tradeoffs(lines, names=names, recompute=recompute)
    header.update(
        td_k=choice,
        regime_counts=regimes,
        error_table=error_table(lines, names),
        quantile_table=quantile_table(lines),
        block_length=block,
        data_checks=data_checks(lines, f1=f1),
        segments={"report": segments, "flips": flips},
        tradeoffs=tradeoffs,
        counts={"choosing_set": temperature_counts(lines, cases, f1)},
        unmatched={"choosing_set": counts_by_reason(lines).as_json()},
        h_training_coverage=None,
        replay_time=replay_time_stats(ctx.replays.seconds()),
        not_built=[
            "A3 (T1)",
            "A7",
            "A12",
            "F2",
            "A11: the largest SQLITE_TMPDIR size",
            LOW_C_NOT_BUILT,
        ],
    )
    draft: dict[str, object] = {
        "format": NOTE_FORMAT,
        "draft": True,
        "site": header["site"],
        "v_b": None if V_B is None else _version_text(V_B),
        "first_h_date": DEFAULT_H_FIRST,
        "excluded_h_dates": [],
        "station_set": facts.station_set,
        "db_import": facts.db_import,
        "manifest": header["manifest"],
        "constants": frozen_constants(),
        "house_station": house_station,
        "failed_dirs": list(failed_dirs),
        "td_k": k,
        "td_k_choice": choice,
        "regime_counts": regimes,
        "block_length": block,
        "owner_answers": {
            "D1": {"w_high": W_HIGH},
            "D2": "A",
            "D4": {"m": G4_MARGIN_PP},
        },
        "commit_sha": header["commit_sha"],
        "statement": None,
        "f2": "not_built",
    }
    return {
        "header.json": _dumps(header),
        "lines.jsonl": _dumps_lines(
            _temperature_line_json(line, ctx.arm) for line in lines
        ),
        "freeze_note.draft.json": _dumps(draft),
    }


def _temperature_heldout(
    conn: sqlite3.Connection,
    ctx: TempContext,
    training: TempTraining,
    cases: Cases,
    *,
    header: dict[str, object],
    facts: HeaderFacts,
    note: FreezeNote,
    house_station: str,
) -> dict[str, bytes]:
    f1 = F1Tally()
    lines = temperature_lines(ctx, training, cases, td_k=note.td_k, f1=f1)
    dates = sorted({key.target for key, _ in cases})
    index_lists = draw_resamples(len(dates))
    checks = data_checks(lines, f1=f1)
    segments, flips = segment_report(split_segments(lines), TEMPERATURE_CANDIDATES)

    def tile(
        sample: Sequence[TempLine],
        name: str,
        against: str,
        lists: Sequence[Sequence[int]],
        params: TempParams = OWNER_PARAMS,
    ) -> tuple[Bounds, int]:
        deltas, missing = case_deltas(sample, name, against, params=params)
        bounds = daily_bounds(
            deltas,
            lists,
            dates=dates,
            quantity_weights=TILE_WEIGHTS,
            lead_weights=LEAD_WEIGHTS,
            level=LEVEL_TILE,
        )
        return bounds, missing

    outcomes: dict[str, CandidateOutcome] = {}
    detail: dict[str, object] = {}
    for name in TEMPERATURE_CANDIDATES:
        bounds, missing = tile(lines, name, "control", index_lists)
        guards = guard_report(lines, name)
        twin_name = TWINS.get(name)
        twin: Bounds | None = None
        twin_missing = 0
        if twin_name is not None:
            twin, twin_missing = tile(lines, name, twin_name, index_lists)
        outcome = candidate_outcome(
            name,
            tile=bounds,
            missing=missing,
            guards=guards,
            twin=twin,
            twin_missing=twin_missing,
            checks=checks,
            flip=flips[name],
            station_set_changed=facts.station_set_changed,
            db_import_changed=facts.db_import_changed,
        )
        outcomes[name] = outcome
        detail[name] = {
            "outcome": outcome,
            "missing": missing,
            "guards": guards,
            "twin": twin_name,
            "twin_bounds": twin,
        }

    def head_to_head(challenger: str, incumbent: str) -> Bounds:
        return tile(lines, challenger, incumbent, index_lists)[0]

    winner, trail = choose_winner(outcomes, head_to_head)
    verdict = tile_verdict(outcomes, winner)

    def points(
        sample: Sequence[TempLine], params: TempParams = OWNER_PARAMS
    ) -> dict[str, float | None]:
        return {
            name: tile(sample, name, "control", [], params)[0].point
            for name in TEMPERATURE_CANDIDATES
        }

    sensitivities: dict[str, object] = {
        "on_time_rows_only": points(
            [
                line
                for line in lines
                if line.row is not None and line.row.write_path == "on_time"
            ]
        )
    }
    for label, params in param_settings():
        recomputed = temperature_lines(
            ctx, training, cases, td_k=note.td_k, f1=F1Tally(), params=params
        )
        sensitivities[label] = points(recomputed, params)
    sensitivities["low=C"] = {"not_built": LOW_C_NOT_BUILT}
    if ctx.arm == "A":
        ctx_b = TempContext(
            site=ctx.site,
            rows=ctx.rows,
            recorded=ctx.recorded,
            recorded_text=ctx.recorded_text,
            replays=ctx.replays,
            rebuilder=ctx.rebuilder,
            published=ctx.published,
            arm="B",
        )
        sensitivities["arm_B"] = points(
            temperature_lines(
                ctx_b, TempTraining(ctx_b), cases, td_k=note.td_k, f1=F1Tally()
            )
        )
    for length in SENSITIVITY_BLOCK_LENGTHS:
        if length == BLOCK_LENGTH:
            continue
        lists = draw_resamples(len(dates), block_length=length)
        sensitivities[f"block_length={length}"] = {
            name: tile(lines, name, "control", lists)[0]
            for name in TEMPERATURE_CANDIDATES
        }
    discarded: dict[str, object] = {
        name: {
            "kept": o.tile.kept,
            "discarded": o.tile.discarded,
            "discard_ok": discard_ok(o.tile),
        }
        for name, o in outcomes.items()
    }
    header.update(
        td_k=note.td_k,
        data_checks=checks,
        candidates=detail,
        winner=winner,
        head_to_head=trail,
        verdict=verdict,
        segments={"report": segments, "flips": flips},
        sensitivities=sensitivities,
        error_table=error_table(lines, (*TEMPERATURE_NAMES, "control")),
        quantile_table=quantile_table(lines),
        regime_counts=regime_counts(lines),
        discarded_resamples=discarded,
        counts={"H": temperature_counts(lines, cases, f1)},
        unmatched={"H": counts_by_reason(lines).as_json()},
        h_training_coverage=h_training_coverage(lines),
        replay_time=replay_time_stats(ctx.replays.seconds()),
        not_built=["A11: the largest SQLITE_TMPDIR size", LOW_C_NOT_BUILT],
    )
    house = load_house_station(
        conn, site=ctx.site, pws_station_id=house_station, variable="temperature"
    )
    return {
        "header.json": _dumps(header),
        "lines.jsonl": _dumps_lines(
            _temperature_line_json(line, ctx.arm) for line in lines
        ),
        "house_station.json": _dumps(
            house_temperature_report(
                house, lines, site=ctx.site, names=TEMPERATURE_NAMES
            )
        ),
    }


# --- Rain: the run (§9, §10.2) ------------------------------------------------------


def rain_s0(conn: sqlite3.Connection, *, site: Site) -> date:
    """S0: the first snapshot date of the published generation's rain rows."""
    row = conn.execute(
        """
        SELECT MIN(snapshot_local_date) FROM forecast_of_record
        WHERE site_id = ? AND tz_generation_id = ? AND variable = 'precip'
        """,
        (site.site_id, site.generation_id),
    ).fetchone()
    if row is None or row[0] is None:
        raise LookupError(f"site {site.site_id} has no rain record rows")
    return date.fromisoformat(str(row[0]))


def _run_rain(
    conn: sqlite3.Connection,
    *,
    site: Site,
    mode: Mode,
    first: date,
    last: date,
    t2_last: date,
    house_station: str | None,
    incumbent_from_record: bool,
    failed_dirs: Sequence[str],
    header_base: Mapping[str, object],
) -> dict[str, bytes]:
    rows = load_record_rows(conn, site=site, variable="precip")
    by_key = {(row.snapshot_date, row.lead): row for row in rows}
    cases, _ = select_cases(by_key, first=first, last=last, excluded_h_dates=[])
    case_rows = [row for _, row in cases if row is not None]
    # The version-map stop (§13.1): every case row's control first, before any
    # event, selection or score; a row of a version the map does not hold
    # raises here.
    controls = rain_controls(case_rows)
    store = StationDayStore(
        load_station_rain(
            conn, site_id=site.site_id, first_day=None, last_day=None, known_by=None
        ),
        timezone=site.timezone,
    )
    events = RainEvents(store, timezone=site.timezone)
    replays = ReplayCache(conn, site=site, variable="precip")
    ctx = RainContext(
        site=site,
        rows=by_key,
        controls=controls,
        recorded=recorded_map(rows),
        recorded_text=recorded_text_map(rows),
        replays=replays,
        store=store,
        events=events,
    )
    quantities = ("precip_total", "precip_occurrence")
    published = load_published_truth(conn, site=site, quantities=quantities)
    stations = load_station_rows(conn, site=site, variable="precip")
    rebuilder = ArmARebuilder(stations, site=site, variable="precip")
    in_scope = rain_in_scope_days(case_rows, timezone=site.timezone)
    gates = compute_gates(
        rebuilder,
        published,
        in_scope_days=in_scope,
        quantities=quantities,
        site=site,
        force_arm=None,
    )
    stale = stale_truth_rows(
        published,
        quantities=quantities,
        date_sets={
            "targets": _days(first, last),
            "training_window_days": in_scope,
            "choosing_set_targets": _days(CHOOSING_FIRST, CHOOSING_LAST),
        },
    )
    facts = header_facts(conn, site=site, note=None)
    header: dict[str, object] = {
        "copy": header_base["copy"],
        "manifest": header_base["manifest"],
        "commit_sha": header_base["commit_sha"],
        "gates": gates,
        "arm": gates.arm,
        "stale_truth_rows": stale,
        **facts.as_header(),
        "excluded_h_dates_count": None,
    }
    if mode == "counts-only":
        lines = rain_lines(ctx, cases, incumbent_from_record=False, with_grid=False)
        header.update(
            counts={
                "record_rows": _rain_record_rows(cases),
                "f1": {
                    "true": sum(1 for line in lines if line.f1 is True),
                    "false": sum(1 for line in lines if line.f1 is False),
                    "none": sum(1 for line in lines if line.f1 is None),
                },
            },
            unmatched=rain_counts(lines).as_json(),
            h_training_coverage=None,
            rain_event_counts=rain_event_counts(lines),
            meteoblue=meteoblue_lists(conn, site=site, variable="precip"),
            coverage=coverage_counts(
                replays, (row for row in case_rows if row.status == "recorded")
            ),
        )
        return {"header.json": _dumps(_checked_counts_only(header))}
    if mode != "freeze-prepare":
        raise ValueError("--heldout is temperature only")
    lines = rain_lines(
        ctx, cases, incumbent_from_record=incumbent_from_record, with_grid=True
    )
    dates = sorted({key.target for key, _ in cases})
    index_lists = draw_resamples(len(dates))
    reports = {
        "symbol": symbol_report(lines, index_lists, dates),
        "chance": chance_report(lines, index_lists, dates),
        "amount": amount_report(lines, index_lists, dates),
    }
    discarded: dict[str, object] = {}
    for name, report in reports.items():
        delta = report["delta"]
        if isinstance(delta, Bounds):
            discarded[name] = {
                "kept": delta.kept,
                "discarded": delta.discarded,
                "discard_ok": discard_ok(delta),
            }
    header.update(
        format=HEADER_FORMAT,
        mode=mode,
        variable="precip",
        site=_site_json(site),
        incumbent_from_record=incumbent_from_record,
        settings=rain_settings(),
        owner_answers=rain_owner_answers(),
        house_station=house_station,
        failed_dirs=list(failed_dirs),
        decisions=reports,
        discarded_resamples=discarded,
        counts={
            "record_rows": _rain_record_rows(cases),
            "cases": rain_case_counts(lines),
        },
        unmatched=rain_counts(lines).as_json(),
        rain_event_counts=rain_event_counts(lines),
        t2=t2_report(conn, site=site, stations=stations, last_day=t2_last),
        block_length={
            name: block_length_check(series)
            for name, series in rain_block_series(lines).items()
        },
        own_days=own_days_summary(lines),
        replay_time=replay_time_stats(replays.seconds()),
        python=list(sys.version_info[:3]),
        not_built=[
            "the rain control from V_B on (stage two)",
            "A6 for rain (the offered w_amount and w_r values)",
            "A7",
            "A16",
            "A17",
            "F2",
            "A11: the largest SQLITE_TMPDIR size",
        ],
    )
    files = {
        "header.json": _dumps(header),
        "lines.jsonl": _dumps_lines(rain_line_json(line) for line in lines),
        "trigger_grid.jsonl": _dumps_lines(trigger_grid_json(lines)),
        "station_days.jsonl": _dumps_lines(station_days_lines(store, events)),
        "fault_states.json": _dumps(fault_states_json(store, events)),
    }
    if house_station is not None:
        house = load_house_station(
            conn, site=site, pws_station_id=house_station, variable="precip"
        )
        files["house_station.json"] = _dumps(house_rain_report(house, lines, site=site))
    return files


def _rain_record_rows(cases: Cases) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for key, row in cases:
        status = "no_row" if row is None else row.status
        per = out.setdefault(status, {})
        per[str(key.lead)] = per.get(str(key.lead), 0) + 1
    return out


# --- The run function and the command (§13.3) --------------------------------------


def run(
    conn: sqlite3.Connection,
    *,
    site: Site,
    variable: Variable,
    mode: Mode,
    first: date,
    last: date,
    h_first: date | None,
    last_complete: date,
    excluded_h_dates: Sequence[date],
    t2_last: date | None,
    note: FreezeNote | None,
    house_station: str | None,
    incumbent_from_record: bool,
    failed_dirs: Sequence[str],
    header_base: Mapping[str, object],
) -> dict[str, bytes]:
    """One run's output files, computed in memory (§13.3).

    ``first`` and ``last`` bound the case targets: temperature's choosing
    set, or rain's S0 to the last complete day. ``h_first`` to
    ``last_complete``, less ``excluded_h_dates``, bound temperature's H.
    ``t2_last`` is T2's last day (rain). The bounds select cases only; the
    training and truth reads follow each case's own window. ``failed_dirs``
    (the ``*.failed`` directories under the output root) goes into the
    draft freeze note and rain's freeze-prepare header, never a counts-only
    header.
    """
    if variable == "temperature":
        if h_first is None:
            raise ValueError("a temperature run needs h_first")
        return _run_temperature(
            conn,
            site=site,
            mode=mode,
            first=first,
            last=last,
            h_first=h_first,
            last_complete=last_complete,
            excluded_h_dates=excluded_h_dates,
            note=note,
            house_station=house_station,
            failed_dirs=failed_dirs,
            header_base=header_base,
        )
    if t2_last is None:
        raise ValueError("a rain run needs t2_last")
    return _run_rain(
        conn,
        site=site,
        mode=mode,
        first=first,
        last=last,
        t2_last=t2_last,
        house_station=house_station,
        incumbent_from_record=incumbent_from_record,
        failed_dirs=failed_dirs,
        header_base=header_base,
    )


def _commit_sha(value: str) -> str:
    """``--commit-sha``: the full 40-character lowercase hex sha, unchanged.

    Any other form, an uppercase sha included, is an argparse error.
    """
    if re.fullmatch(r"[0-9a-f]{40}", value):
        return value
    raise argparse.ArgumentTypeError(
        f"must be the full 40-character lowercase hex sha of the add-on commit, "
        f"not {value!r}"
    )


def build_parser() -> argparse.ArgumentParser:
    """The command line of §13.3."""
    parser = argparse.ArgumentParser(
        prog="python -m wxverify.verification.daily_shadow",
        description=(
            "Read-only replay of the daily forecast method on a database copy: "
            "counts, the freeze preparation and the held-out verdict. Start it "
            "with SQLITE_TMPDIR set to the copy's directory."
        ),
    )
    parser.add_argument("--copy", type=Path, required=True, help="the database copy")
    parser.add_argument(
        "--site-id", type=int, required=True, help="the trial's sites.id"
    )
    parser.add_argument("--out", type=Path, required=True, help="the output root")
    parser.add_argument("--variable", choices=("temperature", "precip"), required=True)
    parser.add_argument(
        "--commit-sha",
        type=_commit_sha,
        required=True,
        help="the 40-character sha of the add-on commit the run uses",
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--counts-only", dest="mode", action="store_const", const="counts-only"
    )
    modes.add_argument(
        "--freeze-prepare", dest="mode", action="store_const", const="freeze-prepare"
    )
    modes.add_argument("--heldout", dest="mode", action="store_const", const="heldout")
    parser.add_argument(
        "--freeze-note", type=Path, help="the freeze note (temperature)"
    )
    parser.add_argument(
        "--expect-sha256", help="the recorded sha256 of the verdict copy"
    )
    parser.add_argument(
        "--house-station",
        help="the house station's pws_station_id (required with --freeze-prepare)",
    )
    parser.add_argument(
        "--incumbent-from-record",
        action="store_true",
        help="read the incumbent from the record rows (rain --freeze-prepare)",
    )
    return parser


def _print_result_line(*, exit_code: int, run_dir: str | None) -> None:
    """Print the result line, the last line on standard output (§13.3).

    ``run_dir`` is the run directory written, relative to ``--out``, in
    POSIX form, or ``None`` when ``error.json`` could not be written.
    """
    result = {"exit_code": exit_code, "run_dir": run_dir}
    print(RESULT_PREFIX + json.dumps(result), flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the tool; 0 on success, 1 on ERROR (error.json), 2 on a refusal.

    After 0 and after 1 the last line on standard output is the result line
    (:data:`RESULT_PREFIX`); a refusal prints none.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    copy: Path = args.copy
    site_id: int = args.site_id
    out: Path = args.out
    variable: Variable = args.variable
    commit_sha: str = args.commit_sha
    mode: Mode = args.mode
    note_path: Path | None = args.freeze_note
    expect: str | None = args.expect_sha256
    house_station: str | None = args.house_station
    from_record: bool = args.incumbent_from_record
    if from_record and not (mode == "freeze-prepare" and variable == "precip"):
        parser.error(
            "--incumbent-from-record is allowed only with --freeze-prepare "
            "--variable precip"
        )
    if variable == "precip" and mode == "heldout":
        parser.error("--heldout is for --variable temperature only")
    if variable == "precip" and note_path is not None:
        parser.error("--freeze-note is for --variable temperature only")
    if mode == "heldout" and (note_path is None or expect is None):
        parser.error("--heldout needs --freeze-note and --expect-sha256")
    if mode == "freeze-prepare" and house_station is None:
        parser.error("--freeze-prepare needs --house-station")
    if house_station is not None and mode != "freeze-prepare":
        parser.error(
            "--house-station is allowed only with --freeze-prepare; "
            "--heldout reads it from the freeze note"
        )
    try:
        note = None if note_path is None else load_freeze_note(note_path)
        if note is not None and note.site_id != site_id:
            raise RefusedError("the freeze note is for another site")
        package_dir = package_root()
        manifest = check_commit(package_dir, commit_sha=commit_sha)
        if mode == "heldout":
            if note is None:
                raise RefusedError("--heldout needs a freeze note")
            check_note_matches_package(note, manifest=manifest)
            if note.house_station is None:
                raise RefusedError("the freeze note names no house station")
        refuse_unsafe_copy(copy)
        tmpdir = os.environ.get("SQLITE_TMPDIR")
        if tmpdir is None or os.path.realpath(tmpdir) != os.path.realpath(copy.parent):
            raise RefusedError(
                "SQLITE_TMPDIR must name the copy's directory in the environment "
                "the tool is started with"
            )
        before = sha256_file(copy)
        if expect is not None and before != expect.strip().lower():
            raise RefusedError("the copy's sha256 differs from --expect-sha256")
        for renamed in rename_stale_partials(out):
            print(f"renamed an unfinished output to {renamed}", file=sys.stderr)
        failed_dirs = list_failed_dirs(out)
        start = datetime.now(UTC)
        copied_at = datetime.fromtimestamp(copy.stat().st_mtime, tz=UTC)
    except RefusedError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(
            f"refused: the run could not be prepared: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    final_dir = (
        out
        / copied_at.date().isoformat()
        / variable
        / f"{mode}-{start.strftime('%Y%m%dT%H%M%SZ')}"
    )
    stage = "open"
    try:
        conn = open_copy_readonly(copy)
        stage = "run"
        try:
            if variable == "precip":
                # The span check (§13.3): rain's first step, before any other
                # read of the copy. The whole table is counted, with no site,
                # station, date or fetched_at filter: the rule is about the copy.
                stage = "span_check"
                named = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master "
                    "WHERE lower(name) = 'station_precip_spans'"
                ).fetchone()
                if int(named[0]) > 0:
                    spans = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM station_precip_spans"
                        ).fetchone()[0]
                    )
                    if spans > 0:
                        raise SpanRowsPresentError(
                            f"the copy holds {spans} station_precip_spans rows; "
                            "rain reads no copy that holds span rows"
                        )
                stage = "run"
            site = load_site(conn, site_id=site_id)
            last_complete = latest_knowable_target(site.timezone, copied_at)
            header_base = {
                "copy": copy_facts(copy, before),
                "manifest": manifest,
                "commit_sha": commit_sha,
            }
            if variable == "temperature":
                files = run(
                    conn,
                    site=site,
                    variable=variable,
                    mode=mode,
                    first=CHOOSING_FIRST,
                    last=CHOOSING_LAST,
                    h_first=DEFAULT_H_FIRST if note is None else note.first_h_date,
                    last_complete=last_complete,
                    excluded_h_dates=[]
                    if note is None
                    else list(note.excluded_h_dates),
                    t2_last=None,
                    note=note,
                    house_station=house_station,
                    incumbent_from_record=from_record,
                    failed_dirs=failed_dirs,
                    header_base=header_base,
                )
            else:
                files = run(
                    conn,
                    site=site,
                    variable=variable,
                    mode=mode,
                    first=rain_s0(conn, site=site),
                    last=last_complete,
                    h_first=None,
                    last_complete=last_complete,
                    excluded_h_dates=[],
                    t2_last=CHOOSING_LAST,
                    note=None,
                    house_station=house_station,
                    incumbent_from_record=from_record,
                    failed_dirs=failed_dirs,
                    header_base=header_base,
                )
        finally:
            conn.close()
        stage = "code_check"
        try:
            after_manifest = package_manifest(package_dir)
        except OSError as exc:
            raise RuntimeError(
                "the package manifest after the run could not be computed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if after_manifest != manifest:
            detail = changed_paths(
                _manifest_digests(manifest), _manifest_digests(after_manifest)
            )
            stage = "code_changed"
            raise CodeChangedError(
                f"the package's .py files changed during the run: {detail}"
            )
        stage = "copy_check"
        after = sha256_file(copy)
        if after != before:
            stage = "copy_changed"
            raise CopyChangedError(
                f"the copy's sha256 changed during the run: {before} -> {after}"
            )
        stage = "write"
        written = write_outputs_atomically(final_dir, files)
    except Exception as exc:
        # The try begins with the open, so every exit through here follows it.
        stage, text = error_after_open(copy, before=before, stage=stage, exc=exc)
        error: dict[str, object] = {
            "stage": stage,
            "error": text,
            "sqlite_errorcode": find_sqlite_errorcode(exc),
            "copy_sha256": before,
            "manifest": manifest,
        }
        print(f"ERROR at stage {stage}: {error['error']}", file=sys.stderr)
        try:
            written = write_outputs_atomically(final_dir, {"error.json": _dumps(error)})
        except OSError as write_exc:
            print(f"error.json not written: {write_exc}", file=sys.stderr)
            _print_result_line(exit_code=1, run_dir=None)
            return 1
        _print_result_line(exit_code=1, run_dir=written.relative_to(out).as_posix())
        return 1
    _print_result_line(exit_code=0, run_dir=written.relative_to(out).as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
