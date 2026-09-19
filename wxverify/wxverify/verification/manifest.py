"""The explicit input manifest a verification run pins at start.

One row per input component in ``verification_run_inputs``, written inside
the run-start transaction from the pinned :class:`RunConfig` (never from live
tables), so "what did this run consume" is a table an operator can query
rather than a hash they cannot decompose. Each stored value is the composite
``"<algorithm>:<payload>"`` (the same shape ``result_basis_fingerprint``
already uses), so a superseded algorithm is diagnosable on read instead of
comparing unequal and lying with ``changed``.

Three components, two roles. ``config_truth_basis`` and ``forecast_arrivals``
are verdict-bearing and required; ``pair_arrivals`` is recorded and reported
but never moves the verdict, because ``forecast_pairs`` reuses rowids after
its many deletes and its noise floor is unmeasured. The roles live in the two
frozensets below and are consulted by name, so a promotion is a one-token
edit.

This module imports from ``verification.runs``; ``runs.py`` never imports it
(the import direction the facade in ``verification.freshness`` depends on).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Final, Literal, cast

from wxverify.core.timeutil import isoformat_utc
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.runs import RunConfig, result_basis_freshness

MANIFEST_COMPONENT_CONFIG_TRUTH: Final = "config_truth_basis"
MANIFEST_COMPONENT_FORECAST_ARRIVALS: Final = "forecast_arrivals"
MANIFEST_COMPONENT_PAIR_ARRIVALS: Final = "pair_arrivals"

# Fixed evaluation and reporting order: the reason reported for an
# ``unknown`` overall state is the FIRST unknown required component in this
# order, so it is deterministic when more than one is unknown.
MANIFEST_COMPONENT_ORDER: Final = (
    MANIFEST_COMPONENT_CONFIG_TRUTH,
    MANIFEST_COMPONENT_FORECAST_ARRIVALS,
    MANIFEST_COMPONENT_PAIR_ARRIVALS,
)
# A required component with no row is reported ``unknown``/``not_recorded``
# and blocks ``fresh``; a verdict-bearing one that is ``changed`` makes the
# overall state ``changed``. ``pair_arrivals`` is in neither set (D8).
MANIFEST_REQUIRED_COMPONENTS: Final = frozenset(
    {MANIFEST_COMPONENT_CONFIG_TRUTH, MANIFEST_COMPONENT_FORECAST_ARRIVALS}
)
MANIFEST_VERDICT_COMPONENTS: Final = frozenset(
    {MANIFEST_COMPONENT_CONFIG_TRUTH, MANIFEST_COMPONENT_FORECAST_ARRIVALS}
)

# Arrivals values are ``"<algorithm>:<MAX(id) at run start>"``; the id is a
# plain INTEGER PRIMARY KEY, so ``id > payload`` means "inserted after this
# run started" for every surviving site.
FORECAST_ARRIVALS_ALGORITHM: Final = "fs1"
PAIR_ARRIVALS_ALGORITHM: Final = "fp1"

# The note vocabulary (D10): exactly the reasons ``result_basis_freshness``
# attaches to an ``unknown`` basis, so the page's ``freshness_reasons`` prose
# already covers every note a component can carry and no new operator-facing
# string exists. The evaluators build their own notes through ``_unknown``,
# which consults this set -- it is a value the code uses, not a set a test
# infers from source text.
MANIFEST_NOTE_TOKENS: Final = frozenset(
    {
        "not_recorded",
        "algorithm_changed",
        "malformed_record",
        "period_unknown",
        "no_published_generation",
    }
)

_State = Literal["fresh", "changed", "unknown"]


@dataclass(frozen=True)
class ManifestComponentState:
    """One pinned input's freshness: its name, state and, if unknown, why.

    ``note`` is None for ``fresh`` and ``changed``; for ``unknown`` it is one
    of :data:`MANIFEST_NOTE_TOKENS`. Named ``note`` rather than ``reason`` so
    the overall verdict's ``reason`` stays the only field of that name.
    """

    component: str
    state: _State
    note: str | None


@dataclass(frozen=True)
class RunInputFreshness:
    """Overall freshness of a published run's pinned inputs, with a breakdown.

    ``state`` and ``reason`` keep :class:`ResultBasisFreshness`'s meanings
    and value domains; ``components`` is every evaluated input in
    :data:`MANIFEST_COMPONENT_ORDER`, observed-only ones included, so the
    API can show what was compared even when it did not move the verdict.
    """

    state: _State
    reason: str | None
    components: tuple[ManifestComponentState, ...]

    def as_payload(self) -> dict[str, object]:
        """The surface payload -- the ONE serializer both surfaces use (D13)."""
        return {
            "state": self.state,
            "reason": self.reason,
            "components": [
                {"component": c.component, "state": c.state, "note": c.note}
                for c in self.components
            ],
        }


RUN_INPUTS_NO_RUN: Final = RunInputFreshness(
    state="unknown", reason="no_published_run", components=()
)


def write_run_manifest(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """Pin the inputs this run consumes, one row per component (D4/D5).

    Called inside the run-start transaction, immediately after ``start_run``
    inserted the run row. Every value is taken from ``cfg`` -- the same
    pinned configuration the run simulates under -- so the manifest's
    horizon cannot disagree with the run's.

    ``config_truth_basis`` is read back from the run row's own column, never
    recomputed (D6): byte-identity with the column is then structural, and a
    ``daily_truth`` write landing between the two statements cannot produce
    two values for one run. ``start_run`` always writes that column, so a
    NULL there is a defensive branch, and the row is simply not written --
    the read side reports the component ``not_recorded`` rather than being
    handed an invented placeholder.
    """
    start = local_day_bounds(date.fromisoformat(cfg.period_start), cfg.timezone)
    end = local_day_bounds(date.fromisoformat(cfg.period_end), cfg.timezone)
    arrivals_scope = json.dumps(
        {
            "horizon_start_utc": isoformat_utc(start.start_utc),
            "horizon_end_utc": isoformat_utc(end.end_utc),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    basis_scope = json.dumps(
        {
            "period_start": cfg.period_start,
            "period_end": cfg.period_end,
            "tz_generation_id": cfg.tz_generation_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    rows: list[tuple[int, str, str, str]] = []
    basis_row = conn.execute(
        "SELECT result_basis_fingerprint FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    basis = None if basis_row is None else basis_row["result_basis_fingerprint"]
    if basis is not None:
        rows.append(
            (cfg.run_id, MANIFEST_COMPONENT_CONFIG_TRUTH, str(basis), basis_scope)
        )
    samples = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS hi FROM forecast_samples WHERE site_id = ?",
        (cfg.site_id,),
    ).fetchone()
    pairs = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS hi FROM forecast_pairs WHERE site_id = ?",
        (cfg.site_id,),
    ).fetchone()
    rows.append(
        (
            cfg.run_id,
            MANIFEST_COMPONENT_FORECAST_ARRIVALS,
            f"{FORECAST_ARRIVALS_ALGORITHM}:{int(samples['hi'])}",
            arrivals_scope,
        )
    )
    rows.append(
        (
            cfg.run_id,
            MANIFEST_COMPONENT_PAIR_ARRIVALS,
            f"{PAIR_ARRIVALS_ALGORITHM}:{int(pairs['hi'])}",
            arrivals_scope,
        )
    )
    conn.executemany(
        """
        INSERT INTO verification_run_inputs (run_id, component, value, scope)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )


def _unknown(component: str, note: str) -> ManifestComponentState:
    """An ``unknown`` component carrying one of the shared note tokens (D10)."""
    if note not in MANIFEST_NOTE_TOKENS:
        raise ValueError(f"manifest note {note!r} is outside MANIFEST_NOTE_TOKENS")
    return ManifestComponentState(component=component, state="unknown", note=note)


def _samples_arrived(
    conn: sqlite3.Connection, site_id: int, watermark: int, start: str, end: str
) -> bool:
    """Did a forecast row land inside ``[start, end)`` after the watermark?

    ``NOT INDEXED`` forbids the covering index (a read of the site's whole
    history) but not the rowid, so the planner range-seeks only the rows
    added since the pin -- the 100x cost decision D7 records and the query
    plan tests pin in the source text.
    """
    row = conn.execute(
        """
        SELECT EXISTS(
            SELECT 1 FROM forecast_samples NOT INDEXED
            WHERE site_id = ? AND id > ?
              AND valid_at >= ? AND valid_at < ?
        ) AS moved
        """,
        (site_id, watermark, start, end),
    ).fetchone()
    return bool(row["moved"])


def _pairs_arrived(
    conn: sqlite3.Connection, site_id: int, watermark: int, start: str, end: str
) -> bool:
    """Did a scored pair land inside ``[start, end)`` after the watermark?

    Same shape as :func:`_samples_arrived`, written out rather than
    table-interpolated so the statement text can be pinned verbatim (D16).
    """
    row = conn.execute(
        """
        SELECT EXISTS(
            SELECT 1 FROM forecast_pairs NOT INDEXED
            WHERE site_id = ? AND id > ?
              AND valid_at >= ? AND valid_at < ?
        ) AS moved
        """,
        (site_id, watermark, start, end),
    ).fetchone()
    return bool(row["moved"])


_Probe = Callable[[sqlite3.Connection, int, int, str, str], bool]

_ARRIVALS_PROBES: Final[dict[str, tuple[str, _Probe]]] = {
    MANIFEST_COMPONENT_FORECAST_ARRIVALS: (
        FORECAST_ARRIVALS_ALGORITHM,
        _samples_arrived,
    ),
    MANIFEST_COMPONENT_PAIR_ARRIVALS: (PAIR_ARRIVALS_ALGORITHM, _pairs_arrived),
}


def _scope_bounds(scope: str) -> tuple[str, str] | None:
    """The two UTC bounds an arrivals row's ``scope`` carries, or None.

    None for anything but a JSON object with both keys as strings. The
    caller reports that as ``malformed_record`` -- never as an unbounded
    probe, which would be the permanently-true warning again.
    """
    try:
        parsed: object = json.loads(scope)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    bounds = cast(dict[str, object], parsed)
    start = bounds.get("horizon_start_utc")
    end = bounds.get("horizon_end_utc")
    if not isinstance(start, str) or not isinstance(end, str):
        return None
    return start, end


def _arrivals_state(
    conn: sqlite3.Connection,
    site_id: int,
    component: str,
    row: sqlite3.Row,
    *,
    algorithm: str,
    probe: _Probe,
) -> ManifestComponentState:
    """The shared arrivals ladder: prefix, payload, scope, then the probe.

    Form checks run before the probe for the same reason
    :func:`result_basis_freshness` orders its own: a value written under a
    superseded algorithm must report ``algorithm_changed``, not compare
    unequal and lie with ``changed``. The payload must be an ASCII decimal
    integer -- ``str.isdigit()`` alone accepts digit forms ``int()`` also
    parses, which would let a corrupt value through.
    """
    value = str(row["value"])
    prefix = f"{algorithm}:"
    if not value.startswith(prefix):
        return _unknown(component, "algorithm_changed")
    payload = value.removeprefix(prefix)
    if not (payload.isascii() and payload.isdigit()):
        return _unknown(component, "malformed_record")
    bounds = _scope_bounds(str(row["scope"]))
    if bounds is None:
        return _unknown(component, "malformed_record")
    start, end = bounds
    moved = probe(conn, site_id, int(payload), start, end)
    return ManifestComponentState(
        component=component, state="changed" if moved else "fresh", note=None
    )


def _component_state(
    conn: sqlite3.Connection,
    site_id: int,
    component: str,
    row: sqlite3.Row,
    *,
    period_start: str | None,
    period_end: str | None,
) -> ManifestComponentState:
    if component == MANIFEST_COMPONENT_CONFIG_TRUTH:
        basis = result_basis_freshness(
            conn,
            site_id,
            recorded=str(row["value"]),
            period_start=period_start,
            period_end=period_end,
        )
        return ManifestComponentState(
            component=component, state=basis.state, note=basis.reason
        )
    algorithm, probe = _ARRIVALS_PROBES[component]
    return _arrivals_state(
        conn, site_id, component, row, algorithm=algorithm, probe=probe
    )


def _overall(
    state: _State, reason: str | None, components: list[ManifestComponentState]
) -> RunInputFreshness:
    return RunInputFreshness(state=state, reason=reason, components=tuple(components))


def _verdict(components: list[ManifestComponentState]) -> RunInputFreshness:
    """D9's precedence over components already in ``MANIFEST_COMPONENT_ORDER``.

    ``changed`` outranks ``unknown`` because a true positive is the reason
    the warning exists; the ``unknown`` reason is the FIRST unknown required
    component's note, so it is deterministic when several are unknown.
    Only the two role sets decide which components count.
    """
    if any(
        c.state == "changed" and c.component in MANIFEST_VERDICT_COMPONENTS
        for c in components
    ):
        return _overall("changed", None, components)
    for c in components:
        if c.state == "unknown" and c.component in MANIFEST_REQUIRED_COMPONENTS:
            return _overall("unknown", c.note, components)
    return _overall("fresh", None, components)


def _legacy_freshness(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    recorded_basis: str | None,
    period_start: str | None,
    period_end: str | None,
) -> RunInputFreshness:
    """A run with no manifest rows, judged from its basis column alone (D11).

    ``changed`` survives (a true positive); ``fresh`` becomes
    ``unknown``/``not_recorded`` because the forecast side of such a run was
    never recorded and cannot be reconstructed; an ``unknown`` basis keeps
    its own reason. The basis verdict is reported as the single
    ``config_truth_basis`` component so the API still shows what was
    evaluated.
    """
    basis = result_basis_freshness(
        conn,
        site_id,
        recorded=recorded_basis,
        period_start=period_start,
        period_end=period_end,
    )
    components = [
        ManifestComponentState(
            component=MANIFEST_COMPONENT_CONFIG_TRUTH,
            state=basis.state,
            note=basis.reason,
        )
    ]
    if basis.state == "fresh":
        return _overall("unknown", "not_recorded", components)
    return _overall(basis.state, basis.reason, components)


def run_input_freshness(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    run_id: int,
    recorded_basis: str | None,
    period_start: str | None,
    period_end: str | None,
) -> RunInputFreshness:
    """Freshness of every input a published run pinned -- READ PATH.

    ONE derivation behind both surfaces -- each reaches it through the
    facade (D17) -- exactly as ``result_basis_freshness`` is for the basis
    alone; this function calls that one for the ``config_truth_basis``
    component rather than reimplementing its ladder (D12). Read-only by
    construction: it SELECTs from ``verification_run_inputs`` and delegates
    the generation resolve to the non-seeding path
    ``result_basis_freshness`` already uses (NB-9).

    Every read -- the manifest rows, the delegated basis comparison, both
    arrivals probes -- goes through the connection handed in, never a
    second reader: two pooled readers observe two database instants, and a
    verdict assembled from both describes a state the database was never
    in. A required component with no row is reported ``unknown`` /
    ``not_recorded`` rather than inferred from the rows present;
    unrecognized component names are ignored (forward-compatible, and
    completeness is guarded by the required set instead).
    """
    rows = conn.execute(
        """
        SELECT component, value, scope FROM verification_run_inputs
        WHERE run_id = ? ORDER BY component
        """,
        (run_id,),
    ).fetchall()
    if not rows:
        return _legacy_freshness(
            conn,
            site_id,
            recorded_basis=recorded_basis,
            period_start=period_start,
            period_end=period_end,
        )
    by_name = {str(row["component"]): row for row in rows}
    components: list[ManifestComponentState] = []
    for component in MANIFEST_COMPONENT_ORDER:
        row = by_name.get(component)
        if row is not None:
            components.append(
                _component_state(
                    conn,
                    site_id,
                    component,
                    row,
                    period_start=period_start,
                    period_end=period_end,
                )
            )
        elif component in MANIFEST_REQUIRED_COMPONENTS:
            components.append(_unknown(component, "not_recorded"))
    return _verdict(components)
