"""Pair phases split into a read-only compute and a write-only apply.

Each pair phase's compute reads its inputs and returns a ``PairDelta``: the
sites whose published generation must exist, and the ordered ``forecast_pairs``
operations. The apply side here only writes: it seeds each site's published
generation (``ensure_published_generation``), then runs the operations. The
monolithic phase functions are ``apply_delta(conn, compute(conn, site_id))``
in their caller's single transaction.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Final

from wxverify.db.tz_generations import ensure_published_generation

type SqlValue = str | int | float | bytes | None
# The 18 insert values of one pair, tz_generation_id excluded.
type PairValues = tuple[SqlValue, ...]

SCORING_APPLY_CHUNK_ROWS: Final = 500  # Step 0b's chosen cap (§3.2)
_DELETE_PAIR_BY_ID_SQL: Final = "DELETE FROM forecast_pairs WHERE id = ?"


@dataclass(frozen=True, slots=True)
class InsertOp:
    """Insert one pair for ``site_id`` under its published generation."""

    site_id: int
    values: PairValues


@dataclass(frozen=True, slots=True)
class DeleteOp:
    """Delete the pair row ``row_id``, which the compute read."""

    row_id: int


@dataclass(frozen=True, slots=True)
class ReplaceOp:
    """Delete the pair row ``row_id``, then insert ``values`` in its place."""

    row_id: int
    site_id: int
    values: PairValues


type PairOp = InsertOp | DeleteOp | ReplaceOp


@dataclass(frozen=True, slots=True)
class PairDelta:
    """One pair phase's computed writes."""

    seed_sites: tuple[int, ...]  # sites to ensure, in the phase's first-use order
    ops: tuple[PairOp, ...]
    insert_sql: str  # the phase's INSERT; takes (*values, tz_generation_id)
    count: int | None  # the phase's return value; None = sum of insert rowcounts


class GenerationResolver:
    """Per-apply cache of each seed site's published generation id."""

    def __init__(self, seed_sites: Sequence[int]) -> None:
        self._seed_sites = tuple(seed_sites)
        self._ids: dict[int, int] = {}

    def seed(self, conn: sqlite3.Connection) -> None:
        """Ensure every seed site's published generation, in seed order."""
        for site_id in self._seed_sites:
            if site_id not in self._ids:
                self._ids[site_id] = ensure_published_generation(conn, site_id)

    def generation_id(self, site_id: int) -> int:
        """The seeded published generation id of ``site_id``."""
        return self._ids[site_id]  # KeyError = an op for an unseeded site: a bug


def op_cost(op: PairOp) -> int:
    """Number of SQL statements ``op`` runs (a replace is DELETE + INSERT)."""
    return 2 if isinstance(op, ReplaceOp) else 1


def chunk_ops(ops: Sequence[PairOp], *, cap: int) -> Iterator[tuple[PairOp, ...]]:
    """Consecutive, order-preserving chunks of at most ``cap`` statements.

    Always yields at least one chunk, so an empty delta still gets one apply
    (the enabled check and the seeding run exactly as the monolithic phase
    runs them).
    """
    if cap < 2:
        raise ValueError(f"chunk cap must be >= 2, got {cap}")
    chunk: list[PairOp] = []
    cost = 0
    for op in ops:
        op_statements = op_cost(op)
        if chunk and cost + op_statements > cap:
            yield tuple(chunk)
            chunk, cost = [], 0
        chunk.append(op)
        cost += op_statements
    yield tuple(chunk)


def apply_pair_ops(
    conn: sqlite3.Connection,
    delta: PairDelta,
    ops: Sequence[PairOp],
    resolver: GenerationResolver,
) -> int:
    """Apply ``ops`` in order; return the summed INSERT rowcounts."""
    inserted = 0
    for op in ops:
        if isinstance(op, InsertOp):
            inserted += conn.execute(
                delta.insert_sql, (*op.values, resolver.generation_id(op.site_id))
            ).rowcount
            continue
        if conn.execute(_DELETE_PAIR_BY_ID_SQL, (op.row_id,)).rowcount != 1:
            raise RuntimeError(f"forecast_pairs row {op.row_id} vanished before apply")
        if isinstance(op, ReplaceOp):
            inserted += conn.execute(
                delta.insert_sql, (*op.values, resolver.generation_id(op.site_id))
            ).rowcount
    return inserted


def apply_delta(conn: sqlite3.Connection, delta: PairDelta) -> int:
    """Whole-delta apply in the caller's transaction (the monolithic path)."""
    resolver = GenerationResolver(delta.seed_sites)
    resolver.seed(conn)
    inserted = apply_pair_ops(conn, delta, delta.ops, resolver)
    return inserted if delta.count is None else delta.count
