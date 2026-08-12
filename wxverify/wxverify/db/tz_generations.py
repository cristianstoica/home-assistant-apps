"""Timezone generations and the per-site published-generation pointer.

Since schema v4 every ``forecast_pairs`` row is generation-tagged
(``tz_generation_id``), and every production statement reading pairs binds
that column to the site's PUBLISHED generation. The published selection is a
``runtime_state`` pointer row per site (key
``tz_generation_published:<site_id>``) storing the published
``timezone_generations.id`` — a retrospective timezone correction builds a
new generation alongside the active one and flips this pointer atomically in
its completing write transaction, so readers never observe a partially
rebuilt generation.

This module is the single shared accessor for that binding:

* writers call :func:`ensure_published_generation` (seeding the site's
  ``initial`` generation + pointer on first use — the same seed shape
  ``migrate_v4`` applies to pre-existing sites);
* SQL readers embed :func:`published_generation_clause`, which resolves the
  pointer in-statement and therefore works unchanged in single-site and
  cross-site queries.
"""

from __future__ import annotations

import sqlite3

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state

_POINTER_KEY_PREFIX = "tz_generation_published:"


def published_pointer_key(site_id: int) -> str:
    """``runtime_state`` key holding the site's published generation id."""
    return f"{_POINTER_KEY_PREFIX}{site_id}"


def published_generation_id(conn: sqlite3.Connection, site_id: int) -> int | None:
    """The site's published ``timezone_generations.id``, or None if unseeded."""
    value = get_runtime_state(conn, published_pointer_key(site_id))
    return None if value is None else int(value)


def ensure_published_generation(conn: sqlite3.Connection, site_id: int) -> int:
    """Return the site's published generation id, seeding it when absent.

    Seeds one ``initial`` generation (state ``published``, NULL-bounded
    effective interval — it covers the whole history) plus the pointer row.
    Idempotent; must run on a write connection.
    """
    existing = published_generation_id(conn, site_id)
    if existing is not None:
        return existing
    site = conn.execute("SELECT timezone FROM sites WHERE id=?", (site_id,)).fetchone()
    if site is None:
        raise ValueError(f"site {site_id} does not exist")
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, ?, 'initial', 'published', ?)
        """,
        (site_id, str(site["timezone"]), isoformat_utc()),
    )
    if cur.lastrowid is None:
        raise RuntimeError("timezone generation insert failed")
    generation_id = int(cur.lastrowid)
    set_runtime_state(conn, published_pointer_key(site_id), str(generation_id))
    return generation_id


def published_generation_clause(alias: str) -> str:
    """SQL predicate binding a pairs read to the row's site's published
    generation.

    ``alias`` is the ``forecast_pairs`` alias (or the bare table name). The
    pointer is resolved in-statement — a primary-key probe of
    ``runtime_state`` — so the same clause serves single-site and cross-site
    statements, and a site with no pointer simply matches no rows.
    """
    return (
        f"{alias}.tz_generation_id = ("
        "SELECT CAST(rs.value AS INTEGER) FROM runtime_state rs "
        f"WHERE rs.key = '{_POINTER_KEY_PREFIX}' || {alias}.site_id)"
    )
