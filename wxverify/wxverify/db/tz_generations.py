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

import json
import sqlite3
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state

_POINTER_KEY_PREFIX = "tz_generation_published:"

#: job_key prefix for timezone_correction chain jobs (``tzcorr:<generation>``).
CORRECTION_JOB_KEY_PREFIX = "tzcorr:"


class TimezoneOperationRefused(ValueError):
    """Base for refusals raised by the timezone operations.

    Subclasses ``ValueError`` deliberately: the CLI (``__main__.py``) and the
    existing tests catch ``ValueError`` and print the message, and they must
    keep working unchanged. The subclasses exist so an HTTP caller can map a
    refusal to a status by TYPE rather than by matching message text.
    """


class UnknownTimezone(TimezoneOperationRefused):
    """The supplied string is not a resolvable IANA timezone key."""


class TimezoneSiteNotFound(TimezoneOperationRefused):
    """The site a timezone operation targets does not exist."""


class CorrectionAlreadyBuilding(TimezoneOperationRefused):
    """A retrospective correction is already building for the site.

    Carries the existing generation's id so the refusal message can name a
    handle the operator can look up rather than a dead end.
    """

    def __init__(self, message: str, generation_id: int) -> None:
        super().__init__(message)
        self.generation_id = generation_id


def correction_job_key(generation_id: int) -> str:
    """``jobs.job_key`` for the correction chain building ``generation_id``."""
    return f"{CORRECTION_JOB_KEY_PREFIX}{generation_id}"


def _validate_timezone(timezone: str) -> None:
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise UnknownTimezone(f"unknown IANA timezone {timezone!r}") from exc


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


def _optional_int(value: object) -> int | None:
    return None if value is None else int(str(value))


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def generation_status(
    conn: sqlite3.Connection, site_id: int | None = None
) -> list[dict[str, object]]:
    """Every timezone generation, oldest first, with the published pointer
    resolved and the reconciliation tally a correction chain records.

    Read-only: the operator surface for §20's "verify the counts" step. The
    counts are NULL until the correction chain starts filling them, and a
    finished correction satisfies ``examined == changed + unchanged +
    excluded``. Raises ValueError for an unknown ``site_id``.
    """
    if site_id is not None:
        site = conn.execute("SELECT id FROM sites WHERE id=?", (site_id,)).fetchone()
        if site is None:
            raise ValueError(f"site {site_id} does not exist")
    rows = conn.execute(
        f"""
        SELECT g.site_id, g.id, g.timezone, g.mode, g.state,
               g.effective_from, g.effective_to, g.published_at,
               g.examined_count, g.changed_count,
               g.unchanged_count, g.excluded_count,
               CAST(rs.value AS INTEGER) AS pointer
        FROM timezone_generations g
        LEFT JOIN runtime_state rs
          ON rs.key = '{_POINTER_KEY_PREFIX}' || g.site_id
        WHERE (? IS NULL OR g.site_id = ?)
        ORDER BY g.site_id, g.id
        """,
        (site_id, site_id),
    ).fetchall()
    return [
        {
            "site_id": int(row["site_id"]),
            "generation_id": int(row["id"]),
            "timezone": str(row["timezone"]),
            "mode": str(row["mode"]),
            "state": str(row["state"]),
            "published_pointer": row["pointer"] is not None
            and int(row["pointer"]) == int(row["id"]),
            "effective_from": _optional_str(row["effective_from"]),
            "effective_to": _optional_str(row["effective_to"]),
            "published_at": _optional_str(row["published_at"]),
            "examined": _optional_int(row["examined_count"]),
            "changed": _optional_int(row["changed_count"]),
            "unchanged": _optional_int(row["unchanged_count"]),
            "excluded": _optional_int(row["excluded_count"]),
        }
        for row in rows
    ]


def resolve_generation_for_instant(
    conn: sqlite3.Connection, site_id: int, instant_utc: str
) -> int | None:
    """The published-state generation whose effective interval contains
    ``instant_utc`` (§14: derivation-time timezone resolution).

    A NULL ``effective_from`` is an open lower bound and a NULL
    ``effective_to`` an open upper bound. The live pairs-read binding stays
    the pointer (:func:`published_generation_clause`); this resolver is the
    seam for derivations (records, backtest) that must honor a prospective
    change's history split. Returns None for an unseeded site.
    """
    instant = parse_utc(instant_utc)  # normalizes/validates the probe instant
    rows = conn.execute(
        """
        SELECT id, effective_from, effective_to
        FROM timezone_generations
        WHERE site_id = ? AND state = 'published'
        ORDER BY id
        """,
        (site_id,),
    ).fetchall()
    for row in rows:
        lower = row["effective_from"]
        upper = row["effective_to"]
        if lower is not None and instant < parse_utc(str(lower)):
            continue
        if upper is not None and instant >= parse_utc(str(upper)):
            continue
        return int(row["id"])
    return None


def start_retrospective_correction(
    conn: sqlite3.Connection, site_id: int, timezone: str
) -> int:
    """Create a BUILDING retrospective-correction generation and enqueue its
    chain job (§13). Returns the new generation id.

    The whole history is rebuilt under ``timezone`` alongside the published
    generation; readers keep serving the published generation until the
    chain's flip transaction. Refuses while another correction is already
    building for the site (one build-alongside at a time).
    """
    from wxverify.db.queue import enqueue_if_absent

    _validate_timezone(timezone)
    site = conn.execute("SELECT timezone FROM sites WHERE id=?", (site_id,)).fetchone()
    if site is None:
        raise TimezoneSiteNotFound(f"site {site_id} does not exist")
    building = conn.execute(
        """
        SELECT id FROM timezone_generations
        WHERE site_id = ? AND state = 'building'
        LIMIT 1
        """,
        (site_id,),
    ).fetchone()
    if building is not None:
        raise CorrectionAlreadyBuilding(
            f"site {site_id} already has a correction building "
            f"(generation {int(building['id'])})",
            generation_id=int(building["id"]),
        )
    ensure_published_generation(conn, site_id)
    provenance = json.dumps(
        {
            "previous_timezone": str(site["timezone"]),
            "target_timezone": timezone,
        },
        separators=(",", ":"),
    )
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, provenance)
        VALUES (?, ?, 'retrospective_correction', 'building', ?)
        """,
        (site_id, timezone, provenance),
    )
    if cur.lastrowid is None:
        raise RuntimeError("timezone generation insert failed")
    generation_id = int(cur.lastrowid)
    enqueue_if_absent(
        conn,
        "timezone_correction",
        site_id,
        correction_job_key(generation_id),
        {"generation_id": generation_id},
    )
    return generation_id


def apply_prospective_change(
    conn: sqlite3.Connection, site_id: int, timezone: str, effective_from: str
) -> int:
    """Apply a prospective timezone change (§13): close the current published
    generation at ``effective_from``, publish a new effective-dated
    generation, flip the pointer, and update ``sites.timezone`` — all in the
    caller's write transaction. No rebuild: earlier history stays under its
    former generation (retained rows, resolvable per-instant via
    :func:`resolve_generation_for_instant`); live pointer-bound reads serve
    the new generation, which accrues pairs going forward.
    """
    _validate_timezone(timezone)
    effective = isoformat_utc(parse_utc(effective_from))
    current_id = ensure_published_generation(conn, site_id)
    current = conn.execute(
        "SELECT timezone, effective_from FROM timezone_generations WHERE id = ?",
        (current_id,),
    ).fetchone()
    if current is None:
        raise RuntimeError(f"published generation {current_id} missing")
    if str(current["timezone"]) == timezone:
        raise ValueError(f"site {site_id} is already on timezone {timezone!r}")
    lower = current["effective_from"]
    if lower is not None and parse_utc(effective) <= parse_utc(str(lower)):
        raise ValueError(
            "effective_from must be after the current generation's own effective_from"
        )
    now = isoformat_utc()
    conn.execute(
        "UPDATE timezone_generations SET effective_to = ? WHERE id = ?",
        (effective, current_id),
    )
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, effective_from, published_at,
             provenance)
        VALUES (?, ?, 'prospective_change', 'published', ?, ?, ?)
        """,
        (
            site_id,
            timezone,
            effective,
            now,
            json.dumps(
                {
                    "previous_timezone": str(current["timezone"]),
                    "target_timezone": timezone,
                    "effective_from": effective,
                },
                separators=(",", ":"),
            ),
        ),
    )
    if cur.lastrowid is None:
        raise RuntimeError("timezone generation insert failed")
    new_id = int(cur.lastrowid)
    set_runtime_state(conn, published_pointer_key(site_id), str(new_id))
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (timezone, site_id))
    return new_id
