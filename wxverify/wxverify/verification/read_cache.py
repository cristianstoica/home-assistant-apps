"""In-process cache for the two read-time verification derivations.

``daily_rank_conclusions`` (W7) and ``observed_wet_precip_mae`` (W13) are
derived on every request by both verification surfaces, and W7 costs seconds
at production cardinality. Their inputs cannot change: a published run's row
is never rewritten and its evidence is never deleted, so the same run id in
the same database always yields the same conclusion.

**Invalidation contract.** The key is ``(generation, derivation_name,
run_id)`` — one entry per run, per derivation, per database generation, so
neither a run mix-up nor a whole-file database import can serve a foreign or
stale conclusion. Only a run in state ``published`` is cached, and the
published check gates the *lookup* as well as the store, because a published
pointer is only a pointer: an import can leave it aimed at a run that is
mid-write. The cache lives in process memory and therefore dies with the
process — a restart empties it by design, which is why ``warm_read_cache``
exists rather than relying on first-read memoization. Invalidation on a code
change is by process replacement: this cache is in-process, and a derivation
change ships as a new image and therefore a new process; if this cache is
ever persisted, a derivation-schema version MUST be added to the key at that
time.

Nothing here edits what either derivation computes. ``verification.ranking``
and ``verification.diagnostics`` stay pure functions of ``(conn, run_id)``
and the cache sits strictly in front of them.
"""

from __future__ import annotations

import copy
import logging
import sqlite3
import threading
from collections.abc import Callable
from typing import cast

from wxverify.db.connection import Database, current_db_generation
from wxverify.verification.diagnostics import observed_wet_precip_mae
from wxverify.verification.ranking import daily_rank_conclusions
from wxverify.verification.runs import PUBLISHED_RUN_KEY_PREFIX

logger = logging.getLogger(__name__)

#: ``(generation, derivation_name, run_id)``.
type _Key = tuple[int, str, int]

_W7_NAME = "daily_rank_conclusions"
_W13_NAME = "observed_wet_precip_mae"

# Four covers the published run's two derivations plus one historical run
# being browsed. The warmed entries do not depend on this number at all --
# they live in the pinned tier, which LRU pressure never touches.
_MAX_ENTRIES = 4

# A fixed tuple, built once at import: it has no lifetime, so there is no
# stale-lock pruning step and no unbounded growth as keys are evicted. False
# sharing costs one extra serialized derivation on a cold cache and cannot
# cause a wrong answer -- correctness comes from the double-checked lookup,
# not from the lock's identity.
_LOCK_STRIPES = 8

# Guards every map mutation and every lookup. It is the sink of the module's
# two-edge lock DAG (stripe -> _LOCK, _WARM_ARBITRATION -> _LOCK): nothing is
# ever acquired while it is held, and it is never held across a derivation or
# across I/O of any kind.
_LOCK = threading.Lock()

# Serializes exactly one thing: a warm's published-pointer snapshot together
# with the epoch ticket that orders it. Separate from _LOCK because _LOCK is
# taken by every request-path lookup and must stay a pure in-memory-map lock,
# so a query must never run under it.
_WARM_ARBITRATION = threading.Lock()

_STRIPES: tuple[threading.Lock, ...] = tuple(
    threading.Lock() for _ in range(_LOCK_STRIPES)
)

# The evictable tier: every request-path insert and every warm insert lands
# here. CPython dicts are insertion-ordered, so move-to-end on hit plus
# "evict the first key" is a complete LRU in three lines.
_ENTRIES: dict[_Key, object] = {}

# The pinned tier: written only by a warm's reconciliation, never by a
# request, never evicted under LRU pressure. Bounded by domain cardinality --
# two entries per enabled site holding a published run.
_PINNED: dict[_Key, object] = {}

# The warm arbitration ticket, and the reset barrier token. Both are
# monotonic counters: only ever incremented, never assigned zero (see
# `reset_read_cache`, where zeroing is the one thing that would make a stale
# ticket current again). Lower case marks them as `global`-rebound module
# state, the idiom `_db_instance` and `_import_in_progress` already follow.
_warm_epoch = 0
_reset_token = 0

# A sentinel rather than `None`: `_cached` is generic in its payload, so a
# `None` return from a `compute` is representable and must not read as a miss.
_MISS: object = object()

# One statement, therefore one implicit read transaction, therefore one point
# in the database's commit history. A per-site loop would be N statements and
# N snapshots, and no single ticket can order two mixtures of old and new
# pointers against each other. The key prefix is BOUND, not interpolated.
_TARGETS_SQL = """
    SELECT s.id, CAST(rs.value AS INTEGER)
    FROM sites s
    JOIN runtime_state rs ON rs.key = ? || s.id
    WHERE s.enabled = 1
    ORDER BY s.id
"""


def _is_published(conn: sqlite3.Connection, run_id: int) -> bool:
    """Whether ``run_id`` names a run in state ``published``.

    Deliberately not cached: a run transitions ``running`` -> ``published``,
    so caching "not publishable" would pin a run as permanently uncacheable.
    The statement is a rowid lookup and costs nothing measurable.
    """
    row = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    return row is not None and str(row[0]) == "published"


def _sweep_stale(generation: int) -> None:
    """Drop every entry from BOTH tiers whose key predates ``generation``.

    Caller must hold ``_LOCK``. Sweeping both tiers is what frees memory
    promptly after a database swap instead of waiting for eviction pressure
    that the pinned tier never feels.
    """
    for tier in (_ENTRIES, _PINNED):
        for key in [k for k in tier if k[0] != generation]:
            del tier[key]


def _lookup(key: _Key, staged: dict[_Key, object] | None) -> object:
    """Population-sequence steps 3 and 5: one tiered lookup under ``_LOCK``.

    Pinned tier first, then the LRU tier, moving an LRU hit to the end. A hit
    is recorded into ``staged`` inside the same critical section, which is
    what lets a publish-time warm re-pin a run that is already cached.
    Returns ``_MISS`` on a miss. The caller must NOT hold ``_LOCK``, and the
    returned object must not be handed out without a copy.
    """
    with _LOCK:
        stored = _PINNED.get(key, _MISS)
        if stored is _MISS:
            stored = _ENTRIES.get(key, _MISS)
            if stored is not _MISS:
                _ENTRIES[key] = _ENTRIES.pop(key)
        if stored is not _MISS and staged is not None:
            staged[key] = stored
        return stored


def _insert(
    key: _Key,
    value: object,
    *,
    reset_token: int,
    staged: dict[_Key, object] | None,
) -> None:
    """Population-sequence step 8, in one ``_LOCK`` critical section.

    Re-reads the generation, sweeps both tiers of everything older, and then
    stores ``value`` ONLY if the key still carries that generation and the
    caller's reset token is still current. Both re-checks are the same rule:
    an operation commits only under the identity it started with. Without the
    generation half, the insert reintroduces exactly the entry the sweep
    exists to remove; without the reset half, ``reset_read_cache`` is a wipe
    that in-flight work lands behind rather than a barrier.

    The caller must NOT hold ``_LOCK``.
    """
    with _LOCK:
        generation = current_db_generation()
        _sweep_stale(generation)
        if key[0] != generation or reset_token != _reset_token:
            return
        _ENTRIES[key] = value
        if staged is not None:
            staged[key] = value
        while len(_ENTRIES) > _MAX_ENTRIES:
            del _ENTRIES[next(iter(_ENTRIES))]


def _next_warm_epoch() -> int:
    """Take the next arbitration ticket and return it.

    Called only from inside the ``_WARM_ARBITRATION`` critical section that
    also takes the pointer snapshot, so ticket order is exactly snapshot
    order.
    """
    global _warm_epoch
    with _LOCK:
        _warm_epoch += 1
        return _warm_epoch


def _epoch_is_current(epoch: int) -> bool:
    """Whether ``epoch`` is still the newest arbitration ticket."""
    with _LOCK:
        return epoch == _warm_epoch


def _reconcile_pins(staged: dict[_Key, object], epoch: int) -> None:
    """Install ``staged`` as the pinned tier, if ``epoch`` is still current.

    REPLACES the pinned tier rather than adding to it, so a process that
    warms a hundred times holds the same number of pinned entries as one that
    warms once, and a pointer flip drops the previous run's pins in the same
    step that installs the new ones. A stale ticket changes nothing at all;
    the superseded warm's entries stay in the evictable tier, where they are
    harmless and correct. Staged keys whose generation has moved are dropped,
    for the same reason ``_insert`` re-checks it.
    """
    with _LOCK:
        if epoch != _warm_epoch:
            return
        generation = current_db_generation()
        fresh = {key: value for key, value in staged.items() if key[0] == generation}
        _PINNED.clear()
        _PINNED.update(fresh)
        for key in fresh:
            _ENTRIES.pop(key, None)


def _published_targets(conn: sqlite3.Connection) -> list[tuple[int, int]]:
    """``(site_id, run_id)`` for every enabled site with a published pointer.

    One statement, so the whole result is one point in the database's commit
    history. A site with no published pointer does not join, contributes no
    target and costs nothing further.
    """
    rows = conn.execute(_TARGETS_SQL, (PUBLISHED_RUN_KEY_PREFIX,)).fetchall()
    return [(int(row[0]), int(row[1])) for row in rows]


def _cached[T](
    conn: sqlite3.Connection,
    run_id: int,
    name: str,
    compute: Callable[[sqlite3.Connection, int], T],
    *,
    staged: dict[_Key, object] | None = None,
) -> T:
    """The one population sequence: the only path that fills this cache.

    Both the request path and the warm path call this, so the published gate,
    the copy-before-insert ordering, the sweep, the eviction and the
    single-flight are each implemented once and tested once.
    """
    # Step 1: the published gate, ahead of the key AND the lookup. A run in
    # any other state is derived and returned uncached, and is never served
    # from an existing entry -- a published pointer is not a verified state.
    if not _is_published(conn, run_id):
        return compute(conn, run_id)

    # Step 2: the key and the reset token, read here rather than at route
    # entry -- the read pool is drained before a swap, so a generation read
    # is stable only for the duration of the read callback it is taken in.
    with _LOCK:
        key: _Key = (current_db_generation(), name, run_id)
        reset_token = _reset_token

    # Step 3: fast-path lookup. The copy is taken after the lock is released.
    stored = _lookup(key, staged)
    if stored is _MISS:
        # Step 4: the key's stripe, with the map lock NOT held.
        with _STRIPES[hash(key) % _LOCK_STRIPES]:
            # Step 5: double-check. A thread that waited serves the winner's
            # value and does not recompute.
            stored = _lookup(key, staged)
            if stored is _MISS:
                # Step 6: compute, stripe held, map lock free.
                result = compute(conn, run_id)
                # Step 7: copy BEFORE any map mutation, and the copy is what
                # gets stored. It validates the payload -- deepcopy raises on
                # a leaked sqlite3.Row, so an uncopyable result fails the
                # request that produced it instead of poisoning the key for
                # every later reader -- and it makes the cache the owner of
                # an object its producer does not hold.
                snapshot = copy.deepcopy(result)
                # Step 8.
                _insert(key, snapshot, reset_token=reset_token, staged=staged)
                stored = snapshot
    # Step 9: the stripe is released by leaving the `with` block on every
    # path including the raise path, and the caller gets a second copy, so
    # its graph is independent of both the cache and the derivation.
    return copy.deepcopy(cast("T", stored))


def cached_daily_rank_conclusions(
    conn: sqlite3.Connection, run_id: int
) -> dict[str, dict[str, object]]:
    """Cached W7: per-variable ``ranking_redesign_indicated`` for one run.

    Exposes no ``leads`` parameter, so a non-default span is unrepresentable
    at the cached surface; the raw ``daily_rank_conclusions`` keeps its
    keyword-only ``leads`` for callers that need one.
    """
    return _cached(conn, run_id, _W7_NAME, daily_rank_conclusions)


def cached_observed_wet_precip_mae(
    conn: sqlite3.Connection, run_id: int
) -> dict[str, object]:
    """Cached W13: mean precip-total error for the run's incumbent depth."""
    return _cached(conn, run_id, _W13_NAME, observed_wet_precip_mae)


def reset_read_cache() -> None:
    """Empty both tiers AND invalidate every computation already in flight.

    A barrier, not a wipe. Clearing alone leaves work already started able to
    land behind it: a warm whose ticket still matched would reconcile the
    previous database's entries into the pinned tier, and a ``_cached`` call
    already past its key step would insert into the evictable tier. Bumping
    ``_warm_epoch`` voids every outstanding ticket -- which both stops that
    reconciliation and, via the warm's per-call ticket check, stops it
    deriving anything further -- and bumping ``_reset_token`` voids the
    insert of any ``_cached`` call that was mid-compute.

    Neither counter is ever ZEROED: zeroing is the one thing that would make
    a stale ticket current again. ``_STRIPES`` is not rebuilt either -- a
    replaced stripe tuple would discard a lock another thread is holding.
    """
    global _warm_epoch, _reset_token
    with _LOCK:
        _ENTRIES.clear()
        _PINNED.clear()
        _warm_epoch += 1
        _reset_token += 1


async def warm_read_cache(db: Database) -> None:
    """Populate both derivations for every enabled site's published run.

    Idempotent and best-effort by contract: returns ``None`` on every path
    except cancellation. ``asyncio.CancelledError`` derives from
    ``BaseException``, not ``Exception``, so it propagates untouched and a
    cancelled warm is never logged as a failure -- it has been told to stop,
    not failed. A cancelled warm leaves whatever it had already inserted in
    the evictable tier and leaves the pinned tier untouched, because
    reconciliation is one step at the end.

    The outer catch is not redundant with the nested one: the nested scope
    covers the derivations, the outer covers the frame around them -- every
    ``db.read``, the pointer snapshot, the epoch ticket and the pin
    reconciliation -- which is exactly the shape that escapes an inner-only
    catch and would reach the lifespan.
    """
    try:

        def _snapshot(
            conn: sqlite3.Connection,
        ) -> tuple[list[tuple[int, int]], int]:
            # The ticket is taken inside THIS callback and inside the same
            # `_WARM_ARBITRATION` critical section as the snapshot. A
            # synchronous callback means no event-loop boundary can fall
            # between them, and the lock means two concurrent warm callbacks
            # cannot interleave their snapshots and increments against each
            # other. Split the two and an earlier-resolved warm can receive
            # the later ticket and overwrite the newer warm's pins.
            with _WARM_ARBITRATION:
                return _published_targets(conn), _next_warm_epoch()

        targets, epoch = await db.read(_snapshot)
        # The warm's OWN staged map: it never reads its results back out of
        # the evictable tier, because by the time it finishes that tier may
        # already have evicted them (three sites produce six keys through a
        # four-entry tier).
        staged: dict[_Key, object] = {}
        for site_id, run_id in targets:
            if not _epoch_is_current(epoch):
                return

            # One `db.read` per site rather than one spanning the whole warm:
            # `replace_from` takes all four pooled connections before it
            # swaps, so a single read would stall an import for the warm's
            # entire duration while a per-site read bounds that stall at one
            # site's work.
            def _fill(
                conn: sqlite3.Connection, site_id: int = site_id, run_id: int = run_id
            ) -> None:
                for name, compute in (
                    (_W7_NAME, daily_rank_conclusions),
                    (_W13_NAME, observed_wet_precip_mae),
                ):
                    if not _epoch_is_current(epoch):
                        return
                    try:
                        _cached(conn, run_id, name, compute, staged=staged)
                    except Exception:
                        logger.exception(
                            "read-cache warm: %s failed for site=%s run=%s",
                            name,
                            site_id,
                            run_id,
                        )

            await db.read(_fill)
        _reconcile_pins(staged, epoch)
    except Exception:
        logger.exception("read-cache warm failed")
