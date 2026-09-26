"""Step 2 tests for the pair-phase read/write split (``scoring.split``).

Every test here runs on a bare ``asof_conn()`` in-memory connection -- no
test instantiates a ``Database`` (the rollback tests call its
``_run_immediate`` transaction wrapper directly against a bare stand-in, see
the T-rollback section below). All fixture data is synthetic.
"""

from __future__ import annotations

import gc
import sqlite3
import tracemalloc
from collections.abc import Callable
from datetime import datetime, timedelta

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_observation,
    asof_insert_pair,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
)
from tests.scoring_ref_0164 import materialize_multimodel_mean_0164
from wxverify.db.connection import Database
from wxverify.db.snapshot import read_only_snapshot
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_id,
)
from wxverify.scoring.engine import PAIR_PHASES, SPLIT_PAIR_PHASES
from wxverify.scoring.multimodel import compute_multimodel_mean
from wxverify.scoring.pair_flags import precip_flags
from wxverify.scoring.pairing import compute_real_model_pairs
from wxverify.scoring.persistence import (
    compute_persistence_pairs,
    materialize_persistence,
)
from wxverify.scoring.split import (
    DeleteOp,
    GenerationResolver,
    InsertOp,
    PairDelta,
    ReplaceOp,
    apply_delta,
    apply_pair_ops,
    chunk_ops,
)

# --------------------------------------------------------------------------
# T-chunk (M19, M22).
# --------------------------------------------------------------------------


def test_chunk_ops_preserves_order_across_chunks() -> None:
    ops = tuple(InsertOp(1, (i,)) for i in range(5))
    chunks = list(chunk_ops(ops, cap=2))
    flattened = tuple(op for chunk in chunks for op in chunk)
    assert flattened == ops


def test_chunk_ops_never_splits_a_replace_across_chunks() -> None:
    i, r = InsertOp(1, (1,)), ReplaceOp(1, 1, (2,))
    ops = (i, i, r)
    assert list(chunk_ops(ops, cap=3)) == [(i, i), (r,)]


def test_chunk_ops_cap_check_runs_only_when_iterated() -> None:
    ops = (InsertOp(1, (1,)),)
    generator = chunk_ops(ops, cap=1)  # raises nothing: it's a generator
    with pytest.raises(ValueError, match="chunk cap must be >= 2"):
        list(generator)


def test_chunk_ops_empty_ops_still_yields_one_chunk() -> None:
    assert list(chunk_ops((), cap=1000)) == [()]


# --------------------------------------------------------------------------
# T-apply (the op shapes).
# --------------------------------------------------------------------------


def test_apply_delta_op_shapes() -> None:
    conn = asof_conn()
    site = asof_make_site(conn, "T-apply site")
    feed = asof_make_real_feed(conn, "t-apply-model")
    asof_insert_sample(
        conn,
        site_id=site,
        feed_id=feed,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-01T06:00:00Z",
        lead_hours=6,
        value=5.0,
        fetched_at="2035-01-01T00:05:00Z",
    )
    asof_insert_observation(
        conn, site_id=site, valid_at="2035-01-01T06:00:00Z", value=4.0, computed_at=None
    )
    conn.commit()

    delta = compute_real_model_pairs(conn, site)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], InsertOp)

    assert apply_delta(conn, delta) == 1
    generation_id = published_generation_id(conn, site)
    assert generation_id is not None
    row = conn.execute(
        "SELECT id, tz_generation_id FROM forecast_pairs WHERE site_id=? AND feed_id=?",
        (site, feed),
    ).fetchone()
    assert row is not None
    assert row["tz_generation_id"] == generation_id
    old_id = int(row["id"])

    # Sentinel row inserted AFTER, at a higher id, on an unrelated site:
    # without AUTOINCREMENT, SQLite's next rowid is max(rowid)+1, so leaving
    # a higher-id row in place while we delete old_id below guarantees the
    # reinsert gets a genuinely new, larger id -- never old_id come back
    # around by coincidence.
    sentinel_site = asof_make_site(conn, "T-apply sentinel site")
    asof_insert_pair(
        conn,
        site_id=sentinel_site,
        feed_id=feed,
        valid_at="1999-01-01T00:00:00Z",
        issued_at="1998-12-31T18:00:00Z",
        forecast=1.0,
        observed=1.0,
        first_known_at=None,
    )
    conn.commit()

    before = dict(
        conn.execute("SELECT * FROM forecast_pairs WHERE id=?", (old_id,)).fetchone()
    )
    del before["id"], before["created_at"]

    resolver = GenerationResolver((site,))
    resolver.seed(conn)
    replace_values = delta.ops[0].values
    replaced = apply_pair_ops(
        conn,
        delta,
        (ReplaceOp(old_id, site, replace_values),),
        resolver,
    )
    assert replaced == 1
    assert (
        conn.execute("SELECT id FROM forecast_pairs WHERE id=?", (old_id,)).fetchone()
        is None
    )
    new_row = conn.execute(
        "SELECT * FROM forecast_pairs WHERE site_id=? AND feed_id=?", (site, feed)
    ).fetchone()
    assert new_row is not None
    after = dict(new_row)
    new_id = int(after.pop("id"))
    assert new_id != old_id
    del after["created_at"]
    assert after == before

    deleted = apply_pair_ops(conn, delta, (DeleteOp(new_id),), resolver)
    assert deleted == 0
    assert (
        conn.execute("SELECT id FROM forecast_pairs WHERE id=?", (new_id,)).fetchone()
        is None
    )

    other_site = asof_make_site(conn, "T-apply other site")
    with pytest.raises(KeyError):
        resolver.generation_id(other_site)

    counted_delta = PairDelta((site,), delta.ops, delta.insert_sql, count=7)
    assert apply_delta(conn, counted_delta) == 7


# --------------------------------------------------------------------------
# T-vanish (MX-vanish).
# --------------------------------------------------------------------------


def test_apply_pair_ops_vanished_row_raises() -> None:
    conn = asof_conn()
    site = asof_make_site(conn, "T-vanish site")
    conn.commit()
    resolver = GenerationResolver((site,))
    resolver.seed(conn)
    before = conn.execute("SELECT count(*) FROM forecast_pairs").fetchone()[0]
    dummy_delta = PairDelta(
        (site,), (), "INSERT INTO forecast_pairs DEFAULT VALUES", None
    )

    with pytest.raises(RuntimeError, match="vanished before apply"):
        apply_pair_ops(conn, dummy_delta, (DeleteOp(999_999),), resolver)
    assert conn.execute("SELECT count(*) FROM forecast_pairs").fetchone()[0] == before

    with pytest.raises(RuntimeError, match="vanished before apply"):
        apply_pair_ops(conn, dummy_delta, (ReplaceOp(999_999, site, (1,)),), resolver)
    assert conn.execute("SELECT count(*) FROM forecast_pairs").fetchone()[0] == before


# --------------------------------------------------------------------------
# T-rollback: a failing op after a valid write rolls back the WHOLE phase
# transaction. These tests exercise rollback through the real
# ``Database._run_immediate`` transaction wrapper (wxverify/db/connection.py),
# using a stand-in that supplies only ``_conn`` -- avoids standing up the
# full async ``Database``/``FencedWriter`` stack for a single-connection test.
# --------------------------------------------------------------------------


class _FakeDbConn:
    """Exposes only the attribute ``Database._run_immediate`` touches."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn


def _run_in_production_wrapper(
    conn: sqlite3.Connection, fn: Callable[[sqlite3.Connection], object]
) -> object:
    return Database._run_immediate(_FakeDbConn(conn), fn)  # noqa: SLF001


def test_apply_delta_rolls_back_valid_insert_before_a_vanished_delete() -> None:
    conn = asof_conn()
    site, _feed_a, _feed_b = _seed_mm_base(conn)
    template = compute_multimodel_mean(conn, site)
    assert len(template.ops) == 1
    assert isinstance(template.ops[0], InsertOp)

    delta = PairDelta(
        template.seed_sites,
        (*template.ops, DeleteOp(999_999)),
        template.insert_sql,
        count=None,
    )

    with pytest.raises(RuntimeError, match="vanished before apply"):
        _run_in_production_wrapper(conn, lambda c: apply_delta(c, delta))

    assert _mean_rows(conn) == []


def test_apply_delta_rolls_back_valid_insert_before_a_colliding_insert() -> None:
    conn = asof_conn()
    site, feed_a, feed_b = _seed_mm_base(conn)
    materialize_multimodel_mean_0164(conn, site)
    conn.commit()
    existing_row = conn.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=? ORDER BY id DESC LIMIT 1",
        (_mean_feed_id(conn),),
    ).fetchone()
    assert existing_row is not None
    colliding_values = (
        existing_row["site_id"],
        existing_row["feed_id"],
        existing_row["variable"],
        existing_row["issued_at"],
        existing_row["valid_at"],
        existing_row["lead_hours"],
        existing_row["day_ahead"],
        existing_row["forecast"],
        existing_row["observed"],
        existing_row["error"],
        existing_row["abs_error"],
        existing_row["sq_error"],
        existing_row["cat_hit"],
        existing_row["cat_false"],
        existing_row["cat_miss"],
        existing_row["cat_correct_neg"],
        existing_row["rain_threshold_mm"],
        existing_row["contributors"],
    )

    # A brand-new key: the valid write that must be rolled back.
    asof_insert_pair(
        conn,
        site_id=site,
        feed_id=feed_a,
        valid_at="2035-01-01T11:00:00Z",
        issued_at=_MM_ISSUED_AT,
        forecast=3.5,
        observed=4.5,
        first_known_at=_MM_FIRST_KNOWN_AT,
    )
    asof_insert_pair(
        conn,
        site_id=site,
        feed_id=feed_b,
        valid_at="2035-01-01T11:00:00Z",
        issued_at=_MM_ISSUED_AT,
        forecast=5.5,
        observed=4.5,
        first_known_at=_MM_FIRST_KNOWN_AT,
    )
    conn.commit()

    template = compute_multimodel_mean(conn, site)
    assert len(template.ops) == 1
    assert isinstance(template.ops[0], InsertOp)
    valid_insert = template.ops[0]

    rows_before = _mean_rows(conn)
    delta = PairDelta(
        template.seed_sites,
        (valid_insert, InsertOp(site, colliding_values)),
        template.insert_sql,
        count=None,
    )

    with pytest.raises(sqlite3.IntegrityError):
        _run_in_production_wrapper(conn, lambda c: apply_delta(c, delta))

    assert _mean_rows(conn) == rows_before


# --------------------------------------------------------------------------
# T-MM1 .. T-MM9 (multimodel diff).
# --------------------------------------------------------------------------

_MM_VALID_AT = "2035-01-01T06:00:00Z"
_MM_ISSUED_AT = "2035-01-01T00:00:00Z"
_MM_FIRST_KNOWN_AT = "2035-01-01T00:05:00Z"


def _seed_mm_base(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """One site, two real feeds, one pair each (forecasts 3.0/5.0, obs 4.0)."""
    site = asof_make_site(conn, "T-MM site")
    feed_a = asof_make_real_feed(conn, "t-mm-model-a")
    feed_b = asof_make_real_feed(conn, "t-mm-model-b")
    asof_insert_pair(
        conn,
        site_id=site,
        feed_id=feed_a,
        valid_at=_MM_VALID_AT,
        issued_at=_MM_ISSUED_AT,
        forecast=3.0,
        observed=4.0,
        first_known_at=_MM_FIRST_KNOWN_AT,
    )
    asof_insert_pair(
        conn,
        site_id=site,
        feed_id=feed_b,
        valid_at=_MM_VALID_AT,
        issued_at=_MM_ISSUED_AT,
        forecast=5.0,
        observed=4.0,
        first_known_at=_MM_FIRST_KNOWN_AT,
    )
    conn.commit()
    return site, feed_a, feed_b


def _mean_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _mean_rows(conn: sqlite3.Connection) -> list[dict[str, object]]:
    mean_feed = _mean_feed_id(conn)
    rows = conn.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=? ORDER BY issued_at, valid_at",
        (mean_feed,),
    ).fetchall()
    result = []
    for row in rows:
        as_dict = dict(row)
        del as_dict["id"], as_dict["created_at"]
        result.append(as_dict)
    return result


def _insert_precip_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    forecast: float,
    observed: float,
) -> None:
    """Precipitation-flavored pair insert (``asof_insert_pair`` is temperature-only)."""
    generation_id = ensure_published_generation(conn, site_id)
    error = forecast - observed
    threshold = conn.execute(
        "SELECT rain_threshold_mm FROM sites WHERE id=?", (site_id,)
    ).fetchone()[0]
    hit, false, miss, correct_neg = precip_flags(
        "precip", forecast, observed, float(threshold)
    )
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm, first_known_at, tz_generation_id)
        VALUES (?, ?, 'precip', ?, ?, 6, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            _MM_ISSUED_AT,
            _MM_VALID_AT,
            forecast,
            observed,
            error,
            abs(error),
            error**2,
            hit,
            false,
            miss,
            correct_neg,
            threshold,
            _MM_FIRST_KNOWN_AT,
            generation_id,
        ),
    )


def test_mm1_no_change_is_a_noop() -> None:
    left, right = asof_conn(), asof_conn()
    site, _a, _b = _seed_mm_base(left)
    _seed_mm_base(right)
    assert materialize_multimodel_mean_0164(left, site) == 1
    assert materialize_multimodel_mean_0164(right, site) == 1
    left_mean_id = int(
        left.execute(
            "SELECT id FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
        ).fetchone()["id"]
    )
    delta = compute_multimodel_mean(left, site)
    assert delta.ops == ()
    assert delta.count == 1
    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    assert _mean_rows(left) == _mean_rows(right)
    assert (
        int(
            left.execute(
                "SELECT id FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
            ).fetchone()["id"]
        )
        == left_mean_id
    )


def test_mm2_value_change_yields_one_replace() -> None:
    left, right = asof_conn(), asof_conn()
    site, feed_a, _b = _seed_mm_base(left)
    _seed_mm_base(right)
    materialize_multimodel_mean_0164(left, site)
    materialize_multimodel_mean_0164(right, site)

    def bump(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE forecast_pairs SET forecast = forecast + 1.0,"
            " error = error + 1.0, abs_error = ABS(error + 1.0),"
            " sq_error = (error + 1.0) * (error + 1.0)"
            " WHERE site_id=? AND feed_id=?",
            (site, feed_a),
        )

    bump(left)
    bump(right)

    mean_id_before = int(
        left.execute(
            "SELECT id FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
        ).fetchone()["id"]
    )
    delta = compute_multimodel_mean(left, site)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], ReplaceOp)
    assert delta.ops[0].row_id == mean_id_before

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    assert _mean_rows(left) == _mean_rows(right)


def test_mm3_new_key_yields_one_insert() -> None:
    left, right = asof_conn(), asof_conn()
    site, feed_a, feed_b = _seed_mm_base(left)
    _seed_mm_base(right)
    materialize_multimodel_mean_0164(left, site)
    materialize_multimodel_mean_0164(right, site)

    def add_hour(conn: sqlite3.Connection) -> None:
        asof_insert_pair(
            conn,
            site_id=site,
            feed_id=feed_a,
            valid_at="2035-01-01T07:00:00Z",
            issued_at=_MM_ISSUED_AT,
            forecast=3.5,
            observed=4.5,
            first_known_at=_MM_FIRST_KNOWN_AT,
        )
        asof_insert_pair(
            conn,
            site_id=site,
            feed_id=feed_b,
            valid_at="2035-01-01T07:00:00Z",
            issued_at=_MM_ISSUED_AT,
            forecast=5.5,
            observed=4.5,
            first_known_at=_MM_FIRST_KNOWN_AT,
        )

    add_hour(left)
    add_hour(right)

    delta = compute_multimodel_mean(left, site)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], InsertOp)
    assert delta.count == 2

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    assert _mean_rows(left) == _mean_rows(right)


def test_mm4_leftover_row_yields_delete(  # MX-leftover
) -> None:
    left, right = asof_conn(), asof_conn()
    site, feed_a, feed_b = _seed_mm_base(left)
    _seed_mm_base(right)
    materialize_multimodel_mean_0164(left, site)
    materialize_multimodel_mean_0164(right, site)

    def delete_all_real_pairs(conn: sqlite3.Connection) -> None:
        conn.execute(
            "DELETE FROM forecast_pairs WHERE site_id=? AND feed_id IN (?, ?)",
            (site, feed_a, feed_b),
        )

    delete_all_real_pairs(left)
    delete_all_real_pairs(right)

    mean_id = int(
        left.execute(
            "SELECT id FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
        ).fetchone()["id"]
    )
    delta = compute_multimodel_mean(left, site)
    assert delta.ops == (DeleteOp(mean_id),)
    assert delta.count == 0

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    assert _mean_rows(left) == _mean_rows(right) == []


def test_mm5_added_contributor_at_unchanged_mean() -> None:  # M8
    left, right = asof_conn(), asof_conn()
    site, feed_a, feed_b = _seed_mm_base(left)
    _seed_mm_base(right)
    materialize_multimodel_mean_0164(left, site)
    materialize_multimodel_mean_0164(right, site)

    def add_third_feed(conn: sqlite3.Connection) -> None:
        feed_c = asof_make_real_feed(conn, "t-mm-model-c")
        asof_insert_pair(
            conn,
            site_id=site,
            feed_id=feed_c,
            valid_at=_MM_VALID_AT,
            issued_at=_MM_ISSUED_AT,
            forecast=4.0,
            observed=4.0,
            first_known_at=_MM_FIRST_KNOWN_AT,
        )

    add_third_feed(left)
    add_third_feed(right)

    delta = compute_multimodel_mean(left, site)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], ReplaceOp)

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    applied_row = left.execute(
        "SELECT contributors FROM forecast_pairs WHERE feed_id=?",
        (_mean_feed_id(left),),
    ).fetchone()
    assert applied_row["contributors"] == 3
    assert _mean_rows(left) == _mean_rows(right)


def test_mm6_precip_flags_follow_site_threshold() -> None:
    left, right = asof_conn(), asof_conn()
    site_l = asof_make_site(left, "T-MM6 site")
    feed_a_l = asof_make_real_feed(left, "t-mm6-model-a")
    feed_b_l = asof_make_real_feed(left, "t-mm6-model-b")
    site_r = asof_make_site(right, "T-MM6 site")
    feed_a_r = asof_make_real_feed(right, "t-mm6-model-a")
    feed_b_r = asof_make_real_feed(right, "t-mm6-model-b")
    assert (site_l, feed_a_l, feed_b_l) == (site_r, feed_a_r, feed_b_r)

    arms = ((left, feed_a_l, feed_b_l), (right, feed_a_r, feed_b_r))
    for conn, feed_a, feed_b in arms:
        _insert_precip_pair(
            conn, site_id=site_l, feed_id=feed_a, forecast=0.3, observed=0.0
        )
        _insert_precip_pair(
            conn, site_id=site_l, feed_id=feed_b, forecast=0.5, observed=0.0
        )
        conn.commit()

    materialize_multimodel_mean_0164(left, site_l)
    materialize_multimodel_mean_0164(right, site_l)
    mean_row = left.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
    ).fetchone()
    assert (
        mean_row["cat_hit"],
        mean_row["cat_false"],
        mean_row["cat_miss"],
        mean_row["cat_correct_neg"],
    ) == (0, 1, 0, 0)

    def raise_threshold(conn: sqlite3.Connection) -> None:
        conn.execute("UPDATE sites SET rain_threshold_mm = 1.0 WHERE id=?", (site_l,))

    raise_threshold(left)
    raise_threshold(right)

    delta = compute_multimodel_mean(left, site_l)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], ReplaceOp)

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site_l)
    applied_row = left.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=?", (_mean_feed_id(left),)
    ).fetchone()
    assert (
        applied_row["cat_hit"],
        applied_row["cat_false"],
        applied_row["cat_miss"],
        applied_row["cat_correct_neg"],
    ) == (0, 0, 0, 1)
    assert applied_row["rain_threshold_mm"] == 1.0
    assert _mean_rows(left) == _mean_rows(right)


def test_mm7_first_known_at_replaced_with_null() -> None:  # MX-first_known_at
    left, right = asof_conn(), asof_conn()
    site, _a, _b = _seed_mm_base(left)
    _seed_mm_base(right)
    materialize_multimodel_mean_0164(left, site)
    materialize_multimodel_mean_0164(right, site)

    def stamp_mean_row(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE forecast_pairs SET first_known_at = ? WHERE feed_id=?",
            (_MM_FIRST_KNOWN_AT, _mean_feed_id(conn)),
        )

    stamp_mean_row(left)
    stamp_mean_row(right)

    delta = compute_multimodel_mean(left, site)
    assert len(delta.ops) == 1
    assert isinstance(delta.ops[0], ReplaceOp)

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    applied_row = left.execute(
        "SELECT first_known_at FROM forecast_pairs WHERE feed_id=?",
        (_mean_feed_id(left),),
    ).fetchone()
    assert applied_row["first_known_at"] is None
    assert _mean_rows(left) == _mean_rows(right)


def test_mm8_duplicate_key_first_group_wins() -> None:  # M7, MX-consumed-marker
    left, right = asof_conn(), asof_conn()
    for conn in (left, right):
        site = asof_make_site(conn, "T-MM8 site")
        feeds = [asof_make_real_feed(conn, f"t-mm8-model-{n}") for n in range(4)]
        for feed_id, observed in zip(feeds, (4.0, 4.0, 5.0, 5.0), strict=True):
            asof_insert_pair(
                conn,
                site_id=site,
                feed_id=feed_id,
                valid_at=_MM_VALID_AT,
                issued_at=_MM_ISSUED_AT,
                forecast=observed,
                observed=observed,
                first_known_at=_MM_FIRST_KNOWN_AT,
            )
        conn.commit()
        if conn is left:
            left_site = site
        else:
            right_site = site
    assert left_site == right_site
    site = left_site

    # First pass: population from scratch, first D group wins on both arms.
    assert materialize_multimodel_mean_0164(left, site) == 1
    assert materialize_multimodel_mean_0164(right, site) == 1
    assert _mean_rows(left) == _mean_rows(right)

    # Second pass: the live diff against the now-populated data must be a
    # no-op -- if a consumed key were released back to "available" (M7) or
    # its marker were deleted instead of set to None (MX-consumed-marker),
    # the second D group (observed=5.0) would overwrite the first
    # (observed=4.0), either crashing (e.g. an AttributeError on a released
    # sentinel) or producing a spurious ReplaceOp here.
    delta = compute_multimodel_mean(left, site)
    assert delta.count == 1
    assert delta.ops == ()

    assert apply_delta(left, delta) == materialize_multimodel_mean_0164(right, site)
    assert _mean_rows(left) == _mean_rows(right)


def test_mm9_mean_insert_no_longer_ignores_conflicts() -> None:  # MX-mean-ignore
    conn = asof_conn()
    site, _a, _b = _seed_mm_base(conn)
    materialize_multimodel_mean_0164(conn, site)
    mean_row = conn.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=? ORDER BY id DESC LIMIT 1",
        (_mean_feed_id(conn),),
    ).fetchone()
    assert mean_row is not None
    values = (
        mean_row["site_id"],
        mean_row["feed_id"],
        mean_row["variable"],
        mean_row["issued_at"],
        mean_row["valid_at"],
        mean_row["lead_hours"],
        mean_row["day_ahead"],
        mean_row["forecast"],
        mean_row["observed"],
        mean_row["error"],
        mean_row["abs_error"],
        mean_row["sq_error"],
        mean_row["cat_hit"],
        mean_row["cat_false"],
        mean_row["cat_miss"],
        mean_row["cat_correct_neg"],
        mean_row["rain_threshold_mm"],
        mean_row["contributors"],
    )
    template = compute_multimodel_mean(conn, site)
    duplicate_delta = PairDelta(
        (site,), (InsertOp(site, values),), template.insert_sql, count=None
    )
    with pytest.raises(sqlite3.IntegrityError):
        apply_delta(conn, duplicate_delta)


def test_multimodel_replace_shares_existing_key_strings() -> (
    None
):  # T-MM10, MX-dup-strings
    """A ReplaceOp built from X's key strings holds no second copy of them.

    N = 4,000 keys, two feeds. A correct replace reuses ``current.key``'s
    strings (variable, issued_at, valid_at) instead of allocating fresh ones
    from the recomputed ``values``, so the per-row memory a replace adds
    beyond an insert (which already shares its strings with D's own dict
    key) is small. The mutant, ``ReplaceOp(current.row_id, row_site_id,
    values)``, holds a second copy of all three key strings per row.
    """
    conn = asof_conn()
    site = asof_make_site(conn, "T-MM10 site")
    feed_a = asof_make_real_feed(conn, "t-mm10-model-a")
    feed_b = asof_make_real_feed(conn, "t-mm10-model-b")
    n = 4_000
    base_valid_at = datetime.fromisoformat(_MM_VALID_AT.replace("Z", "+00:00"))
    for k in range(n):
        valid_at = (base_valid_at + timedelta(hours=k)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for feed_id, forecast in ((feed_a, 3.0), (feed_b, 5.0)):
            asof_insert_pair(
                conn,
                site_id=site,
                feed_id=feed_id,
                valid_at=valid_at,
                issued_at=_MM_ISSUED_AT,
                forecast=forecast,
                observed=4.0,
                first_known_at=_MM_FIRST_KNOWN_AT,
            )
    conn.commit()
    apply_delta(conn, compute_multimodel_mean(conn, site))
    conn.commit()

    mean_feed = _mean_feed_id(conn)

    started_tracing = not tracemalloc.is_tracing()
    if started_tracing:
        tracemalloc.start()
    gc_was_enabled = gc.isenabled()
    try:
        conn.execute(
            "UPDATE forecast_pairs SET sq_error = sq_error + 1.0 WHERE feed_id = ?",
            (mean_feed,),
        )
        conn.commit()
        gc.collect()
        base = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        delta = compute_multimodel_mean(conn, site)
        replace_rise = tracemalloc.get_traced_memory()[1] - base
        assert len(delta.ops) == n
        assert all(isinstance(op, ReplaceOp) for op in delta.ops)
        del delta

        conn.execute("DELETE FROM forecast_pairs WHERE feed_id = ?", (mean_feed,))
        conn.commit()
        gc.collect()
        base = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        delta = compute_multimodel_mean(conn, site)
        insert_rise = tracemalloc.get_traced_memory()[1] - base
        assert len(delta.ops) == n
        assert all(isinstance(op, InsertOp) for op in delta.ops)
        del delta
    finally:
        if started_tracing:
            tracemalloc.stop()
        assert gc.isenabled() == gc_was_enabled

    per_row = (replace_rise - insert_rise) / n
    assert per_row < 100


# --------------------------------------------------------------------------
# T-MM-converge: the new apply's own recompute must see its own writes as
# unchanged -- test_mm1/test_mm8 only populate with the frozen 0.16.4
# reference code, so neither proves this about the NEW apply.
# --------------------------------------------------------------------------

_MM_VALID_AT_KEEP = "2035-01-01T09:00:00Z"
_MM_VALID_AT_INSERT = "2035-01-01T10:00:00Z"


def _mean_rows_with_ids(conn: sqlite3.Connection) -> list[dict[str, object]]:
    """Full ``forecast_pairs`` mean-feed row snapshot, ids and all."""
    mean_feed = _mean_feed_id(conn)
    rows = conn.execute(
        "SELECT * FROM forecast_pairs WHERE feed_id=? ORDER BY issued_at, valid_at",
        (mean_feed,),
    ).fetchall()
    return [dict(row) for row in rows]


def test_new_apply_converges_with_insert_replace_and_delete_in_one_delta() -> None:
    """The new apply's own recompute must see its own writes as unchanged.

    One fixture assembled so a SINGLE ``compute_multimodel_mean`` call yields
    all three op kinds at once: a leftover published row whose underlying
    real pairs have since vanished (``DeleteOp``), a published row stamped
    with a non-NULL ``first_known_at`` that must be nulled out (``ReplaceOp``,
    §5.4), and a brand-new group with no published row yet (``InsertOp``).
    Then the SECOND compute against the new apply's own writes must be a
    true no-op, and applying that empty second delta must leave every row
    -- ids included -- byte-identical.
    """
    conn = asof_conn()
    site = asof_make_site(conn, "T-converge site")
    feed_a = asof_make_real_feed(conn, "t-converge-model-a")
    feed_b = asof_make_real_feed(conn, "t-converge-model-b")

    for valid_at in (_MM_VALID_AT, _MM_VALID_AT_KEEP):
        for feed_id, forecast in ((feed_a, 3.0), (feed_b, 5.0)):
            asof_insert_pair(
                conn,
                site_id=site,
                feed_id=feed_id,
                valid_at=valid_at,
                issued_at=_MM_ISSUED_AT,
                forecast=forecast,
                observed=4.0,
                first_known_at=_MM_FIRST_KNOWN_AT,
            )
    conn.commit()

    # Pre-populate published mean rows for the delete- and keep-keys with the
    # frozen 0.16.4 reference -- these are the OLD published rows the first
    # compute under test (below) diffs against, mirroring a live system that
    # already has published mean rows before Step 2 ships.
    materialize_multimodel_mean_0164(conn, site)
    conn.commit()

    # DeleteOp source: the underlying real pairs for this key vanish.
    conn.execute(
        "DELETE FROM forecast_pairs"
        " WHERE site_id=? AND valid_at=? AND feed_id IN (?, ?)",
        (site, _MM_VALID_AT, feed_a, feed_b),
    )
    # ReplaceOp source: the keep-key's published mean row gets a first_known_at.
    conn.execute(
        "UPDATE forecast_pairs SET first_known_at = ? WHERE feed_id=? AND valid_at=?",
        (_MM_FIRST_KNOWN_AT, _mean_feed_id(conn), _MM_VALID_AT_KEEP),
    )
    conn.commit()

    # InsertOp source: a brand-new group with no published mean row yet.
    for feed_id, forecast in ((feed_a, 3.0), (feed_b, 5.0)):
        asof_insert_pair(
            conn,
            site_id=site,
            feed_id=feed_id,
            valid_at=_MM_VALID_AT_INSERT,
            issued_at=_MM_ISSUED_AT,
            forecast=forecast,
            observed=4.0,
            first_known_at=_MM_FIRST_KNOWN_AT,
        )
    conn.commit()

    delta = compute_multimodel_mean(conn, site)
    assert any(isinstance(op, InsertOp) for op in delta.ops)
    assert any(isinstance(op, ReplaceOp) for op in delta.ops)
    assert any(isinstance(op, DeleteOp) for op in delta.ops)

    apply_delta(conn, delta)
    conn.commit()

    rows_by_valid_at = {row["valid_at"]: row for row in _mean_rows_with_ids(conn)}
    assert _MM_VALID_AT not in rows_by_valid_at
    assert rows_by_valid_at[_MM_VALID_AT_KEEP]["first_known_at"] is None
    assert rows_by_valid_at[_MM_VALID_AT_KEEP]["forecast"] == 4.0
    assert rows_by_valid_at[_MM_VALID_AT_INSERT]["forecast"] == 4.0

    snapshot_after_first_apply = _mean_rows_with_ids(conn)

    second = compute_multimodel_mean(conn, site)
    assert second.ops == ()
    assert second.count == 2  # keep-key + insert-key: the only derivable keys

    apply_delta(conn, second)
    conn.commit()
    assert _mean_rows_with_ids(conn) == snapshot_after_first_apply


# --------------------------------------------------------------------------
# T-M9 (M9): persistence seed_sites come from every group's site.
# --------------------------------------------------------------------------


def test_persistence_seeds_from_every_group_even_with_no_ops() -> None:
    conn = asof_conn()
    site = asof_make_site(conn, "T-M9 site")
    asof_insert_observation(
        conn, site_id=site, valid_at="2035-01-01T06:00:00Z", value=4.0, computed_at=None
    )
    conn.commit()

    assert published_generation_id(conn, site) is None
    assert materialize_persistence(conn, site) == 0
    assert published_generation_id(conn, site) is not None


# --------------------------------------------------------------------------
# T-M17 (M17): the pairing/persistence computes never write under a snapshot.
# --------------------------------------------------------------------------


def test_computes_never_write_the_published_generation() -> None:
    conn = asof_conn()
    site = asof_make_site(conn, "T-M17 site")
    feed = asof_make_real_feed(conn, "t-m17-model")
    asof_insert_sample(
        conn,
        site_id=site,
        feed_id=feed,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-01T06:00:00Z",
        lead_hours=6,
        value=5.0,
        fetched_at="2035-01-01T00:05:00Z",
    )
    asof_insert_observation(
        conn, site_id=site, valid_at="2035-01-01T06:00:00Z", value=4.0, computed_at=None
    )
    conn.commit()
    assert published_generation_id(conn, site) is None

    with read_only_snapshot(conn, label="t"):
        pairing_delta = compute_real_model_pairs(conn, site)
        persistence_delta = compute_persistence_pairs(conn, site)

    assert pairing_delta.seed_sites == (site,)
    assert persistence_delta.seed_sites == (site,)
    assert len(pairing_delta.ops) >= 1

    total_changes_before = conn.total_changes
    with (
        pytest.raises(sqlite3.OperationalError),
        read_only_snapshot(conn, label="t2"),
    ):
        ensure_published_generation(conn, site)
    assert conn.total_changes == total_changes_before


# --------------------------------------------------------------------------
# T-names (the log contract).
# --------------------------------------------------------------------------


def test_split_pair_phase_names_match_pair_phases_order() -> None:
    assert [p.name for p in SPLIT_PAIR_PHASES] == [f.__name__ for f in PAIR_PHASES]
