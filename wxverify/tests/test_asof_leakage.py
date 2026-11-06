"""§18.1 leakage oracle for the as-of sample loader (plan §6).

The as-of ranking/leaderboard machinery (``verification/asof.py``,
``scoring.metrics.strategy_for``, ``scoring.leaderboard.asof_leaderboard``,
``forecast.data.forecast_ranking``) was removed with the daily close made
permanent (W6b): the daily close's stored pairs/rankings are now the only
scoring path, so there is no more as-of pair-knowability query to leak
post-T data into.

``load_future_samples(as_of=...)`` survives: the record still reconstructs
a day's samples as they stood at T (9.5). This file keeps only the
leakage oracle for that still-live path.

Every fixture value is synthetic. Decision time T is far-future (2035) so
``fetched_at``/``issued_at`` defaults are always <= T.
"""

from __future__ import annotations

from tests.helpers import (
    asof_conn,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify.forecast.data import count_null_availability_samples, load_future_samples

# Canonical decision time for these fixtures.
_T = "2035-01-02T12:00:00Z"


def test_post_t_sample_backfill_leaves_asof_selection_bit_identical() -> None:
    """Ingestion-backfill guard (§18.1): runs seeded with pre-T `issued_at`/
    `valid_at` but post-T `fetched_at` — and post-T-issued runs with
    adversarially pre-T `fetched_at` — must leave `load_future_samples(as_of=T)`
    bit-identical, and a feed whose only runs are post-T-fetched must be
    absent at the FEED level, not just outscored.

    Kills: the `issued_at <= T` bound dropped from the as-of clause (the
    post-T-issued run would win the latest-run pick), and the
    `fetched_at <= T` bound dropped (the backfilled newer run would win).
    """
    conn = asof_conn()
    # Several labels in one hour exist only in a file below v16 (the rain
    # release, Part B, keeps one row per hour and no interim values): this
    # pins the as-of pick over the labels such a file held.
    conn.execute("DROP INDEX idx_samples_slot")
    site_id = asof_make_site(conn, "Backfill Site")
    feed_a = asof_make_real_feed(conn, "model-a")
    slot1, slot2 = "2035-01-03T06:00:00Z", "2035-01-03T07:00:00Z"
    # Baseline run R1: fully available at T.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=slot1,
        lead_hours=30,
        value=1.0,
        fetched_at="2035-01-01T00:10:00Z",
    )
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=slot2,
        lead_hours=31,
        value=1.5,
        fetched_at="2035-01-01T00:10:00Z",
    )
    since = "2035-01-01T00:00:00Z"
    baseline = load_future_samples(
        conn, site_id=site_id, since_valid_at=since, as_of=_T, served_only=False
    )
    assert [row.value for row in baseline] == [1.0, 1.5]

    # --- Post-T injections ---
    # Backfilled newer run: issued before T, fetched after T (both slots).
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=slot1,
        lead_hours=30,
        value=2.0,
        fetched_at="2035-01-02T18:00:00Z",
    )
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=slot2,
        lead_hours=31,
        value=2.5,
        fetched_at="2035-01-02T18:00:00Z",
    )
    # Issued after T, fetched (adversarially) before T: the issued_at bound
    # must exclude it on its own.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T13:00:00Z",
        valid_at=slot1,
        lead_hours=17,
        value=3.0,
        fetched_at="2035-01-02T11:00:00Z",
    )
    # NULL fetched_at: unplaceable on the as-of timeline, counted not silent.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T01:00:00Z",
        valid_at=slot2,
        lead_hours=30,
        value=3.5,
        fetched_at=None,
    )
    # A second feed whose ONLY runs are backfill-shaped (pre-T issued/valid,
    # post-T fetched): absent from the as-of selection entirely.
    feed_b = asof_make_real_feed(conn, "model-b-backfilled")
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_b,
        issued_at="2035-01-02T02:00:00Z",
        valid_at=slot1,
        lead_hours=28,
        value=4.0,
        fetched_at="2035-01-02T20:00:00Z",
    )

    after = load_future_samples(
        conn, site_id=site_id, since_valid_at=since, as_of=_T, served_only=False
    )
    assert after == baseline
    assert {row.feed_id for row in after} == {feed_a}
    # Paired positive: the injected rows ARE live-visible — the as-of absence
    # above is the bound at work, not invalid rows being dropped.
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at=since, served_only=False
    )
    assert {row.value for row in live} == {3.0, 3.5, 4.0}
    assert (
        count_null_availability_samples(conn, site_id=site_id, since_valid_at=since)
        == 1
    )
